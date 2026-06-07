---
title: "AWS Step Functions - Intermediate"
topic: aws-services
subtopic: step-functions
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, step-functions, orchestration, state-machine, error-handling]
---

# AWS Step Functions — Intermediate Concepts

## State Types in Depth

| State Type | Purpose | Key Use Case |
|-----------|---------|--------------|
| Task | Execute work (Lambda, API call, SDK) | Run ETL step, invoke Glue job |
| Choice | Branch logic (if/else) | Route based on file size or status |
| Parallel | Run branches concurrently | Process multiple data sources at once |
| Map | Iterate over a collection | Process each file in an S3 prefix |
| Wait | Pause execution | Wait for external system readiness |
| Pass | Transform data, no-op | Inject defaults or reshape state |
| Succeed/Fail | Terminal states | End workflow with status |

### Choice State — Branching Logic

```json
{
  "Type": "Choice",
  "Choices": [
    {
      "Variable": "$.fileCount",
      "NumericGreaterThan": 1000,
      "Next": "LargeFileProcessing"
    },
    {
      "Variable": "$.fileFormat",
      "StringEquals": "csv",
      "Next": "CSVParser"
    }
  ],
  "Default": "StandardProcessing"
}
```

### Map State — Iterate Over Items

```json
{
  "Type": "Map",
  "ItemsPath": "$.files",
  "MaxConcurrency": 10,
  "Iterator": {
    "StartAt": "ProcessFile",
    "States": {
      "ProcessFile": {
        "Type": "Task",
        "Resource": "arn:aws:lambda:us-east-1:123456789:function:process-file",
        "End": true
      }
    }
  },
  "ResultPath": "$.processingResults"
}
```

---

## Input/Output Processing

Step Functions controls data flow with four JSON path fields:

```
InputPath  → filters input BEFORE the state runs
Parameters → constructs the payload sent to the task
ResultPath → places the task output into the state input
OutputPath → filters the combined result AFTER the state runs
```

### Practical Example

```json
{
  "Type": "Task",
  "Resource": "arn:aws:lambda:us-east-1:123456789:function:transform",
  "InputPath": "$.rawData",
  "Parameters": {
    "bucket.$": "$.s3Bucket",
    "key.$": "$.s3Key",
    "format": "parquet"
  },
  "ResultPath": "$.transformResult",
  "OutputPath": "$",
  "Next": "Validate"
}
```

**Key rule:** `ResultPath` merges task output back into the original input. Setting `"ResultPath": "$.transformResult"` keeps all original fields and adds the result under a new key.

---

## Error Handling — Retry and Catch

### Retry with Exponential Backoff

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::glue:startJobRun.sync",
  "Parameters": {
    "JobName": "etl-daily-transform"
  },
  "Retry": [
    {
      "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
      "IntervalSeconds": 60,
      "MaxAttempts": 3,
      "BackoffRate": 2.0
    },
    {
      "ErrorEquals": ["States.TaskFailed"],
      "IntervalSeconds": 30,
      "MaxAttempts": 2,
      "BackoffRate": 1.5
    }
  ],
  "Catch": [
    {
      "ErrorEquals": ["States.ALL"],
      "ResultPath": "$.error",
      "Next": "NotifyFailure"
    }
  ],
  "Next": "ValidateOutput"
}
```

**Error hierarchy:** Specific errors first, then `States.ALL` as fallback. Common error types: `States.TaskFailed`, `States.Timeout`, `States.Permissions`, `Lambda.ServiceException`.

---

## Callback Pattern (Wait for External Event)

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
  "Parameters": {
    "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/approval-queue",
    "MessageBody": {
      "taskToken.$": "$$.Task.Token",
      "requestType": "data-access-approval",
      "requestedBy.$": "$.requestor"
    }
  },
  "TimeoutSeconds": 86400,
  "Next": "GrantAccess"
}
```

The workflow pauses until an external system calls `SendTaskSuccess` with the token:

```python
import boto3

sfn = boto3.client('stepfunctions')
sfn.send_task_success(
    taskToken=token_from_message,
    output='{"approved": true, "approver": "admin@company.com"}'
)
```

---

## Express vs Standard Workflows

| Feature | Standard | Express |
|---------|----------|---------|
| Max duration | 1 year | 5 minutes |
| Execution model | Exactly-once | At-least-once (async) or at-most-once (sync) |
| Pricing | Per state transition ($0.025/1000) | Per request + duration |
| Max executions | 25,000/sec start rate | 100,000/sec |
| History | Full (90 days in console) | CloudWatch Logs only |
| Best for | Long ETL, human approval | High-volume event processing |

**Cost comparison (1M executions, 5 states each):**
- Standard: 5M transitions × $0.025/1000 = $125
- Express: 1M requests × $0.000001 + duration = ~$1-5

> Use Express for short, high-volume workflows (API backends, stream processing). Use Standard for ETL orchestration where exactly-once matters.

---

## Interview Tips

> **Tip 1:** "How do you handle errors in Step Functions?" — "Use Retry for transient failures with exponential backoff (set IntervalSeconds, MaxAttempts, BackoffRate). Use Catch as a fallback to route to error-handling states (notification, cleanup). Always catch States.ALL as a last resort. The key is ordering: most specific errors first, generic catch last."

> **Tip 2:** "Explain input/output processing" — "Four fields control data flow: InputPath filters what the state sees, Parameters constructs the task payload, ResultPath places the result back into the original state (critical for preserving context), OutputPath filters what passes to the next state. ResultPath is the most important — it lets you accumulate results across states without losing earlier data."

> **Tip 3:** "When would you use Express over Standard workflows?" — "Express for high-volume, short-duration tasks (API orchestration, stream enrichment, IoT processing) where at-least-once is acceptable. Standard for ETL pipelines needing exactly-once, long-running jobs, human approvals, or when you need execution history for debugging. Cost difference can be 100x at scale."
