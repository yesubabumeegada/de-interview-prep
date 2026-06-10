---
title: "AWS Step Functions - Scenario Questions"
topic: aws-services
subtopic: step-functions
content_type: scenario_question
tags: [aws, step-functions, interview, scenarios, orchestration]
---

# Scenario Questions — AWS Step Functions

<article data-difficulty="junior">

## 🟢 Junior: When to Use Step Functions vs Lambda Chaining

**Scenario:** Your pipeline has 4 steps: validate file → transform data → load to Redshift → send notification. A colleague chains them by having each Lambda directly invoke the next. What problems does this create and how do Step Functions fix them?

<details>
<summary>✅ Solution</summary>

**Problems with Lambda-to-Lambda chaining:**
1. **Tight coupling:** Each Lambda must know the next Lambda's ARN
2. **No visibility:** No central view of pipeline state or progress
3. **Error handling:** Each Lambda must implement its own retry logic
4. **Timeout risk:** If step 1 invokes step 2 synchronously, the timeout clock ticks for both
5. **Hard to modify:** Adding a step requires changing the previous Lambda's code

**Step Functions solution:**
```json
{
  "StartAt": "Validate",
  "States": {
    "Validate": {"Type": "Task", "Resource": "arn:lambda:validate", "Next": "Transform",
                 "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2}]},
    "Transform": {"Type": "Task", "Resource": "arn:lambda:transform", "Next": "Load"},
    "Load": {"Type": "Task", "Resource": "arn:lambda:load-redshift", "Next": "Notify"},
    "Notify": {"Type": "Task", "Resource": "arn:lambda:send-notification", "End": true}
  }
}
```

**Benefits:** Visual workflow, built-in retries, error handling, easy to add/remove steps, execution history, and each Lambda only does its own job.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design Error Handling for a Data Pipeline

**Scenario:** Design a Step Function for a pipeline where: Transform may fail (transient errors). If transform fails 3 times, route to a manual review queue. If load fails, roll back the transform output. Always send a notification regardless of success/failure.

<details>
<summary>✅ Solution</summary>

```json
{
  "StartAt": "Transform",
  "States": {
    "Transform": {
      "Type": "Task",
      "Resource": "arn:lambda:transform",
      "Retry": [{"ErrorEquals": ["TransientError"], "MaxAttempts": 3, "BackoffRate": 2}],
      "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "ManualReview"}],
      "Next": "Load"
    },
    "Load": {
      "Type": "Task",
      "Resource": "arn:lambda:load",
      "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Rollback"}],
      "Next": "NotifySuccess"
    },
    "Rollback": {
      "Type": "Task",
      "Resource": "arn:lambda:delete-transform-output",
      "Next": "NotifyFailure"
    },
    "ManualReview": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sqs:sendMessage",
      "Parameters": {"QueueUrl": "https://sqs.../review-queue", "MessageBody.$": "$.error"},
      "Next": "NotifyFailure"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:lambda:notify",
      "Parameters": {"status": "SUCCESS"},
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:lambda:notify",
      "Parameters": {"status": "FAILED"},
      "End": true
    }
  }
}
```

**Key patterns used:** Retry with backoff (transient errors), Catch (route to error handler), Rollback (compensating action), and guaranteed notification (both success and failure paths end with notify).

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is AWS Step Functions and why use it for data pipelines?**
A: Step Functions is a serverless workflow orchestration service that coordinates multiple AWS services into stateful workflows using visual state machines. For data pipelines, it provides: automatic retry with exponential backoff, error handling, parallel execution, conditional branching, and a full execution history — without writing orchestration code.

**Q: What is the difference between Standard and Express Workflows?**
A: Standard Workflows are for long-running (up to 1 year), exactly-once execution workflows with full execution history persisted for 90 days. Express Workflows are for high-volume, short-duration (up to 5 minutes) workloads — they run at-least-once with lower cost per execution and are suited for IoT data processing or real-time event handling.

**Q: What are the key state types in Amazon States Language (ASL)?**
A: Key states include: Task (invoke a service like Lambda, Glue, or ECS), Choice (conditional branching on state data), Parallel (run branches concurrently), Map (iterate over an array, running a sub-workflow per item), Wait (pause for a duration or until a timestamp), Pass (pass input to output, useful for transformation), and Succeed/Fail (terminal states).

**Q: How does Step Functions handle errors and retries?**
A: Each Task state can define a `Retry` block (with `MaxAttempts`, `IntervalSeconds`, `BackoffRate`, and `MaxDelaySeconds`) and a `Catch` block for fallback states on specific error types. This enables fine-grained retry logic per step — for example, retrying a Glue job on `ServiceUnavailable` but failing immediately on `InvalidInput`.

**Q: What is the Map state and when would you use it in a data pipeline?**
A: The Map state runs a sub-workflow for each item in an input array, with configurable concurrency (`MaxConcurrency`). Use it to process multiple S3 files in parallel, fan out to multiple Glue jobs per partition, or process multiple records concurrently — replacing custom parallelization code with a managed pattern.

**Q: How do Step Functions integrate with Glue, EMR, and Batch?**
A: Step Functions has optimized service integrations for Glue (start and wait for job completion), EMR (add steps and wait), Batch (submit jobs and wait), Lambda, ECS, and more. The `.sync` integration pattern submits the job and polls until completion, returning results to the workflow without Lambda polling code.

**Q: What is the Step Functions execution history and what are its limits?**
A: Each Standard Workflow execution retains a full event history (up to 25,000 events) for 90 days, viewable in the console or via API. For very large workflows with many steps, the 25,000-event limit can be hit — use Distributed Map or break workflows into child executions with `StartExecution`.

**Q: What is Step Functions Distributed Map?**
A: Distributed Map is a high-scale variant of the Map state that can process millions of S3 objects or CSV rows concurrently, spawning up to 10,000 concurrent child workflow executions. It's designed for large-scale parallel data processing directly on S3 without an intermediary queue.

---

## 💼 Interview Tips

- Position Step Functions as the orchestrator for complex multi-step pipelines that need visibility and error handling: Glue → EMR → Lambda → SNS with retry and error routing — rather than custom retry logic in application code.
- Senior interviewers expect you to compare Step Functions with Airflow: Step Functions is serverless with AWS-native integrations and per-execution pricing; Airflow is open-source with a rich ecosystem, Python-native DAGs, and is better for complex dependency graphs and data-aware scheduling.
- Mention the `.sync` service integration pattern explicitly — it's the key to orchestrating long-running Glue or EMR jobs without Lambda polling, and knowing it demonstrates real Step Functions experience.
- Demonstrate Distributed Map knowledge for large-scale data processing: processing 10 million S3 objects with 10,000 concurrent workers is a Step Functions Distributed Map use case that would be complex to implement otherwise.
- Know the Standard vs. Express Workflow decision: Standard for business-critical pipelines where exactly-once semantics and full audit history matter; Express for high-frequency, short-duration event processing.
- Avoid treating Step Functions as a message queue — it's a workflow engine, not a messaging system. For buffering and backpressure, combine Step Functions with SQS; for event routing, combine with EventBridge.
