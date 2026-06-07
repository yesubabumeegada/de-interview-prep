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
