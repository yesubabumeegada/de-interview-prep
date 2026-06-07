---
title: "AWS Step Functions - Real-World Production Examples"
topic: aws-services
subtopic: step-functions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, step-functions, production, etl-orchestration]
---

# AWS Step Functions — Real-World Production Examples

## Pattern 1: Multi-Step ETL with Validation Gate

```json
{
  "StartAt": "ExtractFromSource",
  "States": {
    "ExtractFromSource": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "extract-from-rds",
        "Arguments": {"--target_path": "s3://lake/raw/orders/"}
      },
      "Retry": [{"ErrorEquals": ["States.TaskFailed"], "MaxAttempts": 2, "IntervalSeconds": 120}],
      "Next": "RunDataQuality"
    },
    "RunDataQuality": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123456789:function:data-quality-check",
      "Parameters": {
        "table": "raw.orders",
        "checks": ["row_count > 1000", "null_rate(order_id) == 0", "freshness < 24h"]
      },
      "ResultPath": "$.qualityResult",
      "Next": "QualityGate"
    },
    "QualityGate": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.qualityResult.passed", "BooleanEquals": true, "Next": "Transform"}
      ],
      "Default": "NotifyQualityFailure"
    },
    "Transform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "transform-orders-to-curated"},
      "Next": "UpdateCatalog"
    },
    "UpdateCatalog": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {"Name": "curated-orders-crawler"},
      "Next": "PipelineSuccess"
    },
    "NotifyQualityFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-alerts",
        "Message.$": "States.Format('Quality check failed: {}', $.qualityResult.details)"
      },
      "Next": "PipelineFailed"
    },
    "PipelineSuccess": {"Type": "Succeed"},
    "PipelineFailed": {"Type": "Fail", "Error": "QualityCheckFailed"}
  }
}
```

---

## Pattern 2: Fan-Out Processing with Distributed Map

```json
{
  "StartAt": "ListPartitions",
  "States": {
    "ListPartitions": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123456789:function:list-unprocessed",
      "ResultPath": "$.partitions",
      "Next": "ProcessAllPartitions"
    },
    "ProcessAllPartitions": {
      "Type": "Map",
      "ItemProcessor": {
        "ProcessorConfig": {
          "Mode": "DISTRIBUTED",
          "ExecutionType": "STANDARD"
        },
        "StartAt": "TransformPartition",
        "States": {
          "TransformPartition": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "partition-transform",
              "Arguments": {
                "--partition.$": "$.partition_key"
              }
            },
            "End": true
          }
        }
      },
      "ItemsPath": "$.partitions",
      "MaxConcurrency": 50,
      "ToleratedFailurePercentage": 10,
      "ResultWriter": {
        "Resource": "arn:aws:states:::s3:putObject",
        "Parameters": {
          "Bucket": "pipeline-results",
          "Prefix": "batch-processing/"
        }
      },
      "Next": "AggregateResults"
    },
    "AggregateResults": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "INSERT INTO curated.daily_summary SELECT * FROM staging.processed",
        "WorkGroup": "etl-workgroup"
      },
      "End": true
    }
  }
}
```

**Production notes:**
- `ToleratedFailurePercentage: 10` means the pipeline succeeds even if 10% of partitions fail
- Failed partitions are logged to S3 for manual reprocessing
- `MaxConcurrency: 50` prevents overwhelming downstream systems (Glue DPU limits)

---

## Pattern 3: Human Approval Workflow for Data Access

```json
{
  "StartAt": "SubmitRequest",
  "States": {
    "SubmitRequest": {
      "Type": "Task",
      "Resource": "arn:aws:states:::dynamodb:putItem",
      "Parameters": {
        "TableName": "access-requests",
        "Item": {
          "requestId": {"S.$": "$$.Execution.Id"},
          "requestor": {"S.$": "$.requestor"},
          "database": {"S.$": "$.database"},
          "status": {"S": "PENDING"}
        }
      },
      "Next": "WaitForApproval"
    },
    "WaitForApproval": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
      "Parameters": {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/approval-queue",
        "MessageBody": {
          "taskToken.$": "$$.Task.Token",
          "requestor.$": "$.requestor",
          "database.$": "$.database",
          "justification.$": "$.justification"
        }
      },
      "TimeoutSeconds": 259200,
      "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "RequestExpired"}],
      "Next": "CheckDecision"
    },
    "CheckDecision": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.decision", "StringEquals": "APPROVED", "Next": "GrantAccess"}
      ],
      "Default": "DenyAccess"
    },
    "GrantAccess": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123456789:function:grant-lf-permissions",
      "Parameters": {
        "principal.$": "$.requestor",
        "database.$": "$.database",
        "permissions": ["SELECT"]
      },
      "Next": "NotifyApproved"
    },
    "NotifyApproved": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123456789:access-notifications",
        "Message.$": "States.Format('Access granted to {} for {}', $.requestor, $.database)"
      },
      "End": true
    },
    "DenyAccess": {"Type": "Succeed"},
    "RequestExpired": {"Type": "Fail", "Error": "RequestExpired", "Cause": "72h timeout"}
  }
}
```

---

## Pattern 4: Error Handling with Fallback and Notification

```json
{
  "StartAt": "PrimaryETL",
  "States": {
    "PrimaryETL": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "primary-transform"},
      "Retry": [
        {"ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 300, "MaxAttempts": 3},
        {"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 60, "MaxAttempts": 2, "BackoffRate": 2}
      ],
      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "FallbackETL"}],
      "Next": "PostProcessing"
    },
    "FallbackETL": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "RunFallback",
          "States": {
            "RunFallback": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {"JobName": "fallback-simple-transform"},
              "End": true
            }
          }
        },
        {
          "StartAt": "AlertOnCall",
          "States": {
            "AlertOnCall": {
              "Type": "Task",
              "Resource": "arn:aws:states:::sns:publish",
              "Parameters": {
                "TopicArn": "arn:aws:sns:us-east-1:123456789:oncall-alerts",
                "Message.$": "States.Format('Primary ETL failed. Error: {}. Running fallback.', $.error.Cause)"
              },
              "End": true
            }
          }
        }
      ],
      "Next": "PostProcessing"
    },
    "PostProcessing": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "MSCK REPAIR TABLE curated.daily_output",
        "WorkGroup": "etl-workgroup"
      },
      "End": true
    }
  }
}
```

---

## Production Monitoring and Operations

```python
import boto3
from datetime import datetime, timedelta

# Monitor execution metrics
cloudwatch = boto3.client('cloudwatch')

# Set up alarm for failed executions
cloudwatch.put_metric_alarm(
    AlarmName='StepFunctions-ETL-Failures',
    Namespace='AWS/States',
    MetricName='ExecutionsFailed',
    Dimensions=[{'Name': 'StateMachineArn', 'Value': 'arn:aws:states:us-east-1:123456789:stateMachine:etl-pipeline'}],
    Statistic='Sum',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:oncall-alerts']
)

# Track execution duration trends
cloudwatch.put_metric_alarm(
    AlarmName='StepFunctions-ETL-Duration',
    Namespace='AWS/States',
    MetricName='ExecutionTime',
    Dimensions=[{'Name': 'StateMachineArn', 'Value': 'arn:aws:states:us-east-1:123456789:stateMachine:etl-pipeline'}],
    Statistic='Average',
    Period=3600,
    EvaluationPeriods=1,
    Threshold=3600000,
    ComparisonOperator='GreaterThanThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:pipeline-alerts']
)
```

---

## Interview Tips

> **Tip 1:** "Walk me through a production ETL pipeline you orchestrated" — "Step Functions orchestrates: Extract (Glue from RDS), Validate (Lambda quality checks with Choice gate), Transform (Glue), Catalog Update (Crawler). Each Task state has Retry with exponential backoff for transient failures and Catch routing to SNS notification. Quality gate prevents bad data propagation. Distributed Map handles backfill of multiple partitions in parallel."

> **Tip 2:** "How do you handle partial failures in batch processing?" — "Use Distributed Map with ToleratedFailurePercentage. If processing 1000 partitions, setting 10% tolerance means the pipeline succeeds even if 100 partitions fail. Failed items are written to S3 for investigation and reprocessing. Combined with per-item Retry in the child workflow, most transient failures self-heal."

> **Tip 3:** "How do you implement approval workflows for data governance?" — "Use the waitForTaskToken callback pattern. The workflow pauses at the approval state, sends the token to an SQS queue (which triggers a Slack notification or UI). When an approver clicks approve, a Lambda calls SendTaskSuccess with the token. TimeoutSeconds ensures requests don't hang forever. The workflow then grants Lake Formation permissions automatically."
