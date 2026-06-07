---
title: "AWS CloudFormation - Scenario Questions"
topic: aws-services
subtopic: cloudformation
content_type: scenario_question
tags: [aws, cloudformation, interview, scenarios, iac]
---

# Scenario Questions — AWS CloudFormation

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Template for a Data Pipeline Stack

**Scenario:** Create a CloudFormation template that deploys: an S3 bucket for data, a Glue database, a Glue ETL job, the IAM role for the job, and a scheduled trigger. This should be reusable across environments (dev/prod).

<details>
<summary>✅ Solution</summary>

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Data Pipeline Infrastructure

Parameters:
  Environment:
    Type: String
    AllowedValues: [dev, staging, prod]
  BucketName:
    Type: String

Resources:
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${BucketName}-${Environment}"
      VersioningConfiguration:
        Status: Enabled
      LifecycleConfiguration:
        Rules:
          - Id: TransitionToIA
            Status: Enabled
            Transitions:
              - StorageClass: STANDARD_IA
                TransitionInDays: 90

  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub "pipeline_${Environment}"

  GlueRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "GlueETLRole-${Environment}"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {Service: glue.amazonaws.com}
            Action: sts:AssumeRole
      Policies:
        - PolicyName: GlueS3Access
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action: [s3:GetObject, s3:PutObject, s3:ListBucket]
                Resource:
                  - !GetAtt DataBucket.Arn
                  - !Sub "${DataBucket.Arn}/*"
              - Effect: Allow
                Action: [glue:*, logs:*]
                Resource: "*"

  ETLJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Sub "daily-etl-${Environment}"
      Role: !GetAtt GlueRole.Arn
      GlueVersion: "4.0"
      Command:
        Name: glueetl
        ScriptLocation: !Sub "s3://${BucketName}-${Environment}/scripts/etl.py"
        PythonVersion: "3"
      DefaultArguments:
        "--TempDir": !Sub "s3://${BucketName}-${Environment}/temp/"
        "--job-bookmark-option": "job-bookmark-enable"
      NumberOfWorkers: 5
      WorkerType: G.1X

  DailyTrigger:
    Type: AWS::Glue::Trigger
    Properties:
      Name: !Sub "daily-trigger-${Environment}"
      Type: SCHEDULED
      Schedule: "cron(0 6 * * ? *)"
      Actions:
        - JobName: !Ref ETLJob

Outputs:
  BucketArn:
    Value: !GetAtt DataBucket.Arn
  JobName:
    Value: !Ref ETLJob
```

**Deploy for each environment:**
```bash
aws cloudformation deploy --stack-name pipeline-dev --template-file template.yaml \
    --parameter-overrides Environment=dev BucketName=mycompany-data

aws cloudformation deploy --stack-name pipeline-prod --template-file template.yaml \
    --parameter-overrides Environment=prod BucketName=mycompany-data
```

**Benefits of IaC:** Reproducible, version-controlled, reviewable (PR process), consistent across environments, easy teardown.

</details>

</article>
