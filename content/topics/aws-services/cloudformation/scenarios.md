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

---

## ⚡ Quick-fire Q&A

**Q: What is AWS CloudFormation and why use it for data engineering?**
A: CloudFormation is AWS's infrastructure-as-code service that lets you define and provision AWS resources via YAML or JSON templates. For data engineering, it ensures reproducible pipeline infrastructure — Glue jobs, S3 buckets, Redshift clusters, and IAM roles — can be version-controlled and deployed consistently across environments.

**Q: What is the difference between a Stack and a StackSet?**
A: A Stack deploys resources in a single AWS account and region. A StackSet extends this to deploy the same template across multiple accounts and regions simultaneously, which is essential for enterprise data platforms operating across organizational boundaries.

**Q: What are CloudFormation Change Sets?**
A: Change Sets preview the changes CloudFormation will make before executing them. They show which resources will be added, modified, or replaced, allowing you to catch destructive changes (like a resource replacement that deletes data) before they happen in production.

**Q: What happens when a CloudFormation stack update fails?**
A: CloudFormation automatically rolls back to the previous known-good state by default. You can disable rollback during development for debugging, but in production automatic rollback protects against partial infrastructure failures.

**Q: What is the difference between Parameters and Mappings in CloudFormation?**
A: Parameters accept user input at deploy time (e.g., environment name, instance type), while Mappings are static lookup tables embedded in the template (e.g., AMI IDs per region). Mappings cannot be changed at deploy time without modifying the template.

**Q: How do you avoid hardcoding sensitive values like database passwords in CloudFormation?**
A: Use CloudFormation's SSM Parameter Store or Secrets Manager dynamic references (`{{resolve:secretsmanager:MySecret:SecretString:password}}`). This keeps secrets out of templates and version control while still allowing CloudFormation to retrieve them at deploy time.

**Q: What are Nested Stacks and when should you use them?**
A: Nested Stacks let you compose a parent stack from reusable child stack templates. Use them when your infrastructure is too large for a single template (CloudFormation has a 500-resource limit per stack) or when you want to share common infrastructure modules (VPC, IAM) across multiple pipelines.

**Q: How does CloudFormation differ from Terraform?**
A: CloudFormation is AWS-native with deep service integration and no state file to manage. Terraform is multi-cloud, uses HCL, and manages state in a backend (S3 + DynamoDB for locking). CloudFormation drift detection and StackSets have no direct Terraform equivalent; Terraform's plan/apply cycle and provider ecosystem are broader.

---

## 💼 Interview Tips

- Always frame CloudFormation answers around the engineering principle: infrastructure should be version-controlled, peer-reviewed, and deployed the same way in every environment. This resonates with senior interviewers who care about operational maturity.
- Mention Change Sets proactively when discussing production deployments — interviewers want to know you review changes before applying them, especially for stateful resources like RDS or Redshift.
- Avoid the mistake of treating CloudFormation as a one-way street — know about drift detection (`detect-stack-drift`) to find resources modified outside CloudFormation, and how to reconcile them.
- Senior interviewers expect you to discuss deletion policies: setting `DeletionPolicy: Retain` on S3 buckets and databases prevents accidental data loss when stacks are deleted.
- Demonstrate modular thinking: describe how you'd split a data platform into separate stacks (networking, storage, compute, IAM) with cross-stack references via `Outputs` and `ImportValue`.
- Know the limits that matter in practice: 500 resources per stack, 200 stacks per account per region, and 51,200 bytes template size (use S3 for larger templates).
