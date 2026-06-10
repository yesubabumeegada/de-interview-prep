---
title: "AWS CloudFormation - Senior Deep Dive"
topic: aws-services
subtopic: cloudformation
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, cloudformation, iac, cicd, best-practices]
---

# AWS CloudFormation — Senior-Level Deep Dive

## Custom Resources (Lambda-Backed)

When CloudFormation doesn't natively support a resource, use custom resources with Lambda:

```yaml
Resources:
  # Custom resource: run Athena query to create Iceberg table
  CreateIcebergTable:
    Type: Custom::AthenaQuery
    Properties:
      ServiceToken: !GetAtt CustomResourceFunction.Arn
      Query: |
        CREATE TABLE IF NOT EXISTS curated.orders (
          order_id STRING, customer_id STRING, amount DOUBLE, order_date DATE
        ) PARTITIONED BY (order_date)
        LOCATION 's3://data-lake/iceberg/orders/'
        TBLPROPERTIES ('table_type' = 'ICEBERG')
      WorkGroup: etl-workgroup

  CustomResourceFunction:
    Type: AWS::Lambda::Function
    Properties:
      Runtime: python3.11
      Handler: index.handler
      Code:
        ZipFile: |
          import boto3
          import cfnresponse
          
          def handler(event, context):
              athena = boto3.client('athena')
              try:
                  if event['RequestType'] in ['Create', 'Update']:
                      response = athena.start_query_execution(
                          QueryString=event['ResourceProperties']['Query'],
                          WorkGroup=event['ResourceProperties']['WorkGroup']
                      )
                      cfnresponse.send(event, context, cfnresponse.SUCCESS,
                          {'QueryExecutionId': response['QueryExecutionId']})
                  elif event['RequestType'] == 'Delete':
                      # Optionally drop table on stack deletion
                      cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
              except Exception as e:
                  cfnresponse.send(event, context, cfnresponse.FAILED, {'Error': str(e)})
      Role: !GetAtt CustomResourceRole.Arn
      Timeout: 300
```

**Common custom resource use cases for data platforms:**
- Create Athena/Iceberg tables during deployment
- Register Glue Crawlers and run initial crawl
- Configure Lake Formation permissions
- Set up Redshift schemas and users
- Seed reference data into DynamoDB

---

## StackSets — Multi-Account/Region Deployment

```python
import boto3

cfn = boto3.client('cloudformation')

# Deploy data lake foundation to all org accounts
cfn.create_stack_set(
    StackSetName='data-lake-foundation',
    TemplateBody=open('foundation.yaml').read(),
    Parameters=[
        {'ParameterKey': 'CentralAccountId', 'ParameterValue': '123456789'},
        {'ParameterKey': 'LogBucketName', 'ParameterValue': 'org-cloudtrail-logs'}
    ],
    PermissionModel='SERVICE_MANAGED',
    AutoDeployment={'Enabled': True, 'RetainStacksOnAccountRemoval': True},
    Capabilities=['CAPABILITY_NAMED_IAM']
)

# Deploy to all accounts in the organization
cfn.create_stack_instances(
    StackSetName='data-lake-foundation',
    DeploymentTargets={
        'OrganizationalUnitIds': ['ou-data-platform', 'ou-analytics']
    },
    Regions=['us-east-1', 'eu-west-1'],
    OperationPreferences={
        'MaxConcurrentPercentage': 25,  # Deploy 25% at a time
        'FailureTolerancePercentage': 10
    }
)
```

**StackSets for data platforms:**
- Deploy S3 bucket policies + Lake Formation config to all domain accounts
- Set up CloudTrail + Config in every account (compliance)
- Create standard IAM roles (DataEngineer, DataAnalyst) across org
- Deploy VPC endpoints and networking in each account

---

## CI/CD Integration (CodePipeline + CloudFormation)

```yaml
# CodePipeline stages for infrastructure deployment
Resources:
  InfrastructurePipeline:
    Type: AWS::CodePipeline::Pipeline
    Properties:
      Stages:
        - Name: Source
          Actions:
            - Name: GitSource
              ActionTypeId:
                Category: Source
                Provider: CodeStarSourceConnection
              Configuration:
                ConnectionArn: !Ref GitConnection
                FullRepositoryId: "company/data-infra"
                BranchName: main
              OutputArtifacts: [Name: SourceOutput]

        - Name: Test
          Actions:
            - Name: ValidateTemplate
              ActionTypeId:
                Category: Test
                Provider: CodeBuild
              Configuration:
                ProjectName: !Ref TemplateLintProject
              InputArtifacts: [Name: SourceOutput]

        - Name: DeployStaging
          Actions:
            - Name: CreateChangeSet
              ActionTypeId:
                Category: Deploy
                Provider: CloudFormation
              Configuration:
                ActionMode: CHANGE_SET_REPLACE
                StackName: data-platform-staging
                TemplatePath: "SourceOutput::template.yaml"
                ChangeSetName: staging-update
                Capabilities: CAPABILITY_NAMED_IAM
            - Name: ApproveChangeSet
              ActionTypeId:
                Category: Approval
                Provider: Manual
            - Name: ExecuteChangeSet
              ActionTypeId:
                Category: Deploy
                Provider: CloudFormation
              Configuration:
                ActionMode: CHANGE_SET_EXECUTE
                StackName: data-platform-staging
                ChangeSetName: staging-update

        - Name: DeployProduction
          Actions:
            - Name: ManualApproval
              ActionTypeId:
                Category: Approval
                Provider: Manual
            - Name: DeployProd
              ActionTypeId:
                Category: Deploy
                Provider: CloudFormation
              Configuration:
                ActionMode: CREATE_UPDATE
                StackName: data-platform-prod
                TemplatePath: "SourceOutput::template.yaml"
```

---

## Infrastructure Testing

```python
# cfn-lint: static template validation
# taskcat: deployment testing across regions

# cfn-lint rules for data platform templates:
# .cfnlintrc
"""
templates:
  - templates/**/*.yaml
rules:
  ignore_checks:
    - W3010  # Hardcoded AZ (acceptable for some cases)
  custom_rules:
    - rules/require_encryption.py  # All S3 must have encryption
    - rules/require_tags.py        # All resources must be tagged
"""

# taskcat config for multi-region testing
# .taskcat.yml
"""
project:
  name: data-platform-infra
  regions:
    - us-east-1
    - eu-west-1
tests:
  data-lake-stack:
    template: templates/data-lake.yaml
    parameters:
      Environment: test
      BucketPrefix: taskcat-test
  etl-stack:
    template: templates/etl.yaml
    parameters:
      Environment: test
"""
```

---

## Resource Import

```bash
# Import existing resources into CloudFormation management
# Use case: team created S3 bucket manually, now want to manage via IaC

# Step 1: Add resource to template with DeletionPolicy: Retain
# Step 2: Create change set with import
aws cloudformation create-change-set \
  --stack-name data-lake-prod \
  --change-set-name import-existing-bucket \
  --change-set-type IMPORT \
  --resources-to-import "[{\"ResourceType\":\"AWS::S3::Bucket\",\"LogicalResourceId\":\"ExistingDataBucket\",\"ResourceIdentifier\":{\"BucketName\":\"my-existing-data-lake\"}}]" \
  --template-body file://template-with-bucket.yaml

# Step 3: Execute import
aws cloudformation execute-change-set \
  --stack-name data-lake-prod \
  --change-set-name import-existing-bucket
```

---

## Stack Update Strategies

| Strategy | How It Works | Risk | Use Case |
|----------|-------------|------|----------|
| In-place update | Modify existing resources | Medium (downtime possible) | Config changes |
| Rolling | Update instances one by one | Low | ASG-based workloads |
| Blue-green | Create new, switch traffic | Lowest | Stateless services |
| Canary | Update small percentage first | Low | High-traffic services |

```yaml
# UpdatePolicy for rolling updates (EMR/ASG)
AutoScalingGroup:
  Type: AWS::AutoScaling::AutoScalingGroup
  UpdatePolicy:
    AutoScalingRollingUpdate:
      MinInstancesInService: 2
      MaxBatchSize: 1
      PauseTime: PT5M
      WaitOnResourceSignals: true

# DeletionPolicy for data resources (NEVER lose data)
DataLakeBucket:
  Type: AWS::S3::Bucket
  DeletionPolicy: Retain  # Keep bucket even if stack is deleted
  UpdateReplacePolicy: Retain
```

---

## CDK Comparison

```python
# AWS CDK: write infrastructure in Python (generates CloudFormation)
from aws_cdk import (
    Stack, aws_s3 as s3, aws_glue as glue, aws_iam as iam
)

class DataLakeStack(Stack):
    def __init__(self, scope, id, environment, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        # S3 bucket with lifecycle
        bucket = s3.Bucket(self, "DataLake",
            bucket_name=f"{environment}-data-lake",
            encryption=s3.BucketEncryption.KMS_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(transitions=[
                    s3.Transition(storage_class=s3.StorageClass.INTELLIGENT_TIERING, transition_after=Duration.days(30))
                ])
            ],
            removal_policy=RemovalPolicy.RETAIN
        )
        
        # Glue job with role
        role = iam.Role(self, "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")]
        )
        bucket.grant_read_write(role)
        
        glue.CfnJob(self, "ETLJob",
            name=f"{environment}-daily-etl",
            role=role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{environment}-scripts/etl/transform.py"
            ),
            glue_version="4.0",
            max_capacity=10
        )
```

**CDK vs raw CloudFormation:**
- CDK: type-safe, IDE autocompletion, loops/conditions native, abstractions (L2 constructs)
- Raw CloudFormation: no build step, more portable, AWS documentation maps directly
- CDK generates CloudFormation under the hood (same deployment mechanism)

---

## Interview Tips

> **Tip 1:** "How do you deploy infrastructure changes safely to production?" — "Four-step process: (1) cfn-lint validates template syntax and custom rules (encryption required, tags required). (2) Deploy to staging via change set (review what will change). (3) Run integration tests against staging. (4) Manual approval gate → deploy to production with automatic rollback on failure. StackPolicies prevent accidental deletion of databases and S3 buckets. DeletionPolicy: Retain on all data resources."

> **Tip 2:** "How do you manage infrastructure across 20+ AWS accounts?" — "StackSets with Organization deployment targets. Define templates once (data lake foundation, IAM roles, VPC endpoints), deploy to all accounts in an OU automatically. New accounts get the standard infrastructure on creation (AutoDeployment: Enabled). MaxConcurrentPercentage controls blast radius. FailureTolerancePercentage allows partial failures without rolling back everything."

> **Tip 3:** "CloudFormation custom resources — when and how?" — "When CloudFormation doesn't support what you need: create Athena tables, run Glue crawlers, configure Lake Formation permissions, seed DynamoDB data. Lambda-backed with cfnresponse library. Critical: handle Create, Update, AND Delete events. Always return SUCCESS or FAILED (stack hangs if Lambda doesn't respond). Set a timeout to prevent indefinite stack operations. Use for one-time setup that should be part of the infrastructure lifecycle."

## ⚡ Cheat Sheet

**Custom Resources — Must-Know Rules**
- Lambda must respond with `cfnresponse.SUCCESS` or `cfnresponse.FAILED` — stack hangs indefinitely if no response
- Handle all three request types: `Create`, `Update`, `Delete`
- Set Lambda timeout generously (300 s); stack waits up to 3× timeout before failing
- Common uses: create Athena/Iceberg tables, run Glue crawlers, configure Lake Formation, seed DynamoDB

**Stack Update Safety Checklist**
- `DeletionPolicy: Retain` on ALL data resources (S3 buckets, RDS, DynamoDB)
- `UpdateReplacePolicy: Retain` prevents data loss when resource must be replaced
- Use Change Sets — always review before executing (especially in production)
- StackPolicy to prevent updates/deletes on stateful resources

**StackSets Key Config**
- `SERVICE_MANAGED` + `AutoDeployment.Enabled: true` — new accounts get infra automatically
- `MaxConcurrentPercentage: 25` — deploy 25% of accounts at a time (limits blast radius)
- `FailureTolerancePercentage: 10` — allow up to 10% failure without global rollback

**CI/CD Deploy Pattern**
1. cfn-lint (static validation) → 2. Change set to staging → 3. Manual approval → 4. Execute change set → 5. Integration tests → 6. Manual approval → 7. Deploy to prod

**CDK vs Raw CloudFormation**
- CDK: Python/TS loops and conditions native, IDE autocomplete, L2 constructs with sensible defaults
- Raw CFN: no build step, portable, maps directly to AWS documentation
- CDK synthesizes to CloudFormation — same rollback, drift detection, change set mechanics

**Resource Import**
- `--change-set-type IMPORT` to bring manually-created resources under CFN management
- Requires `DeletionPolicy: Retain` on the resource before importing
- Import uses resource identifier (e.g., `BucketName`) not the ARN
