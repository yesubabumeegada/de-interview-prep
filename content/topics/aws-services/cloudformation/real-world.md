---
title: "AWS CloudFormation - Real-World Production Examples"
topic: aws-services
subtopic: cloudformation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, cloudformation, production, infrastructure]
---

# AWS CloudFormation — Real-World Production Examples

## Pattern 1: Complete Data Pipeline Infrastructure Stack

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Data Pipeline - S3 + Glue + IAM + EventBridge

Parameters:
  Environment:
    Type: String
    AllowedValues: [dev, staging, prod]
  ProjectName:
    Type: String
    Default: data-platform

Resources:
  # --- S3 Buckets ---
  RawBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties:
      BucketName: !Sub "${ProjectName}-${Environment}-raw"
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: aws:kms
              KMSMasterKeyID: !Ref DataLakeKMSKey
      LifecycleConfiguration:
        Rules:
          - Id: TransitionToIA
            Status: Enabled
            Transitions:
              - StorageClass: STANDARD_IA
                TransitionInDays: 30
              - StorageClass: GLACIER
                TransitionInDays: 90
      NotificationConfiguration:
        EventBridgeConfiguration:
          EventBridgeEnabled: true

  CuratedBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties:
      BucketName: !Sub "${ProjectName}-${Environment}-curated"
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: aws:kms
              KMSMasterKeyID: !Ref DataLakeKMSKey

  # --- KMS Key ---
  DataLakeKMSKey:
    Type: AWS::KMS::Key
    Properties:
      Description: Data Lake encryption key
      KeyPolicy:
        Version: '2012-10-17'
        Statement:
          - Sid: AdminAccess
            Effect: Allow
            Principal:
              AWS: !Sub "arn:aws:iam::${AWS::AccountId}:root"
            Action: "kms:*"
            Resource: "*"
          - Sid: GlueAccess
            Effect: Allow
            Principal:
              AWS: !GetAtt GlueRole.Arn
            Action: [kms:Decrypt, kms:Encrypt, kms:GenerateDataKey]
            Resource: "*"

  # --- IAM Role for Glue ---
  GlueRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${ProjectName}-${Environment}-glue-role"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: DataLakeAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action: [s3:GetObject, s3:PutObject, s3:DeleteObject, s3:ListBucket]
                Resource:
                  - !GetAtt RawBucket.Arn
                  - !Sub "${RawBucket.Arn}/*"
                  - !GetAtt CuratedBucket.Arn
                  - !Sub "${CuratedBucket.Arn}/*"
              - Effect: Allow
                Action: [kms:Decrypt, kms:Encrypt, kms:GenerateDataKey]
                Resource: !GetAtt DataLakeKMSKey.Arn

  # --- Glue Database and Job ---
  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub "${ProjectName}_${Environment}_curated"

  DailyETLJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Sub "${ProjectName}-${Environment}-daily-etl"
      Role: !GetAtt GlueRole.Arn
      GlueVersion: "4.0"
      Command:
        Name: glueetl
        ScriptLocation: !Sub "s3://${ProjectName}-${Environment}-scripts/etl/daily_transform.py"
        PythonVersion: "3"
      DefaultArguments:
        "--source_path": !Sub "s3://${RawBucket}/incoming/"
        "--target_path": !Sub "s3://${CuratedBucket}/orders/"
        "--TempDir": !Sub "s3://${RawBucket}/temp/"
        "--enable-metrics": "true"
      MaxRetries: 1
      Timeout: 120
      NumberOfWorkers: 10
      WorkerType: G.1X

  # --- EventBridge Rule (trigger daily) ---
  DailyTrigger:
    Type: AWS::Events::Rule
    Properties:
      Name: !Sub "${ProjectName}-${Environment}-daily-trigger"
      ScheduleExpression: "cron(0 6 * * ? *)"
      State: ENABLED
      Targets:
        - Id: StartGlueJob
          Arn: !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:job/${DailyETLJob}"
          RoleArn: !GetAtt EventBridgeGlueRole.Arn

  EventBridgeGlueRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: events.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: StartGlueJob
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action: glue:notifyEvent
                Resource: !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:job/${DailyETLJob}"

Outputs:
  RawBucketName:
    Value: !Ref RawBucket
    Export:
      Name: !Sub "${AWS::StackName}-RawBucket"
  CuratedBucketName:
    Value: !Ref CuratedBucket
    Export:
      Name: !Sub "${AWS::StackName}-CuratedBucket"
  GlueRoleArn:
    Value: !GetAtt GlueRole.Arn
    Export:
      Name: !Sub "${AWS::StackName}-GlueRoleArn"
```

---

## Pattern 2: Multi-Environment Deployment

```yaml
# Single template deployed with different parameter files per environment

# parameters/dev.json
# [{"ParameterKey": "Environment", "ParameterValue": "dev"},
#  {"ParameterKey": "GlueWorkers", "ParameterValue": "2"},
#  {"ParameterKey": "EnableRedshift", "ParameterValue": "false"}]

# parameters/prod.json
# [{"ParameterKey": "Environment", "ParameterValue": "prod"},
#  {"ParameterKey": "GlueWorkers", "ParameterValue": "10"},
#  {"ParameterKey": "EnableRedshift", "ParameterValue": "true"}]

Parameters:
  GlueWorkers:
    Type: Number
    Default: 2
  EnableRedshift:
    Type: String
    AllowedValues: ["true", "false"]

Conditions:
  CreateRedshift: !Equals [!Ref EnableRedshift, "true"]

Resources:
  RedshiftCluster:
    Type: AWS::Redshift::Cluster
    Condition: CreateRedshift
    DeletionPolicy: Snapshot
    Properties:
      ClusterType: multi-node
      NumberOfNodes: !If [CreateRedshift, 4, !Ref "AWS::NoValue"]
      NodeType: dc2.large
      DBName: analytics
      MasterUsername: admin
      MasterUserPassword: !Sub "{{resolve:secretsmanager:${Environment}/redshift-password}}"
      Encrypted: true
      KmsKeyId: !Ref DataLakeKMSKey
      EnhancedVpcRouting: true
      VpcSecurityGroupIds:
        - !Ref RedshiftSecurityGroup
      ClusterSubnetGroupName: !Ref RedshiftSubnetGroup
```

```bash
# Deployment script
#!/bin/bash
ENV=$1  # dev, staging, or prod

aws cloudformation deploy \
  --stack-name "data-platform-${ENV}" \
  --template-file template.yaml \
  --parameter-overrides file://parameters/${ENV}.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Environment=${ENV} Team=data-engineering CostCenter=DE-001

# Production: use change set for safety
if [ "$ENV" = "prod" ]; then
  aws cloudformation create-change-set \
    --stack-name "data-platform-prod" \
    --template-body file://template.yaml \
    --parameter-overrides file://parameters/prod.json \
    --change-set-name "deploy-$(date +%Y%m%d%H%M)" \
    --capabilities CAPABILITY_NAMED_IAM
  echo "Change set created. Review and execute manually."
fi
```

---

## Pattern 3: StackSets for Org-Wide Data Lake Setup

```yaml
# Foundation template deployed to all accounts via StackSet
AWSTemplateFormatVersion: '2010-09-09'
Description: Organization-wide data lake foundation (deployed via StackSet)

Parameters:
  CentralCatalogAccountId:
    Type: String
    Default: "123456789"
  OrgId:
    Type: String

Resources:
  # Every account gets a raw landing bucket
  AccountRawBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties:
      BucketName: !Sub "datalake-${AWS::AccountId}-raw"
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: AES256

  # Every account gets standard IAM roles
  DataEngineerRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: DataEngineer
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              AWS: !Sub "arn:aws:iam::${CentralCatalogAccountId}:root"
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

  # Cross-account Glue Catalog access
  GlueCatalogPolicy:
    Type: AWS::Glue::ResourcePolicy
    Properties:
      PolicyInJson: !Sub |
        {
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::${CentralCatalogAccountId}:root"},
            "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
            "Resource": [
              "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog",
              "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/*",
              "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/*"
            ]
          }]
        }

  # CloudTrail for audit (data access logging)
  DataAccessTrail:
    Type: AWS::CloudTrail::Trail
    Properties:
      TrailName: data-access-audit
      IsLogging: true
      S3BucketName: !Sub "org-audit-logs-${CentralCatalogAccountId}"
      EventSelectors:
        - DataResources:
            - Type: AWS::S3::Object
              Values: [!Sub "arn:aws:s3:::datalake-${AWS::AccountId}-raw/"]
          ReadWriteType: All
```

---

## Template Library Organization

```
infrastructure/
├── templates/
│   ├── foundation/
│   │   ├── vpc.yaml                # Network foundation
│   │   ├── iam-roles.yaml          # Standard roles
│   │   └── kms-keys.yaml           # Encryption keys
│   ├── data-lake/
│   │   ├── s3-buckets.yaml         # Storage layer
│   │   ├── glue-catalog.yaml       # Catalog setup
│   │   └── lake-formation.yaml     # Governance
│   ├── compute/
│   │   ├── glue-jobs.yaml          # ETL jobs
│   │   ├── step-functions.yaml     # Orchestration
│   │   └── lambda-functions.yaml   # Event processing
│   ├── analytics/
│   │   ├── redshift.yaml           # Data warehouse
│   │   ├── athena-workgroups.yaml  # Query service
│   │   └── quicksight.yaml         # BI dashboards
│   └── monitoring/
│       ├── cloudwatch-alarms.yaml  # Alerting
│       └── dashboards.yaml         # Observability
├── parameters/
│   ├── dev.json
│   ├── staging.json
│   └── prod.json
├── stacksets/
│   └── org-foundation.yaml         # Deployed to all accounts
├── scripts/
│   ├── deploy.sh
│   └── validate.sh
└── tests/
    └── .taskcat.yml
```

---

## Interview Tips

> **Tip 1:** "Show me how you'd define a complete data pipeline as infrastructure" — "Single CloudFormation template with: S3 buckets (raw + curated, KMS encrypted, lifecycle policies), IAM role for Glue (least privilege, scoped to specific buckets), Glue database and job definitions, EventBridge rule for scheduling. DeletionPolicy: Retain on all S3 buckets. KMS key with key policy granting access to the Glue role. Outputs exported for downstream stacks (analytics layer imports bucket names)."

> **Tip 2:** "How do you manage multiple environments?" — "One template, different parameter files (dev.json, staging.json, prod.json). Conditions for environment-specific resources (Redshift only in prod). Mappings for sizing (2 DPU dev, 10 DPU prod). Deploy script uses change sets for production, direct deploy for dev. Stack naming convention: `project-environment-component`. Tags for cost allocation."

> **Tip 3:** "How do you standardize infrastructure across 50 AWS accounts?" — "StackSets with Organization targets. Foundation template (IAM roles, S3 buckets, CloudTrail, Glue policies) deployed to all accounts in the data OU. AutoDeployment ensures new accounts get the foundation automatically. Individual accounts then deploy their domain-specific stacks (Glue jobs, Step Functions) on top of the foundation. Central governance account can read all catalogs via the cross-account Glue resource policy."
