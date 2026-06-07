---
title: "AWS CloudFormation - Intermediate"
topic: aws-services
subtopic: cloudformation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, cloudformation, iac, templates, stacks]
---

# AWS CloudFormation — Intermediate Concepts

## Intrinsic Functions

```yaml
Resources:
  DataLakeBucket:
    Type: AWS::S3::Bucket
    Properties:
      # Ref: reference another resource or parameter
      BucketName: !Ref BucketNameParameter
      
      # Sub: string interpolation
      Tags:
        - Key: Environment
          Value: !Sub "${Environment}-data-lake"
        - Key: Stack
          Value: !Sub "arn:aws:s3:::${AWS::StackName}-${AWS::Region}"
      
  GlueJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Sub "${Environment}-daily-etl"
      Role: !GetAtt GlueRole.Arn          # GetAtt: get attribute of a resource
      Command:
        ScriptLocation: !Sub "s3://${ScriptsBucket}/etl/transform.py"
      DefaultArguments:
        "--output_path": !Sub "s3://${DataLakeBucket}/curated/"
        
  # Select: pick from a list by index
  SubnetSelection:
    Type: AWS::EC2::Subnet
    Properties:
      AvailabilityZone: !Select [0, !GetAZs ""]  # First AZ in region
      
  # Split + Select: parse a string
  AccountFromArn:
    Value: !Select [4, !Split [":", !Ref SomeArn]]  # Extract account ID from ARN
```

---

## Conditions

```yaml
Parameters:
  Environment:
    Type: String
    AllowedValues: [dev, staging, prod]

Conditions:
  IsProd: !Equals [!Ref Environment, "prod"]
  IsNotDev: !Not [!Equals [!Ref Environment, "dev"]]
  NeedsEncryption: !Or
    - !Equals [!Ref Environment, "prod"]
    - !Equals [!Ref Environment, "staging"]

Resources:
  RedshiftCluster:
    Type: AWS::Redshift::Cluster
    Condition: IsProd  # Only create in production
    Properties:
      ClusterType: multi-node
      NumberOfNodes: 4

  DataLakeBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - !If
            - NeedsEncryption
            - ServerSideEncryptionByDefault:
                SSEAlgorithm: aws:kms
                KMSMasterKeyID: !Ref DataLakeKMSKey
            - ServerSideEncryptionByDefault:
                SSEAlgorithm: AES256
```

---

## Mappings

```yaml
Mappings:
  EnvironmentConfig:
    dev:
      GlueDPUs: 2
      RedshiftNodes: 1
      S3LifecycleDays: 30
    staging:
      GlueDPUs: 5
      RedshiftNodes: 2
      S3LifecycleDays: 90
    prod:
      GlueDPUs: 10
      RedshiftNodes: 4
      S3LifecycleDays: 365

Resources:
  GlueJob:
    Type: AWS::Glue::Job
    Properties:
      MaxCapacity: !FindInMap [EnvironmentConfig, !Ref Environment, GlueDPUs]
```

---

## Nested Stacks

```yaml
# Parent stack: orchestrates child stacks
Resources:
  NetworkStack:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: https://s3.amazonaws.com/templates/network.yaml
      Parameters:
        VpcCidr: "10.0.0.0/16"
        Environment: !Ref Environment

  DataLakeStack:
    Type: AWS::CloudFormation::Stack
    DependsOn: NetworkStack
    Properties:
      TemplateURL: https://s3.amazonaws.com/templates/data-lake.yaml
      Parameters:
        VpcId: !GetAtt NetworkStack.Outputs.VpcId
        PrivateSubnets: !GetAtt NetworkStack.Outputs.PrivateSubnetIds
        Environment: !Ref Environment

  ETLStack:
    Type: AWS::CloudFormation::Stack
    DependsOn: DataLakeStack
    Properties:
      TemplateURL: https://s3.amazonaws.com/templates/etl.yaml
      Parameters:
        DataLakeBucket: !GetAtt DataLakeStack.Outputs.BucketName
        GlueRoleArn: !GetAtt DataLakeStack.Outputs.GlueRoleArn
```

---

## Cross-Stack References (Exports/Imports)

```yaml
# Stack A: exports VPC resources
Outputs:
  VpcId:
    Value: !Ref DataPlatformVPC
    Export:
      Name: !Sub "${AWS::StackName}-VpcId"
  PrivateSubnet1:
    Value: !Ref PrivateSubnet1
    Export:
      Name: !Sub "${AWS::StackName}-PrivateSubnet1"

# Stack B: imports from Stack A
Resources:
  GlueConnection:
    Type: AWS::Glue::Connection
    Properties:
      ConnectionInput:
        PhysicalConnectionRequirements:
          SubnetId: !ImportValue "network-stack-PrivateSubnet1"
          SecurityGroupIdList:
            - !Ref GlueSecurityGroup
```

**Nested stacks vs cross-stack references:**

| Feature | Nested Stacks | Cross-Stack Exports |
|---------|--------------|-------------------|
| Coupling | Tight (parent manages) | Loose (independent stacks) |
| Deployment | Deploy together | Deploy independently |
| Deletion | Must delete parent | Cannot delete if imported |
| Best for | Related resources | Shared infrastructure |

---

## Change Sets and Drift Detection

```bash
# Create change set (preview changes before applying)
aws cloudformation create-change-set \
  --stack-name data-lake-prod \
  --template-body file://template.yaml \
  --change-set-name update-glue-config \
  --parameters ParameterKey=GlueDPUs,ParameterValue=15

# Review changes
aws cloudformation describe-change-set \
  --stack-name data-lake-prod \
  --change-set-name update-glue-config

# Execute only after review
aws cloudformation execute-change-set \
  --stack-name data-lake-prod \
  --change-set-name update-glue-config

# Detect drift (manual changes outside CloudFormation)
aws cloudformation detect-stack-drift --stack-name data-lake-prod
aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id abc123
```

---

## CloudFormation vs Terraform Comparison

| Feature | CloudFormation | Terraform |
|---------|---------------|-----------|
| Provider | AWS only | Multi-cloud (AWS, GCP, Azure) |
| Language | YAML/JSON | HCL |
| State | Managed by AWS | Local or remote (S3 + DynamoDB) |
| Drift detection | Built-in | `terraform plan` |
| Rollback | Automatic on failure | Manual (no auto-rollback) |
| Module ecosystem | Limited (Modules) | Large (Terraform Registry) |
| Preview changes | Change Sets | `terraform plan` (better) |
| Import resources | Yes (resource import) | Yes (`terraform import`) |
| Speed | Slower (API polling) | Faster (parallel by default) |
| Cost | Free | Free (OSS) or paid (Cloud) |

**When to choose CloudFormation:**
- AWS-only shop, want native integration
- Need automatic rollback on failures
- Using CDK (generates CloudFormation)
- Compliance requires AWS-managed state

---

## Interview Tips

> **Tip 1:** "How do you organize CloudFormation for a large data platform?" — "Nested stacks for lifecycle grouping: network stack, data-lake stack (S3 + IAM), ETL stack (Glue + Step Functions), analytics stack (Redshift + Athena). Cross-stack exports for shared resources (VPC, KMS keys). Parameters with mappings for multi-environment config (dev/staging/prod). Change sets in production for safe deployments."

> **Tip 2:** "CloudFormation vs Terraform?" — "CloudFormation: native AWS integration, automatic rollback, no state management overhead, free. Terraform: multi-cloud, better plan output, faster execution, larger module ecosystem. For AWS-only data platforms, CloudFormation with CDK is excellent. For multi-cloud or teams already using Terraform, stick with it. Key: pick one and standardize — mixing causes confusion."

> **Tip 3:** "How do you handle CloudFormation failures in production?" — "Change sets to preview every change before execution. Stack policies to prevent accidental deletion of critical resources (S3 buckets, databases). Automatic rollback is the default — if any resource fails to update, the entire stack reverts. For nested stacks, failures propagate up to the parent. DeletionPolicy: Retain on data resources ensures accidental stack deletion doesn't destroy data."
