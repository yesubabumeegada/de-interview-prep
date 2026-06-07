---
title: "AWS IAM - Real-World Production Examples"
topic: aws-services
subtopic: iam
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, iam, production, security, data-platform]
---

# AWS IAM — Real-World Production Examples

## Pattern 1: Data Platform Role Hierarchy

```json
{
    "roles": {
        "DataPlatformAdmin": {
            "description": "Full control of data infrastructure",
            "permissions": "s3:*, glue:*, athena:*, redshift:*, kinesis:*, lakeformation:*, iam:PassRole",
            "assigned_to": "Platform team leads (2-3 people)",
            "boundary": "Cannot modify IAM policies or SCPs"
        },
        "DataEngineer": {
            "description": "Build and operate pipelines",
            "permissions": "s3:Get/Put/List, glue:*, athena:*, redshift:Read+Write, kinesis:Put/Get",
            "assigned_to": "Engineering team",
            "boundary": "Cannot delete production databases, no IAM modifications"
        },
        "DataAnalyst": {
            "description": "Query and analyze data",
            "permissions": "athena:StartQuery/GetResults, s3:GetObject (results only), glue:GetTable/GetDatabase",
            "assigned_to": "Analytics team",
            "boundary": "Read-only, cannot modify any resources"
        },
        "ServiceAccount_ETL": {
            "description": "Automated pipeline execution",
            "permissions": "s3:*, glue:StartJobRun/GetJobRun, cloudwatch:PutMetricData",
            "assigned_to": "Glue jobs, Lambda functions",
            "boundary": "Scoped to specific S3 prefixes and job names"
        }
    }
}
```

### DataEngineer Role Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3DataLakeAccess",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
            "Resource": [
                "arn:aws:s3:::data-lake-raw/*",
                "arn:aws:s3:::data-lake-curated/*",
                "arn:aws:s3:::data-lake-raw",
                "arn:aws:s3:::data-lake-curated"
            ]
        },
        {
            "Sid": "GlueFullAccess",
            "Effect": "Allow",
            "Action": "glue:*",
            "Resource": [
                "arn:aws:glue:us-east-1:123456789:catalog",
                "arn:aws:glue:us-east-1:123456789:database/*",
                "arn:aws:glue:us-east-1:123456789:table/*",
                "arn:aws:glue:us-east-1:123456789:job/*",
                "arn:aws:glue:us-east-1:123456789:crawler/*"
            ]
        },
        {
            "Sid": "AthenaQueryAccess",
            "Effect": "Allow",
            "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"],
            "Resource": "arn:aws:athena:us-east-1:123456789:workgroup/engineers"
        },
        {
            "Sid": "PassRoleToGlue",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::123456789:role/GlueServiceRole",
            "Condition": {
                "StringEquals": {"iam:PassedToService": "glue.amazonaws.com"}
            }
        }
    ]
}
```

---

## Pattern 2: Cross-Account Data Lake Access

```python
import boto3
import json

# Architecture: 3 accounts
# Account A (123456789): Data Lake (S3 + Glue Catalog)
# Account B (111111111): ETL/Processing
# Account C (222222222): Analytics/BI

# Account A: Trust policy for cross-account role
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::111111111:role/ETLServiceRole",
                    "arn:aws:iam::222222222:role/AnalyticsServiceRole"
                ]
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "data-lake-access-2024"
                }
            }
        }
    ]
}

# Account A: Permissions for the cross-account role
data_reader_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                "arn:aws:s3:::data-lake-curated",
                "arn:aws:s3:::data-lake-curated/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
            "Resource": "*"
        }
    ]
}

# Account B: ETL job assumes the cross-account role
def run_cross_account_etl():
    sts = boto3.client('sts')
    credentials = sts.assume_role(
        RoleArn='arn:aws:iam::123456789:role/CrossAccountDataReader',
        RoleSessionName='etl-daily-job',
        ExternalId='data-lake-access-2024'
    )['Credentials']
    
    # Use credentials to read from Account A
    s3 = boto3.client('s3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    
    response = s3.list_objects_v2(
        Bucket='data-lake-curated',
        Prefix='orders/year=2024/'
    )
    return response['Contents']
```

---

## Pattern 3: Temporary Credentials for ETL Jobs

```python
# Glue job IAM role with session-scoped permissions
# The Glue job runs with a role, but we further scope credentials per table

def get_scoped_credentials(table_name, operation='read'):
    """Get narrowly scoped credentials for a specific table operation"""
    sts = boto3.client('sts')
    
    if operation == 'read':
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::data-lake-curated/{table_name}/*"
            }]
        })
    elif operation == 'write':
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:DeleteObject"],
                "Resource": f"arn:aws:s3:::data-lake-curated/{table_name}/*"
            }]
        })
    
    # AssumeRole with session policy (further restricts the role's permissions)
    credentials = sts.assume_role(
        RoleArn='arn:aws:iam::123456789:role/GlueETLRole',
        RoleSessionName=f'etl-{table_name}-{operation}',
        Policy=policy,  # Session policy: narrows permissions for THIS session only
        DurationSeconds=3600
    )['Credentials']
    
    return boto3.client('s3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

# Usage: each stage of ETL gets only the permissions it needs
extract_client = get_scoped_credentials('raw_orders', 'read')
load_client = get_scoped_credentials('curated_orders', 'write')
```

---

## Pattern 4: Automated Least-Privilege Audit

```python
import boto3
from datetime import datetime, timedelta

def run_iam_audit():
    """Weekly IAM audit for data platform roles"""
    iam = boto3.client('iam')
    findings = []
    
    data_roles = ['DataEngineer', 'DataAnalyst', 'GlueETLRole', 'RedshiftServiceRole']
    
    for role_name in data_roles:
        # Check 1: Last used
        role = iam.get_role(RoleName=role_name)['Role']
        last_used = role.get('RoleLastUsed', {}).get('LastUsedDate')
        if last_used:
            days_unused = (datetime.utcnow() - last_used.replace(tzinfo=None)).days
            if days_unused > 60:
                findings.append({
                    'severity': 'MEDIUM',
                    'role': role_name,
                    'finding': f'Role unused for {days_unused} days',
                    'recommendation': 'Review and consider deletion'
                })
        
        # Check 2: Overly broad permissions
        policies = iam.list_attached_role_policies(RoleName=role_name)
        for policy in policies['AttachedPolicies']:
            if policy['PolicyName'] in ['AdministratorAccess', 'PowerUserAccess']:
                findings.append({
                    'severity': 'CRITICAL',
                    'role': role_name,
                    'finding': f'Has {policy["PolicyName"]} attached',
                    'recommendation': 'Replace with least-privilege policy'
                })
        
        # Check 3: Access Analyzer unused permissions
        # (Requires Access Analyzer to be configured)
    
    return findings

def remediate_finding(finding):
    """Auto-remediate low-risk findings"""
    if finding['severity'] == 'MEDIUM' and 'unused' in finding['finding']:
        # Tag role for review (don't auto-delete)
        iam = boto3.client('iam')
        iam.tag_role(
            RoleName=finding['role'],
            Tags=[
                {'Key': 'audit-status', 'Value': 'pending-review'},
                {'Key': 'audit-date', 'Value': datetime.utcnow().isoformat()}
            ]
        )
```

---

## Security Incident Response

```python
def respond_to_credential_leak(compromised_role_name):
    """Immediate response when credentials are compromised"""
    iam = boto3.client('iam')
    
    # Step 1: Revoke all active sessions immediately
    iam.put_role_policy(
        RoleName=compromised_role_name,
        PolicyName='DenyAllAfterCompromise',
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Action": "*",
                "Resource": "*",
                "Condition": {
                    "DateLessThan": {
                        "aws:TokenIssueTime": datetime.utcnow().isoformat()
                    }
                }
            }]
        })
    )
    
    # Step 2: Review CloudTrail for unauthorized actions
    cloudtrail = boto3.client('cloudtrail')
    events = cloudtrail.lookup_events(
        LookupAttributes=[
            {'AttributeKey': 'Username', 'AttributeValue': compromised_role_name}
        ],
        StartTime=datetime.utcnow() - timedelta(hours=24)
    )
    
    # Step 3: Log all actions taken by compromised credential
    suspicious_actions = []
    for event in events['Events']:
        detail = json.loads(event['CloudTrailEvent'])
        suspicious_actions.append({
            'time': event['EventTime'].isoformat(),
            'action': detail['eventName'],
            'source_ip': detail.get('sourceIPAddress'),
            'resources': detail.get('resources', [])
        })
    
    # Step 4: Notify security team
    sns = boto3.client('sns')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:123456789:security-incidents',
        Subject=f'CRITICAL: Credential compromise - {compromised_role_name}',
        Message=json.dumps({
            'role': compromised_role_name,
            'actions_taken': 'Revoked sessions, auditing CloudTrail',
            'suspicious_events': len(suspicious_actions),
            'details': suspicious_actions[:10]
        })
    )
    
    return suspicious_actions
```

---

## Interview Tips

> **Tip 1:** "Design an IAM strategy for a data platform" — "Four-tier role hierarchy: Admin (infra changes, 2-3 people), Engineer (build pipelines, read/write data), Analyst (query only, specific workgroups), Service Accounts (automated jobs, scoped to specific resources). Permission boundaries prevent escalation. ABAC (tags) for team-based isolation at scale. Cross-account access via AssumeRole with external IDs. Weekly automated audits flag unused roles and overly broad permissions."

> **Tip 2:** "How do you handle a credential compromise?" — "Immediate response: (1) Attach inline Deny-all policy with condition on TokenIssueTime (invalidates all existing sessions). (2) Audit CloudTrail for last 24-48 hours of activity from that principal. (3) Identify what was accessed/modified. (4) Rotate any secrets that were accessed. (5) Determine root cause (leaked in logs, exposed in code, phishing). The key insight: you can't revoke IAM role sessions directly, but the TokenIssueTime condition trick effectively blocks all existing credentials."

> **Tip 3:** "How do you scale IAM management for 100+ data engineers?" — "IAM Identity Center (SSO) with permission sets assigned to groups. Engineers join their team's AD/Okta group and automatically get appropriate AWS access across all accounts. ABAC policies use the team tag from SSO — one policy works for all teams. Automated audit Lambda runs weekly, flags stale roles, overly broad permissions, and unknown cross-account trusts. Permission boundaries ensure no team can exceed their scope."
