---
title: "AWS IAM - Intermediate"
topic: aws-services
subtopic: iam
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, iam, security, policies, roles, cross-account]
---

# AWS IAM — Intermediate Concepts

## Policy Structure in Depth

Every IAM policy follows the same JSON structure with five key elements:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueJobExecution",
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:BatchStopJobRun"
            ],
            "Resource": "arn:aws:glue:us-east-1:123456789:job/etl-*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "us-east-1"
                },
                "IpAddress": {
                    "aws:SourceIp": "10.0.0.0/8"
                }
            }
        }
    ]
}
```

| Element | Purpose | Example |
|---------|---------|---------|
| Effect | Allow or Deny | `"Allow"` |
| Action | API operations | `"s3:GetObject"`, `"glue:*"` |
| Resource | What the policy applies to | ARN with wildcards |
| Condition | When the policy applies | IP range, time, tags |
| Principal | Who (resource-based only) | Account, role, service |

---

## Cross-Account Roles (AssumeRole)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCrossAccountAssume",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::987654321:role/DataEngineerRole"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "shared-data-lake-access"
                }
            }
        }
    ]
}
```

```python
import boto3

# Account B assumes a role in Account A
sts = boto3.client('sts')
credentials = sts.assume_role(
    RoleArn='arn:aws:iam::123456789:role/CrossAccountDataReader',
    RoleSessionName='etl-job-session',
    ExternalId='shared-data-lake-access',
    DurationSeconds=3600
)

# Use temporary credentials to access Account A's resources
s3 = boto3.client('s3',
    aws_access_key_id=credentials['Credentials']['AccessKeyId'],
    aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
    aws_session_token=credentials['Credentials']['SessionToken']
)
# Now read from Account A's bucket
data = s3.get_object(Bucket='account-a-data-lake', Key='curated/orders/2024/')
```

---

## Service-Linked Roles

Pre-defined roles that AWS services use to call other services on your behalf:

```python
iam = boto3.client('iam')

# Common service-linked roles for data engineering:
# - AWSServiceRoleForGlue (Glue accessing S3, CloudWatch)
# - AWSServiceRoleForRedshift (Redshift accessing S3, Glue Catalog)
# - AWSServiceRoleForLakeFormation (LF managing permissions)
# - AWSServiceRoleForEMR (EMR provisioning EC2, S3 access)

# List service-linked roles
response = iam.list_roles(PathPrefix='/aws-service-role/')
for role in response['Roles']:
    print(f"{role['RoleName']} → {role['AssumeRolePolicyDocument']}")
```

---

## Permission Boundaries

Permission boundaries set the MAXIMUM permissions a role can have, regardless of what identity policies grant:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "BoundaryAllowedServices",
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "glue:*",
                "athena:*",
                "logs:*",
                "cloudwatch:*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "DenyProductionModification",
            "Effect": "Deny",
            "Action": [
                "s3:DeleteBucket",
                "glue:DeleteDatabase",
                "glue:DeleteTable"
            ],
            "Resource": "*"
        }
    ]
}
```

**Effective permissions = Identity policy INTERSECT Permission boundary**

Even if an admin accidentally attaches `AdministratorAccess` to a role with this boundary, the role can only use S3, Glue, Athena, Logs, and CloudWatch — and can never delete production resources.

---

## Policy Evaluation Logic

```
Decision flow (in order):
1. Explicit Deny → DENY (always wins)
2. Organization SCP → must Allow (or implicit deny)
3. Permission boundary → must Allow (or implicit deny)
4. Identity policy → must Allow
5. Resource-based policy → can Allow independently (same account)

All conditions above must pass for Allow. Any Deny at any level = DENY.
```

**Practical example:**
```
Q: Can role X read s3://data-lake/pii/customers.parquet?

Check 1: Any explicit Deny in any policy? → No
Check 2: Does the Org SCP allow s3:GetObject? → Yes
Check 3: Does the permission boundary allow s3:GetObject? → Yes (s3:* allowed)
Check 4: Does the identity policy allow s3:GetObject on this resource? → Yes
Result: ALLOW

If Condition on identity policy says "only from VPC endpoint" and request 
came from public internet → Condition fails → implicit deny at step 4 → DENY
```

---

## IAM Access Analyzer

```python
# Find resources shared outside your account
access_analyzer = boto3.client('accessanalyzer')

# Create an analyzer (account-level or org-level)
access_analyzer.create_analyzer(
    analyzerName='data-lake-analyzer',
    type='ACCOUNT'  # or 'ORGANIZATION'
)

# List findings (external access)
findings = access_analyzer.list_findings(analyzerName='data-lake-analyzer')
for finding in findings['findings']:
    print(f"Resource: {finding['resource']}")
    print(f"External principal: {finding['principal']}")
    print(f"Access: {finding['action']}")
    # Example: S3 bucket policy allows public access
    # Example: IAM role trusts an external account
```

**Use cases for data engineers:**
- Detect S3 buckets accidentally shared publicly
- Find cross-account access you didn't intend
- Validate IAM roles aren't assumable by unauthorized accounts
- Audit before/after security reviews

---

## Resource-Based vs Identity-Based Policies

| Aspect | Identity-Based | Resource-Based |
|--------|---------------|----------------|
| Attached to | IAM user/role/group | AWS resource (S3, SQS, KMS) |
| Principal field | Not needed (implied) | Required (who can access) |
| Cross-account | Requires AssumeRole | Direct grant (no role switch) |
| Example | Role policy for Glue job | S3 bucket policy |

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCrossAccountRead",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::987654321:root"
            },
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::shared-data-lake/curated/*"
        }
    ]
}
```

> **Key difference for cross-account:** Resource-based policies (like S3 bucket policies) allow direct access without AssumeRole. The calling account's identity retains their original permissions. With AssumeRole, you "become" the target role and lose your original permissions for that session.

---

## Interview Tips

> **Tip 1:** "How does IAM policy evaluation work?" — "Five-layer evaluation: (1) Explicit Deny always wins. (2) Organization SCPs must allow. (3) Permission boundaries must allow. (4) Identity policy must allow. (5) Resource-based policies can independently allow (same account). All layers must pass — any deny or missing allow at any layer results in denial. This is why an explicit Deny in an SCP overrides any Allow in a role policy."

> **Tip 2:** "When would you use a permission boundary vs a policy?" — "Permission boundaries set the ceiling for what a role CAN do (max permissions), while identity policies define what it DOES do. Use boundaries when delegating role creation — let developers create roles for their services, but the boundary ensures those roles can never exceed data-team permissions (can't access production databases, can't delete infrastructure). Effective permissions = identity policy INTERSECT boundary."

> **Tip 3:** "Resource-based vs identity-based for cross-account access?" — "Two approaches: (1) Resource-based (S3 bucket policy grants external account) — simpler, caller keeps their own permissions. (2) Identity-based with AssumeRole — caller assumes a role in the target account. Use resource-based for simple read access to S3/KMS. Use AssumeRole for complex multi-service access or when you need to audit who did what (each session has a unique name). AssumeRole is more secure for sensitive operations."
