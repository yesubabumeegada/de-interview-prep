---
title: "AWS Secrets Manager - Senior Deep Dive"
topic: aws-services
subtopic: secrets-manager
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, secrets-manager, security, compliance, multi-account, governance]
---

# AWS Secrets Manager — Senior-Level Deep Dive

## Enterprise Secret Governance

### Organization-Wide Secret Policies

```python
# Use AWS Organizations SCP to enforce Secrets Manager usage
# Deny any service from using plaintext credentials in environment variables:
scp_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "DenyPlaintextSecrets",
        "Effect": "Deny",
        "Action": [
            "lambda:CreateFunction",
            "lambda:UpdateFunctionConfiguration"
        ],
        "Resource": "*",
        "Condition": {
            "ForAnyValue:StringLike": {
                "lambda:FunctionEnvironmentVariables": ["*PASSWORD*", "*SECRET*", "*API_KEY*"]
            }
        }
    }]
}
# Prevents anyone from putting passwords in Lambda environment variables
```

### Secret Access Tiers

```
Tier 1 (Critical): Database master credentials, encryption keys
  - Access: Infrastructure team only
  - Rotation: Every 7 days
  - Approval: Change request required
  - Audit: Real-time alerts on access

Tier 2 (Standard): Application DB users, API keys
  - Access: Specific service roles
  - Rotation: Every 30 days
  - Approval: Automated
  - Audit: Daily review

Tier 3 (Low Risk): Internal service tokens, test credentials
  - Access: Team roles
  - Rotation: Every 90 days
  - Approval: Self-service
  - Audit: Monthly review
```

---

## Automated Compliance Reporting

```python
import boto3
from datetime import datetime, timedelta

def generate_secrets_compliance_report():
    """Weekly compliance report: rotation status, access patterns, aging."""
    secrets_client = boto3.client('secretsmanager')
    paginator = secrets_client.get_paginator('list_secrets')
    
    report = {'compliant': [], 'non_compliant': [], 'warnings': []}
    
    for page in paginator.paginate():
        for secret in page['SecretList']:
            name = secret['Name']
            last_rotated = secret.get('LastRotatedDate')
            last_accessed = secret.get('LastAccessedDate')
            rotation_enabled = secret.get('RotationEnabled', False)
            
            # Check: rotation enabled for production secrets
            if 'prod/' in name and not rotation_enabled:
                report['non_compliant'].append({
                    'secret': name,
                    'issue': 'Production secret without rotation enabled',
                    'severity': 'HIGH'
                })
            
            # Check: rotated within last 90 days
            if last_rotated:
                days_since_rotation = (datetime.now(last_rotated.tzinfo) - last_rotated).days
                if days_since_rotation > 90:
                    report['non_compliant'].append({
                        'secret': name,
                        'issue': f'Not rotated in {days_since_rotation} days',
                        'severity': 'MEDIUM'
                    })
            
            # Check: accessed recently (unused secrets should be deleted)
            if last_accessed:
                days_since_access = (datetime.now(last_accessed.tzinfo) - last_accessed).days
                if days_since_access > 90:
                    report['warnings'].append({
                        'secret': name,
                        'issue': f'Not accessed in {days_since_access} days — consider deletion',
                    })
    
    return report
```

---

## Rotation Failure Handling

```python
# What happens when rotation fails:
# 1. New password is AWSPENDING (not promoted to AWSCURRENT)
# 2. AWSCURRENT still has the OLD password (apps continue working!)
# 3. Secrets Manager retries rotation after the configured interval
# 4. You should monitor for stuck rotations:

def check_rotation_health():
    """Detect secrets stuck in rotation (AWSPENDING exists but never promoted)."""
    paginator = secrets_client.get_paginator('list_secret_version_ids')
    
    for secret in list_all_secrets():
        versions = secrets_client.list_secret_version_ids(SecretId=secret['ARN'])
        
        for version in versions['Versions']:
            if 'AWSPENDING' in version.get('VersionStages', []):
                # AWSPENDING exists — rotation may be stuck
                created = version['CreatedDate']
                age_hours = (datetime.now(created.tzinfo) - created).total_seconds() / 3600
                
                if age_hours > 24:
                    alert(f"Secret {secret['Name']} has AWSPENDING version for {age_hours:.0f} hours — rotation may be stuck!")

# Recovery from stuck rotation:
# Option 1: Fix the rotation Lambda bug, then:
secrets_client.rotate_secret(SecretId='stuck-secret')  # Re-trigger rotation

# Option 2: Cancel the pending rotation manually:
secrets_client.cancel_rotate_secret(SecretId='stuck-secret')
# Then fix the issue and re-enable rotation
```

---

## Multi-Region DR for Secrets

```python
# Primary region: us-east-1 (secrets source of truth)
# DR region: eu-west-1 (replicated copies)

# Create secret with replication
secrets_client.create_secret(
    Name='prod/rds/credentials',
    SecretString=json.dumps(creds),
    AddReplicaRegions=[
        {
            'Region': 'eu-west-1',
            'KmsKeyId': 'arn:aws:kms:eu-west-1:123:key/eu-key-id'  # Region-specific KMS key
        }
    ]
)

# Failover: applications in EU automatically read from local replica
# No code change needed — Secrets Manager endpoint resolves to local region

# Promotion: if primary region is permanently lost
secrets_client_eu = boto3.client('secretsmanager', region_name='eu-west-1')
secrets_client_eu.stop_replication_to_replica(SecretId='prod/rds/credentials')
# EU replica becomes a standalone primary secret (can now be updated/rotated independently)
```

---

## Integration with Data Pipeline Frameworks

### dbt + Secrets Manager

```yaml
# profiles.yml (dbt) — use environment variables populated from Secrets Manager
# In CI/CD pipeline: fetch secret → export as env vars → run dbt

# CI/CD step:
# SECRET=$(aws secretsmanager get-secret-value --secret-id prod/redshift --query SecretString --output text)
# export DBT_HOST=$(echo $SECRET | jq -r .host)
# export DBT_PASSWORD=$(echo $SECRET | jq -r .password)
# dbt run --target prod
```

### Spark + Secrets Manager

```python
# In PySpark job: fetch credentials at driver level, pass to executors
import boto3, json

# Driver fetches secret (once)
creds = json.loads(
    boto3.client('secretsmanager').get_secret_value(SecretId='prod/jdbc/source')['SecretString']
)

# Use in JDBC read (credentials passed to executors via Spark internals)
df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['dbname']}") \
    .option("user", creds['username']) \
    .option("password", creds['password']) \
    .option("dbtable", "orders") \
    .load()
```

---

## Cost Optimization

| Optimization | Savings | How |
|-------------|---------|-----|
| Cache secrets (5-min TTL) | 90%+ API call reduction | Use aws_secretsmanager_caching library |
| Use Parameter Store for non-rotating config | $0.40/secret/month saved | Only put credentials in Secrets Manager |
| Delete unused secrets | $0.40/month per unused secret | Audit with last_accessed_date |
| Batch secret retrieval | Fewer API calls | `batch_get_secret_value()` (if supported) |

---

## Interview Tips

> **Tip 1:** "How do you ensure secret rotation doesn't break production?" — "Three safeguards: (1) Alternating-user rotation so there's never a moment with zero valid passwords. (2) Applications use caching with 5-minute TTL — picks up new password within 5 minutes of rotation. (3) Monitor rotation health — alert if AWSPENDING exists for >24 hours (stuck rotation). Test rotation in staging first."

> **Tip 2:** "Design secret management for a multi-account data platform" — "Central account owns all secrets. Resource policies grant cross-account GetSecretValue to specific IAM roles. Naming convention: {env}/{service}/{purpose}. IAM policies grant by prefix: prod/* to production roles only. Cross-region replication for DR. CloudTrail + EventBridge for access auditing and anomaly detection."

> **Tip 3:** "Secrets Manager vs Vault (HashiCorp) on AWS?" — "Secrets Manager: native AWS integration (IAM, Glue, Lambda, MWAA auto-discovery), managed rotation, no infrastructure. Vault: multi-cloud, dynamic secrets (generates temp credentials on demand), more powerful policy engine, but requires self-managed infrastructure. For AWS-only data platforms: Secrets Manager wins on simplicity. For multi-cloud: Vault."

## ⚡ Cheat Sheet

**Secrets Manager vs Parameter Store**
| Feature | Secrets Manager | Parameter Store |
|---|---|---|
| Auto-rotation | Yes (built-in Lambda) | No |
| Cost | $0.40/secret/month | Free (standard) |
| Max size | 65 KB | 4 KB (std) / 8 KB (advanced) |
| Best for | DB credentials, API keys | Config values, feature flags |

**Rotation setup**
```python
# Rotation Lambda must implement 4 steps:
# createSecret, setSecret, testSecret, finishSecret
# AWS provides templates for RDS, Redshift, DocumentDB
aws secretsmanager rotate-secret --secret-id mydb/prod/password   --rotation-lambda-arn arn:aws:lambda:...   --rotation-rules AutomaticallyAfterDays=30
```

**Access patterns**
```python
import boto3, json
client = boto3.client('secretsmanager')
secret = json.loads(client.get_secret_value(SecretId='mydb/prod')['SecretString'])
# Cache with: aws-secretsmanager-caching-python (reduces API calls)
```

**Airflow integration**
```python
# In airflow.cfg:
# [secrets]
# backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
# backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

**IAM policy (least privilege)**
```json
{"Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"],
 "Resource": "arn:aws:secretsmanager:us-east-1:123:secret:mydb/prod-*",
 "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1"}}}
```

**Cross-account access**
- Resource-based policy on secret: grant `secretsmanager:GetSecretValue` to external account
- KMS key policy: if using CMK, grant decrypt to the external account's role

**Key operational rules**
- Never put secrets in environment variables, code, or CloudFormation Parameters (plain)
- Rotate within 24h of suspected compromise; use `force-delete-without-recovery` for immediate delete
- Use secret ARN (not name) in cross-account and cross-region references
