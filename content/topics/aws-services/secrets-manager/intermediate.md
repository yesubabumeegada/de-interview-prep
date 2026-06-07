---
title: "AWS Secrets Manager - Intermediate"
topic: aws-services
subtopic: secrets-manager
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, secrets-manager, rotation, caching, cross-account, parameter-store]
---

# AWS Secrets Manager — Intermediate Concepts

## Caching for Performance

Every `get_secret_value()` call is an API request (~50ms latency + cost). Cache secrets in memory:

```python
# AWS provides an official caching library
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

# Configure cache with 5-minute TTL
cache_config = SecretCacheConfig(
    max_cache_size=100,                    # Max secrets to cache
    exception_retry_delay_base=1,          # Retry backoff base
    secret_refresh_interval=300,           # Refresh every 5 minutes (300s)
    secret_version_stage_refresh_interval=300
)

cache = SecretCache(config=cache_config)

# Usage (cached — no API call if within TTL)
secret_string = cache.get_secret_string('prod/rds/credentials')
creds = json.loads(secret_string)

# Benefits:
# - First call: ~50ms (API call)
# - Subsequent calls within 5 min: <1ms (memory cache)
# - Handles rotation automatically (cache refreshes on TTL expiry)
# - Thread-safe for multi-threaded applications
```

---

## Rotation Lambda Patterns

### Single-User Rotation (Simplest)

```python
# One database user, password changes in-place
# Downside: brief moment where old password stops working

def lambda_handler(event, context):
    """AWS-provided rotation template for single-user."""
    step = event['Step']  # createSecret, setSecret, testSecret, finishSecret
    secret_arn = event['SecretId']
    
    if step == 'createSecret':
        # Generate new random password
        new_password = secrets.get_random_password(
            PasswordLength=32, ExcludeCharacters='/@"\\\'`'
        )['RandomPassword']
        secrets.put_secret_value(
            SecretId=secret_arn,
            SecretString=json.dumps({**current_secret, 'password': new_password}),
            VersionStages=['AWSPENDING']
        )
    
    elif step == 'setSecret':
        # Update the database with new password
        pending = get_secret(secret_arn, 'AWSPENDING')
        conn = connect_as_admin(secret_arn)
        conn.execute(f"ALTER USER {pending['username']} PASSWORD '{pending['password']}'")
    
    elif step == 'testSecret':
        # Verify new password works
        pending = get_secret(secret_arn, 'AWSPENDING')
        test_conn = psycopg2.connect(host=pending['host'], password=pending['password'], ...)
        test_conn.close()
    
    elif step == 'finishSecret':
        # Promote AWSPENDING → AWSCURRENT
        secrets.update_secret_version_stage(
            SecretId=secret_arn,
            VersionStage='AWSCURRENT',
            MoveToVersionId=event['ClientRequestToken']
        )
```

### Alternating-User Rotation (Zero Downtime)

```python
# Two database users alternate: user_A and user_B
# While A is active (AWSCURRENT), B gets its password changed
# Then B becomes AWSCURRENT, A becomes available for next rotation
# Result: zero moment where NO password works

# Secret stores both users:
# {"username": "etl_user_a", "password": "...", "alt_username": "etl_user_b", "alt_password": "..."}
```

---

## Cross-Region Replication

For multi-region applications that need low-latency secret access:

```python
# Create a secret with replicas
secrets.create_secret(
    Name='global/kafka/credentials',
    SecretString=json.dumps({'username': 'kafka', 'password': 'secret'}),
    AddReplicaRegions=[
        {'Region': 'eu-west-1'},
        {'Region': 'ap-southeast-1'}
    ]
)

# Each region has a read replica of the secret
# Applications in eu-west-1 call their LOCAL Secrets Manager endpoint
# Latency: <10ms (local) vs 100ms+ (cross-region)
# Rotation happens in primary region → automatically replicates to replicas
```

---

## Integration with Infrastructure as Code

### CloudFormation

```yaml
Resources:
  DatabaseSecret:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: !Sub "${Environment}/rds/credentials"
      GenerateSecretString:
        SecretStringTemplate: '{"username": "admin"}'
        GenerateStringKey: "password"
        PasswordLength: 32
        ExcludeCharacters: '/@"\\`'
  
  RotationSchedule:
    Type: AWS::SecretsManager::RotationSchedule
    Properties:
      SecretId: !Ref DatabaseSecret
      RotationLambdaARN: !GetAtt RotationLambda.Arn
      RotationRules:
        AutomaticallyAfterDays: 30
```

### Terraform

```hcl
resource "aws_secretsmanager_secret" "db_creds" {
  name = "${var.environment}/rds/credentials"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "db_creds" {
  secret_id     = aws_secretsmanager_secret.db_creds.id
  secret_string = jsonencode({
    username = "admin"
    password = random_password.db.result
    host     = aws_db_instance.main.endpoint
    port     = 5432
  })
}
```

---

## Monitoring and Auditing

```python
# CloudTrail automatically logs ALL Secrets Manager API calls:
# - GetSecretValue (who accessed which secret)
# - CreateSecret, UpdateSecret, DeleteSecret
# - RotateSecret (rotation events)

# Alert on suspicious access:
# EventBridge rule → detect access from unexpected roles
events.put_rule(
    Name='unexpected-secret-access',
    EventPattern=json.dumps({
        "source": ["aws.secretsmanager"],
        "detail-type": ["AWS API Call via CloudTrail"],
        "detail": {
            "eventName": ["GetSecretValue"],
            "errorCode": ["AccessDeniedException"]  # Failed access attempts
        }
    })
)

# Monitor rotation health:
# Alert if rotation fails (secret stuck in AWSPENDING state)
cloudwatch.put_metric_alarm(
    AlarmName='secret-rotation-failure',
    Namespace='AWS/SecretsManager',
    MetricName='RotationFailed',
    Threshold=0,
    ComparisonOperator='GreaterThanThreshold',
    Period=86400,
    EvaluationPeriods=1,
    AlarmActions=['arn:aws:sns:...:security-alerts']
)
```

---

## Best Practices

| Practice | Why |
|----------|-----|
| Never hardcode credentials | Code is shared (Git), logged, leaked |
| Use IAM roles (not access keys) where possible | No secret to manage at all |
| Cache secrets in memory (5-min TTL) | Reduce API calls and latency |
| Enable rotation for all database passwords | Compliance + limits blast radius of compromise |
| Use naming conventions with environment prefix | Easy IAM policy by prefix (prod/* vs dev/*) |
| Set `recovery_window_in_days` = 7 | Prevents accidental permanent deletion |
| Tag secrets (team, environment, service) | Cost allocation and access auditing |
| Monitor rotation health | Catch failures before apps break |

---

## Interview Tips

> **Tip 1:** "How do you handle credential rotation without breaking pipelines?" — "Alternating-user rotation: two DB users swap roles each rotation cycle. While user_A is active, user_B's password changes. Then B becomes active. There's never a moment where no valid password exists. Applications using Secrets Manager auto-get the current password on next call."

> **Tip 2:** "Secrets Manager vs environment variables?" — "Env vars: visible in logs/console, no audit trail, no rotation, shared across all processes. Secrets Manager: encrypted (KMS), CloudTrail audit log, automatic rotation, fine-grained IAM access. The 50ms latency cost is negligible — cache to <1ms for subsequent calls."

> **Tip 3:** "How do you manage secrets across multiple AWS accounts?" — "Central account owns all secrets. Resource-based policies grant cross-account access to specific IAM roles. Or use AWS Organizations with organization-wide access. For multi-region: enable cross-region replication so each region has a local copy (lower latency, region failure resilience)."
