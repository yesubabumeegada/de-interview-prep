---
title: "AWS Secrets Manager - Fundamentals"
topic: aws-services
subtopic: secrets-manager
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [aws, secrets-manager, credentials, security, rotation, pipelines]
---

# AWS Secrets Manager — Fundamentals


## 🎯 Analogy

Think of Secrets Manager like a bank vault for credentials: instead of hardcoding passwords in code or config files, your pipeline calls the vault at runtime and gets the secret — rotated automatically, never in plaintext in source code.

---
## What Is AWS Secrets Manager?

AWS Secrets Manager is a service that **securely stores, retrieves, and automatically rotates** credentials (database passwords, API keys, tokens) used by your applications and data pipelines.

**The analogy:** Instead of writing passwords on sticky notes (hardcoded in code/config files), Secrets Manager is a locked vault where credentials live. Your applications request them at runtime — never storing them in code or environment variables.

> **Why Secrets Manager matters for DE:** Every pipeline connects to databases, APIs, and cloud services. Hardcoded credentials in Glue scripts, Airflow DAGs, or Lambda functions are a security risk. Secrets Manager centralizes credential management with automatic rotation.

---

## Core Concepts

| Concept | What It Is |
|---------|-----------|
| **Secret** | A stored credential (JSON key-value pairs, binary, or plaintext) |
| **Secret Version** | Versioned value (AWSCURRENT, AWSPREVIOUS, AWSPENDING) |
| **Rotation** | Automatic periodic credential change (e.g., new password every 30 days) |
| **Resource Policy** | Controls who/what can access the secret |
| **Encryption** | All secrets encrypted at rest with KMS (customer or AWS-managed key) |

---

## Storing and Retrieving Secrets

```python
import boto3
import json

secrets = boto3.client('secretsmanager')

# Store a secret (database credentials)
secrets.create_secret(
    Name='prod/redshift/warehouse',
    Description='Redshift warehouse credentials',
    SecretString=json.dumps({
        'host': 'cluster.xxx.us-east-1.redshift.amazonaws.com',
        'port': 5439,
        'dbname': 'warehouse',
        'username': 'etl_user',
        'password': 'super-secret-password-123'
    }),
    Tags=[
        {'Key': 'Environment', 'Value': 'production'},
        {'Key': 'Team', 'Value': 'data-engineering'}
    ]
)

# Retrieve a secret (in your ETL job)
def get_db_credentials(secret_name: str) -> dict:
    """Retrieve database credentials from Secrets Manager."""
    response = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Usage in a pipeline
creds = get_db_credentials('prod/redshift/warehouse')
connection = psycopg2.connect(
    host=creds['host'],
    port=creds['port'],
    dbname=creds['dbname'],
    user=creds['username'],
    password=creds['password']
)
```

---

## Integration with Data Services

### Glue ETL Jobs

```python
# In a Glue job: retrieve credentials at runtime
import boto3, json
from awsglue.context import GlueContext

secrets = boto3.client('secretsmanager')
creds = json.loads(
    secrets.get_secret_value(SecretId='prod/source-db/credentials')['SecretString']
)

# Use credentials to read from JDBC source
df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['dbname']}",
        "user": creds['username'],
        "password": creds['password'],
        "dbtable": "public.orders"
    }
)
```

### Lambda Functions

```python
# Lambda: cache secret to avoid calling Secrets Manager on every invocation
import boto3, json

# Initialize outside handler (cached across warm invocations)
_cached_secret = None

def get_secret_cached(name):
    global _cached_secret
    if _cached_secret is None:
        client = boto3.client('secretsmanager')
        _cached_secret = json.loads(
            client.get_secret_value(SecretId=name)['SecretString']
        )
    return _cached_secret

def handler(event, context):
    creds = get_secret_cached('prod/api/credentials')
    # Use creds['api_key'] for external API call
```

### MWAA (Airflow) — Automatic Connection Discovery

```python
# MWAA can automatically discover Airflow connections from Secrets Manager
# Secret name: airflow/connections/my_redshift
# Secret value (JSON):
{
    "conn_type": "redshift",
    "host": "cluster.xxx.redshift.amazonaws.com",
    "login": "airflow_user",
    "password": "secret-password",
    "port": 5439,
    "schema": "warehouse"
}
# In your DAG: just reference the connection by name
hook = PostgresHook(postgres_conn_id='my_redshift')  # Auto-resolved from Secrets Manager!
```

---

## Automatic Rotation

```python
# Enable rotation: Secrets Manager automatically changes the password periodically
secrets.rotate_secret(
    SecretId='prod/rds/credentials',
    RotationLambdaARN='arn:aws:lambda:...:function:rotate-rds-secret',
    RotationRules={'AutomaticallyAfterDays': 30}  # Rotate every 30 days
)

# How rotation works:
# 1. Secrets Manager invokes your rotation Lambda
# 2. Lambda generates a new password
# 3. Lambda updates the database with the new password
# 4. Lambda stores the new password in Secrets Manager (AWSCURRENT)
# 5. Old password moves to AWSPREVIOUS (still works briefly for in-flight requests)
# 6. Applications using get_secret_value() automatically get the new password
```

**Built-in rotation support for:**
- Amazon RDS (all engines)
- Amazon Redshift
- Amazon DocumentDB
- Custom (any system via your Lambda)

---

## Secrets Manager vs Parameter Store

| Feature | Secrets Manager | Systems Manager Parameter Store |
|---------|----------------|-------------------------------|
| Automatic rotation | ✅ Built-in | ❌ Must build yourself |
| Cross-account sharing | ✅ Resource policies | Limited |
| Encryption | ✅ Always (KMS) | Optional (SecureString) |
| Versioning | ✅ Automatic | ✅ Automatic |
| Cost | $0.40/secret/month + $0.05/10K API calls | Free (standard) or $0.05/advanced |
| Best for | Credentials that rotate (DB passwords) | Config values, feature flags, non-rotating |

> **Rule of thumb:** Use Secrets Manager for credentials (passwords, API keys) that should rotate. Use Parameter Store for configuration values (endpoints, feature flags) that don't need rotation.

---

## Naming Conventions

```
Recommended naming structure:
{environment}/{service}/{purpose}

Examples:
  prod/redshift/warehouse        → Production Redshift credentials
  prod/rds/analytics-db          → Production RDS credentials
  prod/api/stripe-key            → Stripe API key
  dev/redshift/warehouse         → Development Redshift credentials
  shared/kafka/msk-credentials   → Shared across environments

Benefits:
- IAM policies can grant access by prefix: "prod/*" for production services
- Easy to find and audit
- Environment isolation built into naming
```

---

## Cost

| Component | Price |
|-----------|-------|
| Per secret stored | $0.40/month |
| API calls (retrieve) | $0.05 per 10,000 calls |
| Rotation Lambda | Standard Lambda pricing |

**Example:** 50 secrets × $0.40 + 1M API calls × $0.05/10K = $20 + $5 = **$25/month** (very affordable for enterprise-grade credential management).

---


## ▶️ Try It Yourself

```python
import boto3
import json

def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])

# Usage: no passwords in code
creds = get_secret("prod/postgres/orders-db")
conn_string = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
print("Connected using rotated credentials from Secrets Manager")

# Auto-rotation: Secrets Manager can rotate the password every N days
# and update the secret value — your code always gets the latest without changes
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How do you manage credentials in data pipelines?" — "Secrets Manager for all database passwords and API keys. Never hardcode credentials in code or environment variables. Pipelines call `get_secret_value()` at runtime. For Airflow: configure Secrets Manager as the backend — connections auto-resolve by name. For Glue: retrieve in the job script. Rotation enabled for all database credentials (30-day cycle)."

> **Tip 2:** "Secrets Manager vs environment variables?" — "Environment variables are visible in the console, logs, and process listings — a security risk. Secrets Manager encrypts at rest (KMS), provides audit trail (CloudTrail), supports automatic rotation, and allows fine-grained IAM access control. The only downside: adds ~50ms latency for retrieval (cache in memory to mitigate)."

> **Tip 3:** "How do you handle secret rotation without downtime?" — "Secrets Manager uses staging labels (AWSCURRENT, AWSPREVIOUS). During rotation: new password is set in the database, stored as AWSCURRENT, old password moves to AWSPREVIOUS. Both work simultaneously for a brief period. Applications using `get_secret_value()` automatically get the new password on next call. Zero-downtime if apps don't cache indefinitely."
