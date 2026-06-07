---
title: "AWS Secrets Manager - Real-World Production Examples"
topic: aws-services
subtopic: secrets-manager
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, secrets-manager, production, rotation, data-platform]
---

# AWS Secrets Manager — Real-World Production Examples

## Pattern 1: Glue Job with Secrets Manager

```python
# Production Glue job: secure credential retrieval
import sys, json, boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret_name'])

# Retrieve credentials securely (never in script or environment)
def get_credentials(secret_name):
    client = boto3.client('secretsmanager')
    return json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])

source_creds = get_credentials(args['secret_name'])

# Use in JDBC connection
df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": f"jdbc:postgresql://{source_creds['host']}:{source_creds['port']}/{source_creds['dbname']}",
        "user": source_creds['username'],
        "password": source_creds['password'],
        "dbtable": "public.orders",
    }
).toDF()

# Process and write output
df.write.parquet("s3://data-lake/curated/orders/")
```

**IAM Role for Glue:**
```json
{
    "Effect": "Allow",
    "Action": "secretsmanager:GetSecretValue",
    "Resource": "arn:aws:secretsmanager:us-east-1:123:secret:prod/source-db/*"
}
```

---

## Pattern 2: Automated RDS Password Rotation

```python
# Setup: RDS + Secrets Manager + Rotation Lambda

# Step 1: Create the secret (linked to RDS)
secrets.create_secret(
    Name='prod/rds/analytics-db',
    SecretString=json.dumps({
        'engine': 'postgres',
        'host': 'analytics.xxx.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'analytics',
        'username': 'app_user',
        'password': 'initial-password',
        'masterarn': 'arn:aws:secretsmanager:...:secret:prod/rds/master-credentials'
    })
)

# Step 2: Enable rotation with AWS-managed Lambda template
secrets.rotate_secret(
    SecretId='prod/rds/analytics-db',
    RotationLambdaARN='arn:aws:lambda:...:function:SecretsManagerRDSPostgreSQLRotationMultiUser',
    RotationRules={
        'AutomaticallyAfterDays': 30,
        'Duration': '2h',           # Rotation window (2 hours max)
        'ScheduleExpression': 'cron(0 4 1 * ? *)'  # 4 AM on 1st of each month
    }
)

# Step 3: Monitor rotation health
cloudwatch.put_metric_alarm(
    AlarmName='rds-secret-rotation-health',
    Namespace='AWS/SecretsManager',
    MetricName='RotationSucceeded',
    Statistic='Sum',
    Period=86400 * 35,  # 35 days (should rotate within 30)
    EvaluationPeriods=1,
    Threshold=0,
    ComparisonOperator='LessThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:...:security-alerts'],
    AlarmDescription='Secret not rotated in 35 days — rotation may be broken'
)
```

---

## Pattern 3: MWAA Connections via Secrets Manager

```python
# MWAA environment configuration:
# AirflowConfigurationOptions:
#   secrets.backend: airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
#   secrets.backend_kwargs: {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}

# Store Airflow connections as secrets:
# Secret name: airflow/connections/redshift_prod
secrets.create_secret(
    Name='airflow/connections/redshift_prod',
    SecretString=json.dumps({
        "conn_type": "redshift",
        "host": "prod-cluster.xxx.redshift.amazonaws.com",
        "login": "airflow_etl",
        "password": "secure-password",
        "port": 5439,
        "schema": "warehouse",
        "extra": json.dumps({"keepalives_idle": 300})
    })
)

# In your DAG: just reference by connection ID (auto-discovered!)
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

def load_data(**context):
    hook = RedshiftSQLHook(redshift_conn_id='redshift_prod')
    hook.run("COPY fact_orders FROM 's3://data-lake/curated/orders/' IAM_ROLE '...' PARQUET;")

# Benefits:
# - No manual connection setup in Airflow UI
# - Secrets rotate automatically (Airflow picks up new password on next task)
# - Version controlled (via IaC), not click-ops in a web UI
# - Same secret used by Glue AND Airflow (single source of truth)
```

---

## Pattern 4: Cross-Account Data Platform Secrets

```python
# Architecture: Central security account manages all credentials
# Data accounts (analytics, ML, reporting) consume them

# Central account: create secrets for shared resources
shared_secrets = [
    ('shared/redshift/reader', {'host': '...', 'user': 'reader', 'password': '...'}),
    ('shared/redshift/writer', {'host': '...', 'user': 'writer', 'password': '...'}),
    ('shared/kafka/msk-creds', {'bootstrap': '...', 'key': '...', 'secret': '...'}),
    ('shared/api/data-catalog', {'api_key': '...', 'endpoint': '...'}),
]

for name, value in shared_secrets:
    secrets.create_secret(Name=name, SecretString=json.dumps(value))
    # Add resource policy for cross-account access
    secrets.put_resource_policy(SecretId=name, ResourcePolicy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::*:root"},
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "*",
            "Condition": {"StringEquals": {"aws:PrincipalOrgID": "o-myorg123"}}
        }]
    }))
    # Any account in our AWS Organization can access (but only org members)
```

---

## Production Operations Checklist

| Task | Frequency | Method |
|------|-----------|--------|
| Verify rotation is occurring | Monthly | Check LastRotatedDate for all prod secrets |
| Audit access patterns | Weekly | CloudTrail logs → unexpected access |
| Remove unused secrets | Monthly | Check LastAccessedDate > 90 days |
| Test rotation manually | Quarterly | `rotate_secret()` in staging |
| Verify cross-account access | Quarterly | Test from each consumer account |
| Review resource policies | Quarterly | Ensure least-privilege |
| Validate secret naming convention | On creation | Automated via Config Rule |
| Backup secret values | Never (they're in Secrets Manager!) | SM handles versioning + recovery |

---

## Interview Tips

> **Tip 1:** "Walk through your secrets management for a data platform" — "All credentials in Secrets Manager (never code/env vars). Named by convention: {env}/{service}/{purpose}. Rotation enabled for all database credentials (30-day cycle with alternating users). MWAA auto-discovers connections. Glue jobs call get_secret_value() at startup. Cross-account access via resource policies within our AWS Organization. CloudTrail audits every access."

> **Tip 2:** "How do you migrate from hardcoded credentials to Secrets Manager?" — "Phased: (1) Inventory all hardcoded credentials across codebase. (2) Create corresponding secrets in Secrets Manager. (3) Update IAM roles to grant GetSecretValue. (4) Refactor code to retrieve at runtime (one service at a time). (5) Verify via CloudTrail that secrets are being accessed. (6) Remove hardcoded values from code. (7) Enable rotation."

> **Tip 3:** "What's the blast radius of a compromised secret?" — "With proper controls: limited. Each secret has its own IAM policy (only specific roles can access). Rotation limits exposure window (max 30 days). CloudTrail detects anomalous access immediately. Cross-account requires organization membership. If compromised: rotate immediately (`rotate_secret()`), revoke the IAM role, investigate CloudTrail for scope of access."
