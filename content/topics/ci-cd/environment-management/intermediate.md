---
title: "Environment Management - Intermediate"
topic: ci-cd
subtopic: environment-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, environments,secrets,config,parity]
---

# Environment Management — Intermediate

See fundamentals for core concepts. This section covers intermediate patterns and real-world implementation.

## AWS Secrets Manager Integration

```python
import boto3, json
from functools import lru_cache

@lru_cache(maxsize=None)
def get_secret(secret_name: str) -> dict:
    """Cache secrets to avoid repeated API calls."""
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Usage
db_creds = get_secret("prod/pipeline/db")
DB_URL = f"postgresql://{db_creds['user']}:{db_creds['password']}@{db_creds['host']}/db"
```

## HashiCorp Vault

```python
import hvac

client = hvac.Client(url="https://vault.internal", token=os.environ["VAULT_TOKEN"])
secret = client.secrets.kv.read_secret_version(path="pipeline/db")
DB_PASSWORD = secret["data"]["data"]["password"]
```

## Infrastructure Parity with Terraform

```hcl
# Same Terraform for all environments — different tfvars
# environments/prod/terraform.tfvars
environment    = "prod"
db_instance    = "db.r5.2xlarge"
min_capacity   = 3

# environments/staging/terraform.tfvars
environment    = "staging"
db_instance    = "db.t3.medium"
min_capacity   = 1
```

## Non-Prod Data Safety

```python
# Never use production data in non-prod environments
# Generate synthetic/anonymized data for staging

def create_staging_data(prod_df: pd.DataFrame) -> pd.DataFrame:
    staging = prod_df.copy()
    staging["email"] = staging["email"].apply(lambda e: f"test_{hash(e)}@example.com")
    staging["name"] = staging["customer_id"].apply(lambda id: f"Test Customer {id}")
    # Shuffle amounts to prevent reverse-engineering
    staging["amount"] = staging["amount"].sample(frac=1).values
    return staging
```
