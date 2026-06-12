---
title: "Environment Management - Senior Deep Dive"
topic: ci-cd
subtopic: environment-management
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ci-cd, environments,secrets,config,parity]
---

# Environment Management — Senior Deep Dive

## Environment Parity Enforcement

```python
# Automated parity check: compare staging vs prod config
def check_environment_parity(staging_config: dict, prod_config: dict) -> list[str]:
    violations = []
    
    # Same database engine
    if staging_config["db_engine"] != prod_config["db_engine"]:
        violations.append(f"DB engine mismatch: staging={staging_config['db_engine']}, prod={prod_config['db_engine']}")
    
    # Same Python version
    import sys
    if staging_config["python_version"] != prod_config["python_version"]:
        violations.append(f"Python version mismatch")
    
    return violations
```

## Dynamic Secrets with Vault + K8s

```yaml
# K8s pod gets short-lived credentials from Vault
apiVersion: v1
kind: Pod
spec:
  serviceAccountName: pipeline-sa
  annotations:
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/role: "pipeline-role"
    vault.hashicorp.com/agent-inject-secret-db: "database/creds/pipeline"
    # Vault injects credentials as files in /vault/secrets/
    # Short-lived (1h TTL), auto-renewed
```

## ⚡ Cheat Sheet

```bash
# AWS Secrets Manager
aws secretsmanager get-secret-value --secret-id prod/pipeline/db
aws secretsmanager create-secret --name prod/pipeline/db --secret-string '{"user":"...", "password":"..."}'
aws secretsmanager rotate-secret --secret-id prod/pipeline/db

# Environment validation
python -c "import os; assert os.environ.get('ENVIRONMENT') in ('dev','staging','production'), 'Invalid env'"

# dbt target check in CI
if [ "$GITHUB_REF" = "refs/heads/main" ]; then DBT_TARGET=prod; else DBT_TARGET=dev; fi
dbt run --target $DBT_TARGET

# Secrets in GitHub Actions
# Add: GitHub → Settings → Secrets and variables → Actions → New secret
# Use: ${{ secrets.MY_SECRET_NAME }}
```
