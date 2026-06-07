---
title: "Environment Variables - Intermediate"
topic: bash-scripting
subtopic: environment-variables
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, environment-variables, secrets, config-management, twelve-factor]
---

# Environment Variables — Intermediate

## Twelve-Factor App Configuration

```bash
# The Twelve-Factor App methodology: "Store config in the environment"
# Same code in dev/staging/prod — behavior controlled by env vars

#!/bin/bash
# Pipeline that adapts to its environment:

# Required config (fail if missing):
: "${ENVIRONMENT:?Must set ENVIRONMENT (production|staging|development)}"
: "${DB_HOST:?Must set DB_HOST}"
: "${S3_BUCKET:?Must set S3_BUCKET}"

# Optional config with sensible defaults:
BATCH_SIZE="${BATCH_SIZE:-10000}"
MAX_RETRIES="${MAX_RETRIES:-3}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
PARALLELISM="${PARALLELISM:-4}"

echo "Config:"
echo "  Environment: $ENVIRONMENT"
echo "  Database: $DB_HOST"
echo "  S3 Bucket: $S3_BUCKET"
echo "  Batch Size: $BATCH_SIZE"
echo "  Parallelism: $PARALLELISM"

# Same script, different behavior per environment!
# Production: DB_HOST=prod-db, BATCH_SIZE=50000, PARALLELISM=8
# Development: DB_HOST=localhost, BATCH_SIZE=100, PARALLELISM=1
```

---

## Secret Management Patterns

### AWS Secrets Manager Integration

```bash
#!/bin/bash
# Fetch secrets at runtime (never stored on disk!)

fetch_secret() {
    local secret_name="$1"
    aws secretsmanager get-secret-value \
        --secret-id "$secret_name" \
        --query 'SecretString' \
        --output text 2>/dev/null
}

# Fetch DB credentials at runtime:
export DB_PASSWORD=$(fetch_secret "prod/etl/db_password")
export API_KEY=$(fetch_secret "prod/etl/api_key")

# Validate:
[ -z "$DB_PASSWORD" ] && { echo "ERROR: Failed to fetch DB_PASSWORD from Secrets Manager"; exit 1; }

# Use in pipeline:
psql "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME" \
    -c "SELECT COUNT(*) FROM orders"

# SECURITY: unset after use (remove from memory)
unset DB_PASSWORD API_KEY
```

### HashiCorp Vault Integration

```bash
#!/bin/bash
# Fetch dynamic secrets from Vault (auto-rotating!)

VAULT_ADDR="https://vault.internal:8200"
VAULT_TOKEN="${VAULT_TOKEN}"

# Get database credentials (Vault generates them dynamically!):
creds=$(vault read -format=json database/creds/etl-role)
export DB_USER=$(echo $creds | jq -r '.data.username')
export DB_PASSWORD=$(echo $creds | jq -r '.data.password')
export DB_LEASE_ID=$(echo $creds | jq -r '.lease_id')

# Use credentials:
python /opt/etl/run_pipeline.py

# Revoke credentials after use (security best practice):
vault lease revoke "$DB_LEASE_ID"
unset DB_USER DB_PASSWORD DB_LEASE_ID
```

---

## .env File Management

```bash
# Structure for multi-environment project:
# /opt/etl/
# ├── .env.example          (template — committed to git)
# ├── .env.production       (real values — NOT in git)
# ├── .env.staging          (real values — NOT in git)
# └── .env.development      (real values — NOT in git)

# .env.example (committed to git as documentation):
# Database Configuration
DB_HOST=
DB_PORT=5432
DB_USER=
DB_PASSWORD=
DB_NAME=

# AWS Configuration
S3_BUCKET=
AWS_REGION=us-east-1

# Pipeline Configuration
BATCH_SIZE=10000
LOG_LEVEL=INFO

# Loading the right .env file:
ENV_FILE="/opt/etl/.env.${ENVIRONMENT:-development}"
if [ -f "$ENV_FILE" ]; then
    set -a  # Auto-export all variables set below
    source "$ENV_FILE"
    set +a
else
    echo "WARNING: $ENV_FILE not found, using defaults"
fi

# Secure .env file handling:
# .gitignore:
# .env.*
# !.env.example

# File permissions:
chmod 600 /opt/etl/.env.*  # Only owner can read/write
chown etl_user:etl_group /opt/etl/.env.*
```

---

## Docker and Container Environments

```bash
# Passing env vars to Docker containers:

# Method 1: Individual -e flags
docker run -e DB_HOST=prod-db -e DB_PORT=5432 etl-image:latest

# Method 2: .env file
docker run --env-file /opt/etl/.env.production etl-image:latest

# Method 3: Docker Compose
# docker-compose.yml:
# services:
#   etl:
#     image: etl-image:latest
#     env_file: .env.production
#     environment:
#       - EXTRA_VAR=value  # Override specific vars

# Kubernetes:
# ConfigMap for non-secret config:
# kubectl create configmap etl-config --from-env-file=.env.production
# Secret for sensitive values:
# kubectl create secret generic etl-secrets --from-literal=DB_PASSWORD=xxx

# Inside container: variables are automatically available
echo "DB_HOST=$DB_HOST"  # Set by Docker/K8s runtime

# IMPORTANT: Don't bake secrets into Docker images!
# BAD: COPY .env /app/.env (secrets in image layer forever!)
# GOOD: Pass at runtime via -e or --env-file
```

---

## Variable Manipulation

```bash
# String operations on variables:
FILE="/data/landing/orders_2024-03-15.csv"

echo "${FILE##*/}"         # orders_2024-03-15.csv (basename — remove path)
echo "${FILE%.*}"          # /data/landing/orders_2024-03-15 (remove extension)
echo "${FILE%.csv}.json"   # /data/landing/orders_2024-03-15.json (change extension)
echo "${FILE/landing/archive}"  # /data/archive/orders_2024-03-15.csv (substitute)

# Extract parts:
DATE="${FILE##*_}"         # 2024-03-15.csv
DATE="${DATE%.csv}"        # 2024-03-15
YEAR="${DATE%%-*}"         # 2024

# Default values with different behaviors:
echo "${VAR:-default}"     # Use default if VAR unset OR empty
echo "${VAR-default}"      # Use default if VAR unset (but NOT if empty)
echo "${VAR:=default}"     # Set VAR to default if unset or empty (and return it)
echo "${VAR:+alternate}"   # Use alternate if VAR IS set (useful for optional flags)

# Length:
echo "${#VAR}"             # Length of VAR's value

# Uppercase/lowercase (bash 4+):
echo "${VAR^^}"            # UPPERCASE
echo "${VAR,,}"            # lowercase
```

---

## Interview Tips

> **Tip 1:** "How do you implement 12-factor config?" — All configuration via environment variables (never in code). Same Docker image/script works in dev/staging/prod — only env vars differ. Required vars: fail fast with `:?` syntax. Optional vars: provide defaults with `:-`. Load from .env files that are environment-specific and gitignored.

> **Tip 2:** "How do you handle secrets securely?" — Never in git, never hardcoded. Options: (1) .env files (chmod 600, gitignored) for simple setups, (2) AWS Secrets Manager / Vault for production (fetch at runtime, auto-rotating). Extra: unset after use, disable trace (set +x) before handling, use env vars not CLI args (ps aux shows args).

> **Tip 3:** "Variable scope confusion?" — `VAR=x` without export: only current shell sees it. `export VAR=x`: child processes inherit it. Subshells: variables set inside `(...)` don't escape to parent. Docker: pass with `-e` or `--env-file` (not available automatically from host!). Cron: minimal environment (must explicitly set PATH, source .env files).
