---
title: "Environment Variables - Real-World Production Examples"
topic: bash-scripting
subtopic: environment-variables
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, environment-variables, production, config, deployment]
---

# Environment Variables — Real-World Production Examples

## Pattern 1: Multi-Environment ETL Configuration

```bash
#!/bin/bash
# /opt/etl/config/load_env.sh — called by all pipeline scripts

set -euo pipefail

# Determine environment (default: development for safety)
ENVIRONMENT="${ENVIRONMENT:-development}"

# Base configuration (non-secret, per-environment)
declare -A CONFIG_FILES=(
    [production]="/opt/etl/config/production.env"
    [staging]="/opt/etl/config/staging.env"
    [development]="/opt/etl/config/development.env"
)

config_file="${CONFIG_FILES[$ENVIRONMENT]:-}"
if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
    echo "ERROR: Unknown environment '$ENVIRONMENT' or missing config file" >&2
    exit 1
fi

# Load base config
set -a
source "$config_file"
set +a

# Fetch secrets (production/staging use Secrets Manager, dev uses local file)
if [ "$ENVIRONMENT" = "production" ] || [ "$ENVIRONMENT" = "staging" ]; then
    export DB_PASSWORD=$(aws secretsmanager get-secret-value \
        --secret-id "${ENVIRONMENT}/etl/db_password" \
        --query SecretString --output text)
    export API_KEY=$(aws secretsmanager get-secret-value \
        --secret-id "${ENVIRONMENT}/etl/api_key" \
        --query SecretString --output text)
else
    # Development: secrets in local file (for convenience)
    source /opt/etl/config/.secrets.dev
fi

# Validate required variables
required_vars=(DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME S3_BUCKET)
for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: Required variable $var is not set!" >&2
        exit 1
    fi
done

echo "Environment loaded: $ENVIRONMENT (DB: $DB_HOST, S3: $S3_BUCKET)"
```

Usage in pipeline scripts:
```bash
#!/bin/bash
source /opt/etl/config/load_env.sh
# Now all config is available:
psql "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME" -c "..."
aws s3 sync "$S3_BUCKET/landing/" /data/landing/
```

---

## Pattern 2: Docker-Based Pipeline Configuration

```bash
# Dockerfile:
# FROM python:3.11-slim
# COPY requirements.txt .
# RUN pip install -r requirements.txt
# COPY etl/ /app/etl/
# ENTRYPOINT ["/app/etl/entrypoint.sh"]

# entrypoint.sh (inside container):
#!/bin/bash
set -euo pipefail

echo "Starting ETL container..."
echo "  Environment: ${ENVIRONMENT:-unknown}"
echo "  DB Host: ${DB_HOST:-not set}"
echo "  S3 Bucket: ${S3_BUCKET:-not set}"

# Validate (container won't start without required config)
: "${ENVIRONMENT:?ENVIRONMENT not set}"
: "${DB_HOST:?DB_HOST not set}"
: "${DB_PASSWORD:?DB_PASSWORD not set}"

# Run the pipeline
exec python /app/etl/main.py "$@"

# Run command (production):
# docker run --rm \
#   --env-file /opt/etl/.env.production \
#   -e DB_PASSWORD="$(aws secretsmanager get-secret-value ...)" \
#   etl-pipeline:latest

# Kubernetes deployment:
# envFrom:
#   - configMapRef:
#       name: etl-config  # Non-secrets
#   - secretRef:
#       name: etl-secrets  # Secrets (from K8s secrets or external-secrets operator)
```

---

## Pattern 3: Feature Flags via Environment Variables

```bash
#!/bin/bash
# Simple feature flags — enable/disable pipeline features without code changes

# Feature flags (set in environment):
ENABLE_NEW_TRANSFORM="${ENABLE_NEW_TRANSFORM:-false}"
ENABLE_DATA_QUALITY="${ENABLE_DATA_QUALITY:-true}"
ENABLE_SLACK_ALERTS="${ENABLE_SLACK_ALERTS:-true}"
DEBUG_MODE="${DEBUG_MODE:-false}"

# Use in pipeline:
if [ "$ENABLE_NEW_TRANSFORM" = "true" ]; then
    echo "Using NEW transform logic (v2)"
    python /opt/etl/transform_v2.py
else
    echo "Using stable transform logic (v1)"
    python /opt/etl/transform_v1.py
fi

if [ "$ENABLE_DATA_QUALITY" = "true" ]; then
    python /opt/etl/quality_checks.py || {
        if [ "$ENABLE_SLACK_ALERTS" = "true" ]; then
            curl -sS -X POST "$SLACK_WEBHOOK" -d '{"text":"DQ check failed!"}'
        fi
    }
fi

[ "$DEBUG_MODE" = "true" ] && set -x  # Enable trace for debugging

# Deploy new feature safely:
# 1. Deploy code with ENABLE_NEW_TRANSFORM=false (feature off)
# 2. Test in staging with ENABLE_NEW_TRANSFORM=true
# 3. Enable in production: update .env file → restart → new feature active!
# 4. If broken: set back to false → instant rollback (no code deploy!)
```

---

## Pattern 4: Dynamic Configuration from Database

```bash
#!/bin/bash
# Fetch runtime config from a config table (allows changes without redeploy)

fetch_dynamic_config() {
    # Config stored in a database table:
    # config_table: key VARCHAR, value VARCHAR, updated_at TIMESTAMP
    
    local config_json=$(psql -t -A -c "
        SELECT json_object_agg(key, value) 
        FROM config_table 
        WHERE active = true
    " "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST/$DB_NAME")
    
    # Parse JSON config into env vars:
    export BATCH_SIZE=$(echo "$config_json" | jq -r '.batch_size // "10000"')
    export MAX_RETRIES=$(echo "$config_json" | jq -r '.max_retries // "3"')
    export ALERT_THRESHOLD=$(echo "$config_json" | jq -r '.alert_threshold // "0.05"')
    export FEATURE_X_ENABLED=$(echo "$config_json" | jq -r '.feature_x // "false"')
    
    echo "Dynamic config loaded: batch=$BATCH_SIZE, retries=$MAX_RETRIES"
}

# Load static config (env files):
source /opt/etl/config/load_env.sh

# Load dynamic config (database — can change without redeploy!):
fetch_dynamic_config

# Use:
python /opt/etl/pipeline.py --batch-size=$BATCH_SIZE --max-retries=$MAX_RETRIES
```

---

## Interview Tips

> **Tip 1:** "How do you structure config for a containerized pipeline?" — Container expects env vars at runtime (not baked into image). Non-secrets: ConfigMap (K8s) or --env-file (Docker). Secrets: K8s Secrets (from Vault/AWS) or fetched in entrypoint.sh. Validation in entrypoint: fail fast if required vars are missing (don't start processing with incomplete config).

> **Tip 2:** "Feature flags in bash?" — Simple boolean env vars: `ENABLE_FEATURE=true/false`. Check in script with if/else. Benefits: deploy code with feature disabled → enable via env var change → instant rollback by setting false. No code deploy needed for enable/disable. Good for: rolling out risky changes gradually.

> **Tip 3:** "Static vs dynamic configuration?" — Static (.env files): changes require redeploy/restart. Good for: infrastructure config (DB host, S3 bucket). Dynamic (database table): changes apply immediately on next run. Good for: tuning parameters (batch size, thresholds), feature flags, and config that non-engineers need to adjust.
