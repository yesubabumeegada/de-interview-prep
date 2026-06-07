---
title: "Environment Variables - Scenario Questions"
topic: bash-scripting
subtopic: environment-variables
content_type: scenario_question
tags: [bash, environment-variables, interview, scenarios]
---

# Scenario Questions — Environment Variables

<article data-difficulty="junior">

## 🟢 Junior: Basic Environment Configuration

**Scenario:** Your ETL script needs: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, and S3_BUCKET. Write a script that loads these from a .env file, validates all are set, and fails with a clear message if any are missing.

<details>
<summary>💡 Hint</summary>
Source the .env file, then loop through required vars checking if each is non-empty. Print which ones are missing.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

# Load .env file
ENV_FILE="/opt/etl/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found: $ENV_FILE"
    echo "Copy .env.example to .env and fill in values."
    exit 1
fi
source "$ENV_FILE"

# Validate required variables
REQUIRED_VARS=(DB_HOST DB_PORT DB_USER DB_PASSWORD S3_BUCKET)
MISSING=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var:-}" ]; then
        MISSING+=("$var")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables:"
    for var in "${MISSING[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Please set them in $ENV_FILE"
    exit 1
fi

# All good — show config (mask password!)
echo "Configuration loaded:"
echo "  DB: $DB_USER@$DB_HOST:$DB_PORT"
echo "  S3: $S3_BUCKET"
echo "  Password: ****" # Never print passwords!

# Continue with pipeline...
python /opt/etl/run_pipeline.py
```

**Key Points:**
- `source .env` loads variables into current shell
- `${!var:-}` dereferences variable name dynamically (indirect expansion)
- Collect ALL missing vars before failing (don't fail on first — show all missing at once)
- Never print passwords in logs (mask them)
- Provide actionable error message (where to set the values)
- `set -euo pipefail`: strict mode catches undefined var usage immediately

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Environment Config

**Scenario:** Your pipeline runs in production, staging, and development. Each has different DB hosts and S3 buckets. Design a config system where: same script works in all environments, determined by a single ENVIRONMENT variable.

<details>
<summary>💡 Hint</summary>
Create per-environment .env files (production.env, staging.env, development.env). Load the right one based on $ENVIRONMENT. Default to development for safety.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

# Determine environment (default: development — safest default!)
ENVIRONMENT="${ENVIRONMENT:-development}"

# Load environment-specific config
ENV_FILE="/opt/etl/config/${ENVIRONMENT}.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: Config file not found for environment '$ENVIRONMENT': $ENV_FILE"
    echo "Available: $(ls /opt/etl/config/*.env 2>/dev/null | xargs -I{} basename {} .env | tr '\n' ' ')"
    exit 1
fi

set -a  # Auto-export all sourced variables
source "$ENV_FILE"
set +a

# Fetch secrets (production uses AWS, dev uses local)
if [ "$ENVIRONMENT" = "production" ] || [ "$ENVIRONMENT" = "staging" ]; then
    export DB_PASSWORD=$(aws secretsmanager get-secret-value \
        --secret-id "${ENVIRONMENT}/db/password" --query SecretString --output text)
else
    # Dev: password in local file (OK for development only!)
    export DB_PASSWORD="${DB_PASSWORD:-devpassword123}"
fi

# Validate
: "${DB_HOST:?DB_HOST not set for $ENVIRONMENT}"
: "${S3_BUCKET:?S3_BUCKET not set for $ENVIRONMENT}"

echo "[$ENVIRONMENT] Pipeline starting — DB: $DB_HOST, S3: $S3_BUCKET"

# --- production.env: ---
# DB_HOST=prod-db.internal.company.com
# DB_PORT=5432
# DB_USER=etl_production
# DB_NAME=analytics
# S3_BUCKET=s3://company-prod-lake

# --- staging.env: ---
# DB_HOST=staging-db.internal.company.com
# DB_PORT=5432
# DB_USER=etl_staging
# DB_NAME=analytics_staging
# S3_BUCKET=s3://company-staging-lake

# --- development.env: ---
# DB_HOST=localhost
# DB_PORT=5432
# DB_USER=dev_user
# DB_NAME=dev_db
# S3_BUCKET=s3://dev-personal-bucket
```

**Key Points:**
- One ENVIRONMENT variable controls everything (single switch)
- Default to development (safe — production requires explicit opt-in)
- Per-env .env files: same variable names, different values
- Secrets: production fetches from secrets manager, dev uses local defaults
- `set -a` / `set +a`: auto-exports all variables sourced between them
- Validation after loading (catch missing config immediately, not mid-pipeline)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Secrets Management Architecture

**Scenario:** Your data platform has 15 pipelines across 3 servers, each needing DB credentials, API keys, and tokens. Currently: passwords are in .env files on each server (security risk, hard to rotate). Design a secure secrets management solution using AWS Secrets Manager.

<details>
<summary>💡 Hint</summary>
Centralize secrets in AWS Secrets Manager. Scripts fetch at runtime (never stored on disk). Cache with TTL to reduce API calls. Handle rotation gracefully. IAM roles per server (least privilege).
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# /opt/etl/lib/secrets.sh — reusable secrets library

SECRETS_CACHE_DIR="/tmp/.secrets_cache"
SECRETS_CACHE_TTL=1800  # 30 minutes

# Initialize cache directory (secure permissions)
init_secrets() {
    mkdir -p "$SECRETS_CACHE_DIR"
    chmod 700 "$SECRETS_CACHE_DIR"
}

# Fetch a secret (with caching)
get_secret() {
    local secret_id="$1"
    local cache_file="$SECRETS_CACHE_DIR/$(echo "$secret_id" | md5sum | cut -d' ' -f1)"
    
    # Return from cache if fresh
    if [ -f "$cache_file" ]; then
        local age=$(( $(date +%s) - $(stat -c %Y "$cache_file") ))
        if [ $age -lt $SECRETS_CACHE_TTL ]; then
            cat "$cache_file"
            return 0
        fi
    fi
    
    # Fetch from Secrets Manager
    local value
    value=$(aws secretsmanager get-secret-value \
        --secret-id "$secret_id" \
        --query SecretString --output text 2>/dev/null) || {
        # Fetch failed — try stale cache as fallback
        [ -f "$cache_file" ] && { cat "$cache_file"; return 0; }
        echo "ERROR: Cannot fetch secret: $secret_id" >&2
        return 1
    }
    
    # Update cache
    echo "$value" > "$cache_file"
    chmod 600 "$cache_file"
    echo "$value"
}

# Load all pipeline secrets
load_pipeline_secrets() {
    local env="${ENVIRONMENT:-production}"
    init_secrets
    
    export DB_PASSWORD=$(get_secret "${env}/etl/db_password")
    export API_KEY=$(get_secret "${env}/etl/api_key")
    export SLACK_WEBHOOK=$(get_secret "${env}/notifications/slack_webhook")
    
    # Validate
    [ -z "$DB_PASSWORD" ] && { echo "FATAL: DB_PASSWORD not available!"; return 1; }
    [ -z "$API_KEY" ] && { echo "FATAL: API_KEY not available!"; return 1; }
}

# Cleanup (call at script end)
cleanup_secrets() {
    unset DB_PASSWORD API_KEY SLACK_WEBHOOK
    # Optionally clear cache: rm -rf "$SECRETS_CACHE_DIR"
}

# --- ARCHITECTURE: ---
# AWS Secrets Manager:
#   production/etl/db_password → rotated every 30 days (automatic!)
#   production/etl/api_key → rotated on demand
#   staging/etl/db_password → different credentials for staging
#
# IAM Roles (least privilege):
#   Server 1 (ingestion): can read production/etl/* secrets
#   Server 2 (transform): can read production/etl/* and production/warehouse/*
#   Server 3 (reporting): can read production/reporting/* only
#
# Rotation handling:
#   Secret rotates → cache expires (30 min TTL) → next fetch gets new value
#   If connection fails → invalidate cache → re-fetch → retry with new creds
#
# COST: AWS Secrets Manager: $0.40/secret/month + $0.05/10K API calls
#   15 secrets × $0.40 = $6/month (negligible!)
#   Caching reduces API calls: ~100/day (not 15 × 1000/day without cache)

# Usage in pipeline scripts:
source /opt/etl/lib/secrets.sh
load_pipeline_secrets || exit 1
trap cleanup_secrets EXIT

# ... pipeline logic using $DB_PASSWORD, $API_KEY ...
```

**Key Points:**
- Centralized: all secrets in AWS Secrets Manager (single source of truth)
- Never on disk: fetched at runtime, cached in /tmp with 700 permissions
- Caching: reduces API calls (30-min TTL → max 48 fetches/day per secret)
- Rotation-safe: cache expires → fresh fetch gets rotated credentials automatically
- Fallback: if API fails → use stale cache (prevents outage if Secrets Manager is briefly unavailable)
- Cleanup: unset secrets from environment on script exit (trap EXIT)
- Least privilege: IAM roles per server limit which secrets each can access
- Cost: ~$6/month for 15 secrets (vs risk of leaked .env files = priceless!)

</details>

</article>
