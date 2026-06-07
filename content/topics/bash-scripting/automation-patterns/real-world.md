---
title: "Automation Patterns - Real-World Production Examples"
topic: bash-scripting
subtopic: automation-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, automation, production, patterns, complete-examples]
---

# Bash Automation Patterns — Real-World Production Examples

## Pattern 1: Complete Daily Pipeline Automation

```bash
#!/bin/bash
# /opt/etl/daily_pipeline.sh — Full production pipeline
# Cron: 0 6 * * * flock -n /tmp/daily.lock /opt/etl/daily_pipeline.sh
set -euo pipefail

source /opt/etl/lib/utils.sh
source /opt/etl/config/load_env.sh

LOG="/var/log/etl/daily_$(date +%Y%m%d).log"
exec > >(tee -a "$LOG") 2>&1
trap 'alert "Daily pipeline FAILED at step: $CURRENT_STEP" "critical"' ERR
CURRENT_STEP="init"

log_info "=== Daily Pipeline: $(date +%Y-%m-%d) ==="

# Pre-flight
CURRENT_STEP="preflight"
require_commands python psql aws
retry 3 10 pg_isready -h "$DB_HOST" -t 5
disk_check "/data" 85

# Ingest from S3
CURRENT_STEP="ingest"
log_info "Downloading from S3..."
timer_start ingest
aws s3 sync "s3://$S3_BUCKET/landing/$(date -d yesterday +%Y/%m/%d)/" /data/landing/ --quiet
log_info "Ingest complete ($(timer_stop ingest)s, $(find /data/landing -type f | wc -l) files)"

# Validate
CURRENT_STEP="validate"
for f in /data/landing/*.csv; do
    [ -f "$f" ] || continue
    validate_csv "$f" 8 100 || { log_error "Validation failed: $f"; exit 1; }
done
log_info "All files valid ✓"

# Transform
CURRENT_STEP="transform"
timer_start transform
python /opt/etl/transform.py --input=/data/landing --output=/data/output
log_info "Transform complete ($(timer_stop transform)s)"

# Load
CURRENT_STEP="load"
timer_start load
retry 3 10 python /opt/etl/load_to_db.py --input=/data/output
row_count=$(psql -t -c "SELECT COUNT(*) FROM silver.orders WHERE load_date = CURRENT_DATE" | tr -d ' ')
log_info "Load complete: $row_count rows ($(timer_stop load)s)"

# Verify
CURRENT_STEP="verify"
[ "$row_count" -gt 1000 ] || { alert "Low row count: $row_count" "warning"; }

# Archive + cleanup
CURRENT_STEP="archive"
mv /data/landing/*.csv "/data/archive/$(date +%Y%m%d)/"
find /data/archive -mtime +30 -delete

log_info "=== Pipeline Complete ($row_count rows) ==="
curl -fsS "https://hc-ping.com/${HC_UUID}" > /dev/null  # Heartbeat
```

---

## Pattern 2: Multi-Server Orchestration

```bash
#!/bin/bash
# Orchestrate ETL across 3 servers (ingestion, transform, reporting)
set -euo pipefail

SERVERS=(
    "ingest:etl-server-01:ingest_pipeline.sh"
    "transform:etl-server-02:transform_pipeline.sh"
    "report:etl-server-03:generate_reports.sh"
)

log() { echo "[$(date '+%H:%M:%S')] $1"; }

run_remote() {
    local server="$1" script="$2"
    ssh -o ConnectTimeout=10 "$server" "bash /opt/etl/scripts/$script" 2>&1
}

# Execute sequentially (each depends on previous):
for entry in "${SERVERS[@]}"; do
    IFS=':' read -r name server script <<< "$entry"
    
    log "Starting: $name on $server"
    if run_remote "$server" "$script"; then
        log "  ✓ $name complete"
    else
        log "  ✗ $name FAILED!"
        alert "Multi-server pipeline failed at: $name ($server)"
        exit 1
    fi
done

log "All servers complete! ✓"
```

---

## Pattern 3: Data Quality Alert System

```bash
#!/bin/bash
# Continuous DQ monitoring (runs every 5 minutes via cron)
set -uo pipefail

CHECKS=(
    "Orders freshness|SELECT EXTRACT(EPOCH FROM NOW() - MAX(created_at))/60 FROM silver.orders|threshold:30"
    "Orders row count|SELECT COUNT(*) FROM silver.orders WHERE load_date = CURRENT_DATE|min:1000"
    "Null rate orders|SELECT COUNT(*) FILTER (WHERE customer_id IS NULL) * 100.0 / COUNT(*) FROM silver.orders WHERE load_date = CURRENT_DATE|threshold:5"
    "Events lag|SELECT EXTRACT(EPOCH FROM NOW() - MAX(event_time))/60 FROM silver.events|threshold:15"
)

issues=()

for check_spec in "${CHECKS[@]}"; do
    IFS='|' read -r name query condition <<< "$check_spec"
    value=$(psql -t -A -c "$query" 2>/dev/null | tr -d ' ')
    
    case "$condition" in
        threshold:*)
            max="${condition#*:}"
            if (( $(echo "$value > $max" | bc -l) )); then
                issues+=("$name: $value (max: $max)")
            fi
            ;;
        min:*)
            min="${condition#*:}"
            if [ "${value:-0}" -lt "$min" ]; then
                issues+=("$name: $value (min: $min)")
            fi
            ;;
    esac
done

if [ ${#issues[@]} -gt 0 ]; then
    msg="⚠️ Data Quality Issues:\n$(printf '• %s\n' "${issues[@]}")"
    curl -sS -X POST "$SLACK_WEBHOOK" -d "{\"text\":\"$msg\"}" > /dev/null 2>&1
fi
```

---

## Pattern 4: GitOps Deployment

```bash
#!/bin/bash
# Automated deployment triggered by git push (called from CI/CD)
set -euo pipefail

DEPLOY_ENV="${1:-staging}"
GIT_SHA=$(git rev-parse --short HEAD)
DEPLOY_DIR="/opt/etl"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

log() { echo "[deploy] $1"; }

log "Deploying $GIT_SHA to $DEPLOY_ENV"

# Pre-deploy: run tests
log "Running tests..."
python -m pytest tests/ -q || { log "Tests FAILED — aborting deploy!"; exit 1; }

# Create release tag
git tag "deploy-${DEPLOY_ENV}-${TIMESTAMP}" && git push --tags

# Deploy based on environment
case "$DEPLOY_ENV" in
    staging)
        log "Deploying to staging..."
        ssh etl-staging "cd /opt/etl && git pull origin main"
        ssh etl-staging "systemctl restart etl-watcher"
        # Run smoke test
        ssh etl-staging "python /opt/etl/tests/smoke_test.py"
        ;;
    production)
        log "Deploying to production (with approval)..."
        read -p "Deploy to PRODUCTION? (yes/no): " confirm
        [ "$confirm" = "yes" ] || { log "Aborted."; exit 0; }
        
        ssh etl-prod "cd /opt/etl && git fetch && git checkout deploy-${DEPLOY_ENV}-${TIMESTAMP}"
        ssh etl-prod "pip install -r requirements.txt --quiet"
        ssh etl-prod "systemctl restart etl-watcher"
        
        log "Production deployed! Monitoring for 5 minutes..."
        sleep 300
        if ssh etl-prod "systemctl is-active etl-watcher"; then
            log "✓ Service healthy after deploy"
        else
            log "✗ Service unhealthy! Rolling back..."
            ssh etl-prod "git checkout deploy-${DEPLOY_ENV}-$(git tag -l 'deploy-production-*' | sort | tail -2 | head -1)"
            ssh etl-prod "systemctl restart etl-watcher"
            alert "Production deploy rolled back!" "critical"
            exit 1
        fi
        ;;
esac

log "Deploy complete: $GIT_SHA → $DEPLOY_ENV ✓"
```

---

## Interview Tips

> **Tip 1:** "Design a complete production pipeline in bash" — Template: source libs → load config → pre-flight checks → ingest (with retry) → validate → transform (timed) → load (with retry) → verify (row count) → archive → heartbeat ping. Error handling via ERR trap + context variable. Log everything. Alert on failure. Heartbeat confirms success.

> **Tip 2:** "How do you automate data quality monitoring?" — Define checks as: name + SQL query + threshold (in a config array). Loop through, execute each, compare result to threshold. Collect all violations, alert once with all issues (not per-check spam). Run every 5 minutes via cron. Catches issues within 5 minutes of occurrence.

> **Tip 3:** "How do you do GitOps for ETL?" — Code in git (source of truth). CI runs tests on every push. Staging: auto-deploy on merge to main + smoke test. Production: manual approval + deploy + monitor for 5 min + auto-rollback if unhealthy. Tag each deploy for easy rollback. Same pattern as software engineering (because ETL IS software!).
