---
title: "Cron Jobs - Real-World Production Examples"
topic: bash-scripting
subtopic: cron-jobs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, cron, production, data-engineering, automation]
---

# Cron Jobs — Real-World Production Examples

## Pattern 1: Complete DE Crontab

```bash
# /opt/etl/cron.d/production.cron
# Data Engineering Pipeline Schedule — Production
# Managed via git, deployed by CI/CD

SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin:/opt/etl/bin
HOME=/home/dataeng
SLACK_WEBHOOK=https://hooks.slack.com/services/XXX/YYY/ZZZ

# === INGESTION (frequent, lightweight) ===
*/15 * * * * flock -n /tmp/s3_sync.lock /opt/etl/scripts/s3_sync.sh >> /var/log/etl/s3sync.log 2>&1
*/5 * * * *  /opt/etl/scripts/check_landing_zone.sh >> /var/log/etl/landing_check.log 2>&1

# === TRANSFORMATION (hourly/daily, heavier) ===
0 * * * *   flock -n /tmp/hourly_silver.lock /opt/etl/scripts/hourly_silver.sh >> /var/log/etl/hourly.log 2>&1
0 6 * * *   flock -n /tmp/daily_etl.lock /opt/etl/scripts/daily_gold.sh >> /var/log/etl/daily.log 2>&1
30 6 * * 1  /opt/etl/scripts/weekly_aggregates.sh >> /var/log/etl/weekly.log 2>&1

# === REPORTING (business hours) ===
0 8 * * 1-5 /opt/etl/scripts/morning_report.sh >> /var/log/etl/reports.log 2>&1
0 17 * * 1-5 /opt/etl/scripts/eod_summary.sh >> /var/log/etl/reports.log 2>&1

# === MAINTENANCE (off-hours) ===
0 2 * * *   /opt/etl/scripts/backup_metadata.sh >> /var/log/etl/backup.log 2>&1
0 3 * * *   /opt/etl/scripts/vacuum_old_data.sh >> /var/log/etl/vacuum.log 2>&1
0 4 * * 0   /opt/etl/scripts/weekly_cleanup.sh >> /var/log/etl/cleanup.log 2>&1
30 4 * * *  find /var/log/etl -name "*.log" -mtime +14 -delete

# === MONITORING (always) ===
* * * * *   /opt/monitoring/check_pipeline_health.sh 2>&1 | grep -q "CRITICAL" && curl -sS -X POST "$SLACK_WEBHOOK" -d '{"text":"🚨 Pipeline health critical!"}'
*/5 * * * * /opt/monitoring/check_disk_space.sh >> /var/log/monitoring/disk.log 2>&1
```

---

## Pattern 2: Self-Healing Cron Job

```bash
#!/bin/bash
# /opt/etl/scripts/hourly_silver.sh
# Self-healing: detects issues, attempts auto-fix, falls back to alerting

set -uo pipefail

LOG="/var/log/etl/hourly_$(date +%Y%m%d_%H).log"
exec > >(tee -a "$LOG") 2>&1

log() { echo "[$(date '+%H:%M:%S')] $1"; }

# Pre-flight: check dependencies
check_dependencies() {
    # Database available?
    if ! pg_isready -h analytics-db -p 5432 -t 5 -q; then
        log "WARN: Database unavailable — waiting 60s..."
        sleep 60
        pg_isready -h analytics-db -p 5432 -t 5 -q || return 1
    fi
    
    # Enough disk space? (auto-cleanup if low)
    local avail=$(df /data --output=avail | tail -1)
    if [ $avail -lt 5242880 ]; then  # < 5 GB
        log "WARN: Low disk! Auto-cleaning temp files..."
        find /data/tmp -mtime +1 -delete
        find /var/log/etl -name "*.log" -mtime +7 -delete
        avail=$(df /data --output=avail | tail -1)
        [ $avail -lt 5242880 ] && return 1
    fi
    
    return 0
}

if ! check_dependencies; then
    log "ERROR: Dependencies check failed after auto-fix attempts"
    curl -sS -X POST "$SLACK_WEBHOOK" -d '{"text":"hourly_silver: Dependencies unavailable!"}'
    exit 1
fi

# Main ETL (with retry)
for attempt in 1 2 3; do
    if python /opt/etl/silver_transform.py; then
        log "SUCCESS (attempt $attempt)"
        curl -fsS "https://hc-ping.com/uuid-here" > /dev/null  # Heartbeat
        exit 0
    fi
    log "RETRY: Attempt $attempt failed, waiting $((attempt*30))s..."
    sleep $((attempt * 30))
done

log "FAILED: All 3 attempts exhausted"
curl -sS -X POST "$SLACK_WEBHOOK" -d '{"text":"🚨 hourly_silver FAILED after 3 retries!"}'
exit 1
```

---

## Pattern 3: Cron-Based Pipeline Orchestration

```bash
#!/bin/bash
# Simple DAG: ingest → validate → transform → report
# Dependencies via marker files + timeout waiting

TODAY=$(date +%Y%m%d)
MARKERS="/tmp/pipeline_markers/$TODAY"
mkdir -p "$MARKERS"

step_ingest() {
    python /opt/etl/ingest.py && touch "$MARKERS/ingest_done"
}

step_validate() {
    wait_for_marker "ingest_done" 1800  # Wait up to 30 min
    python /opt/etl/validate.py && touch "$MARKERS/validate_done"
}

step_transform() {
    wait_for_marker "validate_done" 1800
    python /opt/etl/transform.py && touch "$MARKERS/transform_done"
}

step_report() {
    wait_for_marker "transform_done" 1800
    python /opt/etl/report.py && touch "$MARKERS/report_done"
}

wait_for_marker() {
    local marker="$MARKERS/$1" timeout="$2" waited=0
    while [ ! -f "$marker" ]; do
        sleep 30; waited=$((waited + 30))
        [ $waited -ge $timeout ] && { echo "TIMEOUT waiting for $1"; exit 1; }
    done
}

# Cron entries (staggered, each waits for previous):
# 0 6 * * * /opt/etl/pipeline.sh step_ingest
# 5 6 * * * /opt/etl/pipeline.sh step_validate
# 10 6 * * * /opt/etl/pipeline.sh step_transform
# 15 6 * * * /opt/etl/pipeline.sh step_report

# Execute requested step:
"$1"
```

---

## Pattern 4: Cron Monitoring Dashboard Data

```bash
#!/bin/bash
# Emit cron job status to a JSON file (consumed by dashboard/monitoring)

STATUS_FILE="/var/www/status/cron_status.json"

generate_status() {
    echo "{"
    echo "  \"generated_at\": \"$(date -Iseconds)\","
    echo "  \"jobs\": ["
    
    first=true
    for log_file in /var/log/etl/*.log; do
        job_name=$(basename "$log_file" .log | sed 's/_[0-9]*$//')
        last_line=$(tail -1 "$log_file" 2>/dev/null || echo "")
        last_modified=$(stat -c %Y "$log_file" 2>/dev/null || echo 0)
        age_minutes=$(( ($(date +%s) - last_modified) / 60 ))
        
        # Determine status from last log line
        if echo "$last_line" | grep -qi "success\|complete\|done"; then
            status="healthy"
        elif echo "$last_line" | grep -qi "error\|fail"; then
            status="failed"
        elif [ $age_minutes -gt 120 ]; then
            status="stale"
        else
            status="unknown"
        fi
        
        $first || echo ","
        first=false
        echo "    {\"name\":\"$job_name\",\"status\":\"$status\",\"age_min\":$age_minutes}"
    done
    
    echo "  ]"
    echo "}"
}

generate_status > "$STATUS_FILE"
# Dashboard reads this JSON file every minute
# Shows: job name, status (green/red/yellow), time since last run
```

---

## Interview Tips

> **Tip 1:** "Show me a production-ready crontab" — Organized by: frequency (frequent ingestion at top, maintenance at bottom), with: flock on every job (prevent overlap), output redirected to dated logs, PATH set globally, alerts on critical failures, and comments explaining each entry. Deploy from git (never edit crontab manually).

> **Tip 2:** "How do you implement self-healing cron jobs?" — Pre-flight dependency checks (DB reachable? disk space?). Auto-fix common issues (clean temp files if disk low, retry if DB temporarily unavailable). Only alert after auto-fix attempts fail. Pattern: check → auto-fix → retry → alert. This reduces noise (only alert on problems that need human intervention).

> **Tip 3:** "When do you outgrow cron?" — When you need: job dependencies (A must finish before B starts), parameterized runs (backfill with date argument), visibility (who ran what, when, with what result), retry with backoff, or dynamic DAGs. At that point: migrate to Airflow/Dagster. But for 5-10 independent scheduled scripts: cron is perfectly fine and much simpler to operate.
