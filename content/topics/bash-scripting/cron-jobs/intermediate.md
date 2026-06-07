---
title: "Cron Jobs - Intermediate"
topic: bash-scripting
subtopic: cron-jobs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, cron, scheduling, monitoring, alerting, systemd-timers]
---

# Cron Jobs — Intermediate

## Advanced Scheduling Patterns

```bash
# Run at specific minutes past each hour (stagger jobs to avoid spike):
5 * * * *  /opt/etl/job_a.sh    # :05 past every hour
20 * * * * /opt/etl/job_b.sh    # :20 past every hour
35 * * * * /opt/etl/job_c.sh    # :35 past every hour
50 * * * * /opt/etl/job_d.sh    # :50 past every hour
# Spreads load evenly across the hour (no "minute 0" stampede!)

# Business hours only (8 AM - 6 PM, weekdays):
*/15 8-17 * * 1-5 /opt/etl/frequent_update.sh

# Skip holidays (check before running):
0 6 * * * [ "$(date +%m%d)" != "1225" ] && /opt/etl/daily.sh
# Won't run on Christmas Day (crude but effective!)

# Run on last day of month (no direct cron syntax for this):
0 6 28-31 * * [ "$(date -d tomorrow +%d)" == "01" ] && /opt/etl/month_end.sh
# Only runs if TOMORROW is the 1st (meaning today is the last day!)

# Different schedules for different environments:
# Production: every hour
# Staging: every 4 hours (less compute cost)
# Development: manual only (no cron entry)
```

---

## Cron Job with Full Error Handling

```bash
#!/bin/bash
# /opt/etl/robust_daily_etl.sh — called by cron at 6 AM

set -euo pipefail

JOB_NAME="daily_orders_etl"
LOG_DIR="/var/log/etl"
LOG_FILE="$LOG_DIR/${JOB_NAME}_$(date +%Y%m%d).log"
LOCK_FILE="/tmp/${JOB_NAME}.lock"
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL}"
MAX_RUNTIME=7200  # 2 hours

# Setup logging
mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }
alert() { curl -sS -X POST "$SLACK_WEBHOOK" -H 'Content-type: application/json' -d "{\"text\":\"$1\"}" > /dev/null 2>&1 || true; }

# Prevent overlap
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    log "SKIP: Previous run still active"
    exit 0
fi

# Cleanup on exit
trap 'rm -f "$LOCK_FILE"' EXIT

# Pre-flight checks
log "=== $JOB_NAME started ==="
if ! pg_isready -h db-prod -p 5432 -q; then
    log "ERROR: Database unavailable!"
    alert "🚨 $JOB_NAME: Database unreachable — skipping run"
    exit 1
fi

# Execute with timeout
start=$(date +%s)
if timeout $MAX_RUNTIME python /opt/etl/daily_orders.py; then
    duration=$(( $(date +%s) - start ))
    log "SUCCESS: Completed in ${duration}s"
    [ $duration -gt 3600 ] && alert "⚠️ $JOB_NAME: Took ${duration}s (SLA warning: >1hr)"
else
    code=$?
    log "FAILED: Exit code $code"
    [ $code -eq 124 ] && alert "🚨 $JOB_NAME: TIMEOUT after ${MAX_RUNTIME}s" \
                       || alert "🚨 $JOB_NAME: FAILED (exit $code). Log: $LOG_FILE"
    exit $code
fi
```

Crontab entry:
```
0 6 * * * /opt/etl/robust_daily_etl.sh
```

---

## Systemd Timers (Modern Alternative to Cron)

```ini
# /etc/systemd/system/daily-etl.service
[Unit]
Description=Daily Orders ETL Pipeline
After=network.target postgresql.service

[Service]
Type=oneshot
User=dataeng
ExecStart=/opt/etl/daily_pipeline.sh
StandardOutput=journal
StandardError=journal
TimeoutStartSec=7200
Restart=on-failure
RestartSec=300

# /etc/systemd/system/daily-etl.timer
[Unit]
Description=Run Daily ETL at 6 AM

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
# Enable and start:
sudo systemctl enable daily-etl.timer
sudo systemctl start daily-etl.timer

# Check status:
systemctl list-timers --all
systemctl status daily-etl.service  # Last run result

# View logs:
journalctl -u daily-etl.service --since today

# Advantages over cron:
# - Automatic retry on failure (Restart=on-failure)
# - Dependency management (After=postgresql.service)
# - Centralized logging (journalctl)
# - Timeout enforcement (TimeoutStartSec)
# - Persistent=true: runs missed executions after reboot
```

---

## Monitoring Cron Job Health

```bash
#!/bin/bash
# /opt/monitoring/check_cron_health.sh — runs every 5 minutes

EXPECTED_JOBS=(
    "daily_orders_etl:0 6 * * *:7200"   # name:schedule:max_age_seconds
    "hourly_ingest:0 * * * *:7200"
    "s3_sync:*/15 * * * *:1800"
)

for job_spec in "${EXPECTED_JOBS[@]}"; do
    IFS=':' read -r name schedule max_age <<< "$job_spec"
    log_file="/var/log/etl/${name}_$(date +%Y%m%d).log"
    
    # Check if log exists and was updated recently
    if [ -f "$log_file" ]; then
        last_modified=$(stat -c %Y "$log_file")
        now=$(date +%s)
        age=$((now - last_modified))
        
        if [ $age -gt $max_age ]; then
            echo "⚠️ $name: log is ${age}s old (max: ${max_age}s) — job may have stopped!"
        fi
        
        # Check for errors in recent output
        if tail -5 "$log_file" | grep -qi "error\|failed\|exception"; then
            echo "❌ $name: errors detected in recent log output"
        fi
    else
        echo "❌ $name: no log file for today — job hasn't run?"
    fi
done
```

---

## Cron Job Deployment (Infrastructure as Code)

```bash
#!/bin/bash
# Deploy cron jobs from a config file (version-controlled, reproducible)

CRON_CONFIG="/opt/etl/config/crontab.conf"

# crontab.conf (version-controlled in git):
cat > "$CRON_CONFIG" << 'EOF'
# Data Engineering Pipeline Cron Jobs
# Managed by: deploy_crons.sh — DO NOT EDIT MANUALLY
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
HOME=/home/dataeng

# === INGESTION ===
*/15 * * * * flock -n /tmp/s3sync.lock /opt/etl/s3_sync.sh >> /var/log/etl/s3sync.log 2>&1
0 * * * *   flock -n /tmp/hourly.lock /opt/etl/hourly_ingest.sh >> /var/log/etl/hourly.log 2>&1

# === TRANSFORMATION ===
0 6 * * *   flock -n /tmp/daily.lock /opt/etl/daily_transform.sh >> /var/log/etl/daily.log 2>&1
30 6 * * *  /opt/etl/build_gold.sh >> /var/log/etl/gold.log 2>&1

# === MAINTENANCE ===
0 2 * * *   /opt/scripts/backup_db.sh >> /var/log/backups/backup.log 2>&1
0 3 * * 0   find /data/archive -mtime +30 -delete >> /var/log/cleanup.log 2>&1
0 4 * * *   find /var/log/etl -name "*.log" -mtime +14 -delete

# === MONITORING ===
*/5 * * * * /opt/monitoring/health_check.sh >> /var/log/monitoring/health.log 2>&1
EOF

# Install crontab:
crontab "$CRON_CONFIG"
echo "Cron jobs deployed: $(crontab -l | grep -v '^#' | grep -v '^$' | wc -l) active entries"
```

---

## Interview Tips

> **Tip 1:** "How do you make cron jobs production-ready?" — Five essentials: (1) flock (prevent overlap), (2) output redirect to log file (audit trail), (3) absolute paths (avoid PATH issues), (4) timeout (prevent infinite runs), (5) failure alerting (Slack/email on exit code ≠ 0). Without these: cron jobs fail silently, overlap, and go unnoticed for days.

> **Tip 2:** "Cron vs systemd timers?" — Cron: one-line setup, universal (all Linux), but no retry, no deps, no monitoring. Systemd timers: built-in retry (Restart=on-failure), dependency ordering (After=), centralized logging (journalctl), catch-up after reboot (Persistent=true). Use cron for simple tasks; systemd timers for critical services needing reliability.

> **Tip 3:** "How do you manage cron jobs across multiple servers?" — Version control: crontab config file in git, deployed by automation (Ansible/Chef/bash script). Never edit crontab manually in production! Deploy from a single source of truth. This gives: audit trail, rollback capability, consistency across servers, and code review for schedule changes.
