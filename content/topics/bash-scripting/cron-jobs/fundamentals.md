---
title: "Cron Jobs - Fundamentals"
topic: bash-scripting
subtopic: cron-jobs
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, cron, scheduling, automation, linux, data-engineering]
---

# Cron Jobs — Fundamentals

## What Is Cron?

Cron is Linux's **built-in task scheduler**. It runs commands or scripts at specified times/intervals automatically — no human intervention needed. For data engineers, cron is the simplest way to schedule: ETL jobs, file transfers, cleanup tasks, and monitoring checks.

```bash
# View your cron jobs:
crontab -l

# Edit your cron jobs:
crontab -e

# A crontab entry looks like:
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of week (0 - 7, 0 and 7 = Sunday)
# │ │ │ │ │
# * * * * * command_to_execute
```

---

## Cron Expression Syntax

| Field | Values | Special Characters |
|-------|--------|-------------------|
| Minute | 0-59 | * , - / |
| Hour | 0-23 | * , - / |
| Day of Month | 1-31 | * , - / |
| Month | 1-12 | * , - / |
| Day of Week | 0-7 (0,7=Sun) | * , - / |

```bash
# COMMON SCHEDULES:

# Every minute:
* * * * * /opt/etl/check_landing.sh

# Every 5 minutes:
*/5 * * * * /opt/etl/quick_check.sh

# Every hour at minute 0:
0 * * * * /opt/etl/hourly_ingest.sh

# Every day at 6 AM:
0 6 * * * /opt/etl/daily_etl.sh

# Every day at 6:30 AM:
30 6 * * * /opt/etl/daily_etl.sh

# Weekdays at 8 AM:
0 8 * * 1-5 /opt/etl/business_hours_report.sh

# Every Sunday at midnight:
0 0 * * 0 /opt/etl/weekly_cleanup.sh

# First day of month at 1 AM:
0 1 1 * * /opt/etl/monthly_report.sh

# Every 15 minutes during business hours:
*/15 8-17 * * 1-5 /opt/etl/frequent_sync.sh

# Twice daily (6 AM and 6 PM):
0 6,18 * * * /opt/etl/twice_daily.sh
```

---

## Setting Up Cron Jobs

```bash
# Method 1: User crontab (per-user)
crontab -e
# Add line: 0 6 * * * /opt/etl/daily_etl.sh
# Save and exit

# Method 2: System crontab (requires root)
# Edit /etc/crontab (includes user field):
# 0 6 * * * dataeng /opt/etl/daily_etl.sh
#                  ^-- runs as this user

# Method 3: Drop-in directory (no editing needed)
# Place script in /etc/cron.daily/ (runs daily)
# Or: /etc/cron.hourly/, /etc/cron.weekly/, /etc/cron.monthly/
cp my_script.sh /etc/cron.daily/

# Verify cron is running:
systemctl status cron   # or: systemctl status crond
```

---

## Best Practices for Data Engineering Cron Jobs

```bash
# RULE 1: Always redirect output to a log file
0 6 * * * /opt/etl/daily_etl.sh >> /var/log/etl/daily_etl.log 2>&1
# Without this: cron sends output via email (fills mailbox, or lost!)

# RULE 2: Use absolute paths (cron has minimal PATH)
# BAD:
0 6 * * * python etl.py                    # Which python? Which etl.py?
# GOOD:
0 6 * * * /usr/bin/python3 /opt/etl/etl.py  # Absolute paths!

# RULE 3: Set environment variables (cron doesn't load .bashrc!)
0 6 * * * . /home/dataeng/.env && /opt/etl/daily_etl.sh
# OR define at top of crontab:
PATH=/usr/local/bin:/usr/bin:/bin
HOME=/home/dataeng
SHELL=/bin/bash
0 6 * * * /opt/etl/daily_etl.sh >> /var/log/etl/daily.log 2>&1

# RULE 4: Prevent overlapping runs (flock)
0 * * * * flock -n /tmp/hourly_etl.lock /opt/etl/hourly_etl.sh
# If previous run is still going: skips this execution (won't overlap!)

# RULE 5: Alert on failure
0 6 * * * /opt/etl/daily_etl.sh >> /var/log/etl/daily.log 2>&1 || curl -X POST "$SLACK_WEBHOOK" -d '{"text":"Daily ETL FAILED!"}'
```

---

## Monitoring Cron Jobs

```bash
# Check if cron job ran (log-based):
grep "daily_etl" /var/log/syslog     # System log shows cron executions
grep "CRON" /var/log/syslog | tail   # Recent cron entries

# Check job output (from redirect):
tail -50 /var/log/etl/daily_etl.log

# Check if job is currently running:
ps aux | grep "daily_etl"

# Common issues:
# 1. Job doesn't run: wrong path, wrong permissions (chmod +x!), cron service stopped
# 2. Job runs but fails: missing env vars (cron doesn't load .bashrc), wrong PATH
# 3. Job overlaps: previous run not finished → use flock!
# 4. No output: forgot to redirect stdout/stderr → add >> log 2>&1

# Debug: test manually first!
/opt/etl/daily_etl.sh
# If it works manually but not in cron: it's an environment issue (PATH, vars)
```

---

## Cron vs Modern Schedulers

| Feature | Cron | Airflow | Systemd Timers |
|---------|------|---------|----------------|
| Setup | 1 line | Install + configure | Unit file |
| Dependencies | None (each job independent) | Full DAG support | Basic (After=) |
| Retry on failure | No (must implement) | Built-in | Built-in (Restart=) |
| Monitoring | Logs only | Web UI, alerts | journalctl |
| Parallelism | No control | Pools, concurrency | No |
| Best for | Simple scheduled tasks | Complex pipelines | System services |

```bash
# Use CRON when:
# - Simple schedule (run script at X time)
# - No dependencies between jobs
# - Server-local execution (no distributed)
# - Quick setup (1 line vs Airflow deployment)

# Use AIRFLOW/Dagster when:
# - Jobs depend on each other (DAG)
# - Need retry, SLA monitoring, alerting
# - Distributed execution (multiple workers)
# - Complex pipelines (50+ tasks)
```

---

## Common DE Cron Job Examples

```bash
# ETL pipeline (daily at 6 AM):
0 6 * * * flock -n /tmp/daily_etl.lock /opt/etl/daily_pipeline.sh >> /var/log/etl/daily.log 2>&1

# Data landing zone check (every 5 min):
*/5 * * * * /opt/etl/check_new_files.sh >> /var/log/etl/file_check.log 2>&1

# Database backup (daily at 2 AM):
0 2 * * * /opt/scripts/backup_db.sh >> /var/log/backups/db_backup.log 2>&1

# Disk cleanup (weekly Sunday 3 AM):
0 3 * * 0 find /data/tmp -mtime +7 -delete >> /var/log/cleanup.log 2>&1

# Health check + alert (every minute):
* * * * * /opt/monitoring/health_check.sh 2>&1 | grep -q "UNHEALTHY" && curl -X POST "$SLACK_WEBHOOK" -d '{"text":"System unhealthy!"}'

# S3 sync (every 15 minutes):
*/15 * * * * flock -n /tmp/s3sync.lock aws s3 sync s3://bucket/landing/ /data/landing/ >> /var/log/s3sync.log 2>&1

# Report generation (weekdays 8 AM):
0 8 * * 1-5 /opt/reports/generate_daily_report.sh >> /var/log/reports/daily.log 2>&1
```

---

## Interview Tips

> **Tip 1:** "Explain cron syntax" — Five fields: minute, hour, day-of-month, month, day-of-week. `*` = every value, `*/N` = every N units, `N-M` = range, `N,M` = specific values. Example: `0 6 * * 1-5` = 6 AM on weekdays. Always test manually before adding to crontab!

> **Tip 2:** "How do you prevent cron job overlap?" — Use `flock`: `flock -n /tmp/job.lock /path/to/script.sh`. If the lock is already held (previous run still going), the new execution exits immediately (no overlap). Essential for: any job that might take longer than its schedule interval.

> **Tip 3:** "Common cron pitfall?" — Environment! Cron runs with minimal PATH and no .bashrc. Scripts that work interactively fail in cron because: wrong Python path, missing env vars, relative paths don't resolve. Fix: use absolute paths for EVERYTHING, source environment files explicitly, and set PATH at the top of your crontab.
