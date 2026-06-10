---
title: "Cron Jobs - Scenario Questions"
topic: bash-scripting
subtopic: cron-jobs
content_type: scenario_question
tags: [bash, cron, interview, scenarios]
---

# Scenario Questions — Cron Jobs

<article data-difficulty="junior">

## 🟢 Junior: Write a Cron Expression

**Scenario:** Schedule an ETL script (`/opt/etl/daily_load.sh`) to run: (A) every day at 6 AM UTC, (B) every 15 minutes during business hours (8 AM - 6 PM weekdays), (C) at 2 AM on the first day of each month. Write the crontab entries.

<details>
<summary>💡 Hint</summary>
Format: minute hour day-of-month month day-of-week command. Use ranges (8-17), intervals (*/15), and specific values (1 for first day).
</details>

<details>
<summary>✅ Solution</summary>

```bash
# (A) Every day at 6 AM UTC:
0 6 * * * /opt/etl/daily_load.sh >> /var/log/etl/daily.log 2>&1
# minute=0, hour=6, day=*, month=*, dow=*

# (B) Every 15 minutes during business hours (8-18, Mon-Fri):
*/15 8-17 * * 1-5 /opt/etl/frequent_sync.sh >> /var/log/etl/sync.log 2>&1
# minute=*/15 (every 15 min), hour=8-17 (8 AM to 5:45 PM), dow=1-5 (Mon-Fri)

# (C) First day of month at 2 AM:
0 2 1 * * /opt/etl/monthly_report.sh >> /var/log/etl/monthly.log 2>&1
# minute=0, hour=2, day-of-month=1, month=*, dow=*

# BEST PRACTICES included:
# - All redirect output to log files (>> file 2>&1)
# - Use absolute paths for scripts
# - Consider adding flock for (A) and (C) to prevent overlap:
0 6 * * * flock -n /tmp/daily.lock /opt/etl/daily_load.sh >> /var/log/etl/daily.log 2>&1
```

**Key Points:**
- `*/15`: every 15 minutes (0, 15, 30, 45)
- `8-17`: hours 8 through 17 (8 AM to 5:XX PM — note: 17:45 is last run)
- `1-5`: Monday through Friday (1=Mon, 5=Fri)
- `1` in day-of-month field: first day of month only
- Always add output redirection (otherwise cron emails output — or loses it!)
- Always use absolute paths (cron has minimal PATH)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Robust Cron Job Script

**Scenario:** Your daily ETL cron job sometimes fails because: (1) the previous run hasn't finished, (2) the database is briefly unavailable, (3) no one notices failures until users complain. Fix all three issues.

<details>
<summary>💡 Hint</summary>
(1) flock prevents overlap. (2) Pre-flight check + retry handles DB unavailability. (3) Alert on failure via webhook + dead man's switch for missed runs.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# /opt/etl/daily_etl.sh — solves all 3 problems
set -euo pipefail

LOCK="/tmp/daily_etl.lock"
SLACK="${SLACK_WEBHOOK_URL}"
HEALTHCHECK_URL="https://hc-ping.com/your-uuid"

# PROBLEM 1: Prevent overlap (flock)
exec 200>"$LOCK"
if ! flock -n 200; then
    echo "[$(date)] SKIP: Previous run still active" >> /var/log/etl/daily.log
    exit 0
fi

# PROBLEM 2: Handle DB unavailability (retry pre-flight check)
for attempt in 1 2 3; do
    if pg_isready -h db-prod -p 5432 -t 5 -q; then
        break
    fi
    echo "[$(date)] DB unavailable, retry $attempt/3..." >> /var/log/etl/daily.log
    sleep 30
done
pg_isready -h db-prod -p 5432 -t 5 -q || {
    echo "[$(date)] DB still unavailable after 3 retries!" >> /var/log/etl/daily.log
    curl -sS -X POST "$SLACK" -d '{"text":"🚨 Daily ETL: Database unreachable after retries!"}'
    exit 1
}

# Main ETL execution
python /opt/etl/daily_transform.py >> /var/log/etl/daily.log 2>&1
exit_code=$?

# PROBLEM 3: Alert on failure + dead man's switch
if [ $exit_code -eq 0 ]; then
    curl -fsS "$HEALTHCHECK_URL" > /dev/null  # Ping = "I'm alive and succeeded"
else
    curl -sS -X POST "$SLACK" -d '{"text":"🚨 Daily ETL FAILED! Exit code: '$exit_code'"}'
    exit $exit_code
fi

# If this script DOESN'T RUN AT ALL (server down, cron stopped):
# → healthchecks.io sends alert because no ping received within expected window!
```

Crontab entry:
```
0 6 * * * /opt/etl/daily_etl.sh
```

**Key Points:**
- Problem 1 (overlap): `flock -n` exits immediately if lock held (no waiting/blocking)
- Problem 2 (DB flaky): retry loop with 30s sleep between attempts (handles brief outages)
- Problem 3a (failure alerting): Slack webhook on non-zero exit (immediate notification)
- Problem 3b (missing execution): dead man's switch ping on success (detects when job DOESN'T RUN)
- Combined: the job handles transient issues itself, alerts on persistent failures, and alerts if it never runs

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cron Job Infrastructure

**Scenario:** You manage 40 cron jobs across 3 servers for a data team of 10. Problems: jobs added/removed without review, no central visibility, failures go unnoticed for hours, and it's unclear what's running where. Design the governance system.

<details>
<summary>💡 Hint</summary>
Git-managed crontabs (code review for changes), centralized deployment script, health monitoring (dead man's switch per job), status dashboard (JSON endpoint per server), and alerting aggregation.
</details>

<details>
<summary>✅ Solution</summary>

```bash
# SOLUTION: Centralized cron management system

# 1. GIT-MANAGED CRONTABS (code review for all changes)
# Repository: github.com/company/cron-configs
# Structure:
# cron-configs/
# ├── server1/
# │   ├── ingestion.cron
# │   └── maintenance.cron
# ├── server2/
# │   ├── transformation.cron
# │   └── reporting.cron
# ├── server3/
# │   └── analytics.cron
# └── deploy.sh

# deploy.sh (runs on merge to main via CI/CD):
#!/bin/bash
SERVER=$1
cat /opt/cron-configs/$SERVER/*.cron | crontab -
echo "Deployed $(crontab -l | grep -cv '^#\|^$') jobs to $SERVER"

# 2. CENTRAL MONITORING (each job pings on success)
# Every cron script ends with:
# curl -fsS "https://hc-ping.com/${JOB_UUID}" > /dev/null
# healthchecks.io dashboard shows: all 40 jobs, last ping time, status

# 3. STATUS DASHBOARD (per-server JSON endpoint)
# Cron: * * * * * /opt/monitoring/emit_status.sh
# Emits: job name, last run, status, duration → central dashboard aggregates

# 4. ALERTING RULES (centralized):
# - Job hasn't pinged in expected window → alert (dead man's switch)
# - Job reported failure (Slack webhook) → alert
# - Job duration trending up → warning (SLA risk)
# - New job added without healthcheck UUID → CI check blocks deploy!

# 5. CI/CD VALIDATION (before deploy):
#!/bin/bash
# validate_crons.sh (runs in CI):
for cron_file in $SERVER/*.cron; do
    while IFS= read -r line; do
        [[ "$line" =~ ^#|^$ ]] && continue
        # Check: has log redirect
        echo "$line" | grep -q ">>" || { echo "FAIL: $line missing log redirect"; exit 1; }
        # Check: has flock (for jobs > 5min frequency)
        # Check: script exists and is executable
        script=$(echo "$line" | awk '{print $6}')
        [ -x "$script" ] || { echo "FAIL: $script not executable"; exit 1; }
    done < "$cron_file"
done
echo "All validations passed ✓"
```

**Key Points:**
- Git-managed: all changes go through PR (review, audit trail, rollback)
- CI validation: catches common mistakes (missing log redirect, non-executable scripts)
- Dead man's switch: detects MISSING executions (server down, cron stopped, script error before ping)
- Central dashboard: single view of all 40 jobs across 3 servers (healthchecks.io or custom)
- No manual crontab editing: deploy.sh is the only way to update (enforced by file permissions)
- Scalable: adding a new job = add a line to a .cron file, create PR, merge, auto-deploy

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What does the cron expression `0 2 * * 1-5` mean?**
A: It means "run at 2:00 AM every weekday (Monday through Friday)." The fields are minute, hour, day of month, month, and day of week respectively.

**Q: How do you prevent a cron job from running if a previous instance is still active?**
A: Use a lockfile with `flock` — for example, `flock -n /tmp/myjob.lock ./myjob.sh` will skip execution if the lock is already held, preventing overlapping runs.

**Q: Where does cron send output by default, and how do you redirect it?**
A: By default cron sends stdout and stderr to the user's local mail. Redirect output explicitly in the crontab entry: `0 2 * * * /path/to/script.sh >> /var/log/myjob.log 2>&1` to capture both streams to a file.

**Q: What is the difference between `/etc/crontab` and a user crontab created with `crontab -e`?**
A: `/etc/crontab` is a system-wide crontab that includes a username field to specify which user runs each job. User crontabs created with `crontab -e` run as the owning user and do not include a username field.

**Q: Why might a cron job fail even though the same command works fine in a terminal?**
A: Cron runs with a minimal environment — `PATH`, shell variables, and sourced profiles differ from an interactive session. Always use absolute paths in cron jobs and explicitly source needed environment files if required.

**Q: How do you run a cron job every 15 minutes?**
A: Use `*/15 * * * * /path/to/script.sh`. The `*/n` syntax means "every n units" for the given field.

**Q: What is anacron and when would you use it instead of cron?**
A: Anacron is a cron-like scheduler designed for machines that are not always running. Unlike cron, it guarantees that missed jobs (e.g., when the machine was off) are run once the machine comes back online, making it suitable for daily/weekly maintenance tasks on laptops or intermittent servers.

**Q: How do you list, edit, and remove a user's crontab?**
A: `crontab -l` lists the current user's crontab, `crontab -e` opens it in an editor, and `crontab -r` removes it entirely. Use `crontab -u username -l` as root to inspect another user's crontab.

---

## 💼 Interview Tips

- Always mention environment differences as the first troubleshooting step when a cron job fails — this is the most common real-world issue and shows practical experience.
- Demonstrate that you log cron job output; interviewers want to know you can debug failures after the fact, not just set and forget.
- Mention using `flock` or similar locking mechanisms for jobs that must not overlap — this shows awareness of production edge cases.
- For data engineering roles, connect cron jobs to orchestration tools like Airflow: know when cron is sufficient versus when you need dependency management and retries.
- Senior interviewers will ask about monitoring — discuss alerting on missed or failed jobs using tools like PagerDuty, CloudWatch, or custom log-based alerts.
- Avoid saying you use cron for complex multi-step pipelines; acknowledge its limitations (no dependency tracking, no retry logic) and when to graduate to a proper orchestrator.
