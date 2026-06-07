---
title: "Error Handling - Scenario Questions"
topic: bash-scripting
subtopic: error-handling
content_type: scenario_question
tags: [bash, error-handling, interview, scenarios]
---

# Scenario Questions — Bash Error Handling

<article data-difficulty="junior">

## 🟢 Junior: set -euo pipefail

**Scenario:** This script has a bug — it continues after `cp` fails, then truncates the production table with no data to reload! Fix it with proper error handling.

```bash
#!/bin/bash
cp /data/source.csv /tmp/working.csv
python transform.py /tmp/working.csv
psql -c "TRUNCATE production.orders"
psql -c "\COPY production.orders FROM '/tmp/working.csv' CSV"
```

<details>
<summary>💡 Hint</summary>
Add `set -euo pipefail` at the top. This makes the script exit immediately if any command fails — preventing the TRUNCATE from running if cp or transform failed.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail  # EXIT on any error!

# Now: if cp fails → script STOPS → TRUNCATE never runs → data safe!
cp /data/source.csv /tmp/working.csv
python transform.py /tmp/working.csv

# Even safer: verify file exists before destructive operation
[ -s /tmp/working.csv ] || { echo "ERROR: Transform produced empty file!"; exit 1; }

psql -c "TRUNCATE production.orders"
psql -c "\COPY production.orders FROM '/tmp/working.csv' CSV"
echo "Load complete: $(psql -t -c 'SELECT COUNT(*) FROM production.orders') rows"
```

**Key Points:**
- `set -euo pipefail` prevents cascade: failed `cp` → script stops → no truncate!
- Without it: `cp` fails silently → `python` may fail on missing file → `TRUNCATE` destroys data → `\COPY` fails → production table is empty!
- Extra safety: check file exists and has content BEFORE the destructive TRUNCATE
- This is the #1 most important line in any production bash script
- ALWAYS put it as the first executable line after `#!/bin/bash`

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cleanup on Failure

**Scenario:** Your script creates a temp directory and a lock file. If the script fails mid-execution, both are left behind (stale lock blocks next run, temp files fill disk). Implement guaranteed cleanup.

<details>
<summary>💡 Hint</summary>
Use `trap cleanup EXIT` — registers a function that runs on ANY exit (success, failure, or signal). The cleanup removes temp dir and lock file regardless of how the script exits.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

TEMP_DIR=""
LOCK_FILE="/tmp/etl_pipeline.lock"

# Cleanup function: ALWAYS runs on exit
cleanup() {
    local exit_code=$?
    
    # Remove temp directory
    [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ] && rm -rf "$TEMP_DIR"
    
    # Release lock
    rm -f "$LOCK_FILE"
    
    # Log exit status
    if [ $exit_code -eq 0 ]; then
        echo "[$(date)] Pipeline exited normally"
    else
        echo "[$(date)] Pipeline FAILED (exit: $exit_code) — cleanup done"
    fi
}

# Register cleanup for ALL exit scenarios:
trap cleanup EXIT

# Acquire lock (fail if already held)
[ -f "$LOCK_FILE" ] && { echo "ERROR: Lock held — another instance running?"; exit 1; }
echo $$ > "$LOCK_FILE"

# Create temp directory
TEMP_DIR=$(mktemp -d /tmp/etl_XXXXXX)

# Main work (if anything below fails → cleanup STILL runs!)
echo "Working in: $TEMP_DIR"
python expensive_transform.py --temp="$TEMP_DIR"
echo "Success!"

# Script exits → trap EXIT fires → cleanup() removes temp + lock
# Even if python fails → trap fires → cleanup still happens!
# Even if kill -9 → ... actually kill -9 can't be trapped (only exception!)
```

**Key Points:**
- `trap cleanup EXIT`: fires on ANY exit (normal, error, SIGTERM, SIGINT)
- Captures exit code FIRST (`local exit_code=$?`) before any cleanup commands change it
- Guards: check `$TEMP_DIR` exists before rm (in case trap fires before assignment)
- Lock file: removed on exit → next cron run can acquire it
- Without trap: failed script leaves stale lock → blocks ALL future runs until manually cleaned!
- Only exception: `kill -9` (SIGKILL) cannot be trapped (that's by design)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Error Handling with Context

**Scenario:** Design an error handler that: (1) captures the exact line and command that failed, (2) includes which pipeline step was executing, (3) sends a Slack alert with all context, (4) writes structured error log (parseable by monitoring tools). Show the complete implementation.

<details>
<summary>💡 Hint</summary>
Use ERR trap with $LINENO, $BASH_COMMAND, custom context variable, and structured JSON logging. Combine with curl for Slack webhook.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

# === ERROR HANDLING FRAMEWORK ===
PIPELINE_NAME="daily_orders_etl"
CURRENT_STEP="initialization"
ERROR_LOG="/var/log/etl/errors.jsonl"
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL:-}"

# Error handler: fires on ANY command failure (when set -e is active)
_on_error() {
    local exit_code=$?
    local line_no=$1
    local failed_command="$BASH_COMMAND"
    local timestamp=$(date -Iseconds)
    
    # 1. Structured log (JSON Lines — parseable by Elasticsearch/Datadog):
    echo "{\"ts\":\"$timestamp\",\"pipeline\":\"$PIPELINE_NAME\",\"step\":\"$CURRENT_STEP\",\"line\":$line_no,\"exit_code\":$exit_code,\"command\":\"$(echo $failed_command | sed 's/"/\\"/g')\",\"host\":\"$(hostname)\"}" >> "$ERROR_LOG"
    
    # 2. Human-readable stderr output:
    cat >&2 << EOF

╔══════════════════════════════════════════════════════╗
║ PIPELINE ERROR                                       ║
╠══════════════════════════════════════════════════════╣
║ Pipeline:  $PIPELINE_NAME
║ Step:      $CURRENT_STEP
║ Line:      $line_no
║ Command:   $failed_command
║ Exit Code: $exit_code
║ Time:      $timestamp
║ Host:      $(hostname)
║ Log:       $ERROR_LOG
╚══════════════════════════════════════════════════════╝
EOF
    
    # 3. Slack alert:
    if [ -n "$SLACK_WEBHOOK" ]; then
        curl -sS -X POST "$SLACK_WEBHOOK" \
            -H 'Content-type: application/json' \
            -d "{\"text\":\"🚨 *$PIPELINE_NAME* failed\\nStep: \`$CURRENT_STEP\`\\nLine: $line_no\\nCommand: \`$failed_command\`\\nExit: $exit_code\"}" \
            > /dev/null 2>&1 || true
    fi
}

trap '_on_error $LINENO' ERR

# === PIPELINE STEPS ===
CURRENT_STEP="preflight"
command -v psql >/dev/null
command -v python >/dev/null
pg_isready -h "$DB_HOST" -t 5

CURRENT_STEP="ingest"
aws s3 sync "s3://bucket/landing/" /data/landing/ --quiet

CURRENT_STEP="transform"
python /opt/etl/transform.py --input=/data/landing --output=/data/output

CURRENT_STEP="load"
psql -c "\COPY silver.orders FROM '/data/output/orders.csv' CSV HEADER"

CURRENT_STEP="validate"
row_count=$(psql -t -c "SELECT COUNT(*) FROM silver.orders WHERE load_date=CURRENT_DATE")
[ "$row_count" -gt 0 ] || { echo "Zero rows!"; exit 1; }

echo "Pipeline complete: $row_count rows loaded"
```

**Key Points:**
- ERR trap captures: line number ($LINENO), command ($BASH_COMMAND), exit code ($?)
- CURRENT_STEP variable: manually set before each logical step (adds business context)
- Structured JSON log: parseable by log aggregators (Elasticsearch, Datadog, Splunk)
- Human-readable box: immediate visibility when watching terminal output
- Slack alert: team notified within seconds of failure (includes which step + command)
- Single framework handles ALL error reporting — add to any script with `source`
- Result: operator receives: "daily_orders_etl failed at step 'transform', line 42, command 'python /opt/etl/transform.py' returned exit code 1" — instant root cause identification!

</details>

</article>
