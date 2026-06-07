---
title: "Error Handling - Fundamentals"
topic: bash-scripting
subtopic: error-handling
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, error-handling, exit-codes, set-e, trap, debugging]
---

# Bash Error Handling — Fundamentals

## Why Error Handling Matters

By default, bash continues executing after errors! This means: a failed `cp` command won't stop your script, leading to downstream commands operating on missing/stale data — silent data corruption.

```bash
# WITHOUT error handling (DANGEROUS!):
cp /data/source.csv /data/dest.csv      # Fails silently (source doesn't exist!)
python transform.py /data/dest.csv       # Runs on OLD data (or crashes!)
psql -c "TRUNCATE TABLE target"          # Deletes good production data!
psql -c "\COPY target FROM dest.csv"     # Loads stale/wrong data!
# DISASTER: production table now has wrong data, no one noticed!

# WITH error handling (SAFE):
set -euo pipefail
cp /data/source.csv /data/dest.csv      # Fails → script STOPS immediately!
# Nothing below executes. Production data is safe.
```

---

## The Essential: set -euo pipefail

```bash
#!/bin/bash
set -euo pipefail  # ALWAYS start production scripts with this!

# What each flag does:
# -e: Exit immediately if any command returns non-zero (error)
# -u: Exit if an undefined variable is used (catches typos!)
# -o pipefail: A pipe fails if ANY command in it fails (not just the last one!)

# WITHOUT -e:
cp nonexistent.csv dest.csv  # Fails with error message, but script CONTINUES!
echo "This still runs!"      # ← BAD! You don't want this!

# WITH -e:
set -e
cp nonexistent.csv dest.csv  # Fails → script exits immediately
echo "This never runs"       # ← GOOD! Stopped before causing damage

# WITHOUT -u:
echo "$DATABSE_HOST"  # Typo! Silently expands to empty string
# With -u: "bash: DATABSE_HOST: unbound variable" → exits! Catches the typo!

# WITHOUT pipefail:
cat nonexistent.csv | wc -l  # cat fails, but wc succeeds → exit code 0!
# With pipefail: pipe's exit code = cat's failure → script stops!
```

---

## Exit Codes

```bash
# Every command returns an exit code: 0 = success, 1-255 = failure

python etl.py
echo $?  # 0 if Python script succeeded, non-zero if it failed

# Check and handle:
if python transform.py; then
    echo "Transform succeeded!"
else
    echo "Transform FAILED (exit code: $?)"
    exit 1
fi

# Common exit codes:
# 0: Success
# 1: General error
# 2: Misuse of command (wrong arguments)
# 126: Permission denied (file not executable)
# 127: Command not found (typo in command name)
# 128+N: Killed by signal N (137 = kill -9 = 128+9)
# 130: Ctrl+C (128+2 = SIGINT)

# Set meaningful exit codes in your scripts:
if [ -z "$DB_HOST" ]; then
    echo "ERROR: DB_HOST not set!" >&2  # Error message to stderr
    exit 2  # Configuration error
fi
```

---

## Handling Expected Failures

```bash
# Sometimes failure is EXPECTED (don't let set -e kill the script):

# Pattern 1: || true (suppress error)
grep "ERROR" logfile.log || true
# If no ERRORs found → grep exits 1 → || true makes it exit 0 → script continues

# Pattern 2: || handle_error
python transform.py || {
    echo "Transform failed! Attempting fallback..."
    python transform_fallback.py
}

# Pattern 3: if/then for commands that might fail
if pg_isready -h "$DB_HOST" -t 5; then
    echo "Database is ready"
else
    echo "Database not available — waiting..."
    sleep 30
fi

# Pattern 4: Capture exit code without stopping
set +e  # Temporarily disable exit-on-error
python risky_operation.py
exit_code=$?
set -e  # Re-enable

if [ $exit_code -ne 0 ]; then
    echo "Operation failed with code $exit_code"
    # Handle gracefully...
fi
```

---

## trap for Cleanup

```bash
#!/bin/bash
set -euo pipefail

TEMP_FILE=$(mktemp)
LOCK_FILE="/tmp/pipeline.lock"

# Cleanup runs on ANY exit (normal, error, signal):
cleanup() {
    rm -f "$TEMP_FILE" "$LOCK_FILE"
    echo "Cleaned up temp files."
}
trap cleanup EXIT

# Now even if the script fails, cleanup happens:
echo $$ > "$LOCK_FILE"
python dangerous_operation.py  # If this fails → cleanup still runs!
# Script exits → cleanup runs automatically
```

---

## Redirecting Errors

```bash
# stderr (fd 2) vs stdout (fd 1):
echo "This is normal output"          # Goes to stdout (fd 1)
echo "This is an error!" >&2          # Goes to stderr (fd 2)

# Redirect stderr to a file:
python etl.py 2> /var/log/etl_errors.log       # stderr to file, stdout to terminal
python etl.py > /var/log/etl.log 2>&1          # Both stdout AND stderr to file
python etl.py > /dev/null 2>&1                  # Suppress ALL output (silent)

# Capture stderr in a variable:
error_output=$(python transform.py 2>&1 >/dev/null)
if [ $? -ne 0 ]; then
    echo "Error details: $error_output"
fi
```

---

## Debugging

```bash
# Enable trace mode (shows every command before execution):
set -x
# + cp source.csv dest.csv
# + python transform.py
# + echo 'Done'
# Super helpful for finding WHERE a script fails!

# Enable for specific section only:
set -x
python transform.py  # Shows this command being executed
set +x

# Run entire script in debug mode (from command line):
bash -x my_script.sh

# Print line number on error:
trap 'echo "ERROR at line $LINENO (exit: $?)"' ERR
# When any command fails: prints which line number caused it!
```

---

## Common DE Error Handling Patterns

```bash
#!/bin/bash
set -euo pipefail

# Pattern: Pre-flight checks (validate before doing anything destructive)
echo "=== Pre-flight checks ==="
command -v psql >/dev/null || { echo "ERROR: psql not installed!"; exit 1; }
command -v python >/dev/null || { echo "ERROR: python not installed!"; exit 1; }
[ -f "/data/landing/orders.csv" ] || { echo "ERROR: Input file missing!"; exit 1; }
pg_isready -h "$DB_HOST" -t 5 || { echo "ERROR: Database unreachable!"; exit 1; }
echo "All checks passed ✓"

# Pattern: Conditional pipeline (each step depends on previous)
echo "=== Running pipeline ==="
python ingest.py && echo "✓ Ingest" || { echo "✗ Ingest FAILED"; exit 1; }
python transform.py && echo "✓ Transform" || { echo "✗ Transform FAILED"; exit 1; }
python load.py && echo "✓ Load" || { echo "✗ Load FAILED"; exit 1; }
echo "=== Pipeline complete ==="
```

---

## Interview Tips

> **Tip 1:** "What does `set -euo pipefail` do?" — Three safety nets: `-e` exits on any error (prevents cascading failures), `-u` exits on undefined variables (catches typos), `pipefail` fails the pipe if ANY command fails (not just the last). Put it at the top of EVERY production bash script. Without it: scripts silently continue after errors → data corruption.

> **Tip 2:** "How do you handle cleanup when a script fails?" — `trap cleanup EXIT`: registers a function that runs on ANY exit (success, failure, signal). Put cleanup logic there: remove temp files, release locks, close connections. The trap fires regardless of HOW the script exits — even if killed by a signal or error.

> **Tip 3:** "How do you debug a failing bash script?" — (1) `set -x` shows every command before execution (find which line fails), (2) `trap 'echo "Error at line $LINENO"' ERR` prints the exact line number, (3) `bash -x script.sh` runs the entire script in trace mode. For production: add `set -x` temporarily in the failing area, check the log, remove after fixing.
