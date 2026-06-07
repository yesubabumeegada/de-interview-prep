---
title: "Process Management - Fundamentals"
topic: bash-scripting
subtopic: process-management
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, process-management, linux, background-jobs, signals, monitoring]
---

# Bash Process Management — Fundamentals

## Why Process Management Matters for DE

Data engineers run: long-running ETL jobs, background data transfers, database connections, and concurrent pipeline steps. Knowing how to manage processes ensures you can: monitor running jobs, kill stuck processes, run tasks in parallel, and handle graceful shutdowns.

---

## Viewing Running Processes

```bash
# List all processes:
ps aux                          # All processes with details
ps aux | grep "python"          # Find Python processes
ps aux | grep "[s]park"         # Find Spark processes (brackets avoid matching grep itself)

# Process tree (parent-child relationships):
pstree -p                       # Tree with PIDs
pstree -p $(pgrep -f "etl")    # Tree of ETL-related processes

# Real-time monitoring:
top                             # Interactive process monitor
htop                            # Better interactive monitor (install: apt install htop)
top -b -n 1 | head -20         # Batch mode (scriptable), top 20 processes

# Key columns in ps/top:
# PID: Process ID (unique identifier)
# %CPU: CPU usage percentage
# %MEM: Memory usage percentage
# STAT: Process state (R=running, S=sleeping, Z=zombie, T=stopped)
# TIME: Total CPU time consumed
# COMMAND: The command that started the process
```

---

## Running Processes in Background

```bash
# Run in background (& at the end):
python long_etl_job.py &
# Returns immediately — job runs in background
# Shows: [1] 12345 (job number and PID)

# Redirect output (prevent terminal clutter):
python etl_job.py > /var/log/etl.log 2>&1 &
# stdout AND stderr → log file, runs in background

# nohup: keeps running even if you disconnect (SSH session ends):
nohup python etl_job.py > /var/log/etl.log 2>&1 &
# Survives logout! The job continues after you close the terminal.
# Essential for: long jobs on remote servers via SSH

# Check background jobs in current shell:
jobs                    # List background jobs
jobs -l                 # List with PIDs
fg %1                   # Bring job 1 to foreground
bg %1                   # Resume stopped job in background

# Practical example: start multiple transfers in parallel
nohup aws s3 sync s3://bucket/data1/ /data/data1/ > /tmp/sync1.log 2>&1 &
nohup aws s3 sync s3://bucket/data2/ /data/data2/ > /tmp/sync2.log 2>&1 &
nohup aws s3 sync s3://bucket/data3/ /data/data3/ > /tmp/sync3.log 2>&1 &
echo "3 syncs running in background. Check with: jobs -l"
```

---

## Killing Processes

```bash
# Kill by PID:
kill 12345              # Send SIGTERM (graceful shutdown request)
kill -9 12345           # Send SIGKILL (force kill — last resort!)
kill -15 12345          # Explicit SIGTERM (same as plain kill)

# Kill by name:
pkill -f "etl_job.py"           # Kill all processes matching pattern
pkill -f "spark-submit"         # Kill all Spark jobs
killall python                   # Kill ALL python processes (dangerous!)

# Graceful vs Force:
# SIGTERM (kill -15): "Please shut down gracefully" — process can clean up
# SIGKILL (kill -9): "Die immediately" — no cleanup possible
# ALWAYS try SIGTERM first, wait 10 seconds, then SIGKILL if still running:
kill $PID
sleep 10
if kill -0 $PID 2>/dev/null; then
    echo "Process didn't stop gracefully, force killing..."
    kill -9 $PID
fi
```

---

## Waiting for Processes

```bash
# Wait for background process to complete:
python job1.py &
PID1=$!                 # $! = PID of last background process

python job2.py &
PID2=$!

# Wait for both to complete:
wait $PID1
echo "Job 1 exit code: $?"
wait $PID2
echo "Job 2 exit code: $?"

# Wait for ALL background processes:
wait
echo "All background jobs complete"

# Practical: run 3 ETL steps in parallel, wait for all, then continue
python ingest_orders.py &
python ingest_events.py &
python ingest_customers.py &
wait  # Blocks until all 3 finish
echo "All ingestion complete — starting transformation..."
python transform.py
```

---

## Process Exit Codes

```bash
# Every process returns an exit code (0 = success, non-zero = failure):
python etl_job.py
echo $?                 # 0 if Python script succeeded, non-zero if error

# Use exit codes in scripts:
python transform.py
if [ $? -ne 0 ]; then
    echo "ERROR: Transform failed!"
    exit 1
fi
echo "Transform succeeded — continuing..."

# Common exit codes:
# 0: Success
# 1: General error
# 2: Misuse of command
# 126: Permission denied (not executable)
# 127: Command not found
# 128+N: Killed by signal N (e.g., 137 = killed by SIGKILL = 128+9)
# 130: Ctrl+C (SIGINT = 128+2)

# Check if a process is running:
if kill -0 $PID 2>/dev/null; then
    echo "Process $PID is still running"
else
    echo "Process $PID has finished"
fi
```

---

## Timeouts

```bash
# Run a command with a time limit:
timeout 300 python slow_job.py
# If slow_job.py doesn't finish in 300 seconds → killed automatically!
# Exit code: 124 (timeout reached)

# With custom signal:
timeout --signal=SIGTERM 600 python etl_job.py
# Sends SIGTERM after 600 seconds (gives process chance to clean up)

# Practical: database query with timeout
timeout 120 psql -c "SELECT COUNT(*) FROM huge_table" > count.txt
if [ $? -eq 124 ]; then
    echo "ERROR: Query timed out after 2 minutes!"
    exit 1
fi

# Pipeline step with timeout:
timeout 3600 python heavy_transform.py || {
    echo "Transform exceeded 1-hour timeout!"
    # Alert + cleanup
    exit 1
}
```

---

## Resource Monitoring

```bash
# Disk usage:
df -h                           # Filesystem usage (human-readable)
du -sh /data/*                  # Size of each subdirectory

# Memory usage:
free -h                         # RAM usage summary
# Check if enough memory before starting a big job:
available_mb=$(free -m | awk 'NR==2{print $7}')
if [ $available_mb -lt 4096 ]; then
    echo "WARNING: Less than 4 GB RAM available ($available_mb MB)"
fi

# CPU load:
uptime                          # Load average (1, 5, 15 min)
nproc                           # Number of CPU cores

# Find resource-hungry processes:
ps aux --sort=-%mem | head -10  # Top 10 by memory
ps aux --sort=-%cpu | head -10  # Top 10 by CPU
```

---

## Interview Tips

> **Tip 1:** "How do you run a long ETL job on a remote server?" — `nohup python etl.py > /var/log/etl.log 2>&1 &`. nohup prevents kill on SSH disconnect. `&` runs in background. Redirect output to a log file for monitoring. Check status with `ps aux | grep etl` or `tail -f /var/log/etl.log`.

> **Tip 2:** "How do you kill a stuck process?" — First `kill PID` (SIGTERM = graceful). Wait 10 seconds. If still running: `kill -9 PID` (SIGKILL = force). Never start with -9 (doesn't allow cleanup: temp files, DB connections, partial writes). For finding the PID: `ps aux | grep "process_name"` or `pgrep -f "pattern"`.

> **Tip 3:** "How do you run tasks in parallel in bash?" — Background them with `&`, capture PIDs with `$!`, then `wait` for all. Example: start 3 ingestion jobs in parallel → `wait` → then run dependent transformation. This is the simplest form of parallel orchestration (good for 2-5 tasks; use Airflow for complex DAGs).
