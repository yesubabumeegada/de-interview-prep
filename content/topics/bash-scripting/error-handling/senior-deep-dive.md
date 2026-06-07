---
title: "Error Handling - Senior Deep Dive"
topic: bash-scripting
subtopic: error-handling
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, error-handling, production, reliability, observability]
---

# Bash Error Handling — Senior-Level Deep Dive

## Production Error Handling Framework

```bash
#!/bin/bash
# /opt/etl/lib/error_handling.sh — production error handling library

set -euo pipefail

# Error context tracking
declare -g ERROR_CONTEXT=""
declare -g ERROR_STEP=""

# Set the current context (for error messages)
set_context() { ERROR_CONTEXT="$1"; ERROR_STEP="${2:-}"; }

# Global error handler
_global_error_handler() {
    local exit_code=$?
    local line=$1
    local command="$BASH_COMMAND"
    
    # Structured error report:
    cat >&2 << EOF
┌─────────────────────────────────────────────────────┐
│ SCRIPT ERROR                                         │
├─────────────────────────────────────────────────────┤
│ Script:   $0
│ Line:     $line
│ Command:  $command
│ Exit:     $exit_code
│ Context:  ${ERROR_CONTEXT:-none}
│ Step:     ${ERROR_STEP:-unknown}
│ Time:     $(date '+%Y-%m-%d %H:%M:%S')
│ Host:     $(hostname)
│ User:     $(whoami)
└─────────────────────────────────────────────────────┘
EOF
    
    # Alert (if configured):
    if [ -n "${SLACK_WEBHOOK:-}" ]; then
        curl -sS -X POST "$SLACK_WEBHOOK" -d "{
            \"text\": \"🚨 Script Error\n\`$0\` failed at line $line\nCommand: \`$command\`\nContext: ${ERROR_CONTEXT:-none}\"
        }" > /dev/null 2>&1 || true
    fi
}

trap '_global_error_handler $LINENO' ERR

# Usage in pipeline scripts:
# source /opt/etl/lib/error_handling.sh
# set_context "Daily Orders Pipeline" "ingest"
# python ingest.py  # If this fails → detailed error report + alert
```

---

## Circuit Breaker Pattern

```bash
#!/bin/bash
# Circuit breaker: stop calling a failing service after N consecutive failures

CIRCUIT_STATE_FILE="/tmp/circuit_breaker_${1:-default}"
MAX_FAILURES=5
RECOVERY_TIME=300  # 5 minutes

circuit_breaker() {
    local service_name="$1"; shift
    local state_file="/tmp/circuit_${service_name}"
    
    # Check if circuit is OPEN (too many recent failures)
    if [ -f "$state_file" ]; then
        local failures=$(head -1 "$state_file")
        local last_fail=$(tail -1 "$state_file")
        local elapsed=$(( $(date +%s) - last_fail ))
        
        if [ $failures -ge $MAX_FAILURES ] && [ $elapsed -lt $RECOVERY_TIME ]; then
            echo "CIRCUIT OPEN: $service_name — skipping (cooling off for $((RECOVERY_TIME - elapsed))s)"
            return 2  # Special code: circuit open
        fi
    fi
    
    # Try the command
    if "$@"; then
        # Success: reset the circuit
        rm -f "$state_file"
        return 0
    else
        # Failure: increment counter
        local current_failures=$(head -1 "$state_file" 2>/dev/null || echo 0)
        echo "$((current_failures + 1))" > "$state_file"
        echo "$(date +%s)" >> "$state_file"
        
        if [ $((current_failures + 1)) -ge $MAX_FAILURES ]; then
            echo "CIRCUIT OPENED: $service_name — too many failures ($MAX_FAILURES)!"
            alert "$service_name circuit breaker OPENED after $MAX_FAILURES failures"
        fi
        return 1
    fi
}

# Usage:
circuit_breaker "database" psql -h db-prod -c "SELECT 1"
case $? in
    0) echo "DB healthy" ;;
    1) echo "DB call failed (circuit counting...)" ;;
    2) echo "Circuit OPEN — not even trying (wait for recovery)" ;;
esac
```

---

## Error Budgets and SLA Tracking

```bash
#!/bin/bash
# Track error rates and alert when approaching SLA budget

ERROR_LOG="/var/log/etl/error_budget.jsonl"

record_run() {
    local pipeline="$1" status="$2" duration="$3"
    echo "{\"pipeline\":\"$pipeline\",\"status\":\"$status\",\"duration\":$duration,\"ts\":\"$(date -Iseconds)\"}" >> "$ERROR_LOG"
}

check_error_budget() {
    local pipeline="$1"
    local sla_target=0.95  # 95% success rate required
    local window_days=7
    
    local total=$(grep "\"pipeline\":\"$pipeline\"" "$ERROR_LOG" | \
        jq -r "select(.ts > \"$(date -d "$window_days days ago" -Iseconds)\")" | wc -l)
    local failures=$(grep "\"pipeline\":\"$pipeline\"" "$ERROR_LOG" | \
        jq -r "select(.ts > \"$(date -d "$window_days days ago" -Iseconds)\" and .status == \"failed\")" | wc -l)
    
    if [ $total -gt 0 ]; then
        local success_rate=$(echo "scale=3; ($total - $failures) / $total" | bc)
        local budget_remaining=$(echo "scale=3; $success_rate - $sla_target" | bc)
        
        echo "Pipeline: $pipeline"
        echo "  Success rate (7d): ${success_rate} (target: $sla_target)"
        echo "  Error budget remaining: $budget_remaining"
        
        if (( $(echo "$budget_remaining < 0.02" | bc -l) )); then
            alert "⚠️ $pipeline error budget almost exhausted! (${success_rate} success rate)"
        fi
    fi
}

# Usage in pipeline scripts:
start=$(date +%s)
if python /opt/etl/pipeline.py; then
    record_run "daily_orders" "success" $(( $(date +%s) - start ))
else
    record_run "daily_orders" "failed" $(( $(date +%s) - start ))
fi

check_error_budget "daily_orders"
```

---

## Cascading Failure Prevention

```bash
#!/bin/bash
# Prevent one failing component from taking down the entire pipeline

# Health check before each dependency call:
safe_call() {
    local service="$1"; shift
    local health_check="$1"; shift
    local actual_command=("$@")
    
    # Pre-check: is the service healthy?
    if ! eval "$health_check" 2>/dev/null; then
        echo "SKIP: $service is unhealthy — using fallback/cache"
        return 2  # Special code: service unavailable
    fi
    
    # Service is healthy: make the call
    "${actual_command[@]}"
}

# Usage: prevent database overload from crashing the pipeline
safe_call "primary_db" \
    "pg_isready -h primary-db -t 3" \
    psql -h primary-db -c "SELECT * FROM orders LIMIT 1000"

case $? in
    0) echo "Got data from primary" ;;
    2) echo "Primary unavailable — trying replica..."
       psql -h replica-db -c "SELECT * FROM orders LIMIT 1000" ;;
    *) echo "Query failed" ;;
esac
```

---

## Interview Tips

> **Tip 1:** "How do you implement a circuit breaker in bash?" — Track consecutive failures in a state file. After N failures: stop calling the service for M seconds (circuit OPEN). After cooldown: try once (HALF-OPEN). If succeeds: reset (CLOSED). If fails: back to OPEN. Prevents: overwhelming a struggling service with retry storms.

> **Tip 2:** "How do you get detailed error context in bash?" — ERR trap with `$LINENO` (line number), `$BASH_COMMAND` (exact command), `${FUNCNAME[@]}` (call stack), plus custom context variables. Log all of these on error. Result: instant identification of WHAT failed, WHERE, and WHY — no more hunting through logs.

> **Tip 3:** "How do you prevent cascading failures?" — Health check before each dependency call. If dependency is down: use fallback (replica DB, cached data, degraded mode) instead of failing entirely. Pattern: check → call if healthy → fallback if unhealthy. Each component's failure is isolated — doesn't propagate to crash the whole pipeline.
