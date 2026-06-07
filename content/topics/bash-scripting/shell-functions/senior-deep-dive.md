---
title: "Shell Functions - Senior Deep Dive"
topic: bash-scripting
subtopic: shell-functions
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, functions, framework, production, architecture]
---

# Shell Functions — Senior-Level Deep Dive

## Building an ETL Framework with Functions

```bash
#!/bin/bash
# /opt/etl/framework/pipeline.sh — reusable pipeline framework

# Framework: defines structure, handles cross-cutting concerns
# User scripts: define step functions, the framework orchestrates them

declare -a PIPELINE_STEPS=()
declare -A STEP_STATUS=()
PIPELINE_START_TIME=0

# Register a pipeline step
register_step() {
    PIPELINE_STEPS+=("$1")
}

# Execute all registered steps with logging, timing, error handling
execute_pipeline() {
    local pipeline_name="$1"
    PIPELINE_START_TIME=$(date +%s)
    
    log_info "═══ Pipeline: $pipeline_name ═══"
    log_info "Steps: ${PIPELINE_STEPS[*]}"
    
    for step in "${PIPELINE_STEPS[@]}"; do
        local step_start=$(date +%s)
        log_info "▶ Starting: $step"
        
        if $step; then
            local duration=$(( $(date +%s) - step_start ))
            STEP_STATUS[$step]="success (${duration}s)"
            log_info "✓ Completed: $step (${duration}s)"
        else
            local duration=$(( $(date +%s) - step_start ))
            STEP_STATUS[$step]="FAILED (${duration}s)"
            log_error "✗ Failed: $step (${duration}s)"
            
            # Print summary of what ran
            print_summary "$pipeline_name"
            return 1
        fi
    done
    
    print_summary "$pipeline_name"
    return 0
}

print_summary() {
    local total_duration=$(( $(date +%s) - PIPELINE_START_TIME ))
    log_info ""
    log_info "═══ Summary: $1 (${total_duration}s total) ═══"
    for step in "${PIPELINE_STEPS[@]}"; do
        log_info "  $step: ${STEP_STATUS[$step]:-not run}"
    done
}

# --- USER SCRIPT uses the framework: ---
# source /opt/etl/framework/pipeline.sh
# source /opt/etl/lib/utils.sh
#
# step_ingest() { ... }
# step_transform() { ... }
# step_validate() { ... }
# step_load() { ... }
#
# register_step "step_ingest"
# register_step "step_transform"
# register_step "step_validate"
# register_step "step_load"
#
# execute_pipeline "Daily Orders ETL"
```

---

## Decorator-Like Patterns

```bash
# "Wrap" any function with cross-cutting concerns (timing, retry, logging)

# Timing decorator:
with_timing() {
    local func_name="$1"; shift
    local start=$(date +%s)
    "$func_name" "$@"
    local status=$?
    local duration=$(( $(date +%s) - start ))
    log_info "TIMER: $func_name took ${duration}s"
    return $status
}

# Retry decorator:
with_retry() {
    local max="$1"; shift
    local func_name="$1"; shift
    retry $max 5 "$func_name" "$@"
}

# Usage:
with_timing step_transform           # Logs how long transform takes
with_retry 3 step_load              # Retries load up to 3 times
with_timing with_retry 3 step_load  # Both! Times the whole retry sequence
```

---

## Callback Patterns

```bash
# Define callbacks for pipeline lifecycle events:
declare -a ON_SUCCESS_HOOKS=()
declare -a ON_FAILURE_HOOKS=()

on_success() { ON_SUCCESS_HOOKS+=("$1"); }
on_failure() { ON_FAILURE_HOOKS+=("$1"); }

run_hooks() {
    local hook_type="$1"
    local -n hooks="${hook_type}"
    for hook in "${hooks[@]}"; do
        $hook || true  # Don't fail if hook fails
    done
}

# Register hooks:
notify_slack_success() { alert "Pipeline completed successfully! 🎉"; }
notify_slack_failure() { alert "Pipeline FAILED! 🚨" "critical"; }
cleanup_temp() { rm -rf /tmp/etl_*; }
update_metrics() { echo "{\"status\":\"success\",\"ts\":\"$(date -Iseconds)\"}" >> /var/log/metrics.jsonl; }

on_success "notify_slack_success"
on_success "update_metrics"
on_success "cleanup_temp"
on_failure "notify_slack_failure"
on_failure "cleanup_temp"

# Execute:
if execute_pipeline "Daily ETL"; then
    run_hooks "ON_SUCCESS_HOOKS"
else
    run_hooks "ON_FAILURE_HOOKS"
    exit 1
fi
```

---

## Function-Based Config Validation

```bash
# Validate complex configurations with composable validator functions:

declare -a VALIDATION_ERRORS=()

validate_not_empty() {
    local var_name="$1"
    local value="${!var_name:-}"
    [ -z "$value" ] && VALIDATION_ERRORS+=("$var_name is required but empty")
}

validate_numeric() {
    local var_name="$1"
    local value="${!var_name:-}"
    [[ "$value" =~ ^[0-9]+$ ]] || VALIDATION_ERRORS+=("$var_name must be numeric (got: '$value')")
}

validate_url() {
    local var_name="$1"
    local value="${!var_name:-}"
    [[ "$value" =~ ^https?:// ]] || VALIDATION_ERRORS+=("$var_name must be a URL (got: '$value')")
}

validate_file_exists() {
    local path="$1"
    [ -f "$path" ] || VALIDATION_ERRORS+=("File not found: $path")
}

# Validate all config:
validate_config() {
    VALIDATION_ERRORS=()
    
    validate_not_empty "DB_HOST"
    validate_not_empty "DB_PASSWORD"
    validate_numeric "DB_PORT"
    validate_numeric "BATCH_SIZE"
    validate_url "SLACK_WEBHOOK"
    validate_file_exists "/opt/etl/transform.py"
    
    if [ ${#VALIDATION_ERRORS[@]} -gt 0 ]; then
        echo "Configuration validation FAILED:"
        printf "  ❌ %s\n" "${VALIDATION_ERRORS[@]}"
        return 1
    fi
    echo "Configuration valid ✓"
    return 0
}

validate_config || exit 1
```

---

## Interview Tips

> **Tip 1:** "How do you build a reusable ETL framework in bash?" — Framework handles: step registration, ordered execution, timing, error handling, and summary reporting. User scripts define step functions and register them. Same framework for all pipelines — consistent behavior, reduced boilerplate. Pattern: register_step → execute_pipeline → hooks on success/failure.

> **Tip 2:** "Decorator pattern in bash?" — Wrapper functions that add behavior: `with_timing` (logs duration), `with_retry` (retries on failure), `with_logging` (captures output). Compose: `with_timing with_retry 3 my_function`. Each decorator is independent and stackable — clean separation of concerns.

> **Tip 3:** "How do you validate complex configuration?" — Composable validator functions: `validate_not_empty`, `validate_numeric`, `validate_url`, `validate_file_exists`. Each adds to a shared error array. After all checks: if errors exist → print all and fail. Benefits: clear error messages (ALL issues at once, not one-at-a-time), reusable validators, testable.
