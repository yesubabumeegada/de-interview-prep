---
title: "Shell Functions - Scenario Questions"
topic: bash-scripting
subtopic: shell-functions
content_type: scenario_question
tags: [bash, functions, interview, scenarios]
---

# Scenario Questions — Shell Functions

<article data-difficulty="junior">

## 🟢 Junior: Writing a Reusable Function

**Scenario:** Write a `validate_csv` function that takes a filename, expected column count, and minimum row count. Returns 0 (success) if valid, 1 (failure) if not. Use it to validate 3 different files.

<details>
<summary>💡 Hint</summary>
Use `local` for internal variables. `wc -l` for row count, `head -1 | awk` for column count. Return 0/1 for pass/fail.
</details>

<details>
<summary>✅ Solution</summary>

```bash
validate_csv() {
    local file="$1"
    local expected_cols="$2"
    local min_rows="$3"
    
    # File exists?
    [ -f "$file" ] || { echo "FAIL: $file not found"; return 1; }
    
    # Not empty?
    [ -s "$file" ] || { echo "FAIL: $file is empty"; return 1; }
    
    # Column count correct?
    local actual_cols=$(head -1 "$file" | awk -F',' '{print NF}')
    [ "$actual_cols" -eq "$expected_cols" ] || { echo "FAIL: $file has $actual_cols cols (expected $expected_cols)"; return 1; }
    
    # Minimum rows (excluding header)?
    local actual_rows=$(( $(wc -l < "$file") - 1 ))
    [ "$actual_rows" -ge "$min_rows" ] || { echo "FAIL: $file has $actual_rows rows (min: $min_rows)"; return 1; }
    
    echo "PASS: $file ($actual_rows rows, $actual_cols cols)"
    return 0
}

# Use for 3 files:
validate_csv "/data/orders.csv" 8 1000 || exit 1
validate_csv "/data/customers.csv" 5 500 || exit 1
validate_csv "/data/products.csv" 4 100 || exit 1
echo "All files valid! Proceeding with ETL..."
```

**Key Points:**
- `local` keeps variables scoped (won't pollute caller's namespace)
- Return 0 = valid, return 1 = invalid (standard bash convention)
- `|| exit 1` after each call: stops pipeline if any file is invalid
- Function is reusable for any CSV with any expected schema
- Could be moved to lib/validation.sh and sourced by multiple scripts

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Retry Function with Backoff

**Scenario:** Write a `retry` function that: takes max_attempts, initial_delay, and a command. Retries the command with exponential backoff. Returns 0 if any attempt succeeds, 1 if all fail. Show usage with a flaky database connection.

<details>
<summary>💡 Hint</summary>
Loop from 1 to max_attempts. Run the command. If success (exit 0): return 0. If failure: sleep(delay), double the delay, continue. After loop: return 1.
</details>

<details>
<summary>✅ Solution</summary>

```bash
retry() {
    local max_attempts="$1"
    local delay="$2"
    shift 2  # Remove first 2 args, leaving the command
    
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        echo "  Attempt $attempt/$max_attempts: $*"
        
        if "$@"; then
            echo "  ✓ Succeeded on attempt $attempt"
            return 0
        fi
        
        if [ $attempt -lt $max_attempts ]; then
            echo "  ✗ Failed. Retrying in ${delay}s..."
            sleep $delay
            delay=$((delay * 2))  # Exponential backoff: 5→10→20→40...
        fi
        
        attempt=$((attempt + 1))
    done
    
    echo "  ✗ All $max_attempts attempts failed!"
    return 1
}

# Usage: retry flaky database connection
echo "Connecting to database..."
retry 4 5 pg_isready -h db-prod.internal -p 5432 -t 3
# Attempt 1/4: pg_isready... (fails — DB restarting)
# Retrying in 5s...
# Attempt 2/4: pg_isready... (fails — still restarting)
# Retrying in 10s...
# Attempt 3/4: pg_isready... (succeeds!)
# ✓ Succeeded on attempt 3

# Usage: retry an API call
retry 3 2 curl -sf "https://api.internal/health"

# Usage: retry a full ETL step
retry 3 30 python /opt/etl/load_to_warehouse.py
```

**Key Points:**
- `shift 2` removes the first 2 args, so `$@` becomes the command to retry
- Exponential backoff: 5→10→20→40 seconds (avoids overwhelming a recovering service)
- `"$@"` preserves the command with its arguments correctly (handles spaces)
- Returns 0 on ANY success, 1 only after ALL attempts exhausted
- Reusable for: DB connections, API calls, S3 operations, any flaky command
- This single function makes all your scripts resilient to transient failures!

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Function-Based Pipeline Framework

**Scenario:** Build a mini framework: register pipeline steps, execute them in order with timing and error handling, print a summary with pass/fail status for each step. No external dependencies (pure bash).

<details>
<summary>💡 Hint</summary>
Use arrays to store step names. Loop through and execute each as a function. Track status in an associative array. Time each step. Print formatted summary at end.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# Mini Pipeline Framework (pure bash, no dependencies)

declare -a STEPS=()
declare -A RESULTS=()
TOTAL_START=0

register() { STEPS+=("$1"); }

execute_pipeline() {
    local name="$1"
    TOTAL_START=$(date +%s)
    
    echo "╔══════════════════════════════════════════╗"
    echo "║  Pipeline: $name"
    echo "╚══════════════════════════════════════════╝"
    echo ""
    
    local failed=false
    for step in "${STEPS[@]}"; do
        local start=$(date +%s)
        printf "  ▶ %-30s" "$step"
        
        if $step 2>/tmp/step_err_$$; then
            local dur=$(( $(date +%s) - start ))
            RESULTS[$step]="✓ ${dur}s"
            echo " ✓ (${dur}s)"
        else
            local dur=$(( $(date +%s) - start ))
            local err=$(head -1 /tmp/step_err_$$ 2>/dev/null)
            RESULTS[$step]="✗ FAILED (${dur}s) $err"
            echo " ✗ FAILED (${dur}s)"
            [ -n "$err" ] && echo "    Error: $err"
            failed=true
            break  # Stop on first failure
        fi
    done
    rm -f /tmp/step_err_$$
    
    # Summary
    local total_dur=$(( $(date +%s) - TOTAL_START ))
    echo ""
    echo "┌────────────────────────────────────────┐"
    echo "│ Summary (${total_dur}s total)            │"
    echo "├────────────────────────────────────────┤"
    for step in "${STEPS[@]}"; do
        printf "│ %-25s %s\n" "$step" "${RESULTS[$step]:-⊘ skipped}"
    done
    echo "└────────────────────────────────────────┘"
    
    $failed && return 1 || return 0
}

# === USER DEFINES STEPS ===
step_download() { sleep 1; echo "Downloaded 5 files"; }
step_validate() { sleep 1; [ -f "/data/orders.csv" ] || return 1; }
step_transform() { sleep 2; echo "Transformed 50000 rows"; }
step_load() { sleep 1; echo "Loaded to warehouse"; }
step_notify() { echo "Notification sent"; }

# === REGISTER & RUN ===
register "step_download"
register "step_validate"
register "step_transform"
register "step_load"
register "step_notify"

execute_pipeline "Daily Orders ETL"
exit $?

# OUTPUT:
# ╔══════════════════════════════════════════╗
# ║  Pipeline: Daily Orders ETL
# ╚══════════════════════════════════════════╝
#
#   ▶ step_download                     ✓ (1s)
#   ▶ step_validate                     ✗ FAILED (1s)
#     Error: file not found
#
# ┌────────────────────────────────────────┐
# │ Summary (2s total)                      │
# ├────────────────────────────────────────┤
# │ step_download             ✓ 1s
# │ step_validate             ✗ FAILED (1s)
# │ step_transform            ⊘ skipped
# │ step_load                 ⊘ skipped
# │ step_notify               ⊘ skipped
# └────────────────────────────────────────┘
```

**Key Points:**
- Framework handles: registration, execution order, timing, error handling, summary
- User only writes: step functions + register calls (minimal boilerplate)
- Stops on first failure (remaining steps shown as "skipped" in summary)
- Easy to add new steps: write function + register it (one line each)
- Pure bash (no external tools) — works on any Linux system
- Could extend: add retry per step, parallel steps, conditional execution
- This pattern scales to 10-15 step pipelines before you need Airflow

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: How do you define and call a function in bash?**
A: Define it with `function_name() { commands; }` or `function function_name { commands; }`. Call it by name like any other command: `function_name arg1 arg2`. Arguments are accessed as `$1`, `$2`, etc. inside the function.

**Q: How do functions return values in bash?**
A: Functions return an integer exit code via `return N` (0–255). To return a string or complex value, print it to stdout and capture it with command substitution: `result=$(my_function arg)`.

**Q: What is the scope of variables inside a bash function?**
A: By default, variables in bash functions are global and persist after the function returns. Declare a variable as local with `local varname=value` to limit its scope to the function and its children.

**Q: How do you pass an array to a bash function?**
A: Arrays cannot be passed directly. Either pass the array elements as individual arguments (`my_func "${arr[@]}"`) or use a nameref (`local -n ref=$1`) in bash 4.3+ to reference the array by name inside the function.

**Q: How do you make a function available in subshells?**
A: Export the function with `export -f function_name`. This serializes the function definition into the environment so child processes spawned with `bash -c` or via subshells can use it.

**Q: What is the difference between `return` and `exit` inside a function?**
A: `return` exits only the function and returns control to the caller with an optional exit code. `exit` terminates the entire script (or subshell if the function runs in one). Using `exit` inside a function is usually a mistake unless you explicitly intend to terminate the script.

**Q: How do you write a reusable logging function for scripts?**
A: Define a function that prepends a timestamp and level: `log() { echo "$(date '+%Y-%m-%dT%H:%M:%S') [$1] $2" >&2; }`. Call it as `log INFO "Starting job"` or `log ERROR "Failed"`. Writing to stderr keeps logs separate from function return values.

**Q: How do you unit test bash functions?**
A: Use the `bats` (Bash Automated Testing System) framework. Source the script under test in a `bats` file, call functions directly, and assert outputs and exit codes with `[ "$output" = "expected" ]` and `[ "$status" -eq 0 ]`.

---

## 💼 Interview Tips

- Emphasize `local` variables in every function you write during a live session — it is one of the clearest signals that you write maintainable bash versus quick one-offs.
- Connect shell functions to DRY principles: explain that centralizing logic in a shared `lib.sh` file that scripts source reduces duplication and makes maintenance easier.
- Mention `bats` for testing bash functions; most candidates have never heard of it and it immediately differentiates you as someone who applies software engineering discipline to scripting.
- For senior roles, discuss how you structure larger bash codebases — a `bin/` directory for entrypoints, a `lib/` directory for shared functions, and sourced utility files.
- Show awareness of the logging function pattern; senior interviewers want to see that you would build observability into reusable functions, not just top-level scripts.
- Avoid writing monolithic scripts during interviews — proactively decompose logic into small, named functions to demonstrate readability and testability instincts.
