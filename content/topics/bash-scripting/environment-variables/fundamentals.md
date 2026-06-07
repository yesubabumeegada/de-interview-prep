---
title: "Environment Variables - Fundamentals"
topic: bash-scripting
subtopic: environment-variables
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, environment-variables, config, secrets, linux]
---

# Environment Variables — Fundamentals

## What Are Environment Variables?

Environment variables are **key-value pairs** available to all processes in a shell session. They configure: application behavior, credentials, paths, and runtime settings without hardcoding values in scripts.

```bash
# Set a variable (local to current shell):
DB_HOST="prod-db.internal"
DB_PORT=5432

# Export (make available to child processes):
export DB_HOST="prod-db.internal"
export DB_PORT=5432

# Use variables:
echo "Connecting to $DB_HOST:$DB_PORT"
psql -h $DB_HOST -p $DB_PORT -d analytics

# View all environment variables:
env                    # All exported variables
printenv DB_HOST       # Specific variable value
echo $DB_HOST          # Same thing
```

---

## Common Built-In Variables

| Variable | Purpose | Example Value |
|----------|---------|---------------|
| `$HOME` | User's home directory | /home/dataeng |
| `$USER` | Current username | dataeng |
| `$PATH` | Executable search path | /usr/local/bin:/usr/bin:/bin |
| `$PWD` | Current working directory | /opt/etl |
| `$SHELL` | Default shell | /bin/bash |
| `$HOSTNAME` | Machine hostname | etl-server-01 |
| `$?` | Last command exit code | 0 (success) |
| `$$` | Current script PID | 12345 |
| `$!` | Last background PID | 12346 |
| `$0` | Script name | ./daily_etl.sh |
| `$1, $2...` | Script arguments | first_arg, second_arg |
| `$#` | Number of arguments | 3 |
| `$@` | All arguments (as array) | arg1 arg2 arg3 |

---

## Setting Variables for Data Pipelines

### .env Files

```bash
# .env file (NOT committed to git — contains secrets!):
# /opt/etl/.env
export DB_HOST=prod-db.internal
export DB_PORT=5432
export DB_USER=etl_service
export DB_PASSWORD=super_secret_password
export S3_BUCKET=company-data-lake
export SLACK_WEBHOOK=https://hooks.slack.com/services/XXX
export ENVIRONMENT=production

# Load in your script:
source /opt/etl/.env
# OR: . /opt/etl/.env (same thing, shorter)

# Now all variables are available:
echo "Running in: $ENVIRONMENT"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d analytics
```

### Environment-Specific Configuration

```bash
#!/bin/bash
# Load config based on ENVIRONMENT variable

ENVIRONMENT="${ENVIRONMENT:-development}"  # Default to development

case "$ENVIRONMENT" in
    production)
        source /opt/etl/config/production.env
        ;;
    staging)
        source /opt/etl/config/staging.env
        ;;
    development)
        source /opt/etl/config/development.env
        ;;
    *)
        echo "ERROR: Unknown environment: $ENVIRONMENT"
        exit 1
        ;;
esac

echo "Running ETL in $ENVIRONMENT (DB: $DB_HOST)"
```

---

## Default Values and Validation

```bash
# Default value (use default if variable is unset or empty):
DB_HOST="${DB_HOST:-localhost}"           # If DB_HOST unset → use "localhost"
DB_PORT="${DB_PORT:-5432}"               # If DB_PORT unset → use 5432
MAX_RETRIES="${MAX_RETRIES:-3}"          # If unset → default 3

# Required variables (fail if not set):
: "${DB_HOST:?ERROR: DB_HOST must be set}"
: "${DB_PASSWORD:?ERROR: DB_PASSWORD must be set}"
# If DB_HOST is empty/unset → script exits with the error message!

# Full validation pattern:
validate_env() {
    local missing=()
    [ -z "${DB_HOST:-}" ] && missing+=("DB_HOST")
    [ -z "${DB_PASSWORD:-}" ] && missing+=("DB_PASSWORD")
    [ -z "${S3_BUCKET:-}" ] && missing+=("S3_BUCKET")
    
    if [ ${#missing[@]} -gt 0 ]; then
        echo "ERROR: Missing required environment variables:"
        printf '  - %s\n' "${missing[@]}"
        exit 1
    fi
}
validate_env
```

---

## Security Best Practices

```bash
# NEVER hardcode secrets in scripts!
# BAD:
DB_PASSWORD="my_secret_password"  # Committed to git → exposed!

# GOOD: use .env files (gitignored) or secret managers:
source /opt/etl/.env  # .env is in .gitignore!

# BETTER: use secret manager (AWS Secrets Manager, Vault):
DB_PASSWORD=$(aws secretsmanager get-secret-value --secret-id prod/db/password --query SecretString --output text)

# Prevent secrets from appearing in logs:
set +x  # Disable trace mode before handling secrets
export DB_PASSWORD=$(get_secret "db_password")
set -x  # Re-enable trace mode

# Prevent secrets in process list (ps aux):
# BAD: pass password as command-line argument
mysql -p"$DB_PASSWORD" ...  # Visible in ps aux!
# GOOD: use environment variable or config file
export MYSQL_PWD="$DB_PASSWORD"
mysql ...  # Password not visible in process list

# .env file permissions:
chmod 600 /opt/etl/.env  # Only owner can read (not group, not others!)
```

---

## PATH Management

```bash
# PATH: where the shell looks for executable commands
echo $PATH
# /usr/local/bin:/usr/bin:/bin

# Add custom directory to PATH:
export PATH="/opt/etl/bin:$PATH"
# Now scripts in /opt/etl/bin/ are runnable by name (no full path needed)

# Common issue: cron doesn't have your PATH!
# In crontab, set PATH explicitly:
PATH=/usr/local/bin:/usr/bin:/bin:/opt/etl/bin
0 6 * * * daily_etl.sh  # Now finds daily_etl.sh in /opt/etl/bin/

# Virtual environments (Python):
export PATH="/opt/etl/venv/bin:$PATH"
# Now 'python' and 'pip' resolve to the virtualenv versions
```

---

## Variable Scope

```bash
# Local variable (current shell only):
MY_VAR="hello"
# Child processes (subshells, scripts called from here) DON'T see MY_VAR

# Exported variable (inherited by child processes):
export MY_VAR="hello"
# Child processes DO see MY_VAR

# Subshell isolation:
(
    MY_LOCAL="inside subshell"
    export MY_EXPORTED="also inside"
)
echo $MY_LOCAL      # Empty! (subshell's local vars don't escape)
echo $MY_EXPORTED   # Empty! (export only goes DOWN, not UP to parent)

# Passing variables to a specific command (one-time):
DB_HOST=staging-db python etl.py
# DB_HOST is set ONLY for this python command, not permanently in shell
```

---

## Interview Tips

> **Tip 1:** "How do you manage configuration across environments?" — .env files per environment (production.env, staging.env, development.env). Load the right one based on an ENVIRONMENT variable. Never commit .env files to git (add to .gitignore). Same script code works in all environments — only the config differs.

> **Tip 2:** "How do you handle secrets in bash scripts?" — Never hardcode! Use: .env files (chmod 600, gitignored), or AWS Secrets Manager/Vault (fetch at runtime). Prevent leaks: `set +x` before handling secrets, use env vars not CLI args (visible in `ps`), and `unset` secrets after use.

> **Tip 3:** "What's the difference between `VAR=x` and `export VAR=x`?" — Without export: variable exists only in current shell (child processes can't see it). With export: variable is inherited by ALL child processes (scripts, commands you call). Rule: export anything that child processes need (DB_HOST, PATH); keep local what's script-internal only (loop counters, temp vars).
