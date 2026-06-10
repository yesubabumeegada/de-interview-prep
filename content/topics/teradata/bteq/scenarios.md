---
title: "Teradata - BTEQ Scenarios"
topic: teradata
subtopic: bteq
content_type: scenario_question
difficulty_level: senior
layer: scenarios
tags: [teradata, bteq, scenarios, etl, error-handling, scripting]
---

# BTEQ — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Write a Basic BTEQ ETL Script

Write a BTEQ script that:
1. Connects to Teradata
2. Deletes yesterday's rows from `fact.daily_sales` (where `sale_date = CURRENT_DATE - 1`)
3. Inserts from `staging.daily_sales` where `sale_date = CURRENT_DATE - 1`
4. Exits with code 0 on success and code 8 on any error

<details>
<summary>💡 Hint</summary>

You need .LOGON, SQL statements, .IF ERRORCODE checks after each SQL step, .GOTO for error handling, .LOGOFF, and .QUIT with appropriate codes.

</details>

<details>
<summary>✅ Solution</summary>

```bteq
.LOGON myserver/etl_user,etl_password;

-- Step 1: Delete existing data for yesterday (idempotent)
DELETE FROM fact.daily_sales
WHERE sale_date = CURRENT_DATE - 1;

.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

-- Step 2: Insert from staging
INSERT INTO fact.daily_sales
SELECT *
FROM staging.daily_sales
WHERE sale_date = CURRENT_DATE - 1;

.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

-- Success
.LOGOFF;
.QUIT 0;

-- Error handler
.LABEL HANDLE_ERROR
.REMARK "ERROR: BTEQ script failed with ERRORCODE = " ;
.LOGOFF;
.QUIT 8;
```

**Key elements explained:**
- `.LOGON`: Connect to Teradata with host/user,password syntax
- `DELETE` then `INSERT`: Delete-then-insert pattern makes the script restartable
- `.IF ERRORCODE <> 0 THEN .GOTO`: Checks after EVERY SQL statement
- `.LOGOFF`: Always disconnect before exiting
- `.QUIT 0` / `.QUIT 8`: Communicate success/failure to the calling scheduler via exit code

**Common mistakes to avoid:**
- Forgetting `.LOGOFF` before `.QUIT` (leaves dangling session)
- Checking ERRORCODE only at the end (misses which step failed)
- Using `.QUIT` without `.LOGOFF` (abrupt disconnect)

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: BTEQ Failure Diagnosis

A production BTEQ script fails at 2 AM with exit code 8. You are the on-call engineer. The script runs a multi-step ETL pipeline:

```bteq
.LOGON ${HOST}/${USER},${PASS};

DELETE FROM fact.orders WHERE load_date = CURRENT_DATE - 1;
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

INSERT INTO fact.orders SELECT * FROM staging.orders WHERE load_date = CURRENT_DATE - 1;
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

CALL sp_update_customer_summary(CURRENT_DATE - 1);
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

COLLECT STATISTICS ON fact.orders COLUMN (load_date);
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

.LOGOFF;
.QUIT 0;

.LABEL ABORT
.LOGOFF;
.QUIT 8;
```

The audit log shows: `status='FAILED', errorcode=NULL`. You can't tell which step failed. How do you diagnose and fix — both immediately and permanently?

<details>
<summary>✅ Solution</summary>

**Immediate diagnosis (first 15 minutes):**

**Step 1: Check what the script wrote to its log file**
```bash
tail -200 /var/log/etl/orders_load_$(date +%Y%m%d).log
```
Look for the last SQL statement output before the ABORT section ran.

**Step 2: Check Teradata session log (DBQL)**
```sql
SELECT LogDate, LogTime, UserName, ErrorCode, ErrorText,
       SUBSTR(QueryText, 1, 200) AS QueryPreview
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE
  AND UserName = 'ETL_USER'
  AND ErrorCode <> 0
ORDER BY LogTime DESC;
```
This will show exactly which SQL statement failed and what the Teradata error was.

**Step 3: Check if staging table has data**
```sql
SELECT COUNT(*) FROM staging.orders WHERE load_date = CURRENT_DATE - 1;
```
If 0 rows → upstream feed failed, not the BTEQ script itself.

**Step 4: Check the stored procedure status**
```sql
-- Did sp_update_customer_summary run?
SELECT * FROM etl.procedure_log
WHERE proc_name = 'sp_update_customer_summary'
  AND run_date = CURRENT_DATE - 1
ORDER BY run_ts DESC;
```

**Immediate fix for tonight:**
Once you identify the failing step, fix the root cause (e.g., staging table empty → wait for upstream or run manually), then re-run the script. The delete-then-insert pattern makes it safe to rerun.

**Permanent fix — add proper error logging to the script:**

```bteq
.LOGON ${HOST}/${USER},${PASS};

-- Log start
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP, 'orders_etl', 'STARTED', USER, NULL, NULL
);

DELETE FROM fact.orders WHERE load_date = CURRENT_DATE - 1;
.IF ERRORCODE <> 0 THEN .GOTO ABORT_STEP1;

INSERT INTO fact.orders SELECT * FROM staging.orders WHERE load_date = CURRENT_DATE - 1;
.IF ERRORCODE <> 0 THEN .GOTO ABORT_STEP2;

CALL sp_update_customer_summary(CURRENT_DATE - 1);
.IF ERRORCODE <> 0 THEN .GOTO ABORT_STEP3;

COLLECT STATISTICS ON fact.orders COLUMN (load_date);

UPDATE etl.pipeline_log SET status = 'COMPLETED', end_ts = CURRENT_TIMESTAMP
WHERE pipeline_name = 'orders_etl' AND status = 'STARTED';
.LOGOFF;
.QUIT 0;

.LABEL ABORT_STEP1
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP, 'orders_etl', 'FAILED_STEP1',
    USER, CAST(ERRORCODE AS VARCHAR(10)), NULL
);
.LOGOFF;
.QUIT 8;

.LABEL ABORT_STEP2
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP, 'orders_etl', 'FAILED_STEP2',
    USER, CAST(ERRORCODE AS VARCHAR(10)), NULL
);
.LOGOFF;
.QUIT 8;

.LABEL ABORT_STEP3
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP, 'orders_etl', 'FAILED_STEP3',
    USER, CAST(ERRORCODE AS VARCHAR(10)), NULL
);
.LOGOFF;
.QUIT 8;
```

Now the audit log tells you exactly which step failed and the Teradata error code. Diagnosis goes from 30 minutes to 30 seconds.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Redesigning a Legacy BTEQ Pipeline

You've inherited a 5-year-old ETL pipeline consisting of 15 BTEQ scripts. It currently:
- Loads 5 GB of data nightly (used to be fine, now barely finishes in 8 hours)
- Has no centralized audit logging (each script has its own ad-hoc .REMARK statements)
- Uses hardcoded credentials in script files (stored on a shared server)
- Has no restart capability (re-running any script can cause duplicate data)
- Is orchestrated via cron (no dependency management, fixed timing)

Design a modernized version of this pipeline architecture. You cannot immediately replace BTEQ — the team needs to continue using it while improvements are made incrementally.

<details>
<summary>💡 Hint</summary>

Think about: what changes are purely operational (no code changes needed)? What requires script modifications? What requires infrastructure additions? What's the priority order for risk reduction vs performance improvement?

</details>

<parameter name="✅ Solution">

**Priority 1: Security (immediate, no downtime required)**

Move credentials out of scripts TODAY:

```bash
# Set up a credential file with restricted permissions
cat > /secure/td_credentials.env <<EOF
TD_HOST=teradata-prod.company.com
TD_USER=etl_prod_user
TD_PASS=$(vault read -field=password secret/teradata/etl-prod)
EOF
chmod 600 /secure/td_credentials.env

# Update all shell wrappers to source credentials
source /secure/td_credentials.env
bteq < script.bteq
```

No BTEQ script changes needed. Immediate risk reduction.

**Priority 2: Centralized Audit Logging (medium effort)**

Create a shared audit table and wrapper:

```sql
-- Create once in Teradata
CREATE TABLE etl.pipeline_log (
    log_id       BIGINT GENERATED ALWAYS AS IDENTITY,
    pipeline     VARCHAR(100),
    step         VARCHAR(100),
    status       VARCHAR(20),  -- STARTED/COMPLETED/FAILED
    errorcode    VARCHAR(10),
    row_count    BIGINT,
    started_at   TIMESTAMP(0),
    ended_at     TIMESTAMP(0)
) PRIMARY INDEX (pipeline, started_at);
```

Add standard START/END/FAIL log inserts to each script incrementally (can be done one script per sprint).

**Priority 3: Restart Capability (high effort, highest risk reduction)**

Systematically add delete-then-insert pattern to each script:

```bteq
-- Before (not restartable):
INSERT INTO fact.sales SELECT * FROM stg.sales;

-- After (restartable):
DELETE FROM fact.sales WHERE load_date = :load_date;
INSERT INTO fact.sales SELECT * FROM stg.sales WHERE load_date = :load_date;
```

Do this during normal maintenance cycles — each script gets updated when it needs modification for another reason (don't touch what isn't broken, change carefully).

**Priority 4: Performance (requires investigation per script)**

For the volume problem (5GB barely in 8 hours):
1. Run DBQL analysis to find the slowest steps
2. Check statistics freshness on all tables touched by the pipeline
3. For steps using BTEQ .IMPORT with large files: replace with FastLoad calls
4. Add PPI to fact tables if not present

```bash
# Shell wrapper that calls TPT for large loads, BTEQ for logic
load_data_tpt.sh      # handles 5GB file load in parallel
bteq < transform_and_aggregate.bteq   # handles SQL transformations
```

**Priority 5: Orchestration (infrastructure change)**

Replace cron with proper job scheduler (Autosys, Airflow, Control-M):
- Dependency-based triggering (job B starts only when A succeeds)
- Automatic retry on transient failures
- SLA monitoring and alerting
- Historical run tracking and reporting

**Implementation roadmap:**

```
Month 1: Security (credential externalization) — no code changes, immediate
Month 2: Audit logging — add to 5 highest-criticality scripts
Month 3: Restart capability — add to all scripts touching fact tables
Month 4: Performance — profile and fix slowest steps
Month 5: Orchestration — migrate from cron to Airflow/Control-M
Month 6: Full audit logging across all 15 scripts
```

**Key principle:** Never change a working pipeline completely at once. Incremental improvements with proper testing reduce blast radius. The security fix is the only truly urgent change — everything else can be phased.


---

## ⚡ Quick-fire Q&A

**Q: What is BTEQ and what is it used for?**
A: BTEQ (Basic Teradata Query) is a Teradata command-line utility for submitting SQL queries and managing batch processing. It supports scripting with conditional logic, error handling, and export/import of data. BTEQ is the backbone of legacy Teradata ETL pipelines and administrative automation.

**Q: What is the ERRORCODE variable in BTEQ and how is it used?**
A: `ERRORCODE` is a BTEQ system variable that holds the return code of the most recently executed SQL statement (0 = success, non-zero = error). You can check it with `.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE` to halt script execution and propagate the error code to the calling shell script for error handling.

**Q: What is the difference between .EXPORT and .IMPORT in BTEQ?**
A: `.EXPORT` redirects query output to a file (flat file, CSV, or binary). `.IMPORT` reads data from a file and substitutes values into parameterized SQL statements. Neither is designed for high-throughput bulk loading—for that, FastLoad or MultiLoad is appropriate.

**Q: What does the .LOGON command do in BTEQ?**
A: `.LOGON` establishes a connection to the Teradata server with a specified host, username, and password. BTEQ scripts typically start with `.LOGON` and end with `.LOGOFF`. In production, credentials are often passed via environment variables or TDWALLET to avoid hardcoding in script files.

**Q: What is the SESSIONS parameter in BTEQ and how does it affect performance?**
A: BTEQ operates with a single session by default. Unlike FastLoad or MultiLoad, BTEQ cannot use multiple sessions to parallelize data loading—this is a fundamental limitation for bulk data movement. For multi-session parallel loading, use FastLoad, MultiLoad, or JDBC/ODBC-based tools.

**Q: How does BTEQ handle multi-statement requests?**
A: BTEQ submits SQL statements terminated by a semicolon one at a time by default, or as a multi-statement request (MSR) when multiple statements are separated by semicolons without `.QUIT` between them. MSRs are sent to Teradata as a single unit, which can improve performance by reducing round-trips.

**Q: What are the main alternatives to BTEQ in modern Teradata environments?**
A: Modern alternatives include Teradata Studio (GUI), JDBC/ODBC drivers for programmatic access, dbt-teradata for transformation pipelines, Teradata Parallel Transporter (TPT) for high-throughput data movement, and Python teradataml or teradatasql libraries. BTEQ remains common in legacy systems but new pipelines favor these more flexible options.

**Q: What is the .LABEL and .GOTO directive in BTEQ?**
A: `.LABEL` defines a named point in the BTEQ script. `.GOTO label_name` jumps execution to that label, similar to a GOTO statement. This enables simple conditional branching: check ERRORCODE after a SQL step and jump to an error-handling section or skip optional steps. It's a form of procedural control without full programming language features.

---

## 💼 Interview Tips

- Frame BTEQ knowledge as legacy expertise that you've maintained while also modernizing pipelines. Companies still running BTEQ scripts value someone who can maintain them and knows when to migrate to TPT or Python-based tools.
- Know ERRORCODE handling cold—it's the primary error management mechanism in BTEQ ETL scripts. Showing you've built reliable pipelines with proper error checking (`.IF ERRORCODE <> 0 THEN`) signals production experience.
- Differentiate BTEQ from FastLoad/MultiLoad clearly: BTEQ is for interactive queries and simple scripts; FastLoad/MultiLoad are for high-throughput bulk loading. Using BTEQ for loading millions of rows is an anti-pattern that senior interviewers will probe.
- Mention security considerations: hardcoded passwords in BTEQ scripts are a compliance risk. Discuss TDWALLET or environment variable substitution as the secure alternative.
- If the company is modernizing, show willingness to migrate BTEQ scripts to Teradata Parallel Transporter (TPT) or dbt—this bridges legacy knowledge with modern DE practices and is a strong differentiator.
