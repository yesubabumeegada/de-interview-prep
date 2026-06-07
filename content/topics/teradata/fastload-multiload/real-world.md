---
title: "Teradata - FastLoad and MultiLoad Real World"
topic: teradata
subtopic: fastload-multiload
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, fastload, multiload, tpt, production, throughput, case-study]
---

# FastLoad and MultiLoad — Real World

## Case Study: Telecom CDR Processing at Scale

**Company:** A major US carrier processing 500 million Call Detail Records (CDRs) daily.

**Architecture:**
```
1. CDR streams from switches → Kafka topics (real-time)
2. Kafka consumer writes CDRs to flat files (batched hourly)
3. FastLoad loads hourly batches into staging table (empty, NoPI)
4. INSERT/SELECT: staging → CDR fact table (with proper PI = subscriber_id)
5. MultiLoad: Apply enrichment updates (rate plan lookup, roaming flags)
```

**FastLoad configuration for CDR load:**
```fastload
SESSIONS 32;        -- 256 AMPs, 32 sessions = 8 AMPs per session
PACK 2000;          -- Maximize block size for throughput
ERRLIMIT 0;         -- Fail on any error (CDR data must be complete)

LOGON td-cdr/etl_user,pass;

DATABASE cdr;

BEGIN LOADING stg_cdrs_hourly
    ERRORFILES stg_cdrs_et, stg_cdrs_uv
    CHECKPOINT 500000;

SET RECORD VARTEXT '|';

DEFINE
    call_id         (BIGINT),
    subscriber_id   (BIGINT),
    called_number   (CHAR(15)),
    call_start       (TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS'),
    duration_sec     (INTEGER),
    call_type        (CHAR(5)),
    cell_id          (INTEGER);

FILE = /data/cdrs/${HOUR_STAMP}.dat;

INSERT INTO stg_cdrs_hourly VALUES (
    :call_id, :subscriber_id, :called_number,
    :call_start, :duration_sec, :call_type, :cell_id
);

END LOADING;
LOGOFF;
```

**Results:** 500M CDRs loaded in 4.5 hours (110M rows/hour) — comfortably within the 8-hour batch window.

---

## Case Study: Retail Price Update via MultiLoad

**Company:** A major retailer updating 50 million product prices daily.

**Challenge:** The `product_prices` table has 500 million rows (full catalog history). 50 million rows need to be updated every morning from a pricing system extract. The update must complete before store systems connect at 6 AM.

**Solution:** MultiLoad with split jobs:

```bash
#!/bin/bash
# Split 50M price update file into 5 batches of 10M each
split -l 10000000 /data/price_updates.dat /data/price_batch_

# Run 5 sequential MultiLoad jobs (sequential to avoid lock conflicts)
for batch_file in /data/price_batch_*; do
    multiload < run_price_update.ml FILENAME=${batch_file}
    if [ $? -ne 0 ]; then
        echo "MultiLoad failed for ${batch_file}" | alert_team
        exit 1
    fi
    echo "Completed batch: ${batch_file}"
done
```

**Each MultiLoad job:**
- Phase 1 (Acquisition): ~15 minutes to read 10M rows into work tables
- Phase 2 (Application): ~8 minutes of table-level write lock
- Total per batch: ~23 minutes
- Total for all 5 batches: ~115 minutes (< 2 hours)

**vs alternative (single MultiLoad job):**
- Phase 2 lock: ~40 minutes on 50M-row update
- Risk: any failure requires full restart of 50M rows

**Why split:** 5 smaller jobs with 8-minute lock windows vs one 40-minute lock. Analysts can read prices between batches.

---

## TPT Migration Case Study

**Company:** Insurance company's policy data warehouse.

**Before (FastLoad + BTEQ):**
```bash
# Old pipeline: 3 separate scripts, 2 hours total
run_fastload_staging.sh       # 45 min: FastLoad into empty staging
run_bteq_transform.sh         # 60 min: BTEQ INSERT/SELECT + transformations
run_fastexport_output.sh      # 15 min: FastExport results to downstream system
```

**After (TPT):**
```tpt
DEFINE JOB insurance_policy_etl
DESCRIPTION 'End-to-end policy data processing'
(
    DEFINE OPERATOR src_files TYPE DATACONNECTOR PRODUCER
    ATTRIBUTES (FileName = '/data/policies_*.csv', Format = 'Delimited');

    DEFINE OPERATOR stage_loader TYPE LOAD
    ATTRIBUTES (TargetTable = 'ins.stg_policies', ...);

    DEFINE OPERATOR transform_step TYPE SQL SELECTOR
    ATTRIBUTES (
        SelectStmt = 'SELECT policy_id, normalize_phone(phone_raw) AS phone,
                             CASE WHEN premium > 10000 THEN ''LARGE'' ELSE ''STANDARD'' END AS segment
                      FROM ins.stg_policies'
    );

    DEFINE OPERATOR fact_loader TYPE UPDATE
    ATTRIBUTES (TargetTable = 'ins.fact_policies', ...);

    APPLY TO OPERATOR (stage_loader)
    SELECT * FROM OPERATOR (src_files);

    APPLY TO OPERATOR (fact_loader)
    SELECT * FROM OPERATOR (transform_step);
);
```

**Results:** 
- Runtime: 45 minutes (75% reduction)
- Single job, single restart point
- No intermediate files needed
- Parallel load and export combined in one job

---

## Monitoring and Alerting for Load Jobs

```sql
-- Monitor active FastLoad/MultiLoad sessions
SELECT SessionNo, UserName, ReqPhysIO, AMPCPUTime, ElapsedTime
FROM DBC.SessionInfoV
WHERE UserName = 'etl_fastload_user'
  AND State = 'Active';

-- Check load throughput (rows per second via DBQL)
SELECT
    LogTime,
    NumResultRows / ElapsedTime AS RowsPerSec
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE
  AND UserName = 'etl_fastload_user'
ORDER BY LogTime;

-- Monitor spool usage during MultiLoad work table operations
SELECT UserName, SUM(CurrentSpool) / 1e9 AS SpoolGB
FROM DBC.SessionInfoV
WHERE UserName = 'etl_multiload_user'
GROUP BY UserName;
```

---

## Production Best Practices

1. **Always validate error tables after FastLoad:**
```bash
bteq <<EOF
.LOGON server/user,pass;
SEL COUNT(*) FROM ${TABLE}_et;
.IF ACTIVITYCOUNT > 0 THEN .QUIT 8;
SEL COUNT(*) FROM ${TABLE}_uv;
.IF ACTIVITYCOUNT > 0 THEN .QUIT 8;
.LOGOFF;
.QUIT 0;
EOF
```

2. **Pre-drop error tables before re-run:**
```sql
DROP TABLE IF EXISTS orders_et;
DROP TABLE IF EXISTS orders_uv;
```

3. **Use CHECKPOINT for large FastLoad jobs:**
```fastload
CHECKPOINT 1000000;  -- Checkpoint every 1M rows
-- Enables restart from last checkpoint on failure
```

4. **Monitor lock wait times for MultiLoad:**
```sql
SELECT UserName, WaitTime, LockType, LockedObject
FROM DBC.LockInfoV
WHERE UserName = 'etl_multiload_user';
```

---

## Interview Tips

> **Tip 1:** "What's your approach for loading 1 billion rows into Teradata?" — "Use FastLoad into an empty NoPI staging table (fastest possible throughput, bypasses all overhead). Then INSERT/SELECT into the final table with the proper Primary Index — Teradata redistributes rows correctly during this set-based operation. Split very large files into parallel FastLoad jobs if the batch window allows."

> **Tip 2:** "How do you handle a FastLoad that failed halfway through?" — "Don't drop the error tables or modify the target table. Re-run the FastLoad script — it reads its restart log and continues from the last checkpoint. If error tables exist from a previous run and you want a fresh start instead, drop them first, truncate/drop the target table, and restart from the beginning."

> **Tip 3:** "How do you minimize MultiLoad's impact on production availability?" — "Split large jobs into smaller batches to reduce individual Phase 2 lock windows. Schedule Phase 2 during low-traffic hours. For tables requiring 24/7 availability, consider TPT Stream (row-hash locks) or a dual-table approach (write to shadow table, rename at cutover)."

> **Tip 4:** "What monitoring do you put around FastLoad jobs in production?" — "Monitor session activity via DBC.SessionInfoV during load, validate error table row counts immediately post-load (fail pipeline if > 0 errors), log throughput metrics (rows/second) to detect degradation, and alert on jobs that run longer than historical baseline."
