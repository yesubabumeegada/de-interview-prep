---
title: "BigQuery — Real-World Case Studies"
topic: gcp
subtopic: bigquery
content_type: study_material
difficulty_level: mid-level
tags: [gcp, bigquery, interview]
---

# BigQuery — Real-World Case Studies

Three production stories you can adapt when an interviewer says "tell me about a time you optimized a data warehouse." Each includes the symptom, the investigation, the fix, and the numbers.

## Case Study 1: The $18,000/Month Dashboard

### Context

An e-commerce company (~400 GB/day of clickstream into a 60 TB events table) connected Looker Studio directly to the raw `events` table. Twelve dashboards, auto-refreshing every 15 minutes, each firing 8–12 queries.

### Symptom

Monthly on-demand bill climbed from ~$3,000 to ~$18,000 in one quarter. No single query looked expensive; the spend was death by a thousand scans.

### Investigation

```sql
-- Who/what is spending? Group by referencing tool + query hash
SELECT
  user_email,
  REGEXP_EXTRACT(query, r'FROM\s+`?([\w.-]+)`?') AS main_table,
  COUNT(*) AS runs,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 0) AS usd
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
GROUP BY user_email, main_table
ORDER BY usd DESC
LIMIT 20;
```

Findings:

- The dashboard service account billed 2.4 PB scanned/month.
- Every tile queried raw `events` with `SELECT *`-style generated SQL.
- The table was partitioned, but tiles used `TIMESTAMP_TRUNC(event_ts, WEEK) = ...` patterns that defeated pruning in several tiles.

### Fix

1. Built three **materialized views** for the aggregates the dashboards actually displayed (daily revenue, funnel counts, top products) — partitioned and clustered.
2. Enabled **BI Engine** (20 GB reservation, ~$600/mo) over the MV dataset.
3. Set `require_partition_filter = TRUE` on the raw table.
4. Added a 1 TiB/day custom query quota for the dashboard service account as a tripwire.

### Outcome

| Metric | Before | After |
|--------|--------|-------|
| Monthly compute spend | $18,000 | $2,100 (incl. BI Engine) |
| Median tile latency | 6–9 s | 0.4 s |
| Bytes scanned/month | 2.4 PB | 31 TB |

Interview soundbite: "Dashboards should never read raw fact tables — put a materialized view boundary between BI and raw data, then make raw tables refuse unfiltered scans."

## Case Study 2: Streaming Pipeline Duplicates After a Region Incident

### Context

A fintech streamed payment events to BigQuery via the legacy `insertAll` API from a fleet of consumers. During a 40-minute network incident, clients retried aggressively.

### Symptom

Finance reconciliation flagged ~0.7% of daily payments double-counted. `insertId`-based dedup hadn't held — it's best-effort within a ~1-minute window, and retries landed outside it.

### Debugging Story

```sql
-- Quantify duplicates by business key
SELECT
  payment_id,
  COUNT(*) AS copies
FROM ds.payments_raw
WHERE DATE(ingest_ts) = '2026-03-14'
GROUP BY payment_id
HAVING COUNT(*) > 1;
-- 41,832 payment_ids with 2+ copies
```

Immediate remediation — rebuild the day with dedup, using time travel as the safety net:

```sql
CREATE SNAPSHOT TABLE ds.payments_raw_backup
CLONE ds.payments_raw;

MERGE ds.payments_clean t
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY ingest_ts DESC
      ) AS rn
    FROM ds.payments_raw
    WHERE DATE(ingest_ts) = '2026-03-14'
  )
  WHERE rn = 1
) s
ON t.payment_id = s.payment_id
WHEN NOT MATCHED THEN INSERT ROW;
```

### Permanent Fix

1. Migrated producers to the **Storage Write API committed stream with offsets** — exactly-once at the API level.
2. Kept a downstream defensive dedup in the silver layer (`ROW_NUMBER` over business key) because exactly-once at ingestion doesn't protect against producer-side logic bugs.
3. Added a daily data-quality query alerting if duplicate rate > 0.01%.

### Outcome

- Streaming cost dropped ~50% as a side effect ($0.05/GB → $0.025/GB; ~9 TB/month streamed: ~$450 → ~$225).
- Zero duplicate incidents in the following year.

Interview soundbite: "insertId dedup is best-effort, not a guarantee. Exactly-once needs the Storage Write API with offsets — and even then, keep an idempotent MERGE downstream."

## Case Study 3: The 4-Hour Nightly Batch That Missed Its SLA

### Context

A logistics company's nightly ELT (orchestrated by Composer/Airflow) transformed ~3 TB across ~140 queries. SLA: warehouse ready by 06:00. It started finishing at 08:30 as data grew.

### Investigation

```sql
SELECT
  job_id,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS runtime_s,
  total_slot_ms / 1000 / NULLIF(TIMESTAMP_DIFF(end_time, start_time, SECOND), 0)
    AS avg_slots,
  total_bytes_processed / POW(1024, 3) AS gib
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND job_type = 'QUERY'
ORDER BY runtime_s DESC
LIMIT 10;
```

Findings:

- Top query: a `MERGE` into a 9 TB un-clustered target, scanning the full target nightly (75 min, 9.2 TB billed ≈ $57/night just for that statement).
- Average slots during the window: ~1,900 — pinned at the on-demand soft cap. Queries were queuing on slots, not on data.
- Several DAG tasks ran serially out of habit, not dependency.

### Fix

1. **Clustered the MERGE target** on the merge key and added a partition filter to the MERGE `ON`/`WHEN` conditions so only recent partitions were rewritten:

```sql
MERGE ds.shipments t
USING staging.shipments_delta s
ON t.shipment_id = s.shipment_id
   AND t.ship_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ROW;
```

MERGE billed bytes: 9.2 TB → 410 GB. Runtime: 75 → 7 minutes.

2. Bought an **Enterprise reservation: baseline 500, autoscale to 1,500** for the 00:00–06:00 window's project, removing the on-demand concurrency cap and making throughput predictable.
3. Re-parallelized the DAG (Airflow `max_active_tasks` raised; false dependencies removed).

### Outcome

| Metric | Before | After |
|--------|--------|-------|
| Batch wall-clock | 8.5 h | 2 h 40 m |
| Nightly compute cost | ~$310 (on-demand) | ~$190 (slots, amortized) |
| SLA misses/month | 12 | 0 |

Interview soundbite: "The execution graph told us we were slot-bound, not data-bound — so the fix was a reservation plus making the biggest MERGE partition-aware, not rewriting all 140 queries."

## Patterns Across All Three Cases

1. **Always start in `INFORMATION_SCHEMA.JOBS`** — it answers who, what, how many bytes, how many slots.
2. **Cost and latency share the same levers**: scan less (partition, cluster, project columns), precompute (MVs), and control concurrency (reservations, quotas).
3. **Guardrails are part of the fix**: `require_partition_filter`, `maximum_bytes_billed`, custom quotas, DQ alerts — assume the bad pattern will return.
4. **Time travel + snapshots make remediation safe** — clone before you rewrite history.
