---
title: "BigQuery — Interview Scenarios"
topic: gcp
subtopic: bigquery
content_type: scenario_question
tags: [gcp, bigquery, interview]
---

# BigQuery — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: The Query That Cost the Same With LIMIT 10

**Scenario:** A teammate runs `SELECT * FROM analytics.events LIMIT 10` against a 5 TB table "just to peek at the data" and is shocked when the job shows ~5 TB processed. They ask you: why did a 10-row query scan the whole table, and what should they do instead to explore tables cheaply?

<details>
<summary>💡 Hint</summary>

Think about *when* billing is determined in BigQuery's on-demand model — is it based on rows returned or data read? Also consider what columnar storage means for `SELECT *`, and whether BigQuery offers any free ways to inspect a table's contents and schema.

</details>

<details>
<summary>✅ Solution</summary>

**Why it happened:** On-demand BigQuery bills by **bytes scanned**, not bytes returned. `LIMIT` is applied *after* reading; it doesn't reduce the scan. And `SELECT *` reads every column — in columnar storage, each column you name adds to the scan, so `*` is the worst case.

**Cheap ways to explore:**

```sql
-- 1. Free: table preview (console "Preview" tab) or tabledata.list
-- 2. Free: schema + row counts from metadata
SELECT table_name, row_count, size_bytes / POW(1024,3) AS gib
FROM analytics.__TABLES__;

-- 3. Cheap: select only the columns you need + partition filter
SELECT event_name, event_ts
FROM analytics.events
WHERE DATE(event_ts) = CURRENT_DATE()
LIMIT 100;
```

```bash
# 4. Always know the cost before running
bq query --dry_run --nouse_legacy_sql \
  'SELECT event_name FROM analytics.events WHERE DATE(event_ts) = CURRENT_DATE()'
```

**Guardrails to suggest:**

| Guardrail | Effect |
|-----------|--------|
| `require_partition_filter=TRUE` | Rejects unfiltered queries on the table |
| `--maximum_bytes_billed` | Job fails instead of overspending |
| Custom per-user daily quota | Caps any single person's blast radius |

Key sentence for the interview: "Preview is free, dry-run is free, and `LIMIT` is not a cost control."

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design the Table for a 200 GB/Day Event Stream

**Scenario:** You're ingesting ~200 GB/day of app events (fields: `event_ts`, `user_id`, `event_name`, `country`, `payload` JSON). Analysts run two query shapes: (a) daily aggregates over the last 7–90 days, and (b) "all events for user X in the last month." Design the BigQuery table: partitioning, clustering, retention, and the ingestion method. Justify each choice and estimate the cost impact.

<details>
<summary>💡 Hint</summary>

Match each query shape to a pruning mechanism: which column do both queries filter on, and which high-cardinality column does query (b) filter on? Also think about the 10,000-partition limit when picking granularity, and whether streaming is actually required or batch loading would do.

</details>

<details>
<summary>✅ Solution</summary>

**DDL:**

```sql
CREATE TABLE analytics.events
(
  event_ts   TIMESTAMP NOT NULL,
  user_id    STRING NOT NULL,
  event_name STRING,
  country    STRING,
  payload    JSON
)
PARTITION BY DATE(event_ts)
CLUSTER BY user_id, event_name
OPTIONS (
  partition_expiration_days = 365,
  require_partition_filter = TRUE
);
```

**Justification:**

| Choice | Why |
|--------|-----|
| Daily partitions on `event_ts` | Both query shapes filter on time; 365 daily partitions is far under the 10,000 limit |
| Cluster by `user_id` first | Query (b) is an equality filter on a high-cardinality column — block pruning makes "user X last month" scan MBs instead of ~6 TB of monthly partitions |
| `event_name` second cluster key | Helps query (a) when aggregating specific events |
| 365-day expiration | Free retention enforcement; reduces storage from unbounded growth |
| `require_partition_filter` | A 73 TB/year table must not allow accidental full scans |

**Ingestion:** if freshness needs are ≥ hourly, batch-load files from GCS — load jobs are **free**. If sub-minute freshness is required, use the **Storage Write API default stream** (~$0.025/GB → 200 GB/day ≈ $5/day ≈ $150/month). Avoid legacy insertAll (2x cost, weaker guarantees).

**Cost sketch (on-demand):**

- Query (a) over 30 days without design: scans 30 × 200 GB = 6 TB ≈ $37.50 per run.
- With column projection (3 columns ≈ 25% of bytes): ~1.5 TB ≈ $9.40.
- Query (b) with clustering: from ~6 TB down to a few hundred MB — effectively cents.

Mention as a bonus: if analysts run query (a) on dashboards many times daily, add a materialized view of daily aggregates so the 6 TB scan happens incrementally once, not per viewer.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: The Reservation Migration That Made Everything Slower

**Scenario:** Your company moved from on-demand to a 500-slot Enterprise reservation (no autoscale) to make costs predictable. Two weeks later: the nightly ETL still meets SLA, but daytime ad-hoc analyst queries that took 5 seconds now take 90+ seconds, and one team's ML feature-generation query takes 3 hours instead of 40 minutes. Leadership refuses to go back to on-demand's unpredictable bills. Diagnose what happened and design a fix using BigQuery's workload management primitives.

<details>
<summary>💡 Hint</summary>

Compare what concurrency the project effectively had under on-demand versus a fixed 500-slot pool shared by everyone, and recall how the fair scheduler divides slots among concurrent queries. Then think about which primitives — multiple reservations, assignments, baseline vs autoscale, idle-slot sharing — let you isolate workloads instead of letting them fight.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis:** Under on-demand, the project could burst toward ~2,000 slots. Moving to a fixed 500-slot reservation cut peak capacity ~4x, and the **fair scheduler** now splits those 500 slots across *all* concurrent workloads: when the ML job (which can consume thousands of slot-hours) runs alongside ad-hoc queries, each query gets a small fair share, so everything slows. The nightly ETL is fine because it runs alone at night.

Verify with data:

```sql
SELECT
  TIMESTAMP_TRUNC(period_start, HOUR) AS hr,
  reservation_name,
  AVG(period_slot_ms) / (1000 * 60 * 60) AS avg_slots_used
FROM `region-us`.INFORMATION_SCHEMA.RESERVATION_TIMELINE
GROUP BY hr, reservation_name
ORDER BY hr;
-- Expect: pegged at 500 during business hours = slot starvation
```

**Fix — workload isolation with assignments and autoscale:**

```text
Admin project
├── res_etl      : baseline 0,   autoscale max 800  (assigned: etl project)
├── res_adhoc    : baseline 100, autoscale max 300  (assigned: analytics folder)
└── res_ml       : baseline 0,   autoscale max 600  (assigned: ml project)
```

```bash
bq mk --reservation --edition=ENTERPRISE \
  --slots=100 --autoscale_max_slots=300 \
  --location=US res_adhoc

bq mk --reservation_assignment \
  --reservation_id=admin-proj:US.res_adhoc \
  --assignee_type=FOLDER --assignee_id=123456 \
  --job_type=QUERY --location=US
```

Key design points:

1. **Separate reservations per workload class** — the ML job can no longer starve analysts; blast radius is contained.
2. **Autoscale with low baselines** — pay near-zero overnight for daytime pools; ETL pool scales from 0 at midnight. Costs stay capped (max slots × hours) — still predictable for leadership.
3. **Idle slot sharing** (default on) lets the ad-hoc pool borrow from idle ETL baseline during the day if you keep any baseline there; disable it for the ML pool if you want hard isolation.
4. Optionally route truly spiky exploratory users back to **on-demand with per-user custom quotas** (e.g., 2 TiB/day) — predictability via quotas rather than reservations.

**Outcome framing:** total max spend = sum of autoscale caps, which finance can budget; latency isolation comes from assignments, not from buying more total slots. The original failure was treating one shared pool as equivalent to on-demand's burst capacity.

</details>

</article>

## Interview Tips

> **Tip 1:** "How do you control BigQuery costs?" is near-guaranteed — answer in layers: query design (columns, partition filters), table design (partitioning, clustering, expiration), precompute (materialized views), then governance (quotas, `maximum_bytes_billed`, reservations). Naming the governance layer is what separates mid from senior answers.

> **Tip 2:** When asked "partitioning vs clustering," don't just define them — give the pairing rule: partition on the time column for pruning and lifecycle, cluster on high-cardinality filter/join columns, and mention that dry-run estimates ignore cluster pruning.

> **Tip 3:** For "how would you debug a slow query," walk the execution graph: check `wait_ms` (slot starvation) vs `shuffle_output_bytes` (join explosion/skew) vs bytes scanned (missing pruning). Interviewers want a diagnostic order, not a list of random fixes.

## ⚡ Quick-fire Q&A

**Q:** What does a BigQuery "slot" represent?
A: A unit of compute (CPU + memory) used to execute query work units; queries get slots from the on-demand pool or your reservation via a fair scheduler.

**Q:** Does `LIMIT` reduce on-demand cost?
A: No — billing is by bytes scanned. Only column projection, partition pruning, and cluster pruning reduce the scan.

**Q:** Max clustering columns and max partitions per table?
A: 4 clustering columns; 10,000 partitions.

**Q:** Storage Write API vs insertAll in one sentence?
A: Write API is gRPC, ~half the cost, higher throughput, and supports exactly-once via stream offsets; insertAll is legacy best-effort dedup.

**Q:** How far back does time travel go?
A: Configurable 2–7 days (default 7), plus a 7-day fail-safe recoverable via support.

**Q:** Can a materialized view return stale results?
A: No — BigQuery merges the MV with the delta from the base table, so results are always current; staleness only affects how much is computed.

**Q:** Why might dry-run overestimate cost on a clustered table?
A: Estimates can't account for cluster (block-level) pruning, so they show the worst-case partition scan.

**Q:** When is BigQuery the wrong choice?
A: High-QPS point lookups, OLTP, frequent single-row mutations, or sub-100ms latency SLOs — use Bigtable, AlloyDB, or Cloud SQL instead.
