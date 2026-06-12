---
title: "BigQuery — Senior Deep Dive"
topic: gcp
subtopic: bigquery
content_type: study_material
layer: senior-deep-dive
difficulty_level: senior
tags: [gcp, bigquery, interview]
---

# BigQuery — Senior Deep Dive

Senior interviews assume you know the features. What gets probed is *why the system behaves the way it does*: Dremel's execution model, shuffle, slot scheduling, storage internals (Capacitor), and the judgment calls — when BigQuery is the wrong tool, how to design for cost at petabyte scale, how to debug a slow query from the execution graph.

## Dremel Execution Model

A query compiles into a DAG of **stages**; each stage runs as many parallel **workers**, each worker occupying a slot while it executes work units.

- **Dynamic work rebalancing**: work units are small; if a worker is slow, remaining units are redistributed. This mitigates stragglers.
- **Stages communicate via shuffle**, an in-memory (spilling to disk) distributed layer built on Jupiter's bisection bandwidth. Shuffle is also how repartitioning between stages happens.
- The plan is **dynamic**: BigQuery can change the number of workers per stage and even re-plan mid-query (e.g., adjusting join strategies based on observed cardinality).

### Reading the Execution Graph

In the console (Query → Execution graph) or `INFORMATION_SCHEMA.JOBS`:

```sql
SELECT
  job_id,
  total_slot_ms,
  total_bytes_processed,
  ARRAY(
    SELECT AS STRUCT name, shuffle_output_bytes, records_read, records_written
    FROM UNNEST(job_stages)
  ) AS stages
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_id = 'bquxjob_12345';
```

What to look for:

| Symptom in graph | Diagnosis | Fix |
|------------------|-----------|-----|
| One stage with huge `wait_ms` | Slot starvation (concurrency) | Reservation/assignments, off-peak scheduling |
| `shuffle_output_bytes` ≫ input | Exploding join / skew | Fix join keys, pre-aggregate, filter earlier |
| Records read ≫ records written in early stage | Late filtering | Push predicates down, partition/cluster |
| Repartition stages repeated | Skewed keys | Salt hot keys, split query |

## Joins: Broadcast vs Hash (Shuffle)

- **Broadcast join**: small table replicated to every worker scanning the big table. No shuffle of the big side. Chosen when one side is small (tens of MB).
- **Hash join**: both sides shuffled on join key so matching keys co-locate. Required for big-big joins; cost dominated by shuffle.

Skew kills hash joins: if 30% of rows share one key (classic: `NULL` keys, "unknown" user), one worker gets 30% of the data.

```sql
-- Mitigation 1: handle NULLs separately
SELECT ... FROM a JOIN b ON a.k = b.k WHERE a.k IS NOT NULL
UNION ALL
SELECT ... FROM a LEFT JOIN b ON FALSE WHERE a.k IS NULL;

-- Mitigation 2: salt the hot key
SELECT ...
FROM (
  SELECT *, MOD(ABS(FARM_FINGERPRINT(CAST(RAND()*10 AS STRING))), 10) AS salt
  FROM big_a
) a
JOIN (
  SELECT b.*, s AS salt
  FROM big_b b, UNNEST(GENERATE_ARRAY(0, 9)) AS s
  WHERE b.k = 'HOT_KEY'
) b
ON a.k = b.k AND a.salt = b.salt;
```

## Storage Internals: Capacitor & the Metadata Layer

- Tables are stored as **Capacitor** files (successor to ColumnIO): columnar, heavily compressed, with run-length encoding, dictionary encoding, and reordering of rows *within* a file to maximize RLE effectiveness.
- Each column block carries min/max and other statistics — this is what enables **block pruning** for clustered tables.
- Storage is immutable: DML rewrites affected storage blocks (hence DML is heavyweight); a background **storage optimizer** compacts and re-clusters.
- **Time travel** (up to 7 days, configurable down to 2) plus **fail-safe** (7 more days, ops-ticket recovery only) are implemented over this immutable log of snapshots. Note: physical storage billing charges for time-travel bytes; logical billing does not — a real trade-off when choosing the dataset storage billing model.

### Logical vs Physical Storage Billing

| | Logical (default) | Physical |
|---|-------------------|----------|
| Billed on | Uncompressed bytes | Compressed bytes (+ time travel + fail-safe) |
| Price | Lower per GB | ~2x per GB, but compression often 4–10x |
| Wins when | Poor compression | High compression (logs, repetitive data) |

```sql
-- Compare before switching
SELECT
  table_name,
  SUM(active_logical_bytes) / POW(1024, 3) AS logical_gib,
  SUM(active_physical_bytes) / POW(1024, 3) AS physical_gib
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
GROUP BY table_name;
```

## Slot Scheduling & Fairness

- Slots are allocated via a **fair scheduler**: within a reservation, projects share fairly; within a project, queries share fairly; within a query, stages get slots as they need.
- A query can run with *fewer* slots than ideal — it just takes longer (work units queue). This is why BigQuery rarely fails on concurrency; it degrades.
- On-demand projects get a soft cap (~2,000 concurrent slots, burstable) shared across the project.
- **Idle slot sharing**: within the same admin project's reservations (Enterprise), idle baseline slots can be borrowed by other reservations unless disabled.

## Advanced Cost Engineering

1. **Per-query byte cap** — hard safety net:

```sql
-- Fails the query if it would bill more than ~100 GB
-- (set via job config: maximumBytesBilled)
```

```bash
bq query --maximum_bytes_billed=107374182400 --nouse_legacy_sql 'SELECT ...'
```

2. **Custom quotas**: per-user and per-project daily bytes-scanned quotas in the admin console.
3. **Chargeback**: tag jobs with labels, aggregate from `INFORMATION_SCHEMA.JOBS`:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'team') AS team,
  SUM(total_bytes_billed) / POW(1024, 4) AS tib_billed,
  SUM(total_bytes_billed) / POW(1024, 4) * 6.25 AS usd_on_demand
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY team
ORDER BY usd_on_demand DESC;
```

4. **Mixed model**: keep prod ETL on a committed reservation, leave exploratory analytics on-demand with quotas, route BI through materialized views + BI Engine.

## Architecture Trade-offs Seniors Should Articulate

**BigQuery vs Snowflake**: both separate storage/compute; Snowflake gives you explicit warehouses (predictable isolation, manual sizing), BigQuery gives a global slot pool (zero ops, less deterministic). BigQuery's on-demand model is unique — great for spiky, dangerous for undisciplined teams.

**BigQuery vs Spark/Dataproc**: BigQuery for SQL-shaped analytics; Spark when you need arbitrary code, ML libraries, iterative algorithms, or fine-grained control. BigQuery now blurs this with stored procedures for Apache Spark.

**When BigQuery is the wrong tool**: high-QPS point lookups (use Bigtable/AlloyDB), sub-second OLTP, frequent single-row mutations, or strict per-query latency SLOs under 100 ms.

**Lakehouse posture**: BigLake + Iceberg external tables let you keep open formats on GCS with BigQuery governance; native storage still wins on performance (clustering, caching, MVs).

## Operational Excellence

- **Monitoring**: `INFORMATION_SCHEMA.JOBS*` for slot usage and spend; Cloud Monitoring for slot utilization vs reservation; alerts on `total_bytes_billed` spikes.
- **CI/CD**: dataform/dbt for SQL, dry-run in CI to validate and estimate, table snapshots before destructive migrations:

```sql
CREATE SNAPSHOT TABLE ds.orders_backup_20260610
CLONE ds.orders
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY));
```

- **Zero-copy clones** (`CREATE TABLE ... CLONE`) for dev/test environments — billed only on divergence.

## ⚡ Cheat Sheet

### Key Limits & Numbers

| Item | Value |
|------|-------|
| On-demand price | ~$6.25 / TB scanned (first 1 TB/mo free) |
| Active / long-term storage | ~$0.02 / ~$0.01 per GB/mo (logical) |
| Max partitions per table | 10,000 |
| Max clustering columns | 4 |
| Time travel window | 2–7 days (+7 days fail-safe) |
| On-demand slot soft cap | ~2,000 per project |
| Autoscale increment | 50 slots, 1-min minimum billing |
| Load jobs | Free; 1,500/table/day limit |
| Storage Write API | ~$0.025/GB, 2 TiB/mo free |

### Commands

```bash
bq query --dry_run --nouse_legacy_sql 'SELECT ...'        # estimate bytes
bq query --maximum_bytes_billed=1073741824 '...'          # cost guardrail
bq show --format=prettyjson ds.table                      # schema + stats
bq mk --reservation --edition=ENTERPRISE --slots=100 ...  # buy slots
bq cp ds.t@-3600000 ds.t_restored                         # time-travel copy (1h ago)
```

### Decision Rules

| Situation | Rule |
|-----------|------|
| Scanning > ~200–300 TB/mo steadily | Move to editions/slots |
| Date-filtered queries | Partition by that date; `require_partition_filter` |
| High-cardinality equality filters | Cluster (up to 4 cols, most-filtered first) |
| Repeated dashboard aggregates | Materialized view (+ BI Engine for sub-second) |
| New streaming pipeline | Storage Write API, never insertAll |
| Frequent row updates | Wrong tool — Bigtable/AlloyDB, or batch MERGE |
| Dev/test copies | Zero-copy CLONE, not CTAS |
| Highly compressible data | Consider physical storage billing |

### One-liners to Say in the Interview

- "BigQuery bills on-demand by bytes *scanned*, so my levers are column projection, partition pruning, and cluster pruning — in that order of certainty."
- "Dremel executes a query as a DAG of stages connected by in-memory shuffle, with dynamic work rebalancing to absorb stragglers."
- "Dry-run estimates ignore cluster pruning — clustered tables often bill far less than estimated."
- "Storage Write API gives exactly-once via stream offsets over gRPC; insertAll is legacy best-effort dedup."
- "Materialized views are never stale-wrong: BigQuery merges the MV with the base-table delta, and can rewrite base-table queries to use the MV automatically."
- "Slots degrade gracefully — under contention queries queue work units and slow down rather than fail."
- "Time travel is 7 days; past that I'd need a snapshot or fail-safe ticket — so I snapshot before destructive migrations."
