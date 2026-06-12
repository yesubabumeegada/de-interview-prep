---
title: "BigQuery — Intermediate"
topic: gcp
subtopic: bigquery
content_type: study_material
layer: intermediate
difficulty_level: mid-level
tags: [gcp, bigquery, interview]
---

# BigQuery — Intermediate

At mid-level you're expected to go beyond "it's serverless and columnar" into the mechanics: how partitioning and clustering actually prune data, when materialized views refresh, how the two pricing models trade off, and the difference between the streaming APIs.

## Partitioning in Depth

Three partitioning strategies:

| Type | Column | Notes |
|------|--------|-------|
| Time-unit column | `DATE`, `TIMESTAMP`, `DATETIME` | Hourly, daily, monthly, yearly granularity |
| Ingestion time | pseudo-column `_PARTITIONTIME` | When no good date column exists |
| Integer range | `INT64` column | e.g., `customer_id` buckets |

Limits worth memorizing: **max 10,000 partitions per table**. Daily partitioning on 30 years of data = ~10,950 partitions — over the limit; use monthly instead.

```sql
CREATE TABLE ds.events
PARTITION BY TIMESTAMP_TRUNC(event_ts, DAY)
OPTIONS (
  partition_expiration_days = 90,
  require_partition_filter = TRUE
)
AS SELECT * FROM ds.events_raw;
```

Two options every production table should consider:

- `require_partition_filter = TRUE` — queries without a partition filter are **rejected**, protecting you from accidental full scans.
- `partition_expiration_days` — partitions auto-delete, implementing retention for free.

### Pruning Gotchas

Pruning only works when the optimizer can statically determine partitions:

```sql
-- ✅ Prunes: constant filter
WHERE DATE(event_ts) = '2026-06-01'

-- ✅ Prunes: scalar subquery is OK in most cases, but verify with dry run
WHERE event_ts >= TIMESTAMP('2026-06-01')

-- ❌ Does NOT prune: function applied in a way the optimizer can't push down
WHERE TIMESTAMP_ADD(event_ts, INTERVAL 1 DAY) > CURRENT_TIMESTAMP()

-- ❌ Does NOT prune: joining to a table to get the date range
WHERE event_ts IN (SELECT ts FROM other_table)
```

Always verify with `--dry_run` — bytes processed tells you if pruning happened.

## Clustering in Depth

Clustering sorts data within partitions by up to **4 columns** (order matters — filter on a prefix of the cluster columns for best effect).

```sql
CREATE TABLE ds.orders
PARTITION BY DATE(order_ts)
CLUSTER BY region, customer_id
AS SELECT * FROM ds.orders_raw;
```

Key mechanics:

- BigQuery maintains clustering automatically via background **re-clustering** (free).
- On-demand cost estimates **cannot** account for cluster pruning at dry-run time — the estimate shows the worst case; actual billed bytes may be much lower.
- Best for high-cardinality columns (`customer_id`, `event_name`). Partitioning is better for low-cardinality time buckets.
- Tables under ~1 GB see little benefit; block-level pruning needs enough data.

**Decision rule:** partition by date for pruning + lifecycle, cluster by your most common equality/range filter columns.

## On-Demand vs Capacity (Editions)

| Dimension | On-demand | Editions (Standard/Enterprise/Enterprise Plus) |
|-----------|-----------|-----------------------------------------------|
| Billing unit | $ per TB scanned (~$6.25/TB) | $ per slot-hour |
| Concurrency | ~2,000 slots soft cap per project | Whatever you reserve + autoscale |
| Predictability | Spiky, can be scary | Predictable budget |
| Idle cost | Zero | Baseline slots bill even when idle (autoscaled slots scale to 0) |
| Best for | Ad-hoc, low/variable volume | Steady heavy workloads, many users |

Editions specifics worth citing in interviews:

- **Slot autoscaling**: set a baseline (can be 0) and a max; BigQuery scales in increments of 50 slots, billed per second with a 1-minute minimum.
- **Commitments** (1yr/3yr) cut slot prices significantly versus pay-as-you-go slots.
- Crossover math: at $6.25/TB, if you scan more than roughly 200–300 TB/month consistently, a slot reservation usually wins. Do the math per workload.

Assign reservations to projects/folders via **assignments**, so prod ETL gets guaranteed slots while ad-hoc analysts share another pool.

```bash
# Create a reservation with autoscaling (Enterprise edition)
bq mk \
  --reservation \
  --edition=ENTERPRISE \
  --slots=100 \
  --autoscale_max_slots=400 \
  --location=US \
  etl_reservation
```

## Streaming: insertAll vs Storage Write API

| | Legacy `tabledata.insertAll` | Storage Write API |
|---|------------------------------|-------------------|
| Protocol | REST + JSON | gRPC + protobuf |
| Cost | ~$0.05/GB | ~$0.025/GB, first 2 TiB/month free |
| Semantics | Best-effort dedup via `insertId` | True exactly-once (committed type with offsets) |
| Throughput | Lower | Much higher (multiplexed streams) |
| Buffer | Streaming buffer (data not in time travel/copy until flushed) | Data committed via streams |

**Interview answer:** "New pipelines should always use the Storage Write API — it's cheaper, faster, and offers exactly-once via stream offsets. `insertAll` is legacy." The Write API offers three stream types:

- **Default stream** — at-least-once, simplest, great for most streaming.
- **Committed** — records visible immediately, exactly-once with offsets.
- **Pending** — records buffered until you commit the stream atomically (batch-like semantics).

```python
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2

# Simplified: append rows to the default stream
client = bigquery_storage_v1.BigQueryWriteClient()
parent = client.table_path("my_project", "ds", "events")
stream_name = f"{parent}/_default"

# In practice you build protobuf rows matching the table schema,
# then call append_rows over a bidirectional gRPC stream.
```

## Materialized Views

```sql
CREATE MATERIALIZED VIEW ds.daily_revenue
PARTITION BY order_date
CLUSTER BY region
OPTIONS (enable_refresh = TRUE, refresh_interval_minutes = 30)
AS
SELECT
  DATE(order_ts) AS order_date,
  region,
  SUM(amount) AS revenue,
  COUNT(*) AS orders
FROM ds.orders
GROUP BY order_date, region;
```

Mechanics that interviewers probe:

- **Incremental refresh**: BigQuery only reprocesses changed partitions of the base table.
- **Smart tuning / automatic rewrite**: queries against the *base table* can be transparently rewritten to read the MV. You get the benefit without changing queries.
- If the MV is stale, BigQuery combines the MV with the delta from the base table — results are always correct, never stale.
- Limitations: restricted SQL surface (historically no full outer joins, limited analytic functions; aggregates must be re-aggregatable like SUM/COUNT/MIN/MAX — `AVG` works because it decomposes into SUM/COUNT).

## Federated (External) Queries

Two distinct features people conflate:

1. **External tables** over GCS/Drive/Bigtable (and BigLake tables for governed access to GCS Parquet/Iceberg):

```sql
CREATE EXTERNAL TABLE ds.ext_logs
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-bucket/logs/*.parquet']
);
```

2. **Federated queries** to Cloud SQL/Spanner via `EXTERNAL_QUERY`:

```sql
SELECT *
FROM EXTERNAL_QUERY(
  'my_project.us.cloudsql_conn',
  'SELECT id, status FROM orders WHERE updated_at > NOW() - INTERVAL 1 DAY'
);
```

Trade-offs: external data isn't cached, can't be clustered, query performance depends on file layout (many small files = slow), and on-demand billing charges for bytes read from the external source. Use for occasional access or as a landing/ELT pattern; copy hot data into native tables.

## BI Engine

BI Engine is an in-memory acceleration layer (SQL interface) that caches table data in RAM for sub-second dashboard queries.

- You reserve capacity in GB per project/location (e.g., 10 GB).
- Works transparently — Looker Studio, Tableau, or any SQL client benefits.
- Best for small-to-medium dimensional data hit repeatedly by dashboards.
- It does not accelerate everything: large scans, some joins, and DML fall back to normal slots.

```bash
bq update --bi_reservation --location=US --size=10G
```

## Common Pitfalls Checklist

- `SELECT *` in production ETL — scans every column; list columns explicitly.
- Forgetting `require_partition_filter` on big tables — one bad ad-hoc query can cost hundreds of dollars.
- Using legacy streaming inserts in new code — Storage Write API is cheaper and exactly-once.
- Joining tables across regions — fails; plan dataset locations up front.
- Relying on dry-run estimates for clustered tables — estimates ignore cluster pruning (worst case shown).
- Many small DML statements (`UPDATE`/`DELETE` row-by-row) — BigQuery DML is set-based; frequent tiny mutations create heavy background work and can hit DML concurrency limits. Batch your mutations or use MERGE.
- Quota surprise: load jobs are free but capped (1,500 loads/table/day); query results to a destination table have size limits unless `allow_large_results` with legacy SQL or just standard SQL defaults.

## Practice Exercise

Estimate then optimize:

```sql
-- Before: full scan of 2 TB table = ~$12.50 on-demand
SELECT *
FROM ds.events
WHERE user_id = 'u_123';

-- After: partition filter + clustering on user_id + column projection
SELECT event_ts, event_name, properties
FROM ds.events_part            -- PARTITION BY DATE(event_ts), CLUSTER BY user_id
WHERE DATE(event_ts) >= '2026-05-01'
  AND user_id = 'u_123';
-- Scans only ~40 GB of recent partitions, cluster-pruned to a few hundred MB billed
```

Being able to narrate that before/after — *why* each change reduces bytes — is exactly what mid-level interviews test.
