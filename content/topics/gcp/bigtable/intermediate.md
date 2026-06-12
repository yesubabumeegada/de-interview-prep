---
title: "Bigtable — Intermediate"
topic: gcp
subtopic: bigtable
content_type: study_material
layer: intermediate
difficulty_level: mid-level
tags: [gcp, bigtable, interview]
---

# Bigtable — Intermediate

## Row-Key Design Patterns, Properly

The mid-level expectation: you can design keys for *specific access patterns* and articulate trade-offs.

### Pattern catalog

| Pattern | Key shape | Use when | Trade-off |
|---|---|---|---|
| Entity-first time series | `device#1234#rev_ts` | "Latest N readings for device X" | Cross-device time scans need fan-out |
| Field promotion | `region#device#rev_ts` | Queries always filter by region first | Key order locks query hierarchy |
| Salting | `hash(key) % 8 # key` | Unavoidably sequential keys | Every read/scan must fan out to all salts |
| Reverse domain | `com.example.www#page` | Web/URL entities, locality by domain | — |
| Composite lookup | `user#order#item` | Hierarchical 1:N reads in one scan | Deep hierarchies inflate key length |

### Salting done right

```python
SALT_BUCKETS = 8

def salted_key(natural_key: str) -> bytes:
    salt = hash(natural_key) % SALT_BUCKETS
    return f"{salt:02d}#{natural_key}".encode()

# Reading "a range" now requires 8 parallel scans, one per salt bucket
from google.cloud.bigtable.row_set import RowSet

row_set = RowSet()
for salt in range(SALT_BUCKETS):
    row_set.add_row_range_from_keys(
        start_key=f"{salt:02d}#metric#cpu#".encode(),
        end_key=f"{salt:02d}#metric#cpu$".encode(),
    )
rows = table.read_rows(row_set=row_set)
```

Say the trade-off out loud in interviews: salting fixes write hotspots **by destroying range-scan locality** — choose bucket count ≈ a small multiple of node count, and only salt when key redesign can't fix distribution.

### Key anti-patterns checklist
- Timestamps or sequential IDs at the **front** of the key
- Mutable attributes in the key (you can't update a row key — only rewrite + delete)
- Unbounded-length keys (every cell stores the full key — key size multiplies storage)
- Low-cardinality prefixes (`status#active#...` → two giant tablets)

## Schema: Tall vs Wide

Two valid shapes for time series:

**Tall (row per event):** `device#rev_ts` → one row per reading. Simple GC by age, natural streaming writes, scan-friendly.

**Wide (row per entity per bucket):** `device#day`, with column qualifier = offset-in-day. Fewer rows, single-row atomicity across a day's readings, efficient "whole day" reads.

Mid-level guidance: default **tall** for high-frequency events; go **wide** when you need atomic updates over a group or read whole groups together. Hard limits to respect: rows should stay well under **100MB** (and ideally <10MB hot rows); a single cell <10MB.

## Filters: Server-Side Query Shaping

Filters reduce data returned, **not** data read from disk for row selection — row selection is still keys/ranges.

```python
from google.cloud.bigtable import row_filters as rf

# Latest version of one column family, value condition
flt = rf.RowFilterChain(filters=[
    rf.FamilyNameRegexFilter("stats"),
    rf.CellsColumnLimitFilter(1),
    rf.ValueRangeFilter(start_value=b"20", end_value=b"30"),
])

rows = table.read_rows(
    start_key=b"device#42#",
    end_key=b"device#42$",
    filter_=flt,
)
```

Pitfall worth naming: a full-table scan with a filter is still a full-table scan — filters are not indexes.

## Replication and App Profiles

A Bigtable **instance** can contain up to multiple **clusters** in different zones/regions. Replication between clusters is **asynchronous and eventually consistent**.

**App profiles** define how clients route:

| Routing | Behavior | Consistency |
|---|---|---|
| **Single-cluster** | All traffic to one cluster (manual failover) | Read-your-writes within that cluster |
| **Multi-cluster (any)** | Nearest available cluster, automatic failover | Eventual across clusters |

```bash
# Add a replica cluster
gcloud bigtable clusters create iot-c2 \
    --instance iot-instance \
    --zone europe-west1-b \
    --num-nodes 3

# Profile for latency-tolerant serving with auto-failover
gcloud bigtable app-profiles create serve-any \
    --instance iot-instance \
    --route-any

# Profile pinning batch writes to one cluster
gcloud bigtable app-profiles create batch-writes \
    --instance iot-instance \
    --route-to iot-c1 \
    --transactional-writes
```

Key interview points:
- **Single-row transactions (read-modify-write, check-and-mutate) require single-cluster routing** — they're disabled on multi-cluster profiles because async replication can't arbitrate conflicts.
- Standard pattern: **separate app profiles per workload** — `batch` pinned to one cluster, `serving` on multi-cluster any — this also isolates workloads (batch jobs hammering cluster 1 don't degrade serving reads on cluster 2).
- Replication is last-write-wins per cell (timestamp-based) — design idempotent writes.

## Garbage Collection in Depth

```bash
# Keep 1 version AND nothing older than 30 days (union = delete if either)
cbt -instance iot-instance setgcpolicy telemetry stats "maxage=30d or maxversions=1"

# Intersection: delete only if BOTH conditions met
cbt -instance iot-instance setgcpolicy telemetry stats "maxage=30d and maxversions=5"
```

Operational realities:
- GC runs **opportunistically during compactions** — up to ~a week before space is reclaimed. Don't promise storage drops same-day.
- Reads filter expired cells immediately (you won't *see* them), but **scans still pass over the bytes** until compaction — performance of scans over heavy-churn data lags the logical delete.
- Explicit deletes write **tombstones** — a delete makes scans *slower* until compaction, never faster. Mass deletes via `DropRowRange` (by prefix) are far cheaper than cell-level deletes.

## Capacity, Autoscaling, and Monitoring

Per-node planning numbers (SSD, order-of-magnitude for interviews):

| Metric | Per node |
|---|---|
| Reads or writes | ~10,000 QPS at ~6ms |
| Throughput | ~220MB/s scans (varies) |
| Storage served | up to 5TB SSD (keep utilization < ~70% for perf) |

```bash
# Native autoscaling on CPU + storage targets
gcloud bigtable clusters update iot-c1 \
    --instance iot-instance \
    --autoscaling-min-nodes 3 \
    --autoscaling-max-nodes 12 \
    --autoscaling-cpu-target 60
```

Watch in Cloud Monitoring:
- **CPU per cluster** — target <60–70% average; >80% = latency cliff
- **CPU of hottest node** — the hotspot detector: hot node ≫ average ⇒ key-design problem, *not* a scaling problem
- **Storage utilization** — >70% throttles performance regardless of CPU
- **Key Visualizer** — heatmap of activity by key range over time; the canonical hotspot debugging tool

## Working with Dataflow and BigQuery

Bulk loads and exports should go through **Dataflow** (the Beam Bigtable connector handles batching, retries, and throughput control):

```python
# Beam pipeline writing to Bigtable (sketch)
import apache_beam as beam
from apache_beam.io.gcp.bigtableio import WriteToBigTable

with beam.Pipeline(options=opts) as p:
    (
        p
        | "Read" >> beam.io.ReadFromParquet("gs://lake/features/*.parquet")
        | "ToMutations" >> beam.ParDo(BuildDirectRowFn())
        | "Write" >> WriteToBigTable(
            project_id="proj",
            instance_id="iot-instance",
            table_id="features",
        )
    )
```

BigQuery can also query Bigtable directly via **external tables** (BigLake) — handy for occasional analytics without an export, but slow versus native BigQuery storage; for heavy analytics, export/stream changes into BigQuery.

## Backups and Change Streams

- **Managed backups**: per-table, stored in the cluster's zone, restorable to a new table — protect against logical corruption (bad deploy deleting rows), not zone loss (that's replication's job).

```bash
gcloud bigtable backups create telemetry-bkp-20260610 \
    --instance iot-instance \
    --cluster iot-c1 \
    --table telemetry \
    --retention-period 30d
```

- **Change streams**: emit row-level changes consumable by Dataflow → BigQuery/PubSub — the standard CDC path for keeping analytics in sync.

## Common Pitfalls Recap

1. Hotspots from sequential keys — diagnose with Key Visualizer + hottest-node CPU.
2. Running single-row transactions on a multi-cluster app profile (rejected) — or assuming cross-cluster strong consistency.
3. Expecting deletes to speed up scans — tombstones until compaction.
4. One default app profile for everything — no workload isolation, accidental multi-cluster transactional writes.
5. Loading bulk data with naive parallel single-row writes instead of Dataflow batched mutations.
6. Storage >70% and wondering why latency rose with CPU flat.

## Interview Sound Bites

> "Row-key design is the schema. I write down the top queries first, then design the key so each is a point read or a tight prefix scan — and I check the write side for hotspots before shipping."

> "App profiles are both routing and isolation: batch pinned to one cluster, serving on multi-cluster-any, and single-row transactions only ever on single-cluster routing."

> "Bigtable deletes are writes. Tombstones make scans slower until compaction — for bulk removal I drop row ranges or let maxage GC do it."
