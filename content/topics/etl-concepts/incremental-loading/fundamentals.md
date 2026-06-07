---
title: "Incremental Loading - Fundamentals"
topic: etl-concepts
subtopic: incremental-loading
content_type: study_material
difficulty_level: beginner
layer: fundamentals
tags: [etl, incremental-loading, watermark, high-water-mark, batch]
---

# Incremental Loading — Fundamentals

## What Is Incremental Loading?

Incremental loading is the practice of extracting and loading **only the new or changed records** since the last successful pipeline run, rather than re-processing the entire dataset every time.

Contrast this with a **full load** (also called a "truncate and reload"), where the entire source table is read, the target is cleared, and all rows are re-inserted. Full loads are simple but become prohibitively expensive as data volumes grow.

```
Full Load:     Source (10 M rows) → Extract ALL → Truncate Target → Insert ALL
Incremental:   Source (10 M rows) → Extract NEW 5,000 rows → Append/Merge to Target
```

---

## Why Incremental Loading Matters

| Concern | Full Load | Incremental Load |
|---|---|---|
| Extract volume | All rows every run | Only new/changed rows |
| Pipeline runtime | Hours (at scale) | Minutes or seconds |
| Source system load | High | Low |
| Target availability | Disrupted during load | Continuous |
| Complexity | Low | Medium |
| Risk of data loss | High (truncate step) | Lower |

For a 1-billion-row fact table, the difference between re-loading everything vs. loading the last hour's delta can be 60x or more in runtime.

---

## Core Concepts

### High-Water Mark (HWM)

A **high-water mark** is a stored value that tracks the boundary of the last successful load. Common HWM columns:

- `updated_at` / `modified_timestamp` — row-level modification timestamp
- `created_at` — insertion timestamp (append-only sources)
- `id` / `sequence_number` — auto-incrementing primary key

```python
import pandas as pd
import sqlalchemy as sa
from datetime import datetime

engine = sa.create_engine("postgresql://user:pass@host/db")

def get_high_water_mark(hwm_table: str, pipeline_name: str) -> datetime:
    """Retrieve the last successful load timestamp."""
    query = f"""
        SELECT max_value
        FROM {hwm_table}
        WHERE pipeline_name = :name
    """
    with engine.connect() as conn:
        result = conn.execute(sa.text(query), {"name": pipeline_name}).fetchone()
    return result[0] if result else datetime(2000, 1, 1)  # epoch default

def update_high_water_mark(hwm_table: str, pipeline_name: str, new_hwm: datetime):
    """Update the HWM after a successful load."""
    upsert_sql = f"""
        INSERT INTO {hwm_table} (pipeline_name, max_value, updated_at)
        VALUES (:name, :value, NOW())
        ON CONFLICT (pipeline_name)
        DO UPDATE SET max_value = EXCLUDED.max_value, updated_at = NOW()
    """
    with engine.begin() as conn:
        conn.execute(sa.text(upsert_sql), {"name": pipeline_name, "value": new_hwm})
```

### Extracting the Delta

```python
def extract_incremental(source_engine, table: str, hwm_col: str, hwm_value: datetime) -> pd.DataFrame:
    """Extract rows created/updated after the high-water mark."""
    query = f"""
        SELECT *
        FROM {table}
        WHERE {hwm_col} > :hwm
        ORDER BY {hwm_col} ASC
    """
    return pd.read_sql(sa.text(query), source_engine, params={"hwm": hwm_value})
```

---

## Append vs. Merge Strategies

### Append (Insert-Only)

Used when source data is **immutable** — records are never updated, only inserted. Event logs, IoT readings, and audit trails are typical examples.

```sql
-- Target grows continuously; no UPDATE or DELETE needed
INSERT INTO target_events (event_id, user_id, event_time, payload)
SELECT event_id, user_id, event_time, payload
FROM staging_events
WHERE event_id NOT IN (SELECT event_id FROM target_events);
```

### Merge (Upsert)

Used when source records **can change** — a customer's email updates, an order status changes. The merge pattern updates existing rows and inserts new ones.

```sql
-- PostgreSQL UPSERT example
INSERT INTO target_customers (customer_id, email, updated_at, status)
SELECT customer_id, email, updated_at, status
FROM staging_customers
ON CONFLICT (customer_id)
DO UPDATE SET
    email      = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at,
    status     = EXCLUDED.status;
```

```sql
-- Snowflake / BigQuery MERGE example
MERGE INTO target_customers AS tgt
USING staging_customers AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET email = src.email, updated_at = src.updated_at, status = src.status
WHEN NOT MATCHED THEN
    INSERT (customer_id, email, updated_at, status)
    VALUES (src.customer_id, src.email, src.updated_at, src.status);
```

---

## Partition-Based Incremental Loads

Many data warehouses support **partition pruning**, which makes date-partitioned incremental loads extremely efficient.

```python
from datetime import date, timedelta

def load_partition(run_date: date):
    """Load a single day's partition into BigQuery."""
    partition_id = run_date.strftime("%Y%m%d")
    query = f"""
        CREATE OR REPLACE TABLE `project.dataset.events${partition_id}`
        AS
        SELECT *
        FROM `project.raw.source_events`
        WHERE DATE(event_time) = '{run_date}'
    """
    # execute via BigQuery client
    print(f"Loading partition {partition_id}")
```

```mermaid
graph TD
    A["Source DB"] --> B["Extract Delta<br>(WHERE updated_at > HWM)"]
    B --> C["Staging Area"]
    C --> D{"Strategy?"}
    D --> E["Append<br>(immutable data)"]
    D --> F["Merge/Upsert<br>(mutable data)"]
    E --> G["Target Table"]
    F --> G
    G --> H["Update HWM"]
```

---

## Handling Late-Arriving Data

Late-arriving data occurs when records appear in the source with a timestamp **earlier than the current HWM**. This is common in:

- Mobile apps with offline sync
- Distributed systems with clock skew
- Manual corrections applied retroactively

### Strategies

1. **Lookback window**: Always re-process the last N hours/days to catch stragglers.

```python
LOOKBACK_HOURS = 4

def get_effective_hwm(stored_hwm: datetime) -> datetime:
    """Apply a lookback window to catch late arrivals."""
    return stored_hwm - timedelta(hours=LOOKBACK_HOURS)
```

2. **Partition reprocessing**: Flag partitions as "dirty" and re-run them.
3. **Event-time watermarks** (streaming): Accept late events up to a configurable delay.

---

## Common Pitfalls

| Pitfall | Description | Mitigation |
|---|---|---|
| Missing `updated_at` index | Full table scan on every run | Add index on the watermark column |
| Clock skew between source nodes | Records missed when clocks drift | Use `>=` instead of `>` with dedup |
| HWM not atomic with load | HWM advances even if load fails | Update HWM only on success |
| Soft deletes ignored | Deleted rows not propagated | Include `deleted_at IS NOT NULL` |
| Schema changes | New columns silently dropped | Schema drift detection layer |

---

## Interview Tips

> **Tip 1:** Always clarify whether source records can be updated (mutable) or only inserted (immutable) — this fundamentally determines whether you need append or merge semantics.

> **Tip 2:** Store the HWM in the same transaction as marking the pipeline successful. A separate update risks advancing the HWM even when the load fails, causing silent data gaps.

> **Tip 3:** Ask about late-arriving data. A 5-minute lookback window is often safer than a sharp HWM cutoff, at the cost of processing some rows twice — which is acceptable if your write operation is idempotent.

> **Tip 4:** For very high-volume tables, consider partition-based incremental loads instead of row-level watermarks — they leverage the warehouse's partition pruning and avoid expensive MERGE operations.

> **Tip 5:** Index the watermark column on the source. Without an index, `WHERE updated_at > ?` performs a full table scan and can overload the OLTP source.
