---
title: "Incremental Loading - Intermediate"
topic: etl-concepts
subtopic: incremental-loading
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, incremental-loading, watermark, merge, scd, partition]
---

# Incremental Loading — Intermediate

## Watermark Patterns in Production

### Single-Column Watermark

The simplest approach: one timestamp column drives the delta.

```python
import sqlalchemy as sa
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class IncrementalLoader:
    def __init__(self, source_engine, target_engine, hwm_store):
        self.source = source_engine
        self.target = target_engine
        self.hwm_store = hwm_store

    def run(self, config: dict) -> dict:
        pipeline = config["pipeline_name"]
        table    = config["source_table"]
        hwm_col  = config["watermark_column"]
        lookback = timedelta(hours=config.get("lookback_hours", 2))

        # Step 1: Get last successful HWM
        stored_hwm = self.hwm_store.get(pipeline)
        effective_hwm = stored_hwm - lookback

        logger.info(f"Extracting {table} WHERE {hwm_col} > {effective_hwm}")

        # Step 2: Extract
        df = self._extract(table, hwm_col, effective_hwm)
        if df.empty:
            logger.info("No new data. Skipping.")
            return {"rows_loaded": 0}

        new_hwm = df[hwm_col].max()

        # Step 3: Load
        rows = self._load(df, config)

        # Step 4: Advance HWM only on success
        self.hwm_store.update(pipeline, new_hwm)
        logger.info(f"Loaded {rows} rows. New HWM: {new_hwm}")
        return {"rows_loaded": rows, "new_hwm": new_hwm}

    def _extract(self, table, hwm_col, hwm_val):
        import pandas as pd
        sql = f"SELECT * FROM {table} WHERE {hwm_col} > :hwm ORDER BY {hwm_col}"
        return pd.read_sql(sa.text(sql), self.source, params={"hwm": hwm_val})

    def _load(self, df, config):
        # delegate to merge or append based on config
        raise NotImplementedError
```

### Composite Watermark

When data arrives in multiple partitions, a single timestamp may not be sufficient.

```python
def extract_composite_watermark(
    source_engine,
    table: str,
    partition_col: str,
    watermark_col: str,
    last_partition: str,   # e.g. "2024-01-15"
    last_hwm: datetime
):
    """
    Extract from the last known partition onward.
    Handles the edge case where a new partition starts mid-pipeline.
    """
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {partition_col} > :last_partition
           OR ({partition_col} = :last_partition AND {watermark_col} > :last_hwm)
        ORDER BY {partition_col}, {watermark_col}
    """
    return pd.read_sql(
        sa.text(sql), source_engine,
        params={"last_partition": last_partition, "last_hwm": last_hwm}
    )
```

---

## Advanced Merge Patterns

### Type-2 SCD-Aware Merge

When targets implement Slowly Changing Dimensions, an incremental load must close old records and open new ones.

```sql
-- Step 1: Identify changed records
CREATE TEMP TABLE changed_records AS
SELECT src.*
FROM staging_customers src
JOIN target_customers tgt
    ON src.customer_id = tgt.customer_id
   AND tgt.is_current = TRUE
WHERE src.email != tgt.email
   OR src.phone  != tgt.phone;

-- Step 2: Close old records
UPDATE target_customers
SET   is_current  = FALSE,
      expired_at  = CURRENT_TIMESTAMP
WHERE customer_id IN (SELECT customer_id FROM changed_records)
  AND is_current  = TRUE;

-- Step 3: Insert new versions
INSERT INTO target_customers
    (customer_id, email, phone, effective_at, expired_at, is_current)
SELECT customer_id, email, phone, CURRENT_TIMESTAMP, NULL, TRUE
FROM changed_records;

-- Step 4: Insert brand-new customers
INSERT INTO target_customers
    (customer_id, email, phone, effective_at, expired_at, is_current)
SELECT src.customer_id, src.email, src.phone, CURRENT_TIMESTAMP, NULL, TRUE
FROM staging_customers src
WHERE NOT EXISTS (
    SELECT 1 FROM target_customers tgt
    WHERE tgt.customer_id = src.customer_id
);
```

### Soft-Delete Propagation

```python
def propagate_soft_deletes(source_engine, target_engine, table: str, hwm: datetime):
    """
    Detect rows soft-deleted in source (deleted_at is set) and mark them in target.
    """
    extract_sql = f"""
        SELECT id, deleted_at
        FROM {table}
        WHERE deleted_at IS NOT NULL
          AND deleted_at > :hwm
    """
    df = pd.read_sql(sa.text(extract_sql), source_engine, params={"hwm": hwm})

    if df.empty:
        return 0

    ids = tuple(df["id"].tolist())
    update_sql = f"""
        UPDATE target_{table}
        SET is_deleted = TRUE, deleted_at = src.deleted_at
        FROM (VALUES {','.join(f"('{r.id}', '{r.deleted_at}')" for r in df.itertuples())})
             AS src(id, deleted_at)
        WHERE target_{table}.id = src.id::uuid
    """
    with target_engine.begin() as conn:
        result = conn.execute(sa.text(update_sql))
    return result.rowcount
```

---

## Partition-Based Incremental Loads at Scale

### BigQuery Partitioned Load Pattern

```python
from google.cloud import bigquery
from datetime import date, timedelta

client = bigquery.Client()

def incremental_partition_load(
    source_project: str,
    target_dataset: str,
    target_table: str,
    run_date: date,
    lookback_days: int = 2
):
    """
    Load N days of partitions to handle late-arriving data gracefully.
    Each partition is replaced atomically (CREATE OR REPLACE).
    """
    for delta in range(lookback_days + 1):
        partition_date = run_date - timedelta(days=delta)
        partition_id   = partition_date.strftime("%Y%m%d")
        full_target    = f"{target_dataset}.{target_table}${partition_id}"

        query = f"""
            CREATE OR REPLACE TABLE `{full_target}`
            AS
            SELECT *
            FROM `{source_project}.raw.{target_table}`
            WHERE DATE(event_time) = '{partition_date}'
        """
        job = client.query(query)
        job.result()
        print(f"Loaded partition {partition_id}: {job.num_dml_affected_rows} rows")
```

### PySpark Incremental Load

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

spark = SparkSession.builder.appName("IncrementalLoad").getOrCreate()

def spark_incremental_load(
    source_path: str,
    target_path: str,
    hwm_value: datetime,
    merge_key: str = "id"
):
    # Read only new data from source
    new_data = (
        spark.read.parquet(source_path)
        .filter(col("updated_at") > lit(hwm_value))
    )

    # Read existing target
    try:
        existing = spark.read.parquet(target_path)
    except Exception:
        # First load
        new_data.write.mode("overwrite").partitionBy("date_partition").parquet(target_path)
        return

    # Anti-join: find IDs to remove from existing (they'll be replaced by new_data)
    ids_to_update = new_data.select(merge_key).distinct()
    existing_keep = existing.join(ids_to_update, on=merge_key, how="left_anti")

    # Union kept rows with new rows
    result = existing_keep.unionByName(new_data)
    result.write.mode("overwrite").partitionBy("date_partition").parquet(target_path)
```

---

## Handling Edge Cases

### Gap Detection

Gaps can occur if a pipeline run is skipped or the HWM is corrupted.

```python
def detect_hwm_gaps(hwm_history_table: str, pipeline: str, engine) -> list[dict]:
    """
    Identify intervals where no successful run was recorded.
    Returns list of gap windows needing backfill.
    """
    sql = f"""
        SELECT
            run_time,
            LAG(run_time) OVER (ORDER BY run_time) AS prev_run_time,
            EXTRACT(EPOCH FROM (run_time - LAG(run_time) OVER (ORDER BY run_time))) / 3600 AS gap_hours
        FROM {hwm_history_table}
        WHERE pipeline_name = :pipeline
        ORDER BY run_time
    """
    df = pd.read_sql(sa.text(sql), engine, params={"pipeline": pipeline})
    return df[df["gap_hours"] > 2].to_dict("records")  # flag gaps > 2 hours
```

### Idempotent HWM Store

```python
class HWMStore:
    """Thread-safe, idempotent high-water mark store backed by PostgreSQL."""

    def __init__(self, engine):
        self.engine = engine
        self._init_table()

    def _init_table(self):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS pipeline_hwm (
                    pipeline_name TEXT PRIMARY KEY,
                    hwm_value     TIMESTAMPTZ NOT NULL,
                    updated_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """))

    def get(self, pipeline: str, default: datetime = None) -> Optional[datetime]:
        sql = "SELECT hwm_value FROM pipeline_hwm WHERE pipeline_name = :p"
        with self.engine.connect() as conn:
            row = conn.execute(sa.text(sql), {"p": pipeline}).fetchone()
        return row[0] if row else (default or datetime(2000, 1, 1))

    def update(self, pipeline: str, hwm: datetime):
        sql = """
            INSERT INTO pipeline_hwm (pipeline_name, hwm_value, updated_at)
            VALUES (:p, :hwm, NOW())
            ON CONFLICT (pipeline_name)
            DO UPDATE SET hwm_value = EXCLUDED.hwm_value, updated_at = NOW()
        """
        with self.engine.begin() as conn:
            conn.execute(sa.text(sql), {"p": pipeline, "hwm": hwm})
```

---

## Incremental Load in Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def run_incremental(**context):
    pipeline = "orders_incremental"
    hwm = Variable.get(f"{pipeline}_hwm", default_var="2000-01-01T00:00:00")
    hwm_dt = datetime.fromisoformat(hwm)

    # ... extract, transform, load ...
    new_hwm = datetime.utcnow()

    Variable.set(f"{pipeline}_hwm", new_hwm.isoformat())
    return str(new_hwm)

with DAG(
    dag_id="orders_incremental_load",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    load_task = PythonOperator(
        task_id="incremental_load",
        python_callable=run_incremental,
    )
```

---

## Comparison: Incremental Strategies

| Strategy | Best For | Pros | Cons |
|---|---|---|---|
| Timestamp HWM | Mutable records with `updated_at` | Simple, universal | Misses hard deletes |
| Auto-increment ID HWM | Append-only tables | Fast, no clock drift | Doesn't catch updates |
| Partition replacement | Date-partitioned data | Atomic, handles late data | Requires partitioned target |
| CDC (log-based) | High-frequency mutation | Captures deletes, exact changes | Higher infra complexity |
| Full differential | Small tables with no `updated_at` | Catches all changes including deletes | Expensive at scale |

---

## Interview Tips

> **Tip 1:** When asked "how do you handle late-arriving data," describe the lookback window pattern and explain the trade-off: wider window = safer but more reprocessing. Tie it to the SLA for freshness.

> **Tip 2:** Distinguish between watermark-based and partition-based incremental loads. The latter is preferred in data warehouses because it leverages partition pruning and supports atomic replacement.

> **Tip 3:** Explain that the HWM must be updated **within the same success boundary** as the load. A pipeline that updates HWM before confirming the load committed will silently skip data on the next run.

> **Tip 4:** Soft deletes are a common gotcha — make sure your incremental query includes `deleted_at IS NOT NULL` to propagate deletions.

> **Tip 5:** In PySpark, the "read existing, anti-join, union with new" pattern avoids costly MERGE operations on large Delta/Parquet tables when a simpler upsert isn't available natively.
