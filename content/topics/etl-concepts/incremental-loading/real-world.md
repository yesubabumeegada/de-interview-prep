---
title: "Incremental Loading - Real World"
topic: etl-concepts
subtopic: incremental-loading
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, incremental-loading, production, case-study, bigquery, airflow]
---

# Incremental Loading — Real World

## Case Study 1: E-commerce Orders Pipeline

### Problem

A large e-commerce platform had a nightly full reload of 800 million order rows taking 6+ hours. By 6 AM, the warehouse data was still 12 hours stale, missing overnight sales for the morning business review.

### Solution: Partitioned Hourly Incremental Load

```python
# airflow/dags/orders_incremental.py
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from datetime import datetime, timedelta

LOAD_SQL = """
MERGE `warehouse.orders` AS tgt
USING (
    SELECT *
    FROM `raw.mysql_orders`
    WHERE DATE(updated_at) BETWEEN
        DATE_SUB(DATE('{{ ds }}'), INTERVAL 2 DAY)  -- 2-day lookback
        AND DATE('{{ ds }}')
) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        status     = src.status,
        total_usd  = src.total_usd,
        updated_at = src.updated_at
WHEN NOT MATCHED THEN
    INSERT ROW
"""

with DAG(
    dag_id="orders_incremental_hourly",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
) as dag:

    merge_orders = BigQueryInsertJobOperator(
        task_id="merge_orders",
        configuration={
            "query": {
                "query": LOAD_SQL,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        project_id="my-project",
    )
```

### Results

| Metric | Before | After |
|---|---|---|
| Pipeline runtime | 6+ hours | 8 minutes |
| Data freshness | 12 hours stale | 1 hour stale |
| Source DB load | Full table scan nightly | Indexed range scan hourly |
| Cost (BigQuery slots) | 1,200 slot-hours/day | 45 slot-hours/day |

---

## Case Study 2: Financial Ledger — Handling Late Adjustments

### Problem

A fintech company's accounting pipeline missed retroactive transaction adjustments. Bookkeepers could post corrections up to 7 days after the original transaction date, causing reporting discrepancies.

### Solution: Rolling 7-Day Partition Reprocessing

```python
from google.cloud import bigquery
from datetime import date, timedelta

def reprocess_rolling_window(
    client: bigquery.Client,
    source_table: str,
    target_table: str,
    run_date: date,
    lookback_days: int = 7
):
    """
    Replace the last N partitions every run to capture retroactive corrections.
    Atomic partition replacement prevents partial reads during load.
    """
    jobs = []
    for offset in range(lookback_days + 1):
        partition_date = run_date - timedelta(days=offset)
        partition_id   = partition_date.strftime("%Y%m%d")
        target_partition = f"{target_table}${partition_id}"

        query = f"""
            CREATE OR REPLACE TABLE `{target_partition}`
            PARTITION BY transaction_date
            AS
            SELECT
                txn_id,
                account_id,
                amount_usd,
                transaction_date,
                posted_at,
                adjustment_reason,
                ROW_NUMBER() OVER (
                    PARTITION BY txn_id
                    ORDER BY posted_at DESC
                ) AS row_num
            FROM `{source_table}`
            WHERE transaction_date = '{partition_date}'
            QUALIFY row_num = 1  -- Latest version of each transaction
        """
        job = client.query(query)
        jobs.append((partition_id, job))

    # Wait for all partitions to complete
    for partition_id, job in jobs:
        job.result()
        print(f"Partition {partition_id} loaded: {job.num_dml_affected_rows} rows")
```

---

## Case Study 3: CDC-Driven Incremental Load for Real-Time Inventory

### Architecture

```mermaid
graph LR
    A["MySQL<br>Inventory DB"] --> B["Debezium<br>Connector"]
    B --> C["Kafka Topic<br>inventory.changes"]
    C --> D["Spark Structured<br>Streaming"]
    D --> E["Delta Lake<br>Silver Layer"]
    E --> F["dbt Incremental<br>Model"]
    F --> G["Snowflake<br>Gold Layer"]
```

### dbt Incremental Model for Inventory

```sql
-- models/silver/inventory_current.sql
{{
    config(
        materialized='incremental',
        unique_key='sku_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        sku_id,
        warehouse_id,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        last_movement_at,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id, warehouse_id
            ORDER BY last_movement_at DESC
        ) AS rn
    FROM {{ source('raw', 'inventory_events') }}

    {% if is_incremental() %}
    -- Only process events newer than what's in the target
    WHERE last_movement_at > (
        SELECT COALESCE(MAX(last_movement_at), '2000-01-01')
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    sku_id,
    warehouse_id,
    quantity_on_hand,
    quantity_reserved,
    quantity_available,
    last_movement_at
FROM source
WHERE rn = 1
```

---

## Case Study 4: Multi-Source Fan-In Incremental Load

### Problem

A data platform aggregates clickstream data from 12 regional data centers, each with its own timestamp and timezone. Records arrive out of order across regions.

### Solution: Logical Clock + Region-Aware HWM

```python
from dataclasses import dataclass
from typing import Dict
import hashlib

@dataclass
class RegionHWM:
    region: str
    hwm: datetime
    last_sequence: int

class MultiRegionIncrementalLoader:
    def __init__(self, regions: list[str], hwm_store):
        self.regions = regions
        self.hwm_store = hwm_store

    def run(self, target_engine):
        all_dfs = []

        for region in self.regions:
            hwm = self.hwm_store.get(f"clickstream_{region}")
            df  = self._extract_region(region, hwm)

            if df.empty:
                continue

            # Normalize timestamps to UTC
            df["event_time_utc"] = pd.to_datetime(df["event_time"], utc=True)

            # Add deterministic dedup key
            df["dedup_key"] = df.apply(
                lambda r: hashlib.md5(
                    f"{region}:{r['session_id']}:{r['event_name']}:{r['event_time']}".encode()
                ).hexdigest(),
                axis=1
            )
            df["region"] = region
            all_dfs.append(df)

        if not all_dfs:
            return

        combined = pd.concat(all_dfs, ignore_index=True)

        # Dedup across regions
        combined = combined.drop_duplicates(subset=["dedup_key"])

        # Load
        self._upsert(combined, target_engine)

        # Advance per-region HWMs only on success
        for region in self.regions:
            region_max = combined[combined["region"] == region]["event_time_utc"].max()
            if pd.notna(region_max):
                self.hwm_store.update(f"clickstream_{region}", region_max)

    def _extract_region(self, region: str, hwm: datetime) -> pd.DataFrame:
        # Implementation varies by region data source
        raise NotImplementedError

    def _upsert(self, df: pd.DataFrame, engine):
        raise NotImplementedError
```

---

## Operational Runbook: Incremental Load Failures

### Failure Modes and Recovery

| Failure | Symptoms | Recovery Steps |
|---|---|---|
| HWM not advanced | Next run re-loads everything | Check pipeline logs; re-run with forced HWM reset |
| HWM over-advanced | Gap in data | Reset HWM to last known good; trigger backfill |
| Source timeout | Partial extract, no load | Retry is safe if write is idempotent |
| Schema drift | Load errors on column mismatch | Run schema alignment script; re-extract |
| Duplicate rows | Row counts exceed source | Check for missing dedup key; run dedup job |

### Reset HWM Script

```python
def reset_hwm(pipeline_name: str, reset_to: datetime, engine, confirm: bool = False):
    """
    Emergency HWM reset. Requires explicit confirmation to prevent accidents.
    After reset, next pipeline run will reprocess from reset_to.
    """
    if not confirm:
        raise ValueError("Pass confirm=True to execute HWM reset. This triggers backfill!")

    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO pipeline_hwm (pipeline_name, hwm_value, updated_at, reset_reason)
            VALUES (:p, :hwm, NOW(), 'manual_reset')
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                hwm_value     = EXCLUDED.hwm_value,
                updated_at    = NOW(),
                reset_reason  = 'manual_reset'
        """), {"p": pipeline_name, "hwm": reset_to})

    print(f"HWM for {pipeline_name} reset to {reset_to}. Next run will backfill from this point.")
```

---

## Interview Tips

> **Tip 1:** When describing a real-world incremental pipeline, quantify the improvement — "reduced from 6-hour full load to 8-minute incremental" is far more compelling than "we made it faster."

> **Tip 2:** The rolling-window reprocessing pattern (always reprocess the last N days/partitions) is the pragmatic solution for late-arriving data in most business contexts. Know when to use it vs. a pure HWM cutoff.

> **Tip 3:** Multi-source fan-in requires per-source HWMs, not a global one. The global completeness boundary is the minimum of all per-source HWMs.

> **Tip 4:** In dbt, `is_incremental()` is the key macro. Understand that on a full-refresh run, the WHERE clause is skipped and all rows are processed — this matters for backfill behavior.

> **Tip 5:** Always have a tested HWM reset procedure documented in the runbook. Interviewers appreciate operational maturity — showing that you've thought about failure recovery, not just happy-path design.
