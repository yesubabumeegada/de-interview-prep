---
title: "Pipeline Design Patterns — Scenarios"
topic: system-design
subtopic: pipeline-design-patterns
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, pipeline, interview, scenarios, design]
---

# Pipeline Design Patterns — Interview Scenarios

## Scenario 1: Design a Real-Time Order Analytics Pipeline

**Question:** Design a pipeline for an e-commerce company that needs: (1) real-time fraud detection (< 1 second), (2) near-real-time dashboards (< 5 minutes lag), (3) daily batch reports. Orders volume: 10,000/minute at peak.

**Answer:**

```
Three-tier pipeline design:

Tier 1 — Real-Time (< 1 second):
  Orders service → Kafka (orders.raw)
    → Flink consumer → fraud scoring (ML model via REST)
    → Kafka (orders.scored) → fraud-alert service
  Latency budget: Kafka <5ms + Flink processing <200ms + model <500ms = ~700ms ✓

Tier 2 — Near-Real-Time (< 5 minutes):
  Kafka (orders.raw)
    → Spark Structured Streaming (5-min micro-batch)
    → Delta Lake silver table (deduped, typed)
    → Kafka Connect → ClickHouse / Druid for dashboard queries
  5-minute trigger: materializes windowed aggregates (revenue by region, last 15 min)

Tier 3 — Batch (daily):
  Delta Lake silver → dbt daily models → Snowflake gold tables
  Airflow DAG: triggered at midnight UTC
  Full reconciliation: compare streaming counts vs batch counts (should match within 0.1%)

Why not one tier for everything:
  - Real-time needs: low latency, stateful processing, ML scoring → Flink
  - Dashboards need: fast queries on pre-aggregated data → ClickHouse
  - Historical reports need: SQL, complex joins, cost efficiency → Snowflake + dbt
```

---

## Scenario 2: Idempotency Bug — Duplicate Rows in Production

**Question:** Your daily pipeline doubled the row count in the `orders_fact` table overnight. How do you fix it now and prevent it in the future?

**Answer:**

**Immediate fix:**
```sql
-- Step 1: Identify duplicates
SELECT order_id, COUNT(*) as cnt
FROM orders_fact
WHERE order_date = CURRENT_DATE - 1
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Step 2: Remove duplicates (keep one row per order_id)
-- BigQuery/Snowflake pattern:
CREATE OR REPLACE TABLE orders_fact AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
  FROM orders_fact
)
WHERE rn = 1;

-- Step 3: Verify count is back to expected
SELECT COUNT(*), order_date FROM orders_fact
WHERE order_date = CURRENT_DATE - 1
GROUP BY order_date;
```

**Root cause analysis:**
```
Likely causes:
1. Pipeline INSERT without DELETE (ran twice for same date — retry after timeout)
2. Upstream source sent data twice (source bug)
3. Airflow DAG ran twice for same execution_date (manual trigger + scheduled)

Prevention:
1. Switch INSERT to: DELETE WHERE order_date = {ds} + INSERT
   OR: MERGE ON order_id (upsert)
2. Add unique constraint/PRIMARY KEY on (order_id) — reject duplicates at DB level
3. Add row count assertion: ASSERT COUNT(*) = (SELECT COUNT(*) FROM staging)
4. Airflow: set max_active_runs=1 to prevent concurrent runs for same DAG
```

---

## Scenario 3: Pipeline Design Review — What's Wrong?

**Question:** Review this pipeline design and identify all issues:

```python
def daily_pipeline():
    # Extract
    df = spark.read.jdbc(PROD_DB_URL, "orders")  # read entire orders table

    # Transform
    df = df.filter(f"order_date = '{datetime.today().strftime('%Y-%m-%d')}'")

    # Load
    df.write.mode("append").saveAsTable("orders_fact")
```

**Answer:**

| Issue | Impact | Fix |
|---|---|---|
| Reading entire orders table from PROD DB | Full table scan; load on source; slow | Push predicate to JDBC: `query="SELECT * FROM orders WHERE order_date = '{ds}'"` |
| `datetime.today()` — hardcoded to "now" | Not replayable; backfill impossible | Parameterize: accept `execution_date` as argument |
| `mode("append")` — no idempotency | Duplicates on retry/re-run | Partition overwrite or MERGE on order_id |
| No data quality checks | Bad data propagates silently | Add assertions before write |
| No error handling | Pipeline fails silently; no alerts | Add try/except with alerting |
| No logging/metrics | Can't diagnose issues | Log row counts in/out |

**Corrected version:**
```python
def daily_pipeline(execution_date: str):
    # Push-down predicate to source (incremental extract)
    df = spark.read.jdbc(
        PROD_DB_URL,
        table="(SELECT * FROM orders WHERE order_date = '{0}') t".format(execution_date),
        properties={"fetchsize": "10000"}
    )

    # Validate
    assert df.count() > 0, f"No orders for {execution_date}"
    assert df.filter(col("order_id").isNull()).count() == 0, "Null order_ids found"

    # Idempotent write (partition overwrite)
    (df.withColumn("order_date", lit(execution_date))
       .write
       .mode("overwrite")
       .partitionBy("order_date")
       .saveAsTable("orders_fact"))
    
    print(f"Loaded {df.count()} orders for {execution_date}")
```
