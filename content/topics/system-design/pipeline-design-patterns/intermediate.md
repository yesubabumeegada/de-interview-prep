---
title: "Pipeline Design Patterns — Intermediate"
topic: system-design
subtopic: pipeline-design-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, pipeline, incremental, cdc, watermark, orchestration]
---

# Pipeline Design Patterns — Intermediate

## Incremental Load Patterns

### Pattern 1: High-Watermark

```python
# Track the last processed timestamp in a metadata table
# Each run: only process rows newer than the watermark

# Metadata table schema:
# pipeline_watermarks(pipeline_name, last_processed_ts, updated_at)

def get_watermark(pipeline_name: str) -> datetime:
    row = db.execute(
        "SELECT last_processed_ts FROM pipeline_watermarks WHERE pipeline_name = %s",
        (pipeline_name,)
    ).fetchone()
    return row[0] if row else datetime(2000, 1, 1)  # default: epoch

def update_watermark(pipeline_name: str, new_ts: datetime):
    db.execute("""
        INSERT INTO pipeline_watermarks (pipeline_name, last_processed_ts, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (pipeline_name) DO UPDATE
          SET last_processed_ts = EXCLUDED.last_processed_ts,
              updated_at = EXCLUDED.updated_at
    """, (pipeline_name, new_ts))

def run_incremental():
    wm = get_watermark("orders_pipeline")
    max_ts = db.execute(
        "SELECT MAX(updated_at) FROM orders_staging WHERE updated_at > %s", (wm,)
    ).scalar()

    df = spark.read.jdbc(url, "orders", properties={
        "query": f"SELECT * FROM orders WHERE updated_at > '{wm}' AND updated_at <= '{max_ts}'"
    })
    # Transform and write...
    update_watermark("orders_pipeline", max_ts)
```

### Pattern 2: CDC (Change Data Capture)

```
CDC approaches:
1. Log-based CDC (Debezium): reads database transaction log
   - Captures INSERT/UPDATE/DELETE with before/after values
   - Zero load on source database (reads binlog/WAL)
   - Low latency (<1 second)
   - Output: Kafka topic per table

2. Query-based CDC: polls source with updated_at watermark
   - Simpler setup (no Debezium/Kafka)
   - Misses hard DELETEs (no updated_at on deleted rows)
   - Adds load to source DB on each poll

3. Triggers: database triggers write changes to an audit table
   - Adds overhead to every write operation
   - Works even without transaction log access

Debezium event structure (Kafka message):
{
  "op": "u",                    // c=create, u=update, d=delete, r=read(snapshot)
  "before": {"id": 1, "status": "PENDING"},
  "after":  {"id": 1, "status": "SHIPPED"},
  "source": {"table": "orders", "ts_ms": 1706000000000}
}
```

---

## Fan-Out and Fan-In Patterns

```
Fan-Out: one source → multiple downstream consumers
  Use case: an orders event triggers:
    - Inventory update
    - Notification service
    - Analytics pipeline
    - Fulfillment service
  Implementation: publish to Kafka topic; each service has its own consumer group

Fan-In: multiple sources → one aggregated output
  Use case: merge sales data from 5 regional databases into one DW
  Implementation:
    - Each regional pipeline writes to a staging table with a region_code column
    - A merge job aggregates into the fact table
    - Or: all pipelines write to a single Kafka topic partitioned by region
```

---

## Backfill Pattern

```python
# Backfill: re-process historical data (e.g., after a bug fix or schema change)
# Key: pipeline must be idempotent and parameterized by date

# Airflow backfill:
# airflow dags backfill orders_pipeline --start-date 2024-01-01 --end-date 2024-03-31

# Design your pipeline to accept execution_date as a parameter:
def load_orders(execution_date: date):
    start = execution_date
    end = execution_date + timedelta(days=1)
    
    # Delete existing data for this date (idempotency)
    spark.sql(f"DELETE FROM orders_fact WHERE order_date = '{start}'")
    
    # Load new data
    df = read_source(start, end)
    df.write.mode("append").partitionBy("order_date").saveAsTable("orders_fact")

# Backfill in parallel (careful: don't overload source)
# Airflow: max_active_runs=3 limits concurrent backfill runs
```

---

## Orchestration Patterns

### DAG Design Best Practices

```python
# Airflow DAG design patterns

# Pattern 1: Sensor → Extract → Transform → Load
with DAG('orders_pipeline', schedule_interval='@daily') as dag:
    wait_for_file = S3KeySensor(
        task_id='wait_for_source_file',
        bucket_key='raw/orders/{{ ds }}/*.parquet',
        timeout=3600, poke_interval=60
    )
    extract = PythonOperator(task_id='extract', python_callable=extract_orders)
    transform = PythonOperator(task_id='transform', python_callable=transform_orders)
    load = PythonOperator(task_id='load', python_callable=load_to_dw)
    dq_check = PythonOperator(task_id='data_quality', python_callable=run_dq_checks)

    wait_for_file >> extract >> transform >> load >> dq_check

# Pattern 2: Cross-DAG dependencies (TriggerDagRunOperator or ExternalTaskSensor)
upstream_done = ExternalTaskSensor(
    task_id='wait_for_customers_pipeline',
    external_dag_id='customers_pipeline',
    external_task_id='load',
    execution_delta=timedelta(0)
)

# Pattern 3: Dynamic task generation (Airflow 2.3+ dynamic task mapping)
@task
def process_region(region: str):
    run_pipeline_for_region(region)

regions = ['US', 'EU', 'APAC', 'LATAM']
process_region.expand(region=regions)  # creates 4 parallel tasks dynamically
```

---

## Data Contract Pattern

A data contract defines the agreed schema, SLA, and quality expectations between a producer and consumer:

```yaml
# data_contract_orders.yaml
name: orders_v2
producer: checkout-service
consumers: [analytics-team, finance-team, ml-platform]
schema:
  - name: order_id
    type: string
    nullable: false
    unique: true
  - name: amount_usd
    type: decimal(10,2)
    nullable: false
    constraints: {min: 0}
  - name: status
    type: string
    enum: [PENDING, SHIPPED, DELIVERED, CANCELLED]
sla:
  freshness_minutes: 15
  availability: 99.9%
quality:
  completeness: 99.5%   # max 0.5% null rate on required fields
  row_count_min: 1000   # at least 1000 orders/day
```

---

## Interview Tips

> **Tip 1:** "How do you handle late-arriving data in a batch pipeline?" — Keep the pipeline parameterized by partition date. For data that arrives late (e.g., transactions from yesterday appearing today), re-run the affected partitions. Use idempotent partition overwrite: the re-run deletes and rewrites just that date's partition. Set a re-processing window (e.g., reprocess any partition that received new data in the last 3 days). Airflow's `catchup=True` can trigger missed runs automatically.

> **Tip 2:** "What is a data contract and why does it matter?" — A data contract is a formal agreement between data producers and consumers specifying schema, semantics, SLAs, and quality expectations. It prevents breaking changes: when the checkout team renames a field, the contract fails validation before the change reaches production, giving the analytics team time to adapt. Tools: Soda, Great Expectations, or custom YAML schemas validated in CI/CD.

> **Tip 3:** "When would you choose CDC over a watermark-based incremental load?" — CDC (Debezium/log-based) when: you need DELETE propagation (watermark misses hard deletes), low-latency sync (<1 minute), or the source table has no `updated_at` column. Watermark when: simpler setup is preferred, CDC is too complex to operate, or the source system doesn't allow log access. Most cloud-native pipelines start with watermark and add CDC when deletes or latency become issues.
