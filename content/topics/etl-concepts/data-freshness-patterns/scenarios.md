---
title: "Data Freshness Patterns: Interview Scenarios"
description: "Practice scenarios for data freshness — from defining SLOs to designing enterprise-scale freshness monitoring systems."
content_type: scenario_question
topic: etl-concepts
subtopic: data-freshness-patterns
tags: [data-freshness, SLO, watermarks, freshness-monitoring, streaming, interview-practice]
---

<article data-difficulty="junior">

## Scenario: Define a Freshness SLO for a Sales Dashboard

Your company has a sales dashboard that Finance uses every morning at 09:00 UTC to review the previous day's revenue. The dashboard pulls from a `daily_sales` table in your data warehouse. The pipeline runs as a nightly batch job.

**Task:**
1. What information do you need from stakeholders before defining the SLO?
2. Write a formal SLO definition for this dashboard.
3. How would you implement a basic staleness check for this table?
4. What alert would you set up, and who should receive it?

<details>
<summary>✅ Solution</summary>

**1. Stakeholder Questions**

Before defining an SLO, gather:
- When must data be available? ("By 08:30 UTC so analysts can prep before 09:00")
- How stale is acceptable? ("Data should reflect events up to midnight UTC of the previous day")
- What is the consequence of a breach? ("Finance can't report — blocks the morning standup")
- How often can breaches occur? ("No more than once a month")

**2. Formal SLO Definition**

```
Dataset:        warehouse.daily_sales
SLI:            MAX(event_date) in the table, checked at 08:00 UTC daily
SLO:            MAX(event_date) = CURRENT_DATE - 1 day, measured daily at 08:00 UTC
Compliance:     ≥ 99.7% of daily checks (allows ~1 miss per year)
Error:          MAX(event_date) < CURRENT_DATE - 1, or table not refreshed by 08:00 UTC
Owner:          Data Engineering
Stakeholder:    Finance Analytics
Escalation:     Page on-call if data is not available by 08:30 UTC
```

**3. Staleness Check**

```sql
-- Run this at 08:00 UTC daily via your orchestration tool
SELECT
  MAX(event_date)                       AS latest_date,
  CURRENT_DATE - 1                      AS expected_date,
  MAX(event_date) = CURRENT_DATE - 1   AS is_fresh,
  DATEDIFF(CURRENT_DATE - 1, MAX(event_date)) AS days_stale
FROM warehouse.daily_sales;
```

**4. Alerting**

```python
# Airflow sensor to check freshness before 08:30 UTC
from airflow.sensors.sql import SqlSensor

freshness_check = SqlSensor(
    task_id="check_daily_sales_freshness",
    conn_id="warehouse",
    sql="""
        SELECT CASE WHEN MAX(event_date) = CURRENT_DATE - 1 THEN 1 ELSE 0 END
        FROM warehouse.daily_sales
    """,
    mode="poke",
    poke_interval=300,  # Check every 5 min
    timeout=1800,       # Fail after 30 min
    email_on_failure=True,
    email=["data-eng@company.com", "finance-analytics@company.com"]
)
```

Alert should go to: data engineering team (immediate fix) and Finance team lead (so they know to wait).

</details>

</article>

<article data-difficulty="mid">

## Scenario: Handle Late-Arriving Data in Spark Structured Streaming

You're building a real-time order analytics pipeline using Spark Structured Streaming. Orders are published to Kafka as JSON events with an `event_time` field. You need to compute the total order value per customer per 5-minute window.

**Problem constraints:**
- Mobile app users often submit orders with up to 15 minutes of delay (offline mode)
- Your downstream BI tool refreshes every 2 minutes
- You cannot afford to wait 15+ minutes for every window to close
- You need accurate totals but also timely updates

**Task:**
1. Design the watermark and window strategy
2. Show the PySpark code to implement it
3. Explain how you would handle the correctness vs. freshness trade-off
4. How do you notify downstream consumers when a window total is corrected?

<details>
<summary>✅ Solution</summary>

**1. Strategy**

Use a 2-tier approach:
- **Preliminary results:** Emit window aggregates after 2 minutes with watermark at 2 minutes (fast, potentially incomplete)
- **Final results:** Re-emit corrections when late events arrive, up to 15 minutes late
- Use `update` output mode so Spark only emits changed windows

**2. PySpark Implementation**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("OrderAnalytics").getOrCreate()

schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("event_time", TimestampType())

# Read from Kafka
orders_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .load()

orders = orders_raw \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Apply watermark: wait up to 15 minutes for late events
orders_with_watermark = orders.withWatermark("event_time", "15 minutes")

# 5-minute tumbling windows
windowed_totals = orders_with_watermark \
    .groupBy(
        F.window("event_time", "5 minutes").alias("window"),
        "customer_id"
    ) \
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("order_count"),
        F.max("event_time").alias("latest_event_time")
    ) \
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "customer_id",
        "total_amount",
        "order_count",
        "latest_event_time",
        F.current_timestamp().alias("updated_at"),
        F.lit(False).alias("is_final")  # Will be set True after watermark passes
    )

# Write to Delta with update mode — corrects existing rows as late events arrive
query = windowed_totals.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/checkpoints/order_windows") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="2 minutes") \
    .start("/data/order_windows")
```

**3. Correctness vs. Freshness Trade-Off**

With 15-minute watermark:
- Results appear within 2 minutes (trigger interval) after the window's events arrive
- Late events up to 15 min after window end will update the result
- After 15 minutes, the window is finalized and no more updates occur

This gives:
- **Freshness:** Results available within 2 minutes of events arriving
- **Correctness:** Up to 15 minutes of late event integration
- **Trade-off:** The first 15 minutes after a window closes, totals may be understated

**4. Notifying Downstream Consumers of Corrections**

```python
# Use Delta Lake change data feed to publish corrections
corrections_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("/data/order_windows") \
    .filter(F.col("_change_type") == "update_postimage") \
    .withColumn("is_correction", F.lit(True))

# Publish corrections to a separate Kafka topic so consumers can reconcile
corrections_stream.selectExpr(
    "CAST(customer_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "order-window-corrections") \
    .option("checkpointLocation", "/checkpoints/corrections") \
    .start()
```

</details>

</article>

<article data-difficulty="senior">

## Scenario: Design a Freshness Monitoring System for 500 Pipelines

Your company has grown to 500+ data pipelines spanning Airflow batch jobs, Kafka+Flink streaming pipelines, and dbt transformation models. The data team is receiving freshness complaints from 12 different business units. Currently there is no centralized freshness monitoring.

**Requirements:**
- Single pane of glass for all pipeline freshness
- Each pipeline/table has its own SLO (defined in a config)
- Alerts routed by business unit, not just data engineering
- Support for batch (event-date freshness) and streaming (lag-in-seconds freshness)
- Self-service: teams can define SLOs without engineering help
- Error budget tracking (how much SLO budget remains this month)

**Task:**
1. Design the overall architecture of the freshness monitoring system
2. Define the data model for SLOs, checks, and breaches
3. Describe the collection mechanism for batch vs. streaming freshness signals
4. Explain the alerting and escalation design
5. How does self-service SLO definition work?

<details>
<summary>✅ Solution</summary>

**1. Overall Architecture**

```
┌─────────────────────────────────────────────────────────────────┐
│                    FRESHNESS SIGNAL COLLECTORS                   │
│                                                                  │
│  Airflow Hook ──────────────────────┐                           │
│  (post_execute callback writes      │                           │
│   freshness metrics)                │                           │
│                                     ▼                           │
│  Flink/Kafka Lag Exporter ─────► Freshness Events API          │
│  (consumer group lag → minutes)     │  (REST + gRPC)            │
│                                     │                           │
│  dbt freshness results ─────────────┘                           │
│  (parsed from dbt artifacts)                                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FRESHNESS STORE                               │
│                                                                  │
│  slo_definitions table  (YAML-backed, version controlled)       │
│  freshness_signals table (time-series, partitioned by hour)     │
│  slo_evaluations table  (check results, breach flags)           │
│  error_budget_ledger    (rolling 30-day budget consumption)     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    ▼             ▼
           Alert Router       Freshness Dashboard
           (routes by BU,     (Grafana / custom UI)
            severity)
```

**2. Data Model**

```sql
-- SLO definitions (owned by teams via YAML + CI)
CREATE TABLE slo_definitions (
  slo_id            VARCHAR(100) PRIMARY KEY,
  pipeline_id       VARCHAR(200) NOT NULL,
  table_name        VARCHAR(200) NOT NULL,
  pipeline_type     VARCHAR(20) NOT NULL,    -- 'batch' | 'streaming'
  freshness_metric  VARCHAR(50) NOT NULL,    -- 'event_age_minutes' | 'consumer_lag_seconds'
  warn_threshold    INT NOT NULL,
  error_threshold   INT NOT NULL,
  check_interval_min INT NOT NULL DEFAULT 15,
  business_unit     VARCHAR(100) NOT NULL,
  owner_slack       VARCHAR(100),
  owner_email       VARCHAR(200),
  slo_target_pct    DECIMAL(5,2) NOT NULL DEFAULT 99.00,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Raw freshness signals from collectors
CREATE TABLE freshness_signals (
  signal_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  pipeline_id       VARCHAR(200) NOT NULL,
  signal_type       VARCHAR(30) NOT NULL,  -- 'batch_run_complete' | 'streaming_lag' | 'dbt_freshness'
  freshness_value   DECIMAL(10,2) NOT NULL,  -- Minutes stale or seconds lag
  max_event_time    TIMESTAMP,
  collected_at      TIMESTAMP NOT NULL,
  metadata          JSON,
  INDEX idx_pipeline_collected (pipeline_id, collected_at)
) PARTITION BY RANGE (UNIX_TIMESTAMP(collected_at)) (
  -- Monthly partitions
  PARTITION p_2024_01 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01')),
  PARTITION p_2024_02 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01'))
  -- etc.
);

-- SLO evaluation results
CREATE TABLE slo_evaluations (
  eval_id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  slo_id            VARCHAR(100) NOT NULL,
  evaluated_at      TIMESTAMP NOT NULL,
  freshness_value   DECIMAL(10,2) NOT NULL,
  status            VARCHAR(10) NOT NULL,    -- 'ok' | 'warn' | 'error'
  is_breach         BOOLEAN NOT NULL,
  alert_sent        BOOLEAN DEFAULT FALSE,
  INDEX idx_slo_evaluated (slo_id, evaluated_at)
);

-- Rolling error budget
CREATE TABLE error_budget_ledger (
  slo_id              VARCHAR(100) NOT NULL,
  budget_window_start DATE NOT NULL,
  total_checks        INT NOT NULL DEFAULT 0,
  breaches            INT NOT NULL DEFAULT 0,
  breach_minutes      DECIMAL(10,2) NOT NULL DEFAULT 0,
  budget_consumed_pct DECIMAL(5,2) NOT NULL DEFAULT 0,
  updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (slo_id, budget_window_start)
);
```

**3. Collection Mechanisms**

```python
# Batch: Airflow callback writes freshness signal after each DAG run
from airflow.listeners import hookimpl

class FreshnessCollector:
    @hookimpl
    def on_dag_run_success(self, dag_run, msg):
        # Called after each successful DAG run
        pipeline_id = dag_run.dag_id
        
        # Query the target table for max event time
        max_event_time = query_max_event_time(pipeline_id)
        freshness_minutes = (datetime.utcnow() - max_event_time).total_seconds() / 60
        
        requests.post("http://freshness-api/signals", json={
            "pipeline_id": pipeline_id,
            "signal_type": "batch_run_complete",
            "freshness_value": freshness_minutes,
            "max_event_time": max_event_time.isoformat(),
            "collected_at": datetime.utcnow().isoformat()
        })

# Streaming: Kafka consumer lag exporter (runs every minute)
from kafka.admin import KafkaAdminClient

def collect_kafka_lag(bootstrap_servers: str):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer_groups = admin.list_consumer_groups()
    
    for group_id, _ in consumer_groups:
        offsets = admin.list_consumer_group_offsets(group_id)
        for tp, offset_info in offsets.items():
            lag = offset_info.offset  # Simplified
            requests.post("http://freshness-api/signals", json={
                "pipeline_id": f"kafka/{group_id}/{tp.topic}",
                "signal_type": "streaming_lag",
                "freshness_value": lag,  # In seconds (converted from offset lag)
                "collected_at": datetime.utcnow().isoformat()
            })
```

**4. Alerting and Escalation Design**

```python
# Alert router: routes by business unit with escalation ladder
class AlertRouter:
    ESCALATION_LADDER = {
        "warn":  ["slack:#{bu}-data-alerts"],
        "error": ["slack:#{bu}-data-alerts", "email:{owner_email}"],
        "page":  ["slack:#{bu}-data-alerts", "email:{owner_email}", "pagerduty:{bu}-oncall"]
    }
    
    def route_alert(self, slo: dict, eval_result: dict):
        severity = eval_result["status"]
        bu = slo["business_unit"]
        
        # Suppress if recently alerted (30-min cooldown per SLO)
        if self._is_suppressed(slo["slo_id"]):
            return
        
        channels = self.ESCALATION_LADDER[severity]
        message = self._format_message(slo, eval_result)
        
        for channel in channels:
            channel_resolved = channel.replace("{bu}", bu) \
                                      .replace("{owner_email}", slo["owner_email"])
            self._send(channel_resolved, message)
        
        self._set_suppression(slo["slo_id"], minutes=30)
    
    def _format_message(self, slo: dict, eval_result: dict) -> str:
        return (
            f"*FRESHNESS BREACH* | {slo['table_name']}\n"
            f"Current lag: {eval_result['freshness_value']:.0f} min "
            f"(SLO: {slo['error_threshold']} min)\n"
            f"Business unit: {slo['business_unit']}\n"
            f"Error budget remaining: {eval_result['budget_remaining_pct']:.1f}%"
        )
```

**5. Self-Service SLO Definition**

Teams define SLOs via YAML files in a Git repo. A CI pipeline validates and applies them:

```yaml
# data-team/slos/finance/daily_sales.yaml
slo_id: finance-daily-sales-freshness
pipeline_id: airflow/daily_sales_etl
table_name: warehouse.daily_sales
pipeline_type: batch
freshness_metric: event_age_minutes
warn_threshold: 60    # Warn if data > 1 hour old
error_threshold: 120  # Error if data > 2 hours old
check_interval_min: 15
business_unit: finance
owner_slack: "#finance-data"
owner_email: finance-analytics@company.com
slo_target_pct: 99.5
```

```yaml
# .github/workflows/slo-deploy.yml
name: Deploy SLO Definitions
on:
  push:
    paths:
      - 'data-team/slos/**/*.yaml'

jobs:
  validate-and-deploy:
    steps:
      - name: Validate SLO YAML schemas
        run: python scripts/validate_slos.py data-team/slos/

      - name: Dry-run impact analysis
        run: python scripts/slo_impact.py --dry-run

      - name: Deploy to freshness store
        run: python scripts/deploy_slos.py data-team/slos/
        env:
          FRESHNESS_API_TOKEN: ${{ secrets.FRESHNESS_API_TOKEN }}
```

This gives teams full self-service: they open a PR with their YAML, get validation feedback, and on merge the SLO is automatically active within minutes.

</details>

</article>
