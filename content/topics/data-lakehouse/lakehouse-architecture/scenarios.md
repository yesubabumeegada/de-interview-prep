---
title: "Lakehouse Architecture — Scenarios"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [lakehouse, scenarios, interview, design, architecture]
---

# Lakehouse Architecture — Interview Scenarios

## Scenario 1: Design a Lakehouse for an E-Commerce Company

**Question:** An e-commerce company has 500GB/day of clickstream events and 50GB/day of order/transaction data. They need: (1) marketing dashboards updated hourly, (2) data science team building recommendation models, (3) finance reports by end of business day. Design a lakehouse architecture.

**Answer:**

```
Requirements Analysis:
  Marketing: hourly freshness → micro-batch streaming
  Data Science: point-in-time correct features → Delta time-travel
  Finance: daily batch → end-of-day batch job
  Scale: 550GB/day = ~200TB/year stored

Architecture:

  Sources:
    Clickstream → Kafka (event bus, 500GB/day)
    Orders → Debezium → Kafka (CDC from Postgres, 50GB/day)
    Products → Fivetran → Bronze (batch, daily)

  Bronze Zone (s3://datalake/bronze/):
    clickstream/     → Kafka → Spark Streaming → Delta (append, 5-min micro-batch)
    orders/          → Kafka → Spark Streaming → Delta (append, MERGE in Silver)
    products/        → Fivetran → Delta (batch daily)
    Partition: by ingestion_date
    Retention: 7 years

  Silver Zone (s3://datalake/silver/):
    clickstream_clean/  → validated events, session_id resolved
    orders/             → upserted by order_id (handles CDC updates/deletes)
    products/           → SCD Type 2 (product history preserved)
    Schedule: Spark Streaming (5-min trigger for clickstream/orders)
    Partition: by event_date (orders), by session_date (clickstream)

  Gold Zone (s3://datalake/gold/):
    marketing/
      hourly_funnel_metrics/   → Spark Streaming (1-hour trigger)
      campaign_performance/    → daily dbt model
    finance/
      daily_revenue/           → dbt model, runs at 5 PM ET
      refund_reconciliation/   → dbt model, runs at 5 PM ET
    ds_features/
      customer_features/       → Spark batch job (daily, point-in-time safe)
      product_features/        → Spark batch job (daily)

  Serving:
    Tableau → Databricks SQL (Gold tables)
    Finance reports → Scheduled export to Snowflake OR direct Databricks SQL
    ML Training → spark.read.format("delta").option("timestampAsOf", ...).load(gold_features_path)
    Online Inference → Feast → Redis (feature serving <5ms)

  Catalog: Unity Catalog
    - Column masking on PII (customer email, phone)
    - Row access policy: finance team sees only finance gold tables
    - Lineage: Bronze → Silver → Gold tracked automatically

Compute: Databricks on AWS
  Ingestion cluster: always-on stream processing (3 × m5.xlarge)
  Transform cluster: auto-scaling batch jobs (1–10 nodes, spot workers)
  Interactive: Databricks SQL Pro (auto-suspend after 10 min)
```

---

## Scenario 2: Debug a Lakehouse Data Quality Issue

**Question:** The marketing team reports that the hourly funnel report shows 0 orders for the last 3 hours. The orders Silver table exists and has data from this morning. What's your debugging process?

**Answer:**

```
Step 1: Check Gold table freshness
  DESCRIBE HISTORY delta.`s3://datalake/gold/marketing/hourly_funnel_metrics`;
  -- Look at last modification timestamp. If it was 3 hours ago → Gold job stopped

Step 2: Check if Silver is still being updated
  SELECT MAX(event_time), COUNT(*) FROM silver.orders
  WHERE event_time > current_timestamp() - INTERVAL 4 HOURS;
  -- If empty: Silver streaming job died
  -- If populated: Gold job died (Silver is fine)

Step 3: Check Spark streaming job status
  # Databricks: check cluster Jobs → Streaming tab
  # EMR: YARN application status
  # Look for: StreamingQueryException, AnalysisException

Step 4: Check Kafka consumer lag (if Silver is empty)
  kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group spark-silver-orders
  -- If LAG is large: Spark fell behind (resource issue or slow processing)
  -- If LAG is 0 but Silver empty: Kafka topic is empty → upstream issue

Step 5: Check Bronze (is data arriving at all?)
  SELECT MAX(_ingested_at), COUNT(*) FROM bronze.orders
  WHERE _ingested_at > current_timestamp() - INTERVAL 4 HOURS;
  -- If empty: Debezium stopped or Kafka topic issue

Step 6: Fix and recover
  If Spark job died: restart job → it resumes from checkpoint (no data loss)
  If Kafka lag: scale up Spark cluster → it will catch up
  If Bronze empty: check Debezium connector status, restart if needed
  After recovery: Gold values will self-heal as Silver catches up
  
  NOTE: Bronze checkpoint ensures no data lost, just delayed.
  Marketing will see a spike in the next refresh as backlog clears.
```

---

## Scenario 3: Optimize a Slow Gold Table Query

**Question:** A dashboard query on `gold.daily_revenue` takes 45 seconds. The table has 3 years of data (1,095 partitions, each 200MB). The query is: `SELECT * FROM gold.daily_revenue WHERE region = 'US' AND order_date BETWEEN '2024-01-01' AND '2024-03-31'`

**Answer:**

```
Step 1: Diagnose
  DESCRIBE DETAIL delta.`s3://datalake/gold/daily_revenue`;
  -- Check: numFiles, sizeInBytes, partitionColumns

  -- If partitioned by order_date, the date filter prunes well (90 partitions)
  -- But region filter is a full scan within each partition → problem

Step 2: Add Z-ordering on region
  OPTIMIZE delta.`s3://datalake/gold/daily_revenue`
  ZORDER BY (region);
  -- This co-locates all 'US' data within each date partition
  -- Subsequent queries with region filter use data-skipping

Step 3: Add partition on region (if query is always region-filtered)
  -- More aggressive: re-partition the table
  df = spark.read.format("delta").load("s3://datalake/gold/daily_revenue")
  df.repartition(col("order_date"), col("region")) \
    .write.format("delta") \
    .partitionBy("order_date", "region") \
    .mode("overwrite") \
    .save("s3://datalake/gold/daily_revenue_v2")
  -- Trade-off: adds partition pruning for region; but creates more small files
  -- Only viable if (order_date, region) cardinality is manageable

Step 4: Pre-aggregate for the specific dashboard query
  -- If query is always aggregated, create a summary table
  CREATE OR REFRESH MATERIALIZED VIEW gold.us_revenue_quarterly AS
  SELECT order_date, SUM(revenue) AS total_revenue
  FROM gold.daily_revenue
  WHERE region = 'US'
  GROUP BY order_date;
  -- Dashboard queries hit this MV (<1 second) instead of full table

Expected result: 45 sec → 2-5 sec with Z-order; <1 sec with MV
```
