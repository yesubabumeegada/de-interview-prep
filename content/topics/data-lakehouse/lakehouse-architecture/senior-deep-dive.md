---
title: "Lakehouse Architecture — Senior Deep Dive"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [lakehouse, architecture, governance, multi-engine, open-standards]
---

# Lakehouse Architecture — Senior Deep Dive

## Open Lakehouse Architecture Principles

```
The "open" in Open Lakehouse means:
  1. Open file format: Parquet (columnar, not proprietary)
  2. Open table format: Delta, Iceberg, Hudi (not tied to one vendor)
  3. Open catalog: Hive Metastore, Nessie, Apache Polaris
  4. Open compute: Spark, Trino, Flink, DuckDB, StarRocks — any engine reads same data

Why openness matters:
  Vendor portability: switch Databricks → EMR without reformatting data
  Multi-engine: Flink for streaming ingest, Trino for interactive queries, 
                Spark for heavy transforms — all on same tables
  No data lock-in: you own the data in S3/GCS/ADLS

Databricks Lakehouse vs Open Lakehouse:
  Databricks: Delta Lake + Unity Catalog + DBR (highly optimized, tight integration)
  Open: Iceberg + Nessie/Polaris + Spark/Trino (portable, multi-cloud)
  Choice: Databricks if you're all-in on that ecosystem; Open if multi-engine needed
```

---

## Lakehouse Reference Architecture (Production-Grade)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                  │
│  OLTP DBs │ SaaS APIs │ Event Streams │ Files │ ML Predictions       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
            ┌────────────────▼────────────────┐
            │       Ingestion Layer            │
            │  Fivetran │ Debezium │ Kafka      │
            │  Spark Structured Streaming       │
            └────────────────┬────────────────┘
                             │
            ┌────────────────▼────────────────┐
            │        Bronze Zone               │
            │  Format: Delta/Iceberg (Parquet) │
            │  Schema: as-received + metadata  │
            │  Partition: by ingest date       │
            │  Retention: 7 years              │
            └────────────────┬────────────────┘
                             │  Spark Structured Streaming / Batch
            ┌────────────────▼────────────────┐
            │        Silver Zone               │
            │  MERGE (upsert by business key)  │
            │  Schema enforced, types cast     │
            │  PII masked, nulls handled       │
            │  Partition: by business date     │
            └────────────────┬────────────────┘
                             │  dbt / Spark
            ┌────────────────▼────────────────┐
            │     Gold Zone (Domain-specific)  │
            │  Aggregated, pre-joined          │
            │  Z-ordered for query patterns    │
            │  Materialized for BI tools       │
            └────────────────┬────────────────┘
                             │
       ┌─────────────────────┼────────────────────────┐
       │                     │                        │
  BI / SQL              ML Platform              Feature Store
  Tableau/Looker        MLflow + Spark           Feast (offline=Gold
  via Trino/Databricks  Training on Gold         online=Redis)
```

---

## Governance Framework for Production Lakehouse

```python
# Unity Catalog governance example (Databricks SQL)

-- 1. Create catalog hierarchy
CREATE CATALOG IF NOT EXISTS prod_lakehouse;
CREATE SCHEMA IF NOT EXISTS prod_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS prod_lakehouse.gold;

-- 2. Tag sensitive columns
ALTER TABLE prod_lakehouse.silver.customers
ALTER COLUMN email SET TAGS ('pii' = 'true', 'classification' = 'email');

ALTER TABLE prod_lakehouse.silver.customers
ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'classification' = 'ssn');

-- 3. Row-level access policy (multi-tenant)
CREATE ROW ACCESS POLICY tenant_isolation
AS (tenant_id STRING) RETURNS BOOLEAN
  RETURN is_member(concat('tenant_', tenant_id));

ALTER TABLE prod_lakehouse.silver.orders
ADD ROW FILTER tenant_isolation ON (tenant_id);

-- 4. Column masking (PII masking for non-privileged users)
CREATE FUNCTION mask_email(email STRING)
RETURNS STRING
  RETURN IF(is_member('pii_access'), email, regexp_replace(email, '(.).*@', '$1***@'));

ALTER TABLE prod_lakehouse.silver.customers
ALTER COLUMN email SET MASK mask_email;

-- 5. Audit log query
SELECT
  user_name,
  action_name,
  request_params,
  event_time
FROM system.access.audit
WHERE
  action_name IN ('commandExecute', 'dataRead')
  AND event_time > current_timestamp() - INTERVAL 24 HOURS
  AND request_params LIKE '%customers%'
ORDER BY event_time DESC;
```

---

## Lakehouse for ML Workflows

```
Why Lakehouse is the ideal ML data platform:

1. Unified historical + real-time data:
   ML training needs years of historical data (Bronze/Silver)
   Feature serving needs latest values (Gold → online store)
   Same tables serve both — no separate feature engineering pipeline

2. Point-in-time correct joins (critical for ML):
   WRONG: join customer features from today to historical labels
   RIGHT: join customer features from the date of the label
   
   Delta time-travel solves this:
   df = spark.read.format("delta").option("timestampAsOf", label_date).load(features_path)
   
3. Experiment tracking with data versioning:
   Store dataset version in MLflow with each experiment:
   mlflow.log_param("training_data_version", delta_version)
   
   Can replay any experiment with exact same data:
   df = spark.read.format("delta").option("versionAsOf", 42).load(training_data_path)

4. Feature store offline layer:
   Gold tables ARE the offline feature store
   No separate copy needed — Feast reads directly from Delta/Iceberg
```

---

## When Lakehouse Is NOT the Right Answer

```
Use-case: small analytics team, 10 analysts, 100GB data, no ML
  Right answer: Snowflake or BigQuery (managed, no ops, fast to start)
  Wrong answer: Set up Databricks + Unity Catalog + Delta Lake (over-engineered)

Use-case: real-time OLTP application, <10ms query latency needed
  Right answer: PostgreSQL / MySQL (OLTP database)
  Wrong answer: Lakehouse (seconds query latency, not milliseconds)

Use-case: unstructured data (PDFs, images, audio) only
  Right answer: Object storage (S3) + AI pipeline
  Wrong answer: Lakehouse (table formats add no value to unstructured data)

Use-case: regulated industry, strict audit, existing Snowflake investment
  Right answer: Stay on Snowflake (compliance tooling, no migration risk)
  Consider lakehouse: only if ML or raw data retention is a real need

Decision trigger: consider lakehouse when ANY of:
  1. You have ML workloads alongside BI workloads
  2. You need to retain raw data long-term
  3. You're paying for two copies of data (lake + warehouse)
  4. Your data engineering team > 5 people (enough to manage it)
```

---

## Interview Tips

> **Tip 1:** "How would you design a lakehouse from scratch for a new company?" — Start with S3 (storage), Delta Lake or Iceberg (table format), Spark on EMR or Databricks (compute), Glue/Unity Catalog (catalog). Define medallion zones (Bronze/Silver/Gold). Start simple: batch ingestion, daily cron. Add streaming when you have a latency requirement that needs it. Add catalog governance when you have multiple teams. Complexity grows with team size and data maturity.

> **Tip 2:** "How does a lakehouse handle schema evolution without breaking downstream consumers?" — Delta Lake schema evolution: `option("mergeSchema", "true")` adds new columns. Old columns remain. Downstream SQL queries that use explicit column names still work. If a column is renamed or dropped, downstream breaks — handle with: aliases, backward-compatible views in the catalog layer, or a schema registry for streaming. The catalog view layer acts as a contract between producers and consumers.

> **Tip 3:** "Databricks Lakehouse vs Snowflake — how do you choose?" — Not either/or. Frame it by workload: Snowflake is best for SQL analytics with large BI teams (better query isolation, easier RBAC, Marketplace). Databricks is best for ML + data engineering + streaming (Python-first, MLflow, Delta streaming, Unity Catalog lineage). Most large companies run both: Databricks for data engineering and ML, Snowflake for BI. The integration is well-established (Databricks → Snowflake via connector).
