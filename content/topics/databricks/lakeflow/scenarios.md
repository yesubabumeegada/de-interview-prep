---
title: "Lakeflow - Scenario Questions"
topic: databricks
subtopic: lakeflow
content_type: scenario_question
tags: [databricks, lakeflow, interview, scenarios]
---

# Scenario Questions — Lakeflow

<article data-difficulty="junior">

## 🟢 Junior: Choosing Ingestion Method

**Scenario:** You need to ingest data from: (A) PostgreSQL database (full CDC), (B) daily CSV files from a partner in S3, (C) Kafka event stream. Which Lakeflow/Databricks tool do you use for each?

<details>
<summary>💡 Hint</summary>
Match the source type to the appropriate tool: databases → Lakeflow Connect, files → Auto Loader, streams → Structured Streaming.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Source A: PostgreSQL (database CDC)
# Tool: Lakeflow Connect
# Why: managed CDC via WAL replication, no custom code needed
# Setup: Create connection in Unity Catalog → configure tables → start replication
# Result: production.bronze.pg_* tables kept in sync continuously

# Source B: CSV files in S3 (batch files from partner)
# Tool: Auto Loader (cloudFiles)
# Why: designed for incremental file ingestion, handles schema evolution
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .load("s3://partner-bucket/daily-drops/")
)
# Result: production.bronze.partner_data (new files processed automatically)

# Source C: Kafka event stream
# Tool: Structured Streaming (Kafka connector)
# Why: native Kafka integration, low-latency, exactly-once
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "events-topic")
    .load()
)
# Result: production.bronze.kafka_events (sub-minute latency)
```

| Source Type | Tool | Why |
|-------------|------|-----|
| Database (PostgreSQL, MySQL, Oracle) | Lakeflow Connect | Managed CDC, no code |
| SaaS (Salesforce, HubSpot) | Lakeflow Connect | Managed API connectors |
| Files in S3/ADLS | Auto Loader | File-based incremental ingestion |
| Kafka/Kinesis streams | Structured Streaming | Native stream processing |
| Custom APIs | Custom Python + Auto Loader | Write files to S3, Auto Loader picks up |

**Key Points:**
- Lakeflow Connect: for DATABASES and SaaS (managed CDC/connectors)
- Auto Loader: for FILES in cloud storage (any format)
- Structured Streaming: for MESSAGE STREAMS (Kafka, Kinesis, Event Hubs)
- Don't use Lakeflow Connect for files (it's for databases/APIs)
- Don't use Auto Loader for databases (it can't do CDC)
- Each tool is optimized for its source type

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Lakeflow Connect Setup

**Scenario:** Set up Lakeflow Connect to replicate the `orders` and `customers` tables from a PostgreSQL database into your Databricks lakehouse with CDC (real-time sync).

<details>
<summary>💡 Hint</summary>
Create a connection in Unity Catalog, configure the tables to replicate, set CDC mode, and specify the target catalog/schema.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Step 1: Create connection (UI or API)
# Catalog → Connections → Create → PostgreSQL
# Host: prod-db.us-east-1.rds.amazonaws.com
# Port: 5432
# Database: production
# Credentials: stored in Databricks Secret Scope

# Step 2: Configure replication
REPLICATION_CONFIG = {
    "connection": "pg_production",
    "tables": [
        {"source": "public.orders", "target": "production.bronze.orders"},
        {"source": "public.customers", "target": "production.bronze.customers"},
    ],
    "mode": "cdc",           # Real-time change data capture
    "schedule": "continuous", # Always running (vs scheduled)
}

# Step 3: Start replication
# UI: Start Ingestion Pipeline → monitors progress
# Lakeflow Connect automatically:
# 1. Performs initial full snapshot of both tables
# 2. Sets up logical replication slot in PostgreSQL
# 3. Begins streaming INSERT/UPDATE/DELETE changes
# 4. Writes to Delta tables in production.bronze schema

# Step 4: Verify
# Check target tables have data:
spark.table("production.bronze.orders").count()  # Should match source
spark.table("production.bronze.customers").count()

# Check CDC is working:
# Make a change in source PostgreSQL → verify it appears in Delta within seconds
```

**Key Points:**
- Lakeflow Connect handles: connection, initial snapshot, ongoing CDC, schema evolution
- CDC uses PostgreSQL's logical replication (WAL-based, low overhead on source)
- Target tables are standard Delta tables in Unity Catalog (query like any other table)
- Initial snapshot: automatic (no manual full load needed)
- After initial load: CDC streams changes continuously (~30s latency)
- Schema evolution: if source adds a column, target auto-adapts

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: End-to-End Lakeflow Pipeline

**Scenario:** Build a complete Lakeflow pipeline: PostgreSQL CDC → bronze → silver (cleaned, typed) → gold (daily metrics). Use Lakeflow Connect for ingestion and DLT for transformation.

<details>
<summary>💡 Hint</summary>
Lakeflow Connect handles bronze automatically. Write a DLT pipeline for silver (clean + dedup) and gold (aggregate). Orchestrate with a Workflow that triggers DLT after Connect updates.
</details>

<details>
<summary>✅ Solution</summary>

```python
# LAYER 1: BRONZE (Lakeflow Connect — no code, managed)
# Configured via UI: PostgreSQL → production.bronze.orders, production.bronze.customers
# Mode: continuous CDC
# Result: bronze tables always reflect latest PostgreSQL state

# LAYER 2 + 3: SILVER + GOLD (DLT Pipeline — code)
import dlt
from pyspark.sql.functions import *

# Silver: clean and deduplicate
@dlt.table(comment="Cleaned orders, latest version per order_id")
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("order_id", col("order_id").cast("bigint"))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
        .withColumn("order_date", col("order_date").cast("date"))
        .dropDuplicates(["order_id"])  # Latest version wins
    )

@dlt.table(comment="Clean customer dimension")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .withColumn("customer_id", col("customer_id").cast("bigint"))
        .select("customer_id", "name", "email", "region", "signup_date")
        .dropDuplicates(["customer_id"])
    )

# Gold: business metrics
@dlt.table(comment="Daily revenue by region")
def gold_daily_revenue():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    
    return (
        orders.join(customers, "customer_id", "left")
        .groupBy("order_date", "region")
        .agg(
            count("*").alias("total_orders"),
            sum("amount").alias("revenue"),
            countDistinct("customer_id").alias("unique_customers"),
        )
    )

# ORCHESTRATION: Databricks Workflow
# Task 1: Lakeflow Connect (continuous — always running)
# Task 2: DLT Pipeline (triggered every 15 minutes)
# Both write to production catalog, governed by Unity Catalog
```

**Key Points:**
- Bronze: fully managed by Lakeflow Connect (zero code, automatic CDC)
- Silver/Gold: DLT pipeline reads from bronze tables (streaming or batch)
- DLT handles: dependencies, incremental processing, quality expectations
- Orchestration: Workflow coordinates DLT triggers (every 15 min or continuous)
- End-to-end latency: depends on DLT trigger interval (15 min for triggered, 1 min for continuous)
- All tables governed by Unity Catalog (permissions, lineage, audit)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Schema Changes

**Scenario:** Your source PostgreSQL database team adds a new column `loyalty_tier VARCHAR(20)` to the orders table. How does Lakeflow Connect handle this, and what happens downstream in your DLT pipeline?

<details>
<summary>💡 Hint</summary>
Lakeflow Connect detects DDL changes and auto-adds the column to bronze. Downstream DLT pipeline behavior depends on how you wrote your transformations (explicit select vs select *).
</details>

<details>
<summary>✅ Solution</summary>

```python
# WHAT HAPPENS AUTOMATICALLY:

# 1. Source: ALTER TABLE orders ADD COLUMN loyalty_tier VARCHAR(20);
# 2. Lakeflow Connect detects the schema change via PostgreSQL catalog
# 3. Bronze Delta table: new column "loyalty_tier" added automatically
# 4. New rows include loyalty_tier; old rows have NULL for this column
# 5. No interruption to ingestion — happens seamlessly

# DOWNSTREAM IMPACT (depends on your DLT code):

# If silver uses explicit column list (CURRENT APPROACH):
@dlt.table
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select("order_id", "customer_id", "amount", "order_date", "status")
        # loyalty_tier NOT included → ignored until you add it here
    )
# Impact: None. New column is ignored until you explicitly add it.
# Action needed: add "loyalty_tier" to the select list when ready.

# If silver uses select("*") (flexible approach):
@dlt.table
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        # New column automatically flows through!
    )
# Impact: loyalty_tier automatically appears in silver table.
# Risk: untested column reaches silver without validation.

# RECOMMENDED PATTERN: explicit select + schema evolution alerting
@dlt.table
def silver_orders():
    bronze = dlt.read_stream("bronze_orders")
    
    # Known columns (validated)
    result = bronze.select(
        "order_id", "customer_id", "amount", "order_date", "status"
    )
    
    # Detect new columns (alert for review)
    known_cols = {"order_id", "customer_id", "amount", "order_date", "status", "_rescued_data"}
    new_cols = set(bronze.columns) - known_cols
    if new_cols:
        log_alert(f"New columns detected in bronze_orders: {new_cols}")
    
    return result
```

**Key Points:**
- Lakeflow Connect: handles schema changes AUTOMATICALLY in bronze (zero intervention)
- Old rows: get NULL for new column (backward compatible)
- Downstream DLT: behavior depends on your SELECT approach
- Explicit select: safe (new columns ignored until you add them) — recommended for production
- Select *: flexible but risky (untested data flows through)
- Best practice: explicit selects + alerting when new columns appear in bronze
- No downtime or pipeline failure from upstream schema changes!

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Lakeflow Architecture for Enterprise

**Scenario:** Design a Lakeflow-based data platform for a company with: 15 database sources (PostgreSQL, MySQL, Oracle), 8 SaaS connectors (Salesforce, HubSpot, Stripe, etc.), real-time requirements for 5 critical sources, and batch-OK for the rest. Budget: $10K/month. Include governance and monitoring.

<details>
<summary>💡 Hint</summary>
Tier sources by latency needs. Use Lakeflow Connect for databases/SaaS, Auto Loader for any file-based sources. Separate continuous (real-time) from scheduled (batch) to optimize cost. Unity Catalog for governance across all.
</details>

<details>
<summary>✅ Solution</summary>

```python
ENTERPRISE_ARCHITECTURE = {
    "ingestion_tier_1_realtime": {
        "sources": [
            "orders_pg (CDC)", "payments_pg (CDC)", "inventory_mysql (CDC)",
            "shipments_pg (CDC)", "customer_events_kafka"
        ],
        "tool": "Lakeflow Connect (continuous) + Structured Streaming (Kafka)",
        "latency": "< 1 minute",
        "compute": "Dedicated always-on connector cluster",
        "monthly_cost": "$3,000",
    },
    "ingestion_tier_2_frequent": {
        "sources": [
            "crm_pg (CDC)", "salesforce (API)", "hubspot (API)",
            "stripe (API)", "zendesk (API)", "auth_mysql (CDC)"
        ],
        "tool": "Lakeflow Connect (scheduled every 15 min)",
        "latency": "< 15 minutes",
        "compute": "Shared connector (runs 5 min, stops, repeats)",
        "monthly_cost": "$1,500",
    },
    "ingestion_tier_3_batch": {
        "sources": [
            "oracle_finance (incremental)", "workday_hr (daily)",
            "partner_sftp (files)", "analytics_exports (files)",
            "marketing_tools (API daily)"
        ],
        "tool": "Lakeflow Connect (daily) + Auto Loader (files)",
        "latency": "Daily (acceptable)",
        "compute": "Runs once/day for 30 min",
        "monthly_cost": "$500",
    },
    "transformation": {
        "tool": "Lakeflow Pipeline (DLT)",
        "config": "Triggered every 15 min (processes all new bronze data)",
        "layers": "Bronze → Silver (clean/dedup) → Gold (aggregations)",
        "compute": "Photon, auto-scale 4-12 workers",
        "monthly_cost": "$3,000",
    },
    "serving": {
        "tool": "Serverless SQL Warehouse",
        "purpose": "BI dashboards + analyst queries",
        "monthly_cost": "$1,500",
    },
    "governance": {
        "tool": "Unity Catalog",
        "features": [
            "All ingested tables auto-registered with lineage",
            "Row-level security on PII tables",
            "Column masking for sensitive fields",
            "Audit logs for compliance",
            "Auto-PII detection and tagging",
        ],
        "monthly_cost": "$500 (monitoring + governance automation jobs)",
    },
    "total_monthly": "$10,000 ✓ (within budget)",
}

# MONITORING:
# - Ingestion lag per source (alert if > threshold)
# - DLT pipeline duration and quality metrics
# - SQL Warehouse query performance
# - Cost per team (system.billing.usage + tags)
# - Data freshness SLAs (per source tier)
```

**Key Points:**
- Tier by latency: real-time ($$$) only for critical sources, batch for the rest
- Lakeflow Connect handles 20 of 23 sources natively (databases + SaaS)
- Auto Loader handles 3 file-based sources (partner files, exports)
- One DLT pipeline transforms ALL sources (bronze → silver → gold)
- Unity Catalog governs everything (one permission model, complete lineage)
- Cost optimization: tiering saves ~40% vs running everything real-time
- 23 sources, complete medallion, governed, monitored — all for $10K/month

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Migration from Fivetran to Lakeflow Connect

**Scenario:** Your company currently uses Fivetran ($5K/month) to ingest 12 sources into Snowflake, then copies to Databricks for ML/ETL. The CFO asks: can we replace Fivetran with Lakeflow Connect and eliminate the Snowflake staging? Design the migration.

<details>
<summary>💡 Hint</summary>
Lakeflow Connect replaces Fivetran for supported sources (databases, some SaaS). The data goes directly to Delta Lake (no intermediate Snowflake). Migration: parallel run, validate, cutover.
</details>

<details>
<summary>✅ Solution</summary>

```python
MIGRATION_PLAN = {
    "current_state": {
        "tool": "Fivetran",
        "cost": "$5,000/month (Fivetran) + $2,000/month (Snowflake staging)",
        "total": "$7,000/month",
        "sources": 12,
        "flow": "Sources → Fivetran → Snowflake → COPY to Databricks Delta (extra ETL!)",
        "latency": "~15-60 minutes (Fivetran sync + Snowflake copy)",
    },
    
    "target_state": {
        "tool": "Lakeflow Connect",
        "cost": "$2,500/month (Lakeflow Connect compute + ingestion)",
        "total": "$2,500/month (no Snowflake staging needed!)",
        "sources": 12,
        "flow": "Sources → Lakeflow Connect → Delta Lake directly",
        "latency": "< 5 minutes (direct CDC to Delta)",
    },
    
    "migration_steps": {
        "week_1": {
            "action": "Assess sources: which are supported by Lakeflow Connect?",
            "result": "10 of 12 supported natively (2 need custom code)",
        },
        "week_2": {
            "action": "Set up Lakeflow Connect for all 10 supported sources",
            "result": "Parallel: both Fivetran AND Lakeflow Connect running",
        },
        "week_3": {
            "action": "Validate: compare row counts and checksums between Fivetran output and Lakeflow Connect output",
            "result": "Confirm data matches (99.9%+ agreement)",
        },
        "week_4": {
            "action": "Switch downstream pipelines to read from Lakeflow Connect tables",
            "result": "All ETL/ML reads from Delta (not Snowflake copy)",
        },
        "week_5": {
            "action": "Turn off Fivetran for migrated sources, cancel Snowflake staging",
            "result": "Full cutover complete",
        },
        "week_6": {
            "action": "Build custom ingestion for 2 unsupported sources (Auto Loader + scripts)",
            "result": "All 12 sources running on Databricks-native tools",
        },
    },
    
    "savings": {
        "fivetran_eliminated": "$5,000/month",
        "snowflake_staging_eliminated": "$2,000/month",
        "lakeflow_connect_cost": "-$2,500/month",
        "net_savings": "$4,500/month ($54,000/year)",
        "additional_benefits": [
            "Lower latency (minutes vs hours)",
            "No intermediate data copy (single source of truth in Delta)",
            "Unified governance (Unity Catalog for ALL data)",
            "Simpler architecture (fewer moving parts)",
        ],
    },
}
```

**Key Points:**
- Lakeflow Connect replaces Fivetran for databases + supported SaaS (10 of 12 sources)
- Direct to Delta: eliminates Snowflake staging layer (cost + complexity reduction)
- Parallel run: both systems active during validation (zero-risk cutover)
- Net savings: $4,500/month ($54K/year) + simpler architecture + lower latency
- 2 unsupported sources: handle with custom code + Auto Loader (minimal effort)
- Migration timeline: 6 weeks (conservative, with thorough validation)
- The key selling point: data goes DIRECTLY to Delta Lake, no intermediate hops

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Databricks Lakeflow and what is its purpose?**
A: Lakeflow is Databricks' unified data engineering product that brings together ingestion (Lakeflow Connect), pipeline development (Lakeflow Pipelines / Delta Live Tables), and orchestration (Lakeflow Jobs / Databricks Workflows) under a single governed platform.

**Q: What is Lakeflow Connect and how does it simplify data ingestion?**
A: Lakeflow Connect provides managed, no-code connectors for ingesting data from SaaS applications and databases (Salesforce, Workday, databases via CDC) into Delta Lake. It handles schema evolution, incremental loading, and delivery guarantees without custom connector code.

**Q: How does Lakeflow Pipelines relate to Delta Live Tables?**
A: Lakeflow Pipelines is the product name for what was previously known as Delta Live Tables (DLT). The underlying technology is the same — a declarative ETL framework — but the Lakeflow branding positions it within the broader Lakeflow data engineering suite.

**Q: What governance capabilities does Lakeflow integrate with?**
A: Lakeflow integrates natively with Unity Catalog, providing automatic lineage tracking for all pipeline inputs and outputs, centralized access control, data discovery, and audit logging — all without additional configuration beyond Unity Catalog setup.

**Q: How does Lakeflow differ from a traditional ETL tool like Informatica or Talend?**
A: Lakeflow is cloud-native, code-first (SQL/Python), and tightly integrated with the Databricks Lakehouse stack (Delta Lake, Unity Catalog, Spark). Traditional ETL tools are often GUI-driven, proprietary, and require separate infrastructure. Lakeflow offers more flexibility for complex transformations but requires coding skills.

**Q: What is the role of Lakeflow Jobs in the Databricks ecosystem?**
A: Lakeflow Jobs is the orchestration layer (Databricks Workflows) that schedules and sequences data pipelines, notebook runs, DLT pipeline updates, and dbt jobs. It supports complex dependencies, retry logic, conditional branching, and event-based triggers.

**Q: How do you monitor Lakeflow pipeline health in production?**
A: Use the Databricks pipeline UI for run history and quality metrics, query the DLT event log (`system.event_log`) for detailed metrics, set up Databricks Workflows alerts for job failures, and export metrics to external monitoring tools (Datadog, PagerDuty) via the REST API or system tables.

**Q: What makes Lakeflow suitable for enterprise data engineering teams?**
A: Lakeflow provides end-to-end coverage from ingestion to serving within a single governed platform, reducing the number of tools to manage. Unity Catalog integration ensures consistent governance, and the managed infrastructure reduces operational overhead compared to self-managed open-source stacks.

---

## 💼 Interview Tips

- Position Lakeflow as an emerging product family — show awareness that it represents Databricks' strategic move to consolidate data engineering under one roof.
- Know that Lakeflow Pipelines = Delta Live Tables under a new name — don't be confused by branding changes in interviews.
- Be ready to discuss when Lakeflow makes sense vs. a mixed-tool approach (Airbyte + dbt + Airflow) — the key differentiators are governance integration and platform consolidation.
- Senior interviewers at Databricks-heavy organizations will ask about Unity Catalog integration — lineage and governance are key selling points of the Lakeflow platform.
- Show awareness that Lakeflow Connect positions Databricks to compete with tools like Fivetran and Airbyte in the managed connector space.
- Common mistake: treating Lakeflow as a single tool — it is a suite of capabilities, and knowing the distinction between Connect, Pipelines, and Jobs shows architectural maturity.
