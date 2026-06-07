---
title: "Lakeflow - Real-World Production Examples"
topic: databricks
subtopic: lakeflow
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, lakeflow, production, patterns, architecture]
---

# Lakeflow — Real-World Production Examples

## Pattern 1: Database Migration to Lakehouse

```python
# Migrate operational PostgreSQL → Databricks Lakehouse using Lakeflow Connect

MIGRATION_PLAN = {
    "source": "PostgreSQL (500 GB, 50 tables, 5000 TPS)",
    "target": "Databricks Lakehouse (production catalog)",
    
    "phase_1_setup": {
        "duration": "1 day",
        "steps": [
            "Create Unity Catalog connection to PostgreSQL",
            "Configure Lakeflow Connect for all 50 tables",
            "Set ingestion mode: CDC (continuous replication)",
            "Configure target: production.bronze schema",
        ],
    },
    "phase_2_initial_load": {
        "duration": "2-4 hours",
        "steps": [
            "Lakeflow Connect performs parallel initial snapshot",
            "500 GB / 16 parallel threads ≈ 2-3 hours",
            "Verify: row counts match source",
            "CDC begins from snapshot boundary (no gap)",
        ],
    },
    "phase_3_validation": {
        "duration": "1 week",
        "steps": [
            "Run in parallel: both source and lakehouse serving queries",
            "Compare query results between systems",
            "Monitor CDC lag (should be < 5 minutes)",
            "Validate schema evolution handling (test DDL on source)",
        ],
    },
    "phase_4_cutover": {
        "duration": "1 day",
        "steps": [
            "Switch read traffic from PostgreSQL to Databricks SQL",
            "Keep CDC running (lakehouse stays current with source)",
            "Eventually: source becomes write-only, all reads from lakehouse",
        ],
    },
}
```

---

## Pattern 2: Multi-Source Customer 360

```python
# Build unified customer view from 5 sources using Lakeflow

# INGESTION (Lakeflow Connect — managed, no code)
SOURCES = {
    "crm_db": {"type": "postgresql", "tables": ["customers", "contacts"], "mode": "cdc"},
    "salesforce": {"type": "salesforce", "objects": ["Account", "Opportunity"], "mode": "cdc"},
    "billing_db": {"type": "mysql", "tables": ["subscriptions", "invoices"], "mode": "cdc"},
    "support": {"type": "zendesk", "objects": ["tickets", "satisfaction"], "mode": "incremental"},
    "product_events": {"type": "auto_loader", "path": "s3://lake/events/", "format": "json"},
}
# Each source → production.bronze.{source}_{table}
# All managed by Lakeflow Connect (automatic schema sync, CDC, error handling)

# TRANSFORMATION (Lakeflow Pipeline — DLT code)
import dlt

@dlt.table
def silver_customer_unified():
    """Merge customer identity from all sources."""
    crm = dlt.read_stream("bronze_crm_customers")
    sf = dlt.read_stream("bronze_salesforce_account")
    billing = dlt.read_stream("bronze_billing_subscriptions")
    
    # Identity resolution: match by email across sources
    return (
        crm.select("customer_id", "name", "email", "phone", lit("crm").alias("source"))
        .unionByName(
            sf.select(col("Id").alias("customer_id"), "Name".alias("name"), 
                     "Email".alias("email"), "Phone".alias("phone"), lit("salesforce").alias("source")),
            allowMissingColumns=True
        )
        .unionByName(
            billing.select("user_id".alias("customer_id"), "full_name".alias("name"),
                          "email", lit(None).alias("phone"), lit("billing").alias("source")),
            allowMissingColumns=True
        )
    )

@dlt.table
def gold_customer_360():
    """Complete customer view with metrics from all sources."""
    customers = dlt.read("silver_customer_unified")
    tickets = dlt.read("bronze_support_tickets")
    events = dlt.read("bronze_product_events")
    
    customer_metrics = (
        customers.groupBy("email").agg(
            first("name").alias("name"),
            collect_set("source").alias("data_sources"),
        )
    )
    
    support_metrics = (
        tickets.groupBy("customer_email").agg(
            count("*").alias("total_tickets"),
            avg("satisfaction_score").alias("avg_satisfaction"),
        )
    )
    
    return customer_metrics.join(support_metrics, 
        customer_metrics.email == support_metrics.customer_email, "left")
```

---

## Pattern 3: Real-Time Operational Analytics

```python
# Lakeflow Connect CDC → near-real-time dashboards

# Source: production PostgreSQL (orders, inventory, shipments)
# Latency requirement: < 5 minutes from source change to dashboard update

# Step 1: Lakeflow Connect with continuous CDC
# Latency: source → bronze = ~30 seconds (WAL → Delta write)

# Step 2: DLT Pipeline in continuous mode
# Latency: bronze → silver → gold = ~60 seconds (streaming transforms)

@dlt.table
def gold_realtime_order_status():
    """Live order status for operations dashboard."""
    orders = dlt.read_stream("bronze_orders")
    inventory = dlt.read_stream("bronze_inventory")
    shipments = dlt.read_stream("bronze_shipments")
    
    return (
        orders
        .join(inventory, "product_id", "left")
        .join(shipments, "order_id", "left")
        .select(
            "order_id", "customer_name", "order_status",
            "inventory_available", "shipment_status",
            "estimated_delivery",
        )
    )

# Step 3: SQL Warehouse auto-refreshes dashboard every 1 minute
# End-to-end latency: ~2-3 minutes (well within 5-min requirement)

# Total architecture:
# PostgreSQL → Lakeflow Connect (CDC) → Bronze Delta → DLT Pipeline → Gold Delta → SQL Warehouse → Dashboard
# All managed, all streaming, all exactly-once
```

---

## Pattern 4: SaaS Data Consolidation

```python
# Consolidate data from 10 SaaS tools into a unified analytics layer

SAAS_SOURCES = [
    {"name": "Salesforce", "connector": "lakeflow_connect", "objects": ["Account", "Opportunity", "Lead"]},
    {"name": "HubSpot", "connector": "lakeflow_connect", "objects": ["contacts", "deals", "emails"]},
    {"name": "Stripe", "connector": "lakeflow_connect", "objects": ["charges", "subscriptions"]},
    {"name": "Zendesk", "connector": "lakeflow_connect", "objects": ["tickets", "users"]},
    {"name": "Google Analytics", "connector": "auto_loader", "format": "json"},  # Exported to S3
    {"name": "Mixpanel", "connector": "auto_loader", "format": "json"},
    # ... more SaaS sources
]

# Each source lands in: production.bronze.{source}_{object}
# All managed by Lakeflow Connect or Auto Loader
# Unity Catalog governs access to ALL data (one permission model)

# BENEFIT vs using Fivetran/Airbyte:
# 1. Data goes DIRECTLY to your lakehouse (no intermediate staging)
# 2. One governance model (Unity Catalog) across all sources
# 3. One billing (Databricks) instead of separate SaaS connector bill
# 4. Transformations in same platform (DLT) — no need to send data elsewhere
```

---

## Pattern 5: Cost-Optimized Lakeflow Architecture

```python
# Optimize Lakeflow costs for a mid-size company (20 data sources, $5K budget)

COST_OPTIMIZED_ARCHITECTURE = {
    "tier_1_critical": {
        "sources": ["orders_db", "payments_db", "inventory_db"],
        "mode": "Continuous CDC (real-time)",
        "compute": "Always-on (dedicated connector)",
        "monthly_cost": "$1,500",
        "rationale": "Revenue-impacting, need real-time visibility",
    },
    "tier_2_important": {
        "sources": ["crm_db", "salesforce", "hubspot", "support"],
        "mode": "CDC every 15 minutes (scheduled)",
        "compute": "Shared connector (runs 15 min, stops, repeats)",
        "monthly_cost": "$1,000",
        "rationale": "Important but 15-min freshness is sufficient",
    },
    "tier_3_batch": {
        "sources": ["analytics_exports", "partner_data", "finance_reports"],
        "mode": "Daily full refresh or incremental",
        "compute": "Shared connector (runs once/day, minimal cost)",
        "monthly_cost": "$300",
        "rationale": "Daily freshness acceptable, low volume",
    },
    "transformation": {
        "compute": "DLT pipeline (Photon, triggered hourly)",
        "monthly_cost": "$1,500",
    },
    "sql_analytics": {
        "compute": "Serverless SQL Warehouse (pay-per-query)",
        "monthly_cost": "$700",
    },
    "total": "$5,000 ✓ (within budget)",
}
```

---

## Interview Tips

> **Tip 1:** "Design a real-time analytics platform with Lakeflow" — Lakeflow Connect (continuous CDC) → bronze Delta tables (30s latency). DLT Pipeline (continuous mode) → silver/gold (30-60s latency). SQL Warehouse queries gold tables (auto-cached). End-to-end: source change → dashboard update in 2-3 minutes. All managed, exactly-once, governed by Unity Catalog.

> **Tip 2:** "How do you consolidate 10 SaaS sources?" — Lakeflow Connect for each source (managed connectors, CDC where available, incremental where not). All land in a unified bronze layer (one catalog, consistent schema). DLT Pipeline unifies/deduplicates across sources (identity resolution by email/ID). Gold layer has unified customer/product/transaction views for analytics.

> **Tip 3:** "How do you optimize Lakeflow costs?" — Tier sources by freshness needs: Tier 1 (real-time, continuous) for revenue-critical, Tier 2 (15-min batches) for important, Tier 3 (daily) for nice-to-have. Only replicate needed columns (not SELECT *). Use incremental mode where full CDC isn't needed. Share compute across multiple connectors where possible.
