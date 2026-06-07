---
title: "Lakehouse Architecture - Real-World Production Examples"
topic: databricks
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, lakehouse, production, enterprise, medallion, cost-optimization]
---

# Lakehouse Architecture — Real-World Production Examples

## Pattern 1: E-Commerce Lakehouse Platform

```python
# Complete e-commerce lakehouse: orders, inventory, customers, analytics

# BRONZE: Auto Loader ingests from multiple sources
sources = {
    "orders": {"path": "s3://lake/landing/orders/", "format": "json"},
    "inventory": {"path": "s3://lake/landing/inventory/", "format": "csv"},
    "clickstream": {"path": "s3://lake/landing/clicks/", "format": "json"},
    "customer_cdc": {"path": "s3://lake/landing/cdc/customers/", "format": "avro"},
}

# SILVER: Clean business entities
# orders_silver: deduplicated, typed, enriched with customer region
# inventory_silver: current stock levels (SCD Type 1)
# clickstream_silver: sessionized events with user mapping
# customers_silver: latest customer profile (from CDC)

# GOLD: Business metrics
gold_tables = {
    "daily_revenue": "Revenue by date/region/category (BI dashboards)",
    "customer_ltv": "Customer lifetime value (marketing segments)",
    "product_performance": "Product rankings (merchandising)",
    "funnel_metrics": "Conversion funnel (product team)",
    "inventory_alerts": "Low stock warnings (operations)",
}
```

---

## Pattern 2: Platform Team Operating Model

```python
# How a 5-person platform team manages a lakehouse for 200 data users

TEAM_STRUCTURE = {
    "platform_team": {
        "size": 5,
        "responsibilities": [
            "Unity Catalog governance (permissions, policies)",
            "Shared infrastructure (clusters, SQL warehouses)",
            "Bronze ingestion pipelines (Auto Loader)",
            "Data quality framework (expectations, monitoring)",
            "Cost optimization and monitoring",
        ],
    },
    "domain_teams": {
        "count": 8,
        "responsibilities": [
            "Silver transformations (their domain's business logic)",
            "Gold tables and data products",
            "Domain-specific data quality rules",
            "Self-serve analytics and ML",
        ],
    },
}

# Infrastructure managed by platform team (Terraform):
PLATFORM_INFRA = {
    "catalogs": ["production", "staging", "development"],
    "shared_resources": {
        "sql_warehouses": [
            {"name": "analytics-small", "size": "Small", "auto_stop": "10min", "users": "analysts"},
            {"name": "reporting-medium", "size": "Medium", "auto_stop": "15min", "users": "dashboards"},
        ],
        "job_clusters": {
            "etl-standard": {"node_type": "m5.xlarge", "min": 2, "max": 8, "spot": True},
            "etl-large": {"node_type": "r5.2xlarge", "min": 4, "max": 16, "spot": True},
        },
    },
    "monitoring": {
        "cost_alerts": "Alert if daily spend > $500",
        "pipeline_freshness": "Alert if bronze tables stale > 2 hours",
        "quality_dashboard": "Grafana: null rates, row counts, freshness per table",
    },
}
```

---

## Pattern 3: Cost Reporting and Chargebacks

```sql
-- Track costs by team/domain for chargeback

-- Usage by catalog (proxy for team)
SELECT 
    u.workspace_id,
    u.sku_name,
    u.usage_unit,
    u.custom_tags.team AS team_tag,
    SUM(u.usage_quantity) AS total_dbus,
    SUM(u.usage_quantity * 
        CASE u.sku_name 
            WHEN 'JOBS_COMPUTE' THEN 0.15
            WHEN 'SQL_COMPUTE' THEN 0.22
            WHEN 'ALL_PURPOSE_COMPUTE' THEN 0.40
        END
    ) AS estimated_cost_usd
FROM system.billing.usage u
WHERE u.usage_date >= DATE_TRUNC('month', current_date())
GROUP BY u.workspace_id, u.sku_name, u.usage_unit, u.custom_tags.team
ORDER BY estimated_cost_usd DESC;

-- Monthly report by domain team:
-- | team        | compute_type | dbus  | cost_usd |
-- | sales       | JOBS_COMPUTE | 5000  | $750     |
-- | marketing   | SQL_COMPUTE  | 3000  | $660     |
-- | ml-team     | ALL_PURPOSE  | 2000  | $800     |
-- | platform    | JOBS_COMPUTE | 8000  | $1,200   |
```

---

## Pattern 4: Migration from Data Warehouse

```python
# Migrating from Redshift to Databricks Lakehouse

MIGRATION_PLAN = {
    "phase_1_parallel": {
        "duration": "4 weeks",
        "actions": [
            "Set up lakehouse (catalogs, schemas, governance)",
            "Replicate Redshift tables to Delta via COPY/UNLOAD → S3 → Auto Loader",
            "Run BOTH systems in parallel (dual-write for new data)",
            "Validate: row counts, checksums, query results match",
        ],
    },
    "phase_2_cutover": {
        "duration": "2 weeks",
        "actions": [
            "Migrate BI dashboards to Databricks SQL",
            "Point ETL pipelines to lakehouse (not Redshift)",
            "Validate all downstream consumers work correctly",
            "Keep Redshift as read-only fallback for 2 weeks",
        ],
    },
    "phase_3_decomission": {
        "duration": "1 week",
        "actions": [
            "Verify no remaining Redshift queries",
            "Archive final Redshift snapshot to S3",
            "Terminate Redshift cluster",
            "Update documentation and runbooks",
        ],
    },
    "expected_savings": {
        "redshift_monthly": "$15,000 (ra3.xlplus × 4 nodes)",
        "lakehouse_monthly": "$4,000 (S3 + jobs compute + SQL warehouse)",
        "annual_savings": "$132,000",
    },
}
```

---

## Pattern 5: Data Quality Framework

```python
class LakehouseQualityFramework:
    """Standardized data quality checks across all medallion layers."""
    
    def __init__(self):
        self.checks = {
            "bronze": [
                self.check_freshness,           # Data arriving on time
                self.check_file_count,          # Expected number of files
            ],
            "silver": [
                self.check_freshness,
                self.check_null_rates,          # Key columns not null
                self.check_row_count_stability, # No unexpected drops/spikes
                self.check_uniqueness,          # Primary keys are unique
                self.check_referential_integrity, # FKs exist in parent
            ],
            "gold": [
                self.check_freshness,
                self.check_aggregation_totals,  # Sums match source
                self.check_completeness,        # All expected dimensions present
            ],
        }
    
    def run_checks(self, layer: str, table: str) -> dict:
        results = {}
        for check_fn in self.checks[layer]:
            result = check_fn(table)
            results[check_fn.__name__] = result
        
        failed = [name for name, r in results.items() if not r["passed"]]
        if failed:
            self.alert(f"Quality checks failed for {table}: {failed}")
        
        return results
    
    def check_freshness(self, table: str, max_hours: int = 4) -> dict:
        result = spark.sql(f"SELECT MAX(_loaded_at) as latest FROM {table}").collect()[0]
        hours_old = (datetime.now() - result["latest"]).total_seconds() / 3600
        return {"passed": hours_old < max_hours, "hours_stale": hours_old}
    
    def check_null_rates(self, table: str, threshold: float = 0.01) -> dict:
        # Check that key columns have <1% nulls
        schema = spark.table(table).schema
        key_cols = [f.name for f in schema if not f.nullable and f.name not in ("_loaded_at",)]
        
        for col_name in key_cols:
            null_pct = spark.sql(f"""
                SELECT SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) / COUNT(*) 
                FROM {table} WHERE _loaded_at >= current_date()
            """).collect()[0][0]
            
            if null_pct > threshold:
                return {"passed": False, "column": col_name, "null_pct": null_pct}
        
        return {"passed": True}
    
    def check_row_count_stability(self, table: str) -> dict:
        """Alert if today's count is <50% or >200% of yesterday's."""
        counts = spark.sql(f"""
            SELECT _loaded_date, COUNT(*) as cnt
            FROM {table}
            WHERE _loaded_date >= current_date() - 2
            GROUP BY _loaded_date
            ORDER BY _loaded_date
        """).collect()
        
        if len(counts) < 2:
            return {"passed": True, "note": "insufficient history"}
        
        today = counts[-1]["cnt"]
        yesterday = counts[-2]["cnt"]
        ratio = today / max(yesterday, 1)
        
        return {"passed": 0.5 <= ratio <= 2.0, "today": today, "yesterday": yesterday, "ratio": ratio}
```

---

## Interview Tips

> **Tip 1:** "How do you migrate from Redshift to a lakehouse?" — Three-phase approach: (1) Parallel run — replicate Redshift data to Delta Lake, run both systems simultaneously, validate results match. (2) Cutover — migrate BI dashboards and ETL to lakehouse, keep Redshift as read-only fallback. (3) Decommission — terminate Redshift after 2 weeks of stable lakehouse operation. Typical savings: 60-80% on compute costs.

> **Tip 2:** "How does a platform team manage a lakehouse for 200 users?" — Platform team (5 people) owns: infrastructure (Terraform), bronze ingestion (Auto Loader), governance (Unity Catalog), monitoring, and cost optimization. Domain teams (self-serve) own: silver/gold transformations, data products, and domain-specific quality rules. Platform provides guardrails; domains have autonomy within them.

> **Tip 3:** "How do you implement data quality in a lakehouse?" — Quality checks at each layer transition: Bronze (freshness, file count), Silver (null rates, uniqueness, referential integrity, row count stability), Gold (aggregation totals, completeness). Run after each pipeline execution. Alert on failure. Track quality metrics over time to detect degradation trends.
