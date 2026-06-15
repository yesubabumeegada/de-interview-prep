---
title: "Microsoft Fabric - Scenario Questions"
topic: azure
subtopic: microsoft-fabric
content_type: scenario_question
tags: [azure, microsoft-fabric, onelake, lakehouse, direct-lake, shortcuts, capacity-planning]
---

# Microsoft Fabric — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Choosing Between a Lakehouse and a Warehouse

Your team is adopting Microsoft Fabric for a new analytics project. You have two data consumers: (1) a data engineering team that runs PySpark transformations on 500 GB of daily transaction data, and (2) a BI team of 20 analysts who write T-SQL queries and build Power BI reports. Should you use a Lakehouse, a Warehouse, or both? Justify your answer.

<details>
<summary>✅ Solution</summary>

### Recommendation: Both — with appropriate usage per team

The correct answer is to use a **Lakehouse for data engineering** and a **Warehouse (or Lakehouse SQL endpoint) for BI analysts**. Here's why:

### For the Data Engineering Team: Lakehouse

```python
# Data engineers use Fabric Notebooks with PySpark
# Lakehouse is the natural choice:

# 1. Attach lakehouse to notebook
# 2. Read raw data
df = spark.read.format("delta").load("Files/raw/transactions/2024/")

# 3. Apply PySpark transformations
from pyspark.sql import functions as F

clean_df = df.filter(F.col("Amount").isNotNull()) \
    .withColumn("TransactionDate", F.to_date("TransactionDate")) \
    .withColumn("AmountUSD", F.col("Amount") * F.col("ExchangeRate"))

# 4. Write as Delta table in the lakehouse
clean_df.write.format("delta").mode("overwrite").saveAsTable("silver_transactions")
```

The Lakehouse supports:
- PySpark and SparkSQL natively
- Schema-on-read for flexible ingestion of raw data
- Delta Lake ACID transactions
- Direct read/write from notebooks

### For the BI Team: Warehouse (or Lakehouse SQL Endpoint)

The Warehouse provides a dedicated T-SQL MPP engine optimised for BI workloads:

```sql
-- Analysts write T-SQL in the Fabric Warehouse UI or SQL client
SELECT
    t.TransactionDate,
    p.ProductName,
    r.RegionName,
    SUM(t.AmountUSD) AS TotalRevenue,
    COUNT(*) AS TransactionCount
FROM silver_transactions t
JOIN dim_products p ON t.ProductId = p.ProductId
JOIN dim_regions r ON t.RegionId = r.RegionId
WHERE t.TransactionDate >= DATEADD(month, -3, GETDATE())
GROUP BY t.TransactionDate, p.ProductName, r.RegionName
ORDER BY TotalRevenue DESC;
```

The Warehouse provides:
- Full T-SQL support (familiar to SQL analysts)
- Higher concurrency for many simultaneous users
- Schema-on-write (DDL enforced — data quality visible to analysts)
- Direct integration with Power BI via Direct Lake

### Architecture Decision

```
Fabric Workspace
├── bronze_lakehouse   (data engineering: raw ingestion)
├── silver_lakehouse   (data engineering: PySpark transformations)
├── gold_lakehouse     (data engineering: final Delta tables)
│
└── analytics_warehouse  (BI analysts: T-SQL queries)
    └── Shortcut → gold_lakehouse tables
        (OR: copy gold tables to warehouse with COPY INTO)
```

**Key insight**: In Fabric, you can create a **Shortcut from the Warehouse pointing to the Lakehouse tables** — the BI team queries their T-SQL warehouse, which transparently reads the same Delta Parquet files maintained by data engineers. No data duplication, no sync jobs.

### What to Tell the Interviewer

"I'd use a Lakehouse for the data engineering team's PySpark workloads and a Warehouse (or the Lakehouse SQL endpoint) for the BI analysts. In Fabric, both are backed by the same Delta Parquet storage in OneLake, so we're not choosing between different data copies — we're choosing between different query engines on top of the same data. The Lakehouse SQL endpoint is actually a good middle ground: it lets the BI team query Lakehouse tables via T-SQL without needing a separate Warehouse item."

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Implementing Shortcuts for Cross-Team Data Sharing

Your organisation has 3 domain teams in Microsoft Fabric: Platform (owns canonical dimension tables), Finance (owns financial transactions), and Marketing (needs access to both). Currently, Marketing copies dimension data from Platform using an ADF pipeline that runs nightly, causing a 6-hour data lag and storage duplication. Redesign this using Fabric Shortcuts, and explain the access control model.

<details>
<summary>✅ Solution</summary>

### Current (Problematic) Architecture

```
Platform Workspace          ADF Pipeline (nightly)     Marketing Workspace
├── gold_lakehouse           ──────────────────────►   ├── marketing_lakehouse
│   └── dim_customers                                  │   └── dim_customers_COPY
│   └── dim_products                                   │   └── dim_products_COPY
│                                                      │   (6h stale, 2x storage)
└──────────────────────────────────────────────────────┘
```

### Redesigned Architecture with Shortcuts

```
Platform Workspace                    Marketing Workspace
├── gold_lakehouse                    ├── marketing_lakehouse
│   ├── dim_customers (master) ──────►│   ├── dim_customers (Shortcut)
│   └── dim_products  (master) ──────►│   ├── dim_products  (Shortcut)
│                                     │   └── fact_marketing_campaigns (owned)
│
Finance Workspace
├── finance_lakehouse
│   └── fact_transactions (master) ──►│   └── fact_transactions (Shortcut)
```

### Creating Shortcuts via Fabric REST API

```python
import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"
TOKEN = get_access_token()  # Azure AD token with Fabric.ReadWrite.All

def create_onelake_shortcut(
    target_workspace_id: str,
    target_lakehouse_id: str,
    table_name: str,
    source_workspace_id: str,
    source_lakehouse_id: str
):
    """Create a shortcut in marketing lakehouse pointing to platform's table."""
    url = (
        f"{FABRIC_API}/workspaces/{target_workspace_id}"
        f"/lakehouses/{target_lakehouse_id}/shortcuts"
    )
    payload = {
        "path": "Tables",
        "name": table_name,
        "target": {
            "type": "OneLake",
            "oneLake": {
                "workspaceId": source_workspace_id,
                "itemId": source_lakehouse_id,
                "path": f"Tables/{table_name}"
            }
        }
    }
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {TOKEN}"},
        json=payload
    )
    response.raise_for_status()
    print(f"Created shortcut: {table_name} → {source_workspace_id}")
    return response.json()

# Create shortcuts
create_onelake_shortcut(
    target_workspace_id=MARKETING_WORKSPACE_ID,
    target_lakehouse_id=MARKETING_LAKEHOUSE_ID,
    table_name="dim_customers",
    source_workspace_id=PLATFORM_WORKSPACE_ID,
    source_lakehouse_id=PLATFORM_GOLD_LAKEHOUSE_ID
)

create_onelake_shortcut(
    target_workspace_id=MARKETING_WORKSPACE_ID,
    target_lakehouse_id=MARKETING_LAKEHOUSE_ID,
    table_name="dim_products",
    source_workspace_id=PLATFORM_WORKSPACE_ID,
    source_lakehouse_id=PLATFORM_GOLD_LAKEHOUSE_ID
)
```

### Access Control Model

```
Permission Structure:

Platform Workspace:
  └── Platform Team (Admins)
  └── Finance Team (Viewers)     ← can see but not modify
  └── Marketing Team (Viewers)   ← can see but not modify

Finance Workspace:
  └── Finance Team (Admins)
  └── Marketing Team (Viewers)

Marketing Workspace:
  └── Marketing Team (Admins)
  └── [No access for Platform or Finance teams]
```

**How Shortcuts respect permissions:**
- A Shortcut in Marketing Workspace reads data from Platform Workspace.
- Marketing team members need **Viewer** access to Platform Workspace to access the shortcut.
- If a Marketing team member's access to Platform Workspace is revoked, the shortcut returns an error — the shortcut doesn't bypass source permissions.

```bash
# Grant Marketing team viewer access to Platform workspace
# (via Fabric workspace settings → Manage access)
az rest --method post \
  --url "https://api.fabric.microsoft.com/v1/workspaces/{platform_workspace_id}/roleAssignments" \
  --body '{
    "principal": {
      "id": "<marketing_aad_group_id>",
      "type": "Group"
    },
    "role": "Viewer"
  }'
```

### Validation

```python
# In Marketing team's Fabric Notebook — validate shortcuts work
dim_customers = spark.table("dim_customers")  # reading via shortcut
dim_products = spark.table("dim_products")
fact_tx = spark.table("fact_transactions")

print(f"Customers: {dim_customers.count()} rows")
print(f"Products: {dim_products.count()} rows")
print(f"Transactions: {fact_tx.count()} rows")

# Verify no data lag — shortcut reads live Delta files
from pyspark.sql.functions import max as spark_max
latest = dim_customers.select(spark_max("updated_at")).collect()[0][0]
print(f"Most recent customer update: {latest}")
```

### Benefits vs. Old Architecture

| Metric | ADF Copy Pipeline | Shortcuts |
|--------|------------------|-----------|
| Data freshness | 6h lag | Real-time (zero lag) |
| Storage cost | 2× (source + copy) | 1× (source only) |
| Maintenance | Nightly pipeline, ADF cost | Zero maintenance |
| Governance | Two copies, inconsistency risk | Single source of truth |

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Designing a Fabric Capacity and Governance Model for Enterprise Adoption

Your company is adopting Microsoft Fabric for 8 business units with 150 data engineers and analysts. The CTO has concerns about: (a) cost governance — preventing one team from consuming all capacity, (b) data governance — ensuring sensitive PII data in OneLake is protected, (c) development lifecycle — how engineers develop safely without breaking production. Design the Fabric architecture, capacity model, and governance framework.

<details>
<summary>✅ Solution</summary>

### Capacity Architecture: Multiple Capacities

**Anti-pattern**: One giant F256 capacity for everyone — one runaway Spark job starves all other teams.

**Production pattern**: Multiple capacities with isolation boundaries.

```
Capacity Strategy:
┌──────────────────────────────────────────────────────────────────┐
│ F64 — Production Capacity (All prod workspaces)                   │
│   ├── Finance-Prod Workspace      (CU budget enforced)           │
│   ├── Marketing-Prod Workspace    (CU budget enforced)           │
│   ├── Platform-Prod Workspace     (owns shared dims)             │
│   └── ... (5 more business units)                                │
│                                                                   │
│ F16 — Development Capacity (All dev workspaces)                   │
│   ├── Finance-Dev Workspace                                       │
│   ├── Marketing-Dev Workspace                                     │
│   └── ... (isolated from prod)                                   │
│                                                                   │
│ F8 — Test/Staging Capacity                                        │
│   └── Staging workspaces (pre-prod validation)                   │
└──────────────────────────────────────────────────────────────────┘
```

**Why separate capacities (not separate workspaces on one capacity)?**
- CU throttling is per-capacity. A runaway job on prod capacity cannot be isolated at workspace level.
- Dev/prod separation prevents development Spark jobs from throttling production Power BI reports.
- Different F SKUs right-sized for each environment's actual load.

### Per-Workspace CU Limits

Within a capacity, set workspace-level CU limits to prevent one team from consuming everything:

```python
# Set workspace CU limits via Fabric REST API
import requests

def set_workspace_cu_limit(workspace_id: str, max_cu_percent: int):
    """
    Limit a workspace to max_cu_percent of the total capacity.
    E.g., max_cu_percent=25 on F64 = max 16 CUs for this workspace.
    """
    requests.patch(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}",
        headers={"Authorization": f"Bearer {get_token()}"},
        json={
            "capacityThrottle": {
                "maxPercent": max_cu_percent
            }
        }
    )

# Give each of 8 business units up to 20% of F64 capacity (16 CUs each)
# Sum > 100% is intentional — smoothing means they don't all peak simultaneously
for workspace_id in business_unit_workspace_ids:
    set_workspace_cu_limit(workspace_id, max_cu_percent=20)

# Platform team gets 40% — they own shared infrastructure
set_workspace_cu_limit(platform_workspace_id, max_cu_percent=40)
```

### Data Governance Model with Microsoft Purview

```
Governance Framework:

1. Sensitivity Labels (Microsoft Purview → Fabric):
   ├── Public     → marketing attribution data
   ├── Internal   → business KPIs, operational reports
   ├── Confidential → financial data, customer segments
   └── Highly Confidential → PII (name, email, SSN, payment data)

2. Auto-labelling policy:
   ├── Tables with columns matching PII pattern → auto-label Highly Confidential
   ├── Tables in finance_gold_lakehouse → auto-label Confidential
   └── Sensitivity labels cascade to shortcuts and Power BI reports

3. Conditional access:
   ├── Highly Confidential items → require MFA + compliant device
   └── Confidential items → require managed device
```

### PII Protection in Notebooks

```python
# In Fabric Notebook — data engineers must mask PII before writing to non-HC lakehouses

from pyspark.sql import functions as F
import hashlib

def mask_pii_for_analytics(df):
    """Transform PII columns to analytics-safe representations."""
    return df \
        .withColumn("email_domain",
                    F.split(F.col("email"), "@").getItem(1)) \
        .withColumn("customer_hash",
                    F.sha2(F.col("customer_id").cast("string"), 256)) \
        .drop("email", "first_name", "last_name", "phone", "ssn") \
        .withColumn("age_band",
                    F.when(F.col("age") < 25, "18-24")
                    .when(F.col("age") < 35, "25-34")
                    .when(F.col("age") < 45, "35-44")
                    .otherwise("45+")) \
        .drop("age", "dob")

# Only raw/bronze lakehouses hold PII (labeled Highly Confidential)
# Silver/gold lakehouses receive masked data (labeled Confidential or Internal)
silver_safe = mask_pii_for_analytics(bronze_customers)
silver_safe.write.format("delta").saveAsTable("silver_customers_masked")
```

### Development Lifecycle: Three-Workspace Pattern

```
Development Lifecycle:
┌────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  DEV Workspace         TEST Workspace          PROD Workspace        │
│  (F16 capacity)        (F8 capacity)           (F64 capacity)        │
│                                                                     │
│  Data Engineers        QA Engineers            Data Consumers        │
│  develop notebooks,    validate changes,       (read-only access     │
│  pipelines, semantic   run integration tests,  for analysts)         │
│  models here           approve before prod     │                     │
│          │                    │               │                     │
│          └──── Git branch ────┘               │                     │
│               (dev → test)                    │                     │
│                              └── Fabric Deployment Pipeline ───────►│
│                                  (test → prod, with approval gate)  │
└────────────────────────────────────────────────────────────────────┘
```

### Git Integration Configuration

```yaml
# .gitignore for Fabric workspace (committed to Git)
# What's tracked:
# - Notebook code (.py files)
# - Pipeline definitions (JSON)
# - Semantic model definitions (TMDL)

# What's NOT tracked:
# - Delta table data (lives in OneLake, environment-specific)
# - Lakehouse binary metadata
```

### Monitoring and Cost Governance

```python
# Weekly Fabric capacity report (automated via Power Automate or Python)
import requests
from datetime import datetime, timedelta

def get_capacity_usage_report(capacity_id: str, days: int = 7):
    """Pull capacity metrics for cost governance review."""
    # Fabric Capacity Metrics App exposes KQL API
    query = f"""
    CapacityUsage
    | where TimeGenerated > ago({days}d)
    | summarize
        AvgCU = avg(CapacityUnitSeconds / 3600),
        MaxCU = max(CapacityUnitSeconds / 3600),
        TotalCostUnits = sum(CapacityUnitSeconds)
        by WorkspaceName, ItemType
    | order by TotalCostUnits desc
    """
    # Query via Log Analytics API if Capacity Metrics forwarded there
    # Return per-team breakdown for chargeback

# Monthly chargeback report:
# Platform team → 40% of F64 cost
# Finance team → 15% of F64 cost
# Marketing team → 12% of F64 cost
# ... etc.
```

### Rollout Strategy

```
Month 1-2: Pilot (2 teams, F16 dev capacity)
  - Finance team migrates Power BI to Direct Lake
  - Validate performance and governance model

Month 3-4: Dev/Test Environment (all 8 teams)
  - Provision F16 dev + F8 test capacities
  - Train data engineers on Fabric notebooks and pipelines
  - Establish Git integration and CI/CD pipeline

Month 5-6: Production Migration
  - Provision F64 prod capacity
  - Migrate domain lakehouses one team at a time
  - Decommission equivalent Azure Synapse / ADF resources as migrated

Month 7+: Optimisation
  - Right-size F SKU based on actual CU consumption
  - Implement Shortcut-based data mesh
  - Enable Copilot for teams on F64+
```

### Success Metrics

| Metric | Baseline | 6-Month Target |
|--------|----------|----------------|
| Power BI refresh time | 45 min (Import) | 0 min (Direct Lake) |
| Data lag (cross-team) | 6h (copy pipelines) | Real-time (Shortcuts) |
| Monthly infrastructure cost | $11,000 | $8,500 |
| Incident response (data freshness) | 2/month | 0 (Direct Lake) |
| PII exposure incidents | Tracked | 0 (label enforcement) |

</details>
</article>
