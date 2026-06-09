---
title: "Trade-Off Analysis — Scenarios"
topic: system-design
subtopic: trade-off-analysis
content_type: scenario_question
tags: [trade-offs, design-decisions, scenarios]
---

# Trade-Off Analysis — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Batch vs Streaming — Choosing the Right Approach

**Scenario:** Your team needs to calculate daily sales metrics (total revenue, order count, avg order value) for a dashboard. The business team says they need "real-time" data. How do you assess whether you truly need streaming or if batch suffices? What are the trade-offs?

<details>
<summary>💡 Hint</summary>

"Real-time" is often misused. Ask: what is the actual business decision being made and how quickly does it need data? A daily sales dashboard refreshing every 15 minutes is "near-real-time" and achievable with micro-batch — full streaming adds complexity without proportional value.

</details>

<details>
<summary>✅ Solution</summary>

**Clarifying Questions:**
1. How often does the business actually look at this dashboard?
2. What business decision requires real-time data? (e.g., detecting a payment outage vs monthly revenue reporting)
3. What is the cost of stale data? ($1M lost per minute vs $0)
4. What latency is "good enough"? 1 second? 1 minute? 15 minutes?

**Trade-Off Matrix:**

| Approach | Latency | Complexity | Cost | When to Use |
|----------|---------|-----------|------|-------------|
| Daily batch | Hours | Low | Low | Monthly/weekly reporting |
| Hourly micro-batch | ~1 hour | Low | Low | Most dashboards |
| 15-min micro-batch | ~15 min | Medium | Medium | Operational dashboards |
| Streaming (Flink) | Seconds | High | High | Fraud, alerting, SLAs |

**For Daily Sales Metrics: 15-min Micro-batch**

```python
# Airflow: run every 15 minutes
from airflow import DAG
from datetime import timedelta

dag = DAG(
    'sales_metrics_refresh',
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
)

# Simple dbt run: refresh materialized view
dbt_task = DbtCloudRunJobOperator(
    task_id='refresh_sales_metrics',
    job_id=dbt_job_id,
    wait_for_termination=True
)
```

```sql
-- Snowflake: DYNAMIC TABLE refreshes automatically every 15 min
CREATE OR REPLACE DYNAMIC TABLE gold.sales_metrics
    TARGET_LAG = '15 minutes'
    WAREHOUSE = analytics_wh
AS
SELECT
    date_trunc('day', order_timestamp) AS order_date,
    sum(order_total) AS total_revenue,
    count(*) AS order_count,
    avg(order_total) AS avg_order_value
FROM silver.orders
WHERE status = 'completed'
GROUP BY 1;
```

**When Streaming IS Worth It:**
- Fraud detection: block a $5000 fraudulent charge in <500ms
- Inventory alerts: flag out-of-stock in real time
- SLA breach detection: alert when API latency > threshold

**Cost of Full Streaming for This Use Case:**
- Flink cluster: +$3K/month
- Additional engineering: 2 weeks to build, 2 hours/week to maintain
- Value delivered vs 15-min micro-batch: near-zero for a daily dashboard

**Recommendation:** 15-minute Snowflake Dynamic Table. Re-evaluate if business requires <5-min latency.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Star Schema vs Data Vault — When to Choose Each

**Scenario:** Your organization is building a new enterprise data warehouse. The data modeling team is debating between a classic Kimball star schema and a Data Vault 2.0 approach. You're asked to make the recommendation for a retail analytics use case with 20 source systems that change frequently.

<details>
<summary>�hint 💡 Hint</summary>

Star schema is optimized for query performance and simplicity — great for stable domains. Data Vault is optimized for auditability and handling frequent source system changes — better for volatile, highly integrated environments. The 20 frequently-changing source systems is a key signal.

</details>

<details>
<summary>✅ Solution</summary>

**Star Schema:**
```
           dim_customer
               │
dim_product ──fact_sales── dim_store
               │
           dim_date
```
- Simple, fast for BI queries
- Denormalized → fewer joins
- Hard to extend when source systems change (requires rebuilding facts)
- Historical changes require SCD (slowly changing dimensions)

**Data Vault 2.0:**
```
Hub_Customer ──Link_Sale── Hub_Product
     │                          │
Sat_Customer_Details     Sat_Product_Details
(historized, source-tagged)
```
- Hubs: business keys only
- Links: relationships between hubs
- Satellites: descriptive attributes with full history
- Highly extensible: add new source → add new satellite

**For 20 Frequently-Changing Source Systems — Hybrid Approach:**

```sql
-- Data Vault Raw Vault (automated ingestion, source-agnostic)
-- Hub: business key
CREATE TABLE h_customer (
    customer_hk BINARY(20),      -- hash key
    customer_bk VARCHAR(50),     -- business key
    load_date TIMESTAMP,
    record_source VARCHAR(50)    -- which system loaded this
);

-- Satellite: attributes with history
CREATE TABLE s_customer_crm (
    customer_hk BINARY(20),
    load_date TIMESTAMP,
    load_end_date TIMESTAMP,
    record_source VARCHAR(50),
    name VARCHAR(100),
    email VARCHAR(200),
    -- hash diff for change detection
    hash_diff BINARY(20)
);

-- Business Vault: derived/cleaned attributes
CREATE TABLE bs_customer_profile AS
SELECT
    customer_hk,
    MAX(CASE WHEN record_source='CRM' THEN name END) AS crm_name,
    MAX(CASE WHEN record_source='ERP' THEN name END) AS erp_name,
    COALESCE(...) AS canonical_name
FROM s_customer_crm GROUP BY customer_hk;
```

```sql
-- Information Mart (Star Schema on top of Data Vault)
-- Best of both worlds: Data Vault handles source complexity,
-- Star Schema serves BI queries efficiently

CREATE TABLE dim_customer AS
SELECT
    customer_hk AS customer_key,
    canonical_name,
    email,
    segment,
    region
FROM bs_customer_profile
WHERE load_end_date IS NULL;  -- current records only

CREATE TABLE fact_sales AS
SELECT
    l.sale_hk,
    l.customer_hk AS customer_key,
    l.product_hk AS product_key,
    s.amount,
    s.sale_date
FROM link_sale l
JOIN sat_sale_details s ON l.sale_hk = s.sale_hk
WHERE s.load_end_date IS NULL;
```

**Recommendation: Data Vault Raw Vault → Business Vault → Star Schema Information Mart**

This hybrid gives:
- Flexibility to handle 20 changing source systems (Data Vault layer)
- Fast BI query performance (Star Schema layer)
- Full audit trail (Data Vault historization)
- Incremental loading without reprocessing (Data Vault append-only)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build vs Buy — Evaluating Open Source vs Managed Services

**Scenario:** Your team of 8 data engineers maintains a self-managed Kafka cluster (6 brokers), Airflow on Kubernetes, and Spark on EMR. You're spending ~40% of engineering time on infrastructure. A VP asks you to evaluate switching to managed services (Confluent Cloud, MWAA, Databricks). Present a structured build-vs-buy analysis.

<details>
<summary>💡 Hint</summary>

Build vs buy is a TCO (Total Cost of Ownership) analysis: direct costs (infrastructure) + indirect costs (engineering time, opportunity cost). Also consider: lock-in risk, feature velocity, operational reliability, and team skill development.

</details>

<details>
<summary>✅ Solution</summary>

**Current State (Build):**

| Component | Self-Managed Cost | Engineering Time |
|-----------|-----------------|-----------------|
| Kafka (6 brokers on EC2) | $8K/month | 1.5 FTE |
| Airflow on EKS | $3K/month | 0.5 FTE |
| EMR (batch clusters) | $15K/month | 0.5 FTE |
| **Total** | **$26K/month** | **2.5 FTE (31%)** |

At $200K/year blended engineer cost:
- 2.5 FTE infrastructure = $500K/year opportunity cost
- Total TCO: $312K (infra) + $500K (eng) = **$812K/year**

**Managed Services Evaluation:**

```
Confluent Cloud vs Self-Managed Kafka:
├── Confluent Cloud: $0.11/GB throughput + $0.003/partition/hour
│   At current volume (500GB/day): ~$18K/month
│   Engineering saved: 1.5 FTE ($300K/year)
│   Net: $18K × 12 + (-$300K) = -$84K/year (savings vs current)
│
├── AWS MSK (semi-managed): $8K/month
│   Engineering saved: 0.7 FTE ($140K/year)
│   Net: $96K + (-$140K) = -$44K/year (savings)
│
└── Self-managed (current): $8K/month + $300K eng = $396K/year
```

**Structured Analysis:**

```python
# TCO Model
class BuildVsBuyAnalysis:
    def __init__(self):
        self.engineer_cost_per_year = 200_000

    def calculate_tco(self, option: dict) -> dict:
        direct_cost = option['monthly_infra'] * 12
        indirect_cost = option['fte_ops'] * self.engineer_cost_per_year
        opportunity_cost = option['fte_ops'] * self.engineer_cost_per_year  # same as indirect
        total = direct_cost + indirect_cost

        return {
            'direct_annual': direct_cost,
            'engineering_annual': indirect_cost,
            'total_tco': total,
            'features_per_year': option.get('feature_velocity', 'unknown')
        }

options = {
    'self_managed': {'monthly_infra': 26_000, 'fte_ops': 2.5},
    'confluent_mwaa_databricks': {'monthly_infra': 45_000, 'fte_ops': 0.5},
    'msk_mwaa_emr': {'monthly_infra': 32_000, 'fte_ops': 1.0},
}
```

**Non-Financial Factors:**

| Factor | Self-Managed | Managed |
|--------|-------------|---------|
| Vendor lock-in | None | Medium-High |
| Feature velocity | Slow (we build) | Fast (vendor builds) |
| Reliability SLA | 99.5% (best effort) | 99.95% (contractual) |
| Team skill development | Deep infra knowledge | Product features |
| Compliance (SOC2, HIPAA) | We certify | Vendor certified |
| Customization | Full | Limited |

**Recommendation: Partial Managed Migration**

Phase 1 (Month 1-3): Kafka → AWS MSK
- Savings: $1.2M over 3 years vs Confluent
- Risk: Low (same Apache Kafka API)
- Eng savings: 1.0 FTE

Phase 2 (Month 4-6): Airflow → MWAA
- Savings: $240K over 3 years
- Risk: Low (same Airflow API)
- Eng savings: 0.5 FTE

Phase 3 (Month 7-12): Evaluate Databricks vs EMR
- Databricks: $30K/month but 0.5 FTE saved + better data scientist experience
- Decision: pilot on one workload, compare total cost + productivity

**Keep Self-Managed:**
- Custom Kafka connectors not available in Confluent
- Compliance requirements that prevent multi-tenant managed services

**Projected Outcome:**
- Infra ops time: 2.5 FTE → 0.5 FTE
- 2 engineers redirected to data product features
- TCO: $812K → $620K/year (24% reduction)
- SLA improvement: 99.5% → 99.9%

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you approach a trade-off question in an interview?" — Use a structured framework: (1) clarify requirements and constraints, (2) identify the key trade-off axes (cost, latency, complexity, consistency), (3) state your recommendation with reasoning, (4) acknowledge what you're trading away.
> **Tip 2:** "What is the CAP theorem and does it apply to data pipelines?" — CAP: a distributed system can guarantee at most 2 of: Consistency, Availability, Partition tolerance. For data pipelines, the relevant trade-off is usually CP (consistent, may be unavailable during partition) vs AP (always available, may serve stale data). Kafka with acks=all is CP; acks=1 is AP-leaning.
> **Tip 3:** "When would you accept eventual consistency in a data system?" — For analytics dashboards (stale by minutes is fine), recommendation systems (stale by hours is usually fine), and reporting (stale by hours acceptable). Never for financial transactions, inventory reservations, or any system where concurrent reads/writes on the same record cause business harm.
