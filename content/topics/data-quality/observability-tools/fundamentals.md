---
title: "Data Observability Tools - Fundamentals"
topic: data-quality
subtopic: observability-tools
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [data-quality, observability, monitoring, data-reliability, lineage]
---

# Data Observability Tools — Fundamentals

## Analogy

Data observability is to data pipelines what application performance monitoring (APM) is to software. Just as Datadog tells engineers when an API response time spikes, data observability tools tell data teams when a table stops updating, a column distribution shifts, or a schema changes unexpectedly.

---

## Data Quality vs Data Observability

These terms are related but distinct:

| Dimension | Data Quality | Data Observability |
|---|---|---|
| **Focus** | Does data meet defined rules? | Is the data pipeline healthy? |
| **Approach** | Rule-based tests (expected values, not_null) | Anomaly detection on metrics over time |
| **Trigger** | Explicit test failures | Statistical deviation from baseline |
| **Example** | `order_id` must be unique | Row count dropped 80% vs yesterday |
| **Tools** | dbt tests, Great Expectations | Monte Carlo, Bigeye, Acceldata |

**Key insight:** Data quality checks catch *known* problems. Data observability catches *unknown* problems — issues you didn't think to write a test for.

---

## The Five Pillars of Data Observability

Coined by Monte Carlo, these five pillars cover what must be monitored:

### 1. Freshness
Is data arriving on time?
- **Metric:** Time since last record was inserted/updated
- **Alert:** "The `orders` table hasn't been updated in 3 hours — expected every 30 minutes"

### 2. Volume
Is the expected amount of data present?
- **Metric:** Row count per table, per partition, per hour
- **Alert:** "Row count dropped 90% compared to last Tuesday"

### 3. Schema
Have column names, types, or nullable properties changed?
- **Metric:** Column list and data type diff vs. baseline
- **Alert:** "`total_amount` changed from FLOAT64 to STRING in the orders table"

### 4. Distribution
Have the statistical properties of data changed?
- **Metric:** Null rate, cardinality, min/max/mean/p95 per column
- **Alert:** "Null rate for `customer_email` jumped from 2% to 45%"

### 5. Lineage
What was the upstream cause? What downstream tables are affected?
- **Metric:** Column-level and table-level dependency graph
- **Alert:** "The `fct_revenue` table depends on `stg_stripe_charges` which was last updated 6 hours ago"

---

## Commercial Tools Overview

### Monte Carlo
- Industry-leading commercial platform (founded 2019)
- **Strengths:** Automated anomaly detection, end-to-end lineage, incident management workflow
- **How it works:** Connects to your warehouse (BigQuery, Snowflake, Redshift) and query logs. Learns baseline patterns via ML. No rules to write for basic monitoring.
- **Key feature:** Automated lineage from SQL parsing — shows exactly which dashboards break when a table changes

### Bigeye
- **Strengths:** Automated threshold learning, column-level monitoring, strong Snowflake integration
- **How it works:** Profiles every table on a schedule, learns thresholds, alerts on deviation
- **Key feature:** "AutoMetrics" — automatically selects which metrics to monitor per column based on data type

### Acceldata
- **Strengths:** Multi-engine (Spark, Hive, Airflow, dbt), pipeline observability beyond just the warehouse
- **Key feature:** Monitors Spark job performance alongside data quality — end-to-end pipeline view

### Anomalo
- **Strengths:** ML-based anomaly detection with strong statistical foundations
- **Key feature:** Detects distribution shifts using learned seasonality (Monday vs. Friday patterns)

---

## Open-Source Observability: OpenMetadata & DataHub

### OpenMetadata
- Combines data catalog + data quality + lineage in one open-source platform
- Integrates with dbt (imports dbt tests as quality checks), Airflow (captures pipeline lineage), and most warehouses
- Self-hosted: deploy via Docker or Kubernetes

```yaml
# openmetadata_ingestion.yaml — connect to BigQuery
source:
  type: bigquery
  serviceName: prod_bigquery
  serviceConnection:
    config:
      type: BigQuery
      credentials:
        gcsConfig:
          path: /secrets/gcp-credentials.json
  sourceConfig:
    config:
      type: DatabaseMetadata
      includeViews: true
      includeTags: true
sink:
  type: metadata-rest
  config: {}
```

### DataHub (LinkedIn, open-source)
- Enterprise-grade data catalog with lineage, profiling, and data quality integration
- Supports "Assertions" — rules that run on a schedule and appear in the UI as pass/fail indicators

---

## Key Concepts to Know

**Anomaly detection approaches:**
- **Static threshold:** alert if row count < 1000 (simple, brittle)
- **Dynamic threshold:** alert if row count deviates > 3 standard deviations from the rolling 30-day average (adapts to growth)
- **Seasonal decomposition:** accounts for weekly patterns (Monday always has fewer orders than Friday)

**Lineage types:**
- **Table-level lineage:** Table A feeds Table B — shown as a graph
- **Column-level lineage:** specific column traced from source to report — critical for impact analysis

**Incident workflow:**
1. **Detect** — automated monitor fires
2. **Alert** — notification to Slack/PagerDuty
3. **Triage** — who owns this? what's the blast radius?
4. **Investigate** — use lineage to find root cause
5. **Remediate** — fix source data or pipeline
6. **Communicate** — notify downstream consumers

---

## Key Interview Points

- Data observability catches *unknown* problems; data quality tests catch *known* problems
- The five pillars: freshness, volume, schema, distribution, lineage
- Monte Carlo and Bigeye use ML to auto-learn thresholds — no manual rules needed for basic monitoring
- Lineage is critical for blast-radius analysis: "which dashboards break if this table changes?"
- Open-source alternatives: OpenMetadata (catalog + quality + lineage), DataHub (enterprise-grade catalog)
- Static thresholds are brittle; dynamic z-score or seasonal thresholds adapt to growth and day-of-week patterns
