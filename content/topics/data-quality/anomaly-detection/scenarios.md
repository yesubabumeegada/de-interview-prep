---
title: "Anomaly Detection — Scenarios"
topic: data-quality
subtopic: anomaly-detection
content_type: scenario_question
tags: [anomaly-detection, interview, scenarios]
---

# Anomaly Detection — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Row Count Drop

**Scenario:** Your daily orders pipeline suddenly processes only 1,000 rows instead of the usual 50,000. How do you detect and respond?

<details>
<summary>💡 Hint</summary>

**Response steps:** 1. Alert the data team immediately 2. Check if the source system is down (check monitoring dashboards) 3. Check if there was a pipeline change deployed today 4. Check if a date filter was accidentally applied (e.g., wrong partition) 5. Do NOT use the partial data for downstream...

</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np

def check_row_count(current: int, historical: list[int], threshold: float = 3.0) -> dict:
    mean = np.mean(historical)
    std = np.std(historical)
    z = abs(current - mean) / max(std, 1)
    
    return {
        "anomaly": z > threshold,
        "current": current,
        "mean": mean,
        "z_score": round(z, 2),
        "pct_of_normal": round(current / mean * 100, 1),
    }

historical = [48000, 51000, 49500, 52000, 50500, 48500, 51500]
result = check_row_count(1000, historical)
# {'anomaly': True, 'current': 1000, 'mean': 50142.86, 'z_score': 17.7, 'pct_of_normal': 2.0}
```

**Response steps:**
1. Alert the data team immediately
2. Check if the source system is down (check monitoring dashboards)
3. Check if there was a pipeline change deployed today
4. Check if a date filter was accidentally applied (e.g., wrong partition)
5. Do NOT use the partial data for downstream reporting
6. If source is healthy but data is missing → check ingestion logs, rerun
7. If source is down → wait, then backfill when restored

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Revenue Distribution Shift

**Scenario:** Mean revenue per order dropped from $85 to $42 overnight. The row count is normal. How do you investigate?

<details>
<summary>💡 Hint</summary>

The row count is normal — so it's not a missing data problem. Think about *distributional* shifts: what could halve the mean without changing the count? Work through the likely causes in order: a new product category, a currency/unit change, a refund/reversal spike, or a data type/rounding issue. Use percentile comparisons and group-by breakdowns to isolate which segment drove the drop.

</details>

<details>
<summary>✅ Solution</summary>

**Investigation:**
```python
# Step 1: Check distribution shift, not just mean
import pandas as pd

today = pd.read_parquet("orders_today.parquet")
yesterday = pd.read_parquet("orders_yesterday.parquet")

# Percentile comparison
for p in [25, 50, 75, 90, 99]:
    t = today["amount"].quantile(p/100)
    y = yesterday["amount"].quantile(p/100)
    print(f"P{p}: Today={t:.2f}, Yesterday={y:.2f}, Change={t-y:+.2f}")

# Step 2: Check by segment
print(today.groupby("product_category")["amount"].mean())
print(yesterday.groupby("product_category")["amount"].mean())

# Step 3: Check for a new record type
print(today["order_type"].value_counts())
print(yesterday["order_type"].value_counts())
```

**Likely root causes:**
1. **New product category with lower prices** added to pipeline
2. **Currency conversion bug** — foreign currency orders not converted to USD
3. **Discount applied incorrectly** — discount_amount being subtracted wrong
4. **Partial data** — high-value orders from one region not yet ingested
5. **Schema change** — `amount` now stores cents instead of dollars

**Resolution:** Add distribution-aware anomaly detection (not just mean), segment metrics by region/category, and add currency validation.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Building a Data Observability System

**Scenario:** You're building a data observability system from scratch. Describe the architecture and key decisions.

<details>
<summary>💡 Hint</summary>

Think in layers: collection (metadata, row counts, distributions at each pipeline stage), detection (statistical baselines, ML-based anomalies, rule-based thresholds), alerting (tiered by severity, routed to owners), and a lineage layer that lets you trace *which upstream table* caused the anomaly. Distinguish between infrastructure observability (pipeline health) and data observability (content quality). Decide early whether to build on open standards (OpenLineage, Great Expectations) or build custom — and what the incremental path looks like.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```mermaid
flowchart TD
    A[Pipeline Runs<br>Airflow / Spark] --> B[Metrics Agent<br>Lightweight sidecar]
    B --> C[Metrics Ingestion API]
    C --> D[Time-Series Store<br>ClickHouse / Delta]
    D --> E[Anomaly Engine]
    E --> F[Alert Deduplicator]
    F --> G[Alert Router<br>Severity-based]
    G --> H[Slack - Warnings]
    G --> I[PagerDuty - Critical]
    D --> J[Observability Dashboard<br>Superset / Grafana]
    D --> K[Lineage Graph<br>OpenLineage / Marquez]
```

**Key design decisions:**

1. **What to collect automatically (no config):** Row count, null rates per column, schema fingerprint, max timestamp, run duration. These apply to every table.

2. **What requires explicit config:** Business metric ranges (revenue bounds), cross-table consistency checks, custom anomaly models.

3. **Metrics store:** ClickHouse for sub-second query on billions of metric rows. Alternative: Delta Lake with Spark queries (slower, but unified with pipeline storage).

4. **Alert deduplication:** Don't alert on the same anomaly twice within 24 hours. Group related anomalies (downstream tables affected by one upstream issue) into one incident.

5. **Lineage integration:** When anomaly detected in `silver.orders`, automatically query lineage graph to find downstream `gold.revenue` and `gold.customer_metrics` — include impact in the alert.

6. **Cost control:** Run lightweight volume/freshness checks on every batch. Run expensive ML-based distribution checks only daily or on configured tables.

7. **Progressive rollout:** Start advisory mode (log everything, alert nothing) for 30 days. Tune thresholds. Enable warnings. Enable critical alerts only for the most important tables.

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is data anomaly detection in the context of data engineering?**
A: Data anomaly detection identifies unexpected patterns, values, or volumes in datasets that may indicate data quality issues, pipeline failures, or upstream source problems. It differs from ML anomaly detection by focusing on data health rather than business signals.

**Q: What are the main types of data anomalies?**
A: Point anomalies (a single outlier value), contextual anomalies (a value that is unusual in a specific context, e.g., zero sales on a weekday), and collective anomalies (a group of related records that together signal an issue, e.g., a missing date range).

**Q: What statistical methods are commonly used for anomaly detection in data pipelines?**
A: Z-score (standard deviations from mean), IQR (interquartile range) for outliers, moving averages for trend deviation, and time series models (ARIMA, Prophet) for seasonal patterns. Simpler threshold-based rules (row count ±20%) are often the most practical first line of defense.

**Q: What is volume anomaly detection and how do you implement it?**
A: Volume anomaly detection monitors the number of rows ingested or processed per pipeline run. Implementation typically involves storing historical run metrics and alerting when current volume deviates beyond a threshold (e.g., >3 standard deviations from a rolling 30-day average).

**Q: How do tools like Monte Carlo or Bigeye approach automated anomaly detection?**
A: These tools continuously profile tables (row counts, null rates, value distributions, freshness) and use ML models trained on historical patterns to detect deviations. They alert data teams to anomalies without requiring explicit threshold configuration for every metric.

**Q: What is the difference between rule-based and ML-based anomaly detection for data quality?**
A: Rule-based detection uses explicit thresholds (e.g., null rate > 5%) and is transparent, fast, and easy to explain. ML-based detection learns patterns automatically and adapts to seasonality but requires historical data, can produce false positives, and is harder to debug.

**Q: How do you reduce alert fatigue in a data anomaly detection system?**
A: Prioritize alerts by business impact, implement severity tiers, use ML-based adaptive thresholds that account for seasonality and trends, require human acknowledgment before auto-escalation, and continuously tune thresholds based on false positive feedback.

**Q: What is freshness anomaly detection?**
A: Freshness anomaly detection monitors whether data in a table has been updated within the expected time window. It catches silent pipeline failures where a job completes without error but produces no new data — one of the most common and dangerous data quality issues.

---

## 💼 Interview Tips

- Start with the simplest effective approach — row count and null rate monitoring catches the majority of real-world data quality issues without complex ML.
- Be ready to discuss how you would implement anomaly detection from scratch in a data platform that has none, including what metrics to monitor first.
- Senior interviewers want to hear about alert fatigue — show that you understand that too many alerts are as harmful as no alerts.
- Mention freshness monitoring specifically; it is frequently overlooked and is one of the most impactful quality checks to implement.
- Connect anomaly detection to SLAs: anomalies only matter if they affect downstream consumers, so prioritization should be driven by business impact.
- Know the difference between detecting anomalies in ingested data (quality checks) vs. in pipeline behavior (operational monitoring) — both are needed.
