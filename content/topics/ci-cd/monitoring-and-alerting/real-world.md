---
title: "Monitoring and Alerting - Real World"
topic: ci-cd
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, monitoring,alerting,observability,prometheus, real-world]
---

# Monitoring and Alerting — Real World

## Case Study: Monitoring Caught Silent Data Corruption for 3 Days

A customer analytics pipeline was producing wrong churn rates for 3 days before anyone noticed. The pipeline ran successfully (no errors), the correct number of rows processed — but a join condition change silently duplicated rows for multi-product customers.

**What monitoring would have caught it:**
- Row count monitor: expected ~50K rows/day, got ~90K. Alert threshold: > 10% deviation. Would have alerted within 24 hours.
- Null rate monitor: a derived column showed increased nulls. Alert on > 2% null increase.
- Reconciliation: compare to source system count. Discrepancy caught immediately.

**After adding these monitors:** Similar issues caught within 1 pipeline run (< 1 hour) rather than days.
