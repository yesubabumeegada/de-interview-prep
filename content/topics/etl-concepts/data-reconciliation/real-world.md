---
title: "Data Reconciliation - Real World"
topic: etl-concepts
subtopic: data-reconciliation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, data-reconciliation, production, case-study, financial, automated]
---

# Data Reconciliation — Real World

## Case Study 1: Automated Revenue Reconciliation System

### Problem

A subscription SaaS company had revenue figures that differed between: (1) the payment processor reports, (2) the data warehouse, and (3) the accounting system. Monthly close took 3 days of manual reconciliation. Finance couldn't trust dashboards during the close period.

### Solution: Automated Three-Way Reconciliation Pipeline

```python
from decimal import Decimal
from datetime import date
import pandas as pd
import sqlalchemy as sa

class ThreeWayRevenueReconciler:
    def __init__(self, stripe_engine, warehouse_engine, quickbooks_engine, results_engine):
        self.stripe     = stripe_engine
        self.warehouse  = warehouse_engine
        self.qb         = quickbooks_engine
        self.results    = results_engine

    def reconcile_month(self, year: int, month: int) -> dict:
        month_start = date(year, month, 1)
        month_end   = date(year, month, 28)  # Simplified; use proper month-end in prod

        # Pull from all three systems
        stripe_total = self._get_stripe_revenue(month_start, month_end)
        wh_total     = self._get_warehouse_revenue(month_start, month_end)
        qb_total     = self._get_quickbooks_revenue(month_start, month_end)

        results = {
            "period":         f"{year}-{month:02d}",
            "stripe":         str(stripe_total),
            "warehouse":      str(wh_total),
            "quickbooks":     str(qb_total),
            "discrepancies":  [],
            "passed":         True,
        }

        tolerance = Decimal("0.01")  # $0.01 tolerance for rounding

        for comparison in [
            ("stripe",    stripe_total,  "warehouse",  wh_total),
            ("warehouse", wh_total,      "quickbooks",  qb_total),
        ]:
            name_a, val_a, name_b, val_b = comparison
            diff = abs(val_a - val_b)
            if diff > tolerance:
                results["discrepancies"].append({
                    "systems":    f"{name_a} vs {name_b}",
                    "diff_usd":   str(diff),
                    "severity":   "critical" if diff > 100 else "warning",
                })
                results["passed"] = False

        # Persist for audit trail
        self._save_results(results)

        if not results["passed"]:
            self._alert_finance_team(results)

        return results

    def _get_stripe_revenue(self, start: date, end: date) -> Decimal:
        sql = """
            SELECT COALESCE(SUM(amount_usd), 0)
            FROM stripe_charges
            WHERE status = 'succeeded'
              AND charge_date BETWEEN :s AND :e
        """
        val = pd.read_sql(sa.text(sql), self.stripe, params={"s": start, "e": end}).iloc[0, 0]
        return Decimal(str(val))

    def _get_warehouse_revenue(self, start: date, end: date) -> Decimal:
        sql = """
            SELECT COALESCE(SUM(revenue_usd), 0)
            FROM warehouse.monthly_revenue
            WHERE revenue_month BETWEEN :s AND :e
        """
        val = pd.read_sql(sa.text(sql), self.warehouse, params={"s": start, "e": end}).iloc[0, 0]
        return Decimal(str(val))

    def _get_quickbooks_revenue(self, start: date, end: date) -> Decimal:
        sql = """
            SELECT COALESCE(SUM(credit_amount), 0)
            FROM gl_entries
            WHERE account_code = '4000'  -- Revenue account
              AND entry_date BETWEEN :s AND :e
        """
        val = pd.read_sql(sa.text(sql), self.qb, params={"s": start, "e": end}).iloc[0, 0]
        return Decimal(str(val))

    def _save_results(self, results: dict):
        import json
        with self.results.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO reconciliation_audit_log
                    (period, stripe_total, warehouse_total, qb_total, passed, detail, run_at)
                VALUES (:period, :stripe, :wh, :qb, :passed, :detail::jsonb, NOW())
            """), {
                "period":  results["period"],
                "stripe":  results["stripe"],
                "wh":      results["warehouse"],
                "qb":      results["quickbooks"],
                "passed":  results["passed"],
                "detail":  json.dumps(results["discrepancies"]),
            })
```

### Results

| Metric | Before | After |
|---|---|---|
| Monthly close duration | 3 days | 4 hours |
| Manual reconciliation effort | 20 hrs/month | 1 hr/month (exception handling only) |
| Time to detect discrepancies | Days | Minutes (automated post-pipeline) |
| Finance trust in dashboards | Low (waited for close) | High (daily validation) |

---

## Case Study 2: CDC Reconciliation for Real-Time Pipelines

### Problem

A CDC pipeline consumed from Kafka and wrote to Snowflake. Consumer group lag spiked occasionally, but nobody knew if messages were being lost or just delayed.

### Solution: Event Count Reconciliation with Source-of-Truth DB

```python
def reconcile_cdc_pipeline(
    source_engine,         # PostgreSQL source
    target_engine,         # Snowflake target
    kafka_admin,           # Kafka admin client
    check_window_minutes: int = 60,
    tolerance_pct: float = 0.001
) -> dict:
    """
    Reconcile CDC pipeline by comparing:
    1. Source DB committed changes
    2. Kafka messages produced
    3. Target DB rows written
    """
    from datetime import datetime, timedelta
    window_start = datetime.utcnow() - timedelta(minutes=check_window_minutes)
    window_end   = datetime.utcnow() - timedelta(minutes=5)  # Allow for CDC lag

    # Source: changes committed to PostgreSQL WAL in window
    src_count = pd.read_sql(sa.text("""
        SELECT COUNT(*) FROM pg_audit_log
        WHERE commit_time BETWEEN :s AND :e
    """), source_engine, params={"s": window_start, "e": window_end}).iloc[0, 0]

    # Target: rows received and written in Snowflake
    tgt_count = pd.read_sql(sa.text("""
        SELECT COUNT(*)
        FROM raw.orders_cdc
        WHERE ingested_at BETWEEN :s AND :e
    """), target_engine, params={"s": window_start, "e": window_end}).iloc[0, 0]

    # Consumer group lag (Kafka messages not yet consumed)
    # (Simplified — actual implementation uses Kafka AdminClient offset APIs)
    consumer_lag = get_consumer_group_lag(kafka_admin, "cdc-consumer-group")

    diff_pct = abs(src_count - tgt_count) / max(src_count, 1) * 100

    result = {
        "window_start":   str(window_start),
        "window_end":     str(window_end),
        "source_changes": src_count,
        "target_writes":  tgt_count,
        "consumer_lag":   consumer_lag,
        "diff_pct":       diff_pct,
        "passed":         diff_pct <= tolerance_pct * 100 and consumer_lag < 10_000,
    }

    if not result["passed"]:
        if consumer_lag > 100_000:
            print("CRITICAL: Consumer lag > 100K — CDC pipeline falling behind")
        if diff_pct > 1.0:
            print(f"WARNING: {diff_pct:.2f}% message loss detected in CDC pipeline")

    return result
```

---

## Case Study 3: Reconciliation Runbook

### Standard Reconciliation Failure Investigation

```
1. DETECT: Automated check fails (row count mismatch)
   Alert: #data-quality-alerts, PagerDuty for critical

2. SCOPE: Run scoping query to understand the extent
   SQL: SELECT DATE(created_at), COUNT(*) FROM source GROUP BY 1
        vs SELECT order_date, COUNT(*) FROM target GROUP BY 1
   Goal: Which dates, which tables, how many rows?

3. INVESTIGATE: Identify root cause
   a. Check ETL pipeline logs for errors during the affected time window
   b. Check source DB for transactions that might have been rolled back
   c. Check for truncation events (pipeline interrupted mid-run)
   d. Verify network/connection issues between source and target

4. REMEDIATE: Based on root cause
   - If partial load: re-run the pipeline for affected dates (must be idempotent!)
   - If data loss in transit: restore from source or backup
   - If transformation bug: fix code + backfill affected dates

5. VERIFY: Post-remediation reconciliation
   Re-run all reconciliation checks; confirm all pass

6. DOCUMENT: Post-mortem
   - What failed, when, why
   - How long until detection
   - Remediation steps taken
   - How to prevent recurrence
```

---

## Interview Tips

> **Tip 1:** The three-way reconciliation (payment processor + warehouse + accounting system) is a real pattern in fintech. It shows business awareness — financial data must match across all systems, not just the pipeline source and target.

> **Tip 2:** CDC reconciliation adds a dimension that batch reconciliation doesn't have: consumer lag as a reconciliation metric. High lag means messages are queued but not yet processed — not lost, but delayed. Know the difference.

> **Tip 3:** The goal of automated reconciliation is to reduce mean time to detection (MTTD). From "finance notices the revenue is wrong on Friday" to "the pipeline alert fires within 5 minutes of the ETL completing" is a concrete improvement to quantify.

> **Tip 4:** Reconciliation SLAs are different from pipeline SLAs. A pipeline might complete at 6 AM; the reconciliation check runs at 6:15 AM; the data is certified as ready at 6:20 AM. This 20-minute certification window should be in the stakeholder agreement.

> **Tip 5:** Post-mortem documentation of reconciliation failures is a professional maturity marker. "We had a $47K revenue gap in March, diagnosed a network timeout that caused a partial load, fixed it in 2 hours, and added a dedicated row count check to catch it faster next time" is the level of specificity that impresses senior interviewers.
