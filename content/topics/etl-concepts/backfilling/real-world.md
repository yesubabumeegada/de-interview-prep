---
title: "Backfilling - Real World"
topic: etl-concepts
subtopic: backfilling
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, backfilling, production, case-study, airflow, dbt]
---

# Backfilling — Real World

## Case Study 1: Revenue Correction Backfill

### Problem

A bug in a currency conversion function caused all EUR orders to be stored in USD at the wrong exchange rate for 45 days. The error was discovered after the monthly close. The finance team needed the corrected data for a re-statement.

### Solution Approach

```python
# Step 1: Identify the exact affected date range
AFFECTED_START = date(2024, 2, 1)
AFFECTED_END   = date(2024, 3, 17)   # Bug introduced and fixed dates

# Step 2: Run a verification query to quantify the error
VERIFY_SQL = """
    SELECT
        COUNT(*) AS affected_orders,
        SUM(total_usd) AS stored_revenue,
        SUM(total_eur * correct_eur_rate) AS corrected_revenue,
        SUM(total_usd - total_eur * correct_eur_rate) AS overstatement
    FROM orders
    WHERE currency = 'EUR'
      AND order_date BETWEEN :start AND :end
"""

# Step 3: Create corrected_orders staging table
CORRECTION_SQL = """
    CREATE TABLE orders_corrected_20240317 AS
    SELECT
        order_id,
        customer_id,
        total_eur,
        total_eur * eur_rates.rate AS total_usd_corrected,  -- Correct rate from rates table
        currency,
        order_date
    FROM orders
    JOIN eur_exchange_rates ON DATE(orders.created_at) = eur_exchange_rates.rate_date
    WHERE currency = 'EUR'
      AND order_date BETWEEN '2024-02-01' AND '2024-03-17'
"""

# Step 4: Apply corrections transactionally
def apply_revenue_correction(engine):
    with engine.begin() as conn:
        # Update orders with corrected amounts
        result = conn.execute(sa.text("""
            UPDATE orders o
            SET total_usd = c.total_usd_corrected,
                corrected_at = NOW(),
                correction_note = 'EUR rate bug fix 2024-03-17'
            FROM orders_corrected_20240317 c
            WHERE o.order_id = c.order_id
        """))
        print(f"Corrected {result.rowcount} orders")

        # Log correction event for audit
        conn.execute(sa.text("""
            INSERT INTO data_corrections_audit
                (correction_date, pipeline, records_affected, reason, corrected_by)
            VALUES (NOW(), 'orders_eur_rate', :count, 'EUR exchange rate bug', 'data_team')
        """), {"count": result.rowcount})
```

### Downstream Impact: dbt Rerun

```bash
# After correcting the orders table, recompute all downstream models
dbt run \
  --select +revenue_daily+monthly_close+executive_summary \
  --vars '{"backfill_start": "2024-02-01", "backfill_end": "2024-03-17"}'

# Verify the correction in the final mart
dbt test --select revenue_daily
```

---

## Case Study 2: New Analytics Column Backfill at Scale

### Problem

The data platform team added a `customer_segment` column to the orders model (computed from RFM analysis). The column needed to be backfilled for 3 years of history — approximately 800 million orders — without disrupting the daily pipeline.

### Solution: Parallel Partition Backfill

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

def backfill_customer_segment(
    run_date: date,
    source_engine,
    target_engine
) -> int:
    """Backfill customer_segment for a single date partition."""
    # Compute segment using RFM logic
    df = pd.read_sql(sa.text("""
        SELECT
            o.order_id,
            o.customer_id,
            rfm.recency_score,
            rfm.frequency_score,
            rfm.monetary_score,
            CASE
                WHEN rfm.recency_score >= 4 AND rfm.frequency_score >= 4 THEN 'champion'
                WHEN rfm.recency_score >= 3 AND rfm.frequency_score >= 3 THEN 'loyal'
                WHEN rfm.recency_score >= 4 THEN 'recent'
                WHEN rfm.monetary_score >= 4 THEN 'big_spender'
                ELSE 'at_risk'
            END AS customer_segment
        FROM orders o
        JOIN customer_rfm_scores rfm
            ON o.customer_id = rfm.customer_id
           AND DATE(o.created_at) = :d
        WHERE DATE(o.created_at) = :d
    """), source_engine, params={"d": run_date})

    if df.empty:
        return 0

    # Update target table partition
    with target_engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(sa.text("""
                UPDATE orders
                SET customer_segment = :seg
                WHERE order_id = :oid
                  AND customer_segment IS NULL
            """), {"seg": row["customer_segment"], "oid": row["order_id"]})

    return len(df)

def parallel_backfill(
    start_date: date,
    end_date: date,
    max_workers: int = 8,     # Parallel date threads
    sleep_between: float = 0.5
) -> dict:
    """
    Backfill 3 years of history in parallel.
    max_workers=8 = 8 dates running simultaneously.
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    results = {"success": 0, "failed": 0, "total_rows": 0}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(backfill_customer_segment, d, src_engine, tgt_engine): d
            for d in dates
        }

        for future in as_completed(futures):
            run_date = futures[future]
            try:
                rows = future.result()
                results["success"] += 1
                results["total_rows"] += rows
                print(f"[OK] {run_date}: {rows} rows")
            except Exception as e:
                results["failed"] += 1
                print(f"[FAIL] {run_date}: {e}")

    return results

# Estimated runtime:
# 3 years * 365 days = 1,095 dates
# With 8 workers: ~137 parallel batches
# At ~1 minute/date: ~137 minutes (vs 1,095 minutes sequential)
```

---

## Case Study 3: Airflow Backfill Best Practices

### Production Backfill Runbook

```markdown
## Backfill Runbook: Standard Operating Procedure

### Before Backfill
1. [ ] Verify pipeline is idempotent: `airflow dags backfill --dry-run <dag_id>`
2. [ ] Estimate row volume: `SELECT COUNT(*) FROM source WHERE date BETWEEN :start AND :end`
3. [ ] Check source system capacity: Confirm with DBA that backfill won't overload prod DB
4. [ ] Notify downstream consumers: Slack #data-stakeholders "Backfill for <pipeline> running"
5. [ ] Schedule off-peak: Prefer weekend or 2-6 AM for large backfills
6. [ ] Set max_active_runs to limit parallelism

### During Backfill
7. [ ] Monitor source DB load every 15 minutes
8. [ ] Monitor Airflow task duration (should be consistent with normal runs)
9. [ ] Check intermediate results every hour

### After Backfill
10. [ ] Run reconciliation: source row count == target row count per day
11. [ ] Verify business metrics: revenue total matches finance team's offline calculation
12. [ ] Notify stakeholders: "Backfill complete, data available from <start> to <end>"
13. [ ] Update runbook with any issues encountered
```

### Airflow Backfill with Safeguards

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def check_backfill_preconditions(ds: str, **context):
    """
    Pre-flight check before running any backfill date.
    Fails if source data isn't available yet.
    """
    # Check if source data exists for this date
    count = pd.read_sql(
        sa.text("SELECT COUNT(*) FROM source.orders WHERE order_date = :d"),
        source_engine, params={"d": ds}
    ).iloc[0, 0]

    if count == 0:
        raise ValueError(f"No source data available for {ds}. Skipping.")

    return True

def backfill_with_prechecks(ds: str, **context):
    """Main backfill task."""
    check_backfill_preconditions(ds, **context)
    # ... backfill logic ...

with DAG(
    "orders_backfill",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=4,       # Max 4 dates in parallel
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    backfill = PythonOperator(
        task_id="backfill_orders",
        python_callable=backfill_with_prechecks,
        provide_context=True,
    )

    reconcile = PythonOperator(
        task_id="reconcile_row_counts",
        python_callable=reconcile_counts_for_date,
        provide_context=True,
    )

    backfill >> reconcile
```

---

## Interview Tips

> **Tip 1:** When describing a real backfill scenario, include the business context (EUR rate bug, revenue correction) and quantify the impact (how many records, how much revenue affected). Numbers make the story concrete.

> **Tip 2:** Parallel backfill (multiple dates simultaneously) can reduce runtime by 5-10x but requires careful management of source DB connections. Always set a max_workers limit and monitor DB load.

> **Tip 3:** The post-backfill reconciliation step is mandatory in production. Row counts, aggregate metrics, and business KPIs should all be validated before notifying stakeholders that the backfill is complete.

> **Tip 4:** For Airflow backfills, `schedule_interval=None` with manual trigger is safer than `catchup=True` for historical backfills — it prevents accidental catchup if the start_date is changed.

> **Tip 5:** Stakeholder communication is part of backfill management. Dashboards showing data "in progress" during a large backfill can confuse business users. Notify them before starting and after completion.
