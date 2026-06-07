---
title: "Data Reconciliation - Intermediate"
topic: etl-concepts
subtopic: data-reconciliation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, data-reconciliation, automated, reconciliation-pipeline, alerting]
---

# Data Reconciliation — Intermediate

## Automated Reconciliation Framework

A production reconciliation framework runs checks automatically after each pipeline, stores results, and alerts on failures.

```python
from dataclasses import dataclass, field
from typing import Callable, Optional
from datetime import datetime
import sqlalchemy as sa
import pandas as pd

@dataclass
class ReconciliationCheck:
    name:       str
    check_fn:   Callable[[], dict]  # Returns {"passed": bool, "detail": any}
    severity:   str = "error"       # "error" or "warn"
    description: str = ""

@dataclass
class ReconciliationResult:
    check_name:  str
    passed:      bool
    severity:    str
    detail:      dict
    run_at:      datetime = field(default_factory=datetime.utcnow)

class ReconciliationRunner:
    def __init__(self, engine, alert_fn=None):
        self.engine    = engine
        self.alert_fn  = alert_fn
        self._init_results_table()

    def _init_results_table(self):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS reconciliation_results (
                    id           BIGSERIAL PRIMARY KEY,
                    pipeline     TEXT NOT NULL,
                    check_name   TEXT NOT NULL,
                    run_date     DATE NOT NULL,
                    passed       BOOLEAN NOT NULL,
                    severity     TEXT NOT NULL,
                    detail       JSONB,
                    run_at       TIMESTAMPTZ DEFAULT NOW()
                )
            """))

    def run_all(self, pipeline: str, run_date: str, checks: list[ReconciliationCheck]) -> dict:
        results     = []
        all_passed  = True
        errors      = []

        for check in checks:
            try:
                outcome = check.check_fn()
                passed  = outcome.get("passed", False)
                detail  = outcome
            except Exception as e:
                passed = False
                detail = {"error": str(e), "exception": True}

            result = ReconciliationResult(
                check_name=check.name,
                passed=passed,
                severity=check.severity,
                detail=detail
            )
            results.append(result)

            # Persist result
            import json
            with self.engine.begin() as conn:
                conn.execute(sa.text("""
                    INSERT INTO reconciliation_results
                        (pipeline, check_name, run_date, passed, severity, detail)
                    VALUES (:p, :c, :d, :passed, :sev, :det::jsonb)
                """), {
                    "p": pipeline, "c": check.name, "d": run_date,
                    "passed": passed, "sev": check.severity,
                    "det": json.dumps(detail)
                })

            if not passed:
                if check.severity == "error":
                    all_passed = False
                    errors.append(f"{check.name}: {detail}")
                print(f"[FAIL-{check.severity.upper()}] {check.name}: {detail}")
            else:
                print(f"[PASS] {check.name}")

        # Alert on failures
        if not all_passed and self.alert_fn:
            self.alert_fn(pipeline=pipeline, run_date=run_date, errors=errors)

        return {
            "all_passed": all_passed,
            "results":    [(r.check_name, r.passed) for r in results],
            "errors":     errors,
        }
```

---

## Multi-System Reconciliation

When data flows through multiple systems, reconcile at each hop:

```python
def reconcile_pipeline_hops(
    pipeline_name: str,
    run_date: str,
    systems: list[dict]  # [{name, engine, table, date_col}]
) -> dict:
    """
    Reconcile row counts across multiple pipeline hops.
    Example: MySQL → Staging S3 → Snowflake Raw → Snowflake Gold
    """
    counts = {}
    for system in systems:
        try:
            count = pd.read_sql(
                sa.text(f"SELECT COUNT(*) FROM {system['table']} WHERE DATE({system['date_col']}) = :d"),
                system["engine"],
                params={"d": run_date}
            ).iloc[0, 0]
            counts[system["name"]] = count
        except Exception as e:
            counts[system["name"]] = f"ERROR: {e}"

    # Check for drops at each hop
    hop_results = []
    count_values = [(name, cnt) for name, cnt in counts.items() if isinstance(cnt, int)]

    for i in range(1, len(count_values)):
        prev_name, prev_count = count_values[i - 1]
        curr_name, curr_count = count_values[i]
        diff_pct = abs(prev_count - curr_count) / max(prev_count, 1) * 100
        hop_results.append({
            "from":       prev_name,
            "to":         curr_name,
            "from_count": prev_count,
            "to_count":   curr_count,
            "diff_pct":   diff_pct,
            "passed":     diff_pct <= 0.01,
        })

    return {"counts": counts, "hop_checks": hop_results}
```

---

## Reconciliation Failure Handling

### Root Cause Investigation Queries

```sql
-- Find missing records: in source but not in target
SELECT src.order_id, src.created_at, src.total_usd
FROM source.orders src
LEFT JOIN target.orders tgt ON src.order_id = tgt.order_id
WHERE tgt.order_id IS NULL
  AND DATE(src.created_at) = '2024-01-15'
ORDER BY src.order_id
LIMIT 100;  -- Sample of missing records

-- Find extra records: in target but not in source
SELECT tgt.order_id
FROM target.orders tgt
LEFT JOIN source.orders src ON tgt.order_id = src.order_id
WHERE src.order_id IS NULL
  AND tgt.order_date = '2024-01-15'
LIMIT 100;

-- Find value mismatches: same key, different total
SELECT
    src.order_id,
    src.total_usd AS source_total,
    tgt.total_usd AS target_total,
    ABS(src.total_usd - tgt.total_usd) AS difference
FROM source.orders src
JOIN target.orders tgt ON src.order_id = tgt.order_id
WHERE ABS(src.total_usd - tgt.total_usd) > 0.01
  AND DATE(src.created_at) = '2024-01-15'
ORDER BY difference DESC
LIMIT 50;
```

### Automated Remediation for Common Failures

```python
class ReconciliationRemediator:
    def __init__(self, pipeline_name: str, engine_source, engine_target):
        self.pipeline = pipeline_name
        self.src      = engine_source
        self.tgt      = engine_target

    def find_and_fill_gaps(self, run_date: str, table: str, key: str) -> int:
        """
        Find records in source that are missing from target and fill the gap.
        Only safe if the insert is idempotent (ON CONFLICT DO NOTHING).
        """
        missing_sql = f"""
            SELECT src.*
            FROM {table} src
            LEFT JOIN target.{table} tgt ON src.{key} = tgt.{key}
            WHERE tgt.{key} IS NULL
              AND DATE(src.created_at) = :d
        """
        missing_df = pd.read_sql(sa.text(missing_sql), self.src, params={"d": run_date})

        if missing_df.empty:
            print(f"No missing records found for {run_date}")
            return 0

        print(f"Found {len(missing_df)} missing records. Filling gap...")
        missing_df.to_sql(table, self.tgt, if_exists="append", index=False, method="multi")
        return len(missing_df)
```

---

## Reconciliation Metrics and Trending

```python
def get_reconciliation_trend(engine, pipeline: str, days: int = 30) -> pd.DataFrame:
    """
    Get reconciliation pass/fail trend for a pipeline.
    Used to detect degrading data quality over time.
    """
    sql = """
        SELECT
            run_date,
            COUNT(*) AS total_checks,
            SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
            ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate_pct
        FROM reconciliation_results
        WHERE pipeline = :p
          AND run_date >= CURRENT_DATE - :days
        GROUP BY run_date
        ORDER BY run_date
    """
    return pd.read_sql(sa.text(sql), engine, params={"p": pipeline, "days": days})

def alert_on_degrading_quality(engine, pipeline: str):
    """Alert if pass rate has been declining for 3+ consecutive days."""
    trend = get_reconciliation_trend(engine, pipeline, days=7)
    if len(trend) >= 3:
        recent = trend.tail(3)
        if all(recent["pass_rate_pct"].diff().dropna() < 0):
            print(f"ALERT: {pipeline} reconciliation pass rate declining for 3+ days")
```

---

## Interview Tips

> **Tip 1:** A reconciliation framework that persists results and trends is far more valuable than ad-hoc checks. "We saw the row count mismatch rate creep from 0% to 0.5% over a week" is actionable intelligence that point-in-time checks can't provide.

> **Tip 2:** Multi-hop reconciliation (MySQL → S3 → Snowflake Raw → Gold) pinpoints exactly where data loss occurs in the pipeline. "We lost 500 rows between S3 and Snowflake" is more actionable than "we're missing 500 rows."

> **Tip 3:** The source-vs-target anti-join query (`LEFT JOIN ... WHERE target.id IS NULL`) is the standard technique for finding missing records. It's expensive but unambiguous — mention it when interviewers ask how to investigate reconciliation failures.

> **Tip 4:** Automated gap-filling (find missing records + re-insert them) should only run for idempotent writes. Without that guarantee, gap-filling causes duplicates.

> **Tip 5:** Reconciliation results should be persisted to a dedicated table, not just logged. This enables trend analysis, SLA reporting, and regulatory audit trails.
