---
title: "Notebooks & Collaboration - Real-World Examples"
topic: databricks
subtopic: notebooks-collaboration
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, notebooks, production, collaboration, git, team-practices]
---

# Notebooks & Collaboration — Real-World Production Examples

## Production Pattern: Parameterized Report Notebook

A finance team runs the same revenue analysis notebook for 12 regions and 4 business lines (48 combinations). One notebook handles all:

```python
# --- Notebook: revenue_analysis.py ---

# 1. Parameters (set by Workflow or manually via UI)
dbutils.widgets.dropdown("region", "us-east",
    ["us-east", "us-west", "eu-central", "apac", "all"], "Region")
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
dbutils.widgets.text("end_date", "2024-01-31", "End Date")
dbutils.widgets.dropdown("business_line", "retail",
    ["retail", "wholesale", "digital", "all"], "Business Line")

region = dbutils.widgets.get("region")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
business_line = dbutils.widgets.get("business_line")

print(f"Running: region={region}, dates={start_date} to {end_date}, line={business_line}")

# 2. Dynamic query based on parameters
filters = [f"order_date BETWEEN '{start_date}' AND '{end_date}'"]
if region != "all":
    filters.append(f"region = '{region}'")
if business_line != "all":
    filters.append(f"business_line = '{business_line}'")

where_clause = " AND ".join(filters)

df = spark.sql(f"""
    SELECT
        DATE_TRUNC('week', order_date) AS week,
        product_category,
        SUM(amount) AS revenue,
        COUNT(*) AS order_count,
        AVG(amount) AS avg_order_value
    FROM prod.sales.orders
    WHERE {where_clause}
    GROUP BY 1, 2
    ORDER BY 1, revenue DESC
""")

display(df)

# 3. Write to a parameterized output table
output_table = f"prod.reporting.revenue_{region}_{business_line}"
df.write.mode("overwrite").saveAsTable(output_table)
print(f"Output written to: {output_table}")

# 4. Exit with summary for workflow tracking
dbutils.notebook.exit(f"success: {df.count()} rows, table={output_table}")
```

**Workflow configuration:** 48 parallel tasks, each calling this notebook with different parameters — runs in 8 minutes total instead of 6+ hours sequentially.

---

## Production Pattern: Collaborative Data Investigation

Three engineers investigated a missing revenue issue simultaneously in the same notebook:

```python
# --- Investigation Notebook: 2024-02-incident-missing-revenue.py ---
# %md
# # Revenue Gap Investigation — 2024-02-15
# **Reported by:** Finance (Bob)
# **Investigating:** Alice, Charlie, David
# **Timeline:** Gap discovered at 9am, resolved 11:30am
# **Root cause:** [FILLED IN AS FOUND]

# %md ## Step 1: Quantify the gap (Alice)
actual_revenue = spark.sql("""
    SELECT SUM(amount) AS actual
    FROM prod.sales.orders
    WHERE order_date = '2024-02-15'
""").collect()[0]["actual"]

expected_revenue = 2_850_000   # from finance team's projection

print(f"Actual: ${actual_revenue:,.0f}")
print(f"Expected: ${expected_revenue:,.0f}")
print(f"Gap: ${expected_revenue - actual_revenue:,.0f} ({(1 - actual_revenue/expected_revenue)*100:.1f}%)")

# %md ## Step 2: Find where the gap is (Charlie)
# Alice was looking at totals. Charlie investigated by region while Alice looked at time series.

by_region = spark.sql("""
    SELECT region, SUM(amount) AS revenue,
           LAG(SUM(amount)) OVER (PARTITION BY region ORDER BY order_date) AS prev_day_revenue,
           SUM(amount) - LAG(SUM(amount)) OVER (PARTITION BY region ORDER BY order_date) AS delta
    FROM prod.sales.orders
    WHERE order_date BETWEEN '2024-02-14' AND '2024-02-15'
    GROUP BY region, order_date
    ORDER BY delta ASC
""")
display(by_region)
# Charlie found: EU region was $1.2M lower than yesterday

# %md ## Step 3: Trace EU drop to source (David)
# Found the root cause:
eu_ingestion_log = spark.sql("""
    SELECT pipeline_name, status, rows_processed, error_message
    FROM prod.ops.pipeline_run_log
    WHERE run_date = '2024-02-15'
      AND pipeline_name LIKE '%eu%'
    ORDER BY started_at DESC
""")
display(eu_ingestion_log)
# EU payment processor pipeline failed at 2am — 4 hours of EU transactions missing

# %md ## Resolution
# Backfill EU transactions from raw event logs using Time Travel
spark.sql("""
    INSERT INTO prod.sales.orders
    SELECT * FROM prod.raw.eu_payment_events
    WHERE event_date = '2024-02-15'
      AND event_hour BETWEEN 2 AND 6
      AND order_id NOT IN (SELECT order_id FROM prod.sales.orders WHERE order_date = '2024-02-15')
""")
```

**Outcome:** All three engineers edited simultaneously. The investigation notebook served as a permanent record of the incident — what was found, by whom, and how it was resolved.

---

## Production Pattern: Notebook Test Suite

A team enforces notebook quality via a test runner notebook that CI triggers before merging:

```python
# --- Notebook: run_integration_tests.py ---

import time
test_results = []

def run_test(name: str, notebook_path: str, params: dict):
    start = time.time()
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=600, arguments=params)
        duration = time.time() - start
        test_results.append({"test": name, "status": "PASS", "duration_s": round(duration, 1), "result": result})
        print(f"✅ PASS: {name} ({duration:.1f}s)")
    except Exception as e:
        duration = time.time() - start
        test_results.append({"test": name, "status": "FAIL", "duration_s": round(duration, 1), "error": str(e)[:200]})
        print(f"❌ FAIL: {name} ({duration:.1f}s)")
        print(f"   Error: {str(e)[:200]}")

# Run all test notebooks
run_test("ingestion_daily", "/Repos/org/de-pipelines/tests/test_ingestion",
         {"date": "2024-01-15", "env": "staging"})
run_test("transformation_revenue", "/Repos/org/de-pipelines/tests/test_transformation",
         {"date": "2024-01-15", "env": "staging"})
run_test("data_quality_orders", "/Repos/org/de-pipelines/tests/test_dq_orders",
         {"table": "staging.sales.orders", "date": "2024-01-15"})

# Summary
passed = sum(1 for r in test_results if r["status"] == "PASS")
failed = len(test_results) - passed

print(f"\n{'='*50}")
print(f"Results: {passed}/{len(test_results)} passed")

if failed > 0:
    print("\nFailed tests:")
    for r in test_results:
        if r["status"] == "FAIL":
            print(f"  - {r['test']}: {r.get('error', 'no details')}")
    dbutils.notebook.exit(f"FAIL: {failed} tests failed")
else:
    dbutils.notebook.exit(f"PASS: all {passed} tests passed")
```

---

## Anti-Pattern Incident: Shared Mutable State

A team had 3 data scientists running the same exploratory notebook concurrently — on the same shared cluster:

```python
# Problem: notebook A stored a DataFrame in a global variable
# When notebook B ran, it mutated the cluster's shared Spark context
# causing notebook A's subsequent cells to see wrong results

# The incident:
# DS1 ran `filtered_df = big_df.filter("region = 'us-east'").cache()`
# DS2 ran `spark.catalog.clearCache()` (they were debugging)
# DS1's next cell reading from cache got an error — cache was gone

# Prevention: always use isolated clusters for production notebooks
# Or: use serverless compute (each notebook gets isolated compute)

# Also: for collaborative exploration, use separate clusters
# Cluster per user for dev: Databricks personal compute
# Shared cluster only for scheduled production pipelines
```
