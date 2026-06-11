---
title: "Pipeline Testing — Scenarios"
topic: ci-cd
subtopic: pipeline-testing
content_type: scenario_question
tags: [ci-cd, testing, interview, scenarios, data-quality]
---

# Pipeline Testing — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Your First DAG Has a Bug

**Scenario:** You wrote an Airflow DAG that extracts orders, transforms revenue, and loads to a warehouse. QA finds the revenue numbers are 20% too high. How do you write tests to prevent this kind of bug in the future?

<details>
<summary>💡 Hint</summary>

Start with a unit test for the revenue calculation function — create a small DataFrame with known values and assert the expected output exactly. Then add a test that checks the filter logic: are you including only `completed` orders, or accidentally including `pending` ones? Finally add a test for the DAG structure itself — does it have the right number of tasks and the right dependencies?

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Identify the bug with a unit test**
```python
# The bug: status filter was wrong
# Old: df[df["status"] != "cancelled"]   ← includes pending
# New: df[df["status"] == "completed"]   ← correct

def test_revenue_excludes_pending_orders():
    df = pd.DataFrame([
        {"order_id": 1, "amount": 100.0, "status": "completed"},
        {"order_id": 2, "amount": 200.0, "status": "pending"},   # should be excluded
        {"order_id": 3, "amount": 50.0,  "status": "cancelled"},  # should be excluded
    ])
    result = calculate_revenue(df)
    assert result["total_revenue"] == 100.0  # only order 1
    assert result["order_count"] == 1
```

**Step 2: Test DAG structure**
```python
from airflow.models import DagBag

def test_revenue_dag_loads():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "revenue_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_revenue_dag_task_order():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["revenue_pipeline"]
    extract = dag.get_task("extract_orders")
    transform = dag.get_task("calculate_revenue")
    assert transform in extract.downstream_list
```

**Step 3: Run in CI**
```yaml
- name: Test pipeline
  run: pytest tests/ -v --tb=short
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Integration Testing Without Production Data

**Scenario:** You need to integration test a pipeline that reads from Snowflake, transforms data with PySpark, and loads to S3. You can't use production credentials in CI. How do you design the test environment?

<details>
<summary>💡 Hint</summary>

Use a layered strategy: mock the external I/O (Snowflake reader returns a fixed DataFrame, S3 writer is mocked), but test the transformation logic against a real Spark session. For the Spark layer, use `pyspark.sql.SparkSession.builder.master("local[2]")` — it runs locally without a cluster. For the Snowflake and S3 boundaries, use dependency injection so tests can swap real clients for mocks.

</details>

<details>
<summary>✅ Solution</summary>

**Dependency-injectable pipeline**
```python
class RevenuePipeline:
    def __init__(self, reader, writer, spark):
        self.reader = reader   # injected — real or mock
        self.writer = writer   # injected — real or mock
        self.spark = spark

    def run(self, date: str):
        raw_df = self.reader.read_orders(date)
        spark_df = self.spark.createDataFrame(raw_df)
        result = self._transform(spark_df)
        self.writer.write_revenue(result, date)

# conftest.py
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

@pytest.fixture
def mock_reader():
    reader = MagicMock()
    reader.read_orders.return_value = pd.DataFrame([
        {"order_id": 1, "amount": 100.0, "status": "completed", "date": "2024-01-01"},
        {"order_id": 2, "amount": 200.0, "status": "completed", "date": "2024-01-01"},
    ])
    return reader

# Integration test
def test_pipeline_produces_correct_revenue(spark, mock_reader):
    mock_writer = MagicMock()
    pipeline = RevenuePipeline(mock_reader, mock_writer, spark)
    pipeline.run("2024-01-01")
    
    written_df = mock_writer.write_revenue.call_args[0][0]
    assert written_df.filter("date = '2024-01-01'").agg({"revenue": "sum"}).collect()[0][0] == 300.0
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Testing Strategy for a 50-Pipeline Platform

**Scenario:** Your team manages 50 data pipelines across Airflow, dbt, and Spark. Today there are essentially no tests. The CTO wants a testing strategy that can be implemented over 3 months. Design it.

<details>
<summary>💡 Hint</summary>

Don't try to get to 100% test coverage immediately — prioritize by risk. Rank pipelines by business impact (revenue reporting first) and change frequency (highest-churn code first). Define a three-tier strategy: unit tests for all transformation functions (highest ROI, fastest to write), dbt tests for all models (already built in), and data reconciliation tests for high-value outputs (compare pipeline output to source count/sum). Add a CI gate from day 1 — even running `dbt compile` in CI catches most syntax errors immediately.

</details>

<details>
<summary>✅ Solution</summary>

**Month 1: Foundation (zero to something)**
```
1. Add CI (GitHub Actions) that runs on every PR — even if it just runs dbt compile
2. Add dbt schema tests to all gold models (not_null, unique, accepted_values)
3. Prioritize top 5 pipelines by business impact → add unit tests
4. Add DAG import tests for all 50 Airflow DAGs (catches syntax errors in <10 min)
```

**Month 2: Integration and contracts**
```python
# Contract test for every pipeline interface
CONTRACTS = {
    "gold.orders_daily": GoldOrdersSchema,
    "gold.revenue_summary": RevenueSchema,
    # ... defined for all 50 pipelines
}

# Run in CI: validate each pipeline's output schema
@pytest.mark.parametrize("table,schema", CONTRACTS.items())
def test_pipeline_output_contract(table, schema, test_engine):
    sample_df = read_sample(test_engine, table, rows=100)
    schema.validate(sample_df)
```

**Month 3: Reconciliation and observability**
```python
# Daily reconciliation job (not just in CI — runs in prod too)
def reconcile_revenue_pipeline():
    pipeline_total = warehouse.query("SELECT SUM(revenue) FROM gold.revenue_daily WHERE date = CURRENT_DATE - 1")
    source_total = source_db.query("SELECT SUM(amount) FROM orders WHERE status='completed' AND DATE(created_at) = CURRENT_DATE - 1")
    
    gap_pct = abs(pipeline_total - source_total) / source_total
    if gap_pct > 0.001:  # >0.1% gap
        alert_pagerduty(f"Revenue reconciliation failed: {gap_pct:.2%} gap")
```

**Test coverage targets by month:**
```
Month 1: dbt tests on 100% of gold models, unit tests on top 5 pipelines
Month 2: Unit tests on top 15 pipelines, contract tests on all interfaces
Month 3: Reconciliation tests on all revenue-critical pipelines, 80% overall
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the test pyramid and how does it apply to data pipelines?**
A: The test pyramid says to have many cheap unit tests, fewer integration tests, and fewest expensive end-to-end tests. For pipelines: unit test transformation functions, integration test component interactions with a test DB, and run full pipeline E2E tests only on main branch or nightly.

**Q: What is a contract test in the context of data pipelines?**
A: A contract test verifies that data flowing between pipeline stages conforms to an agreed schema. The producer asserts it produces a valid schema; the consumer asserts it can process it. This catches breaking changes before they reach integration.

**Q: How do you test an Airflow DAG without running it?**
A: Load it with `DagBag` and assert: no import errors, correct task count, correct dependencies, correct schedule, and required connections exist. These structural tests run in seconds without an Airflow cluster.

**Q: What is the difference between mocking and using a test database for SQL tests?**
A: Mocking returns fake data without touching SQL. A test database runs real queries against real SQL. Use mocking for testing business logic in isolation; use a real test DB (SQLite, Postgres in Docker) to test SQL correctness — mocks can mask query bugs.

**Q: What is great_expectations and how is it used in CI?**
A: Great Expectations is a data quality framework that defines assertions ("expect column values to not be null") stored as JSON suites and run as checkpoints. In CI, run a checkpoint against a sample of data after the pipeline runs — if assertions fail, the CI step fails and the PR is blocked.

**Q: How do you measure test coverage for a data pipeline?**
A: Use `pytest --cov=pipelines` for Python code coverage. But code coverage alone is insufficient for pipelines — add data coverage (are edge cases like nulls, duplicates, out-of-range values tested?). Target >80% code coverage and explicit tests for known data quality edge cases.

**Q: What is mutation testing and why does it matter?**
A: Mutation testing introduces small code changes (mutations) and checks if your tests detect them. If a test still passes after the mutation, it wasn't actually testing that behavior. It reveals gaps in test assertions that coverage metrics miss.

---

## 💼 Interview Tips

- Always mention the test pyramid — it shows you think about test ROI, not just test quantity.
- Distinguish between testing the pipeline structure (DAG tests) and testing the data transformation logic (unit tests) — interviewers often conflate them.
- Bring up reconciliation tests proactively: comparing pipeline output to source counts/sums is the most business-valuable test and many candidates miss it entirely.
- For senior roles, frame testing as a program with prioritization — "test everything immediately" is not a strategy; "test revenue-critical paths first" is.
- Mention that dbt's built-in tests (not_null, unique, accepted_values) are the easiest way to immediately add pipeline testing — it shows practical knowledge of real-world DE tooling.
- Avoid claiming you'd achieve 100% coverage in month one — it signals inexperience with the effort required. Show you'd prioritize ruthlessly.
