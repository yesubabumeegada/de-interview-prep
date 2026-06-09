---
title: "Testing PySpark — Real-World Patterns"
topic: pyspark
subtopic: testing-pyspark
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [pyspark, testing, medallion-pipeline, MERGE-testing, window-functions, end-to-end]
---

# Testing PySpark — Real-World Patterns

These are test patterns you'll apply to actual production pipeline code — testing a medallion pipeline end-to-end, verifying MERGE operations, validating window function logic, and ensuring the full pipeline works from raw input to final output.

---

## Pattern 1: Testing a Medallion Pipeline (Bronze → Silver → Gold)

```python
# tests/test_medallion_pipeline.py
import pytest
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, count, sum as spark_sum
from chispa.dataframe_comparer import assert_df_equality


# ── The pipeline code under test ──────────────────────────────────────────

def bronze_ingest(raw_df, bronze_path: str):
    """Write raw data to Bronze (append-only)."""
    from pyspark.sql.functions import current_timestamp
    raw_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .write.format("delta").mode("append").save(bronze_path)


def silver_promote(bronze_path: str, silver_path: str, pk_col: str):
    """Clean and dedup Bronze → Silver (upsert)."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    from delta.tables import DeltaTable

    bronze = spark.read.format("delta").load(bronze_path)

    cleaned = bronze \
        .withColumn("name", trim(lower(col("name")))) \
        .withColumn("email", lower(trim(col("email")))) \
        .filter(col("email").contains("@"))

    window = Window.partitionBy(pk_col).orderBy(col("_ingested_at").desc())
    deduped = cleaned \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn", "_ingested_at")

    try:
        target = DeltaTable.forPath(spark, silver_path)
        target.alias("t").merge(deduped.alias("s"), f"t.{pk_col} = s.{pk_col}") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        deduped.write.format("delta").mode("overwrite").save(silver_path)


def gold_aggregate(silver_path: str, gold_path: str):
    """Aggregate Silver → Gold daily summary."""
    silver = spark.read.format("delta").load(silver_path)
    gold = silver.groupBy("signup_date").agg(
        count("*").alias("new_customers"),
        spark_sum("lifetime_value").alias("total_ltv")
    )
    gold.write.format("delta").mode("overwrite").save(gold_path)


# ── Tests ──────────────────────────────────────────────────────────────────

@pytest.fixture
def pipeline_paths(tmp_path):
    return {
        "bronze": str(tmp_path / "bronze"),
        "silver": str(tmp_path / "silver"),
        "gold":   str(tmp_path / "gold"),
    }


def test_bronze_ingest_preserves_all_rows(spark, pipeline_paths):
    """Bronze ingest should append all rows with ingestion metadata."""
    raw = spark.createDataFrame([
        (1, "Alice", "alice@email.com", "2024-01-15", 500.0),
        (2, "Bob",   "bob@email.com",   "2024-01-15", 200.0),
    ], ["customer_id", "name", "email", "signup_date", "lifetime_value"])

    bronze_ingest(raw, pipeline_paths["bronze"])

    bronze = spark.read.format("delta").load(pipeline_paths["bronze"])
    assert bronze.count() == 2
    assert "_ingested_at" in bronze.columns


def test_silver_cleans_names_and_emails(spark, pipeline_paths):
    """Silver promotion should clean whitespace and lowercase."""
    raw = spark.createDataFrame([
        (1, "  Alice  ", "ALICE@Email.com", "2024-01-15", 500.0),
        (2, "Bob",       "bob@email.com",   "2024-01-15", 200.0),
    ], ["customer_id", "name", "email", "signup_date", "lifetime_value"])

    bronze_ingest(raw, pipeline_paths["bronze"])
    silver_promote(pipeline_paths["bronze"], pipeline_paths["silver"], "customer_id")

    silver = spark.read.format("delta").load(pipeline_paths["silver"])
    rows = {r.customer_id: r for r in silver.collect()}
    assert rows[1].name == "alice"
    assert rows[1].email == "alice@email.com"


def test_silver_filters_invalid_emails(spark, pipeline_paths):
    """Silver should filter out rows with invalid (no @) email addresses."""
    raw = spark.createDataFrame([
        (1, "Alice", "alice@email.com", "2024-01-15", 500.0),
        (2, "Bad",   "not-an-email",    "2024-01-15", 100.0),  # No @
    ], ["customer_id", "name", "email", "signup_date", "lifetime_value"])

    bronze_ingest(raw, pipeline_paths["bronze"])
    silver_promote(pipeline_paths["bronze"], pipeline_paths["silver"], "customer_id")

    silver = spark.read.format("delta").load(pipeline_paths["silver"])
    assert silver.count() == 1  # Invalid email filtered out
    assert silver.filter(col("customer_id") == 2).count() == 0


def test_silver_deduplicates_on_rerun(spark, pipeline_paths):
    """If Bronze has duplicates, Silver should keep only the latest."""
    raw1 = spark.createDataFrame(
        [(1, "Alice", "alice@email.com", "2024-01-15", 500.0)],
        ["customer_id", "name", "email", "signup_date", "lifetime_value"]
    )
    raw2 = spark.createDataFrame(
        [(1, "Alice Updated", "alice@email.com", "2024-01-15", 600.0)],  # Updated name/LTV
        ["customer_id", "name", "email", "signup_date", "lifetime_value"]
    )

    bronze_ingest(raw1, pipeline_paths["bronze"])
    bronze_ingest(raw2, pipeline_paths["bronze"])  # Second ingest = new batch

    silver_promote(pipeline_paths["bronze"], pipeline_paths["silver"], "customer_id")

    silver = spark.read.format("delta").load(pipeline_paths["silver"])
    assert silver.count() == 1  # Only one customer_id=1 row
    assert silver.collect()[0].name == "alice updated"  # Latest version


def test_gold_aggregation_correctness(spark, pipeline_paths):
    """Gold aggregation should correctly sum LTV by signup_date."""
    raw = spark.createDataFrame([
        (1, "Alice", "alice@email.com", "2024-01-15", 500.0),
        (2, "Bob",   "bob@email.com",   "2024-01-15", 300.0),
        (3, "Carol", "carol@email.com", "2024-01-16", 700.0),
    ], ["customer_id", "name", "email", "signup_date", "lifetime_value"])

    bronze_ingest(raw, pipeline_paths["bronze"])
    silver_promote(pipeline_paths["bronze"], pipeline_paths["silver"], "customer_id")
    gold_aggregate(pipeline_paths["silver"], pipeline_paths["gold"])

    gold = spark.read.format("delta").load(pipeline_paths["gold"])
    rows = {r.signup_date: r for r in gold.collect()}

    assert rows["2024-01-15"].new_customers == 2
    assert abs(rows["2024-01-15"].total_ltv - 800.0) < 0.001
    assert rows["2024-01-16"].new_customers == 1
    assert abs(rows["2024-01-16"].total_ltv - 700.0) < 0.001
```

---

## Pattern 2: Testing MERGE Operations

```python
# tests/test_merge_operations.py
from delta.tables import DeltaTable
from pyspark.sql.functions import col


def test_merge_insert_only(spark, tmp_path):
    """MERGE with only new records should insert all of them."""
    delta_path = str(tmp_path / "test_table")
    existing = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    existing.write.format("delta").save(delta_path)

    new_records = spark.createDataFrame([(2, "Bob"), (3, "Carol")], ["id", "name"])

    target = DeltaTable.forPath(spark, delta_path)
    target.alias("t").merge(new_records.alias("s"), "t.id = s.id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 3


def test_merge_update_only(spark, tmp_path):
    """MERGE where all source records match should update all of them."""
    delta_path = str(tmp_path / "test_table")
    existing = spark.createDataFrame([
        (1, "Alice"), (2, "Bob")
    ], ["id", "name"])
    existing.write.format("delta").save(delta_path)

    updates = spark.createDataFrame([
        (1, "Alice Smith"), (2, "Robert Jones")
    ], ["id", "name"])

    target = DeltaTable.forPath(spark, delta_path)
    target.alias("t").merge(updates.alias("s"), "t.id = s.id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    result = spark.read.format("delta").load(delta_path)
    rows = {r.id: r.name for r in result.collect()}
    assert rows[1] == "Alice Smith"
    assert rows[2] == "Robert Jones"
    assert result.count() == 2  # No new records added


def test_merge_delete(spark, tmp_path):
    """MERGE with delete clause should remove matched records."""
    delta_path = str(tmp_path / "test_table")
    existing = spark.createDataFrame([
        (1, "Alice", "keep"),
        (2, "Bob",   "delete"),
        (3, "Carol", "keep"),
    ], ["id", "name", "action"])
    existing.write.format("delta").save(delta_path)

    to_delete = spark.createDataFrame([(2,)], ["id"])

    target = DeltaTable.forPath(spark, delta_path)
    target.alias("t").merge(to_delete.alias("s"), "t.id = s.id") \
        .whenMatchedDelete().execute()

    result = spark.read.format("delta").load(delta_path)
    assert result.count() == 2
    assert result.filter(col("id") == 2).count() == 0


def test_merge_idempotency(spark, tmp_path):
    """Running the same MERGE twice should produce identical results."""
    delta_path = str(tmp_path / "test_table")
    existing = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    existing.write.format("delta").save(delta_path)

    updates = spark.createDataFrame([(1, "Alice Smith"), (2, "Bob")], ["id", "name"])

    def run_merge():
        target = DeltaTable.forPath(spark, delta_path)
        target.alias("t").merge(updates.alias("s"), "t.id = s.id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    run_merge()
    result_after_first = spark.read.format("delta").load(delta_path).collect()

    run_merge()  # Second identical run
    result_after_second = spark.read.format("delta").load(delta_path).collect()

    assert sorted([(r.id, r.name) for r in result_after_first]) == \
           sorted([(r.id, r.name) for r in result_after_second])
```

---

## Pattern 3: Testing Window Function Logic

```python
# src/window_transforms.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, lag, lead, sum as spark_sum, avg
from pyspark.sql.window import Window


def add_running_total(df: DataFrame, partition_col: str, order_col: str,
                      value_col: str, output_col: str = "running_total") -> DataFrame:
    """Add a running total within each partition."""
    window = Window.partitionBy(partition_col).orderBy(order_col) \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(output_col, spark_sum(value_col).over(window))


def add_previous_value(df: DataFrame, partition_col: str, order_col: str,
                       value_col: str, output_col: str = "prev_value") -> DataFrame:
    """Add the previous row's value within each partition (lag by 1)."""
    window = Window.partitionBy(partition_col).orderBy(order_col)
    return df.withColumn(output_col, lag(value_col, 1).over(window))


# tests/test_window_transforms.py
def test_running_total(spark):
    input_df = spark.createDataFrame([
        ("Alice", "2024-01-01", 100.0),
        ("Alice", "2024-01-02", 200.0),
        ("Alice", "2024-01-03",  50.0),
        ("Bob",   "2024-01-01", 300.0),
        ("Bob",   "2024-01-02", 150.0),
    ], ["user", "date", "amount"])

    result = add_running_total(input_df, "user", "date", "amount")
    rows = {(r.user, r.date): r.running_total for r in result.collect()}

    # Alice's running totals
    assert abs(rows[("Alice", "2024-01-01")] - 100.0) < 0.001
    assert abs(rows[("Alice", "2024-01-02")] - 300.0) < 0.001
    assert abs(rows[("Alice", "2024-01-03")] - 350.0) < 0.001

    # Bob's running totals are independent (separate partition)
    assert abs(rows[("Bob", "2024-01-01")] - 300.0) < 0.001
    assert abs(rows[("Bob", "2024-01-02")] - 450.0) < 0.001


def test_previous_value_first_row_is_null(spark):
    """First row in each partition should have null previous value."""
    input_df = spark.createDataFrame([
        ("Alice", "2024-01-01", 100.0),
        ("Alice", "2024-01-02", 200.0),
    ], ["user", "date", "amount"])

    result = add_previous_value(input_df, "user", "date", "amount")
    rows = {r.date: r.prev_value for r in result.filter(col("user") == "Alice").collect()}

    assert rows["2024-01-01"] is None   # First row → no previous
    assert abs(rows["2024-01-02"] - 100.0) < 0.001
```

---

## Pattern 4: End-to-End Pipeline Test

```python
def test_full_pipeline_end_to_end(spark, tmp_path):
    """
    Test the entire pipeline from raw CSV-like input to Gold aggregation.
    Simulates a full daily run.
    """
    # Set up paths
    paths = {
        "bronze": str(tmp_path / "bronze"),
        "silver": str(tmp_path / "silver"),
        "gold":   str(tmp_path / "gold"),
    }

    # Input: 10 records with known expected outputs
    raw_data = spark.createDataFrame([
        (1, "  alice  ", "ALICE@Email.com", "2024-01-15", 500.0),
        (2, "bob",       "bob@email.com",   "2024-01-15", 300.0),
        (3, "carol",     "carol@email.com", "2024-01-16", 700.0),
        (4, "dave",      "not-valid-email", "2024-01-16", 200.0),  # Bad email → filtered
        (2, "bob",       "bob@email.com",   "2024-01-15", 300.0),  # Duplicate → deduped
    ], ["customer_id", "name", "email", "signup_date", "lifetime_value"])

    # Run the full pipeline
    bronze_ingest(raw_data, paths["bronze"])
    silver_promote(paths["bronze"], paths["silver"], "customer_id")
    gold_aggregate(paths["silver"], paths["gold"])

    # ── Assertions at each layer ───────────────────────────────────────────

    # Bronze: all 5 rows ingested (including duplicates)
    bronze = spark.read.format("delta").load(paths["bronze"])
    assert bronze.count() == 5

    # Silver: 3 unique, valid customers (no dupe, no invalid email)
    silver = spark.read.format("delta").load(paths["silver"])
    assert silver.count() == 3
    assert silver.filter(col("customer_id") == 4).count() == 0  # Bad email removed
    # Deduplication worked
    assert silver.filter(col("customer_id") == 2).count() == 1
    # Cleaning applied
    alice = silver.filter(col("customer_id") == 1).collect()[0]
    assert alice.name == "alice"
    assert alice.email == "alice@email.com"

    # Gold: correct aggregations by date
    gold = spark.read.format("delta").load(paths["gold"])
    gold_rows = {r.signup_date: r for r in gold.collect()}
    assert gold_rows["2024-01-15"].new_customers == 2   # Alice + Bob
    assert abs(gold_rows["2024-01-15"].total_ltv - 800.0) < 0.001
    assert gold_rows["2024-01-16"].new_customers == 1   # Carol (Dave filtered)
    assert abs(gold_rows["2024-01-16"].total_ltv - 700.0) < 0.001
```

---

## Key Takeaways

1. **Layer-by-layer assertions** in medallion pipeline tests catch exactly which stage introduced a bug — don't just test the final output.
2. **MERGE tests must verify idempotency** — running the same MERGE twice should produce the same result (upsert semantics guarantee this, but your tests should verify it explicitly).
3. **Window function tests** must verify partition boundaries — the first row's null lag value and the independence of different partition groups are the most common failure points.
4. **End-to-end tests** with known input/output pairs are the most valuable — they document exactly what the pipeline is supposed to do, and they catch cross-stage interaction bugs that unit tests miss.
5. **Use `tmp_path` pytest fixture** for Delta table paths — it creates a fresh temp directory per test, preventing test pollution.
