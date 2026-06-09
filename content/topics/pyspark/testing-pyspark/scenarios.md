---
title: "Testing PySpark — Scenarios"
topic: pyspark
subtopic: testing-pyspark
content_type: scenario_question
tags: [pyspark, testing, pytest, CI-CD, streaming, contract-testing, scenarios]
---

# Testing PySpark — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Write a Basic pytest for a Transformation Function

**Scenario:** You've written a PySpark function that takes an orders DataFrame and categorizes orders into 'small' (< $100), 'medium' ($100-$999), and 'large' ($1000+). Your tech lead asks you to write pytest tests for it before merging. Write the test file.

<details>
<summary>💡 Hint</summary>

Think about: exact boundary values (99.99, 100.00, 999.99, 1000.00), null amounts, empty DataFrames, and schema assertions. The boundary cases are where bugs hide.

</details>

<details>
<summary>✅ Solution</summary>

```python
# src/categorize.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def categorize_orders(df: DataFrame) -> DataFrame:
    """Categorize orders by amount into small/medium/large tiers."""
    return df.withColumn(
        "tier",
        when(col("amount") < 100, "small")
        .when(col("amount") < 1000, "medium")
        .otherwise("large")
    )
```

```python
# tests/test_categorize.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.categorize import categorize_orders


# ── Happy path tests ───────────────────────────────────────────────────────

def test_small_order(spark: SparkSession):
    df = spark.createDataFrame([(1, 50.0)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "small"


def test_medium_order(spark: SparkSession):
    df = spark.createDataFrame([(1, 500.0)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "medium"


def test_large_order(spark: SparkSession):
    df = spark.createDataFrame([(1, 1500.0)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "large"


# ── Boundary tests (most important!) ──────────────────────────────────────

def test_boundary_99_99_is_small(spark):
    """$99.99 should be 'small' (strictly less than 100)."""
    df = spark.createDataFrame([(1, 99.99)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "small"


def test_boundary_100_00_is_medium(spark):
    """$100.00 is the lower bound of medium — NOT small."""
    df = spark.createDataFrame([(1, 100.00)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "medium"


def test_boundary_999_99_is_medium(spark):
    """$999.99 should be 'medium' (strictly less than 1000)."""
    df = spark.createDataFrame([(1, 999.99)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "medium"


def test_boundary_1000_00_is_large(spark):
    """$1000.00 is the lower bound of large — NOT medium."""
    df = spark.createDataFrame([(1, 1000.00)], ["order_id", "amount"])
    result = categorize_orders(df)
    assert result.collect()[0].tier == "large"


# ── Edge case tests ───────────────────────────────────────────────────────

def test_null_amount_maps_to_large(spark):
    """Null amount: when(null < 100) = null → falls through to otherwise('large').
    This may be correct or a bug — the test documents current behavior."""
    df = spark.createDataFrame([(1, None)], "order_id INT, amount DOUBLE")
    result = categorize_orders(df)
    # Spark: when(null condition) → skip → when(null condition) → skip → otherwise
    # Result: "large" for null amounts — document this explicitly
    tier = result.collect()[0].tier
    assert tier == "large", (
        "Null amount currently maps to 'large' via otherwise() — "
        "if this is undesired, add explicit null handling"
    )


def test_empty_dataframe(spark):
    """Empty input should return empty output with 'tier' column present."""
    df = spark.createDataFrame([], schema="order_id INT, amount DOUBLE")
    result = categorize_orders(df)
    assert result.count() == 0
    assert "tier" in result.columns


def test_multiple_orders_categorized_correctly(spark):
    """Test multiple rows across all tiers in one DataFrame."""
    df = spark.createDataFrame([
        (1, 50.0),
        (2, 100.0),
        (3, 500.0),
        (4, 999.0),
        (5, 1000.0),
        (6, 9999.0),
    ], ["order_id", "amount"])

    result = categorize_orders(df)
    rows = {r.order_id: r.tier for r in result.collect()}

    assert rows[1] == "small"
    assert rows[2] == "medium"
    assert rows[3] == "medium"
    assert rows[4] == "medium"
    assert rows[5] == "large"
    assert rows[6] == "large"


def test_schema_output(spark):
    """Output should have 'tier' column of StringType."""
    from pyspark.sql.types import StringType
    df = spark.createDataFrame([(1, 100.0)], ["order_id", "amount"])
    result = categorize_orders(df)
    schema_dict = {f.name: f.dataType for f in result.schema.fields}
    assert "tier" in schema_dict
    assert isinstance(schema_dict["tier"], StringType)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Test Suite for a Bronze → Silver ETL Job

**Scenario:** Your team has a Bronze → Silver ETL job that: reads raw JSON events from a Delta Bronze table, filters invalid events (no user_id, amount < 0), deduplicates on `event_id` (keep latest by `created_at`), standardizes `event_type` to lowercase, and upserts the cleaned records into a Silver Delta table. Design and implement a complete test suite with unit tests, edge case tests, and an integration test.

<details>
<summary>💡 Hint</summary>

Break the ETL into testable sub-functions first. Then: (1) unit test each transformation in isolation, (2) unit test each filter, (3) integration test the full MERGE into a real Delta table, (4) test idempotency. Use `tmp_path` fixtures for Delta paths.

</details>

<details>
<summary>✅ Solution</summary>

```python
# src/bronze_to_silver.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, row_number, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def filter_invalid_events(df: DataFrame) -> DataFrame:
    """Remove events with null user_id or negative amount."""
    return df.filter(
        col("user_id").isNotNull() &
        (col("amount") >= 0)
    )


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Keep only the latest event per event_id."""
    window = Window.partitionBy("event_id").orderBy(col("created_at").desc())
    return df.withColumn("_rn", row_number().over(window)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")


def standardize_event_types(df: DataFrame) -> DataFrame:
    """Lowercase all event_type values."""
    return df.withColumn("event_type", lower(col("event_type")))


def run_bronze_to_silver(spark: SparkSession, bronze_path: str, silver_path: str):
    """Full Bronze → Silver ETL pipeline."""
    bronze = spark.read.format("delta").load(bronze_path)
    silver_ready = bronze \
        .transform(filter_invalid_events) \
        .transform(deduplicate_events) \
        .transform(standardize_event_types)

    try:
        target = DeltaTable.forPath(spark, silver_path)
        target.alias("t").merge(silver_ready.alias("s"), "t.event_id = s.event_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        silver_ready.write.format("delta").mode("overwrite").save(silver_path)


# tests/test_bronze_to_silver.py
import pytest
from pyspark.sql.functions import col
from src.bronze_to_silver import (
    filter_invalid_events, deduplicate_events,
    standardize_event_types, run_bronze_to_silver
)


# ── Unit tests: filter_invalid_events ─────────────────────────────────────

def test_filter_removes_null_user_id(spark):
    df = spark.createDataFrame([
        ("e1", None,  "click", 0.0,  "2024-01-15T10:00:00"),
        ("e2", "u1",  "click", 10.0, "2024-01-15T10:01:00"),
    ], ["event_id", "user_id", "event_type", "amount", "created_at"])
    result = filter_invalid_events(df)
    assert result.count() == 1
    assert result.collect()[0].event_id == "e2"


def test_filter_removes_negative_amounts(spark):
    df = spark.createDataFrame([
        ("e1", "u1", "purchase", -1.0, "2024-01-15T10:00:00"),
        ("e2", "u1", "purchase", 0.0,  "2024-01-15T10:01:00"),  # 0 is valid
        ("e3", "u1", "purchase", 100.0, "2024-01-15T10:02:00"),
    ], ["event_id", "user_id", "event_type", "amount", "created_at"])
    result = filter_invalid_events(df)
    assert result.count() == 2
    assert result.filter(col("event_id") == "e1").count() == 0


def test_filter_empty_input(spark):
    df = spark.createDataFrame([], "event_id STRING, user_id STRING, event_type STRING, amount DOUBLE, created_at STRING")
    result = filter_invalid_events(df)
    assert result.count() == 0


# ── Unit tests: deduplicate_events ────────────────────────────────────────

def test_dedup_keeps_latest(spark):
    df = spark.createDataFrame([
        ("e1", "u1", "click", 0.0, "2024-01-15T10:00:00"),
        ("e1", "u1", "click", 0.0, "2024-01-15T10:05:00"),  # Same event_id, later timestamp
    ], ["event_id", "user_id", "event_type", "amount", "created_at"])
    result = deduplicate_events(df)
    assert result.count() == 1
    assert result.collect()[0].created_at == "2024-01-15T10:05:00"


def test_dedup_preserves_unique_events(spark):
    df = spark.createDataFrame([
        ("e1", "u1", "click",    0.0, "2024-01-15T10:00:00"),
        ("e2", "u1", "purchase", 99.0, "2024-01-15T10:01:00"),
    ], ["event_id", "user_id", "event_type", "amount", "created_at"])
    result = deduplicate_events(df)
    assert result.count() == 2


# ── Unit tests: standardize_event_types ──────────────────────────────────

def test_event_types_lowercased(spark):
    df = spark.createDataFrame([
        ("e1", "u1", "CLICK",    0.0),
        ("e2", "u1", "Purchase", 50.0),
        ("e3", "u1", "page_view", 0.0),
    ], ["event_id", "user_id", "event_type", "amount"])
    result = standardize_event_types(df)
    types = {r.event_id: r.event_type for r in result.collect()}
    assert types["e1"] == "click"
    assert types["e2"] == "purchase"
    assert types["e3"] == "page_view"


# ── Integration test: full pipeline ──────────────────────────────────────

def test_full_pipeline_integration(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")

    # Write Bronze with known data
    bronze_data = spark.createDataFrame([
        ("e1", "u1",  "CLICK",    10.0, "2024-01-15T10:00:00"),  # Valid
        ("e2", None,  "purchase", 50.0, "2024-01-15T10:01:00"),  # Null user_id → filtered
        ("e3", "u2",  "Purchase", -5.0, "2024-01-15T10:02:00"),  # Negative amount → filtered
        ("e1", "u1",  "CLICK",    10.0, "2024-01-15T10:03:00"),  # Duplicate e1 → deduped
        ("e4", "u3",  "VIEW",     0.0,  "2024-01-15T10:04:00"),  # Valid
    ], ["event_id", "user_id", "event_type", "amount", "created_at"])
    bronze_data.write.format("delta").save(bronze_path)

    run_bronze_to_silver(spark, bronze_path, silver_path)

    silver = spark.read.format("delta").load(silver_path)
    rows = {r.event_id: r for r in silver.collect()}

    # e2 filtered (null user_id), e3 filtered (negative amount)
    assert silver.count() == 2
    assert "e2" not in rows
    assert "e3" not in rows

    # Dedup: e1 appears once (latest timestamp)
    assert rows["e1"].created_at == "2024-01-15T10:03:00"

    # Lowercase applied
    assert rows["e1"].event_type == "click"
    assert rows["e4"].event_type == "view"

    # Idempotency: run again with same Bronze data → same Silver output
    run_bronze_to_silver(spark, bronze_path, silver_path)
    silver_after_rerun = spark.read.format("delta").load(silver_path)
    assert silver_after_rerun.count() == 2
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Full Test Strategy for a Production Streaming Pipeline

**Scenario:** Your team runs a Spark Structured Streaming pipeline that: reads from Kafka (JSON events), parses and validates events, computes per-user 5-minute windowed aggregations, and writes the results to a Delta Silver table using `foreachBatch` + MERGE. You need to design a complete test strategy that will run in CI/CD. What do you test, at which layer, with what tools, and what are your coverage requirements?

<details>
<summary>💡 Hint</summary>

Think in layers: unit tests for parsing/validation logic, integration tests for the MERGE in `foreachBatch`, streaming-specific tests for the windowing logic (use file-based streaming or memory source instead of Kafka), CI/CD requirements (Kafka is unavailable in CI — how do you mock it?), and end-to-end acceptance tests in a staging environment.

</details>

<details>
<summary>✅ Solution</summary>

```python
# ── Test Strategy Architecture ─────────────────────────────────────────────
#
# Layer 1 — Unit Tests (fast, no Spark required)
#   - JSON parsing functions (Python, pure functions)
#   - Validation logic (field presence, type coercion, range checks)
#   - Aggregation business logic (given inputs, verify outputs)
#   - Coverage target: 90%+ for all transformation logic
#
# Layer 2 — PySpark Unit Tests (SparkSession local[2])
#   - DataFrame transformation functions
#   - MERGE logic (via Delta + tmp_path)
#   - Window aggregation correctness
#   - Coverage target: 85%+ for all PySpark transformations
#
# Layer 3 — Streaming Integration Tests (file-based source, no Kafka)
#   - End-to-end streaming flow: read → parse → aggregate → write
#   - Test late data handling (watermark behavior)
#   - Test foreachBatch MERGE idempotency
#   - Coverage target: all streaming paths covered
#
# Layer 4 — Staging Acceptance Tests (real Kafka, real Delta, small scale)
#   - Produce known events to Kafka test topic
#   - Run pipeline against staging cluster
#   - Verify output matches expected aggregations
#   - Run nightly, not on every PR

# ── Layer 1: Pure Python unit tests ──────────────────────────────────────

import json
import pytest
from datetime import datetime


def parse_event(raw_json: str) -> dict | None:
    """Parse a raw Kafka JSON event. Returns None if invalid."""
    try:
        event = json.loads(raw_json)
        required = ["event_id", "user_id", "event_type", "amount", "event_time"]
        if not all(k in event for k in required):
            return None
        if event["amount"] < 0:
            return None
        event["event_time"] = datetime.fromisoformat(event["event_time"])
        return event
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def test_parse_valid_event():
    raw = '{"event_id": "e1", "user_id": "u1", "event_type": "purchase", "amount": 50.0, "event_time": "2024-01-15T10:00:00"}'
    result = parse_event(raw)
    assert result is not None
    assert result["user_id"] == "u1"
    assert isinstance(result["event_time"], datetime)


def test_parse_invalid_json_returns_none():
    assert parse_event("not json") is None
    assert parse_event("{corrupt}") is None


def test_parse_missing_field_returns_none():
    raw = '{"event_id": "e1", "user_id": "u1"}'  # Missing amount, event_type, event_time
    assert parse_event(raw) is None


def test_parse_negative_amount_returns_none():
    raw = '{"event_id": "e1", "user_id": "u1", "event_type": "refund", "amount": -50.0, "event_time": "2024-01-15T10:00:00"}'
    assert parse_event(raw) is None


# ── Layer 2: PySpark transformation tests ─────────────────────────────────

def test_windowed_aggregation(spark):
    """Verify 5-minute windowed sum of amounts per user."""
    from pyspark.sql.functions import window, sum as spark_sum, count

    events = spark.createDataFrame([
        ("u1", 100.0, "2024-01-15T10:00:00"),
        ("u1", 200.0, "2024-01-15T10:03:00"),  # Same 5-min window
        ("u1", 50.0,  "2024-01-15T10:06:00"),  # Different window
        ("u2", 300.0, "2024-01-15T10:01:00"),  # Different user, same window
    ], ["user_id", "amount", "event_time"]) \
        .withColumn("event_time", col("event_time").cast("timestamp"))

    result = events \
        .groupBy(window(col("event_time"), "5 minutes"), col("user_id")) \
        .agg(spark_sum("amount").alias("total_amount"), count("*").alias("event_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("user_id"),
            col("total_amount"),
            col("event_count")
        )

    rows = result.collect()
    # Window 10:00-10:05 for u1: 100 + 200 = 300
    u1_window1 = [r for r in rows if r.user_id == "u1" and "10:00" in str(r.window_start)]
    assert abs(u1_window1[0].total_amount - 300.0) < 0.001
    assert u1_window1[0].event_count == 2

    # Window 10:05-10:10 for u1: just 50
    u1_window2 = [r for r in rows if r.user_id == "u1" and "10:05" in str(r.window_start)]
    assert abs(u1_window2[0].total_amount - 50.0) < 0.001


# ── Layer 3: Streaming integration test ──────────────────────────────────

def test_streaming_pipeline_with_file_source(spark, tmp_path):
    """
    Test the full streaming pipeline using file-based source (no Kafka needed in CI).
    Kafka is mocked by writing JSON files to a temp directory.
    """
    import os
    input_dir = str(tmp_path / "kafka_mock")
    checkpoint_dir = str(tmp_path / "checkpoint")
    delta_output = str(tmp_path / "silver")
    os.makedirs(input_dir)

    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    from pyspark.sql.functions import window, sum as spark_sum, from_json
    from delta.tables import DeltaTable

    # Schema for events
    schema = StructType([
        StructField("event_id",   StringType(),   True),
        StructField("user_id",    StringType(),   True),
        StructField("event_type", StringType(),   True),
        StructField("amount",     DoubleType(),   True),
        StructField("event_time", TimestampType(), True),
    ])

    # "Produce" test events to mock Kafka (write JSON files)
    batch1 = spark.createDataFrame([
        ("e1", "u1", "purchase", 100.0, "2024-01-15T10:00:00"),
        ("e2", "u1", "purchase", 200.0, "2024-01-15T10:02:00"),
        ("e3", "u2", "purchase",  50.0, "2024-01-15T10:01:00"),
    ], ["event_id", "user_id", "event_type", "amount", "event_time"]) \
        .withColumn("event_time", col("event_time").cast("timestamp"))
    batch1.write.json(input_dir + "/batch1")

    # Define foreachBatch writer
    def upsert_to_silver(batch_df, batch_id):
        agg = batch_df \
            .groupBy(window(col("event_time"), "5 minutes"), col("user_id")) \
            .agg(spark_sum("amount").alias("total_amount")) \
            .select(
                col("user_id"),
                col("window.start").alias("window_start"),
                col("total_amount")
            )
        agg.write.format("delta").mode("append").save(delta_output)

    # Run streaming query
    query = spark.readStream \
        .schema(schema) \
        .json(input_dir + "/*/") \
        .writeStream \
        .foreachBatch(upsert_to_silver) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(availableNow=True) \
        .start()
    query.awaitTermination()

    # Verify Silver output
    silver = spark.read.format("delta").load(delta_output)
    rows = {r.user_id: r.total_amount for r in silver.collect()}

    # u1: 100 + 200 = 300 (both in same 5-min window)
    assert abs(rows["u1"] - 300.0) < 0.001
    assert abs(rows["u2"] - 50.0) < 0.001
```

**Complete test strategy summary:**

| Layer | Tool | What's Tested | CI Runtime |
|-------|------|--------------|-----------|
| Unit (Python) | pytest | JSON parsing, validation | < 5s |
| Unit (PySpark) | pytest + local[2] | Transformations, MERGE, windows | < 60s |
| Integration (streaming) | pytest + file source | Full streaming flow | < 120s |
| Acceptance (staging) | Pytest + real Kafka | End-to-end with real infra | ~15 min (nightly) |

Kafka is never required in unit/integration CI — file-based sources are functionally equivalent for testing logic. Only acceptance tests need real Kafka, and those run in a staging environment on a slower cadence.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you test a PySpark job that reads from S3?" — The answer isn't "I mock S3." The correct answer is: refactor the function to accept a DataFrame as a parameter instead of a path. The function that reads from S3 becomes a thin wrapper around the testable transformation function. Test the transformation function directly; test the reader separately with a small local file.

> **Tip 2:** "How do you make PySpark tests fast enough to run in CI?" — Three things: (1) `scope="session"` fixture so SparkSession is created once, not per test; (2) `local[2]` not `local[*]` — you don't need all cores for tiny DataFrames; (3) `spark.sql.shuffle.partitions = 4` — default 200 shuffle partitions for 5-row test DataFrames is absurd overhead. These three changes typically reduce test suite time from 5+ minutes to under 60 seconds.

> **Tip 3:** "What's the difference between a unit test and an integration test for PySpark?" — A unit test exercises a single transformation function with an in-memory DataFrame created by `spark.createDataFrame()`. An integration test writes actual Delta files to `tmp_path`, runs a MERGE, reads the result back, and verifies it. Unit tests are fast (< 1s) and catch logic bugs. Integration tests are slower (10-30s) and catch issues with Delta-specific behavior, file formats, and checkpoint handling. You need both.
