---
title: "Testing PySpark — Senior Deep Dive"
topic: pyspark
subtopic: testing-pyspark
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, integration-testing, streaming-tests, CI-CD, contract-testing, test-data-generation]
---

# Testing PySpark — Senior Deep Dive

Senior-level testing goes beyond unit tests. You need integration tests that verify Delta Lake operations work correctly end-to-end, tests for streaming logic, a CI/CD pipeline that runs tests on every commit, and a strategy for generating realistic test data at scale.

---

## Integration Testing with Delta Lake

Unit tests with in-memory DataFrames are fast but don't catch issues with Delta operations: MERGE behavior, time travel correctness, CDF output, or file compaction effects.

```python
# tests/integration/test_delta_operations.py
import pytest
import tempfile
import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@pytest.fixture(scope="module")
def delta_table_path(tmp_path_factory):
    """Create a temporary Delta table for integration tests."""
    return str(tmp_path_factory.mktemp("delta"))


@pytest.fixture(scope="module")
def initialized_delta_table(spark, delta_table_path):
    """Set up a Delta table with initial data for integration tests."""
    initial_data = spark.createDataFrame([
        (1, "Alice", "alice@email.com"),
        (2, "Bob",   "bob@email.com"),
        (3, "Carol", "carol@email.com"),
    ], ["id", "name", "email"])

    initial_data.write.format("delta").mode("overwrite").save(delta_table_path)
    return delta_table_path


def test_merge_updates_existing_records(spark, initialized_delta_table):
    """MERGE should update existing records, leave others unchanged."""
    updates = spark.createDataFrame([
        (1, "Alice Smith", "alice.smith@email.com"),  # Updated
        (4, "Dave",        "dave@email.com"),          # New
    ], ["id", "name", "email"])

    target = DeltaTable.forPath(spark, initialized_delta_table)
    target.alias("t").merge(
        updates.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    result = spark.read.format("delta").load(initialized_delta_table)
    rows = {r.id: r for r in result.collect()}

    assert rows[1].name == "Alice Smith"       # Updated
    assert rows[1].email == "alice.smith@email.com"
    assert rows[2].name == "Bob"               # Unchanged
    assert rows[4].name == "Dave"              # Inserted
    assert result.count() == 4


def test_time_travel_returns_previous_version(spark, initialized_delta_table):
    """After writes, previous version should still be accessible via time travel."""
    # Version 0 was the initial load (3 rows)
    v0 = spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(initialized_delta_table)
    assert v0.count() == 3

    # Current version should have 4 rows (after the MERGE above)
    current = spark.read.format("delta").load(initialized_delta_table)
    assert current.count() == 4


def test_cdf_captures_merge_changes(spark, initialized_delta_table):
    """Change Data Feed should record what the MERGE changed."""
    # CDF must be enabled for this test — enable it
    spark.sql(f"""
        ALTER TABLE delta.`{initialized_delta_table}`
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Do a targeted update
    updates = spark.createDataFrame([(2, "Robert", "robert@email.com")], ["id", "name", "email"])
    target = DeltaTable.forPath(spark, initialized_delta_table)
    target.alias("t").merge(
        updates.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().execute()

    # Read CDF for the last version
    history = DeltaTable.forPath(spark, initialized_delta_table).history(1)
    last_version = history.select("version").collect()[0][0]

    cdf = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_version) \
        .load(initialized_delta_table)

    preimage  = cdf.filter(col("_change_type") == "update_preimage").collect()
    postimage = cdf.filter(col("_change_type") == "update_postimage").collect()

    assert len(preimage)  == 1
    assert len(postimage) == 1
    assert preimage[0].name  == "Bob"
    assert postimage[0].name == "Robert"
```

---

## Testing Streaming Jobs with MemoryStream

Spark Structured Streaming is harder to test because it's continuous. Use `MemoryStream` to inject test data deterministically.

```python
# tests/test_streaming.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum as spark_sum
from pyspark.sql.streaming import DataStreamWriter


def test_streaming_aggregation(spark, tmp_path):
    """Test a streaming aggregation using MemoryStream for controlled input."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    from pyspark.sql import Row
    import time

    # Define event schema
    schema = StructType([
        StructField("event_id",   StringType(),   True),
        StructField("user_id",    StringType(),   True),
        StructField("event_type", StringType(),   True),
        StructField("amount",     DoubleType(),   True),
        StructField("event_time", TimestampType(), True),
    ])

    # Create MemoryStream
    from pyspark.sql.streaming import MemoryStream  # Available in PySpark 3.x
    # Note: MemoryStream is primarily a test utility in Scala; in Python use rate source
    # Alternative: use file-based streaming with a temp directory

    # Method: write test data to temp dir, use file streaming source
    input_dir = str(tmp_path / "input")
    checkpoint_dir = str(tmp_path / "checkpoint")
    output_dir = str(tmp_path / "output")

    os.makedirs(input_dir, exist_ok=True)

    # Write batch 1 of test events
    batch1 = spark.createDataFrame([
        ("e1", "u1", "purchase", 100.0, "2024-01-15 10:00:00"),
        ("e2", "u1", "purchase", 200.0, "2024-01-15 10:01:00"),
        ("e3", "u2", "purchase",  50.0, "2024-01-15 10:00:00"),
    ], ["event_id", "user_id", "event_type", "amount", "event_time"]) \
        .withColumn("event_time", col("event_time").cast("timestamp"))

    batch1.write.json(input_dir + "/batch1/")

    # Start streaming job
    stream = spark.readStream \
        .schema(schema) \
        .json(input_dir + "/*/") \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("user_id")
        ) \
        .agg(
            count("*").alias("event_count"),
            spark_sum("amount").alias("total_amount")
        )

    query = stream.writeStream \
        .format("memory") \
        .queryName("test_aggregation") \
        .outputMode("complete") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    # Wait for one trigger to process batch1
    query.processAllAvailable()

    # Assert results
    results = spark.sql("SELECT * FROM test_aggregation")
    rows = {r.user_id: r for r in results.collect()}

    assert abs(rows["u1"].total_amount - 300.0) < 0.001
    assert rows["u1"].event_count == 2
    assert abs(rows["u2"].total_amount - 50.0) < 0.001

    query.stop()
```

---

## CI/CD Pipeline for PySpark Tests

```yaml
# .github/workflows/pyspark-tests.yml
name: PySpark Test Suite

on:
  push:
    branches: [main, develop]
    paths:
      - 'src/**'
      - 'tests/**'
      - 'requirements*.txt'
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest

    strategy:
      matrix:
        python-version: ["3.10", "3.11"]
        pyspark-version: ["3.4.0", "3.5.0"]

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}

      - name: Set up Java (required for PySpark)
        uses: actions/setup-java@v3
        with:
          distribution: temurin
          java-version: '11'

      - name: Cache pip dependencies
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements*.txt') }}

      - name: Install dependencies
        run: |
          pip install pyspark==${{ matrix.pyspark-version }}
          pip install delta-spark==2.4.0
          pip install pytest pytest-cov chispa

      - name: Run unit tests
        run: |
          pytest tests/unit/ -v \
            --cov=src \
            --cov-report=xml \
            --cov-report=term-missing \
            -x  # Stop on first failure

      - name: Run integration tests
        run: |
          pytest tests/integration/ -v --timeout=300

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
          fail_ci_if_error: true
```

---

## Test Data Generation Strategies

Production-like test data reveals bugs that hand-crafted small DataFrames miss.

```python
# tests/data_generators.py
import random
import string
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def generate_orders(spark: SparkSession, n: int = 10_000, seed: int = 42) -> "DataFrame":
    """
    Generate realistic order data with known statistical properties.
    Designed to catch real-world data quality issues.
    """
    random.seed(seed)

    null_probability = 0.02  # 2% null customer_id (guest checkouts)
    duplicate_probability = 0.005  # 0.5% exact duplicates (retry storm simulation)
    skewed_products = ["prod_001", "prod_002"]  # 60% of orders on these 2 products

    rows = []
    for i in range(n):
        # Skewed product distribution
        if random.random() < 0.6:
            product_id = random.choice(skewed_products)
        else:
            product_id = f"prod_{random.randint(3, 500):03d}"

        customer_id = None if random.random() < null_probability else random.randint(1, 10_000)
        amount = round(random.uniform(1.0, 2000.0), 2)
        order_date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).date()

        rows.append((i + 1, customer_id, product_id, amount, str(order_date)))

    df = spark.createDataFrame(rows, ["order_id", "customer_id", "product_id", "amount", "order_date"])

    # Inject duplicates
    n_dupes = max(1, int(n * duplicate_probability))
    dupes = df.orderBy(col("order_id")).limit(n_dupes)
    return df.union(dupes)


def generate_cdc_events(spark: SparkSession, n: int = 1000) -> "DataFrame":
    """Generate CDC events with realistic op distribution."""
    ops = ["c"] * 600 + ["u"] * 350 + ["d"] * 50  # 60% insert, 35% update, 5% delete
    rows = [
        (random.randint(1, 10_000), f"Name {i}", f"email{i}@test.com",
         datetime.now().isoformat(), random.choice(ops))
        for i in range(n)
    ]
    return spark.createDataFrame(rows, ["customer_id", "name", "email", "updated_at", "op"])


# Use generated data in tests
def test_cdc_pipeline_at_scale(spark, delta_table_path):
    """Verify CDC pipeline handles realistic data volumes and distributions."""
    cdc_events = generate_cdc_events(spark, n=10_000)
    process_cdc_batch(spark, cdc_events, delta_table_path)

    result = spark.read.format("delta").load(delta_table_path)

    # Verify no duplicates in output
    assert result.count() == result.select("customer_id").distinct().count(), \
        "Duplicate customer_id found in Delta table after CDC processing"

    # Verify deleted records are gone
    delete_ids = [r.customer_id for r in
                  cdc_events.filter(col("op") == "d").select("customer_id").collect()]
    remaining_deleted = result.filter(col("customer_id").isin(delete_ids)).count()
    assert remaining_deleted == 0, f"Deleted records found in output: {remaining_deleted}"
```

---

## Contract Testing Between Producer and Consumer

When your pipeline produces a table consumed by other teams, contract testing ensures schema changes don't break downstream consumers.

```python
# tests/contract/test_schema_contracts.py
import pytest
from pyspark.sql.types import StructField, LongType, StringType, DoubleType, DateType


# Define the published contract for the silver_orders table
SILVER_ORDERS_CONTRACT = {
    "required_columns": ["order_id", "customer_id", "product_id", "amount", "order_date"],
    "column_types": {
        "order_id":   LongType(),
        "customer_id": LongType(),
        "product_id":  StringType(),
        "amount":      DoubleType(),
        "order_date":  DateType(),
    },
    "non_nullable_columns": ["order_id"],
    "max_null_pct": {
        "customer_id": 0.05,  # Max 5% null customer_id (guest orders)
        "amount":      0.00,  # Never null
    }
}


def validate_contract(df, contract: dict) -> list:
    """Validate a DataFrame against a schema contract. Returns list of violations."""
    violations = []
    schema_dict = {f.name: f for f in df.schema.fields}

    # Check required columns exist
    for col_name in contract["required_columns"]:
        if col_name not in schema_dict:
            violations.append(f"Missing required column: {col_name}")

    # Check column types
    for col_name, expected_type in contract["column_types"].items():
        if col_name in schema_dict:
            if not isinstance(schema_dict[col_name].dataType, type(expected_type)):
                violations.append(
                    f"Column {col_name}: expected {expected_type}, "
                    f"got {schema_dict[col_name].dataType}"
                )

    # Check non-nullable columns
    for col_name in contract["non_nullable_columns"]:
        if col_name in schema_dict and schema_dict[col_name].nullable:
            violations.append(f"Column {col_name} must be non-nullable per contract")

    # Check null percentages (data quality)
    n = df.count()
    if n > 0:
        from pyspark.sql.functions import col, isnan, isnull, sum as spark_sum
        for col_name, max_pct in contract.get("max_null_pct", {}).items():
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_pct = null_count / n
                if null_pct > max_pct:
                    violations.append(
                        f"Column {col_name}: null pct {null_pct:.2%} exceeds contract max {max_pct:.2%}"
                    )

    return violations


def test_silver_orders_meets_contract(spark):
    """Verify the silver_orders output meets its published schema contract."""
    # Run the pipeline to generate the output
    raw_orders = generate_orders(spark, n=1000)
    silver_orders = produce_silver_orders(raw_orders)  # your pipeline function

    violations = validate_contract(silver_orders, SILVER_ORDERS_CONTRACT)

    assert not violations, (
        f"Schema contract violations found:\n" +
        "\n".join(f"  - {v}" for v in violations)
    )
```

---

## Key Takeaways for Senior DEs

1. **Integration tests with real Delta operations** catch MERGE, CDF, and time travel edge cases that unit tests miss — worth the additional setup cost.
2. **MemoryStream and file-based streaming tests** let you verify streaming logic deterministically without a live Kafka cluster.
3. **CI/CD with matrix testing** (multiple PySpark versions) catches version-specific API changes before they reach production.
4. **Generated test data** with controlled statistical properties (known null %, duplicate %, skew) finds real bugs that hand-crafted tiny DataFrames don't.
5. **Contract tests** between producers and consumers prevent the most painful incident type in DE: a schema change that silently breaks downstream pipelines.
