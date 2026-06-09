---
title: "Testing PySpark — Fundamentals"
topic: pyspark
subtopic: testing-pyspark
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, pytest, testing, SparkSession, DataFrame, schema-assertion]
---

# Testing PySpark — Fundamentals

Testing PySpark jobs is one of the most neglected skills in data engineering — and one of the most valuable. Untested pipelines cause silent data quality issues, costly incident response, and loss of trust from data consumers. This section covers how to structure tests, set up SparkSession fixtures, and write assertions on DataFrame output.

---

## Why Test PySpark Jobs?

Unlike a web API where a bug causes an immediate 500 error, bad data pipelines often fail silently: wrong aggregation logic produces slightly off numbers, a NULL-handling bug drops 0.1% of rows, a schema change passes the write but corrupts downstream reports.

Testing gives you:
1. **Confidence to refactor** — change the implementation without fear of breaking behavior.
2. **Regression protection** — a fix for a data bug prevents it from recurring.
3. **Documentation by example** — tests describe what the function is supposed to do.
4. **Faster debugging** — reproduce a bug with a tiny test DataFrame instead of re-running the full pipeline.

---

## Setting Up pytest with PySpark

### Installation

```bash
pip install pyspark pytest chispa pytest-spark
```

### SparkSession Fixture

The `SparkSession` is expensive to create. Use a `session`-scoped pytest fixture so it's created once for the entire test run.

```python
# conftest.py (place at the root of your tests directory)
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the entire test session."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pyspark-tests") \
        .config("spark.sql.shuffle.partitions", "4")  # Reduce for test speed
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Suppress noisy Spark logs in tests
    yield spark
    spark.stop()
```

**Why `local[2]`?** Tests run locally with 2 CPU threads — fast enough for small DataFrames, no cluster required. For CI/CD this is the standard.

---

## Writing Your First PySpark Test

Structure your pipeline code so that each transformation is a pure function: takes a DataFrame in, returns a DataFrame out. This makes individual steps independently testable.

```python
# src/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, trim, when


def clean_customer_names(df: DataFrame) -> DataFrame:
    """Trim whitespace and uppercase customer names."""
    return df.withColumn("name", upper(trim(col("name"))))


def calculate_order_total(df: DataFrame) -> DataFrame:
    """Calculate order total = price * quantity * (1 - discount)."""
    return df.withColumn(
        "total",
        col("price") * col("quantity") * (1 - col("discount"))
    )


def flag_high_value_orders(df: DataFrame, threshold: float = 1000.0) -> DataFrame:
    """Flag orders above the threshold as high_value."""
    return df.withColumn(
        "is_high_value",
        when(col("total") >= threshold, True).otherwise(False)
    )
```

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

from src.transformations import clean_customer_names, calculate_order_total, flag_high_value_orders


def test_clean_customer_names(spark: SparkSession):
    # Arrange: create test input DataFrame
    input_df = spark.createDataFrame([
        (1, "  alice smith  "),
        (2, "BOB JONES"),
        (3, "  carol  "),
    ], ["id", "name"])

    # Act
    result = clean_customer_names(input_df)

    # Assert: collect and check
    rows = {row.id: row.name for row in result.collect()}
    assert rows[1] == "ALICE SMITH"
    assert rows[2] == "BOB JONES"     # Already upper, still correct
    assert rows[3] == "CAROL"


def test_calculate_order_total(spark: SparkSession):
    input_df = spark.createDataFrame([
        (1, 100.0, 2, 0.1),   # total = 100 * 2 * 0.9 = 180.0
        (2, 50.0,  3, 0.0),   # total = 50 * 3 * 1.0 = 150.0
    ], ["order_id", "price", "quantity", "discount"])

    result = calculate_order_total(input_df)
    rows = {row.order_id: row.total for row in result.collect()}

    assert abs(rows[1] - 180.0) < 0.001
    assert abs(rows[2] - 150.0) < 0.001


def test_flag_high_value_orders_default_threshold(spark: SparkSession):
    input_df = spark.createDataFrame([
        (1, 999.99),
        (2, 1000.0),
        (3, 1500.0),
    ], ["order_id", "total"])

    result = flag_high_value_orders(input_df)
    rows = {row.order_id: row.is_high_value for row in result.collect()}

    assert rows[1] is False  # 999.99 < 1000 threshold
    assert rows[2] is True   # exactly 1000 = high value
    assert rows[3] is True


def test_flag_high_value_orders_custom_threshold(spark: SparkSession):
    input_df = spark.createDataFrame([(1, 500.0), (2, 200.0)], ["order_id", "total"])
    result = flag_high_value_orders(input_df, threshold=300.0)
    rows = {row.order_id: row.is_high_value for row in result.collect()}
    assert rows[1] is True
    assert rows[2] is False
```

---

## Schema Assertions

Verifying that a transformation produces the correct schema is as important as verifying values.

```python
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, BooleanType
)


def test_output_schema(spark: SparkSession):
    """Verify that the enrichment function returns the expected schema."""
    input_df = spark.createDataFrame(
        [(1, 100.0, 2, 0.1)],
        ["order_id", "price", "quantity", "discount"]
    )

    result = calculate_order_total(input_df)

    # Method 1: Check that specific columns exist with correct types
    schema_dict = {f.name: f.dataType for f in result.schema.fields}
    assert "total" in schema_dict
    assert isinstance(schema_dict["total"], DoubleType)

    # Method 2: Assert exact schema (strict)
    expected_schema = StructType([
        StructField("order_id",  LongType(),   True),
        StructField("price",     DoubleType(), True),
        StructField("quantity",  LongType(),   True),
        StructField("discount",  DoubleType(), True),
        StructField("total",     DoubleType(), True),
    ])
    assert result.schema == expected_schema

    # Method 3: Check column names (less strict — useful when type inference varies)
    assert set(result.columns) == {"order_id", "price", "quantity", "discount", "total"}
```

---

## Testing Edge Cases

Always test the boundary conditions that production data will hit.

```python
def test_empty_dataframe(spark: SparkSession):
    """Transformation on empty DataFrame should return empty DataFrame, not error."""
    empty_df = spark.createDataFrame([], schema="order_id LONG, price DOUBLE, quantity LONG, discount DOUBLE")
    result = calculate_order_total(empty_df)
    assert result.count() == 0
    assert "total" in result.columns  # Schema still correct


def test_null_handling(spark: SparkSession):
    """Nulls in numeric columns should propagate correctly."""
    input_df = spark.createDataFrame([
        (1, None, 2, 0.1),   # null price
        (2, 50.0, None, 0.0), # null quantity
    ], ["order_id", "price", "quantity", "discount"])

    result = calculate_order_total(input_df)
    rows = {row.order_id: row.total for row in result.collect()}

    # price * null = null, null * anything = null
    assert rows[1] is None
    assert rows[2] is None


def test_no_new_columns_added_unexpectedly(spark: SparkSession):
    """Transformation should not add unexpected columns."""
    input_df = spark.createDataFrame(
        [(1, "Alice")], ["id", "name"]
    )
    result = clean_customer_names(input_df)
    # clean_customer_names modifies "name" in place — no new columns
    assert result.columns == ["id", "name"]


def test_row_count_preserved(spark: SparkSession):
    """A transformation that doesn't filter should preserve row count."""
    input_df = spark.createDataFrame(
        [(i, f"Name {i}") for i in range(100)],
        ["id", "name"]
    )
    result = clean_customer_names(input_df)
    assert result.count() == 100
```

---

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run a specific test file
pytest tests/test_transformations.py -v

# Run a specific test
pytest tests/test_transformations.py::test_null_handling -v

# Run with coverage report
pytest tests/ --cov=src --cov-report=html

# Output:
# tests/test_transformations.py::test_clean_customer_names PASSED
# tests/test_transformations.py::test_calculate_order_total PASSED
# tests/test_transformations.py::test_null_handling PASSED
# ...
```

---

## Key Takeaways for Junior DEs

1. **Fixture scope matters:** `scope="session"` creates the SparkSession once — using `scope="function"` creates it per test, making your test suite 10x slower.
2. **Test pure transformation functions** — if your code is one giant script, it's untestable. Factor transformations into functions that take DataFrames and return DataFrames.
3. **Always test nulls and empty DataFrames** — these are the most common sources of silent pipeline failures.
4. **Assert schema + values** — checking only values means a type change can go undetected.
5. **`local[2]` + small test DataFrames** — tests should run in under 30 seconds total. If they don't, you're using too much data or missing the `scope="session"` on the fixture.
