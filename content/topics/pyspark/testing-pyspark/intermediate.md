---
title: "Testing PySpark — Intermediate"
topic: pyspark
subtopic: testing-pyspark
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, pytest, chispa, parameterized-tests, mocking, edge-cases]
---

# Testing PySpark — Intermediate

Beyond basic assertions, production-grade testing requires parameterized tests for multiple scenarios, DataFrame equality assertions with the `chispa` library, mocking data sources for isolation, and systematic edge case coverage.

---

## chispa: DataFrame Equality the Right Way

The `chispa` library provides clean DataFrame equality assertions that handle floating-point precision, null values, and row ordering.

```python
# Install: pip install chispa
from chispa.dataframe_comparer import assert_df_equality
from chispa.schema_comparer import assert_schema_equality

def test_with_chispa(spark):
    """Use chispa for clean DataFrame equality assertions."""
    input_df = spark.createDataFrame([
        (1, 100.0, 2, 0.10),
        (2, 50.0,  3, 0.00),
    ], ["order_id", "price", "quantity", "discount"])

    result = calculate_order_total(input_df)

    expected = spark.createDataFrame([
        (1, 100.0, 2, 0.10, 180.0),
        (2, 50.0,  3, 0.00, 150.0),
    ], ["order_id", "price", "quantity", "discount", "total"])

    # Checks: schema, values (with floating-point tolerance), row count
    assert_df_equality(result, expected, ignore_row_order=True)

    # Check only schema
    assert_schema_equality(result.schema, expected.schema)
```

### Ignoring Row Order and Columns

```python
# Real pipelines often don't guarantee row order — ignore it
assert_df_equality(result, expected, ignore_row_order=True)

# Check only specific columns (ignore extra columns added by the function)
assert_df_equality(
    result.select("order_id", "total"),
    expected.select("order_id", "total"),
    ignore_row_order=True
)

# Floating point tolerance (useful for decimal calculations)
from chispa import assert_approx_df_equality
assert_approx_df_equality(result, expected, precision=0.001)
```

---

## Parameterized Tests

Use `pytest.mark.parametrize` to test multiple input scenarios without duplicating test code.

```python
import pytest
from pyspark.sql import SparkSession


@pytest.mark.parametrize("input_data, expected_flag", [
    # (price, qty, discount, expected_is_high_value)
    [(100.0, 2, 0.0, 200.0)],   # 200 < 1000 → False
    [(500.0, 2, 0.0, 1000.0)],  # exactly 1000 → True
    [(1000.0, 2, 0.0, 2000.0)], # 2000 > 1000 → True
    [(100.0, 10, 0.5, 500.0)],  # 100*10*0.5=500 < 1000 → False
])
def test_high_value_threshold_parametrized(spark, input_data, expected_flag):
    price, qty, discount, expected_total = input_data
    df = spark.createDataFrame(
        [(1, price, qty, discount)],
        ["order_id", "price", "quantity", "discount"]
    )
    result = calculate_order_total(df)
    result = flag_high_value_orders(result)
    row = result.collect()[0]
    assert abs(row.total - expected_total) < 0.001


# More realistic parameterized test
@pytest.mark.parametrize("name_input, expected_output", [
    ("  alice  ", "ALICE"),
    ("BOB", "BOB"),
    ("carol jones", "CAROL JONES"),
    ("  ", ""),    # Whitespace only → trimmed to empty string, then uppercased
])
def test_name_cleaning_parametrized(spark, name_input, expected_output):
    df = spark.createDataFrame([(1, name_input)], ["id", "name"])
    result = clean_customer_names(df)
    assert result.collect()[0].name == expected_output
```

---

## Mocking Data Sources

Pure transformation functions are easy to test. The challenge is when your function reads from an external source (S3, database, Delta table). Mock the source for unit tests.

```python
# src/pipeline.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast


def enrich_orders_with_products(
    spark: SparkSession,
    orders_path: str,
    products_path: str
) -> DataFrame:
    """Load and join orders with product dimension."""
    orders = spark.read.parquet(orders_path)
    products = spark.read.parquet(products_path)
    return orders.join(broadcast(products), on="product_id", how="left")
```

**Problem:** this function reads from S3 — we can't test it without real data files.

**Solution 1: Dependency injection** — pass DataFrames instead of paths.

```python
# src/pipeline.py (refactored for testability)
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast


def enrich_orders_with_products(orders: DataFrame, products: DataFrame) -> DataFrame:
    """Join orders with product dimension. Testable — takes DFs, not paths."""
    return orders.join(broadcast(products), on="product_id", how="left")
```

```python
# tests/test_pipeline.py
def test_enrich_orders_with_products(spark):
    orders = spark.createDataFrame([
        (1, "prod_001", 100.0),
        (2, "prod_002", 200.0),
        (3, "prod_999", 50.0),  # Unknown product
    ], ["order_id", "product_id", "amount"])

    products = spark.createDataFrame([
        ("prod_001", "Widget A", "Electronics"),
        ("prod_002", "Widget B", "Electronics"),
    ], ["product_id", "name", "category"])

    result = enrich_orders_with_products(orders, products)

    rows = {r.order_id: r for r in result.collect()}
    assert rows[1].category == "Electronics"
    assert rows[2].category == "Electronics"
    assert rows[3].name is None  # Left join — unknown product → null
    assert result.count() == 3  # No rows dropped


def test_enrich_preserves_all_orders(spark):
    """Left join should never drop order rows."""
    n = 100
    orders = spark.createDataFrame(
        [(i, f"prod_{i}", float(i * 10)) for i in range(n)],
        ["order_id", "product_id", "amount"]
    )
    products = spark.createDataFrame([], schema="product_id STRING, name STRING, category STRING")

    result = enrich_orders_with_products(orders, products)
    assert result.count() == n  # All orders preserved even with empty products
```

**Solution 2: `monkeypatch` with `unittest.mock`** — for functions that can't be refactored.

```python
# tests/test_pipeline_mock.py
from unittest.mock import patch, MagicMock


def test_enrich_with_mock_reads(spark, monkeypatch):
    """Mock spark.read.parquet to return test DataFrames."""
    mock_orders = spark.createDataFrame([(1, "p1", 100.0)], ["order_id", "product_id", "amount"])
    mock_products = spark.createDataFrame([("p1", "Widget", "Electronics")], ["product_id", "name", "category"])

    read_results = {
        "s3://orders/": mock_orders,
        "s3://products/": mock_products,
    }

    original_parquet = spark.read.parquet

    def mock_parquet(path):
        return read_results.get(path, spark.createDataFrame([], schema="id LONG"))

    with patch.object(spark.read, "parquet", side_effect=mock_parquet):
        from src.pipeline import enrich_orders_with_products_from_paths
        result = enrich_orders_with_products_from_paths(spark, "s3://orders/", "s3://products/")
        assert result.count() == 1
```

---

## Testing Edge Cases Systematically

Create a standard set of edge case fixtures:

```python
# conftest.py — shared edge case fixtures

@pytest.fixture
def empty_orders_df(spark):
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
    schema = StructType([
        StructField("order_id",   LongType(),   True),
        StructField("product_id", StringType(), True),
        StructField("amount",     DoubleType(), True),
    ])
    return spark.createDataFrame([], schema=schema)


@pytest.fixture
def orders_with_nulls_df(spark):
    return spark.createDataFrame([
        (1, None,      100.0),  # null product_id
        (2, "prod_01", None),   # null amount
        (3, None,      None),   # all join/measure columns null
    ], ["order_id", "product_id", "amount"])


@pytest.fixture
def orders_with_duplicates_df(spark):
    return spark.createDataFrame([
        (1, "prod_01", 100.0),
        (1, "prod_01", 100.0),  # exact duplicate
        (1, "prod_02", 200.0),  # same order_id, different product (real duplicate scenario)
    ], ["order_id", "product_id", "amount"])


# Test using edge case fixtures
def test_handles_null_product_id(spark, orders_with_nulls_df):
    products = spark.createDataFrame([("prod_01", "Widget")], ["product_id", "name"])
    result = enrich_orders_with_products(orders_with_nulls_df, products)
    # Null product_id should not match anything → name is null
    null_product_rows = result.filter(col("product_id").isNull()).collect()
    for row in null_product_rows:
        assert row.name is None


def test_handles_empty_input(spark, empty_orders_df):
    products = spark.createDataFrame([("prod_01", "Widget")], ["product_id", "name"])
    result = enrich_orders_with_products(empty_orders_df, products)
    assert result.count() == 0
    # Schema should still be correct even for empty input
    assert "name" in result.columns


def test_deduplication_function(spark, orders_with_duplicates_df):
    """Dedup function should reduce 3 rows to 2 (by order_id)."""
    from src.transformations import deduplicate_orders
    result = deduplicate_orders(orders_with_duplicates_df)
    assert result.count() == 2  # order_id 1 kept once per product
```

---

## Testing Window Functions and Aggregations

```python
from pyspark.sql.functions import col, row_number, sum as spark_sum
from pyspark.sql.window import Window
from src.transformations import rank_orders_by_customer


def test_rank_orders_by_customer(spark):
    """Verify window function assigns correct rank within each customer partition."""
    input_df = spark.createDataFrame([
        (1, "c1", 300.0),
        (2, "c1", 100.0),
        (3, "c1", 200.0),
        (4, "c2", 500.0),
        (5, "c2", 150.0),
    ], ["order_id", "customer_id", "amount"])

    result = rank_orders_by_customer(input_df)

    # Each customer should have rank 1 for their highest order
    top_orders = result.filter(col("rank") == 1).collect()
    top_by_customer = {r.customer_id: r.order_id for r in top_orders}
    assert top_by_customer["c1"] == 1  # order 1 has highest amount for c1
    assert top_by_customer["c2"] == 4  # order 4 has highest amount for c2

    # Each customer should have sequential ranks
    c1_ranks = sorted([r.rank for r in result.filter(col("customer_id") == "c1").collect()])
    assert c1_ranks == [1, 2, 3]


def test_aggregation(spark):
    """Test a GROUP BY aggregation produces correct results."""
    from src.transformations import daily_revenue_by_category

    input_df = spark.createDataFrame([
        ("2024-01-15", "Electronics", 100.0),
        ("2024-01-15", "Electronics", 200.0),
        ("2024-01-15", "Clothing",    50.0),
        ("2024-01-16", "Electronics", 150.0),
    ], ["date", "category", "amount"])

    result = daily_revenue_by_category(input_df)
    rows = {(r.date, r.category): r.total_revenue for r in result.collect()}

    assert abs(rows[("2024-01-15", "Electronics")] - 300.0) < 0.001
    assert abs(rows[("2024-01-15", "Clothing")]    -  50.0) < 0.001
    assert abs(rows[("2024-01-16", "Electronics")] - 150.0) < 0.001
    assert len(rows) == 3  # Exactly 3 groups
```

---

## Key Takeaways

1. **`chispa`** is the cleanest way to assert DataFrame equality — handles row order, floats, and nulls properly.
2. **Parameterized tests** with `pytest.mark.parametrize` let you cover multiple scenarios without code duplication.
3. **Dependency injection** (pass DataFrames, not paths) makes functions independently testable without file I/O.
4. **Edge case fixtures** (empty, null, duplicate) in `conftest.py` ensure every transformation is tested against real-world data quality issues.
5. **Window function tests** must verify partition-level correctness, not just row-level.
