---
title: "Python Testing with pytest - Real-World Production Examples"
topic: python
subtopic: testing-pytest
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, testing, pytest, production, glue, spark, data-quality, cicd]
---

# Python Testing with pytest — Real-World Production Examples

## Pattern 1: Testing a Glue/Spark ETL Job

Complete test setup for an AWS Glue job that processes daily events:

```python
"""
Test suite for daily_events Glue job.
Tests the core transformation logic independent of Glue runtime.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date, datetime
from decimal import Decimal

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("glue-job-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

@pytest.fixture
def raw_events(spark):
    """Simulate raw events as they arrive from source."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("amount", StringType()),  # Arrives as string!
        StructField("event_timestamp", StringType()),
        StructField("metadata", StringType()),  # JSON string
    ])
    data = [
        ("e1", "u1", "purchase", "99.99", "2024-01-15 10:00:00", '{"channel": "web"}'),
        ("e2", "u2", "purchase", "invalid", "2024-01-15 11:00:00", '{"channel": "app"}'),
        ("e3", "u1", "refund", "-25.00", "2024-01-15 12:00:00", '{}'),
        ("e1", "u1", "purchase", "99.99", "2024-01-15 10:00:00", '{"channel": "web"}'),  # Dupe
        ("e4", None, "click", "0", "2024-01-15 13:00:00", None),  # Null user
    ]
    return spark.createDataFrame(data, schema)

class TestEventTransformations:
    """Unit tests for individual transform steps."""
    
    def test_deduplicate_by_event_id(self, spark, raw_events):
        from jobs.daily_events import deduplicate_events
        
        result = deduplicate_events(raw_events)
        assert result.count() == 4  # e1 duplicate removed
    
    def test_cast_amount_handles_invalid(self, spark, raw_events):
        from jobs.daily_events import cast_and_validate
        
        valid, invalid = cast_and_validate(raw_events)
        
        assert valid.count() == 3  # "invalid" and null user excluded
        assert invalid.count() == 2
        assert valid.schema["amount"].dataType == DecimalType(10, 2)
    
    def test_parse_metadata_json(self, spark, raw_events):
        from jobs.daily_events import parse_metadata
        
        result = parse_metadata(raw_events)
        
        channels = [row.channel for row in result.select("channel").collect()]
        assert "web" in channels
        assert "app" in channels
    
    def test_full_transform_pipeline(self, spark, raw_events):
        """Integration test of all transforms chained together."""
        from jobs.daily_events import transform_events
        
        result = transform_events(raw_events)
        
        # Schema contract
        assert "event_id" in result.columns
        assert "amount_decimal" in result.columns
        assert "event_date" in result.columns
        assert "channel" in result.columns
        
        # Data quality
        null_users = result.filter(result.user_id.isNull()).count()
        assert null_users == 0, "Null users should be filtered"
        
        # No duplicates
        assert result.count() == result.select("event_id").distinct().count()
```

---

## Pattern 2: Testing API Connectors with Mocks

Testing a data connector that fetches from external APIs:

```python
"""
Tests for API connector — isolates network calls with mocks.
Tests retry logic, pagination, and error handling.
"""
import pytest
from unittest.mock import patch, MagicMock, call
import requests
from connectors.rest_api import PaginatedAPIConnector, APIConfig

@pytest.fixture
def api_config():
    return APIConfig(
        base_url="https://api.example.com/v2",
        auth_token="test-token",
        page_size=100,
        max_retries=3,
        rate_limit_rps=10.0
    )

@pytest.fixture
def connector(api_config):
    return PaginatedAPIConnector(api_config)

class TestPaginatedFetching:
    
    @patch("connectors.rest_api.requests.Session.get")
    def test_fetches_all_pages(self, mock_get, connector):
        """Verify connector follows pagination to the end."""
        mock_get.side_effect = [
            self._mock_response({"results": [{"id": i} for i in range(100)], "next_cursor": "abc"}),
            self._mock_response({"results": [{"id": i} for i in range(100, 150)], "next_cursor": None}),
        ]
        
        records = list(connector.fetch_all("/events"))
        
        assert len(records) == 150
        assert mock_get.call_count == 2
    
    @patch("connectors.rest_api.requests.Session.get")
    def test_handles_empty_response(self, mock_get, connector):
        mock_get.return_value = self._mock_response({"results": [], "next_cursor": None})
        
        records = list(connector.fetch_all("/events"))
        assert records == []
    
    @patch("time.sleep")  # Skip actual waiting
    @patch("connectors.rest_api.requests.Session.get")
    def test_retries_on_server_error(self, mock_get, mock_sleep, connector):
        mock_get.side_effect = [
            self._mock_response(status=503),
            self._mock_response(status=503),
            self._mock_response({"results": [{"id": 1}], "next_cursor": None}),
        ]
        
        records = list(connector.fetch_all("/events"))
        
        assert len(records) == 1
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2  # Backoff between retries
    
    @patch("time.sleep")
    @patch("connectors.rest_api.requests.Session.get")
    def test_respects_rate_limit_header(self, mock_get, mock_sleep, connector):
        mock_get.side_effect = [
            self._mock_response(status=429, headers={"Retry-After": "5"}),
            self._mock_response({"results": [{"id": 1}], "next_cursor": None}),
        ]
        
        records = list(connector.fetch_all("/events"))
        
        assert len(records) == 1
        mock_sleep.assert_any_call(5.0)
    
    @patch("connectors.rest_api.requests.Session.get")
    def test_raises_after_max_retries(self, mock_get, connector):
        mock_get.side_effect = requests.ConnectionError("refused")
        
        with pytest.raises(Exception, match="after 3 attempts"):
            list(connector.fetch_all("/events"))
    
    def _mock_response(self, json_data=None, status=200, headers=None):
        resp = MagicMock()
        resp.status_code = status
        resp.headers = headers or {}
        resp.json.return_value = json_data or {}
        resp.raise_for_status.side_effect = (
            requests.HTTPError(response=resp) if status >= 400 else None
        )
        return resp
```

---

## Pattern 3: Data Quality Test Suite

Automated data quality checks that run after each pipeline execution:

```python
"""
Data quality test framework.
Runs assertions against pipeline output to catch regressions.
"""
import pytest
from datetime import date

@pytest.fixture(scope="module")
def pipeline_output(request):
    """Load the latest pipeline output for quality testing."""
    import pyarrow.parquet as pq
    execution_date = request.config.getoption("--execution-date", default=str(date.today()))
    
    path = f"s3://data-lake/curated/events/dt={execution_date}/"
    return pq.read_table(path).to_pandas()

class TestDataCompleteness:
    """Verify no data loss or unexpected gaps."""
    
    def test_record_count_within_bounds(self, pipeline_output):
        count = len(pipeline_output)
        # Based on historical patterns: 100K-500K daily events
        assert count >= 100_000, f"Suspiciously low count: {count}"
        assert count <= 500_000, f"Suspiciously high count: {count}"
    
    def test_no_null_primary_keys(self, pipeline_output):
        null_pks = pipeline_output["event_id"].isna().sum()
        assert null_pks == 0, f"Found {null_pks} null primary keys"
    
    def test_all_expected_sources_present(self, pipeline_output):
        expected_sources = {"web", "mobile", "api"}
        actual_sources = set(pipeline_output["source"].unique())
        missing = expected_sources - actual_sources
        assert not missing, f"Missing sources: {missing}"

class TestDataAccuracy:
    """Verify values are within expected ranges."""
    
    def test_amounts_are_reasonable(self, pipeline_output):
        amounts = pipeline_output["amount"].dropna()
        assert amounts.min() >= -10000, "Unreasonable negative amount"
        assert amounts.max() <= 100000, "Unreasonable positive amount"
    
    def test_timestamps_within_expected_range(self, pipeline_output):
        timestamps = pipeline_output["event_timestamp"]
        min_ts = timestamps.min()
        max_ts = timestamps.max()
        
        # Events should be within the processing date +/- 1 day
        assert min_ts >= "2024-01-14", f"Event too old: {min_ts}"
        assert max_ts <= "2024-01-16", f"Event too new: {max_ts}"
    
    def test_no_duplicate_event_ids(self, pipeline_output):
        total = len(pipeline_output)
        unique = pipeline_output["event_id"].nunique()
        dup_rate = 1 - (unique / total)
        assert dup_rate == 0, f"Duplicate rate: {dup_rate:.4%}"

class TestSchemaConsistency:
    """Verify schema hasn't drifted."""
    
    def test_expected_columns_present(self, pipeline_output):
        required = {"event_id", "user_id", "event_type", "amount", "event_timestamp", "source"}
        actual = set(pipeline_output.columns)
        missing = required - actual
        assert not missing, f"Missing columns: {missing}"
    
    def test_column_types(self, pipeline_output):
        assert pipeline_output["amount"].dtype == "float64"
        assert pipeline_output["user_id"].dtype == "object"  # string
```

---

## Pattern 4: CI/CD Integration

```yaml
# .github/workflows/pipeline-tests.yml (conceptual structure)
# Runs: unit tests on every PR, integration tests on merge to main
```

```python
# conftest.py — CI-aware fixture configuration
import pytest
import os

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: fast unit tests")
    config.addinivalue_line("markers", "integration: requires external resources")
    config.addinivalue_line("markers", "data_quality: post-pipeline validation")

@pytest.fixture(scope="session")
def is_ci():
    """Detect CI environment."""
    return os.environ.get("CI", "false").lower() == "true"

@pytest.fixture(scope="session")
def spark(is_ci):
    """Spark session — smaller config in CI for speed."""
    from pyspark.sql import SparkSession
    
    builder = SparkSession.builder.master("local[1]" if is_ci else "local[4]")
    builder = builder.config("spark.sql.shuffle.partitions", "1" if is_ci else "4")
    builder = builder.config("spark.driver.memory", "1g" if is_ci else "4g")
    
    session = builder.appName("tests").getOrCreate()
    yield session
    session.stop()
```

```ini
# pytest.ini — separate test profiles
[pytest]
markers =
    unit: Unit tests (no external deps)
    integration: Integration tests (needs Docker)
    data_quality: Data quality checks (needs data)

# CI runs only unit by default
# Full suite via: pytest -m "unit or integration"
```

---

## Interview Tips

> **Tip 1:** For Glue/Spark testing, explain the separation: "I extract transformation logic into pure functions that accept and return DataFrames. The Glue job script just handles I/O (reading from catalog, writing to S3). This makes the transforms testable with a local SparkSession without needing the Glue runtime."

> **Tip 2:** Data quality tests as code is a modern DE practice. Frame it: "I treat data quality assertions like unit tests — they run automatically after each pipeline execution. If record count drops below historical baseline or duplicates appear, the test fails and triggers an alert before bad data reaches dashboards."

> **Tip 3:** For CI/CD integration, mention the test pyramid: "Unit tests run on every PR (fast gate). Integration tests with testcontainers run on merge to main. Data quality tests run post-deployment against real output. This catches bugs at the earliest, cheapest point in the development cycle."
