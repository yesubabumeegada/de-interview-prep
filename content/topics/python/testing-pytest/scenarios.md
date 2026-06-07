---
title: "Python Testing with pytest - Scenario Questions"
topic: python
subtopic: testing-pytest
content_type: scenario_question
tags: [python, testing, pytest, interview, scenarios, mocking, test-strategy]
---

# Scenario Questions — Python Testing with pytest

<article data-difficulty="junior">

## Junior: Write Tests for a Transform Function

**Scenario:** Write comprehensive tests for this data transformation function. Cover happy path, edge cases, and error handling.

```python
def transform_user_record(record: dict) -> dict:
    """Transform raw user record for warehouse loading."""
    if not record:
        raise ValueError("Record cannot be empty")
    
    required_fields = ["user_id", "email", "signup_date"]
    missing = [f for f in required_fields if f not in record]
    if missing:
        raise KeyError(f"Missing required fields: {missing}")
    
    return {
        "user_id": record["user_id"].strip(),
        "email": record["email"].lower().strip(),
        "signup_date": record["signup_date"][:10],  # Extract date part
        "is_premium": record.get("plan", "free") != "free",
        "full_name": f"{record.get('first_name', '')} {record.get('last_name', '')}".strip(),
    }
```

<details>
<summary>Solution</summary>

```python
import pytest
from pipeline.transforms import transform_user_record

class TestTransformUserRecord:
    """Comprehensive tests for user record transformation."""
    
    # Happy path tests
    def test_transforms_complete_record(self):
        record = {
            "user_id": " u123 ",
            "email": "  Alice@Example.COM  ",
            "signup_date": "2024-01-15T10:30:00Z",
            "plan": "premium",
            "first_name": "Alice",
            "last_name": "Smith",
        }
        
        result = transform_user_record(record)
        
        assert result["user_id"] == "u123"
        assert result["email"] == "alice@example.com"
        assert result["signup_date"] == "2024-01-15"
        assert result["is_premium"] is True
        assert result["full_name"] == "Alice Smith"
    
    def test_free_plan_user(self):
        record = {"user_id": "u1", "email": "a@b.com", "signup_date": "2024-01-15"}
        result = transform_user_record(record)
        assert result["is_premium"] is False
    
    # Edge cases
    def test_missing_optional_fields(self):
        record = {"user_id": "u1", "email": "test@test.com", "signup_date": "2024-01-15"}
        result = transform_user_record(record)
        assert result["full_name"] == ""
        assert result["is_premium"] is False
    
    def test_only_first_name(self):
        record = {"user_id": "u1", "email": "t@t.com", "signup_date": "2024-01-15", "first_name": "Alice"}
        result = transform_user_record(record)
        assert result["full_name"] == "Alice"
    
    # Error handling
    def test_empty_record_raises_valueerror(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            transform_user_record({})
    
    def test_none_record_raises_valueerror(self):
        with pytest.raises(ValueError):
            transform_user_record(None)
    
    def test_missing_required_field_raises_keyerror(self):
        with pytest.raises(KeyError, match="Missing required fields"):
            transform_user_record({"user_id": "u1", "email": "a@b.com"})
    
    # Parametrized edge cases
    @pytest.mark.parametrize("email,expected", [
        ("TEST@TEST.COM", "test@test.com"),
        ("  spaces@test.com  ", "spaces@test.com"),
        ("MiXeD@CaSe.Org", "mixed@case.org"),
    ])
    def test_email_normalization(self, email, expected):
        record = {"user_id": "u1", "email": email, "signup_date": "2024-01-15"}
        assert transform_user_record(record)["email"] == expected
```

</details>

</article>

<article data-difficulty="mid-level">

## Mid-Level: Mock an S3 Client for Testing

**Scenario:** Write tests for this S3 extraction function. You need to mock boto3 to test without real AWS credentials. Test: success case, empty bucket, and permission error.

```python
# pipeline/extract.py
import boto3
import json
from typing import List, Dict

def extract_events_from_s3(bucket: str, prefix: str, date: str) -> List[Dict]:
    """Read JSON event files from S3 for a given date partition."""
    s3 = boto3.client("s3")
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}dt={date}/")
    
    if "Contents" not in response:
        return []
    
    all_events = []
    for obj in response["Contents"]:
        if obj["Key"].endswith(".json"):
            file_response = s3.get_object(Bucket=bucket, Key=obj["Key"])
            content = file_response["Body"].read().decode("utf-8")
            events = json.loads(content)
            all_events.extend(events)
    
    return all_events
```

<details>
<summary>Solution</summary>

```python
import pytest
import json
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from pipeline.extract import extract_events_from_s3

class TestExtractEventsFromS3:
    
    @patch("pipeline.extract.boto3.client")
    def test_extracts_events_from_multiple_files(self, mock_boto):
        """Happy path — reads and merges multiple JSON files."""
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        
        # Mock list_objects_v2
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "events/dt=2024-01-15/part-001.json"},
                {"Key": "events/dt=2024-01-15/part-002.json"},
                {"Key": "events/dt=2024-01-15/_SUCCESS"},  # Not .json
            ]
        }
        
        # Mock get_object for each file
        file1_data = [{"id": "e1", "type": "click"}, {"id": "e2", "type": "view"}]
        file2_data = [{"id": "e3", "type": "purchase"}]
        
        mock_s3.get_object.side_effect = [
            {"Body": MagicMock(read=MagicMock(return_value=json.dumps(file1_data).encode()))},
            {"Body": MagicMock(read=MagicMock(return_value=json.dumps(file2_data).encode()))},
        ]
        
        result = extract_events_from_s3("my-bucket", "events/", "2024-01-15")
        
        assert len(result) == 3
        assert result[0]["id"] == "e1"
        assert result[2]["id"] == "e3"
        
        # Verify correct S3 calls
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="my-bucket", Prefix="events/dt=2024-01-15/"
        )
        assert mock_s3.get_object.call_count == 2  # Skipped _SUCCESS file
    
    @patch("pipeline.extract.boto3.client")
    def test_empty_partition_returns_empty_list(self, mock_boto):
        """No files for the given date."""
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}  # No "Contents" key
        
        result = extract_events_from_s3("my-bucket", "events/", "2024-01-15")
        
        assert result == []
        mock_s3.get_object.assert_not_called()
    
    @patch("pipeline.extract.boto3.client")
    def test_permission_error_propagates(self, mock_boto):
        """S3 permission error should bubble up."""
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        
        mock_s3.list_objects_v2.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "ListObjectsV2"
        )
        
        with pytest.raises(ClientError) as exc_info:
            extract_events_from_s3("restricted-bucket", "events/", "2024-01-15")
        
        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
```

</details>

</article>

<article data-difficulty="senior">

## Senior: Design a Test Strategy for a Data Pipeline

**Scenario:** You're building a pipeline that:
1. Extracts from 3 sources (Postgres, REST API, S3 files)
2. Transforms with PySpark (joins, aggregations, deduplication)
3. Loads to Redshift and updates a dashboard cache

Design a complete test strategy covering: test levels, fixture approach, CI/CD integration, and how you'd test data quality. Provide the conftest.py and one example test at each level.

<details>
<summary>Solution</summary>

```python
# Test Strategy Overview:
# Level 1: Unit tests (transforms) — fast, no deps, run on every PR
# Level 2: Component tests (connectors) — mocked external services
# Level 3: Integration tests (real DB in Docker) — run on main branch
# Level 4: Data quality tests (post-deploy) — run after production load

# ========================
# tests/conftest.py
# ========================
import pytest
import os
from unittest.mock import MagicMock

def pytest_collection_modifyitems(config, items):
    """Auto-skip integration tests unless --run-integration is passed."""
    if not config.getoption("--run-integration", default=False):
        skip = pytest.mark.skip(reason="Use --run-integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    is_ci = os.environ.get("CI") == "true"
    session = (
        SparkSession.builder
        .master("local[1]" if is_ci else "local[4]")
        .config("spark.sql.shuffle.partitions", "2")
        .appName("pipeline-tests")
        .getOrCreate()
    )
    yield session
    session.stop()

@pytest.fixture
def mock_postgres():
    """Mock Postgres connector for unit tests."""
    mock = MagicMock()
    mock.extract.return_value = iter([
        {"user_id": "u1", "name": "Alice", "region": "US"},
        {"user_id": "u2", "name": "Bob", "region": "EU"},
    ])
    return mock

@pytest.fixture
def mock_api_connector():
    """Mock API connector."""
    mock = MagicMock()
    mock.fetch_all.return_value = iter([
        {"user_id": "u1", "score": 85},
        {"user_id": "u2", "score": 92},
    ])
    return mock

# ========================
# Level 1: Unit Test (Transform Logic)
# tests/unit/test_transforms.py
# ========================
class TestJoinAndAggregate:
    def test_user_enrichment_join(self, spark):
        """Test the join logic between users and scores."""
        from pipeline.transforms import enrich_users
        
        users_df = spark.createDataFrame([
            ("u1", "Alice", "US"),
            ("u2", "Bob", "EU"),
        ], ["user_id", "name", "region"])
        
        scores_df = spark.createDataFrame([
            ("u1", 85),
            ("u2", 92),
        ], ["user_id", "score"])
        
        result = enrich_users(users_df, scores_df)
        
        assert result.count() == 2
        u1_row = result.filter(result.user_id == "u1").first()
        assert u1_row.score == 85
        assert u1_row.region == "US"
    
    def test_handles_missing_scores(self, spark):
        """Users without scores should still appear (left join)."""
        from pipeline.transforms import enrich_users
        
        users_df = spark.createDataFrame([("u1", "Alice", "US")], ["user_id", "name", "region"])
        scores_df = spark.createDataFrame([], "user_id STRING, score INT")
        
        result = enrich_users(users_df, scores_df)
        assert result.count() == 1
        assert result.first().score is None

# ========================
# Level 2: Component Test (Connector with Mock)
# tests/component/test_extraction.py
# ========================
from unittest.mock import patch

class TestExtractionOrchestrator:
    @patch("pipeline.extract.PostgresConnector")
    @patch("pipeline.extract.APIConnector")
    def test_extracts_from_all_sources(self, MockAPI, MockPG):
        from pipeline.extract import extract_all_sources
        
        MockPG.return_value.extract.return_value = [{"user_id": "u1"}]
        MockAPI.return_value.fetch_all.return_value = [{"user_id": "u1", "score": 90}]
        
        result = extract_all_sources(config={"date": "2024-01-15"})
        
        assert "users" in result
        assert "scores" in result
        MockPG.return_value.extract.assert_called_once()

# ========================
# Level 3: Integration Test
# tests/integration/test_full_pipeline.py
# ========================
@pytest.mark.integration
class TestPipelineIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, spark, tmp_path):
        self.spark = spark
        self.output_path = str(tmp_path / "output")
    
    def test_end_to_end_small_dataset(self):
        from pipeline.main import run_pipeline
        
        result = run_pipeline(
            spark=self.spark,
            source_config={"type": "local", "path": "tests/fixtures/"},
            target_path=self.output_path,
            execution_date="2024-01-15"
        )
        
        assert result.status == "success"
        assert result.records_loaded > 0
        
        # Verify output
        output_df = self.spark.read.parquet(self.output_path)
        assert output_df.count() == result.records_loaded

# ========================
# Level 4: Data Quality Tests (post-deploy)
# tests/data_quality/test_output_quality.py
# ========================
@pytest.mark.data_quality
class TestOutputDataQuality:
    def test_freshness(self, pipeline_output):
        max_ts = pipeline_output["updated_at"].max()
        hours_old = (datetime.now() - max_ts).total_seconds() / 3600
        assert hours_old < 24, f"Data is {hours_old:.1f} hours old"
    
    def test_completeness(self, pipeline_output):
        null_rates = pipeline_output.isnull().mean()
        for col in ["user_id", "email"]:
            assert null_rates[col] == 0, f"{col} has nulls"
```

**CI/CD Integration:**
- PR: `pytest -m "not integration and not data_quality"` (unit + component, <2 min)
- Merge to main: `pytest --run-integration` (full suite with Docker, ~5 min)
- Post-deploy: `pytest -m data_quality --execution-date=$(date +%Y-%m-%d)`

</details>

</article>
