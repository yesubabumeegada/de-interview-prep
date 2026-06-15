---
title: "ETL Interview Practice Scenarios"
description: "Hands-on ETL design scenarios for interview preparation — from basic pipeline design to fault-tolerant multi-source ETL systems."
content_type: scenario_question
topic: etl-concepts
subtopic: interview-scenarios
tags: [ETL, pipeline-design, CDC, fault-tolerance, interview-practice, system-design]
---

<article data-difficulty="junior">

## Scenario: Design a Basic ETL Pipeline for E-Commerce Orders

You're joining a small e-commerce startup as their first data engineer. They have a PostgreSQL database with an `orders` table. The business analyst needs a daily summary of orders in a Google Sheets-friendly CSV, broken down by product category and region. Orders are inserted but never updated after creation.

**The orders table:**
```sql
CREATE TABLE orders (
  order_id     SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL,
  product_id   INT NOT NULL,
  category     VARCHAR(50),
  region       VARCHAR(30),
  amount       DECIMAL(10,2),
  created_at   TIMESTAMP NOT NULL
);
```

**Requirements:**
- Daily refresh (run at 06:00 UTC)
- Summarize: total orders, total revenue, by category and region
- Output: CSV file with yesterday's summary
- Should be safe to re-run if it fails

**Task:**
1. Sketch the pipeline steps (Extract → Transform → Load)
2. Write the SQL for the transformation
3. Write pseudocode or Python for the full pipeline
4. How do you make this idempotent?

<details>
<summary>✅ Solution</summary>

**1. Pipeline Steps**

```
Extract:    SELECT from orders WHERE created_at >= yesterday AND < today
Transform:  GROUP BY category, region — compute total_orders and total_revenue
Load:       Write to CSV file named orders_summary_YYYY-MM-DD.csv
```

**2. Transformation SQL**

```sql
-- Run with yesterday's date substituted in
SELECT
  category,
  region,
  COUNT(*)           AS total_orders,
  SUM(amount)        AS total_revenue,
  AVG(amount)        AS avg_order_value,
  MIN(created_at)    AS first_order_at,
  MAX(created_at)    AS last_order_at
FROM orders
WHERE created_at >= '2024-01-14 00:00:00'   -- yesterday start
  AND created_at <  '2024-01-15 00:00:00'   -- today start
GROUP BY category, region
ORDER BY total_revenue DESC;
```

**3. Full Python Pipeline**

```python
import psycopg2
import csv
from datetime import datetime, timedelta

def run_daily_summary_pipeline(run_date: datetime = None):
    """
    Extract orders for yesterday, summarize, and write to CSV.
    Safe to re-run: overwrites the same output file.
    """
    if run_date is None:
        run_date = datetime.utcnow().date()
    
    yesterday = run_date - timedelta(days=1)
    
    # Extract + Transform: run SQL in PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="ecommerce",
        user="etl_user",
        password="secret"
    )
    
    query = """
        SELECT
          category,
          region,
          COUNT(*) AS total_orders,
          SUM(amount) AS total_revenue,
          AVG(amount) AS avg_order_value
        FROM orders
        WHERE created_at >= %(start)s AND created_at < %(end)s
        GROUP BY category, region
        ORDER BY total_revenue DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query, {
            "start": yesterday,
            "end": run_date
        })
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    conn.close()
    
    # Load: write to CSV (overwrites if exists — idempotent)
    output_file = f"orders_summary_{yesterday}.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows([dict(zip(columns, row)) for row in rows])
    
    print(f"Written {len(rows)} rows to {output_file}")
    return output_file

if __name__ == "__main__":
    run_daily_summary_pipeline()
```

**4. Idempotency**

This pipeline is idempotent because:
- The output CSV is named with the date — re-running overwrites the same file
- The SQL query reads the same date range every time (deterministic)
- Running twice produces the same output file with the same content

If we were loading to a database instead of CSV, we'd make it idempotent by:
```sql
-- Option 1: Delete and re-insert
DELETE FROM orders_daily_summary WHERE summary_date = '2024-01-14';
INSERT INTO orders_daily_summary ...

-- Option 2: UPSERT on (category, region, summary_date)
INSERT INTO orders_daily_summary (category, region, summary_date, total_orders, total_revenue)
VALUES (...)
ON CONFLICT (category, region, summary_date) DO UPDATE
  SET total_orders = EXCLUDED.total_orders,
      total_revenue = EXCLUDED.total_revenue;
```

</details>

</article>

<article data-difficulty="mid">

## Scenario: Handle CDC from MySQL to Snowflake

You're a data engineer at a fintech company. The core banking system uses MySQL. You need to replicate the `accounts` table to Snowflake in near-real-time (< 5 minute lag) to power risk analytics dashboards.

**The MySQL accounts table:**
```sql
CREATE TABLE accounts (
  account_id   VARCHAR(36) PRIMARY KEY,
  customer_id  VARCHAR(36) NOT NULL,
  balance      DECIMAL(15,2) NOT NULL,
  status       ENUM('active','suspended','closed') NOT NULL,
  account_type ENUM('checking','savings','credit') NOT NULL,
  opened_at    DATETIME NOT NULL,
  updated_at   DATETIME NOT NULL,
  closed_at    DATETIME NULL
);
```

**Constraints:**
- Accounts are updated frequently (balance changes on every transaction)
- Accounts can be closed (status → 'closed', closed_at populated)
- You must detect both updates AND soft deletes (closed accounts)
- Snowflake target must always reflect the current state

**Task:**
1. Recommend and justify a CDC approach
2. Describe the Kafka topic structure
3. Write the Snowflake MERGE statement to apply CDC events
4. How do you handle out-of-order CDC events?

<details>
<summary>✅ Solution</summary>

**1. CDC Approach: Debezium Log-Based CDC**

Timestamp-based CDC is insufficient here because:
- High update frequency means we need sub-minute latency (binlog gives < 1s)
- Closed accounts need to be replicated (soft delete — status change, not hard delete)
- Balance changes are critical financial data — we can't afford to miss any

**Recommended approach: Debezium → Kafka → Kafka Connect Snowflake Sink**

```yaml
# Debezium MySQL connector config
{
  "name": "accounts-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql.banking.internal",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${MYSQL_PASSWORD}",
    "database.server.id": "12345",
    "topic.prefix": "banking",
    "database.include.list": "core_banking",
    "table.include.list": "core_banking.accounts",
    "include.schema.changes": "true",
    "snapshot.mode": "initial",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "decimal.handling.mode": "string"
  }
}
```

**2. Kafka Topic Structure**

```
Topic: banking.core_banking.accounts
Partitions: 20 (partitioned by account_id for ordering)
Retention: 7 days
Key: account_id (JSON)
Value: Debezium envelope (JSON)
```

**Debezium envelope format:**
```json
{
  "op": "u",
  "before": {
    "account_id": "acc-123", "balance": 5000.00, "status": "active", "updated_at": 1705312800000
  },
  "after": {
    "account_id": "acc-123", "balance": 4500.00, "status": "active", "updated_at": 1705312860000
  },
  "source": {
    "db": "core_banking", "table": "accounts",
    "ts_ms": 1705312860500, "file": "mysql-bin.000012", "pos": 98234
  }
}
```

**3. Snowflake MERGE to Apply CDC Events**

```sql
-- Load CDC events to staging (Kafka Connect Snowflake sink writes here)
CREATE OR REPLACE TABLE accounts_cdc_staging (
  account_id    VARCHAR(36),
  customer_id   VARCHAR(36),
  balance       NUMBER(15,2),
  status        VARCHAR(20),
  account_type  VARCHAR(20),
  opened_at     TIMESTAMP_NTZ,
  updated_at    TIMESTAMP_NTZ,
  closed_at     TIMESTAMP_NTZ,
  cdc_op        VARCHAR(1),      -- 'c' (create), 'u' (update), 'd' (delete)
  cdc_ts_ms     BIGINT,          -- Binlog timestamp for ordering
  _kafka_offset BIGINT
);

-- Apply CDC events to target table
MERGE INTO accounts_current AS target
USING (
  -- Deduplicate: keep only the latest event per account_id
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY account_id
             ORDER BY cdc_ts_ms DESC
           ) AS rn
    FROM accounts_cdc_staging
    WHERE cdc_op IN ('c', 'u', 'r')  -- Create, Update, Read (snapshot)
  )
  WHERE rn = 1
) AS source
ON target.account_id = source.account_id

-- Update if source event is newer than what we have
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
    customer_id  = source.customer_id,
    balance      = source.balance,
    status       = source.status,
    updated_at   = source.updated_at,
    closed_at    = source.closed_at,
    _last_cdc_ts = source.cdc_ts_ms

-- Insert new accounts
WHEN NOT MATCHED THEN
  INSERT (account_id, customer_id, balance, status, account_type,
          opened_at, updated_at, closed_at, _last_cdc_ts)
  VALUES (source.account_id, source.customer_id, source.balance,
          source.status, source.account_type,
          source.opened_at, source.updated_at, source.closed_at,
          source.cdc_ts_ms);
```

**4. Handling Out-of-Order CDC Events**

Because Kafka partitions by `account_id`, all events for one account arrive in binlog order within the partition. Cross-partition ordering is not guaranteed but doesn't matter since different accounts are independent.

The key guard is the `source.updated_at > target.updated_at` condition in the MERGE:

```sql
-- If a delayed event arrives with an older updated_at, this condition is FALSE
-- → the WHEN MATCHED branch is skipped
-- → the stale event is ignored
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE ...
```

For the staging deduplication step:
```sql
-- Use cdc_ts_ms (binlog position timestamp) not updated_at for deduplication
-- This handles the edge case where two updates happen in the same millisecond
ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY cdc_ts_ms DESC) AS rn
```

If Kafka Connect delivers duplicate messages (at-least-once), the MERGE is idempotent — same event applied twice produces the same result.

</details>

</article>

<article data-difficulty="senior">

## Scenario: Design a Fault-Tolerant Multi-Source ETL System

You're the lead data engineer at a healthcare analytics company. You need to design an ETL system with the following requirements:

**Sources:**
- 5 hospital EMR systems (Epic, Cerner) — batch file exports to S3 every 4 hours
- 1 insurance claims API — REST API, paginated, 100K records/day
- 1 lab results system — Kafka stream, 10K events/hour

**Target:** A unified `patient_encounters` fact table in Snowflake, used for:
- Clinical dashboards (8-hour freshness SLO)
- Regulatory reporting (must be complete — no data loss tolerance)
- ML feature engineering (daily full refresh of features)

**Constraints:**
- Each source has a different schema — normalization required
- Patient IDs differ across systems — must link using a probabilistic matching service
- PHI (protected health information) must be masked before entering Snowflake
- Full audit trail required: what data came from which source, when
- If one source fails, other sources must continue processing

**Task:**
1. Design the full architecture with fault isolation
2. Explain how you handle PHI masking before data enters Snowflake
3. Design the patient ID linking strategy
4. How do you ensure regulatory completeness (zero data loss)?
5. How do you handle schema differences across 5 EMR systems?

<details>
<summary>✅ Solution</summary>

**1. Architecture with Fault Isolation**

```
┌──────────────────────────────────────────────────────────────────────┐
│  SOURCE INGESTION (per-source isolation)                             │
│                                                                      │
│  S3 (EMR files) ──► per-source Glue jobs (5 separate, independent)  │
│  Claims API ──────► API extractor Lambda (retries, DLQ)             │
│  Lab Kafka ────────► Kafka consumer (Flink, dedicated consumer group)│
└────────────────────────┬─────────────────────────────────────────────┘
                         │ Each source writes to its own raw S3 prefix
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PHI MASKING LAYER (runs BEFORE data leaves VPC)                    │
│                                                                      │
│  AWS Lambda or containerized Python jobs                             │
│  Mask: SSN, DOB, name, address, phone → tokenized references        │
│  Store mapping in: AWS Secrets Manager / dedicated PHI vault        │
│  Output: masked records to "clean" S3 prefix                        │
└────────────────────────┬─────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  NORMALIZATION + PATIENT LINKING                                     │
│                                                                      │
│  Spark job: normalize each source to canonical schema                │
│  Patient linker: call probabilistic matching service API             │
│  Output: normalized records with unified patient_id                  │
└────────────────────────┬─────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SNOWFLAKE LOAD                                                      │
│                                                                      │
│  Raw layer (one table per source)                                    │
│  Staging layer (normalized, linked)                                  │
│  Gold layer: patient_encounters_fact (MERGE with audit columns)      │
└──────────────────────────────────────────────────────────────────────┘
```

**Fault Isolation:** Each source has its own ingestion job, its own S3 prefix, and its own Airflow DAG. A failure in the Epic EMR job does not block Cerner, Claims, or Lab data. The final MERGE into `patient_encounters_fact` only includes sources that completed successfully for each batch window.

**2. PHI Masking Before Snowflake**

```python
import hashlib
import boto3

class PHIMasker:
    """
    Mask PHI fields before data leaves the secure ETL VPC.
    Uses deterministic tokenization (same input → same token)
    so patient records can still be joined without exposing PHI.
    """
    
    SALT = os.environ["PHI_MASKING_SALT"]  # From AWS Secrets Manager
    
    PHI_FIELDS = {
        "patient_ssn":     "tokenize",
        "patient_dob":     "generalize_to_year",   # Keep year, drop month/day
        "patient_name":    "hash",
        "patient_address": "drop",                  # Not needed downstream
        "patient_phone":   "tokenize",
        "patient_email":   "hash",
    }
    
    def mask_record(self, record: dict) -> dict:
        masked = dict(record)
        for field, strategy in self.PHI_FIELDS.items():
            if field not in record:
                continue
            if strategy == "tokenize":
                masked[field] = self._tokenize(record[field])
            elif strategy == "hash":
                masked[field] = self._hash(record[field])
            elif strategy == "generalize_to_year":
                masked[field] = str(record[field])[:4]  # Keep year only
            elif strategy == "drop":
                del masked[field]
        return masked
    
    def _tokenize(self, value: str) -> str:
        """Deterministic token: same input always produces same output."""
        combined = f"{self.SALT}:{value}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    
    def _hash(self, value: str) -> str:
        combined = f"{self.SALT}:{value}"
        return hashlib.sha256(combined.encode()).hexdigest()
```

The masking step runs inside the VPC, before any data is written to the "clean" S3 bucket that Snowflake's external stage reads from.

**3. Patient ID Linking Strategy**

Each source uses a different patient identifier (MRN, insurance_id, etc.). We need a unified `patient_id` for cross-source analysis.

```python
class PatientLinker:
    """
    Call a probabilistic matching service to link patient records
    across sources using non-PHI identifiers.
    """
    
    def __init__(self, matching_service_url: str):
        self.url = matching_service_url
    
    def link_batch(self, records: list[dict]) -> list[dict]:
        """
        Submit a batch of records to the matching service.
        Service uses: name_hash + dob_year + zip_code_partial for matching.
        Returns records with unified patient_id added.
        """
        payload = [{
            "source_system": r["source_system"],
            "source_patient_id": r["source_patient_id"],
            "name_hash": r["patient_name"],   # Already hashed
            "dob_year": r["patient_dob"],     # Already generalized to year
            "zip_partial": r.get("zip_code", "")[:3]  # First 3 digits only
        } for r in records]
        
        response = requests.post(
            f"{self.url}/link/batch",
            json={"records": payload},
            timeout=30
        )
        response.raise_for_status()
        
        linked = response.json()["results"]
        
        # Merge unified_patient_id back into records
        for record, link_result in zip(records, linked):
            record["patient_id"] = link_result["unified_patient_id"]
            record["link_confidence"] = link_result["confidence_score"]
            record["is_new_patient"] = link_result["is_new_patient"]
        
        return records
```

Store all linking decisions in an audit table:
```sql
CREATE TABLE patient_id_links (
  unified_patient_id  VARCHAR(36) NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_patient_id   VARCHAR(100) NOT NULL,
  confidence_score    DECIMAL(4,3),
  linked_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_system, source_patient_id)
);
```

**4. Regulatory Completeness (Zero Data Loss)**

For regulatory reporting, completeness is non-negotiable. Strategy:

```python
class CompletenessValidator:
    """
    Before each load window closes, verify all expected records arrived.
    Block downstream reporting if completeness is not met.
    """
    
    def validate_emr_completeness(self, source: str, batch_date: str) -> bool:
        # Compare file manifest (what S3 received) against expected file list
        expected_files = get_expected_file_manifest(source, batch_date)
        actual_files = list_s3_objects(f"s3://raw/{source}/{batch_date}/")
        
        missing = set(expected_files) - set(actual_files)
        if missing:
            alert_on_call(f"Missing files for {source}/{batch_date}: {missing}")
            return False
        
        # Compare record counts against source system API
        expected_count = call_source_api_count(source, batch_date)
        actual_count = count_records_in_raw(source, batch_date)
        
        if actual_count < expected_count * 0.999:  # Allow 0.1% for late files
            alert_on_call(
                f"Completeness failure: {source}/{batch_date} "
                f"expected {expected_count}, got {actual_count}"
            )
            return False
        
        return True
    
    def hold_regulatory_reports(self, batch_date: str) -> list[str]:
        """Return list of sources that have not passed completeness check."""
        incomplete = []
        for source in ALL_SOURCES:
            if not self.validate_emr_completeness(source, batch_date):
                incomplete.append(source)
        return incomplete
```

Regulatory reports are blocked until all sources pass completeness checks. Clinical dashboards (8-hour SLO) can proceed with available data and show completeness indicators.

**5. Handling Schema Differences Across 5 EMR Systems**

```python
# Canonical schema: all sources must map to this
CANONICAL_ENCOUNTER_SCHEMA = {
    "encounter_id":        str,
    "patient_id":          str,   # Unified (from linker)
    "encounter_date":      date,
    "encounter_type":      str,   # 'inpatient' | 'outpatient' | 'emergency'
    "primary_diagnosis":   str,   # ICD-10 code
    "attending_physician": str,   # Tokenized
    "facility_id":         str,
    "source_system":       str,
    "source_encounter_id": str,   # Original ID from source
    "_ingested_at":        datetime,
    "_batch_id":           str,
}

# Per-source mapping
SOURCE_SCHEMA_MAPPINGS = {
    "epic": {
        "PAT_ENC_CSN_ID":    "encounter_id",
        "HSP_ACCOUNT_ID":    "source_encounter_id",
        "CONTACT_DATE":      "encounter_date",
        "ENC_TYPE_TITLE":    "encounter_type",
        "PRIM_DX_CODE":      "primary_diagnosis",
        "BILL_ATTEND_PROV":  "attending_physician",
        "LOC_ID":            "facility_id",
    },
    "cerner": {
        "encounter_id":      "source_encounter_id",
        "service_date":      "encounter_date",
        "encounter_class":   "encounter_type",
        "diagnosis_code_1":  "primary_diagnosis",
        "primary_provider":  "attending_physician",
        "organization_id":   "facility_id",
    }
    # ... other sources
}

def normalize_to_canonical(records: list[dict], source: str) -> list[dict]:
    mapping = SOURCE_SCHEMA_MAPPINGS[source]
    normalized = []
    for record in records:
        canonical = {"source_system": source}
        for src_field, canonical_field in mapping.items():
            canonical[canonical_field] = record.get(src_field)
        canonical["_ingested_at"] = datetime.utcnow()
        canonical["_batch_id"] = generate_batch_id()
        normalized.append(canonical)
    return normalized
```

Schema changes are handled by:
1. Failing fast with a clear error if a required field is missing
2. Appending new fields to the canonical schema via a change process
3. Using Delta Lake's schema evolution for the raw layer (`mergeSchema=true`)
4. Never silently dropping unknown source fields — log them for review

</details>

</article>
