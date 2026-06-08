---
title: "PII & Compliance — Real World"
topic: data-governance
subtopic: pii-and-compliance
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [pii, gdpr, ccpa, masking, production-patterns]
---

# PII & Compliance — Real World Patterns

## Pattern 1: Non-Production Data Masking Pipeline

Replace PII with realistic synthetic data in dev/staging:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

spark = SparkSession.builder.appName("pii_masking").getOrCreate()

# Table-level masking config
MASKING_CONFIG = {
    "gold.customers": {
        "email":         "hash",
        "first_name":    "fake_name",
        "last_name":     "fake_name",
        "phone":         "fake_phone",
        "address":       "fake_address",
        "date_of_birth": "generalize_year",  # Keep year only
        "customer_id":   "keep",             # Not PII — needed for joins
    },
    "gold.orders": {
        "customer_email": "hash",
        "billing_name":   "fake_name",
        "shipping_address": "fake_address",
        "order_id":       "keep",
        "amount":         "keep",
    },
}

# Register UDFs for masking
@F.udf(StringType())
def hash_udf(val):
    if val is None:
        return None
    return hashlib.sha256(val.lower().encode()).hexdigest()[:16]

@F.udf(StringType())
def fake_email_udf(val):
    if val is None:
        return None
    from faker import Faker
    return Faker().email()

@F.udf(StringType())
def fake_name_udf(val):
    if val is None:
        return None
    from faker import Faker
    return Faker().name()

@F.udf(StringType())
def generalize_year_udf(val):
    """Keep year only from date string."""
    if val is None:
        return None
    return str(val)[:4] + "-01-01"

MASKING_UDFS = {
    "hash": hash_udf,
    "fake_email": fake_email_udf,
    "fake_name": fake_name_udf,
    "generalize_year": generalize_year_udf,
    "keep": None,
}

def mask_table_for_non_prod(table_name: str, source_env: str = "prod", target_env: str = "staging"):
    """Copy a table to a non-prod environment with PII masked."""
    config = MASKING_CONFIG.get(table_name)
    if not config:
        raise ValueError(f"No masking config for {table_name}. Add to MASKING_CONFIG before processing.")
    
    # Read from prod
    df = spark.read.table(f"{source_env}.{table_name}")
    
    # Apply column-level masking
    for col_name, strategy in config.items():
        if strategy == "keep" or strategy is None:
            continue
        udf_fn = MASKING_UDFS.get(strategy)
        if udf_fn is None:
            raise ValueError(f"Unknown masking strategy: {strategy}")
        df = df.withColumn(col_name, udf_fn(F.col(col_name)))
    
    # Write to staging
    df.write.mode("overwrite").saveAsTable(f"{target_env}.{table_name}")
    print(f"Masked {table_name} from {source_env} → {target_env}")

# Run for all configured tables
for table in MASKING_CONFIG:
    mask_table_for_non_prod(table)
```

---

## Pattern 2: Automated GDPR Erasure Pipeline (Airflow)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def process_erasure_requests(**context):
    """Process all pending erasure requests."""
    import sqlalchemy as sa
    
    engine = sa.create_engine("postgresql://...")
    
    with engine.connect() as conn:
        pending = conn.execute(sa.text("""
            SELECT request_id, subject_email, received_at
            FROM erasure_requests
            WHERE status = 'pending'
              AND received_at >= NOW() - INTERVAL '30 days'  -- GDPR: 30 day window
            ORDER BY received_at ASC
        """)).fetchall()
    
    from pii_processor import RightToErasureProcessor
    processor = RightToErasureProcessor(engine, spark, notification_client)
    
    results = []
    for req in pending:
        try:
            result = processor.process_erasure(req.subject_email, req.request_id)
            
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    UPDATE erasure_requests
                    SET status = 'completed', completed_at = NOW(), result_detail = :detail
                    WHERE request_id = :id
                """), {"detail": str(result), "id": req.request_id})
            
            results.append({"id": req.request_id, "status": "completed"})
        except Exception as e:
            print(f"Erasure failed for {req.request_id}: {e}")
            results.append({"id": req.request_id, "status": "failed", "error": str(e)})
    
    print(f"Processed {len(results)} erasure requests")
    return results

def check_gdpr_sla(**context):
    """Alert if any erasure request is approaching 30-day GDPR deadline."""
    import sqlalchemy as sa
    
    engine = sa.create_engine("postgresql://...")
    
    with engine.connect() as conn:
        at_risk = conn.execute(sa.text("""
            SELECT request_id, subject_email, received_at,
                   30 - DATE_DIFF('day', received_at, NOW()) AS days_remaining
            FROM erasure_requests
            WHERE status = 'pending'
              AND DATE_DIFF('day', received_at, NOW()) >= 25  -- 5 days before deadline
        """)).fetchall()
    
    if at_risk:
        alert_message = "\n".join(
            f"  {r.request_id}: {r.days_remaining} days remaining (received {r.received_at.date()})"
            for r in at_risk
        )
        # Alert DPO
        notification_client.send(
            to="dpo@company.com",
            subject=f"[URGENT] {len(at_risk)} erasure requests approaching GDPR deadline",
            body=f"The following requests are approaching the 30-day GDPR limit:\n\n{alert_message}",
        )

with DAG(
    "gdpr_erasure_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",  # Daily at 8 AM
    default_args={"retries": 1},
    catchup=False,
) as dag:
    
    process = PythonOperator(
        task_id="process_erasure_requests",
        python_callable=process_erasure_requests,
    )
    
    check_sla = PythonOperator(
        task_id="check_gdpr_sla",
        python_callable=check_gdpr_sla,
    )
    
    process >> check_sla
```

---

## PII Compliance Anti-Patterns

| Anti-Pattern | Risk | Fix |
|---|---|---|
| PII in S3 key names | Logs expose PII | Use opaque IDs in file paths: `customers/id=12345/` not `customers/email=john@company.com/` |
| PII in pipeline logs | Log aggregation tools expose PII | Redact PII from logs before writing |
| Copying prod to dev without masking | Dev environment = compliance risk | Always run masking pipeline before dev copy |
| Storing raw PII in analytics tables | Every analyst can see PII | Tokenize at ingestion, not downstream |
| No DSAR response tracking | GDPR violation (30-day deadline) | Track all DSARs with SLA monitoring |
| Right to erasure not applied to backups | PII persists in backups after erasure | Coordinate erasure with backup retention policy |
