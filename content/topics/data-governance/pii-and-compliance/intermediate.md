---
title: "PII & Compliance — Intermediate"
topic: data-governance
subtopic: pii-and-compliance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pii, gdpr, anonymization, pseudonymization, right-to-erasure]
---

# PII & Compliance — Intermediate

## Anonymization vs. Pseudonymization

```
Anonymization:      Irreversible. Subject can never be re-identified.
                    GDPR no longer applies to truly anonymous data.
                    Hard in practice — quasi-identifiers can re-identify.

Pseudonymization:   Reversible with a key. PII replaced with pseudonym.
                    Still covered by GDPR, but lower risk if key is secured.
                    Example: customer_id replaces email; mapping held separately.
```

### Common Techniques

```python
import hashlib
import secrets
from typing import Optional

# 1. Hashing (deterministic pseudonymization)
def hash_pii(value: str, salt: str = "") -> str:
    """
    SHA-256 hash with optional salt.
    Deterministic: same email → same hash (useful for joining).
    Not reversible without rainbow tables.
    """
    return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()

# 2. Tokenization (reversible pseudonymization)
class TokenizationService:
    """Replace PII with random tokens. Mapping stored securely."""
    
    def __init__(self, token_store):  # Redis or encrypted DB
        self.store = token_store
    
    def tokenize(self, pii_value: str) -> str:
        """Replace PII with a random token."""
        # Check if already tokenized
        existing = self.store.get(f"pii:{pii_value}")
        if existing:
            return existing
        
        token = secrets.token_urlsafe(16)
        self.store.set(f"pii:{pii_value}", token)
        self.store.set(f"token:{token}", pii_value)  # Reverse lookup for DSAR
        return token
    
    def detokenize(self, token: str) -> Optional[str]:
        """Reverse lookup (only for authorized use cases like DSAR)."""
        return self.store.get(f"token:{token}")

# 3. Generalization (k-anonymity)
def generalize_age(age: int, bucket_size: int = 10) -> str:
    """Replace exact age with age range (reduces re-identification risk)."""
    lower = (age // bucket_size) * bucket_size
    return f"{lower}-{lower + bucket_size - 1}"

def generalize_zip(zip_code: str, digits_to_keep: int = 3) -> str:
    """Replace full ZIP with prefix (reduces geo-specificity)."""
    return zip_code[:digits_to_keep] + "*" * (len(zip_code) - digits_to_keep)

# 4. Data masking for non-production environments
def mask_for_non_prod(df, pii_columns: dict) -> "pd.DataFrame":
    """
    Mask PII columns for staging/dev environments.
    pii_columns: {column_name: masking_strategy}
    """
    import pandas as pd
    from faker import Faker
    fake = Faker()
    
    df = df.copy()
    for col, strategy in pii_columns.items():
        if strategy == "hash":
            df[col] = df[col].apply(lambda v: hash_pii(str(v)) if pd.notna(v) else v)
        elif strategy == "fake_email":
            df[col] = [fake.email() for _ in range(len(df))]
        elif strategy == "fake_name":
            df[col] = [fake.name() for _ in range(len(df))]
        elif strategy == "null":
            df[col] = None
    
    return df
```

---

## Right to Erasure (Right to Be Forgotten)

GDPR Article 17: Users can request deletion of their personal data:

```python
import sqlalchemy as sa
from datetime import datetime
from typing import List

class RightToErasureProcessor:
    """
    Handle GDPR right-to-erasure requests.
    
    Approach:
    - For mutable tables: DELETE or UPDATE to NULL
    - For immutable tables (Delta/Iceberg): pseudonymize then rewrite
    - For backups: schedule purge on backup expiry
    """
    
    def __init__(self, engine, spark, notification_client):
        self.engine = engine
        self.spark = spark
        self.notify = notification_client
    
    # Tables and how to identify the subject
    PII_TABLE_CONFIG = {
        "gold.customers": {"id_col": "email", "method": "delete"},
        "gold.orders": {"id_col": "customer_email", "method": "nullify", "nullify_cols": ["customer_email", "shipping_address"]},
        "gold.events": {"id_col": "user_email", "method": "delete"},
        "silver.customers": {"id_col": "email", "method": "delete"},
        "bronze.customer_raw": {"id_col": "email", "method": "delta_rewrite"},  # Immutable
    }
    
    def process_erasure(self, subject_email: str, request_id: str) -> dict:
        results = {"request_id": request_id, "subject": subject_email, "tables": {}}
        
        with self.engine.begin() as conn:
            for table, config in self.PII_TABLE_CONFIG.items():
                id_col = config["id_col"]
                method = config["method"]
                
                if method == "delete":
                    affected = conn.execute(sa.text(
                        f"DELETE FROM {table} WHERE {id_col} = :email"
                    ), {"email": subject_email}).rowcount
                    results["tables"][table] = {"method": "delete", "rows_affected": affected}
                
                elif method == "nullify":
                    null_set = ", ".join(f"{c} = NULL" for c in config["nullify_cols"])
                    affected = conn.execute(sa.text(
                        f"UPDATE {table} SET {null_set} WHERE {id_col} = :email"
                    ), {"email": subject_email}).rowcount
                    results["tables"][table] = {"method": "nullify", "rows_affected": affected}
                
                elif method == "delta_rewrite":
                    # For immutable Delta tables: rewrite without the subject's rows
                    self._delta_erasure(table, id_col, subject_email)
                    results["tables"][table] = {"method": "delta_rewrite", "status": "queued"}
        
        results["completed_at"] = datetime.utcnow().isoformat()
        
        # Notify the subject that erasure is complete
        self.notify.send(
            to=subject_email,
            subject="Your erasure request has been processed",
            body=f"Request {request_id} has been completed. Your data has been removed from our systems.",
        )
        
        return results
    
    def _delta_erasure(self, table: str, id_col: str, email: str):
        """Rewrite a Delta table excluding the subject's rows."""
        df = self.spark.read.format("delta").table(table)
        df_without_subject = df.filter(f"{id_col} != '{email}'")
        df_without_subject.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        
        # VACUUM to remove old files containing the subject's data
        self.spark.sql(f"VACUUM {table} RETAIN 0 HOURS")  # In practice: coordinate with backup policy
```

---

## Data Retention Policy Implementation

```python
from datetime import datetime, timedelta
import sqlalchemy as sa

RETENTION_POLICIES = {
    "bronze.*":   {"years": 7, "action": "delete"},      # Raw: 7 year regulatory requirement
    "silver.*":   {"years": 3, "action": "delete"},      # Processed: 3 years
    "gold.*":     {"years": 5, "action": "archive"},     # Business: 5 years, then archive
    "gold.events": {"years": 1, "action": "delete"},     # High-volume events: 1 year only
}

def enforce_retention(engine, table: str, date_col: str, policy_years: int, action: str) -> int:
    """Delete or archive rows older than the retention period."""
    cutoff = datetime.utcnow() - timedelta(days=policy_years * 365)
    
    with engine.begin() as conn:
        if action == "delete":
            result = conn.execute(sa.text(
                f"DELETE FROM {table} WHERE {date_col} < :cutoff"
            ), {"cutoff": cutoff})
            rows_deleted = result.rowcount
        elif action == "archive":
            archive_table = f"archive.{table.split('.')[-1]}"
            conn.execute(sa.text(
                f"INSERT INTO {archive_table} SELECT * FROM {table} WHERE {date_col} < :cutoff"
            ), {"cutoff": cutoff})
            result = conn.execute(sa.text(
                f"DELETE FROM {table} WHERE {date_col} < :cutoff"
            ), {"cutoff": cutoff})
            rows_deleted = result.rowcount
    
    print(f"Retention enforcement: {table} → {rows_deleted:,} rows {action}d (cutoff: {cutoff.date()})")
    return rows_deleted
```

---

## Interview Tips

> **Tip 1:** "What's the difference between anonymization and pseudonymization?" — Anonymization is irreversible (hash without key, generalization, k-anonymity). GDPR doesn't apply to truly anonymous data. Pseudonymization is reversible (tokenization, encryption): PII replaced by pseudonym, key stored separately. Still GDPR-regulated but lower risk. Most "anonymization" in practice is pseudonymization.

> **Tip 2:** "How do you handle right to erasure for immutable data (Delta Lake, S3)?" — Delta Lake: rewrite the table using Spark overwrite mode excluding the subject's rows, then VACUUM to remove old files. S3 Glacier: coordinate with backup lifecycle policy. Must also erase from snapshots and replicas within the 30-day GDPR response window. Log each erasure for audit.

> **Tip 3:** "What is k-anonymity?" — A property where each record in a dataset is indistinguishable from at least k-1 other records on quasi-identifier attributes (ZIP+age+gender). If k=5, you can't identify a specific individual from the combination of attributes. Used for releasing public datasets safely. More sophisticated: l-diversity (sensitive values diverse within group).
