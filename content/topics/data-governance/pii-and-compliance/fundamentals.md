---
title: "PII & Compliance — Fundamentals"
topic: data-governance
subtopic: pii-and-compliance
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pii, gdpr, ccpa, compliance, data-privacy]
---

# PII & Compliance — Fundamentals

## What Is PII?

Personally Identifiable Information (PII) is any data that can identify a specific individual, directly or indirectly.

| Category | Examples |
|---|---|
| **Direct identifiers** | Name, email, SSN, phone number, passport number |
| **Quasi-identifiers** | ZIP code + birthdate + gender (linkable to individual) |
| **Sensitive PII** | Medical records, financial data, biometric data, sexual orientation |
| **Derived PII** | User behavior patterns that uniquely identify someone |

---

## Key Privacy Regulations

### GDPR (EU General Data Protection Regulation)
- **Scope:** Any company processing EU residents' data
- **Key rights:** Right to access, right to erasure (right to be forgotten), data portability
- **Penalties:** Up to 4% of global annual revenue or €20M (whichever is greater)
- **Lawful basis:** Must have a legal basis to process data (consent, contract, legitimate interest)

### CCPA (California Consumer Privacy Act)
- **Scope:** Businesses processing California residents' data (revenue > $25M or 100K+ consumers)
- **Key rights:** Right to know, right to delete, right to opt-out of data selling
- **Penalties:** Up to $7,500 per intentional violation

### HIPAA (US Health Insurance Portability and Accountability Act)
- **Scope:** Healthcare data (Protected Health Information / PHI)
- **Key requirement:** Strict access controls, audit logs, encryption

---

## PII Handling Principles

```python
# The 5 key principles for PII in data engineering:

# 1. Minimize: Only collect what you need
BAD:  SELECT * FROM users  # Pulls PII you don't need
GOOD: SELECT user_id, created_at FROM users  # Only needed columns

# 2. Classify: Tag all PII columns in your catalog
schema = {
    "table": "customers",
    "columns": [
        {"name": "email", "type": "string", "pii": True, "pii_type": "email"},
        {"name": "phone", "type": "string", "pii": True, "pii_type": "phone"},
        {"name": "customer_id", "type": "string", "pii": False},
    ]
}

# 3. Mask: Don't expose raw PII unless necessary
def mask_email(email: str) -> str:
    """Hash email for analytics — same hash = same user, but not reversible."""
    import hashlib
    return hashlib.sha256(email.lower().encode()).hexdigest()

# 4. Restrict: Enforce access controls
# (See access-control subtopic)

# 5. Retain: Delete after retention period expires
def is_past_retention(created_at, retention_years: int = 7) -> bool:
    from datetime import datetime, timedelta
    return created_at < datetime.utcnow() - timedelta(days=retention_years * 365)
```

---

## PII Detection

Automatically identify PII columns in datasets:

```python
import re
from typing import List, Dict

# Common PII patterns
PII_PATTERNS = {
    "email": re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
    "phone": re.compile(r"^\+?[\d\s\-().]{7,15}$"),
    "ssn": re.compile(r"^\d{3}-?\d{2}-?\d{4}$"),
    "credit_card": re.compile(r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$"),
    "ip_address": re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"),
}

# PII column name keywords
PII_COLUMN_KEYWORDS = [
    "email", "e_mail", "phone", "mobile", "ssn", "social_security",
    "first_name", "last_name", "full_name", "address", "street",
    "city", "zip", "postal", "dob", "birth", "credit_card", "card_number",
    "passport", "license", "ip_address", "user_id", "customer_id",
]

def detect_pii_columns(df, sample_size: int = 100) -> List[Dict]:
    """
    Heuristic PII detection:
    1. Check column names for known PII keywords
    2. Check sample values against PII patterns
    """
    results = []
    sample = df.head(sample_size)
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Name-based detection
        name_match = any(kw in col_lower for kw in PII_COLUMN_KEYWORDS)
        
        # Value-based detection
        value_match = None
        non_null = sample[col].dropna().astype(str)
        
        for pii_type, pattern in PII_PATTERNS.items():
            match_rate = non_null.apply(lambda v: bool(pattern.match(v))).mean()
            if match_rate > 0.8:  # 80% match = likely PII
                value_match = pii_type
                break
        
        if name_match or value_match:
            results.append({
                "column": col,
                "pii_type": value_match or "inferred_from_name",
                "confidence": "high" if (name_match and value_match) else "medium",
            })
    
    return results
```

---

## Data Subject Access Request (DSAR) — Right to Know

```python
import sqlalchemy as sa
from datetime import datetime

def handle_dsar(engine, subject_email: str) -> dict:
    """
    Respond to a Data Subject Access Request:
    Find all data held about a specific individual.
    """
    collected_data = {}
    
    # Tables that may contain PII (identified by catalog tagging)
    pii_tables = [
        ("gold.customers", "email"),
        ("gold.orders", "customer_email"),
        ("gold.events", "user_email"),
        ("gold.support_tickets", "requester_email"),
    ]
    
    with engine.connect() as conn:
        for table, email_col in pii_tables:
            try:
                rows = conn.execute(sa.text(
                    f"SELECT * FROM {table} WHERE {email_col} = :email LIMIT 1000"
                ), {"email": subject_email}).fetchall()
                
                if rows:
                    collected_data[table] = {
                        "row_count": len(rows),
                        "columns": list(rows[0]._mapping.keys()),
                        "data": [dict(r._mapping) for r in rows],
                    }
            except Exception as e:
                collected_data[table] = {"error": str(e)}
    
    return {
        "subject": subject_email,
        "request_date": datetime.utcnow().isoformat(),
        "tables_searched": len(pii_tables),
        "tables_with_data": len(collected_data),
        "data": collected_data,
    }
```

---

## Interview Tips

> **Tip 1:** "What is PII and give examples?" — Any data that can identify an individual. Direct: email, SSN, phone, name. Quasi-identifiers: ZIP+birthdate+gender combo. Sensitive: medical records, financial accounts. Derived: clickstream that uniquely identifies behavior pattern.

> **Tip 2:** "What's the difference between GDPR and CCPA?" — GDPR: EU law, broader scope (any company with EU users), strong enforcement (4% revenue fine), requires lawful basis for all processing, covers all residents. CCPA: California law, lighter (opt-out model), primarily a disclosure law. CCPA is less strict than GDPR.

> **Tip 3:** "What is a DSAR?" — Data Subject Access Request: a legal right (under GDPR/CCPA) for individuals to request all data an organization holds about them. You must respond within 30 days (GDPR) or 45 days (CCPA). Data engineers build DSAR fulfillment pipelines that search all PII-tagged tables for a subject's records.
