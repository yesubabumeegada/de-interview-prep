---
title: "SCD Implementation - Fundamentals"
topic: etl-concepts
subtopic: scd-implementation
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [etl, scd, slowly-changing-dimensions, type-1, type-2, dimensional-modeling]
---

# SCD Implementation — Fundamentals

## What Are Slowly Changing Dimensions?

In dimensional modeling, a **Slowly Changing Dimension (SCD)** is a dimension table where attribute values change over time, but slowly. Common examples:
- Customer address changes
- Product category reclassification
- Employee department transfer
- Price tier changes

The challenge: **how do you track these changes in a data warehouse?** The answer depends on whether historical accuracy matters.

---

## SCD Type 0 — Fixed / Retain Original

Type 0 means the attribute never changes once loaded. Any updates to the source are ignored.

```sql
-- Type 0: Insert once; never update
INSERT INTO dim_country (country_code, country_name, region)
SELECT country_code, country_name, region
FROM source_countries
WHERE country_code NOT IN (SELECT country_code FROM dim_country);
```

**Use when:** Reference data that is truly immutable (ISO country codes, currency codes).

---

## SCD Type 1 — Overwrite

Type 1 overwrites the old value with the new one. **No history is retained.**

```sql
-- Type 1: Update in place (history lost)
UPDATE dim_customer
SET email = src.email,
    phone = src.phone
FROM staging_customers src
WHERE dim_customer.customer_id = src.customer_id;
```

```python
import pandas as pd
import sqlalchemy as sa

def scd1_upsert(df: pd.DataFrame, table: str, key: str, engine):
    """SCD Type 1: overwrite existing records, insert new ones."""
    cols        = list(df.columns)
    update_cols = [c for c in cols if c != key]
    updates     = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES ({', '.join(':' + c for c in cols)})
        ON CONFLICT ({key})
        DO UPDATE SET {updates}
    """
    with engine.begin() as conn:
        conn.execute(sa.text(sql), df.to_dict("records"))
```

**Pros:** Simple; no history overhead.
**Cons:** Historical reports show current values, not values at time of event. "What was the customer's address when they placed order X?" can't be answered.

---

## SCD Type 2 — Add New Row (Full History)

Type 2 preserves history by adding a new row for each change, with effective date and current-record markers.

### Table Structure

```sql
CREATE TABLE dim_customer_scd2 (
    customer_sk  BIGINT PRIMARY KEY,   -- Surrogate key (unique per version)
    customer_id  TEXT NOT NULL,        -- Business/natural key
    email        TEXT,
    phone        TEXT,
    address      TEXT,
    city         TEXT,
    effective_at TIMESTAMPTZ NOT NULL, -- When this version became active
    expired_at   TIMESTAMPTZ,          -- When this version became inactive (NULL = current)
    is_current   BOOLEAN NOT NULL DEFAULT TRUE
);
```

### Type 2 Insert Logic

```sql
-- Step 1: Close the previous version of changed records
UPDATE dim_customer_scd2
SET   is_current  = FALSE,
      expired_at  = :change_time
WHERE customer_id IN (
    SELECT src.customer_id
    FROM staging_customers src
    JOIN dim_customer_scd2 tgt
        ON src.customer_id = tgt.customer_id
       AND tgt.is_current = TRUE
    WHERE src.email != tgt.email
       OR src.phone != tgt.phone
       OR src.address != tgt.address
)
AND is_current = TRUE;

-- Step 2: Insert new versions for changed records
INSERT INTO dim_customer_scd2
    (customer_sk, customer_id, email, phone, address, effective_at, expired_at, is_current)
SELECT
    nextval('customer_sk_seq'),   -- Generate new surrogate key
    src.customer_id,
    src.email,
    src.phone,
    src.address,
    :change_time,
    NULL,
    TRUE
FROM staging_customers src
JOIN dim_customer_scd2 prev
    ON src.customer_id = prev.customer_id
   AND prev.is_current = FALSE
   AND prev.expired_at = :change_time
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer_scd2 curr
    WHERE curr.customer_id = src.customer_id
      AND curr.is_current = TRUE
);

-- Step 3: Insert brand-new customers (no existing record)
INSERT INTO dim_customer_scd2
    (customer_sk, customer_id, email, phone, address, effective_at, expired_at, is_current)
SELECT
    nextval('customer_sk_seq'),
    src.customer_id, src.email, src.phone, src.address,
    :change_time, NULL, TRUE
FROM staging_customers src
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer_scd2 d
    WHERE d.customer_id = src.customer_id
);
```

### Querying Type 2 Dimensions

```sql
-- Get customer's address at the time of their order (point-in-time query)
SELECT
    o.order_id,
    o.total_usd,
    c.email,
    c.city AS city_at_order_time
FROM orders o
JOIN dim_customer_scd2 c
    ON o.customer_id = c.customer_id
   AND o.created_at BETWEEN c.effective_at AND COALESCE(c.expired_at, NOW())
WHERE o.order_id = 'ORD-001';

-- Get current customer record
SELECT * FROM dim_customer_scd2
WHERE customer_id = 'CUST-001'
  AND is_current = TRUE;
```

---

## SCD Type 3 — Add Attribute Column

Type 3 adds a "previous value" column — stores current and one prior value only.

```sql
CREATE TABLE dim_customer_scd3 (
    customer_id   TEXT PRIMARY KEY,
    current_email TEXT,
    prev_email    TEXT,          -- Only the immediately previous value
    email_changed_at TIMESTAMPTZ
);

-- Update: shift current to previous, insert new current
UPDATE dim_customer_scd3
SET prev_email    = current_email,
    current_email = :new_email,
    email_changed_at = NOW()
WHERE customer_id = :cid;
```

**Pros:** Maintains one historical value without extra rows.
**Cons:** Only one level of history; older history is lost.

---

## SCD Comparison Summary

| SCD Type | History Preserved | Storage Overhead | Query Complexity | Best For |
|---|---|---|---|---|
| Type 0 | None | Minimal | Low | Immutable reference data |
| Type 1 | None | Minimal | Low | Error corrections, soft metadata |
| Type 2 | Full | High | Medium | Analytics requiring point-in-time accuracy |
| Type 3 | One version | Low | Low | Tracking "current vs. previous" |
| Type 4 | Full (in mini-dim) | Medium | Medium | Rapidly changing attributes |
| Type 6 (hybrid) | Full | High | Medium | Combined Type 1+2+3 |

---

## Surrogate Keys in SCD

Surrogate keys (auto-generated integers or UUIDs) are essential for SCD Type 2:

```python
import hashlib

def generate_surrogate_key(*args) -> str:
    """
    Deterministic surrogate key from business key + effective date.
    Each unique (customer_id, effective_at) pair gets a stable SK.
    """
    composite = "|".join(str(a) for a in args)
    return hashlib.md5(composite.encode()).hexdigest()

# Example: sk for customer CUST-001's version starting 2024-01-15
sk = generate_surrogate_key("CUST-001", "2024-01-15T00:00:00")
```

---

## Interview Tips

> **Tip 1:** Always ask the interviewer: "Does history matter?" If yes, SCD Type 2. If only current state matters, Type 1. This question often reveals the actual business requirement behind the technical question.

> **Tip 2:** SCD Type 2's biggest operational challenge is the join: to get point-in-time accuracy, you must join on both the natural key AND the effective date range (`BETWEEN effective_at AND expired_at`). This is the query pattern to memorize.

> **Tip 3:** `is_current = TRUE` is a denormalized convenience flag. It makes current-record queries fast and simple. Without it, you'd need `expired_at IS NULL` or `expired_at > NOW()` — equivalent but slightly less readable.

> **Tip 4:** Surrogate keys (not business keys) are the primary key in Type 2 dimensions. This allows the same customer to appear multiple times with different surrogate keys — one per version.

> **Tip 5:** SCD Type 6 = 1+2+3 combined: maintain full history (Type 2 rows), overwrite selected "current" attributes on all historical rows (Type 1), and store a "previous value" column (Type 3). Know this exists even if you rarely implement it.
