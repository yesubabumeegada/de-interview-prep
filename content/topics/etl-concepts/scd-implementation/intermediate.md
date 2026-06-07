---
title: "SCD Implementation - Intermediate"
topic: etl-concepts
subtopic: scd-implementation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, scd, dbt-snapshots, merge, type-2, type-4, effective-date]
---

# SCD Implementation — Intermediate

## dbt Snapshots for SCD Type 2

dbt's snapshot feature automates SCD Type 2 maintenance.

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',     -- Use updated_at column to detect changes
        updated_at='updated_at',
        invalidate_hard_deletes=True  -- Expire records when source row is deleted
    )
}}

SELECT
    customer_id,
    email,
    phone,
    city,
    country,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

dbt automatically adds:
- `dbt_scd_id` — unique surrogate key per version
- `dbt_updated_at` — when dbt processed this version
- `dbt_valid_from` — effective_at equivalent
- `dbt_valid_to` — expired_at equivalent (NULL = current)

Run snapshots: `dbt snapshot`

### Using dbt Snapshots in Models

```sql
-- models/silver/customer_at_order_time.sql
SELECT
    o.order_id,
    o.customer_id,
    o.created_at AS order_time,
    c.email     AS email_at_order_time,
    c.city      AS city_at_order_time,
    c.country   AS country_at_order_time
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('customers_snapshot') }} c
    ON o.customer_id = c.customer_id
   AND o.created_at  >= c.dbt_valid_from
   AND (o.created_at < c.dbt_valid_to OR c.dbt_valid_to IS NULL)
```

---

## Full SCD Type 2 MERGE Implementation

```sql
-- Snowflake / BigQuery MERGE for SCD Type 2
-- Step 1: Detect changes
CREATE OR REPLACE TEMPORARY TABLE changed_records AS
SELECT
    src.customer_id,
    src.email,
    src.city,
    src.updated_at,
    MD5(CONCAT(src.email, '|', src.city)) AS src_hash,
    MD5(CONCAT(tgt.email, '|', tgt.city)) AS tgt_hash
FROM staging_customers src
JOIN dim_customer tgt
    ON src.customer_id = tgt.customer_id
   AND tgt.is_current = TRUE
WHERE MD5(CONCAT(src.email, '|', src.city)) != MD5(CONCAT(tgt.email, '|', tgt.city));

-- Step 2: Close old versions
UPDATE dim_customer
SET  is_current = FALSE,
     expired_at = CURRENT_TIMESTAMP
WHERE (customer_id, is_current) IN (
    SELECT customer_id, TRUE FROM changed_records
);

-- Step 3: Insert new versions for changes + new customers
INSERT INTO dim_customer (customer_sk, customer_id, email, city, effective_at, expired_at, is_current)
SELECT
    gen_random_uuid()::text AS customer_sk,
    src.customer_id,
    src.email,
    src.city,
    CURRENT_TIMESTAMP AS effective_at,
    NULL              AS expired_at,
    TRUE              AS is_current
FROM staging_customers src
WHERE src.customer_id IN (SELECT customer_id FROM changed_records)
   OR NOT EXISTS (
       SELECT 1 FROM dim_customer d WHERE d.customer_id = src.customer_id
   );
```

### Change Detection with Hash

Hashing multiple attributes into one value simplifies change detection:

```python
import hashlib
import pandas as pd

def add_row_hash(df: pd.DataFrame, cols_to_hash: list[str]) -> pd.DataFrame:
    """Add a hash column for change detection across multiple columns."""
    df["row_hash"] = df[cols_to_hash].apply(
        lambda row: hashlib.md5(
            "|".join(str(v) for v in row).encode()
        ).hexdigest(),
        axis=1
    )
    return df

# Usage
staging = add_row_hash(staging_df, ["email", "phone", "city", "country"])
target  = add_row_hash(target_df,  ["email", "phone", "city", "country"])

# Changed records: where hash differs
changed = staging.merge(target[["customer_id", "row_hash"]], on="customer_id", suffixes=("_src", "_tgt"))
changed = changed[changed["row_hash_src"] != changed["row_hash_tgt"]]
```

---

## SCD Type 4 — History Table

Type 4 separates current and historical records into two tables:

```sql
-- Current values (fast lookups)
CREATE TABLE dim_customer (
    customer_id TEXT PRIMARY KEY,
    email       TEXT,
    phone       TEXT,
    city        TEXT,
    updated_at  TIMESTAMPTZ
);

-- Historical values (all changes)
CREATE TABLE dim_customer_history (
    history_id  BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    email       TEXT,
    phone       TEXT,
    city        TEXT,
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ
);

-- On change: update dim_customer (current), insert to dim_customer_history (history)
WITH old AS (
    UPDATE dim_customer
    SET email = :new_email, phone = :new_phone, updated_at = NOW()
    WHERE customer_id = :cid
    RETURNING customer_id, email AS old_email, phone AS old_phone, updated_at AS changed_at
)
INSERT INTO dim_customer_history (customer_id, email, phone, valid_from, valid_to)
SELECT customer_id, old_email, old_phone, changed_at - INTERVAL '1 second', NOW()
FROM old;
```

---

## SCD Type 6 — Hybrid (1+2+3)

Type 6 = Type 2 (full history rows) + Type 1 (current value on all rows) + Type 3 (previous value):

```sql
CREATE TABLE dim_customer_scd6 (
    customer_sk     BIGINT PRIMARY KEY,
    customer_id     TEXT NOT NULL,

    -- Type 2: per-version values
    email_version   TEXT,    -- Email as of this version
    city_version    TEXT,

    -- Type 1: always reflects current value (updated on all rows)
    email_current   TEXT,
    city_current    TEXT,

    -- Type 3: previous value
    email_previous  TEXT,
    city_previous   TEXT,

    effective_at    TIMESTAMPTZ NOT NULL,
    expired_at      TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

-- When a customer changes their email:
BEGIN;

-- 1. Close old current row
UPDATE dim_customer_scd6
SET is_current = FALSE, expired_at = NOW()
WHERE customer_id = :cid AND is_current = TRUE;

-- 2. Insert new row (Type 2)
INSERT INTO dim_customer_scd6 (
    customer_sk, customer_id,
    email_version, city_version,
    email_current, city_current,
    email_previous, city_previous,
    effective_at, is_current
)
SELECT
    nextval('customer_sk_seq'), :cid,
    :new_email, :city,        -- Type 2: this version
    :new_email, :city,        -- Type 1: current
    old.email_version, old.city_version,  -- Type 3: previous
    NOW(), TRUE
FROM dim_customer_scd6 old
WHERE old.customer_id = :cid AND old.is_current = FALSE
  AND old.expired_at = NOW();

-- 3. Update ALL historical rows with the current value (Type 1 part)
UPDATE dim_customer_scd6
SET email_current = :new_email
WHERE customer_id = :cid;

COMMIT;
```

---

## Effective Date Management

Managing effective dates correctly is critical for point-in-time accuracy.

```python
from datetime import datetime, timezone

def get_effective_date(source_updated_at: datetime, batch_run_time: datetime) -> datetime:
    """
    Determine the effective date for a new SCD2 version.
    Options:
    1. Use source `updated_at` (most accurate — when the change happened in source)
    2. Use batch run time (when we loaded the change — introduces lag)
    3. Use a business-defined date
    """
    # Prefer source timestamp for historical accuracy
    return source_updated_at or batch_run_time

def is_same_day_change(old_effective: datetime, new_effective: datetime) -> bool:
    """
    Check if two changes happened on the same day.
    Used to decide whether to create a new row or update the existing one.
    """
    return old_effective.date() == new_effective.date()
```

### Handling Multiple Changes in the Same Batch

```python
def apply_scd2_batch(
    changes: pd.DataFrame,    # Multiple changes per customer in one batch
    current_dim: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply SCD Type 2 when a customer may have multiple changes in one batch.
    Sort by timestamp; only the latest version is 'current'.
    """
    # Sort by business key + change timestamp
    changes = changes.sort_values(["customer_id", "updated_at"])

    # For each customer, only the LAST change in the batch becomes the current row
    latest  = changes.groupby("customer_id").last().reset_index()

    # All intermediate states (non-latest changes) become expired rows immediately
    intermediate = changes.merge(
        latest[["customer_id", "updated_at"]],
        on="customer_id",
        suffixes=("", "_latest")
    )
    intermediate = intermediate[
        intermediate["updated_at"] < intermediate["updated_at_latest"]
    ]

    # For intermediate rows: effective_at = this change, expired_at = next change
    intermediate = intermediate.sort_values(["customer_id", "updated_at"])
    intermediate["expired_at"] = intermediate.groupby("customer_id")["updated_at"].shift(-1)
    intermediate["is_current"] = False

    # For latest rows: effective_at = latest change, expired_at = NULL, is_current = True
    latest["expired_at"]  = None
    latest["is_current"]  = True
    latest["effective_at"] = latest["updated_at"]

    return latest, intermediate
```

---

## Interview Tips

> **Tip 1:** dbt snapshots handle SCD Type 2 automatically using `strategy='timestamp'` or `strategy='check'`. Know that dbt adds `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` columns automatically — you don't manage these yourself.

> **Tip 2:** The hash-based change detection pattern (`MD5(col1 || '|' || col2 || ...)`) is more efficient than comparing each column individually. One hash comparison replaces N column comparisons.

> **Tip 3:** Handling multiple changes for the same record in a single batch (intraday changes) requires sorting by timestamp and treating all but the last as immediately-expired versions. This is a common interview trap.

> **Tip 4:** The point-in-time join (`BETWEEN effective_at AND COALESCE(expired_at, NOW())`) is the fundamental SCD2 query pattern. Know it by heart — interviewers often ask you to write it.

> **Tip 5:** Type 4 (current + history tables) is the operational choice when current-state lookups must be blazing fast. The main table stays small; history grows separately. Consider this when the dimension has very high query throughput.
