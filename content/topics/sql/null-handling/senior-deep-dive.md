---
title: "NULL Handling - Senior Deep Dive"
topic: sql
subtopic: null-handling
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, null, unique-constraints, json-null, spark-nullable, data-quality, sentinel-values, hive-partition]
---

# NULL Handling — Senior Deep Dive

Senior-level NULL topics cover schema design decisions (NULL vs sentinel), NULL behavior in constraints, JSON columns, Spark schema evolution, and building data quality systems that can distinguish intentional from accidental NULLs.

---

## NULL in UNIQUE Constraints

Most databases allow multiple NULL values in a UNIQUE-constrained column because two NULLs are not considered equal (NULL != NULL). This surprises engineers who assume UNIQUE means "at most one NULL."

```sql
-- Postgres, Snowflake, BigQuery: multiple NULLs allowed in UNIQUE column
CREATE TABLE users (email VARCHAR UNIQUE);
INSERT INTO users VALUES ('a@example.com');  -- OK
INSERT INTO users VALUES ('a@example.com');  -- ERROR: duplicate
INSERT INTO users VALUES (NULL);             -- OK
INSERT INTO users VALUES (NULL);             -- OK in Postgres/Snowflake! (2 NULLs allowed)

-- SQL Server: only ONE NULL allowed in UNIQUE index (different behavior!)
-- MySQL: multiple NULLs allowed (like Postgres)
```

If you want "at most one NULL" behavior, add a partial unique index:

```sql
-- Postgres: unique index that excludes NULLs (allows multiple NULLs)
CREATE UNIQUE INDEX idx_users_email ON users(email) WHERE email IS NOT NULL;

-- Or use a filtered index to enforce "at most one active row per entity"
CREATE UNIQUE INDEX idx_one_active_per_user
ON subscriptions(user_id)
WHERE status = 'active';  -- can have many cancelled rows, only one active
```

---

## NULL Propagation in Expressions

NULL is "contagious" — any arithmetic or string operation on NULL returns NULL.

```sql
-- Arithmetic
SELECT NULL + 1;           -- NULL
SELECT NULL * 0;           -- NULL (not 0!)
SELECT 5 / NULL;           -- NULL

-- String operations
SELECT NULL || ' suffix';  -- NULL (Postgres string concatenation)
SELECT CONCAT(NULL, ' suffix');  -- NULL in most dialects
SELECT CONCAT_WS(' ', NULL, 'world');  -- 'world' (CONCAT_WS skips NULLs!)
```

`CONCAT_WS(separator, ...)` is NULL-safe: it skips NULL arguments and doesn't produce a NULL output because of them. Useful for building display strings from nullable columns.

```sql
-- Safe full name with nullable middle name
SELECT CONCAT_WS(' ', first_name, middle_name, last_name) AS full_name
FROM users;
-- 'John Doe' when middle_name is NULL (not 'John  Doe' with extra space)
```

---

## Designing for NULLs: Sentinel Values vs Actual NULL

The design decision of whether to use NULL or a sentinel value (0, -1, 'N/A', 'UNKNOWN') has downstream consequences for every query that touches the column.

| Approach | Use when | Risks |
|---|---|---|
| Use NULL | Value is genuinely unknown/inapplicable | AVG/SUM silently skip it; NOT IN subquery bombs |
| Use sentinel (0, -1) | Zero is meaningful and distinct from "missing" | Must document sentinel; `AVG` will be wrong if sentinel included |
| Use sentinel ('N/A', 'UNKNOWN') | Categorical field where "not provided" is a valid state | String comparisons must exclude sentinel; GROUP BY includes it |

```sql
-- Example: revenue column
-- NULL revenue: "we don't know what this order's revenue is"
-- 0 revenue: "this order had zero revenue (e.g., a coupon order)"
-- These are DIFFERENT and should not both be NULL

SELECT AVG(revenue) FROM orders;                        -- skips NULLs
SELECT AVG(COALESCE(revenue, 0)) FROM orders;           -- treats NULL as 0

-- If you used -1 as sentinel for "unknown", AVG includes -1 in the average!
-- This is why NULL is better than -1 for unknowns
```

Snowflake and BigQuery support nullable columns by default. In Spark, nullability is part of the schema:

```scala
// Spark: non-nullable column (will fail on NULL insert)
StructField("order_id", IntegerType, nullable = false)
// Nullable column
StructField("discount", DoubleType, nullable = true)
```

---

## NULL in JSON Columns

There are two different kinds of "null" when working with JSON in SQL:

1. **SQL NULL**: the column itself is absent / has no value
2. **JSON null**: the JSON object contains a key with the value `null` (the JSON literal)

```sql
-- Postgres JSONB example
CREATE TABLE events (payload JSONB);
INSERT INTO events VALUES (NULL);           -- SQL NULL: entire payload is absent
INSERT INTO events VALUES ('{}');           -- JSON: empty object, no keys
INSERT INTO events VALUES ('{"score": null}');  -- JSON: score key exists, value is JSON null

-- Distinguish them
SELECT
    payload IS NULL                          AS is_sql_null,
    payload -> 'score' IS NULL               AS score_key_missing_or_json_null,
    jsonb_typeof(payload -> 'score') = 'null' AS score_is_json_null
FROM events;

-- Extract or default JSON value, handling both SQL NULL and JSON null
SELECT
    COALESCE(
        (payload ->> 'score')::numeric,
        0
    ) AS score
FROM events
WHERE payload IS NOT NULL;  -- exclude SQL NULLs first
```

In BigQuery JSON:
```sql
-- BigQuery: JSON_VALUE returns NULL for both missing key and JSON null
SELECT JSON_VALUE(payload, '$.score') AS score
FROM events;
-- Returns NULL for: SQL NULL, missing key, or JSON null — can't distinguish!

-- Use JSON_QUERY to detect JSON null vs missing key
SELECT
    JSON_QUERY(payload, '$.score') AS raw_score  -- returns 'null' string for JSON null
FROM events;
```

---

## NULL Handling in Spark

Spark has specific behaviors around NULLs that differ from ANSI SQL in important ways.

### nullable Flag in Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("user_id",  IntegerType(), nullable=False),  # NOT NULL equivalent
    StructField("email",    StringType(),  nullable=True),
    StructField("revenue",  DoubleType(),  nullable=True),
])
```

Setting `nullable=False` doesn't enforce the constraint on read — it's a hint to the optimizer. Spark will NOT raise an error if NULL values arrive in a nullable=False column from CSV/JSON.

### dropna vs fillna Strategies

```python
# Drop rows where ANY column is NULL
df.dropna()

# Drop rows where SPECIFIC columns are NULL
df.dropna(subset=["user_id", "event_ts"])

# Fill NULLs with defaults per column type
df.fillna({"revenue": 0.0, "region": "UNKNOWN", "is_active": False})

# Fill with mean (more sophisticated)
from pyspark.ml.feature import Imputer
imputer = Imputer(inputCols=["revenue"], outputCols=["revenue_imputed"])
```

In Spark SQL:
```sql
-- Drop rows where key columns are NULL
SELECT * FROM events WHERE user_id IS NOT NULL AND event_ts IS NOT NULL;

-- Fill NULLs using COALESCE (same as other dialects)
SELECT user_id,
       COALESCE(revenue, 0.0)     AS revenue,
       COALESCE(region, 'UNKNOWN') AS region
FROM events;
```

---

## Partition Key NULL Behavior — Hive / Spark

In Hive and Spark SQL, NULLs in partition key columns are stored in a special partition named `__HIVE_DEFAULT_PARTITION__`.

```sql
-- Writing a partitioned table with NULL partition keys
CREATE TABLE events_partitioned
PARTITIONED BY (event_date DATE);

INSERT INTO events_partitioned
SELECT user_id, event_ts, NULL AS event_date   -- creates __HIVE_DEFAULT_PARTITION__
FROM raw_events
WHERE event_date IS NULL;

-- Querying the NULL partition
SELECT * FROM events_partitioned
WHERE event_date IS NULL;
-- Hive/Spark translates this to scan the __HIVE_DEFAULT_PARTITION__ directory

-- BigQuery: NULLs in partition column go to the __NULL__ partition
-- Access it with:
SELECT * FROM `project.dataset.table` WHERE DATE(_PARTITIONTIME) IS NULL;
```

---

## Data Quality: Unexpected NULLs as a Signal

In production pipelines, unexpected NULLs are one of the most reliable data quality signals. Design your monitoring around them.

```sql
-- NULL rate per column (Postgres)
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(*) FILTER (WHERE user_id IS NULL)     AS null_user_id,
    COUNT(*) FILTER (WHERE email IS NULL)       AS null_email,
    COUNT(*) FILTER (WHERE revenue IS NULL)     AS null_revenue,
    ROUND(100.0 * COUNT(*) FILTER (WHERE revenue IS NULL) / COUNT(*), 2)
                                                AS null_revenue_pct
FROM orders
WHERE order_date = CURRENT_DATE - 1;  -- yesterday's data

-- Alert if NULL rate exceeds threshold
WITH null_rates AS (
    SELECT
        ROUND(100.0 * COUNT(*) FILTER (WHERE revenue IS NULL) / COUNT(*), 2) AS null_revenue_pct
    FROM orders
    WHERE order_date = CURRENT_DATE - 1
)
SELECT *
FROM null_rates
WHERE null_revenue_pct > 5.0;  -- alert if > 5% of revenue is NULL
```

---

## Key Takeaways

- Most databases allow multiple NULLs in a UNIQUE column — add a partial index if you need uniqueness for non-NULL values
- `NULL + 1 = NULL` — any expression touching NULL produces NULL; `CONCAT_WS` is one exception
- Choose NULL for "truly unknown" and sentinel for "not applicable but known state" — document the convention
- SQL NULL and JSON `null` are different things — `JSON_QUERY` vs `JSON_VALUE` behave differently on each
- Spark's `nullable=False` is an optimizer hint, not an enforcement — NULLs can still arrive from file reads
- Hive/Spark NULL partition keys go to `__HIVE_DEFAULT_PARTITION__` — remember this when querying or backfilling
