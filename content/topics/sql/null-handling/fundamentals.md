---
title: "NULL Handling - Fundamentals"
topic: sql
subtopic: null-handling
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, null, three-valued-logic, coalesce, nullif, is-null]
---

# NULL Handling — Fundamentals

NULLs are the most misunderstood concept in SQL and the source of a surprising number of production bugs. Understanding three-valued logic is the foundation of everything else.

---

## Three-Valued Logic: TRUE / FALSE / UNKNOWN

Standard SQL uses three-valued logic. Any comparison involving NULL produces `UNKNOWN`, not TRUE or FALSE.

```sql
SELECT NULL = NULL;    -- UNKNOWN (not TRUE!)
SELECT NULL != NULL;   -- UNKNOWN
SELECT NULL = 1;       -- UNKNOWN
SELECT NULL IS NULL;   -- TRUE   ← this is the correct check
SELECT NULL IS NOT NULL; -- FALSE
```

In a `WHERE` clause, only rows where the condition evaluates to `TRUE` pass through. `UNKNOWN` rows are silently filtered out — this is where most NULL bugs hide.

```sql
-- Assume some rows have status = NULL
SELECT * FROM orders WHERE status = 'active';    -- NULL rows excluded (correct)
SELECT * FROM orders WHERE status != 'active';   -- NULL rows ALSO excluded! (surprise)
SELECT * FROM orders WHERE status IS NULL;        -- NULL rows included (correct)
```

---

## IS NULL vs = NULL

This is the most common junior mistake:

```sql
-- WRONG: always returns 0 rows (comparing anything to NULL = UNKNOWN)
SELECT * FROM users WHERE email = NULL;

-- CORRECT: IS NULL specifically tests for the absence of a value
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;
```

---

## NULLs in WHERE Clauses

Compound conditions with NULLs behave according to three-valued logic rules:

| A | B | A AND B | A OR B |
|---|---|---|---|
| TRUE | UNKNOWN | UNKNOWN | TRUE |
| FALSE | UNKNOWN | FALSE | UNKNOWN |
| UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |

```sql
-- Practical example: filtering with OR on a nullable column
SELECT * FROM users
WHERE city = 'NYC'
   OR city IS NULL;   -- must explicitly include IS NULL to get NULLs

-- Without IS NULL, the OR still filters out NULLs:
SELECT * FROM users
WHERE city = 'NYC' OR city = 'LA';
-- Rows where city IS NULL: FALSE OR FALSE = FALSE → filtered out
```

---

## NULLs in JOIN Conditions

NULL keys never match each other in a JOIN. This is a critical source of silent row loss.

```sql
-- Setup
CREATE TABLE orders   (order_id INT, customer_id INT);
CREATE TABLE customers(customer_id INT, name VARCHAR);

INSERT INTO orders VALUES (1, NULL), (2, 100);
INSERT INTO customers VALUES (100, 'Alice'), (NULL, 'Unknown');

-- INNER JOIN: order 1 (customer_id = NULL) does NOT join to customer_id = NULL
SELECT o.order_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
-- Returns only: (2, 'Alice')
-- NULL = NULL is UNKNOWN, so order 1 is dropped

-- To include unmatched rows, use LEFT JOIN + IS NULL check
SELECT o.order_id, COALESCE(c.name, 'Unknown') AS name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

---

## COALESCE — Return First Non-NULL Value

`COALESCE(expr1, expr2, ..., exprN)` evaluates arguments left to right and returns the first one that is not NULL.

```sql
-- Replace NULL phone with 'No phone on file'
SELECT user_id,
       COALESCE(mobile_phone, home_phone, work_phone, 'No phone on file') AS contact_phone
FROM users;

-- Use COALESCE for default values in calculations
SELECT
    revenue - COALESCE(discount, 0) AS net_revenue,  -- discount may be NULL
    COALESCE(delivery_date, CURRENT_DATE + 7) AS expected_delivery
FROM orders;
```

---

## NULLIF — Return NULL if Two Values Are Equal

`NULLIF(expr1, expr2)` returns NULL when both arguments are equal, otherwise returns expr1. Useful for turning sentinel values (0, empty string, 'N/A') into proper NULLs.

```sql
-- Prevent division by zero
SELECT total_sales / NULLIF(total_units, 0) AS avg_price_per_unit
FROM daily_summary;
-- When total_units = 0: NULLIF returns NULL, division returns NULL (not an error)

-- Convert empty strings to NULL for cleaner data
SELECT NULLIF(TRIM(email), '') AS clean_email
FROM raw_users;
-- '' (blank) → NULL; 'user@example.com' → 'user@example.com'
```

---

## Dialect Differences: IFNULL, NVL, ISNULL

Different databases use different names for the two-argument "replace NULL with default" function:

| Function | Dialects |
|---|---|
| `COALESCE(col, default)` | All dialects (ANSI SQL standard) |
| `IFNULL(col, default)` | MySQL, SQLite, BigQuery |
| `NVL(col, default)` | Oracle, Snowflake |
| `ISNULL(col, default)` | SQL Server, Sybase |

**Always prefer `COALESCE`** — it's ANSI standard and works everywhere. Only use the dialect-specific versions when you need the code to be idiomatic in that system.

```sql
-- All equivalent:
COALESCE(discount, 0)   -- ANSI: works everywhere
IFNULL(discount, 0)     -- BigQuery / MySQL
NVL(discount, 0)        -- Snowflake / Oracle
ISNULL(discount, 0)     -- SQL Server
```

---

## NULL in ORDER BY — NULLS FIRST / NULLS LAST

By default, NULL sort behavior varies by database:
- Postgres, Snowflake: NULLs sort LAST in ASC, FIRST in DESC
- MySQL, SQLite: NULLs sort FIRST in ASC
- SQL Server: NULLs sort FIRST in ASC

Use explicit `NULLS FIRST` / `NULLS LAST` for predictable behavior:

```sql
-- Sort by score descending, put NULL scores at the bottom
SELECT user_id, score
FROM leaderboard
ORDER BY score DESC NULLS LAST;

-- Sort by date ascending, put NULL dates at the top (to investigate missing dates)
SELECT order_id, ship_date
FROM orders
ORDER BY ship_date ASC NULLS FIRST;
```

---

## Key Takeaways

- NULL is not a value — it represents the absence of a value; any comparison produces UNKNOWN
- Always use `IS NULL` / `IS NOT NULL`, never `= NULL`
- NULLs are silently dropped in WHERE clauses (both the equal and not-equal sides)
- NULL keys never match in JOINs — unmatched NULLs disappear in INNER JOINs
- `COALESCE` is the ANSI standard for default values; `NULLIF` converts sentinel values to NULL
- Control NULL sort position explicitly with `NULLS FIRST` / `NULLS LAST`
