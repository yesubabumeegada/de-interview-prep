---
title: "SQL Interview Coding Problems — Real World"
topic: sql
subtopic: interview-coding-problems
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [sql, interview, incremental-load, data-quality, partition-pruning, debugging]
---

# SQL Interview Coding Problems — Real World

These are the patterns you encounter in production DE work, not just interview exercises. They show up in system design interviews, take-home assignments, and "tell me about a time when..." behavioral questions.

---

## Pattern 1: Incremental Load Query

**Interview prompt:** "How would you load only new or changed records from a source table to a target table? The source has an `updated_at` timestamp column."

Incremental loads are the foundation of every real data pipeline. Full-table reloads don't scale.

### Approach 1: Watermark SELECT (append-only targets)

```sql
-- Step 1: Find the current high-water mark
SELECT MAX(updated_at) AS last_loaded_at
FROM target_table;

-- Step 2: Extract rows from source that are newer than watermark
-- (In a pipeline, the watermark value is passed as a parameter)
SELECT *
FROM source_table
WHERE updated_at > '2024-01-15 12:34:56'  -- :last_loaded_at parameter
ORDER BY updated_at;
```

### Approach 2: MERGE / UPSERT (for targets that track changes)

```sql
-- PostgreSQL: INSERT ... ON CONFLICT
INSERT INTO target_customers (customer_id, name, email, updated_at)
SELECT customer_id, name, email, updated_at
FROM source_customers
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM target_customers)
ON CONFLICT (customer_id) DO UPDATE SET
    name       = EXCLUDED.name,
    email      = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at
WHERE target_customers.updated_at < EXCLUDED.updated_at;  -- only update if source is newer
```

```sql
-- SQL Server / Azure Synapse: MERGE statement
MERGE target_customers AS tgt
USING (
    SELECT customer_id, name, email, updated_at
    FROM source_customers
    WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM target_customers)
) AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET name = src.name, email = src.email, updated_at = src.updated_at
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, name, email, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.updated_at);
```

### Idempotency: running the load twice must produce the same result

```sql
-- Safe: uses MAX watermark, so re-running picks up the same records
-- but ON CONFLICT / MERGE prevents duplicates

-- UNSAFE pattern to avoid:
INSERT INTO target SELECT * FROM source WHERE updated_at > :watermark;
-- If run twice, creates duplicates!
```

### Watermark edge cases

| Edge case | Risk | Mitigation |
|---|---|---|
| Source updated_at has clock skew | Records updated in the same second as watermark are missed | Use `>= watermark - 5 seconds` with a small safety buffer |
| Late-arriving data | Records backfilled in source after watermark advances | Reprocess a lookback window (e.g., last 7 days) |
| NULL updated_at | Records with no timestamp are never loaded | Add `OR updated_at IS NULL` to the WHERE clause |
| Source deletes | Deleted source records are not reflected | Capture deletes via CDC or soft-delete flag |

---

## Pattern 2: Data Quality Check Queries

**Interview prompt:** "You inherit a new data source. What SQL queries do you run to assess data quality?"

These are production-grade DQ checks that every DE should have memorized.

### Check 1: NULL rate per column

```sql
SELECT
    'customer_id'                                     AS column_name,
    COUNT(*)                                          AS total_rows,
    COUNT(customer_id)                                AS non_null_count,
    COUNT(*) - COUNT(customer_id)                     AS null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(customer_id))
          / NULLIF(COUNT(*), 0), 2)                   AS null_pct
FROM customers
UNION ALL
SELECT 'email', COUNT(*), COUNT(email),
       COUNT(*)-COUNT(email),
       ROUND(100.0*(COUNT(*)-COUNT(email))/NULLIF(COUNT(*),0),2)
FROM customers
UNION ALL
SELECT 'updated_at', COUNT(*), COUNT(updated_at),
       COUNT(*)-COUNT(updated_at),
       ROUND(100.0*(COUNT(*)-COUNT(updated_at))/NULLIF(COUNT(*),0),2)
FROM customers;
```

### Check 2: Duplicate detection

```sql
-- Find business-key duplicates
SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- How many rows are duplicates?
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_row_count
FROM customers;
```

### Check 3: Referential integrity (orphan foreign key check)

```sql
-- Find orders with no matching customer
SELECT
    o.order_id,
    o.customer_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Summary count
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_orders,
    ROUND(100.0 * SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2) AS orphan_pct
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

### Check 4: Range / domain validation

```sql
SELECT
    -- Negative amounts (should not occur)
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)      AS negative_amount_count,
    -- Future-dated orders (likely bad data)
    SUM(CASE WHEN order_date > CURRENT_DATE THEN 1 ELSE 0 END) AS future_order_count,
    -- Implausibly large amounts (outlier check)
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END)  AS extreme_amount_count,
    -- Valid status values
    SUM(CASE WHEN status NOT IN ('completed','cancelled','pending') THEN 1 ELSE 0 END) AS invalid_status_count
FROM orders;
```

### Check 5: Freshness check

```sql
-- How old is the latest record? Used in pipeline monitoring
SELECT
    MAX(updated_at)                            AS latest_record,
    CURRENT_TIMESTAMP - MAX(updated_at)        AS data_lag,
    CASE
        WHEN CURRENT_TIMESTAMP - MAX(updated_at) > INTERVAL '2 hours'
        THEN 'STALE'
        ELSE 'FRESH'
    END                                        AS freshness_status
FROM source_table;
```

### DQ check framework summary

| Check | What it detects | Severity |
|---|---|---|
| NULL rate | Missing required data | High for PK/FK columns |
| Duplicate detection | Fanout in joins, double-counting | High for business keys |
| Orphan FK | Broken referential integrity | High |
| Range/domain validation | Bad values, data entry errors | Medium |
| Freshness | Pipeline failure, source delay | High for real-time SLAs |

---

## Pattern 3: Partition Pruning Verification

**Interview prompt:** "You have a large partitioned table. How do you verify your query is actually using partition pruning?"

Partition pruning is when the query engine skips reading partitions that can't possibly match the WHERE clause. Without it, you're doing a full table scan on a table that might be 10TB.

### BigQuery: partition pruning via INFORMATION_SCHEMA and dry run

```sql
-- BigQuery: use the dry run to check bytes processed
-- If partition pruning works, bytes_processed << total table size

-- Check that your filter uses the partition column
SELECT *
FROM `project.dataset.orders`
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31';
-- ^ order_date must be the partition column for pruning to work

-- Verify partitioning in BigQuery
SELECT
    table_name,
    partition_id,
    total_rows,
    total_logical_bytes
FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders'
ORDER BY partition_id;
```

### Redshift: check via EXPLAIN

```sql
EXPLAIN
SELECT *
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31';
-- Look for "Partition Filter" in the output
-- If absent, your predicate isn't hitting the sort key
```

### Snowflake: check via query profile

```sql
-- After running a query, check the query profile in Snowflake UI:
-- "Partitions scanned" vs "Partitions total"
-- Or use QUERY_HISTORY:
SELECT
    query_id,
    partitions_scanned,
    partitions_total,
    ROUND(100.0 * partitions_scanned / NULLIF(partitions_total, 0), 1) AS pct_scanned
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_id = '<your_query_id>';
```

### Common reasons pruning fails

```sql
-- FAILS: function applied to partition column disables pruning
WHERE DATE_TRUNC('month', order_date) = '2024-01-01'
-- FIX: use range condition
WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01'

-- FAILS: implicit type cast
WHERE order_date = 20240101  -- integer vs date
-- FIX: explicit cast
WHERE order_date = DATE '2024-01-01'

-- FAILS: OR condition on non-partition column
WHERE order_date = '2024-01-01' OR customer_id = 101
-- FIX: separate queries or restructure logic
```

### Partition pruning rules

| Condition | Pruning behavior |
|---|---|
| `partition_col = value` | Scans exactly 1 partition |
| `partition_col BETWEEN a AND b` | Scans partitions in range |
| `partition_col IN (a, b, c)` | Scans exactly those partitions |
| `CAST(partition_col AS ...)` | Pruning disabled |
| `FUNCTION(partition_col)` | Pruning disabled |
| `OR` with non-partition col | May disable pruning |

---

## Pattern 4: Debugging "Wrong Numbers"

**Interview prompt:** "Two queries that should return the same revenue figures return different numbers. Walk me through how you debug this."

This is a system design / behavioral pattern, but DE interviews often ask you to walk through the SQL debugging process.

### Step-by-step debugging framework

**Step 1: Check the grain of each query**

```sql
-- Query A produces: $1,250,000 revenue
-- Query B produces: $1,320,000 revenue

-- Are they at the same granularity?
-- Query A:
SELECT COUNT(*), COUNT(DISTINCT order_id) FROM query_a_result;
-- If COUNT(*) != COUNT(DISTINCT order_id), you have fanout (duplicate rows)
```

**Step 2: Check join types and fanout**

```sql
-- A common cause: INNER vs LEFT JOIN changes which rows are included
-- Another: joining on non-unique keys creates a cartesian explosion

-- Diagnose fanout from a join:
SELECT
    o.order_id,
    COUNT(*) AS row_count
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id
HAVING COUNT(*) > 1;
-- If order has 3 items, SUM(o.amount) will triple-count the order amount!
```

**Step 3: Check date filter differences**

```sql
-- Are both queries using the same date range?
-- Watch for: BETWEEN (inclusive on both ends) vs > / >= confusion

-- 'BETWEEN 2024-01-01 AND 2024-01-31' includes Jan 31
-- '< 2024-02-01' also includes Jan 31
-- '< 2024-01-31' excludes Jan 31 (off-by-one!)

-- Check the distribution of dates in your result:
SELECT order_date, COUNT(*), SUM(amount)
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY order_date
ORDER BY order_date;
```

**Step 4: Check NULL handling in aggregations**

```sql
-- SUM ignores NULLs — if some amounts are NULL, they're silently excluded
SELECT
    COUNT(*)            AS total_rows,
    COUNT(amount)       AS non_null_amounts,
    SUM(amount)         AS sum_with_nulls_excluded,
    SUM(COALESCE(amount, 0)) AS sum_treating_null_as_zero
FROM orders;
-- These two SUM values will differ if any amount is NULL
```

**Step 5: Check filter conditions for subtle differences**

```sql
-- Query A: status IN ('completed', 'shipped')
-- Query B: status = 'completed'
-- These have different row counts!

-- Check unique status values:
SELECT status, COUNT(*), SUM(amount)
FROM orders
GROUP BY status
ORDER BY COUNT(*) DESC;
```

**Step 6: Reconcile row by row using EXCEPT**

```sql
-- Find rows in Query A's result that are NOT in Query B's result
SELECT order_id, amount FROM query_a_result
EXCEPT
SELECT order_id, amount FROM query_b_result;

-- Find rows in Query B not in Query A
SELECT order_id, amount FROM query_b_result
EXCEPT
SELECT order_id, amount FROM query_a_result;
```

### Debugging checklist

| Check | What to look for |
|---|---|
| Grain check | COUNT(*) vs COUNT(DISTINCT key) |
| Join type | INNER vs LEFT vs FULL |
| Fanout | Multiple rows per join key |
| Date range | BETWEEN vs <, inclusive/exclusive edges |
| NULL handling | SUM(col) vs SUM(COALESCE(col, 0)) |
| Filter differences | Different status/type/flag conditions |
| Timezone | TIMESTAMP WITH TIME ZONE vs naive timestamp |
| Deduplication | Same order counted in both a and b period |

---

## Real-World Pattern Summary

| Pattern | When you need it | Key technique |
|---|---|---|
| Incremental load | Any pipeline loading from an OLTP source | Watermark + MERGE/UPSERT |
| DQ checks | Onboarding new source, pipeline monitoring | NULL%, duplicate count, orphan FK |
| Partition pruning | Querying large partitioned tables cost-effectively | EXPLAIN, avoid functions on partition col |
| Debugging mismatches | Any time two numbers disagree | Grain check, EXCEPT, fanout analysis |
