---
title: "Teradata - Query Optimization Intermediate"
topic: teradata
subtopic: query-optimization
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [teradata, query-optimization, explain, spool, skew-factor, statistics, join]
---

# Query Optimization — Intermediate

## Deep EXPLAIN Reading

A real EXPLAIN contains rich diagnostic detail. Here's how to parse it:

```
Explanation
-----------
1) First, we lock a distinct SALES."pseudo table" for read on a
   RowHash to prevent global deadlock for SALES.orders.
   We lock SALES.orders for read.

2) Next, we do an all-AMPs RETRIEVE step from SALES.orders
   by way of an all-rows scan with a condition of
   ("orders.order_date >= DATE '2024-01-01'")
   extracting rows into spool 1 (all AMPs), which is built locally
   on the AMPs.  The size of spool 1 is estimated with low confidence
   to be 1,234,567 rows.  The estimated time for this step is 0.34 seconds.

3) We do an all-AMPs JOIN step from spool 1 (all AMPs) by way of
   a MERGE JOIN operator, matched by rowkey only, combined with a
   grouping and SUM/COUNT aggregate operation.
   ...
```

**Key signals to look for:**

| Signal | What It Tells You |
|---|---|
| `low confidence` | Statistics missing or stale → plan may be wrong |
| `no confidence` | No statistics at all → optimizer is guessing |
| `high confidence` | Statistics are fresh and trusted |
| Estimated rows vs actual rows | Large gap = stale stats |
| `PRODUCT JOIN` | Almost always a problem |
| Large spool estimates | Risk of spool exhaustion |

---

## Spool Space Management

Spool errors are among the most common production issues. Understanding spool usage:

```mermaid
flowchart LR
    Query["Complex Query"] --> Step1["Step 1:<br>Filter orders → Spool 1"]
    Step1 --> Step2["Step 2:<br>Join with customer → Spool 2"]
    Step2 --> Step3["Step 3:<br>Aggregate → Spool 3"]
    Step3 --> Result["Final Result → Client"]
    Step1 -. "Each spool consumes<br>AMP disk space" .-> Disk["AMP Disk"]
    Step2 -. "" .-> Disk
    Step3 -. "" .-> Disk
```

**Spool reduction techniques:**

1. **Filter earlier:** Add WHERE clauses that reduce row count before joins
2. **Project fewer columns:** SELECT only needed columns — spool stores full rows
3. **Use derived tables/CTEs efficiently:** Each CTE materializes into spool
4. **Increase user spool quota:** If legitimate, raise the limit in DBC.Users
5. **Break complex queries:** Execute in steps, storing partial results in volatile/temp tables

```sql
-- Check current spool limit for your user
SELECT SpoolSpace FROM DBC.UsersV WHERE UserName = USER;

-- Volatile table as alternative to spool-heavy CTE
CREATE VOLATILE TABLE vt_filtered_orders AS (
    SELECT order_id, customer_id, total_amount
    FROM orders
    WHERE order_date >= '2024-01-01'
) WITH DATA PRIMARY INDEX (customer_id) ON COMMIT PRESERVE ROWS;
```

---

## Skew Factor in EXPLAIN

EXPLAIN reports a **skew factor** for spool steps — showing how unevenly the intermediate data is distributed across AMPs:

```
The size of spool 1 is estimated with high confidence to be
1,000,000 rows (skew_factor = 30%)
```

- Skew factor 0% = perfectly even
- Skew factor > 50% = some AMPs are doing 3× the work of others
- Skew factor > 90% = effectively single-AMP operation

**Root causes of spool skew:**
- Joining on a skewed column (heavy-hitter values)
- GROUP BY on a low-cardinality column
- Filtering that leaves most data on a few AMPs

---

## Query Rewriting for Performance

### Avoid Functions on Filtered Columns
Functions prevent index/partition usage:

```sql
-- BAD: CAST prevents partition elimination
WHERE CAST(order_date AS CHAR(7)) = '2024-01'

-- GOOD: Direct date range filter uses PPI
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
```

### Use QUALIFY Instead of Subquery for Row Ranking
```sql
-- BAD: Correlated subquery (product join risk)
SELECT * FROM orders o
WHERE order_id = (
    SELECT MAX(order_id) FROM orders WHERE customer_id = o.customer_id
);

-- GOOD: QUALIFY with window function (AMP-local)
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_id DESC) = 1;
```

### Join Order Hints
When the optimizer picks the wrong join order:

```sql
-- Force join order with hint
SELECT /*+ AVOID_FULL_JOIN(small_table) */ ...

-- Or restructure query to give optimizer correct size info
-- (best done by collecting statistics)
```

---

## Collect Statistics: Key Decisions

```sql
-- Basic column stats
COLLECT STATISTICS COLUMN (customer_id) ON orders;

-- Multi-column stats (for compound predicates)
COLLECT STATISTICS COLUMN (customer_id, order_date) ON orders;

-- Index stats (critical for PI)
COLLECT STATISTICS INDEX (customer_id) ON orders;

-- Partition stats (critical for PPI)
COLLECT STATISTICS COLUMN (PARTITION) ON orders;

-- Check existing stats
SHOW STATISTICS ON orders;
```

**Statistics refresh strategy:**
- Collect stats after every significant data load (> 10% change)
- Schedule weekly full stats refresh during off-peak hours
- Use `USING SAMPLE PERCENT 10` for ultra-large tables when full scan is too expensive:

```sql
COLLECT STATISTICS USING SAMPLE 10 PERCENT
COLUMN (order_date) ON orders;
```

---

## DBQL: Database Query Log

Teradata's **Database Query Log (DBQL)** captures query execution details:

```sql
-- Find top 10 most expensive queries by CPU
SELECT TOP 10
    UserName,
    SUBSTRING(QueryText FROM 1 FOR 100) AS QueryPreview,
    AMPCPUTime,
    TotalIOCount,
    SpoolUsage / 1e9 AS SpoolGB,
    ElapsedTime
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE - 1
ORDER BY AMPCPUTime DESC;

-- Find queries with product joins (by high CPU + low row count)
SELECT UserName, AMPCPUTime, TotalIOCount, NumSteps
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE
  AND AMPCPUTime > 1000
  AND NumResultRows < 100  -- lots of CPU for few rows = suspect
ORDER BY AMPCPUTime DESC;
```

---

## Interview Tips

> **Tip 1:** "How do you identify a missing statistics problem?" — "In EXPLAIN, look for 'low confidence' or 'no confidence' row estimates. If the optimizer estimates 1,000 rows but the table has 1 billion, the plan will be wrong. Run SHOW STATISTICS to see what's collected; COLLECT STATISTICS on the relevant columns."

> **Tip 2:** "What is skew factor in query optimization?" — "Skew factor in the EXPLAIN plan shows how unevenly intermediate spool data is distributed. High skew means some AMPs do far more work than others, degrading the parallel advantage. Root cause is usually joining/grouping on low-cardinality or skewed columns."

> **Tip 3:** "How do you reduce spool space usage?" — "Filter early (reduce rows before joins), project only needed columns (fewer columns = smaller spool rows), use volatile tables to materialize and re-use intermediate results with proper PI, and avoid unnecessary CTEs that each materialize into spool."

> **Tip 4:** "What's the difference between redistribution and duplication in a join?" — "Redistribution re-hashes one table's rows to AMPs based on the join key — used for large tables. Duplication broadcasts the entire smaller table to every AMP — used when the small table is tiny enough that copying is cheaper than redistributing the large table. EXPLAIN will tell you which is happening."
