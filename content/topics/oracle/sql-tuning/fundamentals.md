---
title: "SQL Tuning — Fundamentals"
topic: oracle
subtopic: sql-tuning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, sql-tuning, execution-plan, indexes, explain-plan]
---

# SQL Tuning — Fundamentals

## What Is SQL Tuning?

SQL tuning is the process of improving query performance by reducing resource consumption (CPU, I/O, memory) and elapsed time. Oracle's Cost-Based Optimizer (CBO) generates execution plans — tuning guides the optimizer to make better choices.

---

## The Oracle Query Lifecycle

```
SQL Text
  → Parse (syntax check, semantic check, privilege check)
  → Optimize (CBO generates execution plan)
  → Execute (access data per the plan)
  → Fetch (return rows to client)
```

Each phase can be a bottleneck. Tuning mostly targets the Optimize and Execute phases.

---

## Reading an Execution Plan

```sql
-- Step 1: Generate explain plan
EXPLAIN PLAN FOR
SELECT c.customer_name, SUM(o.amount)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.region = 'WEST'
GROUP BY c.customer_name;

-- Step 2: Display the plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

**Sample output:**
```
--------------------------------------------------------------------
| Id | Operation                | Name        | Rows | Cost (%CPU) |
--------------------------------------------------------------------
|  0 | SELECT STATEMENT         |             |   50 |   120   (2) |
|  1 |  HASH GROUP BY           |             |   50 |   120   (2) |
|  2 |   HASH JOIN              |             | 5000 |   118   (1) |
|  3 |    TABLE ACCESS BY INDEX | CUSTOMERS   |  200 |    15   (0) |
|  4 |     INDEX RANGE SCAN     | IDX_REGION  |  200 |     3   (0) |
|  5 |    TABLE ACCESS FULL     | ORDERS      | 100K |   100   (1) |
--------------------------------------------------------------------
```

**Key columns:**
- **Operation**: how Oracle accesses data (full scan, index scan, join method)
- **Rows (cardinality estimate)**: Oracle's guess of rows at each step — if wildly wrong, stats are stale
- **Cost**: relative unit of work (lower is not always better if cardinality is misestimated)

---

## Access Methods

| Operation | When Oracle Uses It | Performance |
|---|---|---|
| `TABLE ACCESS FULL` | No usable index, small table, or >10-15% of rows | Reads all blocks — slow for large tables |
| `TABLE ACCESS BY INDEX ROWID` | After index lookup to get remaining columns | Fast — single block access |
| `INDEX UNIQUE SCAN` | Equality predicate on unique/primary key index | Best: 1 index block + 1 table block |
| `INDEX RANGE SCAN` | Range predicates (`BETWEEN`, `<`, `>`, `LIKE 'X%'`) | Reads a range of index leaf blocks |
| `INDEX SKIP SCAN` | Leading column of composite index not in WHERE | Less efficient than range scan |
| `INDEX FULL SCAN` | Need all rows in index order; avoids sort | Reads all index blocks — no table access if covering |
| `INDEX FAST FULL SCAN` | Need all index data; order not required | Reads index blocks with multiblock I/O |

---

## Common Tuning Techniques

### 1. Add a Missing Index
```sql
-- Slow: full scan on large table
SELECT * FROM orders WHERE order_date = DATE '2024-01-01';

-- Add index
CREATE INDEX idx_orders_date ON orders(order_date);

-- Now: INDEX RANGE SCAN instead of TABLE ACCESS FULL
```

### 2. Fix a Bad JOIN
```sql
-- Bad: Cartesian product caused by missing join condition
SELECT * FROM customers, orders WHERE customers.region = 'WEST';

-- Good: explicit join condition
SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id
WHERE c.region = 'WEST';
```

### 3. Avoid Functions on Indexed Columns
```sql
-- Bad: index on order_date cannot be used
SELECT * FROM orders WHERE TRUNC(order_date) = DATE '2024-01-01';

-- Good: rewrite to use the index
SELECT * FROM orders 
WHERE order_date >= DATE '2024-01-01' 
  AND order_date <  DATE '2024-01-02';
```

### 4. Use Bind Variables
```sql
-- Bad: hard parse every time (new plan for each value)
SELECT * FROM orders WHERE status = 'PENDING';
SELECT * FROM orders WHERE status = 'SHIPPED';

-- Good: cursor sharing; plan reuse
SELECT * FROM orders WHERE status = :status_bind;
```

---

## Gathering Optimizer Statistics

The CBO makes decisions based on statistics. Stale stats → bad plans.

```sql
-- Gather stats on a table
EXEC DBMS_STATS.GATHER_TABLE_STATS('HR', 'EMPLOYEES');

-- Gather stats with auto-sample size (recommended)
EXEC DBMS_STATS.GATHER_TABLE_STATS(
  ownname  => 'HR',
  tabname  => 'ORDERS',
  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
  method_opt       => 'FOR ALL COLUMNS SIZE AUTO'
);

-- Check when stats were last gathered
SELECT table_name, last_analyzed, num_rows
FROM dba_tables
WHERE owner = 'HR'
ORDER BY last_analyzed NULLS FIRST;
```

---

## Autotrace — Quick Plan in SQL*Plus

```sql
SET AUTOTRACE ON EXPLAIN STATISTICS
SELECT order_id, amount FROM orders WHERE customer_id = 42;
-- Shows: plan + statistics (logical reads, physical reads, sorts)

SET AUTOTRACE OFF
```

**Key statistics to watch:**
- **consistent gets**: logical I/O (buffer cache reads) — minimize this
- **physical reads**: disk I/O — should be 0 for hot data
- **sorts (memory)** / **sorts (disk)**: disk sorts are expensive

---

## Interview Tips

> **Tip 1:** "How do you tune a slow query?" — Start with EXPLAIN PLAN or DBMS_XPLAN to see the current plan. Look for: full scans on large tables, bad cardinality estimates, wrong join order, missing indexes. Gather fresh stats first — most bad plans come from stale statistics.

> **Tip 2:** "What's the difference between logical reads and physical reads?" — Logical reads (consistent gets) are buffer cache hits; physical reads go to disk. A query with 1M logical reads but 0 physical reads is working from cache — fast. Same query with 500K physical reads is hitting disk repeatedly — needs optimization.

> **Tip 3:** "Why avoid functions on indexed columns?" — Oracle can only use an index when the predicate references the column value directly. `TRUNC(order_date) = DATE '2024-01-01'` transforms the column, so Oracle can't map it to the index entries. Rewrite using range conditions to let Oracle use the index.
