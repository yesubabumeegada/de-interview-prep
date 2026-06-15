---
title: "Oracle Interview Scenarios - Fundamentals"
topic: oracle
subtopic: interview-scenarios
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, interview, scenarios, sql-tuning, pl-sql]
---

# Oracle Interview Scenarios – Fundamentals

## Overview

This guide covers foundational Oracle concepts commonly tested in junior to mid-level data engineering and DBA interviews. Topics include reading EXPLAIN PLANs, understanding index hints, and leveraging partition pruning for performance.

---

## 1. EXPLAIN PLAN Basics

### What is EXPLAIN PLAN?

`EXPLAIN PLAN` is Oracle's tool to show the execution strategy the Cost-Based Optimizer (CBO) will use for a given SQL statement — without actually executing the query.

```sql
EXPLAIN PLAN FOR
SELECT e.employee_id, e.last_name, d.department_name
FROM   employees e
JOIN   departments d ON e.department_id = d.department_id
WHERE  e.salary > 50000;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

**Sample output:**
```
Plan hash value: 2134576890

-----------------------------------------------------------------------
| Id | Operation                    | Name        | Rows  | Cost (%CPU)|
-----------------------------------------------------------------------
|  0 | SELECT STATEMENT             |             |   120 |    15   (0)|
|  1 |  HASH JOIN                   |             |   120 |    15   (0)|
|  2 |   TABLE ACCESS FULL          | DEPARTMENTS |    27 |     3   (0)|
|  3 |   TABLE ACCESS BY INDEX ROWID| EMPLOYEES   |   120 |    12   (0)|
|  4 |    INDEX RANGE SCAN          | EMP_SAL_IDX |   120 |     2   (0)|
-----------------------------------------------------------------------
```

### Key columns to understand

| Column | Meaning |
|--------|---------|
| Operation | The step the database performs (e.g., TABLE ACCESS FULL, INDEX RANGE SCAN) |
| Name | The object accessed |
| Rows | Estimated row count (cardinality) |
| Cost | Optimizer's estimated cost (relative unit) |
| Bytes | Estimated bytes processed |

### Common interview question

**Q: What is the difference between EXPLAIN PLAN and AUTOTRACE?**

**A:** `EXPLAIN PLAN` shows the *estimated* execution plan before query execution. `AUTOTRACE` (enabled with `SET AUTOTRACE ON` in SQL*Plus) shows the *actual* execution statistics — including logical reads (consistent gets), physical reads, and the actual plan used after execution.

```sql
-- Enable autotrace to see actual stats
SET AUTOTRACE ON STATISTICS
SELECT COUNT(*) FROM employees WHERE department_id = 10;
```

---

## 2. Index Hints

### Why hints exist

Oracle's CBO is generally reliable but can choose suboptimal plans when statistics are stale or the data distribution is skewed. Hints let developers *suggest* (not force) a plan.

### Common index hints

```sql
-- Force use of a specific index
SELECT /*+ INDEX(e EMP_DEPT_IDX) */
       e.employee_id, e.last_name
FROM   employees e
WHERE  e.department_id = 20;

-- Force a full table scan (bypass all indexes)
SELECT /*+ FULL(e) */
       e.employee_id
FROM   employees e
WHERE  e.salary BETWEEN 40000 AND 60000;

-- Force index range scan (useful when CBO picks full scan)
SELECT /*+ INDEX_RS_ASC(e EMP_SAL_IDX) */
       e.employee_id, e.salary
FROM   employees e
WHERE  e.salary > 80000;
```

### Interview Q&A

**Q: When would you use the `NO_INDEX` hint?**

**A:** When the optimizer incorrectly picks an index for a query that returns a large percentage of rows (e.g., >15–20%), a full table scan is usually faster because index range scans incur random I/O for each ROWID lookup. `NO_INDEX` forces the optimizer to ignore a specific index.

```sql
SELECT /*+ NO_INDEX(e EMP_SAL_IDX) */
       e.employee_id, e.salary
FROM   employees e
WHERE  e.salary > 10000;  -- returns most rows, full scan is cheaper
```

**Q: Are hints guaranteed to work?**

**A:** No. Hints are *suggestions*. Oracle may ignore them if the hinted object doesn't exist, the alias is wrong, or the hint syntax is malformed. Hints are also not validated at parse time — a misspelled hint is silently ignored.

---

## 3. Partition Pruning

### What is partition pruning?

Partition pruning is Oracle's ability to skip entire partitions that cannot contain rows satisfying a query's WHERE clause. This dramatically reduces I/O on large partitioned tables.

### Example setup

```sql
-- Create a range-partitioned table
CREATE TABLE sales (
    sale_id      NUMBER,
    sale_date    DATE,
    amount       NUMBER,
    region       VARCHAR2(20)
)
PARTITION BY RANGE (sale_date) (
    PARTITION p_2022 VALUES LESS THAN (DATE '2023-01-01'),
    PARTITION p_2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p_2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION p_max  VALUES LESS THAN (MAXVALUE)
);
```

### Verifying pruning in EXPLAIN PLAN

```sql
EXPLAIN PLAN FOR
SELECT SUM(amount)
FROM   sales
WHERE  sale_date >= DATE '2023-01-01'
AND    sale_date <  DATE '2024-01-01';

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

Look for `Pstart` and `Pstop` columns in the plan output:

```
| Id | Operation              | Name  | Rows | Pstart | Pstop |
|  1 |  PARTITION RANGE SINGLE|       |   1M |      2 |     2 |
|  2 |   TABLE ACCESS FULL    | SALES |   1M |      2 |     2 |
```

`Pstart=2, Pstop=2` means only partition 2 (p_2023) is scanned. All other partitions are skipped.

### Common pitfalls that prevent pruning

```sql
-- BAD: wrapping partition key in a function prevents pruning
SELECT * FROM sales WHERE TRUNC(sale_date) = DATE '2023-06-01';

-- GOOD: use range predicates on the raw column
SELECT * FROM sales
WHERE  sale_date >= DATE '2023-06-01'
AND    sale_date <  DATE '2023-06-02';
```

### Interview Q&A

**Q: What types of partitioning does Oracle support?**

**A:**
- **RANGE** – rows distributed by value ranges (e.g., dates, numeric IDs)
- **LIST** – rows distributed by explicit value lists (e.g., region = 'US', 'EU')
- **HASH** – rows distributed by a hash function for even distribution
- **COMPOSITE** – combinations such as RANGE-HASH or RANGE-LIST

**Q: What is dynamic partition pruning?**

**A:** Dynamic pruning happens at runtime when the pruning predicate cannot be evaluated at parse time — for example, when the partition key is compared against a bind variable or a subquery result. Oracle determines which partitions to scan during execution.

---

## 4. Common Foundational Interview Questions

### Q: What is the difference between a B-Tree index and a Bitmap index?

**A:**

| Feature | B-Tree Index | Bitmap Index |
|---------|-------------|--------------|
| Best for | High-cardinality columns (e.g., employee_id) | Low-cardinality columns (e.g., gender, status) |
| DML performance | Good (row-level locking) | Poor (bitmap segment locking causes contention) |
| Space | More space | Compact |
| Typical use | OLTP | Data warehouses |

### Q: What causes a "TABLE ACCESS FULL" when an index exists?

**A:** Common reasons:
1. The query returns >10–15% of rows — full scan is cheaper than random index I/O
2. The indexed column is wrapped in a function (`TO_CHAR(hire_date, 'YYYY')`)
3. Statistics are stale — optimizer underestimates selectivity
4. The leading column of a composite index is not in the WHERE clause

### Q: What is a consistent get vs. a physical read?

**A:** A *consistent get* (logical read) reads a block from Oracle's buffer cache in memory. A *physical read* fetches the block from disk into the buffer cache first. Tuning goal: maximize consistent gets, minimize physical reads by sizing the buffer cache appropriately and writing index-friendly queries.

### Q: How do you gather statistics on a table?

```sql
-- Gather stats on a single table with auto sample size
EXEC DBMS_STATS.GATHER_TABLE_STATS(
    ownname   => 'HR',
    tabname   => 'EMPLOYEES',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    method_opt       => 'FOR ALL COLUMNS SIZE AUTO',
    cascade          => TRUE  -- also gather index stats
);
```

---

## 5. Quick Reference Cheat Sheet

| Concept | Key Command / Hint |
|---------|-------------------|
| View execution plan | `SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)` |
| Force index | `/*+ INDEX(alias index_name) */` |
| Skip index | `/*+ NO_INDEX(alias index_name) */` |
| Force full scan | `/*+ FULL(alias) */` |
| Real-time plan | `DBMS_XPLAN.DISPLAY_CURSOR` |
| Gather stats | `DBMS_STATS.GATHER_TABLE_STATS` |
| Check pruning | Look for `Pstart`/`Pstop` in EXPLAIN PLAN |
