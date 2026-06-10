---
title: "Partitioning — Scenarios"
topic: oracle
subtopic: partitioning
content_type: scenario_question
tags: [oracle, partitioning, interview, scenarios, design, troubleshooting]
---

# Partitioning — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Choose the Right Partition Strategy

**Scenario:** You have a 200GB `customer_orders` table with columns: order_id, order_date, customer_id, region, amount. Queries are 90% date-range queries for the last 1-3 months. You also need to purge data older than 3 years. What partition strategy do you choose?

<details>
<summary>💡 Hint</summary>

Match the partition key to the most common filter: if 90% of queries filter on `order_date`, that's your key. Between range and interval, prefer *interval* partitioning (a range variant) — it auto-creates monthly partitions without manual maintenance. The killer feature for this use case is data lifecycle: you can `DROP PARTITION` for old data in milliseconds instead of a slow `DELETE`. Hash partitioning would distribute data evenly but doesn't help with date-range pruning.

</details>

<details>
<summary>✅ Solution</summary>

**Best choice: Range-Interval on `order_date`**

```sql
CREATE TABLE customer_orders (
  order_id    NUMBER,
  order_date  DATE NOT NULL,
  customer_id NUMBER,
  region      VARCHAR2(20),
  amount      NUMBER(12,2)
)
PARTITION BY RANGE (order_date) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
  PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')
)
ENABLE ROW MOVEMENT;

-- Local index on region (for region filter queries)
CREATE INDEX idx_co_region ON customer_orders(region) LOCAL;

-- Local index on customer_id (for customer lookup)
CREATE INDEX idx_co_customer ON customer_orders(customer_id, order_date) LOCAL;
```

**Why Range on order_date:**
- 90% of queries filter on `order_date` → partition pruning eliminates 97%+ of data
- Monthly interval: ~36 partitions for 3 years → manageable
- Purge by dropping oldest partition (instant, vs DELETE which reads/logs all rows)
- Auto-creates new monthly partitions → no manual management

**Why NOT hash:** No natural grouping for range queries; hash doesn't help with date ranges.
**Why NOT list on region:** Regions can have very different row counts (skew); date queries still scan all region partitions.

**Purge automation:**
```sql
-- Drop partitions older than 3 years (monthly job)
ALTER TABLE customer_orders DROP PARTITION p2021_01 UPDATE GLOBAL INDEXES;
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Not Using Partition Pruning

**Scenario:** You partitioned `sales` by `sale_date` (monthly partitions). A developer reports their query is doing a full scan across all 48 partitions even though it has a date filter. What are the possible causes?

<details>
<summary>💡 Hint</summary>

Partition pruning fails when Oracle can't determine at parse time *which* partition boundaries the filter touches. Check the explain plan — look at Pstart and Pstop columns. If it says KEY or ALL, pruning didn't work. The two most common causes: (1) the WHERE clause wraps the partition column in a function (`TRUNC(sale_date)`, `TO_CHAR(sale_date, 'YYYY')`) — Oracle can't reverse the function to find partition bounds, and (2) using a bind variable whose value isn't known at parse time — Oracle must scan all partitions to be safe. Fix: use the column directly in the filter, or create a function-based partition key that matches the function used in queries.

</details>

<details>
<summary>✅ Solution</summary>

**Run the explain plan first:**
```sql
EXPLAIN PLAN FOR <the slow query>;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
-- Look for: Pstart and Pstop values
-- ALL partitions scanned → Pstart=1, Pstop=48
```

**Cause 1: Function on the partition key column**
```sql
-- BAD: TRUNC() prevents partition pruning
SELECT * FROM sales WHERE TRUNC(sale_date) = DATE '2024-06-15';
-- Oracle can't map TRUNC(sale_date) to partition boundaries

-- GOOD: range predicate on the column directly
SELECT * FROM sales WHERE sale_date >= DATE '2024-06-15' AND sale_date < DATE '2024-06-16';
```

**Cause 2: Implicit type conversion**
```sql
-- BAD: sale_date is DATE but comparing to a string literal
SELECT * FROM sales WHERE sale_date = '15-JUN-2024';
-- TO_DATE(sale_date) happens implicitly → no pruning

-- GOOD:
SELECT * FROM sales WHERE sale_date = DATE '2024-06-15';
```

**Cause 3: Bind variable with wrong type**
```python
# BAD: passing string instead of date
cursor.execute("SELECT * FROM sales WHERE sale_date = :dt", dt="2024-06-15")

# GOOD: pass a datetime object
import datetime
cursor.execute("SELECT * FROM sales WHERE sale_date = :dt", dt=datetime.date(2024, 6, 15))
```

**Cause 4: OR condition mixing partition key with other columns**
```sql
-- BAD: OR prevents pruning
SELECT * FROM sales WHERE sale_date = DATE '2024-06-15' OR customer_id = 42;

-- Better: use UNION ALL if possible
SELECT * FROM sales WHERE sale_date = DATE '2024-06-15'
UNION ALL
SELECT * FROM sales WHERE customer_id = 42 AND sale_date != DATE '2024-06-15';
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Online Repartitioning of a Live Production Table

**Scenario:** You need to repartition a live 2TB `transactions` table from hash partitioning to range-interval monthly partitioning. The table can't have downtime. How do you do it?

<details>
<summary>💡 Hint</summary>

`DBMS_REDEFINITION` is Oracle's zero-downtime table restructuring mechanism. The pattern: create an interim table with the target partition structure, call `START_REDEF_TABLE` to begin copying data (production table stays online), call `SYNC_INTERIM_TABLE` periodically to catch up on DML changes, then `FINISH_REDEF_TABLE` which atomically swaps the two tables (a brief lock at the end, not a full downtime). Verify with `CAN_REDEF_TABLE` first — primary key or ROWID requirement must be met. The main risk is the space requirement: you need ~2TB free for the interim copy.

</details>

<details>
<summary>✅ Solution</summary>

**Use Online Redefinition (DBMS_REDEFINITION) — zero-downtime repartitioning:**

```sql
-- Step 1: Verify the table can be redefined online
BEGIN
  DBMS_REDEFINITION.CAN_REDEF_TABLE('FINANCE_SCHEMA', 'TRANSACTIONS');
END;
/
-- Raises error if not redefinable; otherwise succeeds silently

-- Step 2: Create the interim (target) table with new partitioning
CREATE TABLE transactions_new (
  txn_id      NUMBER,
  txn_date    DATE NOT NULL,
  account_id  NUMBER,
  amount      NUMBER(15,2),
  status      VARCHAR2(20)
)
PARTITION BY RANGE (txn_date) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION p_init VALUES LESS THAN (DATE '2020-01-01'))
TABLESPACE USERS;

-- Step 3: Start online redefinition
-- This copies data from old table to new, setting up a change tracking mechanism
BEGIN
  DBMS_REDEFINITION.START_REDEF_TABLE(
    uname       => 'FINANCE_SCHEMA',
    orig_table  => 'TRANSACTIONS',
    int_table   => 'TRANSACTIONS_NEW'
  );
END;
/
-- At this point: original table remains fully accessible for DML
-- Oracle tracks changes in the background

-- Step 4: Copy dependent objects (indexes, constraints, triggers, grants)
DECLARE
  num_errors PLS_INTEGER;
BEGIN
  DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS(
    uname            => 'FINANCE_SCHEMA',
    orig_table       => 'TRANSACTIONS',
    int_table        => 'TRANSACTIONS_NEW',
    copy_indexes     => DBMS_REDEFINITION.CONS_ORIG_PARAMS,
    copy_triggers    => TRUE,
    copy_constraints => TRUE,
    copy_privileges  => TRUE,
    ignore_errors    => FALSE,
    num_errors       => num_errors
  );
  DBMS_OUTPUT.PUT_LINE('Errors: ' || num_errors);
END;
/

-- Step 5: Optionally sync interim table periodically (reduces final lock time)
BEGIN
  DBMS_REDEFINITION.SYNC_INTERIM_TABLE('FINANCE_SCHEMA', 'TRANSACTIONS', 'TRANSACTIONS_NEW');
END;
/
-- Run this a few times as the final cutover approaches

-- Step 6: Finish redefinition (brief lock — seconds, not minutes)
-- Oracle applies final deltas and swaps table names atomically
BEGIN
  DBMS_REDEFINITION.FINISH_REDEF_TABLE('FINANCE_SCHEMA', 'TRANSACTIONS', 'TRANSACTIONS_NEW');
END;
/
-- After FINISH: TRANSACTIONS is now the partitioned table; TRANSACTIONS_NEW is the old table

-- Step 7: Drop the old table (now named TRANSACTIONS_NEW)
DROP TABLE FINANCE_SCHEMA.TRANSACTIONS_NEW PURGE;

-- Step 8: Gather stats on the newly partitioned table
EXEC DBMS_STATS.GATHER_TABLE_STATS('FINANCE_SCHEMA', 'TRANSACTIONS', degree => 16);
```

**Key points for the interview:**
- DBMS_REDEFINITION maintains the original table fully online during the copy phase
- The final cutover lock lasts seconds (only time the table is locked)
- Monitor progress via `dba_redefinition_progress` (19c+) or by checking new table row counts
- If something goes wrong during redefinition: `DBMS_REDEFINITION.ABORT_REDEF_TABLE` to clean up
- Always test on a dev environment first — the procedure can fail on complex table structures

</details>

</article>