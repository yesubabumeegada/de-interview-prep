---
title: "SQL Transaction Isolation - Intermediate"
topic: sql
subtopic: transaction-isolation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, transactions, isolation, mvcc, deadlocks, locking, snapshot-isolation, serializable]
---

# SQL Transaction Isolation — Intermediate Concepts

## MVCC Deep Dive

Multi-Version Concurrency Control is the mechanism PostgreSQL, Oracle, and MySQL InnoDB use to provide isolation without reader-writer blocking.

### How PostgreSQL Implements MVCC

Every row in PostgreSQL has hidden system columns:
- `xmin` — transaction ID that created this row version
- `xmax` — transaction ID that deleted/updated this row version (0 if still live)

```sql
-- See MVCC internals (PostgreSQL):
SELECT *, xmin, xmax FROM orders WHERE order_id = 101;
-- xmin: 12345  (created by transaction 12345)
-- xmax: 0      (not yet deleted — still live)

-- After an UPDATE:
-- PostgreSQL marks the old row's xmax = current_txn_id
-- Inserts a NEW row with xmin = current_txn_id
-- Both versions exist on disk until VACUUM removes the dead version

-- Each transaction has a snapshot: "which transaction IDs are committed as of my start"
-- Reading a row: visible if xmin is committed AND xmax is 0 (or xmax is not yet committed)
```

**VACUUM** is PostgreSQL's garbage collector — it removes dead row versions (old xmax'd rows) and frees space:

```sql
-- Manual vacuum (usually handled automatically by autovacuum):
VACUUM orders;           -- Remove dead tuples, but don't shrink file
VACUUM FULL orders;      -- Rewrite the table file (compacts disk space, but locks table)
VACUUM ANALYZE orders;   -- Vacuum + update query planner statistics

-- View dead tuple accumulation:
SELECT relname, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum
FROM pg_stat_user_tables
WHERE relname = 'orders';
```

---

## Snapshot Isolation vs Serializable Snapshot Isolation

### Snapshot Isolation (SI)

Each transaction works from a consistent snapshot taken at the start of the transaction. No other transaction's concurrent changes are visible.

```sql
-- PostgreSQL REPEATABLE READ implements Snapshot Isolation
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
-- PostgreSQL takes a snapshot here: "see all data as of this moment"

SELECT SUM(balance) FROM accounts WHERE account_id IN (1, 2);
-- Returns 1000 (A: 600, B: 400)

-- Meanwhile, Transaction B: MOVE $100 from account 1 to account 2 and commits
-- Account 1: 500, Account 2: 500 (total still 1000)

SELECT SUM(balance) FROM accounts WHERE account_id IN (1, 2);
-- Still returns 1000 — sees the snapshot, not B's changes
COMMIT;
```

**Write Skew Anomaly** — the weakness of Snapshot Isolation:

```sql
-- Two doctors both check "is there at least one doctor on call?"
-- Both see yes (Doctor A and Doctor B are on call)
-- Both decide to go off call (each thinks the other is still on)
-- Both commit — now NO doctor is on call!

-- Transaction A (Doctor A going off call):
BEGIN;  -- Isolation: REPEATABLE READ
SELECT COUNT(*) FROM on_call WHERE status = 'on_call';  -- Returns 2
UPDATE on_call SET status = 'off' WHERE doctor_id = 'A';
COMMIT;

-- Transaction B (Doctor B going off call, concurrently with A):
BEGIN;  -- Isolation: REPEATABLE READ
SELECT COUNT(*) FROM on_call WHERE status = 'on_call';  -- Also returns 2 (snapshot before A committed)
UPDATE on_call SET status = 'off' WHERE doctor_id = 'B';
COMMIT;

-- Result: Both doctors are off call — constraint violated!
-- Write Skew: each transaction reads data that the OTHER transaction modifies
-- REPEATABLE READ cannot prevent this
```

### Serializable Snapshot Isolation (SSI)

PostgreSQL 9.1+ implements True Serializable using SSI — it detects write skew and related anomalies and aborts one of the conflicting transactions:

```sql
-- Fix: use SERIALIZABLE isolation
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM on_call WHERE status = 'on_call';  -- Returns 2
UPDATE on_call SET status = 'off' WHERE doctor_id = 'A';
COMMIT;

-- If Transaction B ran concurrently with Transaction A:
-- PostgreSQL detects the dependency cycle: A read what B writes, B read what A writes
-- One of them gets: ERROR: could not serialize access due to read/write dependencies
-- The application retries the failed transaction
```

---

## Locking in Depth

### Row-Level Locks

```sql
-- SELECT FOR UPDATE: acquires an exclusive row lock
-- Other transactions cannot update/delete/lock these rows until you commit
BEGIN;
SELECT * FROM inventory WHERE product_id = 42 FOR UPDATE;
-- Row is locked — concurrent: SELECT FOR UPDATE on same row will WAIT

-- SELECT FOR SHARE: acquires a shared row lock
-- Other transactions can read but cannot modify
SELECT * FROM inventory WHERE product_id = 42 FOR SHARE;

-- SELECT FOR UPDATE SKIP LOCKED: skip rows already locked (non-blocking)
-- Useful for job queues: each worker picks available jobs
SELECT job_id, payload FROM job_queue WHERE status = 'pending'
FOR UPDATE SKIP LOCKED
LIMIT 1;
-- If all 'pending' jobs are locked by other workers, returns 0 rows immediately
-- No waiting!

-- SELECT FOR UPDATE NOWAIT: immediately error if rows are locked
SELECT * FROM accounts WHERE id = 1 FOR UPDATE NOWAIT;
-- ERROR: could not obtain lock on row in relation "accounts"
```

### Table-Level Locks

```sql
-- PostgreSQL lock modes (from least to most restrictive):
-- ACCESS SHARE     → SELECT (allows most concurrent operations)
-- ROW SHARE        → SELECT FOR UPDATE/SHARE
-- ROW EXCLUSIVE    → INSERT, UPDATE, DELETE
-- SHARE UPDATE EXCLUSIVE → VACUUM, CREATE INDEX CONCURRENTLY
-- SHARE            → CREATE INDEX (non-concurrent)
-- SHARE ROW EXCLUSIVE → CREATE TRIGGER, ALTER TABLE
-- EXCLUSIVE        → Rare; blocks all except ACCESS SHARE
-- ACCESS EXCLUSIVE → ALTER TABLE, DROP TABLE, VACUUM FULL (blocks everything!)

-- Explicitly lock a table:
BEGIN;
LOCK TABLE orders IN ACCESS EXCLUSIVE MODE;  -- Blocks all other access
-- Perform schema changes...
COMMIT;
```

### Deadlocks

A deadlock occurs when Transaction A holds a lock needed by B, and B holds a lock needed by A — both wait forever.

```sql
-- Transaction A:                   Transaction B:
BEGIN;                               BEGIN;
UPDATE accounts SET balance=...      UPDATE accounts SET balance=...
WHERE account_id = 1;                WHERE account_id = 2;  ← B gets lock on 2
-- (A holds lock on 1)
UPDATE accounts SET balance=...      UPDATE accounts SET balance=...
WHERE account_id = 2;  ← A WAITS    WHERE account_id = 1;  ← B WAITS
-- DEADLOCK! Database detects and kills one transaction with:
-- ERROR: deadlock detected
-- DETAIL: Process X waits for ShareLock on transaction Y; blocked by process Z
```

**Preventing deadlocks:**

```sql
-- Rule 1: Always lock rows in a consistent order
-- Both transactions should lock account_id = 1 first, then 2:
UPDATE accounts SET balance = balance - 100 WHERE account_id = LEAST(from_id, to_id);
UPDATE accounts SET balance = balance + 100 WHERE account_id = GREATEST(from_id, to_id);

-- Rule 2: Lock all needed rows at once (SELECT FOR UPDATE with order)
BEGIN;
SELECT * FROM accounts WHERE account_id IN (1, 2) ORDER BY account_id FOR UPDATE;
-- Locks both rows in deterministic order — no deadlock possible
UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;
COMMIT;

-- Rule 3: Use NOWAIT to fail fast and retry with backoff
BEGIN;
SELECT * FROM accounts WHERE account_id = 1 FOR UPDATE NOWAIT;
-- Immediately get an error if locked, rather than waiting indefinitely
```

---

## SQL Server Specific: SNAPSHOT and READ COMMITTED SNAPSHOT

SQL Server offers two additional isolation modes not in the SQL standard:

```sql
-- Enable Snapshot Isolation at the database level:
ALTER DATABASE MyDB SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE MyDB SET READ_COMMITTED_SNAPSHOT ON;
-- READ_COMMITTED_SNAPSHOT: changes READ COMMITTED to use row versions
-- instead of locks — readers don't block writers!

-- Transaction using SNAPSHOT isolation:
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;
SELECT balance FROM accounts WHERE account_id = 1;
-- ... other work ...
SELECT balance FROM accounts WHERE account_id = 1;
-- Returns same value as first read (snapshot) even if committed changes occurred
COMMIT;
```

| SQL Server Isolation | Blocking | Dirty Read | Non-Repeatable Read | Phantom |
|---------------------|---------|-----------|--------------------|----|
| READ COMMITTED | Readers block | No | Yes | Yes |
| READ COMMITTED SNAPSHOT | No blocking | No | Yes | Yes |
| SNAPSHOT | No blocking | No | No | No |
| SERIALIZABLE | Readers block | No | No | No |

> **SQL Server tip:** Enable `READ_COMMITTED_SNAPSHOT ON` for most OLTP databases — it gives you the consistency of READ COMMITTED without reader-writer blocking. The default READ COMMITTED uses locks that cause contention.

---

## Advisory Locks (PostgreSQL)

Advisory locks are application-level locks that you control explicitly — useful for distributed coordination:

```sql
-- Application-level lock (non-table-specific)
-- Good for: ensuring only one process runs a scheduled job at a time

-- Try to acquire lock (non-blocking):
SELECT pg_try_advisory_lock(12345);  -- Returns true if acquired, false if already taken

-- Acquire lock (blocking):
SELECT pg_advisory_lock(12345);  -- Waits until the lock is available

-- Release lock:
SELECT pg_advisory_unlock(12345);

-- Practical pattern: only one instance of a scheduled job runs at a time
BEGIN;
IF pg_try_advisory_lock(hashtext('nightly_etl')) THEN
    -- Run the ETL
    CALL run_nightly_etl();
    PERFORM pg_advisory_unlock(hashtext('nightly_etl'));
ELSE
    RAISE NOTICE 'Another process is already running nightly_etl — skipping';
END IF;
COMMIT;
```

---

## Transaction Isolation in Cloud Data Warehouses

### Snowflake

Snowflake uses multi-version concurrency control with REPEATABLE READ as the default (renamed "READ COMMITTED" in their docs but with snapshot semantics):

```sql
-- Snowflake: each statement sees a consistent snapshot of committed data
-- Transactions within Snowflake are session-scoped
BEGIN;
SELECT COUNT(*) FROM orders WHERE status = 'pending';
-- Even if other sessions INSERT/UPDATE orders between BEGIN and here,
-- this transaction sees a consistent snapshot
COMMIT;
```

### BigQuery

BigQuery doesn't have traditional transactions for DML (INSERT/UPDATE/DELETE is per-statement atomic), but it does support multi-statement transactions in BigQuery Studio:

```sql
-- BigQuery: multi-statement transaction (Storage Write API or interactive)
BEGIN TRANSACTION;
UPDATE orders SET status = 'shipped' WHERE order_id = 101;
UPDATE shipments SET tracking_number = 'TRACK123' WHERE order_id = 101;
COMMIT TRANSACTION;
```

---

## Interview Tips

> **Tip 1:** "What is write skew and which isolation level prevents it?" — "Write skew is a concurrency anomaly where two transactions each read a set of rows, make decisions based on what they read, and update different rows based on those decisions — resulting in an inconsistent state that neither transaction could have created alone. The classic example is two doctors both going off-call after verifying coverage. SERIALIZABLE (or SSI in PostgreSQL) prevents write skew. REPEATABLE READ does not."

> **Tip 2:** "How does MVCC eliminate reader-writer blocking?" — "MVCC keeps multiple versions of each row on disk. A reader sees the version of the row that was current at the start of their transaction (the snapshot). A writer creates a new version. They work on different versions simultaneously — no contention. The trade-off is storage for old row versions (dead tuples in PostgreSQL) which VACUUM reclaims."

> **Tip 3:** "How do you use SELECT FOR UPDATE SKIP LOCKED in practice?" — "It's the standard pattern for job queues. Multiple worker processes each run `SELECT job_id FROM jobs WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1`. Each gets a different unlocked job. If all jobs are being processed, workers immediately see zero rows and can sleep briefly before retrying. This avoids the thundering herd of all workers fighting to lock the same rows."
