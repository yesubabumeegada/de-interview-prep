---
title: "SQL Transaction Isolation - Fundamentals"
topic: sql
subtopic: transaction-isolation
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, transactions, isolation-levels, acid, dirty-read, phantom-read, concurrency]
---

# SQL Transaction Isolation — Fundamentals


## 🎯 Analogy

Think of transaction isolation levels like library reading rules: READ UNCOMMITTED lets you peek at someone's draft notes (dirty reads). SERIALIZABLE forces everyone to queue up and take turns — perfectly consistent but slower.

---
## What Is a Transaction?

A **transaction** is a group of SQL statements that execute as a single atomic unit. Either all statements succeed, or none of them are applied. This is the foundation of data integrity in relational databases.

> **Analogy:** A bank transfer is the classic example. Transferring $100 from Account A to Account B requires two steps: debit A by $100, credit B by $100. If step 1 succeeds but step 2 fails (server crash), you'd have $100 disappear. A transaction wraps both steps — either both happen or neither does.

---

## ACID Properties

Transactions guarantee four properties:

| Property | Meaning | Example |
|----------|---------|---------|
| **Atomicity** | All-or-nothing | Transfer debit and credit both succeed or both roll back |
| **Consistency** | Database moves from one valid state to another | Account balance can't go negative (if constrained) |
| **Isolation** | Concurrent transactions don't see each other's intermediate states | Two simultaneous transfers don't interfere |
| **Durability** | Committed data survives crashes | Committed transfer survives a power outage |

---

## Basic Transaction Syntax

```sql
-- PostgreSQL / MySQL / SQL Server
BEGIN;  -- Or: START TRANSACTION;

UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;

-- If both succeed:
COMMIT;

-- If something goes wrong:
ROLLBACK;  -- Undo all changes since BEGIN
```

```sql
-- SQL Server: implicit transaction
BEGIN TRANSACTION;
UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
IF @@ERROR != 0 ROLLBACK TRANSACTION;
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;
IF @@ERROR != 0 ROLLBACK TRANSACTION;
COMMIT TRANSACTION;
```

**Autocommit:** By default, most databases run in "autocommit" mode — each statement is its own transaction. Using `BEGIN` disables autocommit for the session until COMMIT or ROLLBACK.

---

## Concurrency Problems

When multiple transactions run simultaneously, they can interfere in these ways:

### Problem 1: Dirty Read

Transaction A reads data that Transaction B has modified but not yet committed. If B rolls back, A has read "phantom" data that never officially existed.

```
Time  Transaction A                    Transaction B
----  -------------------------------- --------------------------------
T1                                     UPDATE accounts SET balance=50 WHERE id=1
T2    SELECT balance FROM accounts      -- (reads 50 — the uncommitted value!)
      WHERE id=1  → returns 50
T3                                     ROLLBACK; (balance stays at 100)
T4    A believes balance is 50 — WRONG!
```

### Problem 2: Non-Repeatable Read

Transaction A reads the same row twice and gets different values because Transaction B modified and committed between A's reads.

```
Time  Transaction A                    Transaction B
----  -------------------------------- --------------------------------
T1    SELECT balance FROM accounts     
      WHERE id=1  → returns 100
T2                                     UPDATE accounts SET balance=50 WHERE id=1; COMMIT;
T3    SELECT balance FROM accounts     
      WHERE id=1  → returns 50   ← Different result!
```

### Problem 3: Phantom Read

Transaction A executes a range query twice. Between the reads, Transaction B inserts or deletes rows matching the range — changing the set of rows returned.

```
Time  Transaction A                         Transaction B
----  ------------------------------------- --------------------------------
T1    SELECT COUNT(*) FROM orders
      WHERE amount > 1000  → returns 5
T2                                          INSERT INTO orders(amount) VALUES (1500); COMMIT;
T3    SELECT COUNT(*) FROM orders
      WHERE amount > 1000  → returns 6  ← New "phantom" row appeared!
```

---

## Isolation Levels

SQL defines four standard isolation levels that trade concurrency for consistency:

| Isolation Level | Dirty Read | Non-Repeatable Read | Phantom Read |
|----------------|-----------|--------------------|--------------| 
| **READ UNCOMMITTED** | Possible | Possible | Possible |
| **READ COMMITTED** | Prevented | Possible | Possible |
| **REPEATABLE READ** | Prevented | Prevented | Possible |
| **SERIALIZABLE** | Prevented | Prevented | Prevented |

Higher isolation = more consistent, but more potential for locking and lower concurrency.

---

## Each Isolation Level Explained

### READ UNCOMMITTED — Lowest Isolation

```sql
-- PostgreSQL (note: PostgreSQL doesn't actually implement dirty reads — 
--  READ UNCOMMITTED behaves like READ COMMITTED)
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
BEGIN;
SELECT * FROM accounts;  -- Could see uncommitted changes (in SQL Server/MySQL)
COMMIT;
```

**Use case:** Almost never in practice. Valid for non-critical read operations where approximate results are acceptable (e.g., row count estimates on a reporting database).

### READ COMMITTED — Default in Most Databases

```sql
-- This is the default in PostgreSQL, Oracle, SQL Server
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
SELECT balance FROM accounts WHERE id = 1;  -- Only sees committed data
-- ...time passes, another transaction commits a change...
SELECT balance FROM accounts WHERE id = 1;  -- May see a different value!
COMMIT;
```

**Prevents:** Dirty reads
**Allows:** Non-repeatable reads, phantom reads
**Default in:** PostgreSQL, Oracle, SQL Server, MySQL (except InnoDB default)

### REPEATABLE READ — Default in MySQL InnoDB

```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
SELECT balance FROM accounts WHERE id = 1;  -- Returns 100
-- Even if another transaction commits balance = 50, this transaction still sees 100
SELECT balance FROM accounts WHERE id = 1;  -- Still returns 100
COMMIT;
```

**Prevents:** Dirty reads, non-repeatable reads
**Allows:** Phantom reads (new rows can appear in range queries)
**Default in:** MySQL InnoDB

### SERIALIZABLE — Highest Isolation

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
SELECT COUNT(*) FROM orders WHERE amount > 1000;  -- Returns 5
-- Even if another transaction inserts a qualifying row and commits...
SELECT COUNT(*) FROM orders WHERE amount > 1000;  -- Still returns 5!
COMMIT;
```

**Prevents:** All concurrency anomalies
**Cost:** Highest locking overhead; lowest concurrency; potential for deadlocks
**Use case:** Financial transactions, inventory management, any operation requiring absolute consistency

---

## Setting Isolation Levels

```sql
-- Session-level (PostgreSQL):
SET default_transaction_isolation = 'repeatable read';

-- Transaction-level (PostgreSQL):
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- SQL Server:
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;  -- SQL Server-specific (between Repeatable Read and Serializable)

-- MySQL:
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

-- Check current level:
SHOW TRANSACTION ISOLATION LEVEL;  -- MySQL
SHOW transaction_isolation;         -- PostgreSQL
SELECT current_setting('transaction_isolation');  -- PostgreSQL
```

---

## Default Isolation Levels by Database

| Database | Default Isolation Level |
|---------|------------------------|
| PostgreSQL | READ COMMITTED |
| MySQL InnoDB | REPEATABLE READ |
| SQL Server | READ COMMITTED |
| Oracle | READ COMMITTED |
| SQLite | SERIALIZABLE (effectively) |
| Snowflake | READ COMMITTED |
| BigQuery | Effectively SERIALIZABLE (per-statement) |

---

## Locks vs MVCC

Databases implement isolation in two primary ways:

**Lock-based (SQL Server, MySQL with locks):**
- Readers block writers, writers block readers
- Higher isolation = more locks = more waiting

**MVCC — Multi-Version Concurrency Control (PostgreSQL, Oracle, MySQL InnoDB):**
- Each transaction sees a snapshot of the data at the time it started
- Readers never block writers; writers never block readers
- "Old" versions of rows are kept until no transaction needs them (VACUUM in PostgreSQL)

```sql
-- PostgreSQL: readers never block writers (MVCC)
-- Transaction A:
BEGIN;
SELECT * FROM orders WHERE customer_id = 1;  -- Reads snapshot version

-- Simultaneously, Transaction B:
BEGIN;
UPDATE orders SET status = 'shipped' WHERE order_id = 101;  -- COMMITS immediately
COMMIT;

-- Back in Transaction A (still in progress):
SELECT * FROM orders WHERE customer_id = 1;
-- READ COMMITTED: sees B's committed change
-- REPEATABLE READ: sees the original snapshot (before B's change)
COMMIT;
```

---


## ▶️ Try It Yourself

```sql
-- Check current isolation level (Postgres)
SHOW transaction_isolation;

-- Set isolation level for a transaction
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- Simulate a bank transfer (must be atomic)
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- Both succeed or both fail

-- Demonstrate dirty read prevention (READ COMMITTED — Postgres default)
-- Session 1: BEGIN; UPDATE orders SET amount = 999 WHERE id = 1;
-- Session 2: SELECT amount FROM orders WHERE id = 1;
-- → Session 2 sees original value (READ COMMITTED protects from dirty reads)
-- Session 1: COMMIT;
-- → Now session 2 sees 999
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What does ACID stand for and which property does isolation level control?" — "ACID stands for Atomicity, Consistency, Isolation, Durability. Isolation levels control the I in ACID — they determine how much one transaction can see another's in-progress changes. Higher isolation levels prevent more concurrency anomalies but reduce throughput because more locking or versioning is required."

> **Tip 2:** "What's the difference between a dirty read and a phantom read?" — "A dirty read is reading uncommitted data from another transaction — data that might be rolled back. A phantom read is when the same range query run twice returns a different set of rows because another committed transaction inserted or deleted rows between the two reads. Dirty reads involve reading modified but not committed data; phantom reads involve the set of matching rows changing."

> **Tip 3:** "What isolation level does PostgreSQL use by default and why?" — "PostgreSQL defaults to READ COMMITTED, which prevents dirty reads but allows non-repeatable reads and phantom reads. This gives good concurrency — readers don't block writers thanks to MVCC. For most OLTP workloads, READ COMMITTED is sufficient. When you need repeatable results within a transaction (reporting, financial aggregations), you'd explicitly use REPEATABLE READ or SERIALIZABLE."
