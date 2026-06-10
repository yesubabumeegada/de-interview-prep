---
title: "SQL Transaction Isolation - Scenario Questions"
topic: sql
subtopic: transaction-isolation
content_type: scenario_question
tags: [sql, transactions, isolation, interview, scenarios, concurrency, deadlocks]
---

# Scenario Questions — SQL Transaction Isolation

<article data-difficulty="junior">

## 🟢 Junior: Identify the Concurrency Problem

**Scenario:** A small e-commerce site runs this code for each order:

```sql
-- Step 1: Check inventory
SELECT quantity FROM inventory WHERE product_id = 99;
-- Returns: 1 (one item left)

-- Step 2: If quantity > 0, book it
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 99;
COMMIT;
```

100 users simultaneously try to buy the last item. After all requests complete, the inventory shows `quantity = -97`. What concurrency problem occurred and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about what happens when 100 transactions simultaneously read `quantity = 1`, all decide "there's stock available," and all proceed to decrement. Each reads the same original value. This is the check-then-act race condition. The fix needs to make the check and the update atomic.

</details>

<details>
<summary>✅ Solution</summary>

**The problem: Check-then-Act Race Condition (Lost Update)**

```
User A reads: quantity = 1 → "available"
User B reads: quantity = 1 → "available"  (before A commits)
User C reads: quantity = 1 → "available"  (before A or B commit)
... all 100 users read 1 ...

User A: UPDATE → quantity = 0. COMMIT.
User B: UPDATE → quantity = -1. COMMIT.
... all 100 update and commit ...
Final: quantity = -99
```

**Fix 1: Combine check and update (most common)**

```sql
BEGIN;
-- Atomic conditional update: only decrease if quantity > 0
UPDATE inventory 
SET quantity = quantity - 1 
WHERE product_id = 99 AND quantity > 0;

-- Check if the update actually happened (0 rows = out of stock)
-- In PostgreSQL:
GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
IF v_rows_affected = 0 THEN
    ROLLBACK;
    RAISE EXCEPTION 'Product 99 is out of stock';
END IF;
COMMIT;
```

**Why this works:** The `WHERE quantity > 0` condition inside the UPDATE is evaluated atomically with the update. If 100 transactions all try this simultaneously, the database's row-level locking ensures they serialize — the first gets quantity = 0, the remaining 99 rows match the WHERE clause fail (quantity is now 0), and GET DIAGNOSTICS shows 0 rows affected.

**Fix 2: SELECT FOR UPDATE (explicit pessimistic locking)**

```sql
BEGIN;
SELECT quantity FROM inventory WHERE product_id = 99 FOR UPDATE;
-- This row is now locked — all other transactions wait here

-- Check quantity in application code:
IF quantity <= 0 THEN
    ROLLBACK;
    RETURN 'out_of_stock';
END IF;

UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 99;
COMMIT;
-- Lock released; next waiting transaction proceeds
```

**Fix 3: Add a CHECK constraint as a safety net**

```sql
ALTER TABLE inventory ADD CONSTRAINT chk_quantity_non_negative CHECK (quantity >= 0);
-- Now even if the application has a bug, the database rejects updates that go negative
-- The UPDATE in Fix 1 would raise a constraint violation rather than going to -97
```

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Explain ACID With a Real Example

**Scenario:** Explain each ACID property using a bank transfer of $500 from Account A ($1000) to Account B ($200). For each property, describe what would go wrong if it were violated.

<details>
<summary>💡 Hint</summary>

Walk through each letter: Atomicity (all-or-nothing), Consistency (valid states only), Isolation (concurrent transactions don't interfere), Durability (committed data survives crashes). For each, imagine a specific failure scenario.

</details>

<details>
<summary>✅ Solution</summary>

```sql
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE account_id = 'A';  -- A: 1000 → 500
UPDATE accounts SET balance = balance + 500 WHERE account_id = 'B';  -- B: 200 → 700
COMMIT;
```

**Atomicity:** Either both UPDATEs happen or neither does.

Without it: If the server crashes after debiting A but before crediting B, A has $500 less and B never received it — $500 disappears. With atomicity: the ROLLBACK restores A to $1000.

**Consistency:** The database moves from one valid state to another.

Without it: If A only has $200, we could transfer $500 and leave A with -$300, violating the business rule that balances can't go negative. With consistency: the `CHECK (balance >= 0)` constraint rejects the transaction, maintaining the rule.

**Isolation:** Concurrent transactions don't see each other's incomplete changes.

```
Without it (dirty read scenario):
Transaction A:                     Transaction C (auditor):
Debit A: 1000 → 500
                                   SELECT SUM(balance) → 500 + 200 = 700 (missing $500!)
Credit B: 200 → 700
COMMIT
```

With isolation (READ COMMITTED): C sees either the before state (1000+200=1200) or the after state (500+700=1200) — never an intermediate inconsistent state.

**Durability:** Once COMMIT succeeds, the change is permanent.

Without it: The server confirms "Transfer complete" but crashes before writing to disk. On restart, the transfer is gone — A and B both revert to original balances. With durability: committed changes are written to the WAL (write-ahead log) on durable storage BEFORE the COMMIT acknowledgment is sent to the client.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Debug a Deadlock

**Scenario:** Your application logs show recurring errors:

```
ERROR: deadlock detected
DETAIL: Process 23456 waits for ShareLock on transaction 78901; blocked by process 34567.
Process 34567 waits for ShareLock on transaction 23456; blocked by process 23456.
HINT: See server log for query details.
```

The error happens during the payment checkout flow. Two concurrent checkouts are each updating the same two tables. Here's the code:

```sql
-- Checkout process for Transaction A (Order 101):
BEGIN;
UPDATE orders SET status = 'processing' WHERE order_id = 101;
UPDATE inventory SET reserved = reserved + 1 WHERE product_id = 42;
COMMIT;

-- Checkout process for Transaction B (Order 102):
BEGIN;
UPDATE inventory SET reserved = reserved + 1 WHERE product_id = 42;
UPDATE orders SET status = 'processing' WHERE order_id = 102;
COMMIT;
```

Explain why the deadlock occurs and provide the fix.

<details>
<summary>💡 Hint</summary>

Draw the lock dependency graph. Transaction A holds a lock on `orders` and waits for a lock on `inventory`. Transaction B holds a lock on `inventory` and waits for a lock on `orders`. They're each waiting for what the other holds.

</details>

<details>
<summary>✅ Solution</summary>

**Why the deadlock occurs:**

```
Time  Transaction A (Order 101)              Transaction B (Order 102)
T1    UPDATE orders WHERE order_id=101       
      → HOLDS lock on orders row 101
T2                                           UPDATE inventory WHERE product_id=42
                                             → HOLDS lock on inventory row 42
T3    UPDATE inventory WHERE product_id=42   
      → WAITS for B to release lock on inventory
T4                                           UPDATE orders WHERE order_id=102
                                             → WAITS for A to release lock on orders
→ DEADLOCK: A waits for B's inventory lock, B waits for A's orders lock
```

**Fix: Always acquire locks in a consistent order**

```sql
-- Fix: ALWAYS update orders FIRST, then inventory (consistent ordering)

-- Updated Transaction A:
BEGIN;
UPDATE orders SET status = 'processing' WHERE order_id = 101;       -- Orders first
UPDATE inventory SET reserved = reserved + 1 WHERE product_id = 42; -- Then inventory
COMMIT;

-- Updated Transaction B:
BEGIN;
UPDATE orders SET status = 'processing' WHERE order_id = 102;       -- Orders first (same order!)
UPDATE inventory SET reserved = reserved + 1 WHERE product_id = 42; -- Then inventory
COMMIT;
```

**Why this eliminates deadlock:**
- Both transactions lock orders first, then inventory
- If A gets the orders lock first → B waits. When A finishes, B proceeds
- If B gets the orders lock first → A waits. When B finishes, A proceeds
- Neither transaction can "cross-wait" because the order is consistent

**Alternative fix: Lock everything upfront**

```sql
BEGIN;
-- Acquire all needed locks at the start (in a defined order)
SELECT 1 FROM orders WHERE order_id = 101 FOR UPDATE;
SELECT 1 FROM inventory WHERE product_id = 42 FOR UPDATE;

-- Now perform updates (no waiting — locks already held)
UPDATE orders SET status = 'processing' WHERE order_id = 101;
UPDATE inventory SET reserved = reserved + 1 WHERE product_id = 42;
COMMIT;
```

**Detecting deadlocks in PostgreSQL:**
```sql
-- Check for locks and who's blocking whom:
SELECT 
    blocked.pid   AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid  AS blocking_pid,
    blocking.query AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking 
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid));
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Choose the Right Isolation Level

**Scenario:** For each of the following use cases, choose the appropriate isolation level (READ COMMITTED, REPEATABLE READ, or SERIALIZABLE) and justify your choice:

1. **Daily sales report:** Queries sales data for a report. The exact number doesn't need to be 100% precise — approximate is fine.
2. **End-of-month financial close:** Aggregates all account balances and generates official financial statements that will be audited.
3. **Real-time inventory check:** Before charging a customer, checks if the item is in stock and reserves it.
4. **Analytics dashboard:** Runs 10 complex queries to build a dashboard. All queries should see the same consistent snapshot of data.

<details>
<summary>💡 Hint</summary>

Think about what anomalies each scenario can tolerate: Can the report tolerate seeing different values if run twice? Can the financial close tolerate phantom rows appearing between aggregation queries? Does the inventory check need to prevent other transactions from modifying stock simultaneously?

</details>

<details>
<summary>✅ Solution</summary>

**1. Daily Sales Report → READ COMMITTED (default)**

```sql
-- READ COMMITTED is sufficient
-- Rationale: slight imprecision in a daily trend report is acceptable
-- A non-repeatable read (different value if same row read twice) is fine
-- for a report that only reads each row once
-- Higher isolation would cause unnecessary lock contention on a busy table

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
SELECT SUM(amount) FROM orders WHERE order_date = CURRENT_DATE - 1;
COMMIT;
```

**2. Financial Close → REPEATABLE READ or SERIALIZABLE**

```sql
-- REPEATABLE READ prevents inconsistent reads within the transaction
-- Each query in the financial close sees the same committed snapshot
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;

-- These two queries see the SAME snapshot (same total)
SELECT SUM(balance) FROM accounts WHERE account_type = 'checking';
SELECT SUM(balance) FROM accounts WHERE account_type = 'savings';
-- No phantom rows, no non-repeatable reads

-- If the close also modifies data (marks period as closed):
-- → Use SERIALIZABLE to prevent write skew (two processes closing the same period)
COMMIT;
```

**3. Inventory Check → READ COMMITTED + Explicit Locking**

```sql
-- Isolation level alone doesn't prevent the race condition
-- Need explicit row locking with FOR UPDATE:
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
SELECT quantity FROM inventory WHERE product_id = 42 FOR UPDATE;
-- Row is locked — concurrent inventory checks wait here
IF quantity > 0 THEN
    UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 42;
    -- Proceed with order
END IF;
COMMIT;
-- Or, atomically: UPDATE ... WHERE product_id = 42 AND quantity > 0
```

**4. Analytics Dashboard → REPEATABLE READ**

```sql
-- All 10 queries see the same consistent snapshot
-- Without REPEATABLE READ: query 1 might see 5,000 orders, query 10 might see 5,050
-- (if new orders arrive between queries) → inconsistent dashboard numbers

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
-- Snapshot taken here

SELECT ... FROM orders WHERE ...;    -- Query 1
SELECT ... FROM customers WHERE ...; -- Query 2
-- ... 8 more queries ...
-- All see the same data as of when the transaction started

COMMIT;
-- Snapshot released
```

**Summary table:**

| Use Case | Isolation Level | Why |
|---------|----------------|-----|
| Daily report | READ COMMITTED | Approximate is fine; lower cost |
| Financial close | REPEATABLE READ | Consistent snapshot across queries |
| Inventory reservation | READ COMMITTED + FOR UPDATE | Explicit lock + atomic update |
| Analytics dashboard | REPEATABLE READ | All queries see same snapshot |

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Safe Multi-Step Withdrawal System

**Scenario:** Design a PostgreSQL procedure for a bank withdrawal that:
1. Checks if the account has sufficient funds (including pending holds)
2. Creates a hold on the funds for 30 seconds while payment processing occurs
3. After payment processor confirms, converts the hold to a permanent debit
4. If payment processor times out, releases the hold
5. Handles concurrent withdrawal attempts on the same account safely

The system processes 500 withdrawals/second. Justify every isolation and locking decision.

<details>
<summary>💡 Hint</summary>

This requires a two-phase commit-like flow within a single database: create_hold → process_payment (external call) → commit_hold or release_hold. The hold creation must be atomic with the balance check. Use FOR UPDATE on the account row to serialize concurrent withdrawals. The external payment call happens OUTSIDE the transaction to avoid holding locks for the network call duration.

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Schema:
CREATE TABLE accounts (
    account_id    BIGINT PRIMARY KEY,
    available_balance NUMERIC(15,2) NOT NULL,
    held_balance      NUMERIC(15,2) NOT NULL DEFAULT 0,
    CHECK (available_balance >= 0),
    CHECK (held_balance >= 0)
);

CREATE TABLE balance_holds (
    hold_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id  BIGINT REFERENCES accounts(account_id),
    amount      NUMERIC(15,2) NOT NULL,
    status      TEXT DEFAULT 'active',  -- active | committed | released
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    expires_at  TIMESTAMPTZ DEFAULT NOW() + INTERVAL '30 seconds',
    payment_ref TEXT
);

-- Phase 1: Create hold (short transaction — no external calls)
CREATE OR REPLACE PROCEDURE create_withdrawal_hold(
    p_account_id BIGINT,
    p_amount     NUMERIC,
    OUT p_hold_id UUID
)
LANGUAGE plpgsql AS $$
DECLARE
    v_available NUMERIC;
BEGIN
    -- Lock the account row (short lock — no external calls in this transaction)
    SELECT available_balance INTO v_available
    FROM accounts WHERE account_id = p_account_id FOR UPDATE NOWAIT;
    -- NOWAIT: if another withdrawal is in progress, fail immediately (don't queue up)
    -- Caller retries with backoff → prevents lock queue buildup at 500 req/s

    IF v_available < p_amount THEN
        RAISE EXCEPTION 'Insufficient funds: available=%, requested=%', v_available, p_amount;
    END IF;

    -- Move funds from available to held
    UPDATE accounts SET
        available_balance = available_balance - p_amount,
        held_balance      = held_balance + p_amount
    WHERE account_id = p_account_id;

    -- Create hold record
    INSERT INTO balance_holds (account_id, amount)
    VALUES (p_account_id, p_amount)
    RETURNING hold_id INTO p_hold_id;

    COMMIT;
    -- Lock released here — other withdrawals can now proceed
END;
$$;

-- Phase 2a: Commit hold (payment succeeded)
CREATE OR REPLACE PROCEDURE commit_withdrawal_hold(
    p_hold_id   UUID,
    p_payment_ref TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_hold balance_holds%ROWTYPE;
BEGIN
    SELECT * INTO v_hold FROM balance_holds WHERE hold_id = p_hold_id FOR UPDATE;

    IF NOT FOUND OR v_hold.status != 'active' THEN
        RAISE EXCEPTION 'Hold % not found or not active (status: %)', p_hold_id, v_hold.status;
    END IF;

    IF v_hold.expires_at < NOW() THEN
        RAISE EXCEPTION 'Hold % has expired', p_hold_id;
    END IF;

    -- Convert hold to permanent debit
    UPDATE accounts SET held_balance = held_balance - v_hold.amount
    WHERE account_id = v_hold.account_id;

    UPDATE balance_holds SET status = 'committed', payment_ref = p_payment_ref
    WHERE hold_id = p_hold_id;

    COMMIT;
END;
$$;

-- Phase 2b: Release hold (payment failed/timed out)
CREATE OR REPLACE PROCEDURE release_withdrawal_hold(p_hold_id UUID)
LANGUAGE plpgsql AS $$
DECLARE
    v_hold balance_holds%ROWTYPE;
BEGIN
    SELECT * INTO v_hold FROM balance_holds WHERE hold_id = p_hold_id FOR UPDATE;
    IF NOT FOUND OR v_hold.status != 'active' THEN RETURN; END IF;

    -- Return funds to available
    UPDATE accounts SET
        available_balance = available_balance + v_hold.amount,
        held_balance      = held_balance - v_hold.amount
    WHERE account_id = v_hold.account_id;

    UPDATE balance_holds SET status = 'released' WHERE hold_id = p_hold_id;
    COMMIT;
END;
$$;

-- Background cleanup: release expired holds automatically
CREATE OR REPLACE PROCEDURE cleanup_expired_holds()
LANGUAGE plpgsql AS $$
DECLARE v_hold RECORD;
BEGIN
    FOR v_hold IN
        SELECT hold_id, account_id, amount FROM balance_holds
        WHERE status = 'active' AND expires_at < NOW()
        FOR UPDATE SKIP LOCKED  -- Non-blocking cleanup
    LOOP
        CALL release_withdrawal_hold(v_hold.hold_id);
    END LOOP;
END;
$$;
```

**Why each decision:**
- `FOR UPDATE NOWAIT` in Phase 1: at 500 req/s, queuing 499 transactions behind one slow lock would cause cascading timeouts. NOWAIT fails fast — the caller retries with exponential backoff, distributing load over time
- External payment call happens OUTSIDE any transaction: holding a database lock during a 200ms HTTP call to a payment processor is a disaster at scale — 500 × 200ms = lock held for the entire second
- Two-phase hold pattern: guarantees funds are never double-counted (available + held = total always) even if the server crashes between phases
- The `expires_at` + cleanup job provides automatic recovery from crashes between Phase 1 and Phase 2

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are the four standard SQL transaction isolation levels?**
A: READ UNCOMMITTED (can see uncommitted changes from other transactions—dirty reads possible), READ COMMITTED (sees only committed data, but non-repeatable reads possible), REPEATABLE READ (same rows return same data within a transaction, but phantom reads possible), and SERIALIZABLE (full isolation—transactions behave as if sequential, no anomalies).

**Q: What is a dirty read and which isolation level prevents it?**
A: A dirty read occurs when transaction A reads data modified by transaction B before B commits. If B rolls back, A read data that never existed. READ COMMITTED and above prevent dirty reads by only showing committed data.

**Q: What is a non-repeatable read and when does it occur?**
A: A non-repeatable read happens when transaction A reads a row, transaction B updates and commits that row, and transaction A reads the same row again and gets a different value. READ COMMITTED allows this; REPEATABLE READ and SERIALIZABLE prevent it.

**Q: What is a phantom read and how is it different from a non-repeatable read?**
A: A phantom read occurs when transaction A executes a range query, transaction B inserts (or deletes) rows that match the range and commits, and transaction A re-executes the range query and sees different rows. A non-repeatable read affects an existing row's values; a phantom read affects the set of rows returned. SERIALIZABLE prevents phantoms; REPEATABLE READ (in most implementations) does not.

**Q: What is MVCC (Multi-Version Concurrency Control) and how does it reduce locking?**
A: MVCC maintains multiple versions of each row, allowing readers to see a consistent snapshot of data as of their transaction start time without blocking writers. Writers create new row versions rather than modifying in place. This enables high read concurrency with minimal locking—PostgreSQL and Snowflake both use MVCC-based approaches.

**Q: What is a deadlock and how do databases handle it?**
A: A deadlock occurs when two transactions each hold a lock the other needs, causing circular waiting. Databases detect deadlocks (by checking the wait-for graph) and resolve them by killing one transaction (the victim) and rolling it back. Applications must handle the resulting error and retry the transaction.

**Q: What is the default isolation level in PostgreSQL and why is it commonly used?**
A: PostgreSQL defaults to READ COMMITTED—transactions see only committed data from other transactions, but each statement within the transaction sees the latest committed state. It's widely used because it avoids dirty reads while being less prone to serialization failures (transaction rollbacks) than REPEATABLE READ or SERIALIZABLE.

**Q: What is a SELECT FOR UPDATE and when do you use it?**
A: SELECT FOR UPDATE locks the selected rows for the duration of the transaction, preventing other transactions from modifying them until the lock is released at COMMIT or ROLLBACK. It's used to implement pessimistic locking patterns—for example, reserving inventory rows before updating them to prevent overselling.

---

## 💼 Interview Tips

- Know all four isolation levels and their anomalies cold—this is a fundamental topic that appears in DE, backend, and data platform interviews at every seniority level.
- Frame isolation levels as a trade-off: higher isolation = fewer anomalies but more lock contention and potentially more transaction rollbacks. Show you understand the operational implications, not just the definitions.
- When discussing MVCC, connect it to Snowflake's Time Travel—both use version history to allow point-in-time reads without blocking writers. Drawing this connection shows cross-system depth.
- Be ready to design a locking strategy for a specific scenario (e.g., double-booking prevention, inventory reservation)—SELECT FOR UPDATE and SERIALIZABLE are the main tools and senior interviewers will probe which you'd choose and why.
- Mention that SERIALIZABLE is rarely the default in production systems because of its performance overhead. Knowing when to escalate to SERIALIZABLE (e.g., financial transactions where correctness is paramount) vs. accepting READ COMMITTED with application-level idempotency is a key senior judgment call.
