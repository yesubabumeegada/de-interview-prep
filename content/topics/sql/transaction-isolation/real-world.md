---
title: "SQL Transaction Isolation - Real-World Production Examples"
topic: sql
subtopic: transaction-isolation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, transactions, isolation, mvcc, deadlocks, production, concurrency]
---

# SQL Transaction Isolation — Real-World Production Examples

## Scenario 1: Preventing Double-Booking in a Seat Reservation System

**Business context:** A concert ticketing platform has 10,000 concurrent users during ticket sales for a popular event. Each seat can only be sold once. The naive approach of "check if seat is available, then book it" has a race condition: two users check the same seat simultaneously, both see it as available, and both try to book it.

**The problematic approach (race condition):**

```sql
-- BAD: Check-then-act race condition
-- Transaction A:
BEGIN;
SELECT status FROM seats WHERE seat_id = 42 AND event_id = 100;
-- Returns: 'available'

-- Transaction B (simultaneously):
BEGIN;
SELECT status FROM seats WHERE seat_id = 42 AND event_id = 100;
-- Also returns: 'available' (Transaction A hasn't committed yet)

-- Transaction A:
UPDATE seats SET status = 'booked', user_id = 1001 WHERE seat_id = 42;
COMMIT;

-- Transaction B:
UPDATE seats SET status = 'booked', user_id = 1002 WHERE seat_id = 42;
COMMIT;
-- DOUBLE BOOKING! Both users are told they got the seat.
```

**The correct approach — Optimistic Locking with version column:**

```sql
-- Schema with version column:
CREATE TABLE seats (
    seat_id   INT,
    event_id  INT,
    status    TEXT DEFAULT 'available',
    user_id   INT,
    version   INT DEFAULT 0,  -- Optimistic lock version
    PRIMARY KEY (seat_id, event_id)
);

-- Booking procedure with optimistic locking:
CREATE OR REPLACE PROCEDURE book_seat(
    p_seat_id  INT,
    p_event_id INT,
    p_user_id  INT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_current_version INT;
    v_rows_updated    INT;
    v_status          TEXT;
BEGIN
    -- Read current state
    SELECT version, status INTO v_current_version, v_status
    FROM seats WHERE seat_id = p_seat_id AND event_id = p_event_id;

    IF v_status != 'available' THEN
        RAISE EXCEPTION 'Seat % is not available (status: %)', p_seat_id, v_status;
    END IF;

    -- Update ONLY if version hasn't changed (compare-and-swap)
    UPDATE seats SET
        status  = 'booked',
        user_id = p_user_id,
        version = version + 1
    WHERE seat_id  = p_seat_id
      AND event_id = p_event_id
      AND version  = v_current_version   -- The key: only update if unchanged
      AND status   = 'available';

    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    IF v_rows_updated = 0 THEN
        -- Another transaction won the race — seat was booked between our read and write
        RAISE EXCEPTION 'Seat % was just booked by another user — please choose a different seat', p_seat_id;
    END IF;

    COMMIT;
    RAISE NOTICE 'Seat % booked for user %', p_seat_id, p_user_id;
END;
$$;
```

**The pessimistic locking approach — better for high contention:**

```sql
-- Pessimistic locking with SELECT FOR UPDATE:
CREATE OR REPLACE PROCEDURE book_seat_pessimistic(
    p_seat_id  INT,
    p_event_id INT,
    p_user_id  INT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_status TEXT;
BEGIN
    -- Lock the row immediately — all other transactions wait here
    SELECT status INTO v_status
    FROM seats
    WHERE seat_id = p_seat_id AND event_id = p_event_id
    FOR UPDATE;  -- Row is now exclusively locked

    IF v_status != 'available' THEN
        RAISE EXCEPTION 'Seat % is not available', p_seat_id;
    END IF;

    -- No race possible here — row is locked
    UPDATE seats SET status = 'booked', user_id = p_user_id
    WHERE seat_id = p_seat_id AND event_id = p_event_id;

    COMMIT;
END;
$$;
```

**Choosing between approaches:**

| Approach | Best For | Risk |
|----------|---------|------|
| Optimistic (version column) | Low contention — most users get different seats | Occasional retry needed |
| Pessimistic (SELECT FOR UPDATE) | High contention on same rows | Row lock held during transaction |
| NOWAIT variant | Immediate failure preferred over waiting | User gets error instantly |

For seat booking: use **pessimistic locking** (FOR UPDATE) because contention on individual popular seats is HIGH. For shopping carts with low per-item contention: optimistic locking.

---

## Scenario 2: Financial Double-Entry Accounting Transactions

**Business context:** A fintech startup processes thousands of transactions per second. Each payment involves debiting one account and crediting another — both must succeed or neither should. The system must also maintain a real-time running balance without table scans.

```sql
-- Schema:
CREATE TABLE accounts (
    account_id      BIGINT PRIMARY KEY,
    balance         NUMERIC(15,2) NOT NULL DEFAULT 0,
    currency        CHAR(3),
    last_updated    TIMESTAMPTZ DEFAULT NOW(),
    CHECK (balance >= 0)  -- Cannot go negative
);

CREATE TABLE journal_entries (
    entry_id        BIGSERIAL PRIMARY KEY,
    transaction_ref UUID NOT NULL,
    account_id      BIGINT NOT NULL REFERENCES accounts(account_id),
    amount          NUMERIC(15,2) NOT NULL,  -- Positive = credit, Negative = debit
    entry_type      TEXT NOT NULL,           -- 'debit' or 'credit'
    description     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Fund transfer procedure with proper isolation and deadlock prevention:
CREATE OR REPLACE PROCEDURE transfer_funds(
    p_from_account   BIGINT,
    p_to_account     BIGINT,
    p_amount         NUMERIC,
    p_description    TEXT,
    p_tx_ref         UUID DEFAULT gen_random_uuid()
)
LANGUAGE plpgsql AS $$
DECLARE
    v_from_balance   NUMERIC;
    v_to_exists      BOOLEAN;
    v_lock_order_1   BIGINT;
    v_lock_order_2   BIGINT;
BEGIN
    -- Deadlock prevention: ALWAYS lock accounts in consistent ID order
    -- Regardless of which is "from" and which is "to"
    v_lock_order_1 := LEAST(p_from_account, p_to_account);
    v_lock_order_2 := GREATEST(p_from_account, p_to_account);

    -- Acquire locks in deterministic order (prevents deadlocks)
    SELECT balance INTO v_from_balance
    FROM accounts
    WHERE account_id = v_lock_order_1
    FOR UPDATE;

    -- Lock second account (if different)
    IF v_lock_order_2 != v_lock_order_1 THEN
        SELECT TRUE INTO v_to_exists
        FROM accounts WHERE account_id = v_lock_order_2 FOR UPDATE;
    END IF;

    -- Validate sufficient funds
    SELECT balance INTO v_from_balance
    FROM accounts WHERE account_id = p_from_account;  -- Re-read after lock

    IF v_from_balance < p_amount THEN
        RAISE EXCEPTION 'Insufficient funds: account % has %, need %',
            p_from_account, v_from_balance, p_amount;
    END IF;

    -- Double-entry journal entries (the canonical record)
    INSERT INTO journal_entries (transaction_ref, account_id, amount, entry_type, description)
    VALUES
        (p_tx_ref, p_from_account, -p_amount, 'debit', p_description),
        (p_tx_ref, p_to_account,   p_amount,  'credit', p_description);

    -- Update account balances (derived from journal but maintained for fast reads)
    UPDATE accounts SET
        balance      = balance - p_amount,
        last_updated = NOW()
    WHERE account_id = p_from_account;

    UPDATE accounts SET
        balance      = balance + p_amount,
        last_updated = NOW()
    WHERE account_id = p_to_account;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        -- Log the failed attempt
        INSERT INTO transfer_failures (tx_ref, from_account, to_account, amount, error, failed_at)
        VALUES (p_tx_ref, p_from_account, p_to_account, p_amount, SQLERRM, NOW());
        COMMIT;
        RAISE;
END;
$$;
```

**Why this design is production-grade:**
- Locking in `LEAST/GREATEST` order prevents deadlocks regardless of which account is the sender/receiver
- Journal entries are the authoritative record; balance is a denormalized cache for fast reads
- The `CHECK (balance >= 0)` constraint provides a final safety net even if the procedure logic has a bug
- If the server crashes after journal entries but before balance update: the journal can be replayed to reconcile balances (periodic reconciliation job)

---

## Scenario 3: Queue-Based Work Distribution with FOR UPDATE SKIP LOCKED

**Business context:** A data pipeline processes webhook events that arrive in batches. 20 worker processes run in parallel, each picking up the next available unprocessed event. Without proper isolation, multiple workers would pick up the same event (double-processing), or workers would pile up waiting for each other.

```sql
-- Schema:
CREATE TABLE webhook_events (
    event_id      BIGSERIAL PRIMARY KEY,
    event_type    TEXT,
    payload       JSONB,
    status        TEXT DEFAULT 'pending',  -- pending | processing | done | failed
    claimed_by    TEXT,                    -- worker identifier
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    retry_count   INT DEFAULT 0,
    error_msg     TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_events_pending ON webhook_events(created_at)
WHERE status = 'pending';  -- Partial index for the common query

-- Worker pickup procedure (safe for parallel workers):
CREATE OR REPLACE PROCEDURE claim_and_process_event(
    p_worker_id    TEXT,
    p_batch_size   INT DEFAULT 10
)
LANGUAGE plpgsql AS $$
DECLARE
    v_event RECORD;
    v_processed INT := 0;
    v_failed    INT := 0;
BEGIN
    -- Pick up available events, skipping any locked by other workers
    FOR v_event IN
        SELECT event_id, event_type, payload
        FROM webhook_events
        WHERE status = 'pending'
          AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '5 minutes')  -- Re-claim stale
        ORDER BY created_at ASC
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED  -- The key: non-blocking, no duplicate processing
    LOOP
        BEGIN
            -- Mark as processing
            UPDATE webhook_events SET
                status     = 'processing',
                claimed_by = p_worker_id,
                claimed_at = NOW()
            WHERE event_id = v_event.event_id;
            COMMIT;  -- Release the SKIP LOCKED lock; other workers can now see this is claimed

            -- Process the event (application logic — simplified here)
            -- In real code: call external API, write to another table, etc.
            PERFORM process_webhook_payload(v_event.event_id, v_event.payload);

            -- Mark as done
            UPDATE webhook_events SET
                status       = 'done',
                completed_at = NOW()
            WHERE event_id = v_event.event_id;
            COMMIT;
            v_processed := v_processed + 1;

        EXCEPTION
            WHEN OTHERS THEN
                -- Mark as failed (allow for retry if retry_count < max)
                UPDATE webhook_events SET
                    status      = CASE WHEN retry_count >= 3 THEN 'dead_letter' ELSE 'pending' END,
                    retry_count = retry_count + 1,
                    error_msg   = SQLERRM,
                    claimed_by  = NULL,
                    claimed_at  = NULL
                WHERE event_id = v_event.event_id;
                COMMIT;
                v_failed := v_failed + 1;
        END;
    END LOOP;

    RAISE NOTICE 'Worker %: processed=%, failed=%', p_worker_id, v_processed, v_failed;
END;
$$;

-- Monitor queue health:
SELECT 
    status,
    COUNT(*) AS event_count,
    MIN(created_at) AS oldest_event,
    AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at))) AS avg_processing_sec
FROM webhook_events
GROUP BY status
ORDER BY status;
```

**Why `SKIP LOCKED` is the right choice:**
- `FOR UPDATE` without SKIP LOCKED: all 20 workers pile up trying to lock the same pending rows — massive contention, one worker succeeds, 19 wait
- `FOR UPDATE SKIP LOCKED`: each worker instantly gets a different batch of events — zero contention, 20× throughput
- The `claimed_at < NOW() - 5 minutes` condition re-claims stale events from crashed workers, providing automatic recovery without a separate cleanup job
- `retry_count >= 3 → 'dead_letter'` prevents infinite retry loops on persistently failing events

---

## Interview Tips

> **Tip 1:** "How do you prevent double-processing in a distributed job queue?" — "The standard solution is `SELECT ... FOR UPDATE SKIP LOCKED LIMIT N`. Each worker atomically claims a batch of unclaimed jobs with no blocking of other workers. SKIP LOCKED means if a job is already claimed by another worker, this worker skips it and moves to the next available one. For crashed workers, I use a heartbeat timeout: re-claim jobs where `claimed_at < NOW() - N minutes` and the job is still in 'processing' status."

> **Tip 2:** "How do you prevent deadlocks in a transfer procedure?" — "Always lock rows in a consistent, deterministic order — typically by primary key, smallest ID first. If every transaction locks account_id = 1 before account_id = 2, no two transactions can deadlock (A waits for 2 while B holds 1, but B would need to lock 1 first too — so they always resolve in order). I encode this with `LEAST(from_id, to_id)` and `GREATEST(from_id, to_id)` and lock in that order regardless of transfer direction."

> **Tip 3:** "A user reports they were charged twice for the same purchase. How do you investigate and prevent it?" — "Investigation: check the journal_entries table for duplicate entries matching the user's transaction reference or timestamp window. Check if the payment service retried without an idempotency key. Prevention: add a unique constraint on a client-generated idempotency_key column in the payments table, and make the payment procedure check for the key before processing. Any retry with the same key returns the original result without charging again."
