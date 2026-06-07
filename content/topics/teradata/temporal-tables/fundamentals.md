---
title: "Teradata - Temporal Tables Fundamentals"
topic: teradata
subtopic: temporal-tables
content_type: study_material
difficulty_level: beginner
layer: fundamentals
tags: [teradata, temporal-tables, ansi-temporal, valid-time, transaction-time, period]
---

# Temporal Tables — Fundamentals

## What Are Temporal Tables?

**Temporal tables** automatically track how data changes over time. Instead of overwriting old values, temporal tables preserve the history — you can query what the data looked like at any point in the past.

Teradata has **native ANSI SQL temporal support** — one of the most complete implementations of the ANSI SQL:2011 temporal standard among commercial databases.

---

## Two Time Dimensions

Temporal tables use two independent time dimensions:

### Valid Time (Application Time)
- When the fact was **true in the real world**
- Examples: "Customer address was 123 Main St from Jan–June 2024"
- Controlled by the **application** (you specify the dates)
- Also called **bi-temporal application time**

### Transaction Time (System Time)
- When the **database recorded** the fact
- Always set by the system (you can't override it)
- Never decreases (monotonically increasing)
- Captures when you inserted/updated/deleted the record

| Dimension | Who Sets It | Can Be Modified? | Use Case |
|---|---|---|---|
| Valid Time | Application | Yes (can be future-dated) | Business validity periods |
| Transaction Time | System (Teradata) | No | Audit trail, data lineage |
| Both (Bitemporal) | Mixed | Valid: Yes, Txn: No | Full audit + business history |

---

## The PERIOD Data Type

Temporal tables use the `PERIOD` data type to represent a time interval:

```sql
-- PERIOD(DATE): interval of dates
PERIOD(DATE) '(2024-01-01, 2024-06-30)'   -- Jan 1 to Jun 30

-- PERIOD(TIMESTAMP(6)): interval of timestamps
PERIOD(TIMESTAMP(6)) '(2024-01-01 00:00:00, 2024-12-31 23:59:59)'

-- The end of a period can be UNTIL_CHANGED (open-ended):
-- Means "current/active"
PERIOD(DATE) '(2024-01-01, 9999-01-01)'  -- UNTIL_CHANGED representation
```

Each PERIOD has:
- **BEGIN value:** Start of the interval (inclusive)
- **END value:** End of the interval (exclusive by ANSI convention)

---

## Creating a Temporal Table

```sql
-- Valid-time table (application time only)
CREATE TABLE customer_address (
    customer_id     INTEGER NOT NULL,
    street_address  VARCHAR(200),
    city            VARCHAR(100),
    state           CHAR(2),
    -- Valid time columns
    valid_start     DATE NOT NULL,
    valid_end       DATE NOT NULL,
    -- Declare the valid-time period
    PERIOD FOR valid_time (valid_start, valid_end)
)
UNIQUE PRIMARY INDEX (customer_id, valid_start);
```

```sql
-- Transaction-time table (system time)
CREATE TABLE product_price (
    product_id      INTEGER NOT NULL,
    list_price      DECIMAL(10,2),
    -- System-maintained transaction time
    sys_start       TIMESTAMP(6) NOT NULL GENERATED ALWAYS AS ROW START,
    sys_end         TIMESTAMP(6) NOT NULL GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING;
```

```sql
-- Bitemporal table (both valid time and transaction time)
CREATE TABLE policy_coverage (
    policy_id       INTEGER NOT NULL,
    coverage_type   VARCHAR(50),
    coverage_amount DECIMAL(12,2),
    -- Valid time (application-controlled)
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    PERIOD FOR valid_time (valid_from, valid_to),
    -- Transaction time (system-controlled)
    txn_start       TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
    txn_end         TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (txn_start, txn_end)
) WITH SYSTEM VERSIONING;
```

---

## Basic Temporal Queries

```sql
-- Current records (no time qualifier = current snapshot)
SELECT * FROM customer_address;

-- AS OF query: What was the address on a specific date?
SELECT * FROM customer_address
FOR VALID_TIME AS OF DATE '2023-06-15'
WHERE customer_id = 1001;

-- AS OF for transaction time: What did the database show on a date?
SELECT * FROM product_price
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 09:00:00'
WHERE product_id = 42;

-- Period between: All records valid during a date range
SELECT * FROM customer_address
FOR VALID_TIME BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'
WHERE customer_id = 1001;
```

---

## Temporal DML

Teradata automatically maintains temporal records during DML:

```sql
-- INSERT: Specify the validity period
INSERT INTO customer_address
    (customer_id, street_address, city, state, valid_start, valid_end)
VALUES (1001, '123 Main St', 'Austin', 'TX', DATE '2024-01-01', DATE '9999-01-01');
-- valid_end = 9999-01-01 = "UNTIL_CHANGED" = currently active

-- UPDATE: Move to new address (temporal update splits the record)
UPDATE customer_address
    FOR VALID_TIME AS OF DATE '2024-07-01'
SET street_address = '456 Oak Ave', city = 'Dallas'
WHERE customer_id = 1001;
-- Automatically: closes old record at 2024-07-01, creates new one from 2024-07-01

-- DELETE: Close the record at a specific time
DELETE FROM customer_address
    FOR VALID_TIME AS OF DATE '2024-12-01'
WHERE customer_id = 1001;
-- Closes valid_end at 2024-12-01, doesn't physically delete
```

---

## Use Cases

| Use Case | Time Dimension | Example |
|---|---|---|
| Customer address history | Valid time | What was customer's address when bill was sent? |
| Product price history | Valid time | What was the price when the order was placed? |
| Audit trail | Transaction time | When did someone change this record? |
| Regulatory compliance | Both (bitemporal) | What did we report and when was it true? |
| Slowly changing dimensions | Valid time | SCD Type 2 managed automatically |

---

## Interview Tips

> **Tip 1:** "What are temporal tables in Teradata?" — "Temporal tables automatically maintain historical versions of rows over time. Valid-time tables track when facts were true in the real world (application-controlled). Transaction-time tables track when the database recorded changes (system-controlled). Bitemporal tables combine both, providing full audit capability."

> **Tip 2:** "What is the PERIOD data type?" — "PERIOD is a range data type with a BEGIN and END value representing a time interval. It's used in temporal tables to express the time span during which a row is valid or when the system recorded it. The standard convention is that the end is exclusive."

> **Tip 3:** "What's the difference between valid time and transaction time?" — "Valid time tracks when something is true in the real world — for example, an insurance policy is valid from Jan 1 to Dec 31. Transaction time tracks when the database recorded that fact — for example, the policy was entered into the system on Jan 3. They can differ (backdated entries)."
