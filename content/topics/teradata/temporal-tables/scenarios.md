---
title: "Teradata - Temporal Tables Scenarios"
topic: teradata
subtopic: temporal-tables
content_type: scenario_question
difficulty_level: senior
tags: [teradata, temporal-tables, scenarios, bitemporal, scd, valid-time]
---

# Temporal Tables — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Point-in-Time Query

You have a temporal table `employee_department` with valid-time support:

```sql
CREATE TABLE employee_department (
    emp_id      INTEGER NOT NULL,
    dept_name   VARCHAR(100),
    manager     VARCHAR(100),
    valid_from  DATE NOT NULL,
    valid_to    DATE NOT NULL,
    PERIOD FOR valid_time (valid_from, valid_to)
) PRIMARY INDEX (emp_id);
```

Sample data:
| emp_id | dept_name | manager | valid_from | valid_to |
|---|---|---|---|---|
| 101 | Engineering | Alice | 2022-01-01 | 2023-06-01 |
| 101 | Product | Bob | 2023-06-01 | 2024-03-01 |
| 101 | Engineering | Carol | 2024-03-01 | 9999-01-01 |

Write the query to find which department and manager employee 101 had on September 15, 2023.

<details>
<summary>💡 Hint</summary>

Use the FOR VALID_TIME AS OF clause to specify the point in time you're querying.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT dept_name, manager
FROM employee_department
FOR VALID_TIME AS OF DATE '2023-09-15'
WHERE emp_id = 101;
```

**Result:**
| dept_name | manager |
|---|---|
| Product | Bob |

**Why this works:**
- `FOR VALID_TIME AS OF DATE '2023-09-15'` filters to rows where `valid_from <= '2023-09-15' < valid_to`
- Row 2: `valid_from = 2023-06-01 <= 2023-09-15 < valid_to = 2024-03-01` → matches
- Row 1: valid_to = 2023-06-01 which is NOT > 2023-09-15 (end is exclusive) → does not match
- Row 3: valid_from = 2024-03-01 is NOT <= 2023-09-15 → does not match

**Without temporal table (manual equivalent):**
```sql
-- Old way without temporal support:
SELECT dept_name, manager
FROM employee_department
WHERE emp_id = 101
  AND valid_from <= DATE '2023-09-15'
  AND valid_to > DATE '2023-09-15';
```

The temporal version is cleaner and the optimizer understands temporal semantics for better query planning.

**To get ALL history:**
```sql
SELECT dept_name, manager, valid_from, valid_to
FROM employee_department
FOR VALID_TIME ALL
WHERE emp_id = 101
ORDER BY valid_from;
```

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Implementing a Bitemporal Correction

Your insurance system has a bitemporal table `policy_premium`. It's discovered that a premium for policy 5678 was entered incorrectly:
- Entered: $800/month premium for the entire year 2024
- Correct: Premium should be $800/month from Jan–June, $950/month from July–December

The correction must be recorded today (for audit), but applied retroactively to the entire year. Show the SQL to make this correction and verify it with queries.

<details>
<summary>💡 Hint</summary>

You need to: (1) delete the incorrect records for the affected valid-time periods, then (2) insert the correct records. The transaction time will automatically capture when the correction was made.

</details>

<details>
<summary>✅ Solution</summary>

**Setup (assuming table exists with system versioning):**

```sql
CREATE TABLE policy_premium (
    policy_id       INTEGER NOT NULL,
    monthly_premium DECIMAL(10,2),
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    PERIOD FOR valid_time (valid_from, valid_to),
    sys_start       TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
    sys_end         TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING;
```

**Current incorrect state:**
```
policy_id=5678, premium=800.00, valid_from=2024-01-01, valid_to=2024-12-31
```

**Step 1: Delete the incorrect record for 2024**
```sql
DELETE FROM policy_premium
FOR PORTION OF VALID_TIME FROM DATE '2024-01-01' TO DATE '2024-12-31'
WHERE policy_id = 5678;
-- This logically closes the old record (sets sys_end = CURRENT_TIMESTAMP)
-- The old record is preserved in transaction-time history
```

**Step 2: Insert the two correct records**
```sql
-- First half of year: $800/month
INSERT INTO policy_premium (policy_id, monthly_premium, valid_from, valid_to)
VALUES (5678, 800.00, DATE '2024-01-01', DATE '2024-07-01');

-- Second half of year: $950/month
INSERT INTO policy_premium (policy_id, monthly_premium, valid_from, valid_to)
VALUES (5678, 950.00, DATE '2024-07-01', DATE '2025-01-01');
```

**Verification queries:**

```sql
-- Current view: what does the policy look like now?
SELECT policy_id, monthly_premium, valid_from, valid_to
FROM policy_premium
FOR VALID_TIME ALL
WHERE policy_id = 5678
ORDER BY valid_from;
```
Expected:
```
5678 | 800.00 | 2024-01-01 | 2024-07-01
5678 | 950.00 | 2024-07-01 | 2025-01-01
```

```sql
-- Point-in-time: What was the premium on April 15, 2024?
SELECT monthly_premium
FROM policy_premium
FOR VALID_TIME AS OF DATE '2024-04-15'
WHERE policy_id = 5678;
-- Returns: 800.00 (correct)
```

```sql
-- Audit: Show the FULL history including the correction
SELECT monthly_premium, valid_from, valid_to, sys_start, sys_end
FROM policy_premium
FOR SYSTEM_TIME ALL
FOR VALID_TIME ALL
WHERE policy_id = 5678
ORDER BY sys_start, valid_from;
```
Expected:
```
800.00 | 2024-01-01 | 2024-12-31 | [original insert ts] | [delete ts]   ← incorrect record (now logically deleted)
800.00 | 2024-01-01 | 2024-07-01 | [correction ts]      | 9999-...     ← correct record 1
950.00 | 2024-07-01 | 2025-01-01 | [correction ts]      | 9999-...     ← correct record 2
```

The audit trail shows the original incorrect record AND when it was corrected.

**What was reported before the correction?**
```sql
-- Simulate what last month's report would have shown
SELECT monthly_premium
FROM policy_premium
FOR VALID_TIME AS OF DATE '2024-09-01'
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-09-30 17:00:00'  -- before the correction
WHERE policy_id = 5678;
-- Returns: 800.00 (the incorrect value we had before the fix)
```

This demonstrates the power of bitemporal — you can show both the "what was true" and "what did we know when."

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Designing a Temporal Data Model for Regulatory Compliance

A financial services firm needs to build a customer positions data model in Teradata. Requirements:
- Track current and historical positions (what a customer holds)
- Support as-of queries for any historical date (regulatory reporting)
- Positions can be backdated (T+2 settlement means trades settle days after execution)
- Support data corrections with full audit trail
- 7-year retention requirement
- High query volume: 500 concurrent analyst queries, some spanning multi-year ranges

Design the temporal data model, justify your choice of temporal type, and address the performance implications.

<details>
<summary>💡 Hint</summary>

Think about: which temporal dimensions are needed and why, how backdating affects the model, how to handle 7-year retention without degrading query performance, and what indexes are needed for 500 concurrent users spanning multi-year ranges.

</details>

<details>
<summary>✅ Solution</summary>

**Why Bitemporal is Required:**

This is a textbook bitemporal case:
- **Valid time needed:** Positions are backdated (T+2 settlement) — trade executed Jan 3, settled Jan 5, so the position is valid from Jan 5 but we may not record it until Jan 7
- **Transaction time needed:** Audit requirement — regulators want to know "what did you know, when did you know it?"

A valid-time-only table can't distinguish "when we recorded it" from "when it was valid." A transaction-time-only table can't express backdating.

**Data Model:**

```sql
CREATE TABLE customer_position (
    -- Business keys
    customer_id     BIGINT NOT NULL,
    security_id     VARCHAR(20) NOT NULL,   -- CUSIP/ISIN
    account_id      BIGINT NOT NULL,
    
    -- Position data
    quantity        DECIMAL(18,6),
    cost_basis      DECIMAL(18,4),
    currency        CHAR(3),
    position_type   VARCHAR(20),  -- LONG, SHORT, OPTION, etc.
    
    -- Valid time: when this position is/was held
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    PERIOD FOR valid_time (valid_from, valid_to),
    
    -- Transaction time: when we recorded this in the database
    sys_start       TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
    sys_end         TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING

-- Primary Index: customer_id (most queries start with a customer filter)
PRIMARY INDEX (customer_id)

-- PPI on valid_from for date-range query performance
PARTITION BY RANGE_N(
    valid_from BETWEEN DATE '2018-01-01' AND DATE '2032-12-31'
    EACH INTERVAL '3' MONTH   -- Quarterly partitions
);

-- Temporal primary key: prevent overlapping positions for same customer/security/account
-- Note: Teradata PERIOD for PK constraint syntax
ALTER TABLE customer_position
    ADD PRIMARY KEY (customer_id, security_id, account_id) FOR PERIOD valid_time;
```

**Index Strategy:**

```sql
-- NUSI for security-level queries (find all customers holding a security)
CREATE INDEX (security_id) ON customer_position;

-- NUSI for date-range lookups optimized for AS OF queries
CREATE INDEX (customer_id, valid_to) ON customer_position;

-- NUSI for transaction-time audit queries
CREATE INDEX (sys_start) ON customer_position;
```

**Retention Strategy:**

7-year retention with hot/cold tiering:
```
Years 1-3: In Teradata hot storage (NVMe/SSD AMP pools)
           All temporal dimensions active
           Full query performance

Years 4-7: In Teradata archive storage (HDD AMPs or object store)
           Still queryable via external tables or QueryGrid
           Slower query response acceptable (regulated, infrequent queries)

After 7 years: Physical deletion of sys_end records
               (regulatory minimum satisfied)
```

```sql
-- Monthly archive job: move old transaction-time records to archive
INSERT INTO customer_position_archive
SELECT * FROM customer_position
FOR SYSTEM_TIME ALL
WHERE sys_end < ADD_MONTHS(CURRENT_TIMESTAMP, -36)  -- Transaction-time ended > 3 years ago
  AND sys_end <> TIMESTAMP '9999-01-01 00:00:00';   -- Not currently active

DELETE FROM customer_position
FOR SYSTEM_TIME ALL  -- Non-sequenced: directly delete old rows
WHERE sys_end < ADD_MONTHS(CURRENT_TIMESTAMP, -36)
  AND sys_end <> TIMESTAMP '9999-01-01 00:00:00';
```

**Performance for 500 Concurrent Users:**

500 users × various query types:
- **Point-in-time (most common):** `FOR VALID_TIME AS OF date WHERE customer_id = ?`
  - Single-AMP (PI = customer_id) + partition elimination (valid_from PPI)
  - Very fast: < 1 second
  
- **Multi-year range (complex reports):** `FOR VALID_TIME BETWEEN 2021 AND 2023 WHERE security_id = ?`
  - NUSI on security_id + date range scan
  - Moderate: 10-60 seconds depending on security popularity
  
- **Regulatory audit:** `FOR SYSTEM_TIME AS OF ... FOR VALID_TIME AS OF ...`
  - NUSI on sys_start + partition elimination
  - May be slower for wide date ranges — acceptable for infrequent regulatory use

**TASM Workload Separation:**
```
Tactical class: API-driven position lookups (SLA: < 2 sec)
  → Single customer + single date → PI + PPI = AMP-local
Strategic class: Analyst/regulatory reports (SLA: 5 min)
  → Multi-customer or date-range scans → use NUSI + partitions
```

**Backdating Handling:**

T+2 settlement means inserts with past valid_from dates are normal:
```sql
-- Trade executed Jan 3, settled Jan 5, recorded Jan 7
INSERT INTO customer_position (
    customer_id, security_id, account_id, quantity, cost_basis, currency,
    valid_from, valid_to  -- valid_from = settlement date = Jan 5
) VALUES (
    12345, 'US0378331005', 67890, 100.0, 18500.00, 'USD',
    DATE '2024-01-05',   -- When position became valid (settlement)
    DATE '9999-01-01'    -- Currently active
);
-- sys_start automatically = '2024-01-07' (when we recorded it)
-- valid_from = '2024-01-05' (when it was legally effective)
-- Difference = the T+2 settlement lag
```

**Key design decisions articulated:**

1. **Bitemporal (not just valid-time):** Backdating creates a gap between valid_from and sys_start. Regulators need both.
2. **Quarterly PPI:** Annual is too coarse for common single-quarter queries; monthly creates too many partitions (180 for 15 years). Quarterly is the sweet spot.
3. **Temporal PK constraint:** Prevents overlapping positions — business correctness enforced at database level, not application layer.
4. **Hot/cold tiering:** 3-year hot / 7-year retention. Avoids letting 7 years of transaction-time accumulate in primary storage.
5. **TASM separation:** Tactical API queries must not be blocked by analyst report scans on the same table.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are temporal tables in Teradata and what problem do they solve?**
A: Temporal tables natively track the history of data changes over time with built-in period columns. They solve the problem of maintaining slowly changing data (e.g., employee records, product prices) without hand-crafting effective-date logic in every query—Teradata handles the version management and temporal predicates automatically.

**Q: What are the two types of time in Teradata temporal tables?**
A: Transaction time tracks when a row was inserted or deleted in the database (system-managed, tied to DML timestamps). Valid time (also called business time) tracks when a fact was true in the real world (user-managed, based on business dates). Bitemporal tables track both dimensions simultaneously.

**Q: What is the PERIOD data type in Teradata?**
A: PERIOD is a Teradata data type that represents a range of values between a start and an end bound (e.g., `PERIOD(DATE)` or `PERIOD(TIMESTAMP)`). It stores both the beginning and ending of a time interval as a single column, enabling efficient temporal period comparisons and overlaps using specialized temporal operators.

**Q: What is a valid-time temporal table and how do you query it as of a specific date?**
A: A valid-time table has a `VALIDTIME PERIOD FOR valid_period` column. To query the state as of a specific date, use: `SELECT * FROM employee AS OF DATE '2023-06-01'` — Teradata automatically filters rows where the valid period contains that date, without writing explicit BETWEEN date conditions.

**Q: What is the difference between a sequenced and a non-sequenced temporal operation?**
A: A sequenced operation respects the time dimension—it applies the DML or query semantics period by period (e.g., updating only the portion of a valid-time row that overlaps a specified period). A non-sequenced operation ignores the time dimension and treats the period columns as regular data, behaving like standard SQL.

**Q: How does Teradata handle temporal UPDATE and DELETE operations differently from standard DML?**
A: Temporal sequenced UPDATE/DELETE on a valid-time table automatically splits existing rows to preserve history. For example, updating a salary for a date range will split the existing row into "before the change period," "during the change period," and "after the change period" without you writing explicit INSERT/UPDATE logic.

**Q: What is a bitemporal table and what scenarios require it?**
A: A bitemporal table tracks both valid time (when the fact was true in the real world) and transaction time (when the fact was recorded in the database). This enables queries like "what did we know as of database state X about facts that were true as of date Y?"—essential for audit trails, regulatory reporting, and correcting historical data errors while preserving the record of what was originally recorded.

**Q: What are the performance considerations for temporal tables?**
A: Temporal tables can have more rows than equivalent non-temporal designs (due to history versions), which increases storage and scan costs. Queries that don't specify temporal predicates may scan all versions. Collect statistics on the period columns to help the Optimizer prune temporal versions efficiently. Archiving old versions to separate tables is a common management strategy.

---

## 💼 Interview Tips

- Lead with the business problem: tracking slowly changing data (prices, employee records, contracts) is a universal DE challenge, and temporal tables solve it at the database layer rather than requiring application-level effective-date logic. This framing shows business value awareness.
- Clearly distinguish valid time from transaction time—conflating them is the most common mistake when explaining temporal tables to interviewers. Use a concrete example (valid time = when the salary was effective; transaction time = when we recorded that salary in the system).
- Bitemporal tables are powerful but operationally complex—acknowledge this. Knowing when the complexity is justified (regulatory audit requirements, financial restatements) vs. when simpler SCD Type 2 logic suffices shows senior judgment.
- Discuss the sequenced vs. non-sequenced distinction as a practical query writing concern: most temporal queries should be sequenced to get correct period-aware semantics. Non-sequenced operations are for administrative operations on the temporal columns themselves.
- Senior interviewers at financial services, insurance, or healthcare companies (heavy Teradata users) will probe temporal table depth. Knowing the automatic row-splitting behavior of sequenced DML shows genuine hands-on experience.
