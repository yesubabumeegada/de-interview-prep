---
title: "Teradata - Temporal Tables Real World"
topic: teradata
subtopic: temporal-tables
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, temporal-tables, regulatory, insurance, banking, compliance]
---

# Temporal Tables — Real World

## Case Study: Insurance Policy History (SCD Type 2 Replacement)

**Company:** A top-10 US property and casualty insurer.

**Problem before temporal tables:** Policy data used a manually managed SCD Type 2 approach:
- 12-column `is_current`, `eff_start`, `eff_end` pattern
- 3 different bugs in the past year: records not closed, gap in coverage dates, double-counting active policies
- Regulatory audits required manual reconstruction of "point-in-time" snapshots

**After migration to Teradata temporal tables:**

```sql
-- New policy table (valid-time temporal)
CREATE TABLE policy (
    policy_id        INTEGER NOT NULL,
    insured_name     VARCHAR(200),
    coverage_type    VARCHAR(50),
    premium          DECIMAL(10,2),
    valid_from       DATE NOT NULL,
    valid_to         DATE NOT NULL,
    PERIOD FOR valid_time (valid_from, valid_to)
) PRIMARY INDEX (policy_id)
PARTITION BY RANGE_N(valid_from BETWEEN DATE '2010-01-01'
                     AND DATE '2030-12-31' EACH INTERVAL '1' YEAR);

-- No more manual is_current flag
-- No more manual closing of old records
```

**Premium update flow (previously 30 lines of error-prone SQL, now 3 lines):**
```sql
-- Old premium was wrong, correct it from a specific date
UPDATE policy
FOR VALID_TIME AS OF DATE '2024-06-01'
SET premium = 2500.00
WHERE policy_id = 987654;
-- Teradata splits the record at 2024-06-01 automatically
```

**Regulatory point-in-time report:**
```sql
SELECT policy_id, coverage_type, premium
FROM policy
FOR VALID_TIME AS OF DATE '2023-12-31'  -- Year-end snapshot
WHERE coverage_type = 'HOMEOWNER'
ORDER BY policy_id;
```

**Result:** SCD bugs eliminated. Audit reports that took 2 hours of manual work are now 1 SQL query.

---

## Case Study: Banking — Rate Plan History

**Company:** A major bank's personal loan servicing system.

**Scenario:** Interest rates on variable-rate loans change monthly based on LIBOR/SOFR. The bank must:
- Know what rate applied on any historical date (regulatory requirement)
- Know when the rate was *entered* into the system (for late-entry audit)
- Be able to correct rates retroactively (when SOFR is restated)

**Bitemporal rate table:**

```sql
CREATE TABLE loan_interest_rate (
    loan_id         BIGINT NOT NULL,
    annual_rate     DECIMAL(6,4),
    -- Valid time: when this rate was effective
    rate_from       DATE NOT NULL,
    rate_to         DATE NOT NULL,
    PERIOD FOR valid_time (rate_from, rate_to),
    -- Transaction time: when we recorded it
    sys_start       TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
    sys_end         TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING;
```

**Queries:**
```sql
-- What rate applied to loan 12345 on March 15, 2024?
SELECT annual_rate
FROM loan_interest_rate
FOR VALID_TIME AS OF DATE '2024-03-15'
WHERE loan_id = 12345;

-- What did we THINK the rate was when we filed the Q1 report on April 30?
SELECT annual_rate
FROM loan_interest_rate
FOR VALID_TIME AS OF DATE '2024-03-15'
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-04-30 17:00:00'
WHERE loan_id = 12345;

-- Show the full correction history for a loan
SELECT annual_rate, rate_from, rate_to, sys_start, sys_end
FROM loan_interest_rate
FOR SYSTEM_TIME ALL
FOR VALID_TIME ALL
WHERE loan_id = 12345
ORDER BY sys_start, rate_from;
```

---

## Migration from Manual SCD to Temporal Tables

A large retailer migrated their `dim_customer` from manual SCD Type 2 to Teradata temporal:

**Migration steps:**
1. **Create new temporal table** with PERIOD columns
2. **Migrate historical data** from old SCD records to temporal format
3. **Update ETL** to use temporal DML instead of manual close/insert logic
4. **Validate** point-in-time queries return same results as old approach
5. **Decommission** old SCD table

```sql
-- Step 2: Migrate SCD data to temporal
INSERT INTO customer_temporal
    (customer_id, customer_name, segment, valid_from, valid_to)
SELECT
    customer_id,
    customer_name,
    segment,
    eff_start_date AS valid_from,
    CASE WHEN is_current = 'Y' THEN DATE '9999-01-01'
         ELSE eff_end_date END AS valid_to
FROM dim_customer_scd_old;
```

**Step 4: Validate equivalence:**
```sql
-- Old SCD query (verbose)
SELECT customer_name, segment
FROM dim_customer_scd_old
WHERE customer_id = 1001
  AND eff_start_date <= DATE '2023-06-15'
  AND (eff_end_date > DATE '2023-06-15' OR is_current = 'Y');

-- New temporal query (clean)
SELECT customer_name, segment
FROM customer_temporal
FOR VALID_TIME AS OF DATE '2023-06-15'
WHERE customer_id = 1001;

-- Both should return identical results
```

---

## Performance Lessons from Production

**Lesson 1: PPI on valid_from is critical**

Without PPI, AS OF queries scan all historical records. With PPI on `valid_from`:
- AS OF DATE '2024-01-01' eliminates all partitions for years 2025+
- 5-year history with annual partitions: AS OF query scans 1 of 5 partitions max

**Lesson 2: Transaction-time tables grow fast for frequently updated data**

A bank's reference data table (exchange rates, updated hourly) grew to 800GB in 2 years:
- 1K rows × 365 × 24 × 2 years = 17.5 million transaction-time versions
- Solution: Archive old transaction-time records to cold storage after 3 years
- Keep only last 7 years of transaction-time in hot Teradata storage

**Lesson 3: AS OF queries need secondary index on valid_end**

```sql
-- AS OF query: needs to find rows where valid_from <= date < valid_end
-- Without index on valid_end: full scan of that partition
-- With NUSI on (customer_id, valid_end): targeted lookup

CREATE INDEX (customer_id, valid_to) ON customer_temporal;
```

---

## Interview Tips

> **Tip 1:** "How would you use temporal tables for regulatory compliance?" — "Bitemporal tables let you answer 'what was true on date X, as we understood it at time Y' — the exact question regulators ask. For example, in banking: what was the loan rate on March 31, as reported in the April filing? Both dimensions are independently queryable with FOR VALID_TIME AS OF and FOR SYSTEM_TIME AS OF."

> **Tip 2:** "What are the operational challenges of temporal tables in production?" — "Storage growth for frequently updated data (each change creates new rows), need for PPI on the period begin column for AS OF performance, and secondary indexes on end columns for period lookups. Also, migrating existing SCD tables requires careful data migration and ETL rewriting."

> **Tip 3:** "When would you NOT use temporal tables?" — "When history is not needed (lookup tables with no audit requirement), when data changes so frequently that storage growth is prohibitive (e.g., real-time telemetry), or when the team lacks temporal SQL expertise. For simple SCD Type 1 (overwrite) cases, temporal is overkill."

> **Tip 4:** "How is Teradata temporal support different from other databases?" — "PostgreSQL has basic transaction-time (pg_audit), SQL Server has temporal tables but limited period operators, BigQuery has no native temporal. Teradata has full ANSI SQL:2011 bitemporal support including both valid-time and transaction-time in one table, temporal primary key constraints to prevent overlap, automatic period splitting on UPDATE, and temporal join predicates (OVERLAPS, CONTAINS, etc.)."
