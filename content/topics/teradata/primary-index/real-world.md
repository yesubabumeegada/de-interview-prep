---
title: "Teradata - Primary Index Real World"
topic: teradata
subtopic: primary-index
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, primary-index, production, skew, ppi, migration]
---

# Primary Index — Real World

## War Story: The Wrong PI That Caused a Production Outage

**Setting:** A major US insurance company, 2TB claims fact table.

**The mistake:** A new table was created with `claim_status` as the PI. The column had 5 distinct values: 'OPEN', 'CLOSED', 'PENDING', 'DENIED', 'VOIDED'. Distribution: 70% of rows were 'CLOSED'.

**What happened:**
- 70% of 2TB = 1.4TB landed on one AMP
- All queries that needed to scan closed claims ran on a single AMP
- During the month-end report run, that AMP CPU hit 100%
- All other AMPs were at 5–10% utilization
- The query that "should have taken 2 minutes" ran for 4 hours before being killed
- Month-end reports were delayed by 6 hours, triggering SLA penalties

**The fix:**
1. Emergency: Added `PARTITION BY RANGE_N(claim_date ...)` to spread I/O across partitions (temporary relief)
2. Permanent: Rebuilt the table with `claim_id` (surrogate, always populated, high cardinality) as NUPI + PPI on `claim_date`
3. Result: Same month-end report ran in 4 minutes

**Lesson learned:** Never choose a status/type/flag column as a PI. Always check cardinality and distribution before committing to a design.

---

## Real Pattern: Star Schema PI Design

At a large retailer (Home Depot-style), the DW team established these PI conventions:

**Fact Tables:**
- PI = most common JOIN foreign key (usually the "grain" of the fact)
- PPI = always on the date dimension key
- Example: `sales_fact` PI on `store_id + product_id`, PPI on `sale_date`

**Dimension Tables:**
- Small dims (< 1M rows): PI on surrogate key (UPI), no PPI — they'll be duplicated in joins anyway
- Large dims (> 10M rows): PI on natural key used in most JOINs

**Bridge/association tables:**
- Composite NUPI on both FK columns (e.g., `customer_id + product_id` for recommendations)

This convention was documented in the team's data model standards and enforced via peer review on all new table DDL.

---

## Real Pattern: NoPI Staging Tables

An e-commerce company's daily ETL pipeline:

```sql
-- Step 1: FastLoad raw events into NoPI staging (fastest bulk load)
CREATE TABLE stg_events_raw (
    event_json VARCHAR(8000)
) NO PRIMARY INDEX;
-- FastLoad populates in round-robin, ~200GB/hour throughput

-- Step 2: Parse and INSERT into proper table with PI
INSERT INTO events_fact
SELECT
    CAST(JSON_VALUE(event_json, '$.user_id') AS BIGINT) AS user_id,
    CAST(JSON_VALUE(event_json, '$.event_ts') AS TIMESTAMP(0)) AS event_ts,
    JSON_VALUE(event_json, '$.event_type') AS event_type
FROM stg_events_raw;
-- INSERT/SELECT redistributes by user_id PI automatically

-- Step 3: Drop staging table (or TRUNCATE for next day)
DELETE FROM stg_events_raw ALL;
```

**Why this pattern works:** FastLoad into NoPI avoids hashing overhead during ingestion. The INSERT/SELECT then properly distributes data in one pass. Total load time: 15 minutes for 500M events.

---

## Case Study: PI Migration on a Live System

**Challenge:** A bank needed to change the PI of a 5TB transaction table (account_id → transaction_id) without a maintenance window.

**Solution using dual-write:**

1. Created `transactions_new` with the new PI (transaction_id)
2. Set up a trigger (via ETL CDC) to write new transactions to BOTH old and new tables
3. Background process: INSERT/SELECT all historical rows into `transactions_new` (ran over 3 days)
4. Verified row counts and checksums matched
5. At 2 AM Sunday: switched application connection strings to point to `transactions_new`, renamed tables
6. Ran for 2 weeks in parallel before decommissioning old table

**Result:** Zero downtime, zero data loss. The 3-day background copy consumed ~30% of off-peak AMP CPU and spool.

---

## Detecting and Diagnosing Skew in Production

```sql
-- Step 1: Check if a table has significant skew
SELECT
    'orders' AS TableName,
    MAX(cnt) AS MaxAMPRows,
    MIN(cnt) AS MinAMPRows,
    AVG(cnt) AS AvgAMPRows,
    (MAX(cnt) - AVG(cnt)) * 100.0 / NULLIFZERO(AVG(cnt)) AS SkewPct
FROM (
    SELECT Hashamp(HashRow(customer_id)) AS amp, COUNT(*) AS cnt
    FROM orders
    GROUP BY amp
) sub;

-- Step 2: Find the heavy-hitter PI values
SELECT TOP 10 customer_id, COUNT(*) AS row_count
FROM orders
GROUP BY customer_id
ORDER BY row_count DESC;
```

**Production rule:** Run skew checks as part of table deployment checklist. Any table with projected skew > 20% gets flagged for PI review before going live.

---

## Interview Tips

> **Tip 1:** "Tell me about a time a bad PI caused a production issue." — Even if you haven't experienced it personally, describe the pattern: low-cardinality PI causing AMP CPU skew, queries serializing on the hot AMP, and the fix being a rebuild with a better PI or adding PPI for partition elimination.

> **Tip 2:** "How do you migrate a PI on a live system?" — "Dual-write pattern: create new table, set up parallel writes, background-copy historical data, validate, then cut over. Or: use a scheduled maintenance window with INSERT/SELECT + rename. The dual-write approach achieves zero downtime at the cost of temporary double-storage and ETL complexity."

> **Tip 3:** "What are your PI design standards for a new DW?" — "Fact tables: NUPI on the most common JOIN FK + PPI on date. Dimension tables: UPI on surrogate key. Staging: NoPI. Always verify cardinality before committing — check the actual data distribution, not just the column semantics."

> **Tip 4:** "How do you monitor PI quality in production?" — "Weekly skew check using Hashamp(HashRow(pi_col)) COUNT per AMP. Alert when skew exceeds 30%. Also monitor DBC.TableSizeV for storage imbalance and DBQL for queries with high AMP CPU skew (one AMP taking 10× longer than others)."
