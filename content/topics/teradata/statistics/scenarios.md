---
title: "Teradata - Statistics Scenarios"
topic: teradata
subtopic: statistics
content_type: scenario_question
difficulty_level: senior
tags: [teradata, statistics, scenarios, stale-stats, optimizer, helpstats]
---

# Statistics — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: New Table, First Query

You've just created and loaded a new table `dim_product` with 2 million rows and the following columns:
- `product_id` (INTEGER, UPI)
- `category` (VARCHAR(50), ~200 distinct values)
- `brand` (VARCHAR(100), ~5,000 distinct brands)
- `list_price` (DECIMAL)

Before going live with queries that join `dim_product` to `fact_sales` on `product_id`, what statistics should you collect and why?

<details>
<summary>💡 Hint</summary>

Think about: which column is the join key? Which columns will appear in WHERE clauses? Does the PI column need special statistics? Does anything have high skew risk?

</details>

<details>
<summary>✅ Solution</summary>

**Statistics to collect (in priority order):**

```sql
-- 1. Primary Index statistics (CRITICAL - optimizer uses this for join decisions)
COLLECT STATISTICS ON dim_product INDEX (product_id);

-- 2. PI column statistics (redundant but some versions need both)
COLLECT STATISTICS ON dim_product COLUMN (product_id);

-- 3. Category - likely WHERE filter (200 distinct values, good for optimizer)
COLLECT STATISTICS ON dim_product COLUMN (category);

-- 4. Brand - may appear in GROUP BY or WHERE
COLLECT STATISTICS ON dim_product COLUMN (brand);

-- Optional: multi-column for common compound queries
-- COLLECT STATISTICS ON dim_product COLUMN (category, brand);
```

**Why each matters:**

- **INDEX (product_id):** The join key. The optimizer needs to know how many distinct products exist and their distribution to plan the join with fact_sales correctly. Without this, the optimizer may underestimate dim_product's size.

- **COLUMN (product_id):** Some optimizer decisions use column-level stats even when index stats exist.

- **COLUMN (category):** If analysts frequently filter `WHERE category = 'Electronics'`, the optimizer needs to estimate selectivity. 200 distinct values = ~0.5% selectivity — optimizer must know this to filter before joining.

- **COLUMN (brand):** Similar reasoning for brand-level queries.

**What you should NOT do:**
- Skip stats and let the optimizer guess — it will default to 1,000 rows, causing wrong join strategy with a 2M-row table
- Only collect one stat — collect on all query-relevant columns before going live

**Verify after collection:**
```sql
SHOW STATISTICS ON dim_product;
-- Confirm RowCount = 2,000,000 (approximately) for each collected stat
```

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Regression Investigation

A query that ran in 8 minutes yesterday now runs in 4 hours. Nothing changed in the query or table structure. When you run EXPLAIN, you see:

```
Step 3: We do an all-AMPs PRODUCT JOIN step from spool 1 (all AMPs)
  and SALES.dim_date (all AMPs)...
  The size of spool 2 is estimated with low confidence to be 1,200 rows.
```

The actual `dim_date` table has 3,650 rows (one per day for 10 years). The spool 1 at that step should have ~50 million rows from fact_sales.

Walk through your investigation and fix.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

The optimizer chose a PRODUCT JOIN because it underestimated the intermediate spool size and/or `dim_date`. With "low confidence," statistics exist but are stale or were collected on a smaller dataset.

**Step 1: Check statistics on the tables involved**

```sql
-- Check stats freshness and row counts
SELECT TableName, ColumnName, LastCollectDate, RowCount, DistinctCount
FROM DBC.StatsV
WHERE TableName IN ('fact_sales', 'dim_date')
ORDER BY TableName, LastCollectDate;
```

Likely finding: `fact_sales` stats show 1 million rows (from when stats were first collected) but the table has grown to 500 million rows. The optimizer thinks fact_sales is tiny → product join seems cheap.

**Step 2: Check for data growth**

```sql
-- Current row count
SELECT COUNT(*) FROM fact_sales;
-- Result: 500,000,000

-- What optimizer thinks
SELECT RowCount FROM DBC.StatsV
WHERE TableName = 'fact_sales' AND ColumnName = 'sale_date';
-- Result: 1,000,000 (stale from initial load)
```

**Step 3: Fix — refresh statistics**

```sql
COLLECT STATISTICS ON fact_sales INDEX (customer_id);   -- PI stats
COLLECT STATISTICS ON fact_sales COLUMN (sale_date);    -- join/filter column
COLLECT STATISTICS ON fact_sales COLUMN (PARTITION);    -- if PPI table

COLLECT STATISTICS ON dim_date INDEX (date_key);
COLLECT STATISTICS ON dim_date COLUMN (date_key);
```

**Step 4: Verify with EXPLAIN**

```sql
EXPLAIN
SELECT ...your_query...;
```

Expected new output:
```
Step 3: We do an all-AMPs MERGE JOIN (or HASH JOIN) step...
  The size of spool 2 is estimated with HIGH confidence to be 50,000,000 rows.
```

**Step 5: Prevent recurrence**

Add COLLECT STATISTICS to the ETL pipeline for `fact_sales` — trigger after any load that increases row count by > 10%.

**Why "low confidence" and not "no confidence":** Statistics existed but were stale. "No confidence" = never collected. "Low confidence" = collected, but data changed significantly. Both require COLLECT STATISTICS as the fix.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Statistics Strategy for a New Data Warehouse

You are the lead data engineer for a new Teradata data warehouse that will go live in 6 weeks. The schema has:
- 5 large fact tables (500M–10B rows each)
- 20 dimension tables (1K–50M rows each)
- 10 staging/work tables (vary daily)
- All fact tables have PPI on date columns
- The system will have 50 concurrent analyst users and 5 ETL pipelines running nightly

Design a comprehensive statistics strategy: what to collect, when, how, and how to maintain it.

<details>
<summary>💡 Hint</summary>

Think about: initial collection before go-live, per-table collection strategy (full vs sample), ETL pipeline integration, maintenance scheduling, monitoring and alerting, and handling the different table tiers (facts vs dims vs staging).

</details>

<details>
<summary>✅ Solution</summary>

**Phase 1: Pre-Go-Live Statistics (Week 5)**

After initial data load but before users have access:

```sql
-- Fact tables: collect all critical stats (no sampling — accuracy critical for go-live)
-- Run during the load testing phase

-- For each large fact table (e.g., fact_sales, 2B rows):
COLLECT STATISTICS ON fact_sales INDEX (customer_id);          -- PI
COLLECT STATISTICS ON fact_sales COLUMN (customer_id);         -- join col
COLLECT STATISTICS ON fact_sales COLUMN (product_id);          -- join col
COLLECT STATISTICS ON fact_sales COLUMN (sale_date);           -- PPI col
COLLECT STATISTICS ON fact_sales COLUMN (PARTITION);           -- partition stats
COLLECT STATISTICS ON fact_sales COLUMN (customer_id, sale_date);  -- multi-col for compound queries

-- For ultra-large tables (10B rows), use sampling for non-critical cols
COLLECT STATISTICS USING SAMPLE 10 PERCENT
    ON fact_transactions COLUMN (merchant_category);

-- Dimension tables: always full scan (they're small enough)
-- Example for dim_customer (50M rows):
COLLECT STATISTICS ON dim_customer INDEX (customer_id);
COLLECT STATISTICS ON dim_customer COLUMN (customer_id);
COLLECT STATISTICS ON dim_customer COLUMN (region);
COLLECT STATISTICS ON dim_customer COLUMN (customer_segment);
```

**Phase 2: ETL Pipeline Integration**

Each nightly ETL pipeline includes a post-load statistics step:

```sql
-- ETL step template (executed after every fact table load)
-- Parameterized BTEQ script

COLLECT STATISTICS ON ${TABLE_NAME} COLUMN (${DATE_COL});
COLLECT STATISTICS ON ${TABLE_NAME} COLUMN (PARTITION);
COLLECT STATISTICS ON ${TABLE_NAME} INDEX (${PI_COL});

-- Log collection to audit table
INSERT INTO etl_stats_log VALUES (
    CURRENT_TIMESTAMP, '${TABLE_NAME}', 'POST_LOAD_STATS', 'SUCCESS'
);
```

**Phase 3: Maintenance Schedule**

```
Daily (via ETL pipeline post-load):
  - All 5 fact tables: PI, join cols, date col, PARTITION stats
  - Dimension tables modified in that night's ETL: PI + join cols

Weekly (Sunday 2-4 AM):
  - Full refresh of ALL table statistics in production schema
  - Includes USING SAMPLE for ultra-large table non-critical columns
  - Run DIAGNOSTIC HELPSTATS review for top 20 queries from DBQL

Monthly:
  - Audit DBC.StatsV against DBC.TableSizeV for anomalies
  - Review DBQL for new slow queries (may indicate new missing stats)
  - Multi-column stats review: add any new compound predicates observed in DBQL
```

**Phase 4: Monitoring and Alerting**

```sql
-- Daily staleness alert (email if any production table has stats > 3 days old)
CREATE MACRO check_stats_staleness AS (
    SELECT TableName, MAX(LastCollectDate) AS LastStats,
           CURRENT_DATE - MAX(LastCollectDate) AS DaysOld
    FROM DBC.StatsV
    WHERE DatabaseName = 'PROD_DW'
    GROUP BY TableName
    HAVING DaysOld > 3
    ORDER BY DaysOld DESC;
);
```

```sql
-- DBQL-based regression detection (run after weekly stats refresh)
SELECT
    SUBSTR(QueryText, 1, 80) AS QueryID,
    AVG(CASE WHEN LogDate > CURRENT_DATE - 7 THEN AMPCPUTime END) AS AvgCPU_ThisWeek,
    AVG(CASE WHEN LogDate BETWEEN CURRENT_DATE - 14 AND CURRENT_DATE - 7 THEN AMPCPUTime END) AS AvgCPU_LastWeek,
    (AvgCPU_ThisWeek - AvgCPU_LastWeek) / NULLIFZERO(AvgCPU_LastWeek) * 100 AS PctChange
FROM DBC.QryLogV
WHERE LogDate >= CURRENT_DATE - 14
GROUP BY 1
HAVING PctChange > 50   -- flag queries that got 50%+ slower
ORDER BY PctChange DESC;
```

**Phase 5: Handling Staging Tables**

Staging tables are ephemeral — they're loaded, used for transformation, then truncated:
- Collect stats immediately after loading (before transformation queries run)
- No need for persistent stats — they'll be refreshed next night anyway
- For large staging tables: SAMPLE 20% is sufficient (they're short-lived)

**Key design decisions to articulate:**

1. **No go-live without statistics** — user experience on Day 1 depends on it
2. **ETL integration is mandatory** — automation, not manual refresh
3. **Tiered approach:** Full scan for critical columns, sampling for secondary
4. **Monitoring closing the loop:** DBQL regression detection catches what manual review misses
5. **Multi-column stats evolve** — add new ones based on actual query patterns observed post-launch

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are Teradata statistics and why do you collect them?**
A: Statistics in Teradata are column-level histograms recording the distribution of values (row count, distinct count, value ranges, null count) for tables and indexes. The Query Optimizer uses them to estimate cardinality at each plan step. Without accurate statistics, the Optimizer makes poor join, redistribution, and aggregation choices.

**Q: What is the syntax for collecting statistics in Teradata?**
A: `COLLECT STATISTICS ON database.tablename COLUMN column_name;` for a single column. For multi-column statistics: `COLLECT STATISTICS ON tablename INDEX (col1, col2);`. You can also collect on a full index: `COLLECT STATISTICS ON tablename INDEX index_name;`. Use `USING SAMPLE` to collect from a sample for very large tables.

**Q: What columns should you prioritize for statistics collection?**
A: Collect statistics on: Primary Index columns, secondary index columns, columns used in JOIN conditions, columns used in WHERE filters (especially with high cardinality or skewed distribution), and columns referenced in GROUP BY clauses. The goal is to give the Optimizer accurate cardinality at every step that influences plan choice.

**Q: How often should you refresh statistics and what triggers a refresh?**
A: Statistics should be refreshed after significant data changes—typically when more than 10-20% of rows have changed (inserts, updates, deletes). In practice, schedule `COLLECT STATISTICS` after major ETL loads. Teradata can track statistics currency via `SHOW STATISTICS` timestamps and suggest stale statistics via the Optimizer diagnostic output.

**Q: What is the difference between COLLECT STATISTICS and COLLECT STATISTICS USING SAMPLE?**
A: `COLLECT STATISTICS` scans the full table for exact statistics. `COLLECT STATISTICS USING SAMPLE` scans a percentage of rows (configurable, default ~10%) and extrapolates, using much less time and I/O for very large tables. Sample statistics are less precise but often good enough for the Optimizer to make better decisions than having no statistics at all.

**Q: What does "no statistics" in an EXPLAIN output mean and what should you do?**
A: "No statistics" means the Optimizer found no collected statistics for the referenced column and fell back to system-level defaults (random AMP estimates). This almost always leads to suboptimal plans—especially bad for join ordering and redistribution decisions. Collect statistics on those columns immediately and re-EXPLAIN the query.

**Q: What is SHOW STATISTICS in Teradata?**
A: `SHOW STATISTICS ON tablename;` displays all currently collected statistics on a table, including column(s), index name, collection date, and row count at collection time. It's used to audit which statistics exist, how recent they are, and whether critical columns are covered before diagnosing optimizer plan issues.

**Q: What is the Optimizer's behavior when table data changes significantly after statistics were collected?**
A: The Optimizer's estimates become increasingly inaccurate as the data drifts from the collected statistics. This can cause plan regressions—a query that ran efficiently with fresh statistics may switch to a Product Join or unnecessary full-table scan after statistics become stale. Monitoring statistics age relative to data change rate is an essential operational practice.

---

## 💼 Interview Tips

- Lead with the connection between statistics freshness and Optimizer plan quality—this is the core concept, not just a technical detail. Every query optimization discussion in Teradata eventually traces back to statistics.
- Be specific about which columns to prioritize: join keys, filter columns, PI columns, and high-cardinality GROUP BY columns. Generic advice ("collect stats on everything") shows less understanding than targeted recommendations.
- Mention that statistics collection itself consumes system resources—full scans on large tables during peak hours can impact production. Discuss scheduling stats collection in off-peak windows or using USING SAMPLE for time-sensitive situations.
- Know the "no statistics" EXPLAIN marker cold. It's a red flag in any EXPLAIN output and the first thing to investigate when queries are underperforming. Interviewers who've worked in production Teradata environments expect you to recognize it immediately.
- Senior interviewers will ask about statistics management at scale—hundreds of tables, multiple ETL loads per day. Discuss automated statistics collection strategies (e.g., collecting stats as part of each ETL job, not as a separate weekly batch) and monitoring for staleness.
