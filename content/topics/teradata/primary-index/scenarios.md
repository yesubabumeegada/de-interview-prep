---
title: "Teradata - Primary Index Scenarios"
topic: teradata
subtopic: primary-index
content_type: scenario_question
difficulty_level: senior
layer: scenarios
tags: [teradata, primary-index, scenarios, skew, ppi, nupi, upi]
---

# Primary Index — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Choose the Right Primary Index

You are designing a new `orders` table in Teradata. The table will have these columns:
- `order_id` (BIGINT, unique, auto-generated)
- `customer_id` (INTEGER, foreign key to customer table, ~1M distinct values)
- `order_date` (DATE)
- `status` (VARCHAR(10): 'OPEN', 'SHIPPED', 'CLOSED', 'CANCELLED')
- `total_amount` (DECIMAL)

The most common queries are:
1. `SELECT ... FROM orders WHERE customer_id = ?` (lookup by customer)
2. `SELECT ... FROM orders o JOIN customer c ON o.customer_id = c.customer_id` (join with customer)
3. `SELECT ... FROM orders WHERE order_date BETWEEN ? AND ?` (date range reports)

Which column(s) should you choose as the Primary Index? Should you add PPI?

<details>
<summary>💡 Hint</summary>

Think about which queries benefit most from AMP-local operations. What column do both the WHERE clause lookups and the JOIN use? What column is used for range filtering that PPI could help with?

</details>

<details>
<summary>✅ Solution</summary>

**Recommended design:**

```sql
CREATE TABLE orders (
    order_id    BIGINT NOT NULL,
    customer_id INTEGER NOT NULL,
    order_date  DATE NOT NULL,
    status      VARCHAR(10),
    total_amount DECIMAL(12,2)
)
PRIMARY INDEX (customer_id)   -- NUPI (multiple orders per customer)
PARTITION BY RANGE_N(
    order_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);
```

**Why `customer_id` as PI:**
- Query 1: `WHERE customer_id = ?` → AMP-local (hash routes to one AMP)
- Query 2: `JOIN customer c ON o.customer_id = c.customer_id` → if customer table also has PI on `customer_id`, this join is AMP-local (no BYNET redistribution)
- 1M distinct values → good distribution, low skew risk
- NUPI because one customer has many orders (not unique)

**Why NOT `order_id`:**
- `order_id` is unique (good distribution), but queries never filter by order_id
- JOIN is on customer_id — using order_id as PI forces redistribution on every customer JOIN

**Why NOT `status`:**
- Only 4 distinct values → severe skew (e.g., 60% 'CLOSED' = one AMP gets 60% of data)

**Why PPI on `order_date`:**
- Query 3 filters by date range → partition elimination reduces scan to only relevant months
- A 5-year table with monthly partitions: date-range query scans 1/60th of partitions

**Trade-off acknowledged:** If some customers have millions of orders (high-value accounts), even `customer_id` NUPI can skew. In that case: composite PI `(customer_id, order_id)` or accept NUPI skew for top customers.

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Diagnosing a Slow Query Due to Skew

A query that used to run in 5 minutes now takes 45 minutes. The EXPLAIN plan shows it's a full table scan on `transactions` (500M rows). The table uses `merchant_category_code` (30 distinct values) as its PI.

The query is:
```sql
SELECT merchant_category_code, SUM(amount) AS total
FROM transactions
WHERE txn_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY merchant_category_code;
```

Diagnose the problem and propose a fix.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

1. **PI skew is the root cause.** With only 30 distinct `merchant_category_code` values across potentially hundreds of AMPs, most AMPs have zero rows and a few have millions of rows.

2. **Verify skew:**
```sql
SELECT Hashamp(HashRow(merchant_category_code)) AS amp, COUNT(*) AS cnt
FROM transactions
GROUP BY amp
ORDER BY cnt DESC;
-- Expected: 30 AMPs have rows, rest have 0
```

3. **Performance model:** If 2 AMPs each have 250M rows (and 200 AMPs have 0), the query effectively runs on 2 AMPs sequentially — losing all parallelism.

4. **The query recently got slower:** Likely because the table grew (more transactions), making the skewed AMPs even more loaded.

**Fix Options (in order of effort):**

**Option A (Quick): Add PPI, change PI**
```sql
-- Rebuild table with better PI + PPI
CREATE TABLE transactions_new (
    txn_id       BIGINT NOT NULL,
    merchant_category_code CHAR(4),
    txn_date     DATE NOT NULL,
    amount       DECIMAL(12,2)
)
PRIMARY INDEX (txn_id)         -- high cardinality, even distribution
PARTITION BY RANGE_N(
    txn_date BETWEEN DATE '2020-01-01' AND DATE '2025-12-31'
    EACH INTERVAL '1' MONTH
);
```
- `txn_id` distributes evenly across all AMPs
- PPI on `txn_date` gives partition elimination for the Q1 2024 filter (3 of 72 partitions)
- The GROUP BY on `merchant_category_code` still runs in parallel across all AMPs, then aggregates

**Option B (Immediate relief without rebuild): NUSI**
```sql
CREATE INDEX (txn_date) ON transactions;
```
- Adds a NUSI on `txn_date` — the query can use the NUSI to find rows in date range faster than full scan
- Still suffers from skew on the base table, but less data needs to be touched

**Recommendation:** Option A (rebuild) is the permanent fix. Option B buys time.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: PI Design for a Multi-Tenant SaaS Data Warehouse

You are designing a Teradata data warehouse for a SaaS company. The warehouse stores data from 500 enterprise customer tenants. Key characteristics:
- 500 tenants, but 10 tenants account for 70% of all data (extreme skew in tenant size)
- Each tenant's data is completely isolated (no cross-tenant queries)
- Most queries: `WHERE tenant_id = ? AND date BETWEEN ? AND ?`
- The largest tenant has 200 billion rows in the fact table; the smallest has 50,000 rows

If you use `tenant_id` as the PI, you'll have severe skew. What are your design options and trade-offs?

<details>
<summary>💡 Hint</summary>

Think about: can you include more columns in the PI to break up the large tenants? What about hash partitioning techniques? Consider also whether row-level security allows alternative architectures.

</details>

<details>
<summary>✅ Solution</summary>

**The Problem:**
- `tenant_id` as PI: Top 10 tenants (70% of data) all hash to 10 AMPs → severe skew
- The largest single tenant (200B rows) → one AMP gets enormous data → all queries for that tenant serialize

**Option 1: Composite PI with tenant + surrogate key**

```sql
CREATE TABLE events_fact (
    tenant_id   INTEGER NOT NULL,
    event_id    BIGINT NOT NULL,
    event_date  DATE NOT NULL,
    event_type  VARCHAR(50),
    payload     VARCHAR(2000)
)
PRIMARY INDEX (tenant_id, event_id)   -- composite NUPI
PARTITION BY RANGE_N(
    event_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);
```

**Pro:** Even distribution (tenant_id + event_id hash is diverse). **Con:** Queries filter only by `tenant_id` — the composite PI still routes to many AMPs per tenant. Not truly AMP-local for tenant filters.

**Option 2: Row Hash Banding (advanced)**

Use a computed hash band to artificially diversify:

```sql
-- Store a hash_band column = MOD(event_id, 100)
-- Composite PI: (tenant_id, hash_band)
-- Queries: WHERE tenant_id = ? AND hash_band IN (0..99) -- implicit full scan with distribution
```

This forces one tenant's data across 100 different AMPs. The cost: every tenant query is now a range/set scan across 100 AMP-local slices instead of one. With parallel AMPs, this is still fast.

**Option 3: Separate databases per large tenant**

For the top 10 large tenants, create **separate Teradata databases** (schemas) with their own tables and PIs tuned to their query patterns. Small tenants share a common multi-tenant table.

```
Large Tenant DB: tenant_bigcorp.events (PI = event_id, PPI = event_date)
Large Tenant DB: tenant_megacorp.events (PI = user_id, PPI = event_date)
Multi-tenant DB: shared.events (PI = tenant_id + event_id, PPI = event_date)
```

**Pro:** Each large tenant's table is fully optimized. **Con:** Schema management complexity, 10× DDL to maintain.

**Option 4: Columnar (column-partitioned) tables for large tenants**

Use Teradata's **Column Partition (CP)** feature on large tenant tables:
```sql
CREATE TABLE events_fact (...) 
NO PRIMARY INDEX
PARTITION BY COLUMN;
```
Column partitioning stores data by column, enabling projection pushdown. Combined with date partitioning, queries touching few columns on date ranges are extremely efficient even without PI-based routing.

**Recommended Architecture:**

- Tier 1 (top 10 tenants): Separate databases, tuned PI per tenant, full Fallback
- Tier 2 (mid 90 tenants): Composite PI (tenant_id + event_id), PPI on date, Fallback enabled
- Tier 3 (400 small tenants): Composite NUPI, PPI on date, Fallback optional

**Trade-offs to mention:**
- Option 1 is simplest but has query routing overhead
- Option 3 gives best performance but multiplies operational complexity
- The right choice depends on tenant SLA requirements and ops team capacity

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Primary Index (PI) in Teradata and what does it control?**
A: The Primary Index is a column (or set of columns) that determines how rows are distributed across AMPs. Teradata hashes the PI value and uses the hash to assign the row to a specific AMP. The PI is the single most important design decision in Teradata—it determines data distribution, parallelism, and join performance.

**Q: What is the difference between a Unique Primary Index (UPI) and a Non-Unique Primary Index (NUPI)?**
A: A UPI guarantees that no two rows have the same PI value, meaning each AMP gets at most one row per PI value—perfect even distribution. A NUPI allows duplicates, which can lead to data skew if certain PI values are common (e.g., NULL or a low-cardinality status field). UPIs are also used to enforce uniqueness constraints.

**Q: What is an AMP skew and how does it impact query performance?**
A: AMP skew occurs when the PI distribution is uneven—some AMPs have significantly more rows than others. Since query performance is bounded by the slowest AMP, skew means the overloaded AMPs become bottlenecks. Queries and loads take longer, and hot AMPs can exhaust disk space. Choose a high-cardinality PI to prevent skew.

**Q: How do you check for AMP skew in Teradata?**
A: Query `DBC.TableSizeV` or `DBCINFO.Table_Size` grouped by `VProc` (AMP number) to see row and byte counts per AMP. A skew factor > 10-15% is typically concerning. Tools like Teradata Viewpoint and `SHOW TABLE` also report skew metrics. The skew factor is `(max_AMP_size - avg_AMP_size) / avg_AMP_size * 100`.

**Q: What is a Primary Index join and why is it preferred?**
A: A PI join occurs when two tables are joined on their Primary Index columns. Since data is co-located on the same AMP by PI hash, the join can be performed locally on each AMP without any inter-AMP data redistribution over BYNET. This is the most efficient join type in Teradata—zero BYNET traffic.

**Q: What is a Product Join (redistribution join) in Teradata?**
A: A Product Join (also called a redistribute join) is triggered when tables are joined on non-PI columns. Teradata must redistribute one or both tables across AMPs by hashing on the join key before the join can execute. This generates BYNET traffic and is more expensive than a PI join. Minimizing redistributions is a key query optimization goal.

**Q: Can you change the Primary Index of an existing table?**
A: Not directly—Teradata doesn't allow altering the PI of an existing table in-place. The standard approach is to create a new table with the desired PI, INSERT-SELECT from the old table, rename the tables, and drop the original. This is expensive for large tables and requires maintenance window planning.

**Q: What is a No Primary Index (NoPI) table and when is it used?**
A: A NoPI table has no Primary Index—rows are assigned to AMPs in a round-robin fashion at insert time, guaranteeing even distribution regardless of data values. NoPI tables are used for staging tables where you want guaranteed even distribution before applying a redistribution or hash join. They cannot be used for PI joins.

---

## 💼 Interview Tips

- The Primary Index is the most important design decision in Teradata—lead with this in any architecture discussion. Interviewers at Teradata shops know this and will probe your depth immediately.
- Skew is the most common production performance problem caused by poor PI choice. Be ready to explain how to detect it, what causes it, and how to fix it (redesign the PI, possibly adding a secondary column to increase cardinality).
- Frame PI joins vs. redistribution joins as a design principle: "I align the PIs of frequently joined tables on the join key to enable PI joins and eliminate BYNET overhead." This is exactly the kind of concrete optimization thinking interviewers want.
- Know when NoPI is the right choice: staging tables that receive unordered data from multiple sources benefit from round-robin distribution to prevent skew at the cost of PI join capability.
- Be prepared to walk through the process of changing a PI on a large production table—it requires careful planning (offline window, storage for double the data, rename strategy). This operational awareness separates senior candidates.
