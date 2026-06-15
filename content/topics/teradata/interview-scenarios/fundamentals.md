---
title: "Teradata Interview Scenarios - Fundamentals"
topic: teradata
subtopic: interview-scenarios
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [teradata, interview, scenarios, primary-index, bteq]
---

# Teradata Interview Scenarios — Fundamentals

This guide covers the foundational Teradata concepts most frequently tested in junior Data Engineer interviews: Primary Index selection, the EXPLAIN plan, and core architecture Q&A.

---

## 1. Primary Index (PI) — UPI vs. NUPI

The Primary Index is the single most important concept in Teradata. It determines **which AMP** (Access Module Processor) stores each row. The hashing algorithm maps the PI value to an AMP, so choosing the right PI directly controls data distribution, parallelism, and query performance.

### Unique Primary Index (UPI)

A UPI guarantees that every row has a distinct PI value. Teradata enforces uniqueness at insert time.

**When to choose UPI:**
- The column is a natural or surrogate key (e.g., `order_id`, `customer_id`).
- You need guaranteed even distribution across AMPs (no skew).
- The table will be joined frequently on this column.

```sql
CREATE TABLE orders (
    order_id     INTEGER NOT NULL,
    customer_id  INTEGER,
    order_date   DATE,
    total_amount DECIMAL(15,2)
) UNIQUE PRIMARY INDEX (order_id);
```

Because `order_id` is unique, rows are evenly spread across all AMPs. A full-table scan or a join on `order_id` runs in perfect parallel.

### Non-Unique Primary Index (NUPI)

A NUPI allows duplicate PI values. Multiple rows with the same PI land on the **same AMP**, stored in a hash synonym chain.

**When to choose NUPI:**
- The best join/access column is not unique (e.g., `customer_id` in a `sales` fact table — one customer has many sales).
- You still want row co-location with a related table that has `customer_id` as its UPI.

```sql
CREATE TABLE sales_fact (
    sale_id      INTEGER NOT NULL,
    customer_id  INTEGER,       -- NUPI: many sales per customer
    product_id   INTEGER,
    sale_date    DATE,
    amount       DECIMAL(15,2)
) PRIMARY INDEX (customer_id);
```

**Trade-off:** If one `customer_id` accounts for 30% of rows, that single AMP holds 30% of the data — this is **skew**, and it kills parallelism.

### Decision Matrix

| Criterion | Choose UPI | Choose NUPI |
|---|---|---|
| Column values unique? | Yes | No |
| Primary join key unique? | Yes | No |
| Skew risk? | None | Possible |
| Co-location needed with a NUPI table? | No | Yes |

### Common Interview Question

**Q: A dimension table has 1 million rows with a unique `dim_id`. What PI would you use?**

> **A:** `UNIQUE PRIMARY INDEX (dim_id)`. It is unique, so UPI guarantees even distribution across AMPs and avoids skew entirely. It also serves as the natural join key for fact-to-dimension joins, enabling AMP-local joins when the fact table also uses `dim_id` (or a related key) as its PI.

---

## 2. EXPLAIN Plan Basics

The `EXPLAIN` modifier prefixes any SQL statement and returns Teradata's query execution plan in plain English — no query actually runs.

```sql
EXPLAIN
SELECT c.customer_name, SUM(s.amount) AS total_sales
FROM   sales_fact s
JOIN   customer_dim c ON s.customer_id = c.customer_id
WHERE  s.sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
GROUP BY c.customer_name;
```

### Reading EXPLAIN Output — Key Phrases

| Phrase | Meaning | Good or Bad? |
|---|---|---|
| `"We do an all-AMPs retrieve"` | Full-table scan | Depends — expected for large fact tables without a WHERE on PI |
| `"We do a single-AMP retrieve"` | PI equality lookup | Good — uses PI hash to go directly to one AMP |
| `"We do a two-AMP retrieve"` | Hash synonym chain | Good for NUPI equality |
| `"(redistributed by hash)"` | Rows moved across AMPs for join | Cost — check if avoidable |
| `"(duplicated on all AMPs)"` | Small table broadcast to all AMPs | Acceptable for small dimensions |
| `"confidence: none"` | No statistics on column — optimizer guessing | Bad — collect stats |
| `"Skew: NN%"` | One AMP doing disproportionate work | Bad — re-evaluate PI |

### Identifying a Missing Statistics Problem

```sql
EXPLAIN
SELECT * FROM orders WHERE order_date = DATE '2024-06-01';
```

If EXPLAIN shows `"confidence: none"` or `"no statistics available"` for `order_date`, the optimizer cannot estimate row count and may choose a suboptimal plan (e.g., full scan instead of partition elimination). Fix:

```sql
COLLECT STATISTICS COLUMN (order_date) ON orders;
```

Re-run EXPLAIN and verify confidence improves to `"low"` or `"high"`.

---

## 3. Junior-Level Interview Q&A

### Q1: What is an AMP and what does it do?

**A:** An AMP (Access Module Processor) is the fundamental processing unit in Teradata's shared-nothing architecture. Each AMP owns a subset of disk (its vdisk) and an independent processor. When a query runs, all AMPs work in parallel on their local data. The hash of the Primary Index value determines which AMP stores each row. A 100-AMP system stores roughly 1/100 of each table's rows per AMP (assuming even distribution).

### Q2: What is the difference between a Primary Index and a Primary Key?

**A:**
- **Primary Key** is a logical, relational constraint — it enforces uniqueness and is used by application logic. In Teradata, a PK is declared with `PRIMARY KEY` syntax and implies a UPI, but it does not physically control data placement.
- **Primary Index** is a *physical* storage mechanism — it controls which AMP stores each row via hashing. A table can have a different PI than its PK. For example, a table with `order_id` as PK might have `customer_id` as its NUPI for better join performance.

### Q3: What happens during a full-table scan in Teradata?

**A:** All AMPs simultaneously read all rows on their local disk. Because Teradata is massively parallel, a full-table scan can be fast — but only if work is evenly distributed. If one AMP has 80% of rows (skew), that AMP becomes the bottleneck and query time approaches a serial scan.

### Q4: What is the Parsing Engine (PE)?

**A:** The PE receives the SQL from the client, parses and optimizes it, generates a query plan, and dispatches step instructions to the AMPs via the BYNET interconnect. The PE does not touch row data itself — all data processing happens on the AMPs. Multiple PEs can run in parallel, each handling different sessions.

### Q5: What is BYNET?

**A:** BYNET is Teradata's high-speed internal network connecting all PEs and AMPs. It is used for two purposes:
1. **Step dispatching** — the PE sends query steps to AMPs.
2. **Data redistribution** — AMPs exchange rows with each other (e.g., for joins where tables are not co-located).

### Q6: How does Teradata differ from a row-store RDBMS like Oracle?

**A:** The key differences are:
- **Shared-nothing vs. shared-disk**: Teradata AMPs own exclusive disk; Oracle RAC nodes share a storage layer.
- **Automatic parallelism**: Teradata parallelizes automatically based on PI hashing. Oracle requires hints or partitioning.
- **No indexes needed for distribution**: Teradata's PI replaces the need for B-tree indexes for most lookups.
- **Scale-out architecture**: Adding cabinets adds AMPs and proportionally increases capacity and throughput.

### Q7: What is fallback protection?

**A:** Fallback keeps a second copy of each row on a different AMP. If an AMP fails, its fallback AMP serves requests. The trade-off is 2× storage cost and slightly more write overhead. For mission-critical tables, fallback is enabled; for staging or temp tables, it is typically omitted.

---

## 4. Common Traps in Junior Interviews

### Trap 1: Confusing PI with a partition

A Partition is defined separately with `PARTITION BY` (e.g., by date range). A PI is a hash-based routing mechanism. You can have both — a **Partitioned Primary Index (PPI)** — which combines hash-routing (PI) with range partitioning (date).

```sql
CREATE TABLE orders_ppi (
    order_id    INTEGER NOT NULL,
    customer_id INTEGER,
    order_date  DATE
)
PRIMARY INDEX (customer_id)           -- routes to AMP by hash of customer_id
PARTITION BY RANGE_N(
    order_date BETWEEN DATE '2020-01-01' AND DATE '2025-12-31'
    EACH INTERVAL '1' MONTH           -- 72 monthly partitions
);
```

### Trap 2: Assuming UPI is always better

A UPI guarantees even distribution, but if most queries join on a NUPI column, forcing UPI on a different column causes redistributions on every join. Match the PI to the most frequent join key, not just the unique key.

### Trap 3: Forgetting that NULL values hash consistently

All NULL PI values hash to the same AMP. A table with many NULL `customer_id` values on a NUPI will have heavy skew on one AMP. Always use `NOT NULL` or a default sentinel for PI columns.
