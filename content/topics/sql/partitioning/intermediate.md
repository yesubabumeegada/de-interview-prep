---
title: "SQL Partitioning - Intermediate"
topic: sql
subtopic: partitioning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, partitioning, sub-partitioning, partition-maintenance, constraint-exclusion, snowflake-clustering]
---

# SQL Partitioning — Intermediate Concepts

## Composite Partitioning (Sub-Partitioning)

You can partition a partition — useful when a single partition key doesn't provide enough granularity:

```sql
-- Partition events by year, then by region within each year
CREATE TABLE events (
    event_id   BIGSERIAL,
    event_date DATE NOT NULL,
    region     TEXT NOT NULL,
    user_id    BIGINT,
    event_type TEXT
) PARTITION BY RANGE (event_date);

-- Year-level partitions:
CREATE TABLE events_2024 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
    PARTITION BY LIST (region);  -- Sub-partitioned by region

-- Region-level sub-partitions within 2024:
CREATE TABLE events_2024_us PARTITION OF events_2024 FOR VALUES IN ('US');
CREATE TABLE events_2024_eu PARTITION OF events_2024 FOR VALUES IN ('EU');
CREATE TABLE events_2024_apac PARTITION OF events_2024 FOR VALUES IN ('APAC');

-- Query benefits from both partition keys:
EXPLAIN SELECT * FROM events WHERE event_date = '2024-06-15' AND region = 'US';
-- Pruning: scans ONLY events_2024_us — all other partitions (3+ years × 3 regions) skipped
```

**When sub-partitioning helps:**
- Very large single partitions (>50GB)
- Queries always filter on BOTH keys
- Data lifecycle management needed at multiple levels (drop a year, or drop a region within a year)

---

## Partition-Wise Joins and Aggregations

When two partitioned tables are joined on the partition key, the database can process each partition pair independently — enabling better parallelism:

```sql
-- Both tables partitioned by the same key: customer_id
-- PostgreSQL performs a partition-wise join (each partition pairs with its counterpart)

EXPLAIN SELECT c.name, SUM(o.amount)
FROM customers c   -- Partitioned by HASH(customer_id), 8 partitions
JOIN orders o ON c.customer_id = o.customer_id  -- Same partitioning scheme
GROUP BY c.name;

-- EXPLAIN output (with enable_partitionwise_join = on):
-- Hash Join (Partition-Wise)
--   -> Seq Scan on customers_p0
--   -> Hash on orders_p0
-- (Repeated for p1, p2, ... p7 independently)
-- Much better parallelism: 8 independent join operations vs. one big join
```

```sql
-- Enable partition-wise operations (PostgreSQL):
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;

-- Verify:
SHOW enable_partitionwise_join;
```

---

## Partition Maintenance Automation

Managing 36+ partitions manually is error-prone. Automate with scripts:

### PostgreSQL: Automatic Partition Creation

```sql
-- Function to create a monthly partition for any table
CREATE OR REPLACE FUNCTION create_monthly_partition(
    p_parent_table TEXT,
    p_year         INT,
    p_month        INT
) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_start_date   DATE := make_date(p_year, p_month, 1);
    v_end_date     DATE := v_start_date + INTERVAL '1 month';
    v_partition    TEXT := format('%s_%s_%s', p_parent_table, p_year, LPAD(p_month::TEXT, 2, '0'));
    v_sql          TEXT;
BEGIN
    v_sql := format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        v_partition, p_parent_table, v_start_date, v_end_date
    );
    EXECUTE v_sql;
    RETURN v_partition || ' created';
END;
$$;

-- Create next 3 months of partitions (run from Airflow or cron):
SELECT create_monthly_partition('orders', 2024, 4);  -- April 2024
SELECT create_monthly_partition('orders', 2024, 5);  -- May 2024
SELECT create_monthly_partition('orders', 2024, 6);  -- June 2024
```

### Archiving Old Partitions

```sql
-- Detach and archive partitions older than 2 years
CREATE OR REPLACE PROCEDURE archive_old_partitions(p_retention_months INT DEFAULT 24)
LANGUAGE plpgsql AS $$
DECLARE
    v_cutoff_date DATE := DATE_TRUNC('month', NOW()) - (p_retention_months || ' months')::INTERVAL;
    v_partition   RECORD;
    v_archive_name TEXT;
BEGIN
    FOR v_partition IN
        SELECT child.relname AS partition_name,
               pg_get_expr(child.relpartbound, child.oid) AS bound
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = 'orders'
    LOOP
        -- Parse the partition bound to check if it's before the cutoff
        -- (simplified: check name convention)
        IF v_partition.partition_name ~ '^orders_20[0-9]{2}_(0[1-9]|1[0-2])$' THEN
            -- Extract year/month from partition name
            -- In production: parse actual partition bounds more carefully
            DECLARE
                v_year INT := (regexp_match(v_partition.partition_name, '_(\d{4})_'))[1]::INT;
                v_month INT := (regexp_match(v_partition.partition_name, '_(\d{2})$'))[1]::INT;
                v_part_date DATE := make_date(v_year, v_month, 1);
            BEGIN
                IF v_part_date < v_cutoff_date THEN
                    v_archive_name := 'archive.' || v_partition.partition_name;
                    EXECUTE format('ALTER TABLE orders DETACH PARTITION %I', v_partition.partition_name);
                    EXECUTE format('ALTER TABLE %I SET SCHEMA archive', v_partition.partition_name);
                    RAISE NOTICE 'Archived partition: %', v_partition.partition_name;
                END IF;
            END;
        END IF;
    END LOOP;
    COMMIT;
END;
$$;
```

---

## Default Partitions

Without a default partition, inserting a row with a value not covered by any partition raises an error:

```sql
-- Without a default partition: this fails if no partition covers May:
INSERT INTO orders VALUES (1, 101, 99.99, '2024-05-01');
-- ERROR: no partition of relation "orders" found for row

-- Add a default partition to catch overflow:
CREATE TABLE orders_default PARTITION OF orders DEFAULT;

-- Now May's order lands in orders_default
-- Later: create orders_2024_05, migrate rows, then drop the default overflow

-- Check what's in the default partition:
SELECT order_date, COUNT(*) FROM orders_default GROUP BY order_date ORDER BY order_date;
```

**Best practice:** Always have a default partition in production. Monitor it for unexpected data — data landing in the default is a sign that partition creation is behind schedule.

---

## Cross-Dialect Partitioning

### Snowflake: Cluster Keys (Not Traditional Partitioning)

Snowflake doesn't use traditional partitioning — instead it organizes data into **micro-partitions** (compressed ~100MB blocks) and allows a **cluster key** to control their physical ordering:

```sql
-- Define a cluster key on the table:
ALTER TABLE orders CLUSTER BY (TO_DATE(order_date), customer_id);
-- Snowflake reorganizes micro-partitions so rows with similar (date, customer_id) are co-located
-- Queries filtering on order_date or customer_id can skip irrelevant micro-partitions

-- Check clustering effectiveness:
SELECT SYSTEM$CLUSTERING_INFORMATION('orders', '(TO_DATE(order_date))');
-- Returns: clustering depth, overlap ratio, etc.

-- Suspend/resume automatic reclustering (costly operation):
ALTER TABLE orders SUSPEND RECLUSTER;
ALTER TABLE orders RESUME RECLUSTER;

-- Manual one-time recluster:
ALTER TABLE orders RECLUSTER;
```

**Snowflake micro-partition characteristics:**
- Each micro-partition: 50–500MB uncompressed, auto-compressed
- Min/max metadata tracked per column per micro-partition
- Query pruning: if `WHERE order_date = '2024-01-15'` and a micro-partition's date range is 2024-03-01 to 2024-04-30, that partition is skipped
- No DDL needed — clustering is a configuration, not a schema change

### BigQuery: Partitioned and Clustered Tables

```sql
-- BigQuery: partition by date, cluster by additional columns
CREATE TABLE `project.dataset.orders`
PARTITION BY DATE(order_date)  -- Auto-partitioned by date (daily)
CLUSTER BY customer_id, status
OPTIONS (
    partition_expiration_days = 730,  -- Auto-delete partitions after 2 years
    require_partition_filter = TRUE   -- Queries MUST filter on order_date (cost control)
)
AS SELECT * FROM `project.dataset.raw_orders`;

-- BigQuery: query must filter on partition column (if require_partition_filter = TRUE):
SELECT SUM(amount) FROM `project.dataset.orders`
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31';
-- Scans only 3 months of data; clustered by customer_id → further pruning

-- Check how much data would be scanned:
-- BigQuery shows estimated bytes scanned in query editor before running
```

### Redshift: Distribution Styles

```sql
-- Redshift: SORTKEY (not partitioning, but achieves similar benefits)
CREATE TABLE orders (
    order_id    BIGINT,
    customer_id BIGINT DISTKEY,  -- Rows with same customer_id go to same node
    order_date  DATE,
    amount      DECIMAL(10,2)
) SORTKEY(order_date);  -- Rows stored in order_date order within each node

-- Zone maps: Redshift automatically tracks min/max per 1MB block
-- Query: WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
-- → Skips blocks whose max order_date < '2024-01-01' or min order_date > '2024-03-31'
-- → Similar effect to partition pruning, without explicit partition management

-- Redshift: VACUUM and ANALYZE needed to maintain sort order:
VACUUM orders;  -- Re-sort unsorted rows
ANALYZE orders; -- Update statistics
```

---

## Performance Testing Partition Pruning

Always verify pruning is actually happening:

```sql
-- PostgreSQL: confirm partition pruning in EXPLAIN output
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(amount) FROM orders WHERE order_date >= '2024-01-01' AND order_date < '2024-04-01';

-- Look for:
-- "Partitions selected: 3 out of 36" → pruning is working
-- If you see all partitions listed: pruning is NOT working (check partition key)

-- Enable detailed partition info:
SET enable_partition_pruning = on;  -- Should be on by default

-- Test partition pruning is enabled:
SELECT current_setting('enable_partition_pruning');
```

---

## Common Pitfalls

| Pitfall | Problem | Fix |
|---------|---------|-----|
| Function on partition key | `YEAR(order_date)` defeats pruning | Use range: `order_date >= '2024-01-01'` |
| Too many partitions | Thousands of partitions → planning overhead | Keep to hundreds, not thousands |
| Missing default partition | Insert fails for out-of-range values | Always create DEFAULT partition |
| Forgetting to create future partitions | Insert fails when month rolls over | Automate partition creation (cron/Airflow) |
| Not indexing partitions | Index on parent doesn't create in child (pre-PG11) | PG11+: indexes automatically propagate |
| Cross-partition sort | ORDER BY without partition key → merge sort across all partitions | Include partition key in ORDER BY |

---

## Interview Tips

> **Tip 1:** "How does Snowflake's clustering key differ from PostgreSQL partitioning?" — "They solve the same problem (skipping irrelevant data) but differently. PostgreSQL creates physically separate tables (partitions) that are merged logically. Snowflake maintains a single logical table as micro-partitions (100MB blocks) with min/max metadata. A clustering key guides Snowflake to keep similar values in the same micro-partitions, enabling pruning. Snowflake's approach is maintenance-free (no partition creation needed) but less explicit — effectiveness depends on how well-clustered the data is."

> **Tip 2:** "What happens if a partition gets too large?" — "A range partition that's too large defeats the purpose — you're scanning a huge partition for most queries. Solutions: (1) use a finer granularity (switch from monthly to weekly partitions), (2) add a second dimension with sub-partitioning, (3) for Snowflake: reclustering with a more selective cluster key. In practice, aim for each partition to be between 1GB and 100GB — large enough to amortize metadata overhead, small enough for meaningful pruning."

> **Tip 3:** "Can you use partitioning with foreign keys?" — "In PostgreSQL, foreign keys to or from partitioned tables have limitations — you cannot have a foreign key pointing to a specific partition (only to the parent), and foreign keys FROM a partitioned table TO another table work but have overhead. In practice, large partitioned tables are often in data warehouses where foreign key constraints are disabled anyway (enforced at ETL time). For OLTP partitioned tables, use application-level or trigger-based FK enforcement."
