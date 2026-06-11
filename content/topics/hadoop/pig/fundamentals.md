---
title: "Pig - Fundamentals"
topic: hadoop
subtopic: pig
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [hadoop, pig, pig-latin, mapreduce, etl, data-processing]
---

# Pig — Fundamentals


## 🎯 Analogy

Think of Pig Latin like SQL's lazy cousin: a dataflow language where you load, filter, group, and store data in explicit steps — easier to write than raw MapReduce, less declarative than SQL.

---
## What is Apache Pig?

Apache Pig is a high-level data flow scripting language (Pig Latin) that compiles to MapReduce jobs. It abstracts the complexity of writing MapReduce Java code, letting analysts and engineers process large datasets with a SQL-like but more flexible syntax.

**Key characteristics:**
- Pig Latin scripts describe data flows, not algorithms
- Each statement creates a relation (like a table alias)
- Pig compiles scripts to optimized MapReduce DAGs
- Schema is optional — Pig can process schema-less data

## Execution Modes

| Mode | Description | Use case |
|------|-------------|----------|
| **Local mode** | Runs on local filesystem, single JVM | Development and testing |
| **MapReduce mode** | Runs on YARN cluster | Production large-scale jobs |
| **Tez mode** | Runs on Apache Tez DAG engine | Faster than MR (default in modern Pig) |

```bash
# Local mode
pig -x local myscript.pig

# MapReduce mode (default)
pig myscript.pig

# Tez mode
pig -x tez myscript.pig

# Interactive Grunt shell
pig -x local
grunt>
```

## Pig Latin Basics — LOAD, STORE, DUMP

```pig
-- LOAD: read data from HDFS
orders = LOAD '/data/raw/orders' 
         USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double, status:chararray, order_date:chararray);

-- DUMP: print to console (development only!)
DUMP orders;

-- STORE: write results to HDFS
STORE orders INTO '/data/output/orders' USING PigStorage('\t');

-- DESCRIBE: show schema
DESCRIBE orders;

-- EXPLAIN: show logical/physical/MapReduce plan
EXPLAIN orders;
```

## Relations and Tuples

```
Pig Data Model:
  Bag      = collection of tuples (like a table)
  Tuple    = ordered set of fields (like a row)
  Field    = a single value (int, double, chararray, etc.)
  Map      = key-value pairs

Example:
  orders: Bag
    (1, 101, 99.99, 'completed', '2024-01-15')  <- Tuple
    (2, 102, 49.50, 'pending',   '2024-01-15')  <- Tuple
```

## Data Types

| Type | Java Equivalent | Example |
|------|----------------|---------|
| `int` | Integer | `42` |
| `long` | Long | `1234567890L` |
| `float` | Float | `3.14f` |
| `double` | Double | `99.99` |
| `chararray` | String | `'hello'` |
| `bytearray` | byte[] | Raw bytes |
| `boolean` | Boolean | `true` |
| `datetime` | DateTime | `2024-01-15` |
| `tuple` | Tuple | `(1, 'a')` |
| `bag` | DataBag | `{(1),(2),(3)}` |
| `map` | Map | `[key#value]` |

## Core Operators

### FILTER
```pig
-- Filter rows based on condition
large_orders = FILTER orders BY amount > 1000.0;
completed_orders = FILTER orders BY status == 'completed';
recent_large = FILTER orders BY amount > 500.0 AND order_date >= '2024-01-01';
```

### FOREACH / GENERATE
```pig
-- Transform columns (like SELECT in SQL)
order_summary = FOREACH orders GENERATE
  order_id,
  customer_id,
  amount * 1.1 AS amount_with_tax,
  UPPER(status) AS status_upper;

-- Flatten nested bags
flattened = FOREACH grouped_orders GENERATE 
  FLATTEN(orders),
  group AS customer_id;
```

### GROUP
```pig
-- Group by a field (like GROUP BY in SQL)
by_customer = GROUP orders BY customer_id;
-- Result schema: {group: int, orders: {(order_id, customer_id, amount, ...)}}

-- Group by multiple fields
by_status_date = GROUP orders BY (status, order_date);

-- Group ALL for global aggregation
all_orders = GROUP orders ALL;
total = FOREACH all_orders GENERATE COUNT(orders) AS total_count, SUM(orders.amount) AS total_amount;
```

### JOIN
```pig
-- Load second dataset
customers = LOAD '/data/raw/customers'
            USING PigStorage(',')
            AS (customer_id:int, name:chararray, email:chararray);

-- Inner join
orders_with_customers = JOIN orders BY customer_id, customers BY customer_id;

-- Left outer join
all_orders_maybe_customers = JOIN orders BY customer_id LEFT OUTER, customers BY customer_id;
```

### ORDER BY
```pig
-- Sort output
sorted_orders = ORDER orders BY amount DESC;
sorted_by_two = ORDER orders BY order_date ASC, amount DESC;
```

### LIMIT
```pig
-- Take first N tuples
top10 = LIMIT sorted_orders 10;
```

### DISTINCT
```pig
-- Remove duplicates
unique_customers = DISTINCT (FOREACH orders GENERATE customer_id);
```

## Complete Example — Basic Aggregation

```pig
-- daily_order_summary.pig
-- Compute per-customer daily totals

-- Step 1: Load raw orders
orders = LOAD '/data/raw/orders/dt=2024-01-15'
         USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double, status:chararray);

-- Step 2: Filter to completed orders only
completed = FILTER orders BY status == 'completed';

-- Step 3: Group by customer
by_customer = GROUP completed BY customer_id;

-- Step 4: Aggregate
customer_totals = FOREACH by_customer GENERATE
  group AS customer_id,
  COUNT(completed) AS order_count,
  SUM(completed.amount) AS total_amount,
  AVG(completed.amount) AS avg_amount,
  MAX(completed.amount) AS max_amount;

-- Step 5: Sort by total
sorted = ORDER customer_totals BY total_amount DESC;

-- Step 6: Write output
STORE sorted INTO '/data/output/customer_totals/dt=2024-01-15'
      USING PigStorage('\t');
```

## Pig vs Hive

| Aspect | Pig | Hive |
|--------|-----|------|
| Language | Pig Latin (procedural) | HiveQL (declarative SQL-like) |
| Schema | Optional (schema on read) | Required (schema on write) |
| Best for | ETL transformations | SQL analytics queries |
| UDF language | Java, Python, Ruby | Java, Python |
| Nested data | Native (bags, tuples) | Complex types (array, struct) |
| Joins | Manual join syntax | SQL JOIN syntax |
| Status (2024) | Legacy, declining | Active (with Tez/Spark) |

## Execution Architecture

```
graph TD
    A["Pig Latin Script<br>.pig file"] --> B["Parser<br>Validates syntax"]
    B --> C["Logical Plan<br>Pig operators"]
    C --> D["Optimizer<br>Filter pushdown etc."]
    D --> E["Physical Plan<br>MR operators"]
    E --> F["MapReduce Jobs<br>on YARN"]
    F --> G["HDFS Output"]
```


## ▶️ Try It Yourself

```bash
-- Pig Latin script: find top 10 regions by revenue
-- Save as top_regions.pig and run: pig -f top_regions.pig

orders = LOAD '/data/raw/orders/' USING PigStorage(',')
         AS (order_id:long, amount:float, region:chararray, order_date:chararray);

-- Filter out nulls and negatives
clean = FILTER orders BY amount > 0 AND region IS NOT NULL;

-- Group by region
by_region = GROUP clean BY region;

-- Sum revenue per region
revenue = FOREACH by_region GENERATE
    group AS region,
    SUM(clean.amount) AS total_revenue;

-- Sort descending and take top 10
sorted = ORDER revenue BY total_revenue DESC;
top10  = LIMIT sorted 10;

STORE top10 INTO '/data/gold/top_regions/' USING PigStorage(',');
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** Pig Latin is a data flow language, not a query language. Each statement produces a new relation — no data moves until you run `DUMP`, `STORE`, or `EXPLAIN`. This lazy evaluation lets Pig optimize the entire script before execution.

> **Tip 2:** The `GROUP ... ALL` pattern is important — it's used for global aggregations (COUNT all rows, SUM entire dataset). Without `ALL`, you group by a key; with `ALL`, you get a single bag containing every tuple.

> **Tip 3:** `FOREACH ... GENERATE` is the workhorse of Pig. Know that `FLATTEN` is used inside `FOREACH` to unnest bags from a `GROUP` operation — this is the Pig equivalent of exploding an array.

> **Tip 4:** When Pig joins two datasets, the field names from both schemas are concatenated with `::` to avoid ambiguity: `orders::customer_id` vs `customers::customer_id`. This is different from SQL and trips up many candidates.

> **Tip 5:** Pig is largely considered legacy in the modern data stack. In interviews, if asked "when would you use Pig over Hive/Spark?", the honest answer is: for legacy scripts that haven't been migrated, or for ad-hoc unstructured data exploration where schema inference saves time.
