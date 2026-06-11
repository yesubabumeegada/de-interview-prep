---
title: "PySpark Joins — Fundamentals"
topic: pyspark
subtopic: joins
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, joins, inner-join, left-join, broadcast, null-handling]
---

# PySpark Joins — Fundamentals

Joins are the backbone of nearly every data pipeline. Whether you're building a star schema, enriching events with user metadata, or reconciling records across systems, you need to understand how PySpark joins work — not just the syntax, but the semantics and gotchas.

---


## 🎯 Analogy

Think of Spark joins like merging two sorted filing cabinets. A broadcast join is like photocopying the small cabinet and handing a copy to every worker — no coordination needed. A sort-merge join is like having all workers sort their drawers first, then zip them together.

---
## Join Types Overview

PySpark supports all standard SQL join types. Each has a specific use case in DE workflows.

| Join Type | Rows Kept | Typical DE Use Case |
|---|---|---|
| `inner` | Only matching rows from both sides | Fact + Dimension enrichment (confirmed matches only) |
| `left` (left outer) | All rows from left, matched from right | Keep all events, optionally enrich with user data |
| `right` (right outer) | All rows from right, matched from left | Less common; used when right side is the "primary" table |
| `full` (full outer) | All rows from both sides | Reconciliation, diff reports, CDC comparison |
| `left_semi` | Left rows WHERE a match exists in right | Filter: keep events for users in a whitelist table |
| `left_anti` | Left rows WHERE no match exists in right | Filter: find events with no corresponding user record |
| `cross` | Cartesian product | Date spine generation, feature cross-join (dangerous at scale) |

---

## Basic Join Syntax

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("joins-demo").getOrCreate()

# Sample data
orders = spark.createDataFrame([
    (1, 101, 250.0),
    (2, 102, 175.0),
    (3, 999, 50.0),   # user 999 does not exist in users table
], ["order_id", "user_id", "amount"])

users = spark.createDataFrame([
    (101, "Alice", "US"),
    (102, "Bob", "UK"),
    (103, "Carol", "US"),  # Carol has no orders
], ["user_id", "name", "country"])

# Inner join — only orders WITH a matching user
inner = orders.join(users, on="user_id", how="inner")
inner.show()
# +-------+--------+------+-----+-------+
# |user_id|order_id|amount| name|country|
# +-------+--------+------+-----+-------+
# |    101|       1| 250.0|Alice|     US|
# |    102|       2| 175.0|  Bob|     UK|
# +-------+--------+------+-----+-------+
# Note: order 3 (user 999) and Carol (user 103) are dropped

# Left join — all orders, fill nulls where user doesn't exist
left = orders.join(users, on="user_id", how="left")
left.show()
# +-------+--------+------+-----+-------+
# |user_id|order_id|amount| name|country|
# +-------+--------+------+-----+-------+
# |    101|       1| 250.0|Alice|     US|
# |    102|       2| 175.0|  Bob|     UK|
# |    999|       3|  50.0| null|   null|
# +-------+--------+------+-----+-------+
# Order 3 is kept but name/country are null

# Full outer join — everything from both sides
full = orders.join(users, on="user_id", how="full")
full.show()
# All 4 unique user_ids appear; nulls where no match
```

---

## Joining on Multiple Columns

When your join key is composite (common in slowly changing dimensions and bridge tables), pass a list:

```python
transactions = spark.createDataFrame([
    ("2024-01-15", "store_001", 1500.0),
    ("2024-01-15", "store_002", 800.0),
    ("2024-01-16", "store_001", 2100.0),
], ["date", "store_id", "revenue"])

targets = spark.createDataFrame([
    ("2024-01-15", "store_001", 1000.0),
    ("2024-01-15", "store_002", 1200.0),
    ("2024-01-16", "store_001", 1800.0),
], ["date", "store_id", "target"])

# Join on both date AND store_id
result = transactions.join(targets, on=["date", "store_id"], how="inner")
result = result.withColumn("vs_target", col("revenue") - col("target"))
result.show()
```

**Alternative with explicit column expressions** — useful when column names differ:

```python
from pyspark.sql.functions import col

result = transactions.join(
    targets,
    on=(col("transactions.date") == col("targets.date")) &
       (col("transactions.store_id") == col("targets.store_id")),
    how="inner"
)
```

---

## Column Ambiguity After Joins

One of the most common junior mistakes: after joining two DataFrames that share column names, Spark creates duplicate columns. Selecting by name becomes ambiguous.

```python
# Both DataFrames have "user_id" — after join, there are TWO user_id columns
orders_df = spark.createDataFrame([(1, 101)], ["order_id", "user_id"])
users_df  = spark.createDataFrame([(101, "Alice")], ["user_id", "name"])

# Using string "user_id" in the join condition creates duplicate columns
bad_join = orders_df.join(users_df, orders_df.user_id == users_df.user_id, "inner")
bad_join.printSchema()
# root
#  |-- order_id: long (nullable = true)
#  |-- user_id: long (nullable = true)   <-- from orders
#  |-- user_id: long (nullable = true)   <-- from users  (DUPLICATE!)
#  |-- name: string (nullable = true)

# FIX 1: Use on="user_id" (string) — Spark deduplicates automatically
good_join = orders_df.join(users_df, on="user_id", how="inner")
good_join.printSchema()  # Only one user_id column

# FIX 2: Drop the duplicate after join
bad_join_fixed = orders_df.join(
    users_df, orders_df.user_id == users_df.user_id, "inner"
).drop(users_df.user_id)  # Drop the users_df copy specifically

# FIX 3: Rename before join
users_renamed = users_df.withColumnRenamed("user_id", "u_user_id")
joined = orders_df.join(users_renamed, orders_df.user_id == col("u_user_id"), "inner")
```

**Best practice for production:** always use `on="col_name"` or `on=["col1", "col2"]` when join keys have the same name. Use explicit column expressions only when names differ.

---

## Null Handling in Joins

SQL-style null behavior: `NULL != NULL`. This means rows with null join keys **never match**, even on inner joins.

```python
from pyspark.sql.functions import col, isnull

df_a = spark.createDataFrame([
    (1, "A"),
    (2, "B"),
    (None, "C"),  # null key
], ["id", "val_a"])

df_b = spark.createDataFrame([
    (1, "X"),
    (None, "Y"),  # null key
    (3, "Z"),
], ["id", "val_b"])

# Inner join — null rows are dropped (null != null)
df_a.join(df_b, on="id", how="inner").show()
# Only id=1 matches. id=None rows from both sides are dropped.

# Left join — null key rows in df_a are kept, right columns are null
df_a.join(df_b, on="id", how="left").show()
# id=None from df_a is kept, val_b is null

# To treat nulls as equal (null-safe join):
from pyspark.sql.functions import col
result = df_a.join(
    df_b,
    df_a["id"].eqNullSafe(df_b["id"]),
    how="inner"
)
result.show()
# Now null == null, so both null rows match each other
```

**DE implication:** when joining on foreign keys that may be null (e.g., `customer_id` in a guest checkout scenario), always decide upfront: do nulls join together or do they stay separate? Use `eqNullSafe()` only when nulls should be treated as a matching value.

---

## Semi and Anti Joins — The Underused Gems

Semi and anti joins are extremely useful for filtering — and they're faster than equivalent `IN` subqueries because they don't produce duplicate rows from the right side.

```python
# Scenario: keep only transactions for VIP users
vip_users = spark.createDataFrame([(101,), (103,)], ["user_id"])
transactions = spark.createDataFrame([
    (1, 101, 500.0),
    (2, 102, 200.0),
    (3, 101, 300.0),
    (4, 103, 100.0),
], ["txn_id", "user_id", "amount"])

# left_semi: keep transactions where user_id IS in vip_users
vip_txns = transactions.join(vip_users, on="user_id", how="left_semi")
vip_txns.show()
# Only columns from transactions, only rows matching VIPs
# No column duplication from vip_users

# left_anti: find transactions for NON-VIP users
non_vip_txns = transactions.join(vip_users, on="user_id", how="left_anti")
non_vip_txns.show()
# Only the row with user_id=102
```

**Performance note:** `left_semi` and `left_anti` only need to check existence — Spark can often use a hash-based lookup without materializing the right side fully.

---

## Cross Joins — Use with Extreme Caution

Cross joins produce M × N rows. With large tables this will OOM or run for hours.

```python
# Legitimate use: generate a date spine × store combination for gap-filling
from pyspark.sql.functions import lit

dates = spark.createDataFrame([("2024-01-01",), ("2024-01-02",), ("2024-01-03",)], ["date"])
stores = spark.createDataFrame([("store_001",), ("store_002",)], ["store_id"])

# Generate all date-store combinations (intentional cross join)
date_store_spine = dates.crossJoin(stores)
date_store_spine.show()
# 3 dates × 2 stores = 6 rows — intentional and bounded

# Guard: Spark will throw if you accidentally trigger a cross join
# spark.conf.set("spark.sql.crossJoin.enabled", "false")  # default in older Spark
# In Spark 3+ it's enabled by default but warns in the plan
```

---

## Key Takeaways for Junior DEs

1. **Default to `left` join** when you want all records from the "primary" table (events, transactions) and optional enrichment from a dimension table.
2. **`inner` join silently drops rows** — always verify row counts before and after.
3. **Use `on="col_name"` syntax** to avoid duplicate column headaches.
4. **Null keys never match** in standard joins — use `eqNullSafe()` only when you explicitly want null == null behavior.
5. **`left_semi` and `left_anti`** are cleaner than `df.filter(col("id").isin(...))` for large filter sets.
6. **`crossJoin` on large data = disaster** — always sanity-check cardinality before running cross joins in production.

## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.master("local[*]").appName("joins").getOrCreate()
orders = spark.createDataFrame([(1,101,50),(2,102,30)], ["id","cust_id","amt"])
customers = spark.createDataFrame([(101,"Alice"),(102,"Bob")], ["id","name"])
# Broadcast small table for efficiency
result = orders.join(broadcast(customers), orders.cust_id == customers.id)
result.select("id","name","amt").show()
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
