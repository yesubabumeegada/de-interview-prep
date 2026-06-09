---
title: "Delta Lake Integration — Intermediate"
topic: pyspark
subtopic: delta-lake-integration
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [delta-lake, MERGE, upsert, time-travel, schema-evolution, CDC]
---

# Delta Lake Integration — Intermediate

This level covers the DML operations that make Delta Lake genuinely useful for production pipelines: MERGE for upserts, DELETE/UPDATE for corrections, time travel for audits and recovery, and schema evolution without pipeline downtime.

---

## MERGE: The Core Upsert Operation

`MERGE` is the most important Delta operation for production pipelines. It lets you upsert records: insert if new, update if exists, optionally delete if matched.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName("delta-intermediate") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Target: existing Delta table (the "sink")
target = DeltaTable.forPath(spark, "s3://lakehouse/tables/customers/")

# Source: new/updated records arriving from CDC or API (the "source")
updates = spark.createDataFrame([
    (101, "Alice Smith",    "alice@new.com",  "2024-01-20"),  # existing, email changed
    (102, "Bob Jones",      "bob@email.com",  "2024-01-18"),  # existing, no change in key
    (999, "New Customer",   "new@email.com",  "2024-01-20"),  # new record
], ["customer_id", "name", "email", "updated_at"])

# Perform MERGE
target.alias("target").merge(
    updates.alias("source"),
    condition="target.customer_id = source.customer_id"
).whenMatchedUpdate(set={
    "name":       "source.name",
    "email":      "source.email",
    "updated_at": "source.updated_at",
}).whenNotMatchedInsert(values={
    "customer_id": "source.customer_id",
    "name":        "source.name",
    "email":       "source.email",
    "updated_at":  "source.updated_at",
}).execute()

# Verify the result
spark.table("warehouse.customers").show()
```

### Conditional MERGE (Only Update When Data Has Changed)

In high-volume pipelines, writing unchanged records wastes I/O and creates unnecessary Delta log entries.

```python
target.alias("t").merge(
    updates.alias("s"),
    condition="t.customer_id = s.customer_id"
).whenMatchedUpdate(
    # Only update if email actually changed
    condition="t.email != s.email OR t.name != s.name",
    set={
        "name":       "s.name",
        "email":      "s.email",
        "updated_at": "s.updated_at",
    }
).whenNotMatchedInsert(values={
    "customer_id": "s.customer_id",
    "name":        "s.name",
    "email":       "s.email",
    "updated_at":  "s.updated_at",
}).execute()
```

### MERGE with Delete (Full CDC Pattern)

```python
# Source includes a "op" column: 'I' = insert, 'U' = update, 'D' = delete
cdc_events = spark.createDataFrame([
    (101, "Alice", "alice@email.com", "U"),
    (200, "Zara",  "zara@email.com",  "I"),
    (55,  None,    None,              "D"),
], ["customer_id", "name", "email", "op"])

target.alias("t").merge(
    cdc_events.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedDelete(
    condition="s.op = 'D'"
).whenMatchedUpdate(
    condition="s.op = 'U'",
    set={"name": "s.name", "email": "s.email"}
).whenNotMatchedInsert(
    condition="s.op = 'I'",
    values={"customer_id": "s.customer_id", "name": "s.name", "email": "s.email"}
).execute()
```

---

## UPDATE and DELETE

For targeted corrections and data governance.

```python
from delta.tables import DeltaTable

table = DeltaTable.forPath(spark, "s3://lakehouse/tables/orders/")

# UPDATE: correct a specific record
table.update(
    condition=col("order_id") == 1234,
    set={"amount": lit(299.99), "updated_at": current_timestamp()}
)

# UPDATE with complex condition
table.update(
    condition="status = 'pending' AND order_date < '2024-01-01'",
    set={"status": lit("expired")}
)

# DELETE: remove specific records
table.delete(condition="order_id = 9999")

# DELETE with complex condition — GDPR right-to-erasure
table.delete(condition="customer_id IN (101, 202, 303) AND is_deleted = true")

# In SQL:
spark.sql("""
    UPDATE warehouse.orders
    SET status = 'cancelled'
    WHERE order_date < '2020-01-01' AND status = 'pending'
""")

spark.sql("""
    DELETE FROM warehouse.orders
    WHERE customer_id = 101
""")
```

**Performance note:** Delta `UPDATE` and `DELETE` work by **rewriting the affected files**. If you update 1 row in a 10 GB partition, that entire partition file is rewritten. For fine-grained updates, use smaller partition files (tune by `OPTIMIZE` target file size) or design your schema to minimize the blast radius of updates.

---

## Time Travel: Production Use Cases

```python
# View full history
table = DeltaTable.forPath(spark, "s3://lakehouse/tables/orders/")
table.history().show(truncate=False)
# +-------+--------------------+----------+------------------------------------------+
# |version|timestamp           |operation |operationParameters                       |
# +-------+--------------------+----------+------------------------------------------+
# |      5|2024-01-20 08:15:00 |MERGE     |{predicate: ..., numTargetRowsUpdated: 150}|
# |      4|2024-01-19 08:10:00 |WRITE     |{mode: Append, ...}                        |

# Use Case 1: Audit — what changed in the last merge?
before_merge = spark.read.format("delta") \
    .option("versionAsOf", 4) \
    .load("s3://lakehouse/tables/orders/")
after_merge = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("s3://lakehouse/tables/orders/")

# Find changed rows
changed = after_merge.join(before_merge, on="order_id", how="left_anti")
changed.show()

# Use Case 2: Recovery — bad data was written, roll back
spark.sql("""
    INSERT OVERWRITE warehouse.orders
    SELECT * FROM warehouse.orders VERSION AS OF 4
""")
# This atomically replaces the table with the pre-bad-write version

# Use Case 3: Incremental processing — what changed since my last run?
# (Better done with Change Data Feed — see senior-deep-dive)
last_processed_version = 3
new_records = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_processed_version + 1) \
    .load("s3://lakehouse/tables/orders/")
```

---

## Schema Evolution in Depth

```python
# Scenario: upstream API adds a new field "loyalty_points"
# You need to accept it without breaking the pipeline

# Option 1: mergeSchema — add new columns, keep existing schema for old rows
new_data_with_extra_column.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("s3://lakehouse/tables/customers/")
# New rows have loyalty_points; old rows have loyalty_points = null

# Enable globally for the session:
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Option 2: overwriteSchema — replace the schema entirely
# USE WITH CAUTION: this rewrites all table metadata and
# may break downstream consumers expecting the old schema
replacement_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3://lakehouse/tables/customers/")

# Option 3: Explicit ALTER TABLE (preferred for controlled schema changes)
spark.sql("ALTER TABLE warehouse.customers ADD COLUMN loyalty_points INT")
spark.sql("ALTER TABLE warehouse.customers RENAME COLUMN old_name TO new_name")
spark.sql("ALTER TABLE warehouse.customers ALTER COLUMN amount TYPE DOUBLE")

# Option 4: Column mapping — rename/drop without rewriting data files
# Requires Delta feature "columnMapping"
spark.sql("""
    ALTER TABLE warehouse.customers
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")
spark.sql("ALTER TABLE warehouse.customers RENAME COLUMN name TO full_name")
# The underlying Parquet files are NOT rewritten — only the schema metadata changes
```

### Schema Evolution Safety Pattern

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def safe_schema_evolving_write(
    df: DataFrame,
    target_path: str,
    mode: str = "append",
    allowed_new_columns: list = None
) -> None:
    """
    Write with mergeSchema only if new columns are pre-approved.
    Prevents accidental schema pollution.
    """
    from delta.tables import DeltaTable

    try:
        existing = DeltaTable.forPath(spark, target_path)
        existing_cols = set(existing.toDF().columns)
        incoming_cols = set(df.columns)
        new_cols = incoming_cols - existing_cols

        if new_cols and allowed_new_columns is None:
            raise ValueError(
                f"New columns detected: {new_cols}. "
                f"Pass allowed_new_columns to permit schema evolution."
            )
        if new_cols and not new_cols.issubset(set(allowed_new_columns)):
            raise ValueError(
                f"Unapproved new columns: {new_cols - set(allowed_new_columns)}"
            )
        use_merge = bool(new_cols)
    except Exception:
        use_merge = False  # Table doesn't exist yet

    writer = df.write.format("delta").mode(mode)
    if use_merge:
        writer = writer.option("mergeSchema", "true")
    writer.save(target_path)
```

---

## MERGE Performance Optimization

MERGE is expensive because it must read the entire target table to find matches. Several optimizations:

```python
# 1. Partition pruning in the MERGE condition
# Add a partition filter to the merge condition so Delta only reads relevant files
target.alias("t").merge(
    source.alias("s"),
    # Include partition column in condition — Delta prunes to matching partitions
    "t.customer_id = s.customer_id AND t.region = s.region"
).whenMatchedUpdate(...).whenNotMatchedInsert(...).execute()

# 2. Low shuffle merge (Spark 3.2+ / Databricks)
# For small source DataFrames, this is faster than a full SMJ
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

# 3. Repartition source to match target partitioning before merge
source_repartitioned = source.repartition(col("region"))
target.alias("t").merge(
    source_repartitioned.alias("s"),
    "t.customer_id = s.customer_id AND t.region = s.region"
).whenMatchedUpdate(...).execute()

# 4. Z-ordering the target table on the merge key
# (see senior-deep-dive for Z-ordering details)
spark.sql("OPTIMIZE warehouse.customers ZORDER BY (customer_id)")
# Delta will read fewer files during the match phase of MERGE
```

---

## Key Takeaways

1. **MERGE is the DE Swiss Army knife** — handles insert-only, upsert, and full CDC (insert + update + delete) in one operation.
2. **Conditional `whenMatchedUpdate`** avoids rewriting files for records that haven't changed — critical for high-volume tables.
3. **Time travel** is your audit log and rollback mechanism — protect it by not running `VACUUM` too aggressively.
4. **`mergeSchema`** for safe column additions; `ALTER TABLE ADD COLUMN` for explicit, controlled schema changes.
5. **MERGE performance** depends on how much of the target table needs to be scanned — partition pruning in the merge condition is the most impactful optimization.
