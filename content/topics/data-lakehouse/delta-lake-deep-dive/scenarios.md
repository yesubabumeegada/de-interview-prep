---
title: "Delta Lake Deep Dive — Scenarios"
topic: data-lakehouse
subtopic: delta-lake-deep-dive
content_type: scenario_question
tags: [delta-lake, scenarios, interview, optimization, debugging]
---

# Delta Lake Deep Dive — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Delta Lake vs Parquet

**Scenario:** Your manager asks you to explain why the team switched from raw Parquet files to Delta Lake on S3. What do you say?

<details>
<summary>💡 Hint</summary>

Focus on what Delta adds on top of Parquet: ACID transactions, time travel, schema enforcement, and the transaction log. Delta Lake is Parquet files + a `_delta_log/` directory.

</details>

<details>
<summary>✅ Solution</summary>

**Key differences:**

| Feature | Raw Parquet | Delta Lake |
|---|---|---|
| ACID transactions | ❌ | ✅ |
| Concurrent writes | ❌ (data corruption risk) | ✅ |
| Schema enforcement | ❌ (silent schema drift) | ✅ |
| Time travel | ❌ | ✅ (RESTORE, AS OF) |
| Upserts/Deletes | ❌ (rewrite entire partition) | ✅ (MERGE) |
| Streaming + batch | ❌ | ✅ |

**The transaction log (`_delta_log/`):** Every write creates a JSON commit file recording what changed. This enables time travel and crash recovery — no more corrupt half-written files.

**Practical example:** With Parquet, if a Spark job fails mid-write, you get a partially written partition. With Delta, either the commit succeeds fully or it doesn't appear at all.

```python
# Time travel - invaluable for debugging
df = spark.read.format("delta").option("versionAsOf", 5).load("s3://bucket/table")
# or
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(...)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: MERGE Performance Optimization

**Scenario:** Your Delta MERGE statement runs in 45 minutes against a 500M-row table. It merges 50K updated records daily. How do you optimize it?

<details>
<summary>💡 Hint</summary>

Three angles: partition pruning (did you partition the target correctly?), reducing the source dataset scope, and Z-ORDER clustering on the merge key so Delta can skip files efficiently.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1 — Check if partition pruning is working:**
```sql
EXPLAIN
MERGE INTO orders AS target
USING updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- Look for: how many files are being scanned?
```

**Step 2 — Partition the target table on a column that filters well:**
```python
# If merging daily updates, partition by date
(df.write.format("delta")
   .partitionBy("order_date")  # restricts which partitions are scanned
   .saveAsTable("orders"))

# Then filter source to only the relevant partitions
updates = spark.sql("SELECT * FROM updates WHERE order_date >= current_date() - 7")
```

**Step 3 — Z-ORDER on the merge key:**
```sql
OPTIMIZE orders ZORDER BY (order_id);
-- Now Delta uses file-level statistics to skip files that can't contain matching order_ids
-- Reduces MERGE scan from 500M rows to ~5M rows
```

**Step 4 — Reduce shuffle with broadcast hint (if source is small enough):**
```sql
MERGE INTO orders AS target
USING (SELECT /*+ BROADCAST */ * FROM updates) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *;
```

**Step 5 — Enable low-shuffle MERGE (Databricks Runtime 10.4+):**
```python
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
```

**Expected result:** 45 min → 3–8 min with partitioning + Z-ORDER + low-shuffle.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a CDC Pipeline with Delta Lake

**Scenario:** You need to ingest CDC events from a PostgreSQL source (via Debezium → Kafka) into a Delta Lake silver table. The source table has 200M rows, receives ~500K changes/day (inserts, updates, deletes), and must support GDPR right-to-erasure requests within 24 hours. Design the end-to-end pipeline.

<details>
<summary>💡 Hint</summary>

Think about: how you process the Kafka CDC stream (Spark Structured Streaming or Flink), how you apply MERGE into Delta, how you handle deletes (hard delete vs soft delete), and how you satisfy GDPR erasure efficiently without rewriting the entire table.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
PostgreSQL → Debezium → Kafka (topic: cdc.orders) 
         → Spark Structured Streaming 
         → Bronze Delta (raw CDC events, append-only)
         → Silver Delta (current state via MERGE)
         → GDPR erasure job (DELETE + VACUUM)
```

**Bronze layer — append-only CDC log:**
```python
(spark.readStream
  .format("kafka")
  .option("subscribe", "cdc.orders")
  .load()
  .selectExpr("CAST(value AS STRING) as cdc_json", "timestamp")
  .withColumn("cdc", from_json("cdc_json", cdc_schema))
  .select("cdc.*", "timestamp")
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3://bucket/checkpoints/bronze_orders")
  .table("bronze.orders_cdc"))
```

**Silver layer — apply MERGE to maintain current state:**
```python
def upsert_to_silver(micro_batch_df, batch_id):
    # Deduplicate within micro-batch (take latest per pk)
    deduped = (micro_batch_df
        .withColumn("rn", row_number().over(
            Window.partitionBy("order_id").orderBy(desc("ts_ms"))))
        .filter("rn = 1"))
    
    # Apply MERGE
    target = DeltaTable.forName(spark, "silver.orders")
    target.alias("t").merge(
        deduped.alias("s"), "t.order_id = s.order_id"
    ).whenMatchedDelete(
        condition="s.op = 'd'"  # Debezium delete event
    ).whenMatchedUpdateAll(
        condition="s.op IN ('u', 'r')"
    ).whenNotMatchedInsertAll(
        condition="s.op = 'c'"
    ).execute()

(spark.readStream
  .table("bronze.orders_cdc")
  .writeStream
  .foreachBatch(upsert_to_silver)
  .option("checkpointLocation", "s3://bucket/checkpoints/silver_orders")
  .start())
```

**GDPR erasure:**
```sql
-- Hard delete the customer's records
DELETE FROM silver.orders WHERE customer_id = 'CUST_12345';

-- Remove from history (required for GDPR)
VACUUM silver.orders RETAIN 0 HOURS;  -- after disabling retention check

-- Alternative: column masking (pseudonymization) avoids full deletes
UPDATE silver.orders 
SET email = SHA2(email, 256), name = 'REDACTED' 
WHERE customer_id = 'CUST_12345';
```

**Key design decisions:**
1. **Bronze is immutable** — never merge into bronze, only append. Enables replay.
2. **Silver uses MERGE** with Debezium `op` field to distinguish C/U/D
3. **GDPR**: Hard delete + VACUUM for right-to-erasure; document that Delta's 30-day default retention means data is still in old files for 30 days unless VACUUM is run
4. **Partition silver by `order_date`** to scope MERGE file scans and improve GDPR delete performance

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What's the Delta transaction log?" — It's the `_delta_log/` directory of JSON commit files. Every write appends a new JSON file recording which Parquet files were added/removed. This is what enables ACID, time travel, and crash recovery.

> **Tip 2:** "When would you use OPTIMIZE + ZORDER?" — After bulk loads or MERGE operations that create many small files. ZORDER on the most common filter/join column dramatically reduces file scanning.

> **Tip 3:** "How do you handle schema evolution in Delta?" — `mergeSchema` option for additive changes. For breaking changes (column rename, type change), use `overwriteSchema` with a coordinated pipeline stop. Always version schema changes in your table DDL history.
