---
title: "Delta Lake Integration — Real-World Patterns"
topic: pyspark
subtopic: delta-lake-integration
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [delta-lake, CDC, medallion-architecture, SCD2, bronze-silver-gold, lakehouse]
---

# Delta Lake Integration — Real-World Patterns

These are the patterns you'll build and maintain in production lakehouse pipelines.

---

## Pattern 1: CDC Pipeline with MERGE

The most common Delta Lake use case: ingesting change events from a database CDC stream (Debezium, AWS DMS, Fivetran) and keeping a Delta table current.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("cdc-pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def process_cdc_batch(cdc_batch_path: str, target_table: str):
    """
    Process a batch of CDC events (Debezium format) into a Delta target table.
    Handles deduplication and all CDC operation types.
    """
    # Read CDC events from staging (Kafka→S3 landing zone or direct Kafka)
    cdc_events = spark.read.parquet(cdc_batch_path)
    # Schema: op (c/u/d/r), after{...}, before{...}, source{ts_ms, ...}

    # Normalize to flat schema
    normalized = cdc_events.select(
        col("after.customer_id").alias("customer_id"),
        col("after.name").alias("name"),
        col("after.email").alias("email"),
        col("after.updated_at").alias("updated_at"),
        col("op"),
        col("source.ts_ms").alias("cdc_timestamp_ms"),
    )

    # Deduplicate within batch: keep only the latest event per customer_id
    # (CDC streams can contain multiple events for the same key in one batch)
    window = Window.partitionBy("customer_id").orderBy(col("cdc_timestamp_ms").desc())
    deduped = normalized \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Apply to Delta target via MERGE
    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        deduped.alias("s"),
        "t.customer_id = s.customer_id"
    ).whenMatchedDelete(
        condition="s.op = 'd'"
    ).whenMatchedUpdate(
        condition="s.op IN ('u', 'r')",
        set={
            "name":       "s.name",
            "email":      "s.email",
            "updated_at": "s.updated_at",
        }
    ).whenNotMatchedInsert(
        condition="s.op IN ('c', 'r')",
        values={
            "customer_id": "s.customer_id",
            "name":        "s.name",
            "email":       "s.email",
            "updated_at":  "s.updated_at",
        }
    ).execute()

    # Log metrics
    history = target.history(1)
    history.select(
        "version", "timestamp", "operation",
        col("operationMetrics.numTargetRowsUpdated").alias("updated"),
        col("operationMetrics.numTargetRowsInserted").alias("inserted"),
        col("operationMetrics.numTargetRowsDeleted").alias("deleted"),
    ).show()
```

---

## Pattern 2: Incremental Processing with Change Data Feed

Instead of re-reading and re-processing the full Bronze table every hour, use CDF to process only what changed.

```python
import json
from pathlib import Path
from delta.tables import DeltaTable

CHECKPOINT_FILE = "/dbfs/pipelines/bronze_to_silver/last_version.json"

def load_last_processed_version() -> int:
    """Load the last processed Bronze version from a checkpoint."""
    try:
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)["last_version"]
    except FileNotFoundError:
        return 0  # Start from the beginning

def save_last_processed_version(version: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_version": version}, f)

def run_bronze_to_silver_incremental():
    """Read only changed records from Bronze and upsert to Silver."""
    bronze_table = "warehouse.bronze_customers"
    silver_table = "warehouse.silver_customers"

    # Get current Bronze version
    current_version = DeltaTable.forName(spark, bronze_table).history(1) \
        .select("version").collect()[0][0]

    last_version = load_last_processed_version()

    if last_version >= current_version:
        print(f"Silver is up to date at version {last_version}. Nothing to process.")
        return

    print(f"Processing Bronze versions {last_version + 1} → {current_version}")

    # Read only the changes
    changes = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_version + 1) \
        .option("endingVersion", current_version) \
        .table(bronze_table) \
        .filter(col("_change_type").isin(["insert", "update_postimage"]))

    # Apply Silver transformations (clean, validate, enrich)
    silver_ready = changes \
        .drop("_change_type", "_commit_version", "_commit_timestamp") \
        .withColumn("name", trim(col("name"))) \
        .withColumn("email", lower(col("email"))) \
        .filter(col("email").contains("@"))  # Basic validation

    # Upsert to Silver
    silver_target = DeltaTable.forName(spark, silver_table)
    silver_target.alias("t").merge(
        silver_ready.alias("s"),
        "t.customer_id = s.customer_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    save_last_processed_version(current_version)
    print(f"Silver updated to Bronze version {current_version}. "
          f"Processed {silver_ready.count()} changed records.")
```

---

## Pattern 3: Medallion Architecture — Bronze / Silver / Gold

The medallion pattern is the standard lakehouse architecture. Each layer has different Delta table properties and write semantics.

```python
class MedallionPipeline:
    """
    Three-layer lakehouse pipeline:
    Bronze: raw ingestion, append-only, schema-on-read
    Silver: cleaned, deduplicated, schema-enforced
    Gold: aggregated, business-metric-ready
    """

    def __init__(self, spark, base_path: str):
        self.spark = spark
        self.base_path = base_path

    def create_bronze_table(self, table_name: str, source_schema):
        """Bronze: minimal transformation, preserve raw data forever."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.{table_name}
            USING DELTA
            LOCATION '{self.base_path}/bronze/{table_name}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 30 days',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

    def ingest_to_bronze(self, df, table_name: str):
        """Append raw data with ingestion metadata."""
        from pyspark.sql.functions import current_timestamp, lit, input_file_name
        import uuid

        enriched = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_batch_id", lit(str(uuid.uuid4())))

        enriched.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{self.base_path}/bronze/{table_name}")

    def promote_to_silver(self, table_name: str, pk_column: str,
                          transformations=None):
        """Silver: deduplicate, clean, validate. Upsert semantics."""
        bronze = self.spark.table(f"bronze.{table_name}")

        # Deduplicate by primary key — keep latest by ingestion time
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        window = Window.partitionBy(pk_column).orderBy(col("_ingested_at").desc())
        deduped = bronze \
            .withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn", "_ingested_at", "_batch_id")

        # Apply optional custom transformations
        if transformations:
            deduped = transformations(deduped)

        # Upsert to Silver
        silver = DeltaTable.forPath(self.spark, f"{self.base_path}/silver/{table_name}")
        silver.alias("t").merge(
            deduped.alias("s"),
            f"t.{pk_column} = s.{pk_column}"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    def build_gold(self, gold_table: str, query: str):
        """Gold: aggregated business metrics. Overwrite on each run."""
        result = self.spark.sql(query)
        result.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{self.base_path}/gold/{gold_table}")

# Usage:
pipeline = MedallionPipeline(spark, "s3://lakehouse")

# Daily pipeline:
# 1. Ingest raw orders from API
pipeline.ingest_to_bronze(raw_orders_df, "orders")

# 2. Promote to Silver (clean, dedup)
pipeline.promote_to_silver(
    "orders",
    pk_column="order_id",
    transformations=lambda df: df
        .filter(col("amount") > 0)  # Remove zero-amount orders
        .withColumn("order_date", col("order_date").cast("date"))
)

# 3. Build Gold aggregate
pipeline.build_gold(
    "daily_revenue",
    """
    SELECT
        order_date,
        product_category,
        SUM(amount)    AS total_revenue,
        COUNT(*)       AS order_count,
        AVG(amount)    AS avg_order_value
    FROM silver.orders o
    JOIN silver.products p ON o.product_id = p.product_id
    GROUP BY order_date, product_category
    """
)
```

---

## Pattern 4: SCD Type 2 with Delta MERGE

Maintain a full history of dimension changes using Delta MERGE and explicit `effective_date` / `expiry_date` columns.

```python
from pyspark.sql.functions import col, current_date, lit, to_date
from delta.tables import DeltaTable

def apply_scd2_merge(updates_df, target_table: str, pk_col: str, tracked_cols: list):
    """
    Apply SCD Type 2 logic via Delta MERGE.
    - Closes the current record when a tracked column changes.
    - Inserts a new record with is_current = True and new effective_date.
    - Leaves unchanged records untouched.
    """
    target = DeltaTable.forName(spark, target_table)
    today = current_date()

    # Identify updated records (where tracked columns changed)
    # Add metadata columns to the source
    staged_updates = updates_df \
        .withColumn("effective_date", today) \
        .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
        .withColumn("is_current", lit(True))

    # Build the change detection condition
    changed_condition = " OR ".join([
        f"target.{c} != source.{c}" for c in tracked_cols
    ])

    # Step 1: Expire records that have changed
    target.alias("target").merge(
        staged_updates.alias("source"),
        f"target.{pk_col} = source.{pk_col} AND target.is_current = true"
    ).whenMatchedUpdate(
        condition=changed_condition,
        set={
            "expiry_date": "source.effective_date",
            "is_current":  "false",
        }
    ).execute()

    # Step 2: Insert new records for changed + brand new keys
    # Find which records to insert (changed + new)
    existing_current = spark.table(target_table).filter(col("is_current"))
    to_insert = staged_updates.join(
        existing_current.select(pk_col).withColumnRenamed(pk_col, "existing_pk"),
        staged_updates[pk_col] == col("existing_pk"),
        how="full_outer"
    ).filter(
        # New records (no match in target)
        col("existing_pk").isNull() |
        # OR changed records (match exists but tracked cols differ)
        " OR ".join([f"existing_target.{c} != staged_updates.{c}" for c in tracked_cols])
    )
    # Simpler: just upsert new current records (expired ones won't conflict)
    to_insert = staged_updates  # Insert all from source; expired rows won't match on is_current=true

    target.alias("target").merge(
        to_insert.alias("source"),
        f"target.{pk_col} = source.{pk_col} AND target.effective_date = source.effective_date"
    ).whenNotMatchedInsertAll() \
     .execute()

# Usage:
apply_scd2_merge(
    updates_df=new_customer_data,
    target_table="warehouse.dim_customer_scd2",
    pk_col="customer_id",
    tracked_cols=["email", "address", "tier"]
)
```

---

## Key Takeaways

1. **CDC + MERGE** is the standard pattern for database replication to Delta — always deduplicate within the batch before merging.
2. **CDF-based incremental processing** eliminates the "re-read full Bronze" anti-pattern — dramatically reduces compute cost for Silver promotion.
3. **Medallion architecture:** Bronze = raw, append-only; Silver = clean, upsert; Gold = aggregated, overwrite.
4. **SCD Type 2 with Delta** requires a two-step MERGE: first expire changed records, then insert new versions.
5. **`operationMetrics`** in table history tells you exactly how many rows were inserted/updated/deleted per MERGE — use it for pipeline monitoring.
