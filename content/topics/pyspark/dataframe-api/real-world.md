---
title: "PySpark DataFrame API - Real-World Production Examples"
topic: pyspark
subtopic: dataframe-api
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, dataframe, production, etl, pipeline, delta-lake, monitoring]
---

# PySpark DataFrame API — Real-World Production Examples

## Pattern 1: Production ETL Pipeline with Validation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime

class ETLPipeline:
    """Production ETL pipeline with validation, metrics, and error handling."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
    
    def extract(self, path: str, expected_schema: StructType) -> "DataFrame":
        """Read with schema enforcement. Fail fast on schema mismatch."""
        df = self.spark.read.schema(expected_schema).parquet(path)
        self.metrics["input_count"] = df.count()
        self.metrics["input_partitions"] = df.rdd.getNumPartitions()
        return df
    
    def validate(self, df, rules: dict) -> tuple:
        """
        Separate valid and invalid records.
        Rules: {"column": "not_null" | "positive" | callable}
        """
        valid_condition = lit(True)
        
        for column, rule in rules.items():
            if rule == "not_null":
                valid_condition = valid_condition & col(column).isNotNull()
            elif rule == "positive":
                valid_condition = valid_condition & (col(column) > 0)
            elif callable(rule):
                valid_condition = valid_condition & rule(col(column))
        
        valid_df = df.filter(valid_condition)
        invalid_df = df.filter(~valid_condition)
        
        self.metrics["valid_count"] = valid_df.count()
        self.metrics["invalid_count"] = invalid_df.count()
        self.metrics["rejection_rate"] = (
            self.metrics["invalid_count"] / max(self.metrics["input_count"], 1) * 100
        )
        
        return valid_df, invalid_df
    
    def transform(self, df) -> "DataFrame":
        """Apply business transformations."""
        return df.withColumn("processed_at", current_timestamp()) \
                 .withColumn("pipeline_version", lit(self.config["version"]))
    
    def load(self, df, target_path: str, partition_cols: list) -> None:
        """Write with partitioning and metrics."""
        df.write \
            .mode("append") \
            .partitionBy(*partition_cols) \
            .option("maxRecordsPerFile", 1000000) \
            .parquet(target_path)
        
        self.metrics["output_count"] = df.count()
    
    def quarantine(self, invalid_df, quarantine_path: str) -> None:
        """Write rejected records for investigation."""
        if self.metrics["invalid_count"] > 0:
            invalid_df.withColumn("quarantine_reason", lit("validation_failed")) \
                      .withColumn("quarantined_at", current_timestamp()) \
                      .write.mode("append").parquet(quarantine_path)
    
    def report_metrics(self) -> dict:
        """Return pipeline execution metrics."""
        self.metrics["success"] = self.metrics.get("rejection_rate", 0) < 5.0
        return self.metrics

# Usage
pipeline = ETLPipeline(spark, config={"version": "2.1.0"})
raw_df = pipeline.extract("s3://raw/events/2024-01-15/", EVENT_SCHEMA)
valid_df, invalid_df = pipeline.validate(raw_df, {
    "user_id": "not_null",
    "amount": "positive",
    "event_type": lambda c: c.isin("click", "purchase", "view"),
})
transformed = pipeline.transform(valid_df)
pipeline.load(transformed, "s3://curated/events/", ["event_date", "event_type"])
pipeline.quarantine(invalid_df, "s3://quarantine/events/")
print(pipeline.report_metrics())
```

## Pattern 2: SCD Type 2 Merge with Delta Lake

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, coalesce

def merge_scd_type2(
    spark: SparkSession,
    incoming_df: "DataFrame",
    target_path: str,
    business_keys: list[str],
    tracked_columns: list[str],
):
    """
    Implement SCD Type 2 logic:
    - New records → INSERT
    - Changed records → Close old row (set effective_to) + INSERT new row
    - Unchanged records → No action
    """
    target = DeltaTable.forPath(spark, target_path)
    
    # Build join condition on business keys + active flag
    join_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in business_keys]
    ) + " AND target.is_current = true"
    
    # Build change detection condition
    change_condition = " OR ".join(
        [f"target.{c} != source.{c}" for c in tracked_columns]
    )
    
    # Stage the incoming data with SCD metadata
    staged = incoming_df.withColumn("effective_from", current_timestamp()) \
                        .withColumn("effective_to", lit(None).cast("timestamp")) \
                        .withColumn("is_current", lit(True))
    
    # Merge operation
    target.alias("target").merge(
        staged.alias("source"),
        join_condition
    ).whenMatchedUpdate(
        # Close the existing current row (it has changes)
        condition=change_condition,
        set={
            "is_current": lit(False),
            "effective_to": current_timestamp(),
        }
    ).whenNotMatchedInsertAll(
    ).execute()
    
    # Insert new versions of changed records
    # (The MERGE above closed old rows; now insert fresh versions)
    changed_records = staged.join(
        target.toDF().filter(
            (col("is_current") == False) & 
            (col("effective_to").isNotNull())
        ),
        business_keys,
        "inner"
    )
    
    if changed_records.count() > 0:
        changed_records.select(staged.columns) \
            .write.format("delta").mode("append").save(target_path)

# Usage
merge_scd_type2(
    spark,
    incoming_df=new_customers_df,
    target_path="s3://warehouse/dim_customer/",
    business_keys=["customer_id"],
    tracked_columns=["name", "email", "segment", "credit_limit"],
)
```

## Pattern 3: Dynamic Schema-Aware Ingestion

```python
from pyspark.sql.functions import col, lit, input_file_name
from pyspark.sql.types import StructType

class SchemaAwareIngestion:
    """
    Handle schema evolution across daily file drops.
    Different days may have different column sets.
    """
    
    def __init__(self, spark: SparkSession, target_schema: StructType):
        self.spark = spark
        self.target_schema = target_schema
        self.target_columns = set(f.name for f in target_schema.fields)
    
    def ingest(self, source_path: str) -> "DataFrame":
        """
        Read with permissive mode. Align to target schema.
        Extra columns → dropped. Missing columns → NULL.
        """
        # Read with inferred schema first (to see what arrived)
        raw = self.spark.read \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .parquet(source_path)
        
        # Track source file for lineage
        raw = raw.withColumn("_source_file", input_file_name())
        
        # Separate corrupt records
        if "_corrupt_record" in raw.columns:
            corrupt = raw.filter(col("_corrupt_record").isNotNull())
            raw = raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        
        # Align to target schema
        aligned = self._align_schema(raw)
        
        return aligned
    
    def _align_schema(self, df) -> "DataFrame":
        """Add missing columns as NULL, drop extra columns."""
        source_columns = set(df.columns)
        
        # Add missing columns
        for field in self.target_schema.fields:
            if field.name not in source_columns:
                df = df.withColumn(field.name, lit(None).cast(field.dataType))
        
        # Select only target columns in correct order + metadata
        target_col_names = [f.name for f in self.target_schema.fields]
        metadata_cols = ["_source_file"]
        
        return df.select(target_col_names + metadata_cols)

# Usage
ingestion = SchemaAwareIngestion(spark, TARGET_EVENT_SCHEMA)
events = ingestion.ingest("s3://landing/events/2024-01-15/")
events.write.mode("append").partitionBy("event_date").parquet("s3://raw/events/")
```

## Pattern 4: Incremental Processing with High-Water Mark

```python
from pyspark.sql.functions import max as spark_max, col

class IncrementalProcessor:
    """
    Process only new data since last run using a watermark column.
    Stores high-water mark in a control table.
    """
    
    def __init__(self, spark: SparkSession, control_table_path: str):
        self.spark = spark
        self.control_table_path = control_table_path
    
    def get_high_water_mark(self, pipeline_name: str) -> str:
        """Read the last processed watermark value."""
        try:
            control = self.spark.read.parquet(self.control_table_path)
            hwm = control.filter(col("pipeline") == pipeline_name) \
                         .select("high_water_mark") \
                         .first()
            return hwm["high_water_mark"] if hwm else "1970-01-01T00:00:00"
        except Exception:
            return "1970-01-01T00:00:00"
    
    def update_high_water_mark(self, pipeline_name: str, new_hwm: str) -> None:
        """Update the control table with new high-water mark."""
        from pyspark.sql import Row
        update_df = self.spark.createDataFrame(
            [Row(pipeline=pipeline_name, high_water_mark=new_hwm, updated_at=str(datetime.now()))]
        )
        update_df.write.mode("overwrite") \
            .option("replaceWhere", f"pipeline = '{pipeline_name}'") \
            .parquet(self.control_table_path)
    
    def process_incremental(
        self, 
        source_path: str,
        pipeline_name: str,
        watermark_col: str,
        transform_fn,
        target_path: str,
    ) -> dict:
        """Run incremental processing pipeline."""
        # Get last processed point
        hwm = self.get_high_water_mark(pipeline_name)
        
        # Read only new data
        new_data = self.spark.read.parquet(source_path) \
            .filter(col(watermark_col) > hwm)
        
        record_count = new_data.count()
        if record_count == 0:
            return {"status": "no_new_data", "records": 0}
        
        # Transform
        result = transform_fn(new_data)
        
        # Write
        result.write.mode("append").parquet(target_path)
        
        # Update watermark
        new_hwm = new_data.agg(spark_max(watermark_col)).first()[0]
        self.update_high_water_mark(pipeline_name, str(new_hwm))
        
        return {"status": "success", "records": record_count, "new_hwm": str(new_hwm)}

# Usage
processor = IncrementalProcessor(spark, "s3://control/watermarks/")
result = processor.process_incremental(
    source_path="s3://raw/clickstream/",
    pipeline_name="clickstream_to_curated",
    watermark_col="event_timestamp",
    transform_fn=lambda df: df.filter(col("event_type") != "heartbeat"),
    target_path="s3://curated/clickstream/",
)
```

## Pattern 5: Data Quality Monitoring

```python
from pyspark.sql.functions import count, sum as spark_sum, when, col, lit
from dataclasses import dataclass

@dataclass
class QualityCheck:
    name: str
    column: str
    check_type: str  # "not_null", "unique", "range", "regex"
    params: dict = None

class DataQualityMonitor:
    """Run data quality checks and produce a report DataFrame."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def run_checks(self, df, checks: list[QualityCheck]) -> "DataFrame":
        total_rows = df.count()
        results = []
        
        for check in checks:
            if check.check_type == "not_null":
                failures = df.filter(col(check.column).isNull()).count()
            elif check.check_type == "unique":
                duplicates = df.groupBy(check.column).count() \
                    .filter(col("count") > 1).count()
                failures = duplicates
            elif check.check_type == "range":
                min_val = check.params.get("min", float("-inf"))
                max_val = check.params.get("max", float("inf"))
                failures = df.filter(
                    (col(check.column) < min_val) | (col(check.column) > max_val)
                ).count()
            elif check.check_type == "regex":
                pattern = check.params["pattern"]
                failures = df.filter(~col(check.column).rlike(pattern)).count()
            else:
                failures = -1
            
            pass_rate = ((total_rows - failures) / max(total_rows, 1)) * 100
            results.append({
                "check_name": check.name,
                "column": check.column,
                "check_type": check.check_type,
                "total_rows": total_rows,
                "failures": failures,
                "pass_rate": round(pass_rate, 2),
                "status": "PASS" if pass_rate >= 99.0 else "WARN" if pass_rate >= 95.0 else "FAIL",
            })
        
        return self.spark.createDataFrame(results)

# Usage
monitor = DataQualityMonitor(spark)
report = monitor.run_checks(events_df, [
    QualityCheck("user_id_not_null", "user_id", "not_null"),
    QualityCheck("user_id_unique", "user_id", "unique"),
    QualityCheck("amount_positive", "amount", "range", {"min": 0, "max": 1000000}),
    QualityCheck("email_format", "email", "regex", {"pattern": r"^.+@.+\..+$"}),
])
report.show(truncate=False)
```

## Interview Tip 💡

> Production PySpark questions test whether you think about failure modes: "What if the source file is empty?" "What if the schema changed?" "What if one partition is 100x larger than others?" Always mention: (1) Schema enforcement at read time, (2) Data validation with quarantine for bad records, (3) Idempotent writes (overwrite partition, not append duplicates), (4) Monitoring/alerting on row counts and quality metrics. This separates senior engineers from those who only handle the happy path.
