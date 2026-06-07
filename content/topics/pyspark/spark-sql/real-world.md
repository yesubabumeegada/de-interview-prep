---
title: "PySpark Spark SQL - Real World Patterns"
topic: pyspark
subtopic: spark-sql
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, spark-sql, hive-migration, dynamic-sql, data-quality, etl, production]
---

# PySpark Spark SQL — Real-World Patterns

## Pattern 1: Migrate Hive Queries to Spark SQL

**Problem:** A company runs 200+ Hive queries nightly. The jobs take 8 hours total. Migration to Spark SQL targets 2-hour SLA with minimal query rewrites.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("HiveMigration")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://metastore:9083")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .enableHiveSupport()
    .getOrCreate())

# Common Hive → Spark SQL compatibility issues and fixes

# Issue 1: Hive's implicit type coercion differs from Spark
# Hive: SELECT * FROM t WHERE int_col = '123' (implicit cast)
# Spark: Strict by default — enable compatibility
spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")

# Issue 2: Hive SORT BY vs Spark ORDER BY
# Hive SORT BY only sorts within each reducer
# Migration: Replace SORT BY with ORDER BY for global ordering,
# or DISTRIBUTE BY + SORT BY for partitioned sorting
migrated_query = """
    SELECT /*+ REPARTITION(region) */
        region, customer_id, amount
    FROM orders
    DISTRIBUTE BY region
    SORT BY amount DESC
"""

# Issue 3: Hive UDFs — register JAR-based UDFs
spark.sql("ADD JAR hdfs:///libs/custom-udfs.jar")
spark.sql("CREATE TEMPORARY FUNCTION parse_ua AS 'com.company.ParseUserAgent'")

# Issue 4: INSERT OVERWRITE with dynamic partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("""
    INSERT OVERWRITE TABLE analytics.daily_metrics
    PARTITION (year, month, day)
    SELECT 
        metric_name,
        metric_value,
        YEAR(event_date) AS year,
        MONTH(event_date) AS month,
        DAY(event_date) AS day
    FROM raw_events
    WHERE event_date = '2024-01-15'
""")
```

### Migration Performance Comparison

| Query Type | Hive (MapReduce) | Spark SQL | Improvement |
|-----------|-----------------|-----------|-------------|
| Simple aggregation | 12 min | 45 sec | 16x |
| Multi-join (5 tables) | 45 min | 8 min | 5.6x |
| Window functions | 30 min | 3 min | 10x |
| Full nightly pipeline | 8 hours | 1.5 hours | 5.3x |

---

## Pattern 2: Dynamic SQL Generation for Multi-Table ETL

**Problem:** An ETL system processes 50 source tables with similar transformations — SCD Type 2 merge, data quality checks, and audit logging. Maintain one template, not 50 copies.

```python
from pyspark.sql import SparkSession
from string import Template
from datetime import date

spark = SparkSession.builder.getOrCreate()

# Table configuration (from metadata store or config file)
table_configs = [
    {
        "source_db": "raw",
        "target_db": "curated",
        "table": "customers",
        "primary_key": "customer_id",
        "partition_cols": ["year", "month"],
        "scd_columns": ["name", "email", "address", "tier"],
    },
    {
        "source_db": "raw",
        "target_db": "curated",
        "table": "products",
        "primary_key": "product_id",
        "partition_cols": ["category"],
        "scd_columns": ["name", "price", "description"],
    },
]

# SCD Type 2 merge template
SCD2_TEMPLATE = Template("""
    WITH source AS (
        SELECT *, CURRENT_TIMESTAMP() AS effective_from
        FROM ${source_db}.${table}
        WHERE load_date = '${load_date}'
    ),
    target AS (
        SELECT * FROM ${target_db}.${table}
        WHERE is_current = true
    ),
    changes AS (
        SELECT s.*
        FROM source s
        LEFT JOIN target t ON s.${primary_key} = t.${primary_key}
        WHERE t.${primary_key} IS NULL
           OR ${change_detection}
    )
    SELECT * FROM changes
""")

def build_change_detection(scd_columns):
    """Generate change detection SQL for SCD columns."""
    conditions = [f"s.{col} != t.{col} OR (s.{col} IS NULL != t.{col} IS NULL)"
                  for col in scd_columns]
    return " OR ".join(conditions)

def process_table(config, load_date):
    """Execute SCD2 merge for a single table."""
    change_sql = build_change_detection(config["scd_columns"])
    
    query = SCD2_TEMPLATE.substitute(
        source_db=config["source_db"],
        target_db=config["target_db"],
        table=config["table"],
        primary_key=config["primary_key"],
        load_date=load_date,
        change_detection=change_sql,
    )
    
    changes_df = spark.sql(query)
    change_count = changes_df.count()
    
    if change_count > 0:
        # Close existing records
        close_sql = f"""
            UPDATE {config['target_db']}.{config['table']}
            SET is_current = false, effective_to = CURRENT_TIMESTAMP()
            WHERE {config['primary_key']} IN (
                SELECT {config['primary_key']} FROM changes_view
            ) AND is_current = true
        """
        changes_df.createOrReplaceTempView("changes_view")
        
        # Insert new versions
        changes_df.withColumn("is_current", F.lit(True)) \
                  .write.mode("append") \
                  .insertInto(f"{config['target_db']}.{config['table']}")
    
    return {"table": config["table"], "changes": change_count}

# Process all tables
load_date = date.today().isoformat()
results = [process_table(cfg, load_date) for cfg in table_configs]
print(f"Processed {len(results)} tables: {results}")
```

---

## Pattern 3: SQL-Based Data Quality Framework

**Problem:** Every table needs automated quality checks — nulls, uniqueness, range validation, freshness — with results stored for trending and alerting.

```python
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Quality rules defined as SQL predicates
QUALITY_RULES = {
    "orders": [
        {"name": "pk_unique", "sql": "COUNT(*) = COUNT(DISTINCT order_id)", "severity": "critical"},
        {"name": "amount_positive", "sql": "COUNT(CASE WHEN amount <= 0 THEN 1 END) = 0", "severity": "high"},
        {"name": "null_customer", "sql": "COUNT(CASE WHEN customer_id IS NULL THEN 1 END) = 0", "severity": "high"},
        {"name": "date_reasonable", "sql": "COUNT(CASE WHEN order_date > CURRENT_DATE() THEN 1 END) = 0", "severity": "medium"},
        {"name": "freshness", "sql": "MAX(order_date) >= DATE_SUB(CURRENT_DATE(), 1)", "severity": "critical"},
        {"name": "row_count", "sql": "COUNT(*) > 1000", "severity": "critical"},
    ],
}

def run_quality_checks(table_name, partition_filter=None):
    """Execute all quality rules for a table and store results."""
    rules = QUALITY_RULES.get(table_name, [])
    if not rules:
        return []
    
    where_clause = f"WHERE {partition_filter}" if partition_filter else ""
    
    # Build a single SQL that evaluates ALL rules in one pass
    check_expressions = [
        f"CASE WHEN {rule['sql']} THEN 'pass' ELSE 'fail' END AS {rule['name']}"
        for rule in rules
    ]
    
    combined_sql = f"""
        SELECT
            '{table_name}' AS table_name,
            CURRENT_TIMESTAMP() AS check_time,
            COUNT(*) AS row_count,
            {', '.join(check_expressions)}
        FROM {table_name}
        {where_clause}
    """
    
    result = spark.sql(combined_sql).collect()[0]
    
    # Build results list
    check_results = []
    for rule in rules:
        status = result[rule["name"]]
        check_results.append({
            "table": table_name,
            "check": rule["name"],
            "status": status,
            "severity": rule["severity"],
            "timestamp": datetime.now().isoformat(),
        })
    
    # Store results
    results_df = spark.createDataFrame(check_results)
    results_df.write.mode("append").parquet("hdfs:///data/quality/results/")
    
    # Alert on critical failures
    critical_failures = [r for r in check_results 
                        if r["status"] == "fail" and r["severity"] == "critical"]
    if critical_failures:
        raise DataQualityException(
            f"Critical quality check failed for {table_name}: "
            f"{[f['check'] for f in critical_failures]}"
        )
    
    return check_results

# Run checks as part of pipeline
results = run_quality_checks("orders", partition_filter="order_date = '2024-01-15'")
```

### Quality Dashboard Query

```python
# Trend quality over time
spark.sql("""
    SELECT
        table_name,
        check_name,
        DATE(timestamp) AS check_date,
        status,
        COUNT(*) AS occurrences
    FROM quality_results
    WHERE timestamp >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY table_name, check_name, DATE(timestamp), status
    ORDER BY check_date DESC
""").show()
```

---

## Interview Tips

> **Tip 1:** "How would you migrate Hive queries to Spark SQL?" — "Start with compatibility configs: storeAssignmentPolicy for type coercion, partitionOverwriteMode for dynamic partitions. Most HiveQL is Spark-compatible. Key differences: SORT BY semantics, lateral views, and some UDFs. I'd run both in parallel, compare outputs with a validation framework, and migrate table-by-table with rollback capability. Performance gains are typically 5-15x due to in-memory execution and Catalyst optimization."

> **Tip 2:** "How do you handle 50+ similar ETL tables without code duplication?" — "SQL templating with metadata-driven configuration. Define table configs (keys, partition columns, SCD columns) in a metadata store. Use Python string templating to generate SQL dynamically. One code path handles all tables — changes to logic propagate everywhere. Test with a few tables in dev, then scale. Add per-table overrides for edge cases rather than forking the template."

> **Tip 3:** "How would you implement data quality checks in Spark?" — "Define rules as SQL predicates evaluated in a single pass over the data — this avoids multiple scans. Run all checks for a table in one query using CASE expressions. Store results in a quality table for trending and alerting. Gate downstream pipelines on critical checks. The key design decision is single-pass evaluation: one query computes all metrics rather than running separate queries per rule."
