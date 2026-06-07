---
title: "Sqoop - Real World"
topic: hadoop
subtopic: sqoop
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, sqoop, oracle, hive, incremental, airflow, etl]
---

# Sqoop — Real-World Patterns

## Daily RDBMS-to-Hive Pipeline

A common production pattern: Oracle OLTP → Sqoop → HDFS → Hive external table → analytics.

```bash
#!/bin/bash
# daily_sqoop_pipeline.sh
DATE=$(date +%Y-%m-%d)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

# Step 1: Full import on first run, incremental thereafter
if hdfs dfs -test -d /data/raw/orders/dt=${DATE}; then
  echo "Already imported for ${DATE}, skipping"
  exit 0
fi

# Step 2: Incremental import
sqoop import \
  --connect "jdbc:oracle:thin:@//prod-oracle:1521/ORCL" \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table ORDERS \
  --where "CREATED_DATE >= DATE '${YESTERDAY}' AND CREATED_DATE < DATE '${DATE}'" \
  --target-dir /data/raw/orders/dt=${DATE} \
  --num-mappers 8 \
  --split-by ORDER_ID \
  --as-parquetfile \
  --compression-codec snappy \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --map-column-java ORDER_AMOUNT=Double \
  --fetch-size 5000

# Step 3: Add partition to Hive
hive -e "ALTER TABLE raw.orders ADD IF NOT EXISTS PARTITION (dt='${DATE}') LOCATION '/data/raw/orders/dt=${DATE}';"

echo "Import complete for ${DATE}"
```

## Handling Oracle CLOB/BLOB Columns

Oracle LOB columns require special handling — Sqoop's default JDBC can't stream them efficiently:

```bash
# Problem: CLOB columns fail with standard import
# ERROR: ORA-01460: unimplemented or unreasonable conversion requested

# Solution 1: Exclude LOB columns
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table DOCUMENTS \
  --columns "DOC_ID,DOC_NAME,DOC_TYPE,CREATED_DATE,AUTHOR_ID" \
  --target-dir /data/raw/documents \
  --num-mappers 4

# Solution 2: Use free-form query to cast CLOB to VARCHAR
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --query "SELECT DOC_ID, DOC_NAME, DBMS_LOB.SUBSTR(DOC_CONTENT, 4000, 1) as DOC_CONTENT_TRUNC FROM DOCUMENTS WHERE \$CONDITIONS" \
  --split-by DOC_ID \
  --target-dir /data/raw/documents \
  --num-mappers 4

# Solution 3: For BLOBs (binary), use --map-column-java
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table IMAGES \
  --map-column-java IMAGE_DATA=String \
  --target-dir /data/raw/images \
  --num-mappers 2
```

## Incremental Load with SCD Type 2

Implementing Slowly Changing Dimensions with Sqoop + Hive:

```bash
# Step 1: Import changed records from source
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table CUSTOMERS \
  --incremental lastmodified \
  --check-column UPDATED_AT \
  --last-value "$(cat /var/etl/customers_last_value.txt)" \
  --target-dir /data/staging/customers_delta \
  --num-mappers 4 \
  --as-parquetfile

# Step 2: Apply SCD Type 2 in Hive
hive << 'EOF'
-- Expire existing records that changed
UPDATE dim.customers
SET effective_end_date = CURRENT_DATE,
    is_current = false
WHERE customer_id IN (
  SELECT customer_id FROM staging.customers_delta
)
AND is_current = true;

-- Insert new versions
INSERT INTO dim.customers
SELECT
  customer_id,
  name,
  email,
  address,
  CURRENT_DATE as effective_start_date,
  '9999-12-31' as effective_end_date,
  true as is_current
FROM staging.customers_delta;
EOF

# Step 3: Save the last imported timestamp
sqoop job --show daily_customer_import | grep "last.value" | awk '{print $3}' > /var/etl/customers_last_value.txt
```

## Multi-Source Import with Airflow Orchestration

```python
# airflow_sqoop_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['de-alerts@company.com']
}

dag = DAG(
    'daily_sqoop_imports',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

TABLES = {
    'customers': {'mappers': 8, 'split_by': 'CUSTOMER_ID'},
    'orders': {'mappers': 16, 'split_by': 'ORDER_ID'},
    'products': {'mappers': 4, 'split_by': 'PRODUCT_ID'},
}

sqoop_tasks = []
for table, config in TABLES.items():
    task = BashOperator(
        task_id=f'import_{table}',
        bash_command=f"""
            sqoop import \\
              --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \\
              --username etl_user \\
              --password-file hdfs:///user/etl/oracle.pass \\
              --table {table.upper()} \\
              --target-dir /data/raw/{table}/dt={{{{ ds }}}} \\
              --num-mappers {config['mappers']} \\
              --split-by {config['split_by']} \\
              --as-parquetfile \\
              --compression-codec snappy
        """,
        dag=dag
    )
    sqoop_tasks.append(task)

# Run all imports in parallel
start >> sqoop_tasks >> end
```

## Handling Timezone Issues in Timestamps

Timezone mismatch between Oracle (UTC) and local Hadoop cluster is a common production bug:

```bash
# Problem: Oracle stores timestamps in UTC, Sqoop converts to JVM local time
# This causes off-by-N hours issues in Hive queries

# Solution 1: Set JVM timezone to UTC for Sqoop
export HADOOP_CLIENT_OPTS="-Duser.timezone=UTC"
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table EVENTS \
  --target-dir /data/raw/events \
  --num-mappers 4

# Solution 2: Cast timestamps to strings in the query
sqoop import \
  --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --query "SELECT EVENT_ID, TO_CHAR(EVENT_TIME AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') as EVENT_TIME_UTC FROM EVENTS WHERE \$CONDITIONS" \
  --split-by EVENT_ID \
  --target-dir /data/raw/events \
  --num-mappers 4

# Solution 3: Handle in Hive with CONVERT_TZ
-- In Hive after import:
-- SELECT *, CONVERT_TZ(event_time, 'America/New_York', 'UTC') as event_time_utc
-- FROM raw.events;
```

## Migrating from Sqoop to Spark JDBC

A phased migration approach used in practice:

```
Phase 1: Parallel run (validate)
  Sqoop → HDFS path A
  Spark → HDFS path B
  Compare row counts and checksums

Phase 2: Switch traffic
  Spark → HDFS path A (production)
  Sqoop → deprecated

Phase 3: Decommission
  Remove Sqoop jobs
  Remove Sqoop metastore
```

```python
# spark_jdbc_replacement.py — mirrors the Sqoop import logic
from pyspark.sql import SparkSession
import sys

def import_table(table_name, split_col, lower, upper, num_partitions, date_str):
    spark = SparkSession.builder \
        .appName(f"jdbc_import_{table_name}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//prod-oracle:1521/ORCL") \
        .option("dbtable", table_name) \
        .option("user", "etl_user") \
        .option("password", open("/secure/oracle.pass").read().strip()) \
        .option("numPartitions", num_partitions) \
        .option("partitionColumn", split_col) \
        .option("lowerBound", lower) \
        .option("upperBound", upper) \
        .option("fetchsize", 10000) \
        .option("sessionInitStatement", "ALTER SESSION SET TIME_ZONE='UTC'") \
        .load()

    output_path = f"/data/raw/{table_name.lower()}/dt={date_str}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Imported {df.count()} rows for {table_name} -> {output_path}")
    spark.stop()
```

## Data Pipeline Architecture

```
graph TD
    A["Oracle OLTP<br>Production DB"] -->|"Sqoop<br>8 mappers"| B["HDFS Raw Zone<br>/data/raw/"]
    B --> C["Hive External Table<br>raw.orders"]
    C -->|"HiveQL ETL"| D["Hive Managed Table<br>refined.orders"]
    D -->|"Spark aggregation"| E["Hive curated<br>curated.order_summary"]
    E --> F["Presto / Athena<br>Analytics queries"]
    G["Airflow Scheduler"] -->|"triggers daily"| A
    H["Sqoop Metastore<br>MySQL"] -->|"job config"| A
```

## Interview Tips

> **Tip 1:** LOB column handling is a classic interview question. The clean answer is: exclude them if not needed, cast CLOB to VARCHAR2 in a `--query` statement, or use a post-processing step. Explain why LOBs fail (JDBC streaming limitations with Oracle).

> **Tip 2:** SCD Type 2 with Sqoop is a two-step process: Sqoop handles the extract/delta detection, Hive or Spark handles the dimension merge. Sqoop alone cannot do SCD logic — it just moves data.

> **Tip 3:** For timezone bugs, the safest production pattern is to cast timestamps to ISO 8601 strings in the SQL query (`TO_CHAR`) so Sqoop treats them as plain strings. This avoids JVM timezone conversion entirely.

> **Tip 4:** When presenting Sqoop-to-Spark migration, emphasize the validation phase (parallel run with row count + checksum comparison) before cutover. This is what separates a professional migration from a risky flag day.

> **Tip 5:** Airflow + Sqoop is the dominant orchestration pattern in Hadoop shops. Know how to set `retries`, `retry_delay`, and `email_on_failure` — these are standard SRE requirements for production data pipelines.
