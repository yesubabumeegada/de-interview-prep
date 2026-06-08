---
title: "Azure SQL & Managed Instance — Real World"
topic: azure
subtopic: azure-sql
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, azure-sql, production, performance, monitoring, cdc, migration]
---

# Azure SQL & Managed Instance — Real World

## Pattern 1: Azure SQL as Serving Layer for BI

```python
# Load Gold Delta data to Azure SQL DB for Power BI
# ADF pipeline: reads Gold Parquet from ADLS → truncates staging → COPY INTO prod

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadToAzureSQL").getOrCreate()

JDBC_URL = (
    "jdbc:sqlserver://myserver.database.windows.net:1433;"
    "database=analytics_db;encrypt=true;trustServerCertificate=false;"
    "loginTimeout=30;authentication=ActiveDirectoryMSI"
)

def load_gold_to_sql(table_name: str, gold_path: str, key_cols: list):
    """Load Gold Delta table to Azure SQL DB with upsert semantics."""
    
    # Read from Gold Delta
    gold_df = spark.read.format("delta").load(gold_path)
    
    # Write to Azure SQL using JDBC
    # For large tables: write in partitioned batches to avoid JDBC timeout
    row_count = gold_df.count()
    num_partitions = max(1, row_count // 100000)  # 100K rows per batch
    
    gold_df.repartition(num_partitions).write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", f"staging.{table_name}") \
        .option("batchsize", 10000) \
        .option("truncate", "true") \   # truncate staging before write
        .option("numPartitions", num_partitions) \
        .mode("overwrite") \
        .save()
    
    print(f"Loaded {row_count:,} rows to staging.{table_name}")
    
    # Merge staging → production (upsert)
    key_join = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
    merge_sql = f"""
        MERGE dbo.{table_name} AS t
        USING staging.{table_name} AS s
        ON {key_join}
        WHEN MATCHED THEN UPDATE SET {', '.join([f"t.{c} = s.{c}" for c in gold_df.columns if c not in key_cols])}
        WHEN NOT MATCHED THEN INSERT ({', '.join(gold_df.columns)}) VALUES ({', '.join([f"s.{c}" for c in gold_df.columns])});
    """
    
    # Execute merge via pyodbc
    import pyodbc
    conn = pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=myserver.database.windows.net;DATABASE=analytics_db;Authentication=ActiveDirectoryMsi")
    conn.execute(merge_sql)
    conn.commit()
    conn.close()
    print(f"Merge to dbo.{table_name} complete")

# Load tables
load_gold_to_sql("daily_revenue",   "abfss://gold@account.dfs.core.windows.net/daily_revenue/",   ["region", "order_date"])
load_gold_to_sql("customer_metrics", "abfss://gold@account.dfs.core.windows.net/customer_metrics/", ["customer_id"])
```

---

## Pattern 2: CDC Pipeline from SQL Managed Instance

```python
# Debezium CDC: SQL Managed Instance → Event Hubs → Bronze Delta
# Architecture: on-prem SQL MI → Debezium Connector (Kafka Connect) → Event Hubs → ADF/Databricks

# 1. Enable CDC on SQL Managed Instance
# (Run on SQL MI — requires sysadmin or db_owner)
# EXEC sys.sp_cdc_enable_db;
# EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL;

# 2. Debezium SQL Server connector config (runs in Kafka Connect cluster or Azure Container Instance):
debezium_config = {
    "name": "sqlmi-orders-cdc",
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": "myinstance.public.database.windows.net",
        "database.port": "3342",                    # SQL MI public endpoint port
        "database.user": "cdc_reader",
        "database.password": "${file:/secrets.properties:sqlmi.password}",
        "database.names": "orders_db",
        "table.include.list": "dbo.orders,dbo.customers,dbo.products",
        "database.history.kafka.bootstrap.servers": "mynamespace.servicebus.windows.net:9093",
        "database.history.kafka.topic": "dbhistory.orders_db",
        "database.history.producer.security.protocol": "SASL_SSL",
        "database.history.producer.sasl.mechanism": "PLAIN",
        "database.history.producer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='<EH_CONN_STRING>';",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    }
}

# 3. Databricks reads CDC events from Event Hubs → appends to Bronze Delta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

EH_CONF = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EH_CONN_STR),
    "eventhubs.consumerGroup": "databricks-cdc"
}

cdc_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**EH_CONF)
    .load()
    .select(F.from_json(F.col("body").cast("string"), cdc_schema).alias("cdc"))
    .select("cdc.*")
)

# Write to Bronze Delta (raw CDC events for replay capability)
cdc_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/cdc_bronze") \
    .outputMode("append") \
    .table("bronze.orders_cdc")
```

---

## Pattern 3: Monitoring and Auto-Scaling

```sql
-- Azure SQL DB auto-scaling via serverless tier monitoring

-- Check current resource utilization
SELECT
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    avg_memory_usage_percent,
    connection_successful_count
FROM sys.dm_db_resource_stats
ORDER BY end_time DESC;

-- Long-running queries (> 30 seconds)
SELECT
    r.session_id,
    r.status,
    r.command,
    r.start_time,
    DATEDIFF(SECOND, r.start_time, GETDATE()) AS duration_sec,
    r.wait_type,
    r.wait_time / 1000 AS wait_sec,
    t.text AS query_text,
    p.query_plan
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) p
WHERE r.session_id <> @@SPID
AND DATEDIFF(SECOND, r.start_time, GETDATE()) > 30
ORDER BY duration_sec DESC;

-- Blocking detection
SELECT
    blocking.session_id AS blocking_session,
    blocked.session_id AS blocked_session,
    blocked_text.text AS blocked_query,
    blocking_text.text AS blocking_query,
    blocked.wait_time / 1000 AS blocked_wait_sec
FROM sys.dm_exec_requests blocked
JOIN sys.dm_exec_sessions blocking ON blocked.blocking_session_id = blocking.session_id
CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) blocked_text
CROSS APPLY sys.dm_exec_sql_text(blocking.sql_handle) blocking_text
WHERE blocked.blocking_session_id <> 0;

-- Azure Monitor alert:
-- Metric: cpu_percent > 80% for 5 min → scale up tier (automation runbook)
-- Metric: deadlock_count > 0 → alert to on-call team
-- Log Analytics: AzureMetrics | where MetricName == "cpu_percent" | where Maximum > 80
```

---

## Interview Tips

> **Tip 1:** "How do you tune a slow SQL query in Azure SQL DB without changing the application code?" — Using Query Store: identify the query by query_id, examine its plan in Query Store's visual plan view. If a bad plan is being used: force the known-good plan via `sp_query_store_force_plan`. Create a missing index recommended by `sys.dm_db_missing_index_details`. If parameter sniffing: use `OPTION (OPTIMIZE FOR UNKNOWN)` via a query hint (requires a stored procedure wrapper) or use `OPTIMIZE FOR (specific value)`. Automatic Tuning (Azure SQL DB feature): enable auto-create index and auto-fix-plan — Azure automatically creates recommended indexes and reverts plan regressions without manual intervention.

> **Tip 2:** "How does Azure SQL DB handle connection pooling in microservices?" — Azure SQL DB has a connection limit (150 for Basic/Standard, up to 3,000+ for Premium). In a microservices architecture with 50 pods each using connection pools of 10 = 500 connections — can exceed limits for smaller tiers. Solutions: (a) Azure SQL Hyperscale: higher connection limits; (b) PgBouncer-style: use Azure SQL connection pool service; (c) Reduce max_pool_size in application config; (d) Use serverless tier (lower concurrency but auto-pauses when idle). For high-concurrency microservices: Business Critical tier (more resources) or scale horizontally using elastic pools per service group.

> **Tip 3:** "What is the difference between Active Geo-Replication and Auto-Failover Groups?" — Active Geo-Replication: up to 4 readable secondary replicas, manual failover (application must change connection string), configured per-database, flexible replica placement. Auto-Failover Group: built on top of geo-replication, adds a single listener endpoint that automatically redirects after failover (application connection string doesn't change — just use the group endpoint), configurable automatic vs manual failover policy, grace period before auto-failover (e.g., 60 minutes), and can include multiple databases in one failover group. Use Failover Groups in production to avoid application-level failover orchestration.
