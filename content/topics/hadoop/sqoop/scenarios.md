---
title: "Sqoop - Scenario Questions"
topic: hadoop
subtopic: sqoop
content_type: scenario_question
tags: [hadoop, sqoop, scenarios, interview, oracle, hdfs, hive]
---

# Scenario Questions — Sqoop

<article data-difficulty="junior">

## 🟢 Junior: Import a MySQL Table to HDFS with Hive Partitioning

**Scenario:** You are a new data engineer at an e-commerce company. Your team has a MySQL database with an `orders` table (columns: `order_id`, `customer_id`, `amount`, `status`, `order_date`). You need to import this table into HDFS in Parquet format, partitioned by `order_date`, and register it as a Hive table.

<details><summary>💡 Hint</summary>

Think about:
- Which Sqoop flags create Hive tables automatically?
- How does Sqoop handle partitioning — does it partition by a column value, or do you partition HDFS directories?
- What format flag enables Parquet output?

</details>

<details><summary>✅ Solution</summary>

**Step 1: Basic import to HDFS**
```bash
sqoop import \
  --connect jdbc:mysql://mysql-host:3306/ecommerce \
  --username sqoop_user \
  --password-file /home/etl/.mysql.pass \
  --table orders \
  --target-dir /data/raw/orders \
  --split-by order_id \
  --num-mappers 4 \
  --as-parquetfile \
  --compression-codec snappy
```

**Step 2: Import directly into Hive with partitioning**

Note: Sqoop's `--hive-partition-key` creates a static partition for the entire import run. For dynamic daily partitioning, use date-scoped WHERE clauses:

```bash
DATE=$(date +%Y-%m-%d)

sqoop import \
  --connect jdbc:mysql://mysql-host:3306/ecommerce \
  --username sqoop_user \
  --password-file /home/etl/.mysql.pass \
  --table orders \
  --where "order_date = '${DATE}'" \
  --target-dir /data/raw/orders/dt=${DATE} \
  --split-by order_id \
  --num-mappers 4 \
  --as-parquetfile \
  --compression-codec snappy \
  --hive-import \
  --hive-table raw.orders \
  --hive-partition-key dt \
  --hive-partition-value "${DATE}"
```

**Verify the result:**
```bash
hdfs dfs -ls /data/raw/orders/
hive -e "SHOW PARTITIONS raw.orders;"
hive -e "SELECT COUNT(*) FROM raw.orders WHERE dt='${DATE}';"
```

**Key points:**
- `--split-by order_id` distributes import across 4 mappers by splitting the ID range
- `--as-parquetfile` writes columnar Parquet instead of CSV
- `--hive-import` with `--hive-partition-key` creates the Hive partition automatically
- Always use `--password-file` not `--password` (CLI args are visible in process list)

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Incremental Load Strategy for a 500GB Table

**Scenario:** You have a 500GB Oracle table `TRANSACTIONS` that grows by ~2GB daily. A full import takes 45 minutes and is too slow. Design and implement an incremental load strategy using Sqoop that:
1. Handles new rows (INSERT only)
2. Handles updated rows (UPDATE tracking)
3. Is idempotent (safe to re-run)
4. Stores state between runs

<details><summary>💡 Hint</summary>

Sqoop has two incremental modes:
- `--incremental append`: for insert-only tables (uses a monotonically increasing column like `id`)
- `--incremental lastmodified`: for tables with an update timestamp column

Consider how to persist the `--last-value` between runs.

</details>

<details><summary>✅ Solution</summary>

**Architecture:**

```
graph LR
    A["Oracle TRANSACTIONS<br>500GB table"] -->|"incremental import<br>2GB delta"| B["HDFS /data/raw/transactions/"]
    B --> C["Hive staging table"]
    C -->|"MERGE/UPSERT"| D["Hive production table"]
    E["Sqoop Metastore<br>saves last-value"] -->|"state persistence"| A
```

**Strategy 1: Append-only (insert-only table)**
```bash
# First run: create saved job
sqoop job --create transactions_incremental \
  -- import \
  --connect "jdbc:oracle:thin:@//prod-oracle:1521/ORCL" \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table TRANSACTIONS \
  --target-dir /data/raw/transactions \
  --incremental append \
  --check-column TRANSACTION_ID \
  --last-value 0 \
  --num-mappers 8 \
  --split-by TRANSACTION_ID \
  --as-parquetfile \
  --compression-codec snappy

# Daily execution — Sqoop auto-updates last-value in metastore
sqoop job --exec transactions_incremental
```

**Strategy 2: Updates + Inserts (lastmodified)**
```bash
# For tables with UPDATED_AT timestamp
sqoop job --create transactions_delta \
  -- import \
  --connect "jdbc:oracle:thin:@//prod-oracle:1521/ORCL" \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table TRANSACTIONS \
  --target-dir /data/raw/transactions_delta \
  --incremental lastmodified \
  --check-column UPDATED_AT \
  --merge-key TRANSACTION_ID \
  --last-value "2024-01-01 00:00:00" \
  --num-mappers 8 \
  --split-by TRANSACTION_ID

sqoop job --exec transactions_delta
```

**Making it idempotent:**
```bash
#!/bin/bash
# idempotent_sqoop.sh
DATE=$1
LOCK_FILE="/var/etl/transactions_${DATE}.lock"

if [ -f "$LOCK_FILE" ]; then
  echo "Import for ${DATE} already completed"
  exit 0
fi

sqoop import \
  --connect "jdbc:oracle:thin:@//prod-oracle:1521/ORCL" \
  --username etl_user \
  --password-file hdfs:///user/etl/oracle.pass \
  --table TRANSACTIONS \
  --where "TRUNC(CREATED_DATE) = DATE '${DATE}'" \
  --target-dir /data/raw/transactions/dt=${DATE} \
  --num-mappers 8 \
  --split-by TRANSACTION_ID \
  --as-parquetfile && touch "$LOCK_FILE"
```

**Merge delta into production in Hive:**
```sql
-- Hive MERGE for upsert
MERGE INTO prod.transactions AS target
USING staging.transactions_delta AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
  UPDATE SET
    amount = source.amount,
    status = source.status,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT VALUES (source.*);
```

**Tradeoffs summary:**

| Strategy | Use case | Data completeness | Complexity |
|----------|----------|-------------------|------------|
| append | Insert-only tables | 100% new rows | Low |
| lastmodified | Tables with updated_at | Inserts + updates | Medium |
| Date-scoped WHERE | Date-partitioned data | Full day idempotent | Low |
| Full refresh | Small/medium tables | 100% always | High (time) |

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Zero-Downtime Migration from On-Prem Oracle to HDFS/Hive

**Scenario:** Your company has a 10TB Oracle database with 50 tables used by production OLTP systems 24/7. You need to design a complete migration plan to HDFS/Hive with:
- Zero downtime for the Oracle OLTP system
- Data consistency guarantees
- Rollback capability
- Completion in under 2 weeks

Design the full migration architecture, tooling, and execution plan.

<details><summary>💡 Hint</summary>

Consider:
- How do you handle initial bulk load vs ongoing CDC (Change Data Capture)?
- What's your validation strategy to prove Hive data matches Oracle?
- How do you cut over downstream consumers without downtime?
- What's your rollback plan if Hive data is corrupt?

</details>

<details><summary>✅ Solution</summary>

**Migration Architecture:**

```
graph TD
    A["Oracle 10TB<br>Production OLTP"] -->|"Phase 1: Bulk load<br>via Sqoop 16 mappers"| B["HDFS Raw Zone<br>/data/migration/"]
    A -->|"Phase 2: CDC<br>Oracle LogMiner / GoldenGate"| C["Kafka Topics<br>oracle.cdc.*"]
    C -->|"Kafka Connect HDFS Sink"| B
    B --> D["Hive External Tables<br>migration.*"]
    D -->|"Validation jobs"| E["Reconciliation Reports<br>row counts, checksums"]
    E -->|"Phase 3: Cutover"| F["Production Hive<br>prod.*"]
    G["Airflow"] -->|"orchestrates all phases"| A
```

**Phase 1: Bulk Initial Load (Days 1-5)**

```bash
#!/bin/bash
# bulk_migration.sh — Run over weekend for initial 10TB load

TABLES=(CUSTOMERS ORDERS ORDER_ITEMS PRODUCTS TRANSACTIONS INVENTORY)

for TABLE in "${TABLES[@]}"; do
  # Get row count for validation
  ROW_COUNT=$(sqoop eval \
    --connect jdbc:oracle:thin:@//prod-oracle:1521/ORCL \
    --username migrator \
    --password-file hdfs:///user/etl/oracle.pass \
    --query "SELECT COUNT(*) FROM ${TABLE}" | tail -2 | head -1 | tr -d '|' | xargs)

  echo "${TABLE}: ${ROW_COUNT} rows" >> /var/migration/row_counts.txt

  # Bulk import with 16 mappers
  sqoop import \
    --connect "jdbc:oracle:thin:@//prod-oracle:1521/ORCL" \
    --username migrator \
    --password-file hdfs:///user/etl/oracle.pass \
    --table $TABLE \
    --target-dir /data/migration/${TABLE,,}/full \
    --num-mappers 16 \
    --split-by $(get_split_col $TABLE) \
    --as-parquetfile \
    --compression-codec snappy \
    --fetch-size 10000 \
    2>/var/migration/logs/${TABLE}_import.log

  echo "${TABLE} bulk import complete: $(date)"
done
```

**Phase 2: CDC Sync (Days 5-13)**

```bash
# Enable Oracle supplemental logging
-- In Oracle:
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER TABLE CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

# Kafka Connect Oracle CDC connector config
cat > oracle-cdc-connector.json << 'EOF'
{
  "name": "oracle-cdc",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "prod-oracle",
    "database.port": "1521",
    "database.user": "logminer_user",
    "database.password": "${file:/opt/kafka/secrets.properties:oracle.password}",
    "database.dbname": "ORCL",
    "database.server.name": "oracle.prod",
    "table.include.list": "SCHEMA.CUSTOMERS,SCHEMA.ORDERS,SCHEMA.ORDER_ITEMS",
    "database.history.kafka.bootstrap.servers": "kafka-broker:9092",
    "database.history.kafka.topic": "schema-changes.oracle"
  }
}
EOF
```

**Phase 3: Validation Framework**

```python
# validate_migration.py
from pyspark.sql import SparkSession

def validate_table(oracle_table, hive_table):
    spark = SparkSession.builder.appName("migration_validation").getOrCreate()

    # Read from Oracle via JDBC
    oracle_df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//prod-oracle:1521/ORCL") \
        .option("dbtable", oracle_table) \
        .option("user", "migrator") \
        .option("password", open("/secure/oracle.pass").read().strip()) \
        .load()

    # Read from Hive
    hive_df = spark.table(hive_table)

    # Row count validation
    oracle_count = oracle_df.count()
    hive_count = hive_df.count()
    print(f"Oracle: {oracle_count}, Hive: {hive_count}, Match: {oracle_count == hive_count}")

    # Checksum validation on key columns
    from pyspark.sql import functions as F
    oracle_checksum = oracle_df.agg(F.sum("id"), F.sum("amount")).collect()[0]
    hive_checksum = hive_df.agg(F.sum("id"), F.sum("amount")).collect()[0]
    print(f"Checksums match: {oracle_checksum == hive_checksum}")

    return oracle_count == hive_count and oracle_checksum == hive_checksum
```

**Phase 4: Cutover (Day 14)**

```
Cutover Runbook:
1. 11:00 PM: Freeze Oracle writes (maintenance window announcement)
2. 11:05 PM: Run final Sqoop delta import (last ~1 hour of changes)
3. 11:15 PM: Run validation suite — must pass 100%
4. 11:30 PM: Switch application DB config from Oracle to Hive (feature flag)
5. 11:35 PM: Monitor error rates in Grafana
6. 11:45 PM: If OK, close maintenance window. If NOT, roll back to Oracle.

Rollback: Revert feature flag → Oracle is untouched, no data loss
```

**Key Design Decisions:**

| Decision | Choice | Reason |
|----------|--------|--------|
| Initial load tool | Sqoop (16 mappers) | Fastest bulk transfer for Oracle |
| CDC tool | Debezium + Kafka | Proven Oracle CDC, replayable |
| Validation | Spark row count + checksum | Comprehensive, scalable |
| Cutover | Feature flag | Instant rollback capability |
| Format | Parquet + Snappy | Query performance + compression |

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Apache Sqoop and what is it used for?**
A: Sqoop is a tool for bulk data transfer between Hadoop (HDFS, Hive, HBase) and relational databases (MySQL, PostgreSQL, Oracle, SQL Server). It uses JDBC to parallelize imports and exports using MapReduce tasks.

**Q: How does Sqoop parallelize data imports?**
A: Sqoop splits the import by dividing the source table's primary key (or a specified split column) into N equally-sized ranges and launches N parallel Map tasks, each responsible for importing one range. This parallelizes the JDBC reads across multiple connections.

**Q: What is the difference between Sqoop import and Sqoop export?**
A: `sqoop import` reads data from a relational database and writes it to HDFS, Hive, or HBase. `sqoop export` reads data from HDFS and writes it back to a relational database table. Exports are typically used to push processed results from Hadoop back to operational systems.

**Q: How does Sqoop support incremental imports?**
A: Sqoop's `--incremental append` mode imports only rows where a specified column (e.g., id or created_at) is greater than the last imported value (stored in a saved job's metadata). `--incremental lastmodified` imports rows where a timestamp column is newer than the last run.

**Q: What are Sqoop saved jobs?**
A: Saved jobs store Sqoop command configurations (connection URL, credentials, table, incremental parameters) in a local metastore. Running a saved job re-executes the stored configuration, automatically updating the incremental watermark after each successful run.

**Q: What are the challenges of Sqoop exports?**
A: Exports are not atomic — multiple parallel Map tasks write to the target table simultaneously, so a partial failure leaves the table in an inconsistent state. Workarounds include writing to a staging table first and swapping, or using `--staging-table` option.

**Q: How does Sqoop handle special characters and NULL values?**
A: Sqoop provides options like `--null-string`, `--null-non-string`, `--escaped-by`, and `--enclosed-by` to handle NULL representations and special delimiter characters in the exported files. Mismatch of these settings between import and export is a common source of data corruption bugs.

**Q: What has largely replaced Sqoop in modern data architectures?**
A: Cloud-native ingestion tools have largely replaced Sqoop: AWS Database Migration Service (DMS), Fivetran, Airbyte, and cloud-native JDBC connectors in Spark or Glue. Sqoop is still found in legacy Hadoop environments but is not developed actively.

---

## 💼 Interview Tips

- Know Sqoop's parallelization mechanism (split-by primary key) and its limitation — if the split column is skewed or non-numeric, you may need `--split-by` with a custom column or reduce mappers to avoid imbalanced tasks.
- Mention the export atomicity problem proactively — it's the most common Sqoop production pitfall and discussing the staging table workaround shows real operational experience.
- Frame Sqoop as legacy knowledge — most modern data platforms have replaced it with CDC (Debezium), managed connectors (Fivetran), or Spark JDBC. Show you know both the tool and its modern alternatives.
- Discuss incremental import watermark management: Sqoop stores the last imported value in its metastore, but if the metastore is lost, you must reset it manually — a common operational headache worth mentioning.
- For senior roles, discuss performance tuning: increasing `--num-mappers`, using `--fetch-size` to control JDBC batch sizes, and choosing a well-indexed split column to avoid full table scans on the source.
- If asked about Sqoop vs. CDC, articulate the difference: Sqoop is bulk batch import; CDC captures row-level changes in real time. Sqoop is appropriate for initial historical loads; CDC handles ongoing replication.
