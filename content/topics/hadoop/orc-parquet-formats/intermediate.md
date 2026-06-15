---
title: "ORC & Parquet Formats - Intermediate"
topic: hadoop
subtopic: orc-parquet-formats
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, orc, parquet, columnar, storage]
---

# ORC & Parquet Formats — Intermediate

## Schema Evolution

Schema evolution is the ability to add, remove, or rename columns without rewriting existing data files.

### Parquet Schema Evolution

Parquet supports schema evolution through column projection: readers read only the columns that exist in both the schema and the file; missing columns are returned as `null`.

```python
# Spark: read old Parquet files with a new, wider schema
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

df = spark.read.option("mergeSchema", "true").parquet("/path/to/data")
# Columns added after some files were written → null for old files
```

**Supported evolutions in Parquet:**
- Add new optional column (backward compatible)
- Remove column (old readers ignore unknown columns)
- Widen numeric types: INT32 → INT64, FLOAT → DOUBLE

**Unsupported:**
- Renaming columns (column identity is by name in Parquet, so rename = remove + add)
- Changing types incompatibly (STRING → INT)

### ORC Schema Evolution

ORC uses column IDs (ordinal position) for type information, making column renaming safe:

```sql
-- Rename a column in ORC (safe: readers use position, not name)
ALTER TABLE events CHANGE COLUMN user_id uid BIGINT;
```

**Supported in ORC:**
- Add columns at the end of the struct
- Rename columns (position-based reads are unaffected)
- Widen types (INT → BIGINT, FLOAT → DOUBLE)

---

## Nested Data Handling

Both formats support nested schemas (structs, arrays, maps) using the **Dremel encoding** (Parquet) or **tree encoding** (ORC).

### Parquet Nested Encoding (Dremel)

Parquet represents nested data using **repetition levels** and **definition levels**:
- **Definition level**: How many nullable fields in the path are defined (non-null).
- **Repetition level**: Position within repeated fields (arrays).

```python
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer", StructType([
        StructField("id", LongType()),
        StructField("name", StringType())
    ])),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", LongType())
    ])))
])

df = spark.read.schema(schema).parquet("/orders")

# Access nested fields
df.select("customer.name", "items.sku").show()

# Explode arrays
from pyspark.sql.functions import explode
df.select("order_id", explode("items").alias("item")) \
  .select("order_id", "item.sku", "item.qty") \
  .show()
```

### ORC Nested Encoding

ORC stores nested types using the type tree in the file footer. Each level of nesting is a separate ORC column stream.

```sql
-- ORC nested table in Hive
CREATE TABLE orders (
  order_id  BIGINT,
  customer  STRUCT<id: BIGINT, name: STRING>,
  items     ARRAY<STRUCT<sku: STRING, qty: INT>>
)
STORED AS ORC;

-- Query nested fields
SELECT order_id, customer.name
FROM   orders
WHERE  customer.id = 12345;
```

---

## Parquet with Spark: Key Configurations

```python
# Read
spark.conf.set("spark.sql.parquet.filterPushdown", "true")      # push predicates to file reader
spark.conf.set("spark.sql.parquet.mergeSchema", "false")        # disable costly schema merge unless needed
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")  # Spark 3 date handling

# Write
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")  # or zstd
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5000000)     # cap rows per output file

# Parquet vectorized reader (Spark's own vectorized path, separate from Hive's)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# Row group size (controls output file size granularity)
spark.conf.set("parquet.block.size", str(128 * 1024 * 1024))  # 128 MB row groups

# Dictionary encoding (auto, enabled by default)
spark.conf.set("parquet.enable.dictionary", "true")
spark.conf.set("parquet.dictionary.page.size", str(1024 * 1024))  # 1 MB dict page
```

### Controlling Output File Count

```python
# Coalesce output files to control small file problem
df.coalesce(50).write.parquet("/output")

# Or use repartition + sort for better data locality
df.repartition(200, "partition_col") \
  .sortWithinPartitions("sort_key") \
  .write.partitionBy("partition_col") \
  .parquet("/output")

# Auto-size files with AQE (Spark 3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
```

---

## File Size Optimization and the Small File Problem

### What Is the Small File Problem?

HDFS NameNode stores metadata (block locations) for every file in memory. The recommended minimum file size is 128 MB (one HDFS block). Files smaller than 128 MB:
- Waste NameNode heap (each file = ~150 bytes of heap)
- Cause excessive map task overhead (one mapper per file)
- Reduce read throughput (more sequential I/O segments)

**Common causes:**
- Streaming ingest (Kafka → HDFS) writing micro-batches
- High-cardinality `partitionBy()` with small data per partition
- Failed/retried jobs leaving partial outputs

### Detection

```bash
hdfs dfs -count /warehouse/data/
# FILE_COUNT   DIR_COUNT   CONTENT_SIZE   PATH
# 50,000       1,200       650 GB         /warehouse/data
# Ideal: 650 GB / 128 MB ≈ 5,200 files. 50,000 files = problem.

# Find partitions with many small files
hdfs dfs -ls /warehouse/data/ | awk '{print $5, $8}' | sort -n | head -20
```

### Remediation

**Option 1: Spark compaction job**
```python
# Read all small files, coalesce, overwrite
spark.read.parquet("/warehouse/data/partition=2024-01/") \
     .coalesce(10) \
     .write.mode("overwrite") \
     .parquet("/warehouse/data/partition=2024-01/")
```

**Option 2: Hive ORC CONCATENATE**
```sql
ALTER TABLE events PARTITION(event_date='2024-01') CONCATENATE;
```

**Option 3: Hive MAJOR compaction (ACID tables)**
```sql
ALTER TABLE events COMPACT 'MAJOR';
```

**Option 4: Spark AQE (prevent small files at write time)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

---

## ORC ACID Support

ORC is the only Hive-native format supporting full ACID transactions with row-level `INSERT`, `UPDATE`, `DELETE`, `MERGE`.

**How ORC ACID works:**
- Base files: full snapshot of data at compaction time.
- Delta files: incremental changes written as ORC files with operation type (INSERT/UPDATE/DELETE) and row ID.
- At read time, the ORC reader merges base + deltas in memory, applying deletes and updates.

```
table/
├── base_0000010/          ← result of last major compaction
│   └── bucket_00000
├── delta_0000011_0000015/ ← 5 INSERT transactions
│   └── bucket_00000
├── delete_delta_0000016_0000016/  ← 1 DELETE transaction
│   └── bucket_00000
```

Read merge overhead increases as delta files accumulate. Run compaction regularly:
```sql
-- Minor: merge only delta files (not into base)
ALTER TABLE orders COMPACT 'MINOR';

-- Major: merge base + all deltas into new base (maximum read performance)
ALTER TABLE orders COMPACT 'MAJOR';

-- Monitor compaction queue
SHOW COMPACTIONS;
```
