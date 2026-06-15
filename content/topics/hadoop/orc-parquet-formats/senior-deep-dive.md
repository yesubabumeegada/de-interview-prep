---
title: "ORC & Parquet Formats - Senior Deep Dive"
topic: hadoop
subtopic: orc-parquet-formats
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hadoop, orc, parquet, columnar, storage]
---

# ORC & Parquet Formats — Senior Deep Dive

## Encoding Internals: How Columnar Compression Actually Works

Beyond the codec (Snappy, ZSTD), both ORC and Parquet apply **encoding** to each column before compression. Encoding exploits column-level patterns:

### Dictionary Encoding

When a column has low cardinality (e.g., `status` has values: PENDING, SHIPPED, DELIVERED), store a dictionary and replace values with integer IDs.

```
Original:  PENDING, SHIPPED, PENDING, DELIVERED, SHIPPED, PENDING
Dict:      {0: PENDING, 1: SHIPPED, 2: DELIVERED}
Encoded:   0, 1, 0, 2, 1, 0
```

Result: STRING values replaced by small integers → 5–20× smaller before compression.

**Parquet**: Dictionary encoding is default for columns with < 1 MB distinct values per row group.
**ORC**: Uses dictionary encoding for STRING/VARCHAR columns with < 10,000 distinct values per stripe (configurable via `orc.dictionary.key.threshold`).

### Run-Length Encoding (RLE)

For columns with repeated consecutive values (e.g., `partition_date` is the same for all rows in a partition file):

```
Original:  2024-01-15, 2024-01-15, 2024-01-15, ... (10,000 times)
RLE:       (2024-01-15, repeat=10000)
```

**ORC RLE v2**: Used for integer, date, and timestamp columns. Four sub-encodings:
- Direct: no pattern
- Patched base: mostly small values with occasional large outliers
- Delta: monotonically increasing/decreasing sequences (e.g., auto-increment IDs)
- Short repeat: same value repeated

### Bit Packing

Integer columns with small max values are packed into fewer bits:

```
Values: 0–7 (3-bit range)
Standard INT32: 32 bits per value
Bit-packed: 3 bits per value → 10× space savings
```

---

## ORC vs Parquet: Deep Technical Comparison

| Dimension | ORC | Parquet |
|---|---|---|
| **Predicate pushdown granularity** | File + stripe + row group (10K rows) | File + row group (128 MB) + page (1 MB, Parquet 1.11+) |
| **Bloom filter** | Native, per stripe | Available via `parquet.bloom.filter.enabled` (Parquet 1.12+) |
| **ACID support** | Native (with Hive TxnManager) | No (need Iceberg/Delta Lake) |
| **Nested type encoding** | Type tree per column stream | Dremel repetition/definition levels |
| **Schema evolution** | Position-based (rename safe) | Name-based (rename = breaking change) |
| **Compression unit** | Per stream within stripe | Per page within column chunk |
| **Statistics** | Stripe + row index (10K row stride) | Row group only (without col index feature) |
| **Ecosystem** | Hive-native; LLAP IO cache optimized | Spark, Presto/Trino, Athena, BigQuery, DuckDB |

---

## Parquet Column Index and Offset Index (Parquet 1.11+)

The **column index** stores min/max statistics per page (1 MB default) within a row group, enabling page-level predicate pushdown — the equivalent of ORC's row index.

```python
# Enable column index in Spark 3.2+
spark.conf.set("spark.sql.parquet.recordLevelFilter.enabled", "true")
spark.conf.set("parquet.page.row.count.limit", "20000")  # rows per page

# Enable bloom filter (Parquet 1.12+)
spark.conf.set("parquet.bloom.filter.enabled", "true")
spark.conf.set("parquet.bloom.filter.expected.ndv", "1000000")  # distinct values hint
spark.conf.set("parquet.bloom.filter.fpp", "0.05")
```

With column index, a Parquet file reading `WHERE customer_id = 12345` can skip individual pages within a row group — critical for point lookup queries in large Parquet files.

---

## Delta Lake vs ORC ACID vs Iceberg

When to pick which transactional format:

| Requirement | Best Choice |
|---|---|
| Hive-only ACID; no Spark | ORC ACID (Hive TxnManager) |
| Spark-first; structured streaming upserts | Delta Lake |
| Multi-engine (Spark + Trino + Flink); hidden partitioning | Apache Iceberg |
| Time travel + partition evolution | Iceberg or Delta |
| Streaming + batch unified | Delta Lake or Iceberg |

```python
# Delta Lake write (auto handles small file compaction via OPTIMIZE)
from delta.tables import DeltaTable

df.write.format("delta").mode("append").save("/delta/orders")

# Time travel
spark.read.format("delta").option("versionAsOf", 5).load("/delta/orders")

# Compaction
DeltaTable.forPath(spark, "/delta/orders").optimize().executeCompaction()

# Iceberg write
df.write.format("iceberg").mode("append").save("catalog.db.orders")

# Iceberg compaction
spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.orders')")
```

---

## Performance Benchmarks: ORC vs Parquet in Practice

**Workload: TPC-DS scale factor 1000 (1 TB), Hive on Tez**

| Query type | ORC + Snappy | Parquet + Snappy | Notes |
|---|---|---|---|
| Selective point query (1 key) | ~5s | ~12s | ORC bloom filters win |
| Full table scan aggregate | ~110s | ~115s | Similar; codec dominates |
| Multi-column filter + group by | ~40s | ~55s | ORC row index wins |
| Nested struct access | ~80s | ~90s | Similar |

**Workload: TPC-DS scale factor 1000, Spark 3.3**

| Query type | ORC + Snappy | Parquet + Snappy | Parquet + ZSTD |
|---|---|---|---|
| Selective point query | ~8s | ~6s | ~5s |
| Full table scan aggregate | ~95s | ~88s | ~72s |
| Multi-column filter | ~42s | ~38s | ~32s |

**Takeaway:**
- ORC wins in Hive (LLAP IO cache + native ORC reader).
- Parquet wins in Spark (optimized vectorized Parquet reader).
- ZSTD beats Snappy by 15–25% in Spark on CPU-adequate clusters.
- For cross-engine workloads, **Parquet + ZSTD + Iceberg** is the modern standard.

---

## File Size and Row Group Tuning

Optimal file/row-group size depends on the query pattern:

**For full-table scans (ETL):**
- Larger files = fewer seeks, better throughput
- ORC stripe: 256 MB; Parquet row group: 256 MB

```python
spark.conf.set("parquet.block.size", str(256 * 1024 * 1024))  # 256 MB row groups
```

**For selective queries (point lookups via bloom filters):**
- Smaller stripes/row groups = finer skipping granularity
- ORC stripe: 32 MB; Parquet row group: 64 MB

```sql
-- ORC fine-grained skipping
CREATE TABLE events STORED AS ORC
TBLPROPERTIES (
  'orc.stripe.size'      = '33554432',  -- 32 MB
  'orc.row.index.stride' = '5000',      -- statistics every 5K rows
  'orc.bloom.filter.columns' = 'customer_id,event_type',
  'orc.bloom.filter.fpp' = '0.01'       -- 1% FPP
);
```

**Golden rule:** Row group size × number of row groups per file should match HDFS block size (128 MB default). A file with 2 row groups of 64 MB fits in 1 HDFS block — no cross-block reads.
