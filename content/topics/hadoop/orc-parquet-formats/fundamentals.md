---
title: "ORC & Parquet Formats - Fundamentals"
topic: hadoop
subtopic: orc-parquet-formats
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [hadoop, orc, parquet, columnar, storage]
---

# ORC & Parquet Formats — Fundamentals

## 🎯 Analogy

A row-based format (CSV, Avro) stores data like a spreadsheet printed row-by-row — to find all values in one column you scan every row. A columnar format (ORC, Parquet) stores each column's data together — like storing all names in one drawer, all ages in another. When your query needs only 3 of 200 columns, you open 3 drawers instead of 200.

---

## Why Columnar Storage?

Analytical queries (SUM, AVG, GROUP BY, aggregations) touch a few columns across many rows. Columnar formats deliver two key benefits:

1. **Column pruning**: Only the columns referenced in the query are read from disk — I/O is proportional to columns selected, not total schema width.
2. **Better compression**: Values within a single column share the same data type and often have similar values (e.g., a `country` column has many repeated values). Run-length encoding and dictionary encoding compress columns far more efficiently than mixed-type rows.

---

## ORC Internal Structure

An ORC file is divided into **stripes** (default 64 MB each). Each stripe contains:

```
ORC File
├── File Header ("ORC")
├── Stripe 1
│   ├── Index Data      ← per-column min/max/sum/count per row group (10,000 rows)
│   ├── Row Data        ← actual column values, compressed
│   └── Stripe Footer   ← column encodings, offsets
├── Stripe 2
│   └── ...
└── File Footer
    ├── File-level statistics (min, max, count per column)
    ├── Stripe list (offsets)
    └── Schema (type tree)
    └── Postscript (compression codec, footer length)
```

**Row groups** (index stride, default 10,000 rows): ORC tracks statistics per row group within each stripe, enabling fine-grained predicate pushdown within a stripe.

**Bloom filters** (optional): Probabilistic filters per column per stripe. A Hive predicate `WHERE customer_id = 12345` checks the bloom filter first — if the filter says "not present," the entire stripe is skipped without reading data.

---

## Parquet Internal Structure

Parquet organizes data as **row groups** (default 128 MB) within a file:

```
Parquet File
├── Magic bytes ("PAR1")
├── Row Group 1
│   ├── Column Chunk: col_A
│   │   ├── Page 1 (data page, default 1 MB)
│   │   ├── Page 2
│   │   └── Dictionary Page (if dict encoding)
│   ├── Column Chunk: col_B
│   └── ...
├── Row Group 2
│   └── ...
└── File Footer
    ├── Row group metadata (offsets, sizes)
    ├── Column statistics (min, max, null count) per row group
    └── Schema (Parquet schema)
    Magic bytes ("PAR1")
```

**Key difference from ORC**: Parquet's statistics are at the row group level (coarser); ORC tracks stats at both stripe level and row group level, enabling finer-grained skipping.

---

## Compression Codecs

Both ORC and Parquet support multiple compression codecs. Compression is applied per column chunk / ORC stripe.

| Codec | Ratio | Speed | Use Case |
|---|---|---|---|
| **SNAPPY** | Moderate (1.5–2×) | Very fast | Default for Hadoop; balance of speed and savings |
| **ZSTD** | High (2–3×) | Fast | Parquet in Spark 3+; best ratio/speed tradeoff |
| **LZ4** | Low-moderate | Fastest | When CPU is the bottleneck |
| **GZIP / ZLIB** | High (2–4×) | Slow | Cold archival; rarely used for query hot paths |
| **BROTLI** | Very high | Very slow | Web assets; not commonly used in Hadoop |
| **NONE** | 1× | N/A | Already-compressed data (JPEG, etc.) |

```sql
-- ORC with Snappy (Hive)
CREATE TABLE events STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ORC with ZSTD
CREATE TABLE events STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZSTD', 'orc.compress.size'='262144');
```

```python
# Parquet with Snappy (PySpark)
df.write.option("compression", "snappy").parquet("/path/to/output")

# Parquet with ZSTD
df.write.option("compression", "zstd").parquet("/path/to/output")
```

---

## Predicate Pushdown Mechanics

Without predicate pushdown, the query engine reads all data into memory and then applies filters. With pushdown:

1. **File-level skipping**: Check file footer statistics — if `max(sale_date) < '2024-01'`, skip the entire file.
2. **Stripe/row-group skipping**: Check per-stripe statistics — if `min(amount) > 1000` and filter is `amount < 100`, skip the stripe.
3. **Bloom filter check (ORC)**: For equality predicates, consult bloom filter — skip stripes that definitely don't contain the value.
4. **Page-level skipping (Parquet)**: Parquet 1.11+ supports column index and offset index for page-level skipping within row groups.

```sql
-- Enable predicate pushdown in Hive
SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;

-- Enable in Spark
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.orc.filterPushdown", "true")
```

---

## ORC vs Parquet vs Avro: Quick Decision Guide

| Format | Best For | Avoid When |
|---|---|---|
| **ORC** | Hive analytics, ACID updates | Interop with non-Hadoop tools |
| **Parquet** | Spark, Presto/Trino, cross-tool analytics | Need row-level updates |
| **Avro** | Schema evolution, Kafka, row-oriented streaming | Analytical column-scan queries |
| **Delta / Iceberg** | ACID + analytics + schema evolution | Simple Hive-only workloads |
