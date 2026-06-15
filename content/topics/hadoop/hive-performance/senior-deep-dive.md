---
title: "Hive Performance Tuning - Senior Deep Dive"
topic: hadoop
subtopic: hive-performance
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hadoop, hive, performance, tez, llap]
---

# Hive Performance Tuning — Senior Deep Dive

## LLAP: Live Long and Process

Hive LLAP (Live Long and Process) is a persistent, in-memory query execution layer that caches hot data and eliminates JVM start-up overhead for interactive workloads.

### Architecture

```
Client → HiveServer2 → LLAP Daemon (per node, long-running)
                            ↓
                     In-memory ORC cache
                            ↓
                     HDFS / S3 (cold data)
```

LLAP daemons are pre-warmed JVMs holding:
- **IO Cache**: ORC column data cached in off-heap memory
- **Executor threads**: Pre-started thread pool ready to process fragments
- **Fragment cache**: Reuse intermediate results across queries

### Configuration

```xml
<!-- hive-site.xml -->
<property>
  <name>hive.llap.execution.mode</name>
  <value>all</value>  <!-- all | none | auto | map -->
</property>
<property>
  <name>hive.llap.daemon.num.executors</name>
  <value>4</value>  <!-- threads per daemon = CPU cores - 1 -->
</property>
<property>
  <name>hive.llap.io.memory.size</name>
  <value>40Gb</value>  <!-- off-heap IO cache size -->
</property>
<property>
  <name>hive.llap.io.enabled</name>
  <value>true</value>
</property>
```

Start LLAP:
```bash
hive --service llap \
  --name llap0 \
  --instances 4 \
  --size 20480m \
  --xmx 2048m \
  --cache 10240m \
  --executors 4 \
  --iothreads 5
```

### LLAP vs Tez (when to use which)

| Scenario | Recommended |
|---|---|
| Interactive BI / dashboards (sub-second) | LLAP |
| Long-running ETL batches | Tez |
| Ad-hoc exploration on hot datasets | LLAP |
| First-time scans of cold data | Tez |

---

## Advanced Query Hints

Hive supports hints embedded in SQL comments to override the optimizer:

```sql
-- Force map join for a specific table
SELECT /*+ MAPJOIN(small_dim) */
       f.order_id, d.name
FROM   fact_orders f JOIN small_dim d ON f.dim_id = d.id;

-- Broadcast multiple tables
SELECT /*+ MAPJOIN(d1, d2) */
       f.*, d1.name, d2.region
FROM   fact_orders f
JOIN   dim_product d1 ON f.product_id = d1.id
JOIN   dim_region  d2 ON f.region_id  = d2.id;

-- Disable map join for a specific query (debugging)
SELECT /*+ NO_MAPJOIN */ f.*, d.name
FROM   fact_orders f JOIN large_dim d ON f.dim_id = d.id;

-- Streaming hint: stream the larger table in a reduce-side join
SELECT /*+ STREAMTABLE(f) */
       f.order_id, d.name
FROM   fact_orders f JOIN small_dim d ON f.dim_id = d.id;
```

### Tez-level hints

```sql
-- Override container memory for a single query
SET tez.am.resource.memory.mb=4096;
SET hive.tez.container.size=2048;
SET hive.tez.java.opts=-Xmx1800m;

-- Increase parallelism
SET tez.grouping.min-size=16777216;    -- 16 MB min split
SET tez.grouping.max-size=1073741824;  -- 1 GB max split
SET hive.exec.reducers.bytes.per.reducer=67108864;  -- 64 MB per reducer
```

---

## Tez DAG Tuning

### Reducer Count

```sql
-- Auto-set reducers based on input data size
SET hive.exec.reducers.bytes.per.reducer=67108864;
SET hive.exec.reducers.max=1009;

-- Or set explicitly
SET mapreduce.job.reduces=200;
```

### Tez Speculative Execution

Stragglers (slow tasks) extend job duration. Speculative execution launches duplicate tasks and uses the first to finish:

```sql
SET tez.am.speculation.enabled=true;
SET tez.task.max-events-per-heartbeat=500;
```

### Container Reuse

Avoid JVM start-up overhead by reusing containers across tasks:

```sql
SET tez.am.container.reuse.enabled=true;
SET tez.am.container.reuse.rack-fallback.enabled=true;
SET tez.am.container.idle.release-timeout-min.millis=5000;
SET tez.am.container.idle.release-timeout-max.millis=60000;
```

---

## Partition Pruning: Dynamic vs Static

**Static pruning**: Filter on literal partition value — evaluated at compile time.

**Dynamic partition pruning (DPP)**: Filter derived from a sub-query or join — evaluated at runtime by reading the dimension table first.

```sql
-- DPP: Hive evaluates the subquery and prunes fact_sales partitions at runtime
SELECT f.order_id, f.amount
FROM   fact_sales   f
JOIN   dim_campaign c ON f.campaign_id = c.campaign_id
WHERE  c.campaign_type = 'FLASH_SALE';
```

```sql
SET hive.tez.dynamic.partition.pruning=true;
SET hive.tez.dynamic.partition.pruning.max.event.size=1048576;
SET hive.tez.dynamic.partition.pruning.max.data.event.size=52428800;
```

---

## ORC Bloom Filters and Column Statistics

ORC files embed per-stripe statistics (min, max, sum, count) and optional bloom filters per column. Hive pushes predicates down to skip stripes at the file level — a second layer of pruning inside a partition.

```sql
-- Create ORC table with bloom filter on frequently filtered columns
CREATE TABLE orders (
  order_id    BIGINT,
  customer_id BIGINT,
  status      STRING,
  amount      DOUBLE
)
STORED AS ORC
TBLPROPERTIES (
  'orc.bloom.filter.columns' = 'customer_id,status',
  'orc.bloom.filter.fpp'     = '0.05',   -- 5% false positive rate
  'orc.compress'             = 'SNAPPY',
  'orc.stripe.size'          = '67108864',  -- 64 MB stripes
  'orc.row.index.stride'     = '10000'
);
```

With bloom filters on `customer_id`, a query for a single customer_id skips stripes where the bloom filter returns false, reading a tiny fraction of the file.

---

## Memory Tuning

```sql
-- Hive on Tez container memory
SET hive.tez.container.size=4096;        -- MB per container
SET hive.tez.java.opts=-Xmx3686m;       -- 90% of container size

-- Hash table memory for map joins
SET hive.mapjoin.followby.gby.localtask.max.memory.usage=0.55;
SET hive.mapjoin.localtask.max.memory.usage=0.90;

-- Query-level memory limit
SET hive.query.max.length=1000000;
SET hive.limit.pushdown.memory.usage=0.1;
```
