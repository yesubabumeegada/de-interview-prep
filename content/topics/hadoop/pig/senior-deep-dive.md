---
title: "Pig - Senior Deep Dive"
topic: hadoop
subtopic: pig
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hadoop, pig, performance, tez, loadfunc, storefunc, debugging, spark-migration]
---

# Pig — Senior Deep Dive

## Performance Tuning

### Combiner Optimization

Pig automatically uses combiners for commutative, associative operations (SUM, COUNT, MIN, MAX) to reduce data transferred to reducers. You can influence this:

```pig
-- Pig auto-detects combiner-eligible aggregations
by_customer = GROUP orders BY customer_id;
summary = FOREACH by_customer GENERATE
  group AS customer_id,
  SUM(orders.amount) AS total,    -- combiner-eligible
  COUNT(orders) AS order_count;   -- combiner-eligible
-- Pig generates a combiner that partially aggregates in map phase

-- Verify with EXPLAIN
EXPLAIN -dot -out plan.dot summary;
-- Look for "CombinerOptimizer" in the logical plan
```

```bash
# Tune combiner memory
pig -Dpig.exec.combineSmallSplits=true \
    -Dpig.maxCombinedSplitSize=67108864 \
    myscript.pig
```

### Map-Side Joins (Advanced)

```pig
-- Skewed join: handles data skew where some keys have many more tuples
-- Pig splits the skewed partition across multiple reducers
skewed_result = JOIN large_orders BY customer_id, customers BY customer_id USING 'skewed';

-- Merge join: for pre-sorted inputs, avoids shuffle entirely
-- Both inputs must be sorted by the join key
sorted_orders = ORDER orders BY order_id;
sorted_items = ORDER order_items BY order_id;
merged = JOIN sorted_orders BY order_id, sorted_items BY order_id USING 'merge';
```

### Parallelism Control

```pig
-- Set number of reducers explicitly
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double);

-- Global default
SET default_parallel 50;

-- Per operation
by_customer = GROUP orders BY customer_id PARALLEL 50;
sorted = ORDER orders BY amount DESC PARALLEL 20;
```

### Memory and JVM Tuning

```bash
pig \
  -Dmapreduce.map.memory.mb=4096 \
  -Dmapreduce.reduce.memory.mb=8192 \
  -Dmapreduce.map.java.opts=-Xmx3686m \
  -Dmapreduce.reduce.java.opts=-Xmx7372m \
  -Dpig.cachedbag.memusage=0.4 \
  myscript.pig
```

## Pig with Tez Execution Engine

Tez replaces MapReduce with a DAG-based execution that avoids unnecessary disk I/O between stages:

```bash
# Run on Tez (Pig 0.14+)
pig -x tez myscript.pig

# Tez-specific tuning
pig -x tez \
  -Dtez.task.resource.memory.mb=4096 \
  -Dtez.am.resource.memory.mb=2048 \
  -Dtez.runtime.io.sort.mb=512 \
  myscript.pig
```

**Tez vs MapReduce for Pig:**

| Dimension | MapReduce | Tez |
|-----------|-----------|-----|
| Multi-stage joins | Write temp files to HDFS | Pipe between stages in memory |
| Startup overhead | Per-job YARN AM | Shared AM for entire script |
| Speed (typical) | Baseline | 2-5x faster |
| Debugging | Established tooling | Tez UI (newer) |

## Custom LoadFunc

Implement `LoadFunc` to read custom file formats:

```java
public class CustomCSVLoader extends LoadFunc {
    private RecordReader reader;
    private TupleFactory tf = TupleFactory.getInstance();
    private String delimiter;

    public CustomCSVLoader(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) return null;
            Text value = (Text) reader.getCurrentValue();
            String[] fields = value.toString().split(delimiter, -1);
            Tuple tuple = tf.newTuple(fields.length);
            for (int i = 0; i < fields.length; i++) {
                tuple.set(i, fields[i].isEmpty() ? null : fields[i]);
            }
            return tuple;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
```

```pig
REGISTER 'custom-loaders.jar';
data = LOAD '/data/custom' USING com.company.CustomCSVLoader('|');
```

## Custom StoreFunc

```java
public class CustomParquetStore extends StoreFunc {
    private ParquetWriter writer;

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new ParquetOutputFormat();
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        // Convert Pig tuple to Parquet record
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < tuple.size(); i++) {
            record.put(i, tuple.get(i));
        }
        writer.write(null, record);
    }
}
```

## Pig for Graph Processing

Pig can implement basic graph algorithms:

```pig
-- PageRank-like iterative computation
-- edges: (src, dst) representing directed graph

edges = LOAD '/data/graph/edges' USING PigStorage(',')
        AS (src:chararray, dst:chararray);

-- Compute out-degree
out_degree = FOREACH (GROUP edges BY src) GENERATE
  group AS node,
  COUNT(edges) AS degree;

-- Find nodes with in-degree > threshold (high-influence nodes)
in_degree = FOREACH (GROUP edges BY dst) GENERATE
  group AS node,
  COUNT(edges) AS in_links;

high_influence = FILTER in_degree BY in_links > 100;
DUMP high_influence;
```

## Debugging: ILLUSTRATE, EXPLAIN, and Logging

### ILLUSTRATE

```pig
-- ILLUSTRATE generates sample data to test your script logic
-- without running the full MapReduce job
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double, status:chararray);

completed = FILTER orders BY status == 'completed';
by_customer = GROUP completed BY customer_id;
totals = FOREACH by_customer GENERATE group, SUM(completed.amount);

ILLUSTRATE totals;
-- Output: sample data showing how each step transforms
```

### EXPLAIN

```pig
-- EXPLAIN shows logical, physical, and MR execution plans
EXPLAIN -script myscript.pig;

-- Output to DOT file for visualization
EXPLAIN -dot -out /tmp/plan.dot totals;
# dot -Tpng /tmp/plan.dot -o plan.png
```

### Logging

```bash
# Enable verbose logging
pig -logfile myjob.log -v myscript.pig

# Debug level
pig -Dpig.log.level=DEBUG myscript.pig

# Check YARN logs for failed tasks
yarn logs -applicationId application_1234567890_001 | grep -i error
```

## Pig vs Spark — When Pig Still Wins

| Scenario | Pig advantage | Spark equivalent |
|----------|--------------|-----------------|
| Quick schema-less exploration | `LOAD` without schema | Spark needs schema or inference |
| Unix pipeline integration | `STREAM` operator | Subprocess in Python executor |
| Existing Pig UDF library | Reuse Java/Python UDFs | Rewrite as Spark UDFs |
| Simple ETL without Scala/Python expertise | Pig Latin is simpler | Spark requires more expertise |
| Cluster without Spark | Native Hadoop tool | Requires Spark install |

**Modern reality (2024):** Pig is considered legacy. New projects use Spark. Migration is the standard path.

## Pig in Modern Data Stacks

```
Replaced by:
  Pig ETL scripts         →  Spark (PySpark/Scala)
  Pig Latin aggregations  →  Hive with ORC/Parquet
  Pig UDFs               →  Spark UDFs or dbt macros
  Pig + Oozie            →  Spark + Airflow
  Pig streaming          →  Spark Structured Streaming / Flink
```

## Interview Tips

> **Tip 1:** When asked about Pig performance, cover three areas: (1) join type selection (replicated for small tables, skewed for data skew, merge for pre-sorted data), (2) parallelism (`PARALLEL N` on GROUP/ORDER), (3) Tez execution engine which eliminates intermediate HDFS writes between stages.

> **Tip 2:** `ILLUSTRATE` is frequently underused but extremely valuable for debugging. It traces synthetic data through your entire script without running MapReduce, making it 100x faster than a test run for catching logic errors.

> **Tip 3:** Skewed joins are the correct answer when a join is slow and the data has hot keys (e.g., a few customers have 90% of orders). Pig's skewed join detects the hot keys and distributes their tuples across multiple reducers.

> **Tip 4:** When comparing Pig to Spark in an interview, don't say "Pig is useless." The nuanced answer: Pig is simpler for schema-less ETL tasks, has a gentler learning curve, and integrates natively with Hadoop. But Spark wins on performance, ecosystem, and active development. Migration is the right long-term choice.

> **Tip 5:** Custom `LoadFunc` is a senior-level topic. Key methods: `getInputFormat()` (what Hadoop input format to use), `prepareToRead()` (store the RecordReader), `getNext()` (convert one record to a Pig Tuple). Getting these three right is the minimum for a working custom loader.

## ⚡ Cheat Sheet

**HDFS architecture**
```
NameNode:   stores metadata (file → block mappings, permissions, namespace)
DataNode:   stores actual data blocks (default 128 MB per block)
Replication: default factor 3 (two local rack + one remote rack)
HA:         Active/Standby NameNode with JournalNodes for edit log sharing
```

**HDFS key commands**
```bash
hdfs dfs -ls /data/warehouse          # list files
hdfs dfs -put local.csv /data/raw/    # upload
hdfs dfs -get /data/output/ ./local/  # download
hdfs dfs -rm -r /data/tmp/            # delete
hdfs dfs -du -s -h /data/warehouse/   # disk usage
hdfs dfs -copyFromLocal -f src dst    # overwrite on upload
hdfs fsck /path -files -blocks        # check file health
```

**YARN resource model**
```
ResourceManager:  cluster master — allocates containers
NodeManager:      per-node agent — runs containers, reports health
ApplicationMaster: per-job — negotiates resources with RM
Container:        allocated unit (CPU cores + memory)

Scheduler types: FIFO, Capacity Scheduler (queues), Fair Scheduler
```

**Hive vs Spark SQL**
```
Hive:      MapReduce by default (slow); good for compatibility; HQL ≈ SQL
Hive LLAP: in-memory daemon; much faster (sub-minute queries)
Spark SQL:  Hive Metastore compatible but Spark execution — 10-100x faster
```

**Hive partitioning**
```sql
CREATE TABLE orders (order_id BIGINT, amount DOUBLE)
PARTITIONED BY (dt STRING, region STRING)
STORED AS PARQUET;
-- Dynamic partition insert
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO orders PARTITION (dt, region)
SELECT order_id, amount, dt, region FROM staging_orders;
```

**MapReduce pattern**
```
Map:    input splits → emit (key, value) pairs
Shuffle: sort + group by key across nodes
Reduce: aggregate values per key → output
Use case today: Hive compatibility, very large batch on older clusters
```

**ZooKeeper use cases in Hadoop**
```
HBase region assignment  — ZK tracks which RegionServer owns which region
HDFS NameNode HA         — ZK elects Active NameNode
YARN RM HA               — ZK elects Active ResourceManager
Kafka broker coordination — ZK stores broker/topic metadata (pre-KRaft)
```

**HBase data model**
```
Table → Row → Column Family → Column Qualifier → Value (versioned by timestamp)
Row key design is critical: avoid hot-spotting (don't use sequential IDs)
Strategies: salt prefix, reverse timestamp, MD5 hash of natural key
```

**Key interview points**
- HDFS is optimized for large files, sequential reads; terrible for many small files
- Sqoop: parallel JDBC import from RDBMS to HDFS/Hive (one mapper per table partition)
- Oozie: XML-based workflow scheduler (predecessor to Airflow in Hadoop ecosystem)
- Pig: dataflow language (Latin) — pre-dbt/Spark era; rarely used in modern stacks
- Ecosystem today: HDFS + YARN still used, but S3/GCS replacing HDFS in cloud-native stacks
