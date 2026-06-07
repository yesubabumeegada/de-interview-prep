---
title: "MapReduce - Intermediate"
topic: hadoop
subtopic: mapreduce
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, mapreduce, joins, secondary-sort, counters, distributed-cache]
---

# MapReduce Intermediate Concepts

## Joins in MapReduce

### Reduce-Side Join (Most Common)
Both datasets are read by mappers; join happens at reducers after shuffle.

```java
// Join two datasets: orders.txt and customers.txt
// orders: order_id, customer_id, amount
// customers: customer_id, name, email

public class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

        if (filename.startsWith("orders")) {
            // Tag with source: "O" prefix
            String customerId = fields[1];
            context.write(new Text(customerId), new Text("O:" + value.toString()));
        } else if (filename.startsWith("customers")) {
            // Tag with source: "C" prefix
            String customerId = fields[0];
            context.write(new Text(customerId), new Text("C:" + value.toString()));
        }
    }
}

public class ReduceSideJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        List<String> orders = new ArrayList<>();
        String customerInfo = null;

        for (Text val : values) {
            String v = val.toString();
            if (v.startsWith("O:")) {
                orders.add(v.substring(2));
            } else if (v.startsWith("C:")) {
                customerInfo = v.substring(2);
            }
        }

        // Join: emit one record per order with customer info
        if (customerInfo != null) {
            for (String order : orders) {
                context.write(key, new Text(order + "," + customerInfo));
            }
        }
    }
}
```

### Map-Side Join (Replicated Join / Broadcast Join)
One dataset is small enough to fit in memory; broadcast to all mappers.

```java
// Setup: load small "customers" dataset into memory
public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> customerMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Load distributed cache file (customers.csv)
        URI[] cacheFiles = context.getCacheFiles();
        Path customerFile = new Path(cacheFiles[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(fs.open(customerFile)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            customerMap.put(fields[0], fields[1] + "," + fields[2]); // id → name,email
        }
        reader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String customerId = fields[1];
        String customerInfo = customerMap.get(customerId);
        if (customerInfo != null) {
            context.write(new Text(fields[0]), new Text(value + "," + customerInfo));
        }
    }
}

// In driver:
job.addCacheFile(new URI("/user/data/customers.csv#customers.csv"));
```

### Map-Side Join vs Reduce-Side Join

| Aspect | Map-Side Join | Reduce-Side Join |
|--------|--------------|-----------------|
| Requirement | One table must fit in memory | No size constraint |
| Shuffle | None (no data sent to reducer) | Heavy network shuffle |
| Speed | Very fast | Slower (shuffle bottleneck) |
| Scalability | Limited by mapper heap | Scales to any size |
| When to use | Dimension table joins | Large fact-to-fact joins |

## Secondary Sort

Problem: MapReduce sorts by key, but values within a key are **not sorted**.

**Use case**: For each stock symbol, get prices in chronological order.

```java
// Composite key: (symbol, date)
public class StockKey implements WritableComparable<StockKey> {
    private String symbol;
    private long date;

    @Override
    public int compareTo(StockKey other) {
        int cmp = this.symbol.compareTo(other.symbol);
        if (cmp != 0) return cmp;
        return Long.compare(this.date, other.date); // Sort by date within symbol
    }

    // writeTo, readFields, hashCode, equals implementations...
}

// Grouping Comparator: tells reducer when to start a new reduce() call
// Group by symbol only (ignore date part for grouping)
public class StockGroupingComparator extends WritableComparator {
    protected StockGroupingComparator() {
        super(StockKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StockKey k1 = (StockKey) a;
        StockKey k2 = (StockKey) b;
        return k1.getSymbol().compareTo(k2.getSymbol()); // Group by symbol
    }
}

// In driver:
job.setSortComparatorClass(StockKey.Comparator.class);
job.setGroupingComparatorClass(StockGroupingComparator.class);
```

## Counters

Counters track job-level statistics without writing to files:

```java
// Define custom counter enum
public enum MyCounters {
    RECORDS_PROCESSED,
    NULL_VALUES_SKIPPED,
    MALFORMED_RECORDS
}

// Use in Mapper
public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
    context.getCounter(MyCounters.RECORDS_PROCESSED).increment(1);

    if (value.toString().contains("null")) {
        context.getCounter(MyCounters.NULL_VALUES_SKIPPED).increment(1);
        return;
    }

    String[] fields = value.toString().split(",");
    if (fields.length < 5) {
        context.getCounter(MyCounters.MALFORMED_RECORDS).increment(1);
        context.getCounter("DataQuality", "missing_fields").increment(1);
        return;
    }

    // Process valid record...
}
```

```bash
# Read counters from job output
mapred job -status job_12345_0001 | grep -A20 "Counters:"

# Or from history server
mapred history -all -output /tmp/job-history/
```

## Distributed Cache

Share read-only files with all tasks:

```java
// In driver
job.addCacheFile(new URI("hdfs:///user/data/lookup_table.csv#lookup.csv"));
job.addCacheArchive(new URI("hdfs:///user/data/mylib.tar.gz#mylib"));

// In mapper setup()
File lookupFile = new File("lookup.csv");  // symlinked in task working dir
```

```bash
# Add files via command line
hadoop jar myapp.jar MyJob \
  -files /local/config.properties,/local/lookup.csv \
  -libjars /local/mylib.jar \
  -archives /local/data.tar.gz \
  /input /output
```

## Output Formats

```java
// Multiple outputs from one job
MultipleOutputs<Text, IntWritable> mos = new MultipleOutputs<>(context);

// In mapper/reducer:
if (value > threshold) {
    mos.write("highValue", key, value, "high/part");
} else {
    mos.write("lowValue", key, value, "low/part");
}

// Lazy output (avoid empty output files)
job.setOutputFormatClass(LazyOutputFormat.class);
LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

// Sequence file output (binary, splittable)
job.setOutputFormatClass(SequenceFileOutputFormat.class);
SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
```

## Compression in MapReduce

```java
// Compress map output (reduces shuffle I/O dramatically)
conf.setBoolean("mapreduce.map.output.compress", true);
conf.setClass("mapreduce.map.output.compress.codec",
    SnappyCodec.class, CompressionCodec.class);

// Compress final output
FileOutputFormat.setCompressOutput(job, true);
FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

// For splittable output (required for parallel reads downstream):
FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
// or use LZO with indexing
```

| Codec | Compression Ratio | Speed | Splittable |
|-------|------------------|-------|------------|
| Gzip | High | Medium | No |
| Snappy | Medium | Very fast | No |
| LZO | Medium | Fast | Yes (with index) |
| Bzip2 | Very high | Slow | Yes |
| Zstandard | High | Fast | No |

## Speculative Execution

Slow "straggler" tasks can delay an entire job. Speculative execution launches duplicate tasks and uses whichever finishes first:

```xml
<property>
  <name>mapreduce.map.speculative</name>
  <value>true</value>
</property>
<property>
  <name>mapreduce.reduce.speculative</name>
  <value>true</value>
</property>
```

**Disable speculative execution when:**
- Tasks write to external databases (duplicate writes)
- Tasks are not idempotent
- Tasks use exclusive resources (file locks)

## Task Memory Tuning

```xml
<!-- Each map/reduce runs in a YARN container -->
<property>
  <name>mapreduce.map.memory.mb</name>
  <value>2048</value>  <!-- Container memory -->
</property>
<property>
  <name>mapreduce.map.java.opts</name>
  <value>-Xmx1638m</value>  <!-- JVM heap = 80% of container -->
</property>
<property>
  <name>mapreduce.reduce.memory.mb</name>
  <value>4096</value>
</property>
<property>
  <name>mapreduce.reduce.java.opts</name>
  <value>-Xmx3276m</value>
</property>

<!-- Sort buffer (for map output sorting) -->
<property>
  <name>mapreduce.task.io.sort.mb</name>
  <value>256</value>  <!-- Default 100 MB; increase for large map output -->
</property>
<property>
  <name>mapreduce.map.sort.spill.percent</name>
  <value>0.8</value>  <!-- Spill to disk at 80% full -->
</property>
```

## Chain Jobs with Tool Runner

```java
// Chain multiple MR jobs
public class PipelineDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        // Job 1: Preprocessing
        Job job1 = Job.getInstance(getConf(), "preprocess");
        // ... configure job1 ...
        if (!job1.waitForCompletion(true)) return 1;

        // Job 2: Aggregation (reads job1 output)
        Job job2 = Job.getInstance(getConf(), "aggregate");
        FileInputFormat.addInputPath(job2, new Path("/tmp/preprocess_output"));
        // ... configure job2 ...
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PipelineDriver(), args));
    }
}
```

## Interview Tips

> **Tip 1:** Know when NOT to use Combiner: average (sum/count separately), median, any non-commutative/non-associative aggregation. A common trick question is "can you use a Combiner for computing the average?" — answer is no directly, but you can combine (sum, count) pairs and compute average in the reducer.

> **Tip 2:** For reduce-side joins, always discuss skew: if one customer_id has millions of orders, that one reducer becomes a bottleneck. Solutions: salting the key, using a skew join (sample, split hot keys), or switching to map-side join for the hot keys.

> **Tip 3:** Secondary sort is a classic interview topic. The key insight is: sort comparator uses the composite key (symbol + date), but grouping comparator uses only the symbol. This separates sorting behavior from grouping behavior.

> **Tip 4:** When discussing compression, interviewers love the splittable question. Gzip and Snappy are NOT splittable — if you compress a large output file with Gzip, downstream MapReduce can't parallelize reading it (one map task reads the whole file). Use Bzip2 or sequence files for splittable output.

> **Tip 5:** Memory tuning matters in production. The rule of thumb is JVM heap = 75-80% of container memory (leave room for off-heap: I/O buffers, OS). Mention that setting `-Xmx` equal to container memory causes container kills (YARN kills the container if it exceeds memory limit).
