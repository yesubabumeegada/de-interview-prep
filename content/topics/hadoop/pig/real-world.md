---
title: "Pig - Real World"
topic: hadoop
subtopic: pig
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, pig, log-processing, etl, sessionization, spark-migration, clickstream]
---

# Pig — Real-World Patterns

## Log Processing Pipeline in Pig

Processing Apache web server logs is one of Pig's most common production use cases:

```pig
-- web_log_analysis.pig
-- Parse Apache Combined Log Format:
-- 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326

raw_logs = LOAD '/data/raw/weblogs/dt=2024-01-15'
           USING TextLoader() AS (line:chararray);

-- Extract fields using regex
parsed = FOREACH raw_logs GENERATE
  REGEX_EXTRACT(line, '^(\\S+)', 1) AS ip,
  REGEX_EXTRACT(line, '\\[(.*?)\\]', 1) AS timestamp_str,
  REGEX_EXTRACT(line, '"(GET|POST|PUT|DELETE) (\\S+)', 2) AS url,
  (int)REGEX_EXTRACT(line, '" (\\d{3}) ', 1) AS status_code,
  (long)REGEX_EXTRACT(line, '\\d{3} (\\d+)$', 1) AS bytes;

-- Filter out bots and health checks
real_traffic = FILTER parsed BY 
  ip IS NOT NULL 
  AND url IS NOT NULL 
  AND NOT REGEX_EXTRACT(url, '(healthcheck|robots\\.txt|favicon)', 0) MATCHES '.*';

-- Status code analysis
by_status = GROUP real_traffic BY status_code;
status_counts = FOREACH by_status GENERATE
  group AS status_code,
  COUNT(real_traffic) AS request_count;

-- Top URLs by traffic
by_url = GROUP real_traffic BY url;
url_stats = FOREACH by_url GENERATE
  group AS url,
  COUNT(real_traffic) AS hits,
  SUM(real_traffic.bytes) AS total_bytes,
  AVG(real_traffic.bytes) AS avg_bytes;

top_urls = ORDER url_stats BY hits DESC;
top_100 = LIMIT top_urls 100;

-- Error rate by IP (potential attackers)
errors = FILTER real_traffic BY status_code >= 400;
by_ip = GROUP errors BY ip;
ip_errors = FOREACH by_ip GENERATE
  group AS ip,
  COUNT(errors) AS error_count;

suspicious_ips = FILTER ip_errors BY error_count > 1000;

-- Store results
STORE status_counts INTO '/data/output/logs/status_counts/dt=2024-01-15' USING PigStorage('\t');
STORE top_100 INTO '/data/output/logs/top_urls/dt=2024-01-15' USING PigStorage('\t');
STORE suspicious_ips INTO '/data/output/logs/suspicious_ips/dt=2024-01-15' USING PigStorage('\t');
```

## ETL for Clickstream Data

```pig
-- clickstream_etl.pig
-- Raw clickstream: user_id, session_id, event_type, url, timestamp, device_type

clicks = LOAD '/data/raw/clickstream/dt=2024-01-15'
         USING PigStorage('\t')
         AS (user_id:chararray, session_id:chararray, event_type:chararray,
             url:chararray, ts:long, device_type:chararray);

-- Data quality: remove rows with null user_id or timestamp
clean_clicks = FILTER clicks BY 
  user_id IS NOT NULL 
  AND ts IS NOT NULL
  AND ts > 0
  AND event_type IS NOT NULL;

-- Enrich with derived fields
enriched = FOREACH clean_clicks GENERATE
  user_id,
  session_id,
  event_type,
  url,
  ts,
  device_type,
  REGEX_EXTRACT(url, 'category=([^&]+)', 1) AS category,
  REGEX_EXTRACT(url, 'product_id=(\\d+)', 1) AS product_id,
  (ts / 3600000L) AS hour_bucket;    -- bucket events by hour

-- Compute per-session event counts
by_session = GROUP enriched BY session_id;
session_stats = FOREACH by_session GENERATE
  group AS session_id,
  FLATTEN(enriched.(user_id, device_type)) AS (user_id, device_type),
  COUNT(enriched) AS event_count,
  MIN(enriched.ts) AS session_start,
  MAX(enriched.ts) AS session_end,
  MAX(enriched.ts) - MIN(enriched.ts) AS session_duration_ms,
  COUNT(FILTER enriched BY event_type == 'page_view') AS page_views,
  COUNT(FILTER enriched BY event_type == 'click') AS clicks;

STORE session_stats INTO '/data/output/clickstream/sessions/dt=2024-01-15'
      USING PigStorage('\t');
```

## Pig for Sessionization of Web Logs

Sessionization groups user events into sessions (gap > 30 minutes = new session):

```pig
-- sessionization.pig
-- Input: user_id, event_type, timestamp (sorted by user_id, timestamp)

events = LOAD '/data/raw/events'
         USING PigStorage(',')
         AS (user_id:chararray, event_type:chararray, ts:long);

-- Sort by user and timestamp (required for sessionization)
sorted = ORDER events BY user_id ASC, ts ASC;

-- Group all events per user
by_user = GROUP sorted BY user_id;

-- Apply sessionization UDF
DEFINE Sessionize com.company.SessionizeUDF('1800000');  -- 30 min gap

sessionized = FOREACH by_user {
  user_events = sorted;
  GENERATE group AS user_id, FLATTEN(Sessionize(user_events)) AS (session_id:chararray, ts:long, event_type:chararray);
}

-- Aggregate session-level stats
by_session = GROUP sessionized BY (user_id, session_id);
session_summary = FOREACH by_session GENERATE
  FLATTEN(group) AS (user_id, session_id),
  COUNT(sessionized) AS event_count,
  MIN(sessionized.ts) AS start_ts,
  MAX(sessionized.ts) AS end_ts;

STORE session_summary INTO '/data/output/sessions';
```

```java
// SessionizeUDF.java
public class SessionizeUDF extends EvalFunc<DataBag> {
    private long sessionGapMs;

    public SessionizeUDF(String gapMs) {
        this.sessionGapMs = Long.parseLong(gapMs);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag events = (DataBag) input.get(0);
        DataBag result = BagFactory.getInstance().newDefaultBag();
        long lastTs = -1;
        int sessionId = 0;

        for (Tuple event : events) {
            long ts = (Long) event.get(1);  // timestamp field
            if (lastTs >= 0 && (ts - lastTs) > sessionGapMs) {
                sessionId++;
            }
            Tuple out = TupleFactory.getInstance().newTuple(3);
            out.set(0, "session_" + sessionId);
            out.set(1, ts);
            out.set(2, event.get(0));  // event_type
            result.add(out);
            lastTs = ts;
        }
        return result;
    }
}
```

## Migrating Pig Scripts to Spark

A systematic mapping from Pig Latin to PySpark:

| Pig Latin | PySpark Equivalent |
|-----------|-------------------|
| `LOAD ... USING PigStorage` | `spark.read.csv(...)` |
| `FILTER ... BY condition` | `.filter(condition)` |
| `FOREACH ... GENERATE` | `.select(...)` or `.withColumn(...)` |
| `GROUP ... BY key` | `.groupBy(key)` |
| `JOIN ... BY key` | `.join(...)` |
| `COGROUP ... BY key` | `.groupBy().agg(collect_list())` |
| `ORDER ... BY col` | `.orderBy(col)` |
| `LIMIT n` | `.limit(n)` |
| `DISTINCT` | `.distinct()` |
| `SPLIT ... INTO` | Multiple `.filter()` calls |
| `STORE ... INTO` | `.write.parquet(...)` |
| `UDF` | `spark.udf.register(...)` |

```python
# Pig script converted to PySpark:
# orders = LOAD ... GROUP BY customer_id; FOREACH ... GENERATE SUM(amount)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders_migration").getOrCreate()

# LOAD equivalent
orders = spark.read.csv("/data/raw/orders", header=False, inferSchema=True) \
    .toDF("order_id", "customer_id", "amount", "status", "order_date")

# FILTER equivalent
completed = orders.filter(F.col("status") == "completed")

# GROUP + FOREACH equivalent
customer_totals = completed.groupBy("customer_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount"),
    F.max("amount").alias("max_amount")
)

# ORDER equivalent
sorted_totals = customer_totals.orderBy(F.desc("total_amount"))

# STORE equivalent
sorted_totals.write.mode("overwrite").parquet("/data/output/customer_totals")
```

## Production Pig Script with Error Handling and Monitoring

```bash
#!/bin/bash
# run_pig_job.sh

DATE=$1
SCRIPT="log_analysis.pig"
LOG_DIR="/var/log/pig"
mkdir -p $LOG_DIR

# Run Pig job with monitoring
START_TIME=$(date +%s)

pig -x tez \
    -param date="${DATE}" \
    -param input="/data/raw/weblogs/dt=${DATE}" \
    -param output="/data/output/logs/dt=${DATE}" \
    -logfile "${LOG_DIR}/pig_${DATE}.log" \
    "${SCRIPT}"

EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [ $EXIT_CODE -eq 0 ]; then
  echo "Pig job succeeded in ${DURATION}s"
  # Count output records for monitoring
  RECORD_COUNT=$(hdfs dfs -cat "/data/output/logs/dt=${DATE}/*" | wc -l)
  echo "Output records: ${RECORD_COUNT}"

  # Write success marker
  hdfs dfs -touchz "/data/output/logs/dt=${DATE}/_SUCCESS"

  # Send metrics to monitoring system
  curl -X POST "http://metrics-api/ingest" \
    -d "{\"job\":\"pig_log_analysis\",\"date\":\"${DATE}\",\"duration\":${DURATION},\"records\":${RECORD_COUNT},\"status\":\"success\"}"
else
  echo "Pig job FAILED after ${DURATION}s (exit code: ${EXIT_CODE})"
  cat "${LOG_DIR}/pig_${DATE}.log" | grep -i "error" | tail -20
  # Alert
  curl -X POST "http://pagerduty-api/trigger" \
    -d "{\"job\":\"pig_log_analysis\",\"date\":\"${DATE}\",\"status\":\"failed\"}"
  exit 1
fi
```

## Interview Tips

> **Tip 1:** Sessionization is one of the most common Pig interview problems. The key insight: you must `ORDER BY user_id, timestamp` before grouping, then apply a UDF inside `FOREACH ... GENERATE` to walk through the sorted bag and assign session IDs based on time gaps.

> **Tip 2:** The Pig-to-Spark migration pattern is frequently discussed. Map each Pig operator to its PySpark equivalent before starting. The trickiest mapping is `COGROUP` → PySpark `groupBy().agg(collect_list())`, which preserves the bag semantics.

> **Tip 3:** `FLATTEN` inside `FOREACH` is how you "unnest" a bag from a `GROUP` operation. If you forget `FLATTEN`, you get bags-within-bags. In the migration to Spark, `FLATTEN` on a grouped bag corresponds to `explode()` in PySpark.

> **Tip 4:** Log processing is where Pig shines: `TextLoader` + `REGEX_EXTRACT` is extremely clean for parsing variable-format text that would require complex schemas in Hive. This is a valid argument for keeping some Pig scripts when migrating.

> **Tip 5:** When presenting production Pig scripts, always mention: (1) `_SUCCESS` file creation after `STORE` so downstream coordinators know the job is done, (2) record count validation after writing, (3) Tez execution for performance. These show operational maturity.
