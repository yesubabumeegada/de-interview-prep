---
title: "Kafka Consumers - Scenario Questions"
topic: kafka
subtopic: consumers
content_type: scenario_question
tags: [kafka, consumers, interview, scenarios]
---

# Scenario Questions — Kafka Consumers

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Consumer Lag Is Growing

**Scenario:** Your consumer group reads from a 12-partition topic. Consumer lag is growing steadily (falling further behind producers). Currently 6 consumers in the group, each processing ~5K records/second. Producer rate: 50K records/second. Diagnose and fix.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**
- Producer rate: 50K/sec
- Consumer capacity: 6 consumers × 5K/sec = 30K/sec
- Gap: 50K - 30K = 20K/sec falling behind

**Fix 1: Add more consumers (up to partition count)**
```bash
# Current: 6 consumers, 12 partitions → each consumer handles 2 partitions
# Add 6 more consumers: 12 consumers, 12 partitions → each handles 1 partition
# New capacity: 12 × 5K = 60K/sec > 50K/sec ✓
```

**Fix 2: Optimize per-consumer throughput**
```python
consumer = KafkaConsumer(
    max_poll_records=1000,   # Process more per poll (was 500)
    fetch_min_bytes=1048576, # Wait for 1 MB before returning (reduce poll overhead)
    fetch_max_wait_ms=500,   # Max 500ms wait
)

# Use batch processing instead of record-by-record
messages = consumer.poll(timeout_ms=1000, max_records=1000)
# Process entire batch at once (bulk DB insert vs 1000 individual inserts)
bulk_insert(messages)  # Much faster than per-record insert
consumer.commit()
```

**Fix 3: If 12 partitions is the bottleneck (can't add more consumers)**
```bash
# Increase partition count to allow more consumers
kafka-topics.sh --alter --topic orders --partitions 24
# Now can scale to 24 consumers
```

**Summary decision tree:**
1. Can you add consumers? (consumers < partitions) → add consumers
2. Can you optimize per-record processing? → batch, reduce I/O calls
3. Need more than partition-count consumers? → increase partitions (irreversible!)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Exactly-Once S3 Sink Consumer

**Scenario:** Design a consumer that reads from Kafka and writes to S3 with exactly-once semantics. Records must not be lost AND must not be duplicated in S3, even across consumer restarts and rebalances.

<details>
<summary>✅ Solution</summary>

**Challenge:** Kafka commit and S3 write are NOT atomic. If you commit then crash before S3 write: data lost. If you S3 write then crash before commit: data duplicated on restart.

**Solution: Offset embedded in output path (idempotent by design)**

```python
from kafka import KafkaConsumer, TopicPartition
import json, io, boto3

s3 = boto3.client('s3')
consumer = KafkaConsumer(
    'events',
    group_id='s3-writer',
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

def write_batch_to_s3(partition, start_offset, end_offset, records):
    """Write records to a deterministic S3 path based on offset range."""
    # Key insight: path includes offset → re-writing the same offsets overwrites
    # (same data), making it idempotent
    key = f"data/events/partition={partition}/offsets={start_offset}-{end_offset}.parquet"
    
    # Convert to Parquet and upload
    df = pd.DataFrame(records)
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    
    s3.put_object(Bucket='data-lake', Key=key, Body=buffer.getvalue())
    return key

# Processing loop
BATCH_SIZE = 10000
batch = []
batch_start_offset = None

for message in consumer:
    if batch_start_offset is None:
        batch_start_offset = message.offset
    
    batch.append(json.loads(message.value))
    
    if len(batch) >= BATCH_SIZE:
        # Write to S3 (idempotent: same offsets → same path → overwrite)
        write_batch_to_s3(
            partition=message.partition,
            start_offset=batch_start_offset,
            end_offset=message.offset,
            records=batch
        )
        
        # Commit AFTER successful S3 write
        consumer.commit()
        
        batch = []
        batch_start_offset = None

# What happens on crash and restart:
# 1. Consumer restarts from last committed offset
# 2. Re-reads the same records (since commit didn't happen or matches S3 write)
# 3. Re-writes to S3 with same offset-based path → OVERWRITES same file
# 4. Result: no duplicates (same data written to same path)
# 5. No data loss (uncommitted offsets are re-read)
```

**Why this is exactly-once:**
- If crash BEFORE S3 write: restart re-reads offsets, writes to S3, commits. No loss.
- If crash AFTER S3 write but BEFORE commit: restart re-reads same offsets, writes same data to same S3 path (overwrite). No duplicates.
- If crash AFTER commit: next batch starts from new offsets. Previous batch is safe in S3.

**The key insight:** Embedding offsets in the S3 path makes the write **idempotent** — writing the same data to the same path multiple times has the same result as writing once.

</details>

</article>
