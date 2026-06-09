---
title: "Kinesis Advanced — Scenarios"
topic: real-time-streaming
subtopic: kinesis-advanced
content_type: scenario_question
tags: [kinesis, aws, interview, scenarios, firehose, kcl, resharding, exactly-once]
---

# Kinesis Advanced — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Real-Time Ad Click Attribution System

**Scenario:** A digital advertising company needs to attribute ad clicks to conversions (purchases). Events flow: ad impressions, ad clicks, and purchase events. You have 500K events/sec total. Design a Kinesis-based solution that can attribute a purchase to the last ad click within 24 hours.

<details>
<summary>💡 Hint</summary>
Think about: how to join ad clicks to purchases within a 24-hour window. Consider KDS for high throughput, Lambda or KDA for processing, DynamoDB for stateful click tracking, and idempotency for exactly-once attribution.
</details>

<details>
<summary>✅ Solution</summary>

```
Architecture:

  Ad Servers → Kinesis (impressions, 20 shards)
  User Browsers → Kinesis (clicks, 10 shards)
  E-commerce → Kinesis (purchases, 5 shards)
  
  Processing:
  Clicks + Purchases → KDA (Flink) → Attribution Result → Kinesis (attributed-purchases)
                                                         → Firehose → S3 → Redshift

Shard sizing:
  Impressions: 500K × 200 bytes / MB = 100 MB/sec → 100 shards (but impressions not needed for attribution)
  Clicks:      50K × 200 bytes = 10 MB/sec → 10 shards
  Purchases:   5K × 300 bytes = 1.5 MB/sec → 2 shards
  Reduce: use Firehose for impressions (archive only, no real-time join needed)

Attribution logic in KDA (Flink):

  // KDA Flink job: last-click attribution
  
  DataStream<Click> clicks = env.addSource(new FlinkKinesisConsumer<>("clicks", ...))
      .withWatermark("event_time", "5 minutes");
  
  DataStream<Purchase> purchases = env.addSource(new FlinkKinesisConsumer<>("purchases", ...))
      .withWatermark("purchase_time", "5 minutes");
  
  // State per user: last N clicks (within 24 hours)
  DataStream<Attribution> attributed = purchases
      .keyBy(Purchase::getUserId)
      .connect(clicks.keyBy(Click::getUserId))
      .process(new LastClickAttributor());
  
  // LastClickAttributor:
  //   Maintains list of clicks per user in MapState (click_time → click_id + ad_id)
  //   When purchase arrives: find most recent click within 24 hours
  //   Emit Attribution(purchase_id, click_id, ad_id, revenue)
  //   Timer: expire clicks older than 24 hours (TTL)

State management:
  State per user: ~10 clicks × 100 bytes = 1 KB/user
  1M active users × 1 KB = 1 GB state → EmbeddedRocksDB in KDA
  State TTL: 24 hours (expire old clicks to bound state size)

Output:
  Kinesis attributed-purchases → Firehose → S3 (Parquet, partitioned by date)
  Redshift Spectrum queries S3 for attribution reports
  Real-time: attributed-purchases → Lambda → DynamoDB (campaign spend tracking)

Exactly-once:
  KDA (Flink): exactly-once via checkpointing (FlinkKinesisConsumer tracks offsets)
  Firehose: at-least-once (idempotent writes to S3 via same key/path)
  DynamoDB: upsert by attribution_id (idempotent)

Latency: < 5 seconds (click → attribution → DynamoDB update)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Kinesis Consumer Falling Behind After Traffic Spike

**Scenario:** Your KCL consumer application processes 2 million events/hour normally. After a product launch, traffic spiked to 20 million events/hour for 4 hours. Your CloudWatch shows GetRecords.IteratorAgeMilliseconds reached 8 hours. Now, 2 hours after the spike, traffic is back to normal but you have 8 hours of backlog. How do you recover?

<details>
<summary>💡 Hint</summary>
Calculate your recovery rate vs production rate. If processing rate > production rate, lag will drain. Consider: add shards (resharding), add Lambda concurrency (parallelization factor), or let it drain naturally. Communicate an ETA based on math.
</details>

<details>
<summary>✅ Solution</summary>

```
Immediate diagnosis:
  Current shard count: 10 (handles 10 MB/sec = 2M events/hour at 1.4KB avg)
  Spike: 20M events/hour = 10× normal
  Shards during spike: 10 (should have been ~100)
  Backlog: 8 hours × 2M events = 16M events accumulated

Step 1: Verify backlog size
  CloudWatch: GetRecords.IteratorAgeMilliseconds = 28,800,000 ms = 8 hours
  Kinesis retention: 24 hours (check: backlog of 8 hours fits within retention)
  ✓ No data loss: 8h backlog << 24h retention

Step 2: Scale up shards immediately (even though spike is over)
  current shards: 10
  To process backlog + current at 2× speed: need 20 shards (2× capacity)
  
  kinesis.update_shard_count(
      StreamName='product-events',
      TargetShardCount=20,
      ScalingType='UNIFORM_SCALING'
  )
  
  Important: existing KCL workers automatically pick up new shards
  KCL distributes leases: if 20 shards, 4 KCL workers → 5 shards each

Step 3: Scale KCL workers (if 4 workers can't keep up)
  Current: 4 EC2 workers, each handling 2.5 shards (after resharding: 5 shards each)
  Target: 20 workers × 1 shard = maximum throughput
  Deploy: auto-scaling group → scale out to 20 workers
  KCL: automatically distributes leases to new workers

Step 4: Monitor recovery progress
  Expected backlog burn-down rate: 2× capacity → burn 1 hour of backlog per hour
  8 hour backlog → 8 hours to clear (while processing current 2M events/hour)
  
  Monitor: IteratorAgeMilliseconds decreasing (from 8h → 0h)
  Alert: if age NOT decreasing → workers not scaling fast enough → add more shards/workers

Step 5: Scale back down after backlog cleared
  After IteratorAgeMilliseconds < 60 seconds:
  Scale shards back to 10 (merge adjacent shards)
  Scale workers back to 4
  
  Note: scale-down is slower than scale-up (Kinesis merge limit: 500 per 24 hours)
  Cost of extra 10 shards for 8 hours: 10 × $0.015/hour × 8 = $1.20 (trivial)

Prevention:
  Implement auto-scaler Lambda (as shown in real-world patterns)
  Alert threshold: IteratorAgeMilliseconds > 30 seconds → scale up
  Pre-scale before known events (product launches): scale to 2× expected capacity
  CloudWatch alarm: WriteProvisionedThroughputExceeded → immediate scale-up trigger
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Migrating from SQS to Kinesis

**Scenario:** Your team uses SQS for a clickstream pipeline: 10 Lambda → SQS → 3 Lambda consumers. You want to add a second consumer (analytics) and keep the existing one (processing). Also, you want to retain events for 7 days for replay. Why can't SQS handle this, and how would you redesign with Kinesis?

<details>
<summary>💡 Hint</summary>
SQS fundamentals that prevent this use case: messages consumed-and-deleted (no replay), one consumer per message (no fan-out), no offset-based consumption. Kinesis solves all three. Design the migration in shadow-run mode.
</details>

<details>
<summary>✅ Solution</summary>

```
SQS limitations for this use case:
  1. Message deleted after consumption: once a consumer reads and deletes, gone
     → Multiple consumers CANNOT read the same message independently
  2. Retention max: 14 days (KDS: up to 365 days)
  3. No ordering guarantee (FIFO SQS: ordering per message group only, expensive)
  4. No replay: once consumed and deleted, message is gone forever
  5. Fan-out: SNS → multiple SQS queues (workaround), but adds complexity and cost

Kinesis advantages:
  1. Multiple independent consumers: each reads at its own pace (EFO for isolation)
  2. Retention: 7 days configurable (default 24h, extend to 7d via API)
  3. Replay: restart consumer from any sequence number within retention window
  4. Ordering: guaranteed per shard (per partition key)

Migration design:

  BEFORE (SQS):
    10 producers → SQS queue → 3 Lambda (processing consumers, competing)
                  × (second consumer impossible without duplication)

  AFTER (Kinesis):
    10 producers → Kinesis Data Streams (5 shards based on click volume)
                 ├── Lambda consumer 1 (EFO): existing processing (real-time, EFO)
                 ├── Lambda consumer 2 (EFO): new analytics consumer
                 └── Firehose: 7-day archive to S3 (for replay capability)

Producer changes:
  Replace: sqs.send_message() → kinesis.put_records()
  Partition key: user_id (groups clicks per user → same shard → ordering)
  Code change: 5 lines of code

Consumer 1 changes (existing Lambda):
  Add EFO consumer registration
  Replace SQS trigger with Kinesis event source mapping (EFO)
  Logic: unchanged (records structure similar)

Consumer 2 (new analytics Lambda):
  Register separate EFO consumer
  Separate Lambda function
  Processes independently (doesn't affect Consumer 1)

Replay capability:
  S3 archive via Firehose → replay from S3 using Spark/Athena
  OR: restart Kinesis consumer with AT_TIMESTAMP starting position
  Example: replay last 7 days of clicks to recalculate attribution after bug fix

Migration steps:
  Week 1: Set up Kinesis stream + Firehose, test with synthetic data
  Week 2: Deploy new producer (dual-write: SQS + Kinesis simultaneously)
  Week 3: Deploy Consumer 1 on Kinesis (validate parity with SQS consumer)
  Week 4: Deploy Consumer 2 on Kinesis, disable SQS pipeline, decommission SQS

Cost comparison:
  SQS (current): 5M messages/day × $0.40/million = $2/day
  Kinesis: 5 shards × $0.015/hour × 24 + data charges ≈ $1.8/day + $0.014/GB
  EFO: 2 consumers × 5 shards × $0.015/hour × 24 ≈ $3.6/day extra
  Total Kinesis: ~$5-6/day (higher, but enables multi-consumer, ordering, replay)
  Business value: replay = prevents $10K+ ad spend misattribution on bug → ROI positive
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "What's the maximum number of consumers you should have on a single Kinesis shard, and why?" — Without Enhanced Fan-Out: 2 MB/sec per shard shared across ALL consumers. With 5 consumers, each gets ~400 KB/sec (may not be enough for real-time). Also, GetRecords API is limited to 5 calls/sec per shard — with 5 consumers polling every 200ms, you hit the limit. With Enhanced Fan-Out: each consumer gets dedicated 2 MB/sec, push-based (no polling). Practical limit: 5 registered EFO consumers per shard (soft limit, can be raised). For more than 5 consumers: fan-out via SNS → multiple Kinesis streams, or use Kafka/MSK which has no such limit.

> **Tip 2:** "How do you handle a Kinesis stream that consistently hits write throttling?" — WriteProvisionedThroughputExceeded means producers are hitting 1 MB/sec or 1,000 records/sec per shard. Solutions: (a) split the hot shard (increase total capacity); (b) use KPL aggregation to pack multiple records per Kinesis record (reduces records/sec); (c) fix hot partition key — if one key generates 80% of traffic, spread writes by appending random suffix `key-{random.randint(0,9)}`; (d) implement producer-side retry with exponential backoff (PutRecords returns per-record success/failure). Monitor with CloudWatch alarm: `WriteProvisionedThroughputExceeded` metric, alert if sum > 0 in any 5-minute window.

> **Tip 3:** "How does Kinesis compare to SQS for a job queue use case?" — SQS is designed for job queues (task distribution, competing consumers, at-least-once delivery with visibility timeout). Use SQS for: distributing work items across multiple workers, delayed processing (SQS delay queues), FIFO ordering with deduplication (SQS FIFO). Use Kinesis for: event streaming (multiple independent consumers, replay, ordering per key, high-throughput ingest). The key difference: SQS messages are CONSUMED AND DELETED (one consumer wins). Kinesis records are READ AND RETAINED (multiple consumers, replay possible). For a mixed requirement (work queue + audit log): put events in Kinesis, use a Lambda consumer to push to SQS for worker distribution.

