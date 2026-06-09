---
title: "Apache Flink — Scenarios"
topic: real-time-streaming
subtopic: apache-flink
content_type: scenario_question
tags: [flink, interview, scenarios, fraud-detection, cdc, backpressure, exactly-once]
---

# Apache Flink — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Real-Time Leaderboard System

**Scenario:** Design a real-time leaderboard for a gaming platform. 10 million concurrent players send score events. Show top 10 players globally and per region, updated every 30 seconds.

<details>
<summary>💡 Hint</summary>
Think about two-phase aggregation to avoid a non-parallelizable global top-10: first keyBy(playerId % N) for per-partition top-100, then a second stage to merge into global top-10. Use Redis ZADD for the sink.
</details>

<details>
<summary>✅ Solution</summary>

```
Architecture:
  Game servers → Kafka (score-events, 32 partitions) → Flink → Redis → API Gateway → Clients

Flink job design:

1. Source: Kafka score-events with event-time watermarks (5 second tolerance)

2. Global leaderboard (tumbling window, 30s):
   events
     .keyBy("always-same-key")  // single global partition — NOT scalable for 10M players
   
   Better approach for scale:
   events
     .map(e -> new ScoreUpdate(e.playerId, e.score, "global"))
     .keyBy(e -> e.playerId % 100)         // 100 pre-aggregate partitions
     .window(TumblingEventTimeWindows.of(Time.seconds(30)))
     .aggregate(new PlayerScoreAggregator()) // sum scores per player per window
     .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
     .process(new GlobalTop10Function())    // non-parallel, sees all players — OK at 30s cadence
   
   For truly massive scale:
     Phase 1: keyBy(playerId % 1000) → window → top-100 per partition (1000 parallel)
     Phase 2: union + process → top-10 from 100K candidates (1 parallel, small load)

3. Per-region leaderboard:
   events
     .keyBy(e -> e.region)           // partition by region (e.g., "us-east", "eu-west")
     .window(TumblingEventTimeWindows.of(Time.seconds(30)))
     .process(new RegionalTop10Function())   // top-10 per region, fully parallel

4. Sink: Redis Sorted Set (ZSET) — O(log N) inserts, O(log N) top-N retrieval
   ZADD leaderboard:global 9450 "player:12345"
   ZREVRANGE leaderboard:global 0 9          // get top 10
   TTL: 90 seconds (3 missed windows → auto-expire stale data)

5. State management:
   Use EmbeddedRocksDB (large state: 10M players × 8 bytes score + 4 bytes ID = ~120MB per window)
   Checkpoint every 30s (aligned with window cadence)
   Savepoint before deploys

Sizing:
   10M score events / 30s = 333K events/sec
   Kafka: 32 partitions, 10 MB/sec throughput (330K × 30 byte events)
   Flink: 32 parallelism (1 per Kafka partition), 4 × 8-core TaskManagers
   Redis: cluster mode, 3 shards, 16GB each (leaderboards are small)

Expected latency: < 35 seconds from event to leaderboard update
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Flink Job Failing with Checkpoint Timeout

**Scenario:** Your Flink fraud detection job (parallelism 8, RocksDB state backend) is failing every 2-4 hours with: `Checkpoint 1234 expired before completing. Maximum checkpoint time is 120000ms.` How do you diagnose and fix it?

<details>
<summary>💡 Hint</summary>
Checkpoint timeout usually means the state is too large to serialize in time. Check: state size per subtask, RocksDB block cache size, whether state has a TTL, and whether incremental checkpointing is enabled.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Understand checkpoint timeout
  Timeout = checkpoint started but didn't complete within 120 seconds
  Causes: slow state snapshot, backpressure preventing barrier propagation,
          slow I/O to checkpoint storage (S3), large state

Step 2: Check checkpoint metrics in Flink Web UI
  Checkpoints tab → Failed checkpoints → click the failed checkpoint
  Look at: 
    - Checkpoint duration per operator (which one is slow?)
    - Checkpoint size (is it growing over time?)
    - Alignment duration (time barriers spent waiting at operators = backpressure)

  Example findings:
    Operator "StatefulFraudDetector" (subtask 3): checkpoint duration = 115 seconds
    State size: 45 GB (growing from 5 GB 6 hours ago)
    Alignment duration: 2 seconds (backpressure is not the issue)

Step 3: Root cause = state size explosion
  State growing from 5 GB → 45 GB in 6 hours:
  Likely cause: MapState/ListState with keys that are never cleaned up
  Check: fraud detector accumulates transaction history per card but never expires old entries
  
  Code fix — add state TTL:
    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.days(30))                // keep state 30 days max
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupIncrementally(1000, true)         // clean 1000 entries per access
        .build();
    
    MapStateDescriptor<String, List<Transaction>> desc = 
        new MapStateDescriptor<>("tx-history", String.class, txListType);
    desc.enableTimeToLive(ttlConfig);
    txHistoryState = getRuntimeContext().getMapState(desc);

Step 4: Tune checkpoint timeout
  Short-term fix (while deploying TTL fix):
    env.getCheckpointConfig().setCheckpointTimeout(300_000);  // 5 minutes
  
  Also enable incremental checkpoints (RocksDB):
    RocksDBStateBackend backend = new RocksDBStateBackend("s3://bucket/checkpoints/", true);
    // Only uploads changed SST files → 10× smaller checkpoints

Step 5: Verify
  After deploy: checkpoint size drops from 45 GB → 5 GB
  Checkpoint duration: 8 seconds (well within 120s timeout)
  Job stable for 24+ hours without failure

Additional improvements:
  - Increase checkpoint interval (every 5 min instead of 30s) while state is large
  - Use local recovery: TaskManager stores local checkpoint copy → faster recovery on same node
    env.getCheckpointConfig().setLocalRecoveryEnabled(true);
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Migrate Storm Topology to Flink

**Scenario:** You have a legacy Apache Storm topology doing real-time session analysis. 50 spouts, 200 bolts, 2M events/sec, at-least-once delivery. Business wants exactly-once and lower latency. Design the Flink migration.

<details>
<summary>💡 Hint</summary>
Design the migration in phases: parallel running (Storm + Flink), shadow validation (compare outputs), gradual traffic shift, and cutover. Map Storm spout/bolt topology to Flink DataStream API sources/operators.
</details>

<details>
<summary>✅ Solution</summary>

```
Phase 1: Assessment (2 weeks)
  Inventory Storm topology:
    - Spouts: Kafka reader, database CDC reader
    - Bolts: parsing, enrichment (Redis lookup), sessionization, aggregation, sink
  
  Identify equivalents:
    Storm spout       → Flink KafkaSource
    Storm bolt        → Flink operator (map, flatMap, process)
    Storm ACKer       → Flink checkpoint (much simpler, exactly-once capable)
    Storm DRPC        → Flink queryable state
    Storm TimedRotatedMap (sessionization) → Flink session windows or KeyedProcessFunction
  
  Key challenge: Storm sessionization uses custom rotating state maps
  → Replace with Flink session windows (gap-based) or KeyedProcessFunction + timers

Phase 2: Implement Flink Job (4 weeks)
  
  Session analysis in Flink:
    DataStream<Event> events = env.fromSource(kafkaSource, wmStrategy, "Kafka");
    
    DataStream<Session> sessions = events
        .keyBy(Event::getUserId)
        .window(EventTimeSessionWindows.withGap(Time.minutes(30)))  // 30-min inactivity = new session
        .apply(new SessionWindowFunction() {
            @Override
            public void apply(String userId, TimeWindow window, 
                            Iterable<Event> events, Collector<Session> out) {
                // Compute session metrics: pages viewed, duration, conversion
                Session s = new Session(userId, window.getStart(), window.getEnd());
                for (Event e : events) s.addEvent(e);
                out.collect(s);
            }
        });
    
    // Enrich with async Redis lookup (non-blocking — async I/O)
    DataStream<EnrichedSession> enriched = AsyncDataStream.unorderedWait(
        sessions, new RedisUserEnricher(), 1000, TimeUnit.MILLISECONDS, 100);
    
    enriched.sinkTo(icebergSink);  // exactly-once via Iceberg 2PC

Phase 3: Shadow Running (2 weeks)
  Run Flink job in parallel with Storm (consuming same Kafka topics)
  Write Flink output to shadow tables
  Compare: Storm output vs Flink output for same time windows
  Expected: Flink output more accurate (better late data handling with watermarks)
  Acceptable diff: < 0.1% variance in session counts

Phase 4: Cutover
  Traffic cut: reduce Storm to 0%, Flink to 100%
  Monitor: Flink Web UI, Prometheus/Grafana
  Rollback: Storm topology still running (paused) for 1 week
  Decommission Storm after 1 week of stable Flink operation

Results achieved:
  Latency:         Storm ~500ms → Flink ~80ms (true streaming vs micro-batch ACK cycle)
  Exactly-once:    Storm at-least-once → Flink exactly-once (Iceberg 2PC)
  Operations:      Storm 250 nodes → Flink 40 nodes (Kubernetes auto-scaling)
  Cost:            60% reduction in compute cost
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "What's the maximum throughput you've seen from Flink, and what limits it?" — Flink can sustain millions of events per second per node. Practical limits: (a) Network bandwidth between TaskManagers (keyBy shuffles serialize/deserialize data — avoid high-cardinality keyBy if not needed); (b) State backend I/O — RocksDB compaction can cause intermittent latency spikes; (c) Checkpoint I/O — large state with slow S3 can block checkpoints; (d) Sink throughput — if the sink (database, API) can't keep up, backpressure propagates upstream. For maximum throughput: use forward partitioning (avoid shuffles), local aggregation before keyBy (reduce network), Photon-enabled Iceberg sinks, and tune buffer sizes.

> **Tip 2:** "How do you test a Flink job before deploying to production?" — Unit test operators with `ProcessFunctionTestHarnesses` (Flink's test harness library). Integration test with MiniClusterWithClientResource (embedded Flink cluster in JUnit). For end-to-end: deploy to a staging environment with a replay of production Kafka data (start from a saved offset). Use Flink's `CollectSink` or `BoundedOutOfOrdernessWatermarks` with synthetic data for deterministic tests. For window tests: inject events with specific timestamps and control the watermark explicitly to trigger window firing. Shadow mode (parallel job consuming same Kafka topic) is essential for validating stateful migration.

> **Tip 3:** "How does Flink scale — can you change parallelism without downtime?" — Yes, but it requires a savepoint. Procedure: `flink stop --savepointPath s3://bucket/sp/ <jobId>` → update parallelism in job config → `flink run -s s3://bucket/sp/<id> myJob.jar`. Flink redistributes KeyedState by rehashing keys across new parallel instances. OperatorState is redistributed in round-robin. This is a stateful rescale — no data loss. In Flink Kubernetes Operator, this can be done by changing the `parallelism` field in the FlinkDeployment manifest and redeploying. For reactive auto-scaling (add/remove TaskManagers without restart), Flink 1.13+ supports reactive mode with `scheduler-mode: reactive`, but it restarts the job from the last checkpoint.

