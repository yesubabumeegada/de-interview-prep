---
title: "Stateful Processing — Senior Deep Dive"
topic: real-time-streaming
subtopic: stateful-processing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, stateful, rocksdb, state-migration, queryable-state, large-state, production]
---

# Stateful Processing — Senior Deep Dive

## RocksDB Internals for Streaming

```
RocksDB: embedded key-value store based on LSM-tree (Log-Structured Merge-tree)

Write path:
  1. Write to MemTable (in-memory write buffer, typically 64MB)
  2. When MemTable full → flush to L0 SST file (on disk, immutable)
  3. Background compaction: merge L0 → L1 → L2 → L3 (sorted, no overlaps)
  
Read path:
  1. Check MemTable (in memory — fastest)
  2. Check block cache (cached SST blocks — fast)
  3. Check L0 SST files (must check all L0 files — slower)
  4. Check L1+ SST files (binary search within sorted files)
  5. Bloom filter: per-SST file, quickly determines if key MAY be in file

For Flink stateful streaming:
  Each Flink state (ValueState per key) → stored as key-value in RocksDB
  Key: stateNameBytes + keyGroupBytes + keyBytes + namespaceBytes
  Value: serialized state value

Tuning RocksDB for streaming state:

1. Block cache (most important for read-heavy workloads):
   // Flink's RocksDB options factory:
   RocksDBOptionsFactory factory = new DefaultConfigurableOptionsFactory()
       .setBlockCacheSize("512mb")        // larger cache → fewer disk reads
       .setWriteBufferSize("64mb")         // MemTable size
       .setMaxWriteBufferNumber(3)         // how many MemTables before flush
       .setLevel0FileNumCompactionTrigger(4) // compact when 4 L0 files exist

2. Bloom filter: always enabled (default) — use for point lookups
   Reduces I/O for non-existent keys (common in fraud detection: card not seen before)

3. Compression:
   L0: no compression (I/O bound — raw speed matters)
   L1+: Snappy compression (reduces disk space 2-3×, slight CPU overhead)
   
4. Block size:
   Default: 4KB blocks
   For large values: increase to 64KB (fewer block reads for large state values)

5. TTL + compaction filter:
   Enable .cleanupInRocksdbCompaction() on StateTtlConfig
   RocksDB compaction filter checks TTL during background compaction
   Expired entries deleted without additional scan overhead

RocksDB metrics to monitor (via Flink's RocksDB metrics reporter):
  rocksdb.block-cache-hit-rate          → % of reads served from cache (target > 90%)
  rocksdb.compaction.estimated-pending-bytes → compaction backlog (high = write stall risk)
  rocksdb.write-stall                   → write stalls (0 = healthy)
  rocksdb.sst-file-size                 → total state size on disk
  rocksdb.column-family.memtable-size   → current MemTable usage
```

---

## State Schema Evolution

```java
/*
 Problem: you have a running Flink job with state.
 You need to change the state schema (add a field, rename, change type).
 You cannot just change the Java class and restart — state serialization format won't match.
 
 Options:
 1. Schema evolution with POJO serializer (limited):
    Adding NEW fields to a POJO: works automatically
    Removing fields: not supported
    Changing field types: not supported
    
 2. Custom serializer (full control):
    Implement TypeSerializer<MyState> with version-aware deserialization
    On schema change: increment version, write migration logic
    
 3. State migration via savepoint:
    a. Stop job with savepoint
    b. Write migration job: reads old state, transforms, writes to new state name
    c. Start new version of main job from scratch (discarding old state)
    d. Use migration job's output to initialize new state (warm-up)
    
 4. Side-by-side state (simplest for non-breaking changes):
    Keep old state field, add new state field
    Migration logic in processElement: if new state is null, initialize from old state
    Gradually migrate all keys as they are accessed
*/

// Example: adding a new field to ValueState
// Old state: UserStats{totalOrders: Long}
// New state: UserStats{totalOrders: Long, totalRevenue: Double}

// Migration approach: use two separate state descriptors
public class MigratingOrderProcessor extends KeyedProcessFunction<String, Order, Void> {
    
    // Old state (keep for migration period)
    private ValueState<Long> legacyOrderCount;
    
    // New state (with new field)
    private ValueState<UserStats> userStats;
    
    @Override
    public void open(Configuration config) {
        legacyOrderCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>("order-count", Long.class));   // old name
        
        userStats = getRuntimeContext().getState(
            new ValueStateDescriptor<>("user-stats", UserStats.class));  // new name
    }
    
    @Override
    public void processElement(Order order, Context ctx, Collector<Void> out)
            throws Exception {
        
        // Migration: if new state doesn't exist, initialize from old state
        if (userStats.value() == null) {
            Long legacyCount = legacyOrderCount.value();
            userStats.update(new UserStats(
                legacyCount != null ? legacyCount : 0L,
                0.0  // revenue: unknown from legacy → start at 0
            ));
            legacyOrderCount.clear();  // free old state
        }
        
        // Update new state
        UserStats stats = userStats.value();
        stats.totalOrders++;
        stats.totalRevenue += order.getAmount();
        userStats.update(stats);
    }
}
// After all keys have been accessed (migration complete), remove legacy state descriptor
```

---

## Queryable State

```java
/*
 Queryable State: query Flink's internal state externally (without emitting to a sink)
 Use case: check current fraud score for a card without waiting for output topic
 
 Architecture:
   Flink job → QueryableStateServer (embedded in TaskManagers)
   External service → QueryableStateClient → TaskManager with that key's state
   
 Limitations:
   - Experimental feature (not for production-critical use cases)
   - At-most-once semantics (query may miss state during checkpoint)
   - Better alternative: emit state to Redis via foreachBatch for production
*/

// In Flink job: make state queryable
ValueStateDescriptor<FraudScore> fraudScoreDesc = 
    new ValueStateDescriptor<>("fraud-score", FraudScore.class);
fraudScoreDesc.setQueryable("fraud-score-query");  // register as queryable

ValueState<FraudScore> fraudScore = getRuntimeContext().getState(fraudScoreDesc);

// External client query:
QueryableStateClient client = new QueryableStateClient("flink-jobmanager", 9069);
TypeSerializer<String>     keySerializer   = StringSerializer.INSTANCE;
TypeSerializer<FraudScore> valueSerializer = new KryoSerializer<>(FraudScore.class, ...);

// Query state for a specific card
CompletableFuture<ValueState<FraudScore>> result = client.getKvState(
    jobId,            // Flink job ID
    "fraud-score-query",  // registered name
    "CARD_12345",     // key to query
    BasicTypeInfo.STRING_TYPE_INFO,
    fraudScoreDesc
);
FraudScore score = result.get().value();

/*
 Production alternative: emit state to Redis (more reliable)
 Update processElement to also write to Redis:
*/
@Override
public void processElement(Transaction tx, Context ctx, Collector<FraudAlert> out) 
        throws Exception {
    
    // Update internal state
    FraudScore score = computeScore(tx);
    fraudScore.update(score);
    
    // Write to Redis for external queries (async to not block processing)
    redisClient.hset("fraud-scores", tx.getCardId(), 
                     objectMapper.writeValueAsString(score));
    
    // Emit alert if high score
    if (score.getValue() > 0.8) {
        out.collect(new FraudAlert(tx.getCardId(), score));
    }
}
```

---

## Large Scale Stateful Processing Architecture

```
Production architecture for 100M keyed state entries:

Job: real-time user personalization (last 30 days of user activity)
  State per user: 1 KB (purchase history, preferences, segment)
  Total state: 100M users × 1 KB = 100 GB
  
  → MUST use RocksDB state backend (heap can't hold 100 GB)
  → Incremental checkpointing (only upload changed SST files)
  → Large executor nodes: i3.4xlarge (16 vCPU, 122 GB RAM, 3.8 TB NVMe SSD)
    RocksDB block cache: 32 GB (26% of RAM)
    JVM heap: 16 GB (Flink overhead + network buffers)
    RocksDB on NVMe SSD: 100 GB state (NVMe = fast random reads)

Checkpoint tuning for 100 GB state:
  Full checkpoint: 100 GB upload → at S3 throughput of 1 GB/s = 100 seconds (too long)
  Incremental checkpoint (RocksDB): only changed files (e.g., 500 MB/min changes)
    → 500 MB upload per checkpoint → 0.5 seconds at 1 GB/s ✓
  
  Configure:
    EmbeddedRocksDBStateBackend(incrementalCheckpointing = true)
    Checkpoint interval: 5 minutes (frequent enough for recovery window)
    Checkpoint timeout: 300 seconds
    Min pause between checkpoints: 120 seconds
    
State partitioning strategy:
  100M users, 32 parallelism → ~3.1M users per slot
  Each slot: 3.1M × 1 KB = 3.1 GB RocksDB state
  Block cache per slot: 1 GB
  Fits on 32 × i3.2xlarge nodes (4 tasks per node)

Recovery time:
  On failure: download 3.1 GB RocksDB state per slot from S3
  At S3 throughput 500 MB/s: 6 seconds per slot recovery
  All slots in parallel → total recovery time: ~30 seconds + job startup

Monitoring:
  rocksdb.block-cache-hit-rate: target > 95% (if < 80%, increase cache)
  Checkpoint duration: alert if > 60 seconds
  State size growth: alert if > 10% per day (unexpected state accumulation)
  GC pause: alert if > 500ms (indicates heap pressure from serialization)
```

---

## Interview Tips

> **Tip 1:** "How do you decide on the right number of Flink task slots per TaskManager for stateful jobs?" — More slots per TaskManager = more state per node (state sharing is efficient). Fewer slots per TaskManager = more nodes needed, but each slot has more exclusive memory. For RocksDB: each slot needs its own RocksDB instance and block cache. Too many slots per node = block caches compete for memory → cache thrashing. Rule of thumb: 1-2 slots per CPU core, with at least 2 GB per slot for small-state jobs, 8+ GB for large-state jobs. For 100 GB total state with 32 parallelism: 32 slots across 8-16 nodes (4-2 slots per node) with NVMe SSDs. Monitor: block cache hit rate and GC pause to validate the slot configuration.

> **Tip 2:** "What is the difference between `ValueState` and `ReducingState` / `AggregatingState`?" — `ValueState`: stores a single value per key (completely replaced on each update). Use when you need the full current value. `ReducingState`: stores a single value per key, but uses a `ReduceFunction` to combine new elements with existing state (commutative + associative). Efficient for running sums, counts. `AggregatingState`: similar to ReducingState but allows input and output types to differ (e.g., add individual values, return average). The advantage of Reducing/Aggregating state: each new element is merged immediately (O(1) state size per key regardless of how many elements). ValueState can grow if you store a collection — use ListState or MapState for that case.

> **Tip 3:** "How would you handle a stateful job where the key space grows unboundedly (e.g., all possible transaction IDs)?" — Unbounded key space means state grows forever (one entry per unique key, never cleaned up). Solutions: (a) TTL on state: `StateTtlConfig` expires keys not seen in N time; (b) Don't use the unique ID as the partition key — use a higher-level key (user_id, card_id) that has bounded cardinality, and aggregate at that level; (c) Windowed processing: key by (transaction_id + window_start) → state only lasts for window duration, then purged; (d) For deduplication specifically: use a time-bounded Bloom filter (accept ~0.01% false negatives for duplicate detection, reset filter every N minutes). Never use unbounded unique IDs as state keys without a TTL.
