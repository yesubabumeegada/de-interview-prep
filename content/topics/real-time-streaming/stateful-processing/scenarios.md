---
title: "Stateful Processing — Scenarios"
topic: real-time-streaming
subtopic: stateful-processing
content_type: scenario_question
tags: [streaming, stateful, interview, scenarios, fraud-detection, cep, state-explosion]
---

# Stateful Processing — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Real-Time Recommendation Engine

**Scenario:** Design a streaming recommendation system for a music platform. 50 million active users, each listening to 30 songs/hour. Recommend the next song within 200ms of a listen event. Consider: user listening history (last 24 hours), song similarity (pre-computed), and real-time popularity.

<details>
<summary>💡 Hint</summary>
Think about pre-computing recommendations in Flink (user state → listening history), writing to Redis (< 1ms serve), and having the API read from Redis directly. Flink processes song events and updates Redis state asynchronously.
</details>

<details>
<summary>✅ Solution</summary>

```
Architecture:

  Listen events → Kafka (50M users × 30 songs/hr = 25M events/min)
               → Flink job (stateful: update user profile)
               → User profile state (RocksDB)
               
  Recommendation request → API Gateway → Recommendation Service
                                        ├── Read user state from Redis (< 5ms)
                                        ├── Song similarity lookup (Redis/Cassandra)
                                        └── Popularity score (Redis sorted set)
  
  Note: recommendations are NOT generated in Flink directly (streaming can't serve 200ms API requests)
        Flink maintains the state (user profile), Redis serves reads for low latency

Flink stateful job:
  
  State per user (ValueState<UserProfile>):
    last_24h_listens:  List of (song_id, listen_time, skip_after_sec) — last 100 songs
    preferred_genres:  Map<genre, weight> (derived from listen history)
    listening_session: current session start + current queue
    
  State TTL: 24 hours (inactive users' state expires automatically)
  
  processElement(ListenEvent):
    1. Update last_24h_listens (add new song, drop songs > 24h old)
    2. Update preferred_genres (increase weight of current song's genres)
    3. If session gap > 30 min: start new session (reset session state)
    4. Write updated profile to Redis (async, non-blocking):
         redisClient.hset("user:profile:" + userId, profileJson)
    
  Redis write: async via AsyncDataStream.unorderedWait()
               → doesn't block Flink processing (maintains throughput)
  
  Throughput sizing:
    25M events/min = 417K events/sec
    Flink: 32 parallelism × 20K events/sec per instance = 640K events/sec capacity
    State: 50M users × 1 KB = 50 GB → EmbeddedRocksDB on SSD
    Block cache: 16 GB (30% cache hit rate improvement vs default 8 GB)
  
  Recommendation serving (< 200ms):
    1. API receives play event
    2. Read user profile from Redis (< 5ms, local DC)
    3. Get last 5 songs from profile → find similar songs using pre-computed embeddings
    4. Score songs: similarity × (1 - already_heard_today) × genre_weight × recency
    5. Boost trending songs: Redis ZRANGE trending:songs BY SCORE (last-hour plays)
    6. Return top 10 recommendations
  
  Pre-computed song embeddings:
    Daily batch job (Spark): train song2vec on full listen history
    Store embeddings in Redis (song_id → 128-dim vector)
    Similarity lookup: approximate nearest neighbor (FAISS index in memory)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: State Explosion in Fraud Detection Job

**Scenario:** Your fraud detection Flink job (32 parallelism, RocksDB) has been running for 3 months. State has grown from 5 GB to 280 GB. Checkpoints now take 25 minutes and are often failing. Diagnose and fix.

<details>
<summary>💡 Hint</summary>
State explosion from ListState without TTL — each key accumulates unbounded history. Check state per key size distribution (top 10 largest keys). Fix: add StateTtlConfig, replace ListState with pre-aggregated ValueState.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Profile state growth
  RocksDB metrics: rocksdb.sst-file-size per state name
  Findings:
    "recent-transactions" state: 275 GB
    "fraud-alerts" state: 5 GB
  
  The "recent-transactions" ListState is the culprit
  
Step 2: Root cause in code
  Review ListState usage:
  
  @Override
  public void processElement(Transaction tx, Context ctx, Collector<FraudAlert> out) {
      recentTransactions.add(tx);    // <-- adds transaction to list
      // But never removes old transactions!
      // After 3 months: every card has ALL transactions ever processed
  }
  
  Root cause: ListState with no TTL and no size limit
  A card used for 3 months: 90 days × avg 5 tx/day = 450 transactions in state
  1 million active cards × 450 tx × 500 bytes = 225 GB → matches observed 275 GB

Step 3: Immediate fixes

  Fix A: Add TTL (preferred — keeps 7 days of history)
    StateTtlConfig ttl = StateTtlConfig
        .newBuilder(Time.days(7))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .cleanupInRocksdbCompaction()  // cleanup during background compaction
        .build();
    
    ListStateDescriptor<Transaction> desc = 
        new ListStateDescriptor<>("recent-transactions", Transaction.class);
    desc.enableTimeToLive(ttl);
    
  Fix B: Replace ListState with pre-aggregated state (reduce state size)
    Instead of storing individual transactions (500 bytes each):
    Store aggregated statistics (spend per day, count per 7 days, etc.) = 50 bytes
    
    ValueStateDescriptor<TransactionStats> statsDesc = 
        new ValueStateDescriptor<>("tx-stats", TransactionStats.class);
    // TransactionStats: {total_30d, count_30d, total_7d, count_7d, last_country}
    // Size: ~50 bytes vs 500 bytes × N transactions
    
    State size: 1M cards × 50 bytes = 50 MB (vs 275 GB!)

Step 4: Deploy fix
  Cannot restart job in-place (state incompatible with ListState → ValueState change)
  Procedure:
    a. Stop current job with savepoint
    b. Deploy new job version (new state names = new state from scratch)
    c. Allow new job to warm up (state builds from scratch — accurate after 7 days)
    d. During warmup: use wider fraud detection rules (less historical context)
  
  Alternative: add new ValueState alongside old ListState (migration pattern)
    - Both states exist in new code
    - processElement reads ListState (old) → compute stats → write to ValueState (new)
    - After 7 days: all active cards have stats in ValueState
    - Remove ListState from code + deploy again

Step 5: Fix checkpointing
  Short-term: increase checkpoint timeout to 30 minutes (covers 25-min checkpoints)
  After Fix A: state drops from 275 GB → 5 GB
    Checkpoint duration: 25 min → 30 seconds
    Normal checkpoint timeout (120s) works fine again

Prevention:
  Add state size monitoring per state descriptor
  Alert: state per slot > 5 GB → investigate
  Code review gate: any new ListState must have explicit TTL or size limit
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing Low-Latency Complex Event Detection

**Scenario:** Design a system to detect account takeover attempts in real-time. Pattern: login from new device → change password → add new payment method, all within 10 minutes. 5 million logins/hour. Must detect within 2 seconds of last event in pattern.

<details>
<summary>💡 Hint</summary>
Use Flink CEP: define a pattern (login → password_change → add_payment) with WITHIN 10 minutes. KeyBy userId so the pattern state is local to each user's partition. Consider: what constitutes 'new device' for the first event filter.
</details>

<details>
<summary>✅ Solution</summary>

```
Pattern definition (CEP):
  begin("login") WHERE isNewDevice = true
    .followedBy("password_change")  // within same session
    .followedBy("add_payment")
    .within(Time.minutes(10))

Latency requirements:
  End-to-end: < 2 seconds
  Components:
    Event → Kafka: ~50ms (producer async)
    Kafka → Flink: ~100ms (polling interval)
    Flink CEP processing: ~50ms (state lookup + pattern match)
    Alert → Kafka (output): ~50ms
    Total: ~250ms ✓ (well within 2s SLA)

Flink CEP implementation:

  Pattern<AccountEvent, ?> takeoverPattern = Pattern
      .<AccountEvent>begin("login")
      .where(e -> e.getType().equals("LOGIN") && e.isNewDevice())
      .followedBy("password_change")
      .where(e -> e.getType().equals("PASSWORD_CHANGE"))
      .followedBy("add_payment")
      .where(e -> e.getType().equals("ADD_PAYMENT_METHOD"))
      .within(Time.minutes(10));
  
  // Apply per user (keyBy userId)
  PatternStream<AccountEvent> patternStream = CEP.pattern(
      events.keyBy(AccountEvent::getUserId),
      takeoverPattern
  );
  
  DataStream<TakeoverAlert> alerts = patternStream.select(match -> {
      AccountEvent login   = match.get("login").get(0);
      AccountEvent pwChange= match.get("password_change").get(0);
      AccountEvent addPay  = match.get("add_payment").get(0);
      
      return new TakeoverAlert(
          login.getUserId(),
          login.getEventTime(),
          addPay.getEventTime(),  // detection time
          login.getIpAddress(),
          login.getDeviceFingerprint(),
          "ACCOUNT_TAKEOVER_PATTERN"
      );
  });

Scaling for 5M logins/hour:
  5M / 3600 = 1,389 events/sec (logins)
  With password changes + payment additions: ~3,000 events/sec total
  Flink: 8 parallelism × 5,000 events/sec = 40,000 capacity (comfortable headroom)
  
  CEP state per user: partial match state (at most 3 events per user = 3 × 500 bytes = 1.5 KB)
  Active users with partial matches: 5M × 0.001% suspicious = 50 users
  CEP state: 50 × 1.5 KB = 75 KB (negligible)
  
  Note: most users never hit even the first event (new device login is rare)
        CEP is very efficient — state only exists for partial matches in progress

False positive reduction:
  Additional checks after pattern match:
    - IP geolocation: same country? (VPN detection)
    - Velocity check: > 3 failed logins before this one? (brute force indicator)
    - User behavior: typical for this user's time-of-day?
  
  Output routing:
    High confidence: immediate account lock + email alert
    Medium confidence: require additional verification (MFA challenge)
    Low confidence: log for human review (fraud analyst queue)

Monitoring:
  Pattern match rate: alert if > 10× normal → potential new attack pattern
  False positive rate: track % of alerts that result in confirmed fraud vs. user dispute
  Alert latency: p99 < 2 seconds (measure event_time → alert emit timestamp)
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "How does Flink CEP maintain state for partial pattern matches?" — Flink CEP uses a Non-deterministic Finite Automaton (NFA) per key. The NFA state contains all in-progress partial matches as a list of "SharedBuffer" references. When a new event arrives: the NFA checks each partial match to see if the event advances the pattern. Matching events extend the partial match chain; non-matching events may be buffered (for `followedBy` which allows intervening events) or skip (for `next` which requires consecutive events). The `.within(Time.minutes(10))` prunes partial matches older than 10 minutes. Memory per key: proportional to the number of concurrent partial matches × events in each match. For account takeover (rare): negligible state. For common patterns on high-cardinality keys: can grow large.

> **Tip 2:** "What's the difference between `next()` and `followedBy()` in Flink CEP patterns?" — `next(name)`: the next event must immediately follow (no intervening events of other types). Very strict — any non-matching event breaks the pattern. `followedBy(name)`: events matching the next pattern stage can appear anywhere after, even with other events in between. More flexible — models "event A eventually followed by event B." `followedByAny(name)`: like `followedBy` but allows multiple parallel pattern matches when there are multiple candidate events. For account takeover: `followedBy` is correct (the user may do other actions between login, password change, and payment add). Use `next` only for strict consecutive patterns (e.g., exactly 3 consecutive failed logins).

> **Tip 3:** "How do you handle stateful processing in a multi-region deployment?" — State in Flink/Spark is local to the job instance in one region. For multi-region: (a) Active-passive: primary region processes all events, secondary region is a warm standby (fed same Kafka events, maintains state but doesn't emit). On primary failure: promote secondary; (b) Active-active: each region processes its own events independently, state is local to the region. Cross-region consistency requires a separate sync layer (DynamoDB global tables, Cosmos DB multi-master). (c) Shared external state: all regions read/write to a global state store (Redis global cluster, DynamoDB global tables). Trade-off: external state adds 5-20ms per lookup vs. in-memory Flink state at ~0.1ms. For fraud detection: active-passive (correctness > multi-region write latency).

