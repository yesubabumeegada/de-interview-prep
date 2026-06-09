---
title: "Stateful Processing — Intermediate"
topic: real-time-streaming
subtopic: stateful-processing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, stateful, rocksdb, state-ttl, broadcast-state, flink, pattern-matching]
---

# Stateful Processing — Intermediate

## State TTL (Time-to-Live)

```java
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

// Problem without TTL:
// State for inactive users never cleared → state grows indefinitely → OOM
// Solution: TTL — expire state after N time of inactivity

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))   // expire after 24 hours
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // reset TTL on write
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupIncrementally(1000, true)  // clean 1000 entries per state access
    .build();

// Apply TTL to state descriptor
ValueStateDescriptor<UserStats> statsDesc = 
    new ValueStateDescriptor<>("user-stats", UserStats.class);
statsDesc.enableTimeToLive(ttlConfig);

MapStateDescriptor<String, SessionData> sessionDesc =
    new MapStateDescriptor<>("sessions", String.class, SessionData.class);
sessionDesc.enableTimeToLive(ttlConfig);

// TTL cleanup strategies:
// 1. cleanupIncrementally: clean N entries when state is accessed (low overhead, slow cleanup)
// 2. cleanupFullSnapshot: run full cleanup on each checkpoint (good for RocksDB)
// 3. cleanupInRocksdbCompaction: RocksDB triggers cleanup during compaction (background)

// Recommended for large state (RocksDB):
StateTtlConfig rocksDbTtl = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompaction()  // cleanup during background compaction (no overhead per op)
    .build();

// Example: fraud detector with 7-day transaction history per card
public class FraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert> {
    
    private ListState<Transaction> recentTransactions;
    
    @Override
    public void open(Configuration config) {
        StateTtlConfig ttl = StateTtlConfig
            .newBuilder(Time.days(7))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .cleanupInRocksdbCompaction()
            .build();
        
        ListStateDescriptor<Transaction> txDesc = 
            new ListStateDescriptor<>("recent-transactions", Transaction.class);
        txDesc.enableTimeToLive(ttl);
        
        recentTransactions = getRuntimeContext().getListState(txDesc);
    }
    
    @Override
    public void processElement(Transaction tx, Context ctx, Collector<FraudAlert> out)
            throws Exception {
        
        // Add current transaction
        recentTransactions.add(tx);
        
        // Get last 7 days of transactions (TTL handles expiry)
        List<Transaction> history = new ArrayList<>();
        recentTransactions.get().forEach(history::add);
        
        // Fraud check: total spend > $5000 in 7 days
        double totalSpend = history.stream().mapToDouble(Transaction::getAmount).sum();
        if (totalSpend > 5_000.0) {
            out.collect(new FraudAlert(tx.getCardId(), 
                String.format("Total spend $%.2f exceeds $5000 in 7 days", totalSpend)));
        }
    }
}
```

---

## Broadcast State

```java
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastContext;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

/*
 Broadcast state: share a small dataset with all parallel instances
 Use case: fraud rules, configuration, lookup tables
 
 Pattern:
   Rules stream → broadcast to ALL instances
   Events stream → partitioned by key
   Each instance: read from broadcast state + process events
*/

// Rules POJO
public class FraudRule {
    String ruleId;
    String metric;       // "velocity" | "amount" | "country_change"
    double threshold;
    boolean enabled;
}

// Broadcast state descriptor
MapStateDescriptor<String, FraudRule> rulesDescriptor =
    new MapStateDescriptor<>("fraud-rules", String.class, FraudRule.class);

// Rules stream (from Kafka "fraud-rules" topic)
DataStream<FraudRule> rulesStream = env.addSource(rulesKafkaSource);

// Events stream
DataStream<Transaction> transactions = env.addSource(txKafkaSource);

// Broadcast rules to all instances
BroadcastStream<FraudRule> broadcastRules = rulesStream.broadcast(rulesDescriptor);

// Connect transactions with broadcast rules
DataStream<FraudAlert> alerts = transactions
    .keyBy(Transaction::getCardId)          // partition transactions by card
    .connect(broadcastRules)                // connect with broadcast rules
    .process(new FraudRuleProcessor());

// Processor: reads from broadcast state + keyed state
class FraudRuleProcessor 
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudAlert> {
    
    // Keyed state: per-card transaction count (updated per card)
    private ValueState<Integer> txCountIn10Min;
    
    @Override
    public void open(Configuration config) {
        txCountIn10Min = getRuntimeContext().getState(
            new ValueStateDescriptor<>("tx-count", Integer.class, 0));
    }
    
    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx, Collector<FraudAlert> out)
            throws Exception {
        
        // Read current broadcast state (rules)
        ReadOnlyBroadcastState<String, FraudRule> rules = ctx.getBroadcastState(rulesDescriptor);
        
        FraudRule velocityRule = rules.get("velocity-rule");
        if (velocityRule != null && velocityRule.isEnabled()) {
            int count = txCountIn10Min.value() + 1;
            txCountIn10Min.update(count);
            
            if (count > velocityRule.getThreshold()) {
                out.collect(new FraudAlert(tx.getCardId(), 
                    String.format("Velocity violation: %d transactions in 10 min", count)));
            }
            
            // Register timer to reset count after 10 minutes
            ctx.timerService().registerProcessingTimeTimer(
                System.currentTimeMillis() + 600_000L);
        }
    }
    
    @Override
    public void processBroadcastElement(FraudRule rule, Context ctx, Collector<FraudAlert> out)
            throws Exception {
        // Update the broadcast state with new/updated rule
        BroadcastState<String, FraudRule> rulesState = ctx.getBroadcastState(rulesDescriptor);
        
        if (rule.isEnabled()) {
            rulesState.put(rule.getRuleId(), rule);
            System.out.println("Updated rule: " + rule.getRuleId());
        } else {
            rulesState.remove(rule.getRuleId());
            System.out.println("Removed rule: " + rule.getRuleId());
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAlert> out) {
        txCountIn10Min.clear();  // reset velocity count
    }
}
```

---

## Complex Event Processing (CEP)

```java
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.PatternSelectFunction;

/*
 CEP (Complex Event Processing): detect patterns of events across time
 Use case: detect login → failed_payment → contact_support within 1 hour
 Flink CEP library uses pattern matching with state
*/

// Pattern: ATM fraud = 3+ withdrawals from different locations within 5 minutes
Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("first")
    .where(new SimpleCondition<Transaction>() {
        @Override
        public boolean filter(Transaction tx) {
            return tx.getType().equals("ATM_WITHDRAWAL");
        }
    })
    .followedBy("second")   // non-contiguous: other events can occur between
    .where(new SimpleCondition<Transaction>() {
        @Override
        public boolean filter(Transaction tx) {
            return tx.getType().equals("ATM_WITHDRAWAL");
        }
    })
    .followedBy("third")
    .where(new SimpleCondition<Transaction>() {
        @Override
        public boolean filter(Transaction tx) {
            return tx.getType().equals("ATM_WITHDRAWAL");
        }
    })
    .within(Time.minutes(5));  // all 3 must happen within 5 minutes

// Apply pattern to stream (keyed by card ID)
PatternStream<Transaction> patternStream = CEP.pattern(
    transactions.keyBy(Transaction::getCardId),
    fraudPattern
);

// Extract matches
DataStream<FraudAlert> alerts = patternStream.select(
    new PatternSelectFunction<Transaction, FraudAlert>() {
        @Override
        public FraudAlert select(Map<String, List<Transaction>> matches) {
            Transaction first  = matches.get("first").get(0);
            Transaction second = matches.get("second").get(0);
            Transaction third  = matches.get("third").get(0);
            
            // Check: all from different locations
            Set<String> locations = Set.of(
                first.getLocation(), second.getLocation(), third.getLocation());
            
            if (locations.size() == 3) {
                return new FraudAlert(first.getCardId(),
                    String.format("3 ATM withdrawals in 5 min from %d locations", 
                                  locations.size()));
            }
            return null;
        }
    }
);
```

---

## Interview Tips

> **Tip 1:** "When should you use broadcast state vs regular keyed state?" — Use broadcast state when: (a) a small lookup table or rule set needs to be available to ALL parallel instances regardless of which key they're processing, (b) the lookup data changes dynamically (can be updated via the broadcast stream), (c) the data is too small to justify a distributed key-value store (Redis). Use keyed state when: (a) state is per-key (per-user, per-device), (b) state can be partitioned (each parallel instance handles a subset of keys), (c) state size grows with the number of unique keys. Broadcast state is limited to the size that fits in each TaskManager's memory (or RocksDB) — typically up to a few GB. For larger lookup tables: use async I/O with Redis/DynamoDB.

> **Tip 2:** "What's the difference between `OnCreateAndWrite` and `OnReadAndWrite` TTL update type?" — `OnCreateAndWrite`: TTL resets when state is created or written. Reading state does NOT reset the TTL. Use for: session tracking where you want TTL from last activity (write), not from last read. `OnReadAndWrite`: TTL resets on both read and write. Any access extends the TTL. Use for: cache-like patterns where any access indicates the data is still needed. For most streaming use cases: `OnCreateAndWrite` is correct — reset TTL when new events arrive (writes), but reading for fraud checks shouldn't extend the TTL.

> **Tip 3:** "How does Flink CEP handle state internally?" — Flink CEP maintains a Non-deterministic Finite Automaton (NFA) per key. The NFA tracks all possible partial pattern matches in state. For each new event: the NFA checks if it advances any partial match. If a partial match reaches the final state (pattern complete): emit the match. If a pattern times out (`.within()` exceeded): discard the partial match. The CEP state size = number of partial matches in progress per key × memory per partial match. High cardinality patterns (many possible matches) can cause state explosion — use `.consecutive()` instead of `.followedBy()` to reduce branching, or set `.within()` time bounds to expire stale partial matches.
