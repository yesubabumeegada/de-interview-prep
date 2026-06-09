---
title: "Watermarks & Windows — Scenarios"
topic: real-time-streaming
subtopic: watermarks-and-windows
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [streaming, watermarks, windows, interview, scenarios, late-data, debugging]
---

# Watermarks & Windows — Interview Scenarios

## Scenario 1: Streaming Counts Don't Match Batch Job

**Question:** Your real-time dashboard shows 50,000 orders per hour, but the batch job (reading from the same Delta Lake source) shows 65,000 orders for the same hour. The streaming job uses event-time windowing with a 2-minute watermark. Root-cause and fix.

**Answer:**

```
Step 1: Quantify the discrepancy
  Streaming: 50,000 orders/hour
  Batch:     65,000 orders/hour
  Difference: 15,000 (23% of orders missing in streaming)

Step 2: Hypothesis — late events dropped by watermark
  Streaming config: withWatermark("event_time", "2 minutes")
  → Events arriving > 2 minutes after their event_time are DROPPED
  
  Silver Delta table (source for batch): includes ALL events (written on arrival, no filtering)
  → Batch sees 15,000 events that arrived after 2-minute watermark

Step 3: Verify hypothesis
  Add a counter for late events:
    Flink: add sideOutputLateData(lateTag), count side output
    Spark: in foreachBatch, count records where (current_batch_time - event_time > 2 min)
  
  Finding: 15,000 events per hour have event_time > 2 minutes before arrival
  → CONFIRMED: watermark too tight, dropping 23% of events

Step 4: Profile event delay distribution
  Query Silver Delta table:
    SELECT 
      percentile_approx(unix_timestamp(bronze_ts) - unix_timestamp(event_time), 0.50) AS p50_lag_sec,
      percentile_approx(unix_timestamp(bronze_ts) - unix_timestamp(event_time), 0.90) AS p90_lag_sec,
      percentile_approx(unix_timestamp(bronze_ts) - unix_timestamp(event_time), 0.99) AS p99_lag_sec,
      percentile_approx(unix_timestamp(bronze_ts) - unix_timestamp(event_time), 0.999) AS p999_lag_sec,
      COUNT(*) as total,
      SUM(CASE WHEN unix_timestamp(bronze_ts) - unix_timestamp(event_time) > 120 THEN 1 ELSE 0 END) as late_count
    FROM delta.`s3://bucket/delta/silver/orders/`
    WHERE date >= '2024-01-15'
  
  Results:
    p50 = 8 seconds (half of events arrive in 8s)
    p90 = 45 seconds
    p99 = 4 minutes (99th percentile arrives 4 minutes late)
    p999 = 12 minutes
    Late (>2 min): 23% → matches discrepancy ✓
  
  Source of lag: mobile app orders (orders placed offline, uploaded later)

Step 5: Fix
  Option A: Increase watermark to cover p99 (4 minutes):
    .withWatermark("event_time", "5 minutes")  # small buffer above p99
    Cost: windows fire 5 minutes later (dashboard shows data 5 min stale)
    
  Option B: Separate mobile and desktop events:
    Desktop: tight watermark (1 minute) → fast updates
    Mobile: loose watermark (10 minutes) → accurate, but 10 min delayed
    
  Option C: Hybrid (streaming + batch reconciliation):
    Streaming: 2-minute watermark for real-time dashboard (acceptable 23% error for live view)
    Hourly batch: recompute from Silver Delta (exact counts) → update dashboard for past hours
    Dashboard: shows "preliminary" for current hour, "final" for past hours

Chosen fix: Option A (increase watermark to 5 minutes)
  Dashboard latency: 2 minutes → 5 minutes (acceptable per SLA)
  Accuracy: 99% (still 1% of events beyond 5 minutes will be dropped)
  Monitor: set alert if late event rate > 5%
```

---

## Scenario 2: Session Window Design for a Ride-Sharing App

**Question:** Design session window analytics for a ride-sharing app. A "trip session" starts when a user requests a ride and ends when they rate the trip. If they don't rate within 30 minutes of trip completion, session closes automatically. Track: trip duration, wait time, surge pricing, rating.

**Answer:**

```
Event flow:
  ride_requested → driver_assigned → pickup → dropoff → (30 min gap or) rated

Session definition:
  Start trigger: ride_requested event
  End trigger:   rated event OR 30-minute inactivity after dropoff

Event schema:
  {
    "trip_id": "T123",
    "user_id": "U456",
    "driver_id": "D789",
    "event_type": "ride_requested|driver_assigned|pickup|dropoff|rated",
    "latitude": 37.4,
    "longitude": -122.1,
    "fare": 18.50,
    "surge_multiplier": 1.5,
    "rating": 4.5,    // only for 'rated' event
    "event_time": "2024-01-15T14:30:00Z"
  }

Flink implementation:
  
  // Session window: 30-minute gap
  DataStream<TripSummary> trips = events
      .keyBy(e -> e.getUserId() + "_" + e.getTripId())  // per user per trip
      .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
      .aggregate(new TripAggregator(), new TripWindowFn());
  
  // TripAggregator maintains:
  //   - requested_time (first ride_requested event_time)
  //   - pickup_time    (pickup event_time)
  //   - dropoff_time   (dropoff event_time)
  //   - rated_time     (rated event_time or null)
  //   - fare           (from dropoff event)
  //   - surge          (from ride_requested event)
  //   - rating         (from rated event, null if not rated)
  
  TripSummary:
    trip_id, user_id, driver_id
    wait_time_sec   = pickup_time - requested_time
    trip_duration_sec = dropoff_time - pickup_time
    total_session_sec = (rated_time or dropoff_time + 30 min) - requested_time
    fare, surge_multiplier
    rating             (null if session ended due to inactivity, not rating)
    rated              (boolean)

Watermark strategy:
  Events arrive within 30 seconds (app sends events directly to Kafka)
  Watermark: 2 minutes (generous for network delays)
  Session gap: 30 minutes
  
  Timeline for a typical trip:
    14:30: ride_requested
    14:35: driver_assigned (5 min wait)
    14:37: pickup (2 min additional wait)
    14:55: dropoff (18 min trip)
    15:00: rated (5 min after dropoff)
    → Session: 14:30 - 15:00 = 30 minutes, fires at watermark > 15:30 (15:00 + 30 min gap)
    
  For unrated trips:
    15:25: last event = dropoff (no rating)
    → Session closes at 15:55 (dropoff + 30 min gap)
    → Watermark fires session at 15:55 + 2 min watermark = ~15:57
    → ~27 minutes after dropoff to get the session summary

Analytics from TripSummary:
  Driver performance: avg(rating) per driver_id per week
  Surge analysis: avg(fare) WHERE surge > 1.2
  Conversion funnel: requested → assigned → pickup → dropoff → rated
  Wait time SLA: P95(wait_time_sec) per region per hour

Monitoring:
  Trips with no rating: rated = false AND time_since_session > 35 min → trigger follow-up push notification
  Session state size: 10M active trips × 200 bytes = 2 GB state (RocksDB backend)
```

---

## Scenario 3: Watermark Stalling Under Low Traffic

**Question:** Your streaming job processes Kafka events, but at 3 AM (low traffic period), all windows stop firing. By 4 AM, you have 1,200 windows waiting to fire. When traffic resumes at 8 AM, all 1,200 windows fire simultaneously, overwhelming the downstream database. How do you prevent this?

**Answer:**

```
Root cause analysis:
  3 AM: Kafka topic receives very few events (1 event/minute instead of 1000/min)
  Watermark = max(event_time) - tolerance
  
  Problem: with only 1 event/minute and 5-minute watermark:
    3:00 AM: last event at 3:00, watermark = 2:55 AM
    3:01 AM: one event at 3:01, watermark = 2:56 AM
    ...slowly advancing, but all windows still waiting
    
  Actual problem: Kafka partition with NO events at all
    If one partition is completely idle, its watermark = last event's time (hours ago)
    Global watermark = min(all partitions) = stalled at 3 AM
    No windows fire → backlog builds → 1,200 windows queued

Fix 1: Idle partition detection (primary fix)
  Flink:
    .withIdleness(Duration.ofMinutes(5))
    // After 5 min of silence, partition excluded from watermark
    // Active partitions advance watermark normally
    // Windows fire during low-traffic periods
  
  Spark: handled automatically — watermark advances based on events seen per batch
  
Fix 2: Bound the window backlog (defense in depth)
  Limit how many windows can fire simultaneously:
  
  Flink (custom trigger):
    // In onEventTime: batch window outputs into mini-batches
    // Fire at most 100 windows per second
    // Use processing-time timer to pace firing
  
  Spark (foreachBatch):
    // Naturally bounded: each batch processes windows fired in that batch
    // Multiple batches needed to catch up → natural pacing

Fix 3: Downstream sink rate limiting
  Even with Fix 1+2, 8 AM surge may still hit DB hard
  Add connection pool size limit: maxConnections = 20 (limits parallel writes)
  Use retry queue with backoff if DB is overwhelmed
  
Fix 4: Monitoring and alerting
  Add alert: if windowsWaiting > 100 AND time > 2 AM → page on-call
  Alert: if batch duration > 5× normal → downstream bottleneck
  Dashboard: "Windows queued" metric (Flink: numWindowsInState)

Fix 5: Process-time fallback for low-traffic windows
  For windows at 3 AM that have no events:
  Don't wait for event-time watermark — just emit empty results on processing-time schedule
  
  Flink approach: ProcessingTimeTrigger as backup:
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .trigger(new EventTimeOrProcessingTimeTrigger(
        EventTimeTrigger.create(),        // fire on watermark
        ProcessingTimeTrigger.create()    // or fire on wall-clock regardless
    ))

Implementation priority:
  1. Fix idle partition detection (withIdleness) → solves root cause
  2. Monitor window backlog → early warning for future occurrences
  3. DB write rate limiting → defense in depth
```

---

## Interview Tips

> **Tip 1:** "How do you set up a reliable system to compare streaming counts vs batch counts for data quality?" — Run a parallel batch validation job that reads from the same source (Silver Delta table) with a 2-hour delay (by then, all late events have arrived). For each time window: compare streaming count vs batch count. If abs(streaming - batch) / batch > 5%: alert and log discrepancy. Store comparison results in a data quality Delta table. This dual-verification approach catches watermark issues, schema drift, and pipeline bugs early. Set a threshold: < 2% discrepancy is acceptable for real-time dashboards; > 5% requires investigation.

> **Tip 2:** "What is the trade-off between window size and state size in sliding windows?" — State size for sliding windows = O(keys × (window_size / slide_interval) × state_per_accumulator). A 1-hour window with 1-minute slide = 60 concurrent window states per key. With 1 million users: 60M window states. If each accumulator is 100 bytes: 6 GB state. Solutions: (a) Use `AggregateFunction` (O(1) accumulator, not O(N) records); (b) Reduce slide granularity (10-minute slide instead of 1-minute → 6× less state); (c) Use RocksDB state backend for large state; (d) Consider whether sliding windows are necessary — tumbling windows with shorter intervals often achieve similar business goals with less state.

> **Tip 3:** "How do you handle clock skew between event generators (e.g., IoT devices with drifted clocks)?" — IoT devices may have clocks off by minutes or hours. Signs: event_time far in the future or past relative to Kafka ingestion time. Strategies: (a) Set `max_allowed_skew = abs(event_time - kafka_timestamp)`: if skew > 10 minutes, replace event_time with Kafka timestamp and flag for investigation; (b) Per-device calibration: track clock drift per device ID, apply correction factor; (c) Use relative timestamps: instead of absolute timestamps, have devices send `seconds_since_last_event` and reconstruct event_time server-side; (d) Wide watermark tolerance (e.g., 30 minutes) accepts most clock drift at the cost of higher latency. Always monitor `avg(event_time - kafka_time)` per device type as a health indicator.
