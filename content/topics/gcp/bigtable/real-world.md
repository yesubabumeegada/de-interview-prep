---
title: "Bigtable — Real-World Cases"
topic: gcp
subtopic: bigtable
content_type: study_material
layer: real-world
difficulty_level: mid-level
tags: [gcp, bigtable, interview]
---

# Bigtable — Real-World Cases

Three production stories — a hotspot launch incident, a batch-vs-serving war, and a cost blowout — each with metrics, code, and the rule that came out of it.

## Case Study 1: Launch Day Hotspot — 6 Nodes at 9% CPU, One at 98%

**Context.** A gaming company launched a leaderboard/telemetry feature backed by a 7-node SSD Bigtable cluster, sized for 60k writes/s. Load tests passed. On launch morning, write latency p99 went from 8ms to **1.9 seconds** and the client queues backed up — at only ~22k writes/s, a third of tested capacity.

**Investigation.** Cluster average CPU: 21%. **Hottest node CPU: 98%.** That gap is the textbook hotspot signature. Key Visualizer showed a single blazing stripe at the top of the keyspace, advancing over time.

The row key:

```python
# THE PROBLEM — match events keyed by event time
row_key = f"{int(time.time()*1000)}#{match_id}#{player_id}".encode()
```

Load tests hadn't caught it because the test harness generated *random historical* timestamps — uniformly distributed. Production wrote *now*, and "now" is one tablet.

**Fix.**

```python
# Field promotion: high-cardinality prefix first, time last and reversed
import sys

reversed_ts = sys.maxsize - int(time.time() * 1000)
row_key = f"{player_id}#{reversed_ts}#{match_id}".encode()
```

Queries were all per-player ("recent matches for player X") so this matched the read pattern perfectly — a prefix scan per player, newest first. For the one global query ("most recent 100 matches anywhere"), they maintained a separate small salted index table written by a Dataflow job, rather than contorting the main key.

Backfill: a Dataflow job rewrote 9 days of data to the new key shape into a new table; clients flipped via config; old table dropped after a week.

**Outcome.** p99 back to 7ms at 70k writes/s on the **same 7 nodes**. No nodes added — the capacity had been there all along.

**Interview takeaway.** Quote the two metrics that diagnose this in 30 seconds: *average CPU vs hottest-node CPU*, then Key Visualizer. And the test-harness lesson: load tests must reproduce production key distribution, not just production QPS.

## Case Study 2: The Nightly Batch Job That Ruined Breakfast

**Context.** A fintech served account-risk features from Bigtable (12-node cluster, multi-region: `us-c1`, `eu-c1`) to a fraud-scoring API with a 30ms p99 SLO. Every morning 6:00–7:30 UTC, p99 spiked to 200–400ms and fraud checks timed out, falling back to a conservative rule set that declined ~2% of legitimate transactions — a real revenue hit (~$40k/month estimated from decline analytics).

**Investigation.** The spike window matched a nightly Dataflow job that rewrote ~800M feature rows (full refresh) using the **default app profile** — multi-cluster-any routing. Two compounding problems:

1. The batch job's massive scan+write load landed on whichever cluster was "nearest" — the same `us-c1` serving US traffic.
2. The bulk writes evicted the **block cache**, so even after the job finished, the first ~20 minutes of serving reads went to Colossus instead of cache.

**Fix — isolation via app profiles + topology:**

```bash
# Dedicated batch profile pinned to the EU cluster (off-peak there at 6 UTC)
gcloud bigtable app-profiles create batch-refresh \
    --instance risk-features \
    --route-to eu-c1 \
    --description "Nightly Dataflow refresh - pinned, isolated"

# Serving profile: multi-cluster for failover
gcloud bigtable app-profiles create serving \
    --instance risk-features \
    --route-any
```

```python
# Dataflow job uses the batch profile explicitly
WriteToBigTable(
    project_id="proj",
    instance_id="risk-features",
    table_id="features",
    app_profile_id="batch-refresh",
)
```

Replication then propagated the refreshed rows to `us-c1` asynchronously, trickling into cache instead of nuking it. The job also switched from full rewrite to **delta writes** (only changed features, ~7% of rows), cutting replication traffic, and used a `CheckConsistency` token before flipping the "feature version" flag the API read.

**Outcome.** Serving p99 during the batch window: 380ms → **22ms**. False-decline fallback events: zero. Batch runtime grew 15% (EU cluster was smaller) — an explicitly accepted trade.

**Interview takeaway.** Replication's most underrated use is **workload isolation**, not DR. The phrase: "pin batch to one cluster with an app profile, serve from another, and let async replication be the buffer."

## Case Study 3: The Table That Grew $9k/Month After "Cleanup"

**Context.** An IoT platform stored 2 years of sensor data (top-level table ~38TB SSD across 11 nodes). To cut costs, the team ran a "cleanup": a Spark job issuing **row-level deletes** for all data older than 13 months (~1.1B rows). Two weeks later: storage *up* ~6%, scan-based daily aggregation jobs **35% slower**, and the bill up ~$9k/month annualized because autoscaling added two nodes to hold latency.

**Investigation.** Three compounding LSM facts the team had not internalized:

1. Deletes are **tombstone writes** — 1.1B new entries that scans must process and skip.
2. Space is reclaimed only at **compaction**, which proceeds opportunistically — tens of TB takes time.
3. Their scans crossed the deleted ranges (key was `sensor#ts`, old data interleaved per sensor), so every aggregation waded through tombstones.

**Fix.**

```bash
# 1. The data had a natural age boundary -> GC policy should own retention
cbt -instance iot setgcpolicy readings data maxage=395d

# 2. For the already-tombstoned mess: nothing to do but wait for compaction
#    (support confirmed); latency mitigated by temporary node bump.
```

For the *next* table generation they made retention structural:

```python
# Monthly-bucketed tables: drop a whole table to expire a month — O(1)
table_id = f"readings_{yyyymm}"          # readings_202605, readings_202606...
# expiry = drop oldest table; no tombstones, no compaction debt
```

(They evaluated `DropRowRange` by prefix — also cheap — but the key didn't lead with time, so monthly tables were cleaner, at the cost of fan-out for cross-month reads.)

**Outcome.** After compaction caught up (~3 weeks), storage dropped to 21TB, the two extra nodes were removed, and scans returned to baseline. Final state: **$11k/month saved** vs pre-cleanup via the maxage policy, and deletes never appear in application code anymore.

**Interview takeaway.** "In an LSM store, a delete is a write." Retention belongs to **GC policies, DropRowRange, or table rotation** — never bulk row deletes. If you say that sentence with the tombstone/compaction mechanism behind it, you sound like you've been burned properly.

## Cross-Case Rules

| Incident class | Detection metric | Standing rule |
|---|---|---|
| Hotspot | Hottest-node CPU vs average; Key Visualizer | Load tests replay production key distributions |
| Batch/serving interference | p99 windows correlated to job schedules | Every workload gets its own app profile; batch pinned |
| Tombstone debt | Storage flat/up after deletes; scan latency up | Retention via GC/maxage/table rotation only |
| Capacity | CPU >70% or storage >70% | Plan for post-failover load, not steady state |

## Telling These Stories

Structure each as: **SLO impact → the one metric that localized it → mechanism (key order / routing / LSM) → fix at the design layer → number after**. Bigtable interview stories land best when the mechanism (sorted tablets, async replication, tombstones) is named explicitly — it proves the fix wasn't cargo-culted.
