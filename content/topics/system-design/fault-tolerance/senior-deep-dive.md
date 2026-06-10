---
title: "Fault Tolerance & Reliability — Senior Deep Dive"
topic: system-design
subtopic: fault-tolerance
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, fault-tolerance, chaos-engineering, two-phase-commit, saga, multi-region]
---

# Fault Tolerance & Reliability — Senior Deep Dive

## Two-Phase Commit (2PC) vs Sagas

### 2PC: Distributed Atomic Transactions
```
Two-Phase Commit:
  Phase 1 (Prepare): Coordinator asks all participants "can you commit?"
    Each participant: locks resources, writes to WAL, responds YES or NO
  Phase 2 (Commit or Abort):
    If all YES: coordinator sends COMMIT to all
    If any NO: coordinator sends ABORT to all, participants rollback

Problem: Coordinator failure during Phase 2 leaves participants in uncertainty
  (resources locked indefinitely — "in-doubt transactions")
  Not used in modern distributed DE systems for this reason

Where 2PC still appears:
  - Databases with XA transactions (JDBC XA)
  - Some older ETL tools
  - Not suitable for high-throughput data pipelines

Saga Pattern (preferred for distributed DE):
  Series of local transactions with compensating actions
  No global lock; no coordinator SPOF
  See pipeline-design-patterns for full saga implementation
```

### Choosing Between 2PC and Saga
| Criterion | 2PC | Saga |
|---|---|---|
| Data consistency | Strong (ACID) | Eventual (BASE) |
| Failure complexity | In-doubt state risk | Compensating transactions needed |
| Performance | High latency (two round trips) | Lower latency |
| SPOF risk | Coordinator is SPOF | No SPOF (choreography) |
| Use for DE | Avoid | Preferred |

---

## Chaos Engineering for Data Pipelines

Proactively inject failures to validate fault tolerance:

```python
# Chaos experiments for DE systems (inspired by Netflix Chaos Monkey)

import random
from contextlib import contextmanager

class ChaosEngine:
    def __init__(self, enabled=False, failure_rate=0.1):
        self.enabled = enabled
        self.failure_rate = failure_rate
    
    @contextmanager
    def random_failure(self, failure_type="random"):
        if self.enabled and random.random() < self.failure_rate:
            failures = {
                "network": NetworkTimeoutError("Chaos: simulated network timeout"),
                "oom":     MemoryError("Chaos: simulated OOM"),
                "corrupt": ValueError("Chaos: simulated corrupt data"),
            }
            ft = failure_type if failure_type != "random" else random.choice(list(failures.keys()))
            raise failures[ft]
        yield

# Use in tests:
chaos = ChaosEngine(enabled=True, failure_rate=0.3)  # 30% failure rate in staging

def read_from_source(query: str):
    with chaos.random_failure("network"):
        return db.execute(query)

# Chaos experiments to run:
# 1. Kill a Spark executor mid-job → verify checkpoint recovery works
# 2. Cut Kafka leader → verify consumer handles partition reassignment
# 3. Inject corrupt rows → verify DQ checks catch and DLQ routes correctly
# 4. Slow down downstream DB → verify backpressure / timeout handling
# 5. Expire S3 credentials mid-run → verify retry with refreshed credentials

# How to run in practice:
# - Dev/staging: ChaosEngine in code (as above)
# - Production: Chaos Monkey (AWS), Gremlin, or manual failure injection during maintenance window
# - Track: mean time to detect (MTTD) and mean time to recover (MTTR) for each experiment
```

---

## Multi-Region Data Architecture

```
Active-Passive (one region serves all traffic):
  Primary region: write + read
  Standby region: receive replicated data (no writes)
  Failover: DNS/load balancer switches to standby on primary failure
  RPO: depends on replication lag (Kafka async: seconds; DW async: minutes)
  RTO: minutes (switchover time)
  Cost: ~2× storage, 0× extra compute until failover

Active-Active (traffic served from multiple regions):
  Both regions accept writes
  Conflict resolution needed (same row updated in both regions)
  Eventual consistency: changes propagate between regions
  RPO ≈ 0 (writes succeed locally); RTO ≈ 0 (other region keeps serving)
  Complex: conflict resolution, cross-region latency for cross-region reads
  Cost: 2× compute + storage

Multi-region Kafka:
  MirrorMaker 2 (MK2): replicates topics across clusters
    Source connector on primary → Kafka in region B
    Topics replicated: kafka-us.orders → kafka-eu.orders (prefixed by source cluster name)
  Confluent Cluster Linking: lower latency, managed replication
    Target cluster passively follows source; consumers read locally

Data residency:
  GDPR: EU user data must stay in EU → region-based partitioning mandatory
  Data segregation: write EU events to EU Kafka cluster only
  Analytics: anonymized aggregates can cross regions
```

---

## Advanced Monitoring: SLO Budget Tracking

```python
# Error budget: how much failure is allowed before SLO breach
# SLO 99.9% pipeline success → error budget = 0.1% failures over 30 days
# 30 days × 24 pipeline runs/day = 720 runs
# Error budget = 720 × 0.001 = 0.72 runs can fail ≈ less than 1 failure per month

class SLOTracker:
    def __init__(self, slo_pct: float, window_days: int):
        self.slo_pct = slo_pct        # e.g., 0.999 for 99.9%
        self.window_days = window_days  # e.g., 30
    
    def get_error_budget(self) -> dict:
        runs = db.execute("""
            SELECT COUNT(*) total,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) success_count
            FROM pipeline_runs
            WHERE run_date >= CURRENT_DATE - :window_days
        """, window_days=self.window_days).fetchone()
        
        actual_rate = runs.success_count / runs.total if runs.total > 0 else 0
        error_budget_pct = 1 - self.slo_pct          # e.g., 0.001
        error_budget_used = 1 - actual_rate           # e.g., 0.005
        budget_remaining = error_budget_pct - error_budget_used  # negative = breached!
        
        return {
            "actual_success_rate": actual_rate,
            "slo_target": self.slo_pct,
            "error_budget_remaining_pct": budget_remaining,
            "budget_exhausted": budget_remaining < 0,
        }
    
    def should_freeze_deploys(self) -> bool:
        """Freeze risky changes when error budget < 10% remaining"""
        budget = self.get_error_budget()
        return budget["error_budget_remaining_pct"] < (1 - self.slo_pct) * 0.1

slo = SLOTracker(slo_pct=0.999, window_days=30)
if slo.budget_exhausted():
    alert("🚨 SLO BREACH: Pipeline success rate below 99.9% this month!")
```

---

## Interview Tips

> **Tip 1:** "How do you design a data pipeline for zero RPO?" — Zero RPO requires synchronous replication: every write must land on at least 2 replicas before acknowledging success. For streaming: Kafka with `acks=all` + `min.insync.replicas=2` ensures the message is on 2 brokers before producer gets ACK. For storage: write to Delta Lake backed by multi-AZ S3 (synchronous replication within region). Accept: write latency increases. Track replication lag continuously; alert if replica falls behind (lagging replica = effective RPO > 0).

> **Tip 2:** "How would you use chaos engineering to validate your pipeline?" — Start small: define the hypothesis ("the pipeline will recover within 10 minutes after a single Kafka broker failure"). Inject the failure in a non-production environment first. Measure: MTTD (time to detect the failure via alerting) and MTTR (time to restore normal operation). Increment gradually: kill one broker → kill two brokers → partition network → fail entire availability zone. The goal is to find weaknesses before production does. Document each experiment and resulting improvement.

> **Tip 3:** "When would you choose active-active vs active-passive multi-region?" — Active-passive when: simplicity > cost, RPO of seconds is acceptable, writes come from one region, operational complexity of conflict resolution is too high. Active-active when: users globally need low write latency, regulatory requirements mandate local writes, or RPO ≈ 0 is non-negotiable (financial systems). Most DE workloads use active-passive: the analytics pipeline is in one region; the standby takes over only during DR events.

## ⚡ Cheat Sheet

**System design framework (DE interviews)**
```
1. Clarify requirements: batch or streaming? latency SLA? scale (rows/day)?
2. Define data flow: source → ingest → transform → serve → consume
3. Choose storage: DW (structured), Data Lake (raw), Lakehouse (both)
4. Choose compute: Spark/Flink for scale; dbt for SQL transforms; Airflow for orchestration
5. Define SLAs: freshness (15 min? 1 hr?), uptime (99.9%?), cost budget
6. Address failure modes: what breaks? how do you detect and recover?
```

**Lambda vs Kappa architecture**
```
Lambda:
  Batch layer:  reprocesses all historical data on a schedule (accurate)
  Speed layer:  processes recent data in real-time (approximate)
  Serving:      merges batch + speed views for queries
  Problem:      two codebases for same logic; complex to maintain

Kappa:
  Streaming only:  one pipeline handles both real-time and reprocessing
  Reprocessing:    replay Kafka from beginning with new consumer group
  Advantage:       single codebase; simpler ops
  Requirement:     Kafka retention must cover reprocessing window
```

**Scalability patterns**
```
Horizontal partitioning:  Kafka partitions, HDFS blocks, table partitions
Data skipping:            Z-ordering, bloom filters, min/max statistics
Push down:                predicates + projections to storage layer
Caching:                  result cache (Snowflake, Databricks SQL), Redis for lookups
Async processing:         decouple ingestion from transformation via message queue
```

**Fault tolerance patterns**
```
Idempotency:     safe to re-run; same output for same input
Checkpointing:   Flink/Spark saves progress; restart from last checkpoint
Dead letter:     failed records go to DLQ for inspection and replay
Circuit breaker: stop pipeline on repeated failures; alert before resuming
Retry with backoff: exponential backoff + jitter for transient failures
Exactly-once:    Kafka + Flink + Delta = end-to-end exactly-once
```

**Cost optimization levers**
```
Compute:
  - Spot/preemptible instances (60-80% cheaper; need checkpointing)
  - Auto-suspend warehouses (pay only when active)
  - Right-size: XL warehouse for batch; S for ad hoc
Storage:
  - Partition + vacuum old snapshots
  - Lifecycle policies: S3 IA after 30 days, Glacier after 1 year
  - Compression: ZSTD > Snappy (better ratio, acceptable CPU cost)
Query:
  - Columnar reads (never SELECT *)
  - Materialized views for expensive repeated aggregations
  - Result cache (Snowflake caches identical queries for 24h)
```

**Data warehouse design checklist**
```
□ Star schema with conformed dimensions
□ Surrogate keys on all dimensions
□ Fact table: numeric measures + FK references only
□ SCD2 on slowly changing dimensions
□ Partition on query predicate (date, region)
□ Cluster/Z-order on high-cardinality filter columns
□ Row counts + DQ checks at each medallion layer boundary
□ Freshness SLA defined and monitored for each gold table
□ Data lineage captured (dbt docs, OpenLineage)
□ Access control: role-based + column masking for PII
```

**Trade-off framework**
```
Latency vs throughput:    streaming (low latency, lower throughput) vs batch (high throughput, higher latency)
Consistency vs availability: strong consistency (slower, single writer) vs eventual (faster, multi-write)
Cost vs freshness:        real-time = expensive compute; hourly batch = cheap; choose based on business SLA
Simplicity vs flexibility: managed service (easy ops) vs self-managed (full control, higher ops burden)
Storage vs compute:       pre-aggregate (storage cost, fast queries) vs compute on demand (fresh data, slower)
```
