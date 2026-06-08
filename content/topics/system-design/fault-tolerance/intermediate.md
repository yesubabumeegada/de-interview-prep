---
title: "Fault Tolerance & Reliability — Intermediate"
topic: system-design
subtopic: fault-tolerance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, fault-tolerance, disaster-recovery, rpo-rto, replication, monitoring]
---

# Fault Tolerance & Reliability — Intermediate

## RPO and RTO

```
RTO (Recovery Time Objective): how long the system can be DOWN
  - RTO = 4 hours: acceptable to be offline for up to 4 hours after a failure
  - Lower RTO → more expensive (hot standby, multi-region)

RPO (Recovery Point Objective): how much DATA LOSS is acceptable
  - RPO = 1 hour: acceptable to lose up to 1 hour of data
  - RPO = 0: zero data loss (synchronous replication)

Common DE scenarios:
  Internal analytics:  RTO = 8h, RPO = 24h  → daily backups, restore from S3
  Reporting dashboard: RTO = 2h, RPO = 1h   → hourly snapshots, hot standby
  Operational data:    RTO = 15min, RPO = 0 → synchronous multi-AZ replication

Recovery strategies vs cost:
  Backup/Restore:    cheapest; RTO = hours; restore from S3 snapshot
  Warm Standby:      standby in reduced capacity; RTO = minutes (scale up + switch)
  Hot Standby:       full-capacity replica; RTO = seconds (just redirect traffic)
  Multi-Active:      active in multiple regions simultaneously; RTO ≈ 0
```

---

## Data Replication Strategies

```
Synchronous replication:
  Primary writes → waits for replica ACK → commits to client
  Guarantee: replica always up-to-date with primary
  Tradeoff: write latency increases (must wait for replica)
  Use for: financial transactions, zero RPO requirements

Asynchronous replication:
  Primary writes → commits immediately → replica lags behind
  Guarantee: replica eventually catches up (eventual consistency)
  Tradeoff: replica may be minutes/hours behind; data loss on failover
  Use for: read replicas, analytics, reporting

Semi-synchronous (MySQL):
  Primary waits for at least one replica ACK (not all)
  Balance: one replica always current; write latency moderate

Kafka replication:
  RF=3: leader + 2 in-sync replicas (ISR)
  min.insync.replicas=2: write requires 2 ISR ACKs
  acks=all: producer waits for all ISR ACKs
  = semi-synchronous with configurable consistency
```

---

## Graceful Degradation

Design systems to degrade partially rather than fail completely:

```
Full failure (bad):
  DW unavailable → dashboards return error → users see blank screen → escalation

Graceful degradation (good):
  DW unavailable → dashboards show cached data with "as of 2 hours ago" banner
  Or: show subset of data from available regions
  Or: show simplified query without expensive joins

Patterns:
  1. Cached responses: return last known good result with staleness indicator
     Redis: GET dashboard:revenue_by_region → return stale value if DW timeout
     
  2. Feature toggles: disable non-critical features under load
     if system.load > 80%:
         disable(real_time_recommendations)  # expensive ML feature
         # keep: core transaction history (critical)
     
  3. Read fallback: try fast path → slow path → static fallback
     try: return live_query(last_30_days)
     except Timeout: return cached_result(last_successful_run)
     except: return static_message("Data temporarily unavailable")
     
  4. Partial results: return what you can, clearly marked
     "Showing data for US region only — APAC data is being refreshed"
```

---

## Pipeline Monitoring and Alerting

```python
# Four golden signals for data pipelines (adapted from Google SRE):
# 1. Freshness: when was data last updated?
# 2. Volume: expected vs actual row count
# 3. Quality: null rates, referential integrity
# 4. Latency: pipeline processing time

# Freshness check (run every 15 min):
def check_freshness():
    result = db.execute("""
        SELECT table_name,
               MAX(updated_at) AS last_update,
               DATEDIFF('minute', MAX(updated_at), CURRENT_TIMESTAMP) AS lag_minutes
        FROM information_schema.tables_metadata
        WHERE table_name IN ('orders_fact', 'customers_dim', 'products_dim')
        GROUP BY table_name
    """)
    for row in result:
        if row.lag_minutes > 90:  # SLO: data < 90 minutes old
            alert(f"FRESHNESS: {row.table_name} is {row.lag_minutes} minutes stale")

# Volume anomaly detection:
def check_volume():
    today = db.execute("SELECT COUNT(*) FROM orders_fact WHERE order_date = CURRENT_DATE").scalar()
    avg_7d = db.execute("""
        SELECT AVG(daily_count) FROM (
            SELECT order_date, COUNT(*) daily_count
            FROM orders_fact
            WHERE order_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 1
            GROUP BY order_date
        )
    """).scalar()
    
    if today < avg_7d * 0.7:   # >30% below average
        alert(f"VOLUME LOW: {today} orders today vs {avg_7d:.0f} avg ({today/avg_7d:.0%})")
    if today > avg_7d * 2.0:   # >100% above average
        alert(f"VOLUME HIGH: {today} orders today — possible duplicate ingestion!")
```

---

## Handling Partial Failures in Multi-Step Pipelines

```python
# Pattern: checkpoint + resume on partial failure

PIPELINE_STAGES = ['extract', 'validate', 'transform', 'load', 'dq_check']

def run_pipeline_with_resume(execution_date: str):
    # Load progress from state store
    completed = get_completed_stages(execution_date)
    
    for stage in PIPELINE_STAGES:
        if stage in completed:
            print(f"Skipping {stage} — already completed")
            continue
        
        try:
            run_stage(stage, execution_date)
            mark_stage_complete(stage, execution_date)
        except Exception as e:
            alert(f"Pipeline failed at stage {stage}: {e}")
            raise  # Airflow will retry the full task, but we skip completed stages

def run_stage(stage: str, execution_date: str):
    if stage == 'extract':   extract_data(execution_date)
    elif stage == 'validate': validate_data(execution_date)
    elif stage == 'transform': transform_data(execution_date)
    elif stage == 'load':    load_data(execution_date)
    elif stage == 'dq_check': run_dq_checks(execution_date)

# State store: simple table
# CREATE TABLE pipeline_progress (
#   execution_date DATE, stage VARCHAR(50), completed_at TIMESTAMP,
#   PRIMARY KEY (execution_date, stage)
# );
```

---

## Interview Tips

> **Tip 1:** "How do you design for RTO=15 minutes and RPO=0?" — Zero data loss requires synchronous replication (every write goes to 2+ replicas before committing). 15-minute RTO requires a warm or hot standby: a replica that's fully up-to-date and can serve traffic with minimal switchover time (just redirect DNS/load balancer). In AWS: Multi-AZ RDS or Aurora Global Database achieves this. For data pipelines: Kafka with RF=3 + acks=all provides RPO=0; auto-scaling consumer groups minimize RTO.

> **Tip 2:** "What metrics would you alert on for a data pipeline?" — At minimum: (1) Freshness — alert if table not updated within 2× expected interval, (2) Volume — alert if daily row count deviates >30% from 7-day average, (3) Pipeline success rate — alert if job fails 2+ consecutive times, (4) Processing lag — alert if end-to-end latency exceeds SLO. Avoid alert fatigue: only alert on things that require human action; log everything else.

> **Tip 3:** "What is the difference between a retry and a rerun?" — Retry: automatic, happens immediately after failure, within the same execution context, short time window, handles transient errors. Rerun: manual or scheduled, re-executes the entire job from scratch with the same parameters, used for fixing logic bugs or data issues. Both require idempotency to be safe. Retry: for network blips and timeouts. Rerun: for bugs that required code changes or upstream data corrections.
