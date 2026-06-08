---
title: "Fault Tolerance & Reliability — Real World"
topic: system-design
subtopic: fault-tolerance
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, fault-tolerance, incident, runbook, monitoring, recovery]
---

# Fault Tolerance & Reliability — Real World

## Pattern 1: Production Incident Runbook

**Scenario:** The daily revenue pipeline failed at 3 AM. Reports show stale data. On-call engineer is paged.

```
RUNBOOK: orders_pipeline failure

1. ASSESS (2 min)
   - Check Airflow: which task failed? What is the error?
   - Check CloudWatch/Datadog: any infrastructure alerts? (disk, memory, network)
   - Check source system: is the source DB/API available?
   - Check downstream: are dashboards showing "data as of yesterday"? (customer impact)

2. TRIAGE (5 min)
   - Transient error (timeout, connection refused)?
     → Manually retry the failed task in Airflow
     → Monitor for success; escalate if retry fails

   - Data error (null PK, schema mismatch)?
     → Check DQ failure log: which check failed?
     → Investigate upstream: did source team change schema?
     → Fix transform, re-run

   - Infrastructure error (Spark cluster unreachable, S3 permission denied)?
     → Check cloud console; escalate to infra on-call
     → Consider: can we use backup cluster?

3. COMMUNICATE (10 min after page)
   → Slack #data-incidents: "orders_pipeline failed at 3:02 AM.
      Last successful run: yesterday 3:01 AM. Data is from yesterday.
      Investigating. ETA for resolution: 30 min."

4. RESOLVE
   → After fixing: re-run the failed DAG task (idempotent = safe)
   → Verify row counts match expected
   → Check downstream dashboards are updated

5. POST-INCIDENT
   → Write 5-line postmortem: what failed, root cause, fix, prevention
   → Add monitoring for the root cause (so next failure is detected earlier)
```

---

## Pattern 2: Data Pipeline SLO Dashboard

```sql
-- Pipeline SLO tracking table
CREATE TABLE pipeline_run_log (
  pipeline_name     VARCHAR(100),
  execution_date    DATE,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  status            VARCHAR(20),  -- success, failed, running
  rows_processed    BIGINT,
  error_message     VARCHAR(1000)
);

-- Daily SLO report
SELECT
  pipeline_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
  ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
  ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1) AS avg_duration_min,
  MAX(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS max_duration_min,
  SUM(CASE WHEN status = 'success'
           AND TIMESTAMPDIFF(MINUTE, start_time, end_time) > 60 THEN 1 ELSE 0 END) AS slow_runs
FROM pipeline_run_log
WHERE execution_date >= CURRENT_DATE - 30
GROUP BY pipeline_name
ORDER BY success_rate_pct ASC;  -- worst performers at top
```

---

## Pattern 3: Graceful Shutdown for Long-Running Jobs

```python
# Spark job that handles SIGTERM gracefully (for spot instance eviction)
import signal
import sys

class GracefulSparkJob:
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)
    
    def _handle_sigterm(self, signum, frame):
        print("SIGTERM received — finishing current batch before shutdown")
        self.shutdown_requested = True
    
    def run(self, execution_date: str):
        offsets = load_checkpoint(execution_date)
        
        while not self.shutdown_requested:
            batch = read_batch(offsets, batch_size=10000)
            if not batch:
                break
            
            processed = transform(batch)
            write_to_sink(processed)
            
            offsets = get_max_offset(processed)
            save_checkpoint(execution_date, offsets)  # safe restart point
            
            print(f"Processed batch up to offset {offsets} — checkpoint saved")
        
        if self.shutdown_requested:
            print(f"Graceful shutdown complete. Checkpoint saved at offset {offsets}")
            # On restart: reads from this checkpoint, no re-processing
```

---

## Common Fault Tolerance Mistakes and Fixes

| Mistake | Consequence | Fix |
|---|---|---|
| No retry on transient errors | Pipeline fails permanently on 5-second network blip | Add retry with exponential backoff |
| Retry without idempotency | Duplicate rows on retry | MERGE / partition overwrite before retry |
| No DLQ for bad messages | One malformed message blocks entire pipeline | Route to DLQ after N retries |
| Alert on every transient failure | Alert fatigue; real issues ignored | Alert only after N consecutive failures |
| No freshness monitoring | Stale data discovered by users, not engineering | Monitor table `updated_at`; alert on lag |
| No postmortem | Same incident happens again | Blameless postmortem after every P1/P2 |
| Single-AZ deployment | AZ outage = full outage | Multi-AZ for Kafka, compute, storage |

---

## Interview Tips

> **Tip 1:** "How do you build an on-call runbook for a data pipeline?" — For each pipeline: document (1) where to check status (Airflow, CloudWatch), (2) top 5 failure modes with their symptoms and fixes, (3) retry procedure (always idempotent — safe to re-run), (4) escalation path (who to call if not resolved in 30 min), (5) how to communicate to stakeholders. Test it quarterly: do a fire drill where on-call simulates the failure. A runbook is only good if someone can follow it at 2am half-asleep.

> **Tip 2:** "How do you prevent alert fatigue?" — Set alerts at the right level: alert on symptoms (data is stale, row count is zero) not on every transient error (connection retry succeeded). Use consecutive failure thresholds (alert after 2 failures, not 1). Assign severity: P1 = immediate wake-up, P2 = working hours response, P3 = weekly review. Route P3 to a team channel, not individual pages. Review alert volume monthly: any alert firing >5× with no action = recalibrate threshold.

> **Tip 3:** "What's your approach to data pipeline reliability?" — Design for failure from the start: (1) idempotent writes (safe to re-run), (2) checkpoints (resume where you left off), (3) retry with backoff on transient errors, (4) DLQ for persistent failures, (5) monitoring on freshness + volume + quality. For infrastructure: multi-AZ deployments, automated failover, auto-scaling. SLO tracking: measure success rate and processing latency; run quarterly DR drills to validate RTO/RPO targets.
