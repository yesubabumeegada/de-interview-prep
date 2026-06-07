---
title: "NiFi Provenance - Real-World Production Examples"
topic: nifi
subtopic: provenance
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [nifi, provenance, production, debugging, compliance, monitoring]
---

# NiFi Provenance — Real-World Production Examples

## Example 1: Debugging a Production Data Loss Incident

```
# INCIDENT: Business reports show 15% fewer orders than expected for 2024-03-14
# Expected: ~100,000 orders. Actual in database: ~85,000

# INVESTIGATION using Provenance:

Step 1: Verify data entered NiFi
  Search: Component=ConsumeKafka, EventType=CREATE, Date=2024-03-14
  Result: 102,341 CREATE events ✓ (all data entered NiFi)

Step 2: Check for DROPs
  Search: EventType=DROP, Date=2024-03-14
  Result: 2,100 DROPs from ValidateRecord (schema failures)
  → Expected: ~2% invalid records (normal)

Step 3: Check for EXPIREs
  Search: EventType=EXPIRE, Date=2024-03-14
  Result: 12,500 EXPIRE events! ← THIS IS THE PROBLEM!
  
Step 4: Identify which queue expired FlowFiles
  Event details show: Connection "ConvertRecord → PutDatabaseRecord"
  Queue had FlowFile Expiration = 30 minutes
  
Step 5: Correlate with outage
  Provenance timestamps: EXPIREs clustered between 14:00-14:45
  → Database was down for maintenance 14:00-14:30!
  → Back pressure queued FlowFiles, 30-min expiration kicked in
  
Step 6: Root Cause
  FlowFile Expiration on that connection was 30 minutes
  DB outage was 30 minutes → FlowFiles expired while waiting

Step 7: Fix
  Remove FlowFile Expiration from that connection (critical data path!)
  Replay: Use provenance to REPLAY the 12,500 expired FlowFiles
  (Content still in archive since < 24 hours)

Step 8: Recovery
  POST /nifi-api/provenance/replays for each expired event
  All 12,500 records reprocessed → database now has 100,000 ✓
```

## Example 2: Compliance Audit Report Generator

```python
# Automated compliance report from NiFi provenance
# Runs weekly, sends report to compliance team

import requests
from datetime import datetime, timedelta

class ProvenanceComplianceReport:
    def __init__(self, nifi_url):
        self.api = f"{nifi_url}/nifi-api"
    
    def generate_weekly_report(self):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        report = {
            "period": f"{start_date.date()} to {end_date.date()}",
            "data_sources": self._get_data_sources(start_date, end_date),
            "data_destinations": self._get_destinations(start_date, end_date),
            "processing_summary": self._get_summary(start_date, end_date),
            "data_quality": self._get_quality_events(start_date, end_date),
            "security_events": self._get_security_events(start_date, end_date)
        }
        
        return report
    
    def _get_data_sources(self, start, end):
        """All external sources data was received from."""
        events = self._query_provenance(
            event_type="RECEIVE", start_date=start, end_date=end)
        
        sources = {}
        for event in events:
            source_uri = event.get('transitUri', 'unknown')
            sources[source_uri] = sources.get(source_uri, 0) + 1
        
        return [{"source": k, "event_count": v} for k, v in sources.items()]
    
    def _get_destinations(self, start, end):
        """All external systems data was sent to."""
        events = self._query_provenance(
            event_type="SEND", start_date=start, end_date=end)
        
        destinations = {}
        for event in events:
            dest_uri = event.get('transitUri', 'unknown')
            total_bytes = destinations.get(dest_uri, {"count": 0, "bytes": 0})
            total_bytes["count"] += 1
            total_bytes["bytes"] += event.get('fileSize', 0)
            destinations[dest_uri] = total_bytes
        
        return destinations
    
    def _get_quality_events(self, start, end):
        """Data quality failures (DROPs from validation)."""
        drops = self._query_provenance(
            event_type="DROP", start_date=start, end_date=end)
        
        return {
            "total_drops": len(drops),
            "by_processor": self._group_by_processor(drops),
            "by_reason": self._extract_drop_reasons(drops)
        }

# Report output:
# {
#   "period": "2024-03-08 to 2024-03-15",
#   "data_sources": [
#     {"source": "kafka://orders-topic", "event_count": 745230},
#     {"source": "sftp://partner.com/outbound/", "event_count": 142}
#   ],
#   "data_destinations": [
#     {"dest": "jdbc:snowflake://...", "count": 740000, "bytes": "45 GB"},
#     {"dest": "s3://data-lake-archive/", "count": 745230, "bytes": "52 GB"}
#   ],
#   "data_quality": {
#     "total_drops": 5230,
#     "by_processor": {"ValidateRecord": 5100, "RouteOnAttribute": 130},
#     "by_reason": {"schema_invalid": 4800, "null_required_field": 300, "expired": 130}
#   }
# }
```

## Example 3: Provenance-Based SLA Dashboard

```
# Grafana dashboard fed by provenance events (via Elasticsearch)

# Panel 1: End-to-End Processing Time (p50, p95, p99)
# Query: Calculate time between CREATE and SEND events per FlowFile UUID
# Visualization: Time-series line chart
# Alert: p99 > 30 seconds → SLA breach warning

# Panel 2: Data Volume (records/sec)
# Query: Count of CREATE events per minute
# Visualization: Area chart with threshold line

# Panel 3: Error Rate  
# Query: DROP events / CREATE events × 100
# Visualization: Gauge (0-5% green, 5-10% yellow, >10% red)

# Panel 4: Processing Bottleneck Heatmap
# Query: Average time between consecutive events per processor
# Visualization: Heatmap (processor × time-of-day → latency)
# Shows: which processor is slow and when

# Elasticsearch index mapping for provenance events:
{
  "mappings": {
    "properties": {
      "eventType": {"type": "keyword"},
      "eventTime": {"type": "date"},
      "componentName": {"type": "keyword"},
      "flowFileUuid": {"type": "keyword"},
      "fileSize": {"type": "long"},
      "processingTime": {"type": "long"},
      "attributes": {"type": "object", "dynamic": true}
    }
  }
}
```

## Example 4: Automated Replay for Failed Batches

```python
# When a batch fails (e.g., database timeout), automatically replay:

def auto_replay_failed_batch(batch_id, max_age_hours=24):
    """Find and replay all FlowFiles from a failed batch."""
    
    # Find all FlowFiles with this batch_id that were DROPPED (failed):
    events = query_provenance(
        search_terms={"batch.id": batch_id},
        event_type="DROP",
        max_results=50000
    )
    
    if not events:
        print(f"No failed events found for batch {batch_id}")
        return
    
    # Verify content is still available (within archive retention):
    replayable = []
    for event in events:
        age_hours = (datetime.now() - parse(event['eventTime'])).total_seconds() / 3600
        if age_hours <= max_age_hours:
            replayable.append(event)
    
    print(f"Found {len(replayable)} replayable events (of {len(events)} total)")
    
    # Replay each FlowFile:
    success = 0
    failed = 0
    for event in replayable:
        try:
            response = nifi_api.post("/provenance/replays", {
                "eventId": event['eventId'],
                "clusterNodeId": event['clusterNodeId']
            })
            if response.status_code == 201:
                success += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
    
    print(f"Replay complete: {success} success, {failed} failed")
    
    # Verify: check that replayed FlowFiles reach SEND event
    # (monitor provenance for new SEND events with batch.id)

# Usage:
# auto_replay_failed_batch("batch-20240315-1400")
# → Replays all FlowFiles from the 14:00 batch that failed
```

## Interview Tips

> **Tip 1:** "Walk through a data loss investigation using provenance" — (1) Count CREATE events at source → verify all data entered NiFi. (2) Count SEND events at destination → verify delivery count. (3) Gap = lost data. Search DROP + EXPIRE events → find where data disappeared. (4) Check event details: which queue, when, why (expiration? validation? error?). (5) If content archived: replay lost FlowFiles to recover.

> **Tip 2:** "How do you build compliance reports from provenance?" — Export provenance events to Elasticsearch (via Reporting Task). Query: data sources (RECEIVE events), destinations (SEND events), transformations (CONTENT_MODIFIED), quality failures (DROP events). Generate weekly/monthly reports showing: where data came from, what happened to it, where it went, what was rejected. Full audit trail without additional logging.

> **Tip 3:** "How do you use provenance for automated recovery?" — When a batch fails: query provenance for DROP events with that batch's attributes. Filter to events within content archive retention (typically 24h). Use REST API /provenance/replays to re-create each FlowFile. The FlowFile re-enters the flow at the same processor with identical content + attributes. Automatable: script detects failure → auto-replays → validates recovery.
