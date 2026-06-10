---
title: "AWS DynamoDB - Senior Deep Dive"
topic: aws-services
subtopic: dynamodb
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, dynamodb, single-table-design, dax, global-tables, access-patterns, cost-optimization]
---

# AWS DynamoDB — Senior-Level Deep Dive

## Single-Table Design Patterns

Single-table design stores multiple entity types in one table, using generic PK/SK attributes with prefixes:

```python
# Data engineering metadata store: pipelines, runs, tasks, alerts
# All in one table with overloaded keys

# Entity patterns:
items = [
    # Pipeline definition
    {'PK': 'PIPELINE#etl-orders', 'SK': 'METADATA',
     'name': 'ETL Orders Daily', 'owner': 'data-team', 'schedule': 'cron(0 2 * * *)'},
    
    # Pipeline run
    {'PK': 'PIPELINE#etl-orders', 'SK': 'RUN#2024-01-15T02:00:00Z',
     'status': 'COMPLETED', 'duration_sec': 342, 'records_processed': 1500000},
    
    # Task within a run
    {'PK': 'RUN#2024-01-15T02:00:00Z#etl-orders', 'SK': 'TASK#extract',
     'status': 'COMPLETED', 'started_at': '2024-01-15T02:00:05Z', 'ended_at': '2024-01-15T02:02:30Z'},
    
    {'PK': 'RUN#2024-01-15T02:00:00Z#etl-orders', 'SK': 'TASK#transform',
     'status': 'COMPLETED', 'started_at': '2024-01-15T02:02:31Z', 'ended_at': '2024-01-15T02:05:00Z'},
    
    # Alert
    {'PK': 'PIPELINE#etl-orders', 'SK': 'ALERT#2024-01-14T02:15:00Z',
     'severity': 'HIGH', 'message': 'Run exceeded SLA (>10 min)', 'acknowledged': False},
]

# Access patterns supported:
# 1. Get pipeline metadata: PK=PIPELINE#X, SK=METADATA
# 2. List all runs for a pipeline: PK=PIPELINE#X, SK begins_with("RUN#")
# 3. Get specific run: PK=PIPELINE#X, SK=RUN#timestamp
# 4. List tasks in a run: PK=RUN#timestamp#pipeline, SK begins_with("TASK#")
# 5. Get alerts for pipeline: PK=PIPELINE#X, SK begins_with("ALERT#")
# 6. GSI (status-index): query all FAILED runs across all pipelines
```

**Single-table design trade-offs:**

| Advantage | Disadvantage |
|-----------|-------------|
| Fewer API calls (fetch related data in one query) | Complex key design (harder to understand) |
| Transact across entity types | Cannot use column filters easily |
| Cost efficient (one table to manage) | GSI projections must be planned carefully |
| Scales to any size | Harder to evolve schema |

---

## Access Pattern Modeling

```python
# Step 1: List all access patterns BEFORE designing the table

access_patterns = {
    # Pattern: description -> key design
    'Get pipeline config': 'PK=PIPELINE#{id}, SK=METADATA',
    'List recent runs (last 30 days)': 'PK=PIPELINE#{id}, SK begins_with("RUN#"), ScanIndexForward=False, Limit=30',
    'Get run details + all tasks': 'Query PK=RUN#{ts}#{pipeline}, SK begins_with("TASK#")',
    'Find all failed runs (any pipeline)': 'GSI: PK=status, SK=run_timestamp (status-index)',
    'Find runs by date range': 'GSI: PK=run_date, SK=pipeline_id (date-index)',
    'Get all pipelines owned by team': 'GSI: PK=owner, SK=pipeline_id (owner-index)',
}

# Step 2: Design keys to satisfy patterns with minimum indexes
# Rule: if you need more than 3-5 GSIs, reconsider your table design

# Step 3: Choose between composite SK and GSI
# Composite SK: free, strong consistency, limited to same PK
# GSI: flexible, costs extra throughput, eventually consistent
```

---

## Hot Partition Handling

```python
# Problem: one partition key gets disproportionate traffic
# Example: status="RUNNING" gets queried 1000x/sec but only 10 distinct values

# Solution 1: Write sharding (distribute hot key across partitions)
import random

def write_with_sharding(pipeline_id, run_data, shard_count=10):
    """Distribute writes across shards to avoid hot partition."""
    shard = random.randint(0, shard_count - 1)
    table.put_item(Item={
        'PK': f'PIPELINE#{pipeline_id}#SHARD#{shard}',
        'SK': f'RUN#{run_data["timestamp"]}',
        **run_data
    })

def read_with_sharding(pipeline_id, shard_count=10):
    """Query all shards and merge results."""
    all_results = []
    for shard in range(shard_count):
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'PIPELINE#{pipeline_id}#SHARD#{shard}'},
            ScanIndexForward=False,
            Limit=10
        )
        all_results.extend(response['Items'])
    
    # Sort merged results
    return sorted(all_results, key=lambda x: x['SK'], reverse=True)[:10]


# Solution 2: Burst capacity and adaptive capacity
# DynamoDB automatically handles short bursts (300 seconds of unused capacity)
# Adaptive capacity redistributes throughput to hot partitions

# Solution 3: For reads, use DAX cache (see next section)
```

---

## DAX Caching (DynamoDB Accelerator)

```python
# DAX: in-memory cache for DynamoDB (microsecond latency)
# Sits between your application and DynamoDB

import amazondax

# DAX client is drop-in replacement for DynamoDB client
dax_client = amazondax.AmazonDaxClient(
    endpoint_url='dax://my-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'
)
dax_table = dax_client.Table('pipeline_runs')

# Same API as DynamoDB (transparent caching)
response = dax_table.get_item(Key={'PK': 'PIPELINE#etl-orders', 'SK': 'METADATA'})
# First call: reads from DynamoDB, caches in DAX
# Subsequent calls: returns from DAX cache (microseconds)

# Cache behavior:
# - Item cache: caches individual GetItem results (TTL configurable)
# - Query cache: caches full query results (TTL configurable)
# - Write-through: writes go to DynamoDB AND invalidate DAX cache
```

**When DAX makes sense:**

| Good Fit | Bad Fit |
|----------|---------|
| Read-heavy workloads (>10:1 read:write) | Write-heavy workloads |
| Same items read repeatedly | Every read is unique |
| Latency-sensitive (need microseconds) | Millisecond latency is fine |
| Hot key mitigation | Evenly distributed access |
| Cost: ~$0.27/hr per node (minimum 3) | Low traffic tables (overkill) |

---

## Global Tables (Multi-Region)

```python
# Global Tables: active-active multi-region replication
# Any region can accept writes; changes replicate in <1 second

# Use cases for data engineering:
# 1. Multi-region pipeline orchestration (run pipelines in any region)
# 2. Disaster recovery (failover without data loss)
# 3. Low-latency access for global teams

# Setup (must use on-demand or provisioned with auto-scaling):
dynamodb_client = boto3.client('dynamodb')

# Add replica to existing table
dynamodb_client.update_table(
    TableName='pipeline-state',
    ReplicaUpdates=[
        {'Create': {'RegionName': 'eu-west-1'}},
        {'Create': {'RegionName': 'ap-southeast-1'}}
    ]
)

# Conflict resolution: last-writer-wins (based on timestamp)
# Each item has a aws:rep:updatetime attribute for conflict detection

# Cost: replicated writes charged per region
# 1000 WCU in us-east-1 replicated to eu-west-1 and ap-southeast-1
# Total write cost: 1000 WCU × 3 regions = 3000 WCU equivalent
```

---

## DynamoDB Streams + Lambda for Event Sourcing

```python
# Event sourcing: store every state change as an immutable event
# DynamoDB Streams provides the event log

def event_sourcing_handler(event, context):
    """Process DynamoDB Stream events for downstream systems."""
    
    for record in event['Records']:
        # Build event envelope
        event_data = {
            'event_id': record['eventID'],
            'event_type': record['eventName'],
            'timestamp': record['dynamodb']['ApproximateCreationDateTime'],
            'table': record['eventSourceARN'].split('/')[1],
        }
        
        if record['eventName'] in ('INSERT', 'MODIFY'):
            event_data['new_state'] = deserialize(record['dynamodb']['NewImage'])
        if record['eventName'] in ('MODIFY', 'REMOVE'):
            event_data['old_state'] = deserialize(record['dynamodb']['OldImage'])
        
        # Fan out to multiple consumers
        publish_to_eventbridge(event_data)   # Trigger downstream workflows
        write_to_kinesis(event_data)          # Analytics stream
        write_to_s3_audit_log(event_data)     # Compliance/audit trail

def publish_to_eventbridge(event_data):
    """Route events to different targets based on type."""
    events_client = boto3.client('events')
    events_client.put_events(Entries=[{
        'Source': 'pipeline.state-changes',
        'DetailType': event_data['event_type'],
        'Detail': json.dumps(event_data),
        'EventBusName': 'data-platform-events'
    }])
```

---

## Cost Optimization at Scale

```python
# DynamoDB cost breakdown for a pipeline metadata table:
# 100K pipelines, 10M run records, 50K reads/sec, 5K writes/sec

# On-Demand:
on_demand_monthly = {
    'reads': 50_000 * 3600 * 24 * 30 / 1_000_000 * 0.25,   # ~$32,400/month
    'writes': 5_000 * 3600 * 24 * 30 / 1_000_000 * 1.25,    # ~$16,200/month
    'storage': 50 * 0.25,  # 50 GB × $0.25/GB = $12.50
    'total': 48_612  # ~$48K/month
}

# Provisioned (with auto-scaling):
provisioned_monthly = {
    'reads': 50_000 * 0.00013 * 730,    # 50K RCU = ~$4,745/month
    'writes': 5_000 * 0.00065 * 730,    # 5K WCU = ~$2,373/month
    'storage': 50 * 0.25,                # $12.50
    'total': 7_130  # ~$7K/month (85% cheaper!)
}

# With Reserved Capacity (1-year commitment):
reserved_monthly = {
    'reads': provisioned_monthly['reads'] * 0.77,  # 23% discount
    'writes': provisioned_monthly['writes'] * 0.77,
    'total': 5_490  # ~$5.5K/month
}

# Optimization strategies:
optimizations = [
    'Switch to provisioned + auto-scaling (biggest savings)',
    'Use ProjectionExpression to read fewer attributes (reduce RCU)',
    'TTL to auto-delete old run records (reduce storage)',
    'DAX for repeated reads (reduce read cost)',
    'Batch operations (reduce request overhead)',
    'Compress large attributes (JSON → gzip base64)',
]
```

---

## Backup and Restore

```python
# Point-in-Time Recovery (PITR): continuous backups, restore to any second
# On-demand backups: manual snapshots (retained until deleted)

# Enable PITR
dynamodb_client.update_continuous_backups(
    TableName='pipeline-state',
    PointInTimeRecoverySpecification={'PointInTimeRecoveryEnabled': True}
)

# Restore to specific timestamp (creates NEW table)
dynamodb_client.restore_table_to_point_in_time(
    SourceTableName='pipeline-state',
    TargetTableName='pipeline-state-restored-20240115',
    RestoreDateTime=datetime(2024, 1, 15, 10, 30, 0),
    # Optionally override settings on restored table
)

# PITR retention: 35 days
# Cost: $0.20/GB/month (storage of continuous backup data)
# Restore time: depends on table size (minutes to hours)

# On-demand backup (zero impact on table performance):
dynamodb_client.create_backup(
    TableName='pipeline-state',
    BackupName='pre-migration-backup-20240115'
)
```

---

## Interview Tips

> **Tip 1:** "Explain single-table design in DynamoDB" — "Store multiple entity types in one table using generic PK/SK with prefixes (e.g., PK=PIPELINE#id, SK=RUN#timestamp). This lets you fetch related entities in a single query and use transactions across types. Design starts by listing ALL access patterns, then choosing key structures that satisfy them. Trade-off: more complex to understand but fewer API calls and better atomicity."

> **Tip 2:** "How do you handle hot partitions in DynamoDB?" — "Three approaches: (1) Write sharding — append a random suffix to the hot key, then scatter-gather on reads. (2) DAX cache — absorb read spikes in-memory. (3) On-demand capacity mode — handles bursts up to double previous peak automatically. DynamoDB also has built-in adaptive capacity that redistributes throughput to hot partitions. For extreme cases, redesign the key to distribute load more evenly."

> **Tip 3:** "DynamoDB cost optimization at high scale?" — "The biggest lever is switching from on-demand to provisioned with auto-scaling (4-7x savings at steady traffic). Add reserved capacity for another 23% off. Use ProjectionExpression to read only needed attributes. TTL old data automatically. DAX reduces read cost for repeated access patterns. At 50K+ RPS, the difference between on-demand and provisioned is tens of thousands of dollars monthly."

## ⚡ Cheat Sheet

**Key Design Rules**
- Define ALL access patterns before designing keys — key design is immutable
- Generic PK/SK with prefix notation: `PK=ENTITY#id`, `SK=TYPE#timestamp`
- Composite SK enables range queries within a partition for free (strongly consistent)
- GSI: flexible but eventually consistent, costs extra throughput — limit to 3–5 per table

**Hot Partition Solutions**
- Write sharding: append `#SHARD#N` suffix (0–9) to hot PK; scatter-gather on read
- Adaptive capacity: DynamoDB auto-redistributes throughput to hot partitions (built-in)
- DAX: absorb read spikes in-memory; minimum 3 nodes at ~$0.27/hr each

**DAX Fit Check**
- Good: read:write >10:1, same items repeatedly read, microsecond latency required
- Bad: write-heavy, every read unique, low-traffic table (overkill)

**Cost Levers (biggest first)**
1. On-Demand → Provisioned + auto-scaling: 4–7× savings at steady traffic
2. Reserved Capacity (1-yr): additional 23% off provisioned
3. TTL: auto-delete old records → reduces storage cost
4. ProjectionExpression: read only needed attributes (reduces RCU)
5. DAX: reduces read cost for repeated patterns

**Key Numbers**
- On-demand: $0.25/million reads, $1.25/million writes
- Provisioned: $0.00013/RCU-hr, $0.00065/WCU-hr
- PITR: $0.20/GB/month, 35-day retention, restores to new table
- Global Tables: replicated write = 1 WCU × N regions (plan for 3× write cost)
- Item max size: 400 KB

**Streams + Lambda**
- StreamViewType: `NEW_AND_OLD_IMAGES` for full before/after diff
- Use sequence number as dedup key in DynamoDB (conditional `attribute_not_exists`) for exactly-once
- Fan out from stream → EventBridge + Kinesis + S3 for downstream systems
