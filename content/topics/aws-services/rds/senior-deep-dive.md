---
title: "AWS RDS - Senior Deep Dive"
topic: aws-services
subtopic: rds
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, rds, cdc, aurora-serverless, blue-green, cross-region, performance-tuning, cost-optimization]
---

# AWS RDS — Senior-Level Deep Dive

## CDC Patterns: DMS, Logical Replication, Binlog

### AWS DMS (Database Migration Service)

```python
# DMS for continuous CDC from RDS to data lake
# Architecture: RDS (source) → DMS → Kinesis Data Streams → S3

import boto3

dms = boto3.client('dms')

# Create replication task for ongoing CDC
task = dms.create_replication_task(
    ReplicationTaskIdentifier='rds-to-kinesis-cdc',
    SourceEndpointArn='arn:aws:dms:us-east-1:123:endpoint:source-rds',
    TargetEndpointArn='arn:aws:dms:us-east-1:123:endpoint:target-kinesis',
    ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:dms-instance',
    MigrationType='cdc',  # full-load, cdc, or full-load-and-cdc
    TableMappings=json.dumps({
        'rules': [{
            'rule-type': 'selection',
            'rule-id': '1',
            'rule-action': 'include',
            'object-locator': {
                'schema-name': 'public',
                'table-name': '%'  # All tables
            }
        }]
    }),
    ReplicationTaskSettings=json.dumps({
        'TargetMetadata': {
            'ParallelLoadThreads': 8,
            'BatchApplyEnabled': True
        },
        'Logging': {
            'EnableLogging': True,
            'LogComponents': [{'Id': 'SOURCE_CAPTURE', 'Severity': 'LOGGER_SEVERITY_DEFAULT'}]
        }
    })
)
```

### PostgreSQL Logical Replication

```sql
-- Native PostgreSQL logical replication (no DMS needed)
-- Lower latency, less operational overhead, but PostgreSQL-only

-- On source RDS:
-- 1. Set parameter: rds.logical_replication = 1 (requires restart)
-- 2. Create publication:
CREATE PUBLICATION cdc_publication FOR TABLE orders, customers, products;

-- On target (or use a logical replication consumer like Debezium):
CREATE SUBSCRIPTION cdc_subscription
    CONNECTION 'host=source.rds.amazonaws.com dbname=prod user=repl_user password=xxx'
    PUBLICATION cdc_publication;

-- Monitoring replication lag:
SELECT slot_name, confirmed_flush_lsn, 
       pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes
FROM pg_replication_slots;
```

### MySQL Binary Log (Binlog)

```python
# MySQL/Aurora MySQL: use binlog for CDC
# DMS reads binlog natively, or use tools like Maxwell/Debezium

# RDS MySQL parameter group settings for CDC:
mysql_cdc_params = {
    'binlog_format': 'ROW',           # Required for CDC (not STATEMENT)
    'binlog_row_image': 'FULL',       # Capture before and after
    'binlog_checksum': 'NONE',        # Some CDC tools require this
    'expire_logs_days': '3',          # Retain binlog for 3 days
    'log_slave_updates': '1',         # If using read replica as CDC source
}

# Binlog retention on RDS:
# CALL mysql.rds_set_configuration('binlog retention hours', 72);
```

**CDC approach comparison:**

| Approach | Latency | Complexity | Source Impact | Best For |
|----------|---------|-----------|---------------|----------|
| DMS | Seconds | Medium (managed) | Low | Multi-engine, AWS-managed |
| Logical replication | Sub-second | Low | Low | PostgreSQL to PostgreSQL |
| Debezium + Kafka | Sub-second | High | Low | Kafka-based architectures |
| RDS snapshot export | Hours | Low | None | Batch analytics (daily) |

---

## Aurora Serverless v2

```python
# Aurora Serverless v2: auto-scales compute in 0.5 ACU increments
# 1 ACU = ~2 GB memory + proportional CPU
# Range: 0.5 ACU to 128 ACU (scales in seconds)

# Use cases for data engineering:
# - Dev/test databases (scale to zero is not supported in v2, min 0.5 ACU)
# - Variable analytics workloads (heavy during business hours, idle at night)
# - Microservice databases with unpredictable traffic

# Cost model:
aurora_serverless_cost = {
    'per_acu_hour': 0.12,  # $0.12/ACU-hour
    'storage_per_gb': 0.10,  # $0.10/GB-month
    'io_per_million': 0.20,  # $0.20/million I/O requests
}

# Example: database that needs 8 ACU during day, 1 ACU at night
# Provisioned equivalent: db.r6g.2xlarge (8 vCPU, 64 GB) = $0.96/hr always
# Serverless: 8 ACU × 12hr × $0.12 + 1 ACU × 12hr × $0.12 = $12.96/day
# Provisioned: $0.96 × 24hr = $23.04/day
# Savings: 44% with serverless for this pattern

# Aurora Serverless v2 vs v1:
# v2: scales in seconds (0.5 ACU steps), compatible with provisioned readers
# v1: deprecated, scaled in 5+ minutes, limited features
```

---

## Cross-Region Replicas for DR

```python
# Aurora Global Database: 1 primary region + up to 5 secondary regions
# Replication lag: typically <1 second
# Failover: promote secondary to primary in <1 minute

import boto3

rds = boto3.client('rds')

# Create global cluster
rds.create_global_cluster(
    GlobalClusterIdentifier='global-analytics-db',
    SourceDBClusterIdentifier='arn:aws:rds:us-east-1:123:cluster:analytics-primary',
    Engine='aurora-postgresql',
    DeletionProtection=True
)

# Add secondary region
rds_secondary = boto3.client('rds', region_name='eu-west-1')
rds_secondary.create_db_cluster(
    DBClusterIdentifier='analytics-secondary-eu',
    GlobalClusterIdentifier='global-analytics-db',
    Engine='aurora-postgresql',
    DBSubnetGroupName='private-subnets-eu',
)

# Planned failover (zero data loss):
rds.failover_global_cluster(
    GlobalClusterIdentifier='global-analytics-db',
    TargetDbClusterIdentifier='arn:aws:rds:eu-west-1:123:cluster:analytics-secondary-eu'
)

# Unplanned failover (potential seconds of data loss):
# 1. Detach secondary from global cluster
# 2. Secondary becomes standalone primary
# 3. Redirect application to new endpoint
```

---

## Blue-Green Deployments

```python
# Blue-Green: create a copy of production, apply changes, switch traffic
# Zero-downtime schema changes, engine upgrades, parameter changes

rds = boto3.client('rds')

# Create blue-green deployment (copies everything)
response = rds.create_blue_green_deployment(
    BlueGreenDeploymentName='schema-migration-v2',
    Source='arn:aws:rds:us-east-1:123:db:production-db',
    TargetEngineVersion='15.4',  # Optional: upgrade engine
    TargetDBParameterGroupName='postgres15-optimized'  # Optional: new params
)

# Green environment is a replica that stays in sync with Blue
# Apply schema changes to Green (no impact on Blue/production):
# - ALTER TABLE orders ADD COLUMN shipping_status VARCHAR(50);
# - CREATE INDEX idx_orders_status ON orders(shipping_status);

# When ready, switchover (< 1 minute downtime):
rds.switchover_blue_green_deployment(
    BlueGreenDeploymentIdentifier=response['BlueGreenDeployment']['BlueGreenDeploymentIdentifier'],
    SwitchoverTimeout=300  # Max seconds to wait
)

# Switchover process:
# 1. Stops writes to Blue
# 2. Waits for Green to catch up (replication lag → 0)
# 3. Renames: Blue → old-production, Green → production
# 4. Applications reconnect to new primary (same endpoint)
# 5. Rollback: switch back if issues detected
```

---

## RDS Data API

```python
# Data API: HTTP-based access to Aurora Serverless (no VPC/connection needed)
# Perfect for Lambda functions, Step Functions, and lightweight clients

import boto3

rds_data = boto3.client('rds-data')

CLUSTER_ARN = 'arn:aws:rds:us-east-1:123:cluster:analytics-serverless'
SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:123:secret:rds-creds'

def query_via_data_api(sql: str, parameters: list = None):
    """Execute SQL via Data API (no connection management)."""
    response = rds_data.execute_statement(
        resourceArn=CLUSTER_ARN,
        secretArn=SECRET_ARN,
        database='analytics',
        sql=sql,
        parameters=parameters or []
    )
    return response['records']

# Parameterized query (prevents SQL injection)
results = query_via_data_api(
    sql="SELECT customer_id, SUM(amount) as total FROM orders WHERE order_date = :date GROUP BY customer_id",
    parameters=[{'name': 'date', 'value': {'stringValue': '2024-01-15'}}]
)

# Batch execution (insert multiple rows efficiently)
rds_data.batch_execute_statement(
    resourceArn=CLUSTER_ARN,
    secretArn=SECRET_ARN,
    database='analytics',
    sql="INSERT INTO metrics (pipeline_id, metric_name, value, ts) VALUES (:pid, :name, :val, :ts)",
    parameterSets=[
        [
            {'name': 'pid', 'value': {'stringValue': 'etl-orders'}},
            {'name': 'name', 'value': {'stringValue': 'row_count'}},
            {'name': 'val', 'value': {'doubleValue': 150000}},
            {'name': 'ts', 'value': {'stringValue': '2024-01-15T02:30:00Z'}}
        ],
        # ... more rows
    ]
)

# Limitations:
# - 1 MB response size limit
# - 1000 requests/second (default)
# - Only Aurora Serverless and provisioned Aurora (newer versions)
# - Higher latency than direct connections (~50ms overhead)
```

---

## Performance Tuning

```python
# Instance sizing strategy:
# 1. Start with Performance Insights to identify bottleneck
# 2. If CPU-bound: scale up vCPUs or optimize queries
# 3. If memory-bound: scale up RAM (more cache = less I/O)
# 4. If I/O-bound: use io2 storage or Aurora (distributed storage)

instance_sizing = {
    # Small databases (< 100 GB, < 1000 QPS)
    'small': 'db.r6g.large (2 vCPU, 16 GB)',
    
    # Medium databases (100 GB - 1 TB, 1000-10000 QPS)
    'medium': 'db.r6g.2xlarge (8 vCPU, 64 GB)',
    
    # Large databases (1+ TB, 10000+ QPS)
    'large': 'db.r6g.4xlarge+ (16+ vCPU, 128+ GB) or Aurora',
    
    # Analytics workloads (complex queries, large joins)
    'analytics': 'db.r6g.4xlarge+ with read replicas',
}

# Key PostgreSQL parameters for data engineering workloads:
analytics_params = {
    'work_mem': '512MB',              # Large sorts and joins in memory
    'maintenance_work_mem': '2GB',     # Fast index creation
    'effective_cache_size': '48GB',    # Tell planner about OS cache
    'random_page_cost': '1.1',         # SSD storage
    'max_parallel_workers_per_gather': '4',  # Parallel query execution
    'max_parallel_workers': '8',       # Total parallel workers
    'jit': 'on',                       # JIT compilation for complex queries
}
```

---

## Cost Optimization

```python
# RDS cost components:
cost_breakdown = {
    'instance': '60-70% of total cost',
    'storage': '10-20% (gp3 is cheapest)',
    'IOPS': '10-15% (only for io2 or Aurora I/O)',
    'backup': '5% (beyond free retention)',
    'data_transfer': '1-5% (cross-AZ, cross-region)',
}

# Reserved Instances (1-year or 3-year commitment):
reserved_savings = {
    '1-year_no_upfront': '20% savings',
    '1-year_all_upfront': '35% savings',
    '3-year_all_upfront': '55% savings',
}

# Aurora I/O-Optimized (predictable pricing):
# Standard Aurora: $0.20/million I/O requests (unpredictable cost)
# I/O-Optimized: 30% higher instance cost, zero I/O charges
# Break-even: when I/O cost exceeds 25% of total Aurora bill
# Typical candidates: write-heavy workloads, OLAP-style queries

# Cost optimization strategies:
strategies = [
    'Reserved Instances for production (35-55% savings)',
    'Graviton instances (r6g vs r5: 20% cheaper, same performance)',
    'gp3 storage (20% cheaper than gp2, configurable IOPS)',
    'Stop dev/test instances outside business hours',
    'Read replicas only when needed (consider schedule-based scaling)',
    'Aurora I/O-Optimized for I/O-heavy workloads',
    'Right-size with Performance Insights (many instances over-provisioned)',
]
```

---

## Interview Tips

> **Tip 1:** "How do you set up CDC from RDS to a data lake?" — "Three approaches depending on requirements. DMS for managed, multi-engine CDC with seconds latency — it reads the transaction log and streams changes to Kinesis or S3. PostgreSQL logical replication for sub-second latency within PostgreSQL ecosystem. For batch analytics (daily), RDS snapshot export to S3 in Parquet format is simplest (zero production impact). Choose based on freshness requirements: real-time (DMS/logical replication) vs daily (snapshot export)."

> **Tip 2:** "When would you use Aurora Serverless v2 vs provisioned?" — "Aurora Serverless v2 for variable workloads: dev/test databases, applications with off-peak idle periods, or new services with unknown traffic patterns. It scales from 0.5 to 128 ACU in seconds. Provisioned is cheaper at steady utilization above 60-70%. For data engineering: use serverless for metadata databases and analytics endpoints with business-hours-only traffic."

> **Tip 3:** "How do you perform zero-downtime schema changes on RDS?" — "Blue-green deployments. RDS creates a synchronized copy (Green), you apply schema changes to Green (DDL, indexes, engine upgrade), then switchover flips traffic in under a minute. Applications reconnect using the same endpoint. If issues arise, you can switch back. This replaced the old approach of manual replica promotion and DNS swaps."
