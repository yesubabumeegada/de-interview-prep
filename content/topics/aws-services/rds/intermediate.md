---
title: "AWS RDS - Intermediate"
topic: aws-services
subtopic: rds
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, rds, read-replicas, multi-az, aurora, rds-proxy, encryption, performance]
---

# AWS RDS — Intermediate Concepts

## Read Replicas — Scaling Reads

Read replicas provide horizontal scaling for read-heavy workloads by replicating data asynchronously:

```python
import boto3
import psycopg2
from contextlib import contextmanager

# Architecture: writes → primary, reads → replica(s)
# Use case: Offload analytics queries from production OLTP database

PRIMARY_HOST = 'mydb-primary.abc123.us-east-1.rds.amazonaws.com'
REPLICA_HOST = 'mydb-replica.abc123.us-east-1.rds.amazonaws.com'

@contextmanager
def get_read_connection():
    """Connect to read replica for analytics queries."""
    conn = psycopg2.connect(
        host=REPLICA_HOST,
        dbname='production',
        user='analytics_reader',
        password=get_secret('rds-reader-password')
    )
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_write_connection():
    """Connect to primary for writes."""
    conn = psycopg2.connect(
        host=PRIMARY_HOST,
        dbname='production',
        user='app_writer',
        password=get_secret('rds-writer-password')
    )
    try:
        yield conn
    finally:
        conn.close()

# Analytics query runs on replica (no impact on production writes)
def get_daily_revenue(date: str):
    with get_read_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT product_category, SUM(amount) as revenue, COUNT(*) as orders
            FROM orders
            WHERE order_date = %s
            GROUP BY product_category
            ORDER BY revenue DESC
        """, (date,))
        return cursor.fetchall()
```

**Read replica characteristics:**

| Property | Value |
|----------|-------|
| Max replicas | 5 (RDS), 15 (Aurora) |
| Replication lag | Seconds (async), milliseconds (Aurora) |
| Cross-region | Supported (additional latency) |
| Promotion | Can promote to standalone primary |
| Engine support | MySQL, PostgreSQL, MariaDB |
| Pricing | Same as primary (full instance cost) |

---

## Multi-AZ — High Availability

```python
# Multi-AZ: synchronous replication to standby in different AZ
# Automatic failover in 1-2 minutes if primary fails

# Multi-AZ is NOT a read scaling solution (standby is not readable)
# Use case: production databases that cannot tolerate downtime

# Multi-AZ deployment options:
deployment_options = {
    'Multi-AZ instance': {
        'standbys': 1,
        'failover_time': '1-2 minutes',
        'readable': False,
        'cost': '2x (primary + standby)',
    },
    'Multi-AZ cluster (Aurora)': {
        'standbys': '2 reader instances',
        'failover_time': '<30 seconds',
        'readable': True,  # Readers serve read traffic
        'cost': '3x (writer + 2 readers)',
    }
}

# Failover triggers:
# - Primary instance failure
# - AZ failure
# - Manual failover (maintenance, testing)
# - Instance type change
# - Software patching
```

> **Key distinction:** Multi-AZ = availability (automatic failover). Read Replicas = scalability (distribute read load). For production data pipelines, use both: Multi-AZ for the primary, read replicas for analytics queries.

---

## Automated Backups and Point-in-Time Recovery (PITR)

```python
# RDS automated backups: daily snapshots + transaction logs
# Retention: 1-35 days (configurable)
# PITR: restore to any second within retention window

import boto3

rds = boto3.client('rds')

# Restore to specific point in time (creates new instance)
response = rds.restore_db_instance_to_point_in_time(
    SourceDBInstanceIdentifier='production-db',
    TargetDBInstanceIdentifier='production-db-restored-20240115',
    RestoreTime='2024-01-15T10:30:00Z',  # Any second in retention window
    DBInstanceClass='db.r6g.xlarge',
    MultiAZ=False,  # Restore as single-AZ for investigation
    PubliclyAccessible=False
)

# Backup window: pick low-traffic period (e.g., 3-4 AM)
# During backup: brief I/O pause on single-AZ (Multi-AZ: no impact)
# Storage: backups stored in S3 (managed by AWS, not in your account)
```

**Backup strategy for data engineering:**
- Automated backups: 14-day retention for production databases
- Manual snapshots: before major schema changes or migrations
- Cross-region snapshot copy: for disaster recovery
- Export to S3: for data lake integration (Parquet format)

---

## RDS Proxy — Connection Pooling

```python
# Problem: Lambda functions open too many connections (1000 concurrent = 1000 connections)
# Solution: RDS Proxy pools and reuses connections

# Without proxy: Lambda → direct connection to RDS (connection per invocation)
# With proxy: Lambda → RDS Proxy (pool of 50 connections) → RDS

# RDS Proxy endpoint replaces direct RDS endpoint:
PROXY_ENDPOINT = 'mydb-proxy.proxy-abc123.us-east-1.rds.amazonaws.com'

def lambda_handler(event, context):
    """Lambda connects through RDS Proxy for connection pooling."""
    conn = psycopg2.connect(
        host=PROXY_ENDPOINT,  # Proxy endpoint instead of RDS
        dbname='production',
        user='lambda_user',
        password=get_secret('rds-password'),
        connect_timeout=5
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders WHERE date = %s", (event['date'],))
    count = cursor.fetchone()[0]
    conn.close()
    
    return {'count': count}
```

**RDS Proxy benefits:**

| Benefit | Explanation |
|---------|-------------|
| Connection pooling | 1000 Lambda invocations share 50 DB connections |
| Faster failover | Proxy maintains connections during Multi-AZ failover |
| IAM authentication | Use IAM roles instead of passwords |
| Connection multiplexing | Reuses idle connections for new requests |

**Cost:** ~$0.015/hr per vCPU of the associated RDS instance (roughly 15% of DB cost)

---

## Parameter Groups

```python
# Parameter groups control database engine configuration
# Two types: DB parameter group (instance) and DB cluster parameter group (Aurora)

# Common tuning parameters for PostgreSQL data workloads:
postgres_params = {
    # Memory
    'shared_buffers': '{DBInstanceClassMemory/4}',  # 25% of instance memory
    'effective_cache_size': '{DBInstanceClassMemory*3/4}',  # 75% of memory
    'work_mem': '256MB',  # Per-operation sort memory (increase for analytics)
    'maintenance_work_mem': '1GB',  # For VACUUM, CREATE INDEX
    
    # Query planner
    'random_page_cost': '1.1',  # SSD storage (default 4.0 is for HDD)
    'effective_io_concurrency': '200',  # Concurrent I/O operations
    
    # Write performance
    'checkpoint_timeout': '15min',  # Less frequent checkpoints
    'max_wal_size': '4GB',  # Allow more WAL before checkpoint
    
    # Connections
    'max_connections': '200',  # Based on instance size
    
    # Logging (for slow query analysis)
    'log_min_duration_statement': '1000',  # Log queries > 1 second
    'log_statement': 'ddl',  # Log DDL statements
}
```

---

## Performance Insights

```python
# Performance Insights: built-in monitoring for query-level performance
# Shows top SQL queries, wait events, and resource bottlenecks

# Key wait events to watch:
wait_events = {
    'CPU': 'Query is compute-bound (optimize query or scale up)',
    'IO:DataFileRead': 'Reading from disk (add memory for caching)',
    'IO:WALWrite': 'Write-ahead log (IO bottleneck on writes)',
    'Lock:transactionid': 'Row-level lock contention (check transactions)',
    'LWLock:BufferMapping': 'Shared buffer contention (reduce connection count)',
    'Client:ClientRead': 'Waiting for client (network or slow app)',
}

# Using Performance Insights API:
pi = boto3.client('pi')

response = pi.get_resource_metrics(
    ServiceType='RDS',
    Identifier='db-ABC123',
    MetricQueries=[{
        'Metric': 'db.load.avg',
        'GroupBy': {'Group': 'db.sql', 'Limit': 10}
    }],
    StartTime=datetime(2024, 1, 15, 0, 0),
    EndTime=datetime(2024, 1, 15, 23, 59),
    PeriodInSeconds=3600
)
# Returns top 10 SQL statements by average active sessions
```

---

## Aurora vs Standard RDS

| Feature | Standard RDS | Aurora |
|---------|-------------|--------|
| Storage | EBS (gp3/io2), manual sizing | Auto-scaling (10 GB to 128 TB) |
| Replication | Async (seconds lag) | Shared storage (milliseconds) |
| Read replicas | 5 max | 15 max |
| Failover time | 1-2 minutes | <30 seconds |
| Backtrack | No | Yes (rewind without restore) |
| Cloning | Snapshot + restore (slow) | Instant (copy-on-write) |
| Serverless | No | Aurora Serverless v2 |
| Performance | Baseline | 3-5x PostgreSQL throughput (AWS claim) |
| Cost | Instance + storage + IOPS | Instance + storage + I/O requests |
| Best for | Small-medium databases | Production, high availability |

---

## Encryption at Rest and In Transit

```python
# Encryption at rest: enabled at creation (cannot add later to existing instance)
# Uses AWS KMS key for AES-256 encryption
# Encrypts: storage, automated backups, snapshots, read replicas

# Encryption in transit: SSL/TLS connections
# Force SSL in parameter group:
# rds.force_ssl = 1 (PostgreSQL)
# require_secure_transport = ON (MySQL)

# Python connection with SSL:
conn = psycopg2.connect(
    host=RDS_HOST,
    dbname='production',
    user='app_user',
    password=get_secret('rds-password'),
    sslmode='require',  # or 'verify-full' for certificate validation
    sslrootcert='/path/to/rds-ca-bundle.pem'
)

# Verify encryption status:
rds_client = boto3.client('rds')
response = rds_client.describe_db_instances(DBInstanceIdentifier='production-db')
encrypted = response['DBInstances'][0]['StorageEncrypted']  # True/False
```

---

## Interview Tips

> **Tip 1:** "How do you scale reads for analytics without impacting production?" — "Create read replicas and route all analytics queries there. The replica receives async replication (seconds lag, milliseconds for Aurora). This isolates the production primary for transactional writes while giving data engineers a full copy to query. For heavy analytics, consider exporting to S3 via RDS snapshot export and querying with Athena instead."

> **Tip 2:** "Multi-AZ vs Read Replicas — what is the difference?" — "Multi-AZ is about availability: a synchronous standby that automatically takes over on failure (not readable). Read replicas are about scalability: asynchronous copies that serve read traffic. You typically use both together: Multi-AZ for HA on the primary, read replicas for distributing read load. Multi-AZ failover takes 1-2 minutes (30 seconds for Aurora)."

> **Tip 3:** "Why use RDS Proxy with Lambda?" — "Lambda can create thousands of concurrent connections to RDS (one per invocation). RDS has connection limits (typically 200-5000 depending on instance size). RDS Proxy sits in between, maintaining a pool of reusable database connections. 1000 Lambda invocations share maybe 50 actual DB connections. It also provides faster failover and IAM-based authentication."
