---
title: "AWS RDS - Real-World Production Examples"
topic: aws-services
subtopic: rds
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, rds, production, cdc-pipeline, dms, aurora-global, rds-proxy, operations]
---

# AWS RDS — Real-World Production Examples

## Pattern 1: RDS to DMS to Kinesis to S3 (Real-Time CDC Pipeline)

```python
# Architecture: RDS PostgreSQL → DMS → Kinesis Data Streams → Lambda → S3 (Parquet)
# Use case: Real-time replication of transactional data to data lake

import json
import boto3
from datetime import datetime

# DMS Task Configuration for CDC to Kinesis:
dms_task_settings = {
    'TargetMetadata': {
        'TargetSchema': '',
        'SupportLobs': True,
        'LimitedSizeLobMode': True,
        'LobMaxSize': 32,  # KB
    },
    'FullLoadSettings': {
        'TargetTablePrepMode': 'DO_NOTHING',
    },
    'StreamBufferSettings': {
        'StreamBufferCount': 3,
        'StreamBufferSizeInMB': 8,
    },
    'ChangeProcessingTuning': {
        'BatchApplyEnabled': True,
        'BatchApplyPreserveTransaction': True,
        'BatchSplitSize': 0,
        'MinTransactionSize': 1000,
        'CommitTimeout': 1,  # seconds
        'MemoryLimitTotal': 1024,  # MB
    }
}

# Table mapping: capture all DML on orders and customers tables
table_mappings = {
    'rules': [
        {
            'rule-type': 'selection',
            'rule-id': '1',
            'rule-action': 'include',
            'object-locator': {
                'schema-name': 'public',
                'table-name': 'orders'
            }
        },
        {
            'rule-type': 'selection',
            'rule-id': '2',
            'rule-action': 'include',
            'object-locator': {
                'schema-name': 'public',
                'table-name': 'customers'
            }
        },
        {
            # Add metadata columns
            'rule-type': 'transformation',
            'rule-id': '3',
            'rule-action': 'add-column',
            'rule-target': 'column',
            'object-locator': {'schema-name': 'public', 'table-name': '%'},
            'value': 'cdc_timestamp',
            'expression': '$AR_H_CHANGE_SEQ',
            'data-type': {'type': 'string', 'length': 50}
        }
    ]
}


# Lambda consumer: Kinesis → Parquet → S3
import pyarrow as pa
import pyarrow.parquet as pq
import io

def kinesis_to_s3_handler(event, context):
    """Process CDC records from Kinesis and write to S3 as Parquet."""
    
    records_by_table = {}
    
    for record in event['Records']:
        payload = json.loads(
            base64.b64decode(record['kinesis']['data']).decode('utf-8')
        )
        
        # DMS CDC record format:
        # {
        #   "data": {"order_id": "123", "amount": 99.99, ...},
        #   "metadata": {
        #     "operation": "insert|update|delete",
        #     "schema-name": "public",
        #     "table-name": "orders",
        #     "timestamp": "2024-01-15T10:30:00Z"
        #   }
        # }
        
        table_name = payload['metadata']['table-name']
        if table_name not in records_by_table:
            records_by_table[table_name] = []
        
        cdc_record = {
            **payload['data'],
            '_operation': payload['metadata']['operation'],
            '_cdc_timestamp': payload['metadata']['timestamp'],
            '_sequence': record['kinesis']['sequenceNumber']
        }
        records_by_table[table_name].append(cdc_record)
    
    # Write each table's changes as Parquet to S3
    s3 = boto3.client('s3')
    now = datetime.utcnow()
    
    for table_name, records in records_by_table.items():
        df_table = pa.Table.from_pylist(records)
        buffer = io.BytesIO()
        pq.write_table(df_table, buffer, compression='snappy')
        
        s3_key = (
            f"cdc/{table_name}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/{context.aws_request_id}.parquet"
        )
        
        s3.put_object(
            Bucket='data-lake-raw',
            Key=s3_key,
            Body=buffer.getvalue()
        )
    
    return {'tables_processed': list(records_by_table.keys()),
            'total_records': sum(len(r) for r in records_by_table.values())}
```

---

## Pattern 2: Read Replica for Analytics Workloads

```python
# Architecture: Production RDS → Read Replica → Analytics team queries
# Use case: Heavy analytical queries without impacting transactional workload

import psycopg2
from psycopg2 import pool
import time

class AnalyticsDB:
    """Connection manager for analytics read replica with monitoring."""
    
    def __init__(self):
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=20,
            host='analytics-replica.abc123.us-east-1.rds.amazonaws.com',
            dbname='production',
            user='analytics_user',
            password=self._get_secret(),
            options='-c statement_timeout=300000'  # 5 min timeout
        )
    
    def execute_analytics_query(self, query: str, params: tuple = None) -> list:
        """Execute read-only analytics query with monitoring."""
        conn = self.pool.getconn()
        try:
            conn.set_session(readonly=True)  # Enforce read-only
            cursor = conn.cursor()
            
            start = time.time()
            cursor.execute(query, params)
            results = cursor.fetchall()
            duration = time.time() - start
            
            # Log slow queries for optimization
            if duration > 30:
                self._log_slow_query(query, duration, len(results))
            
            return results
        finally:
            self.pool.putconn(conn)
    
    def check_replication_lag(self) -> float:
        """Monitor replication lag (stale data risk)."""
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) 
                AS lag_seconds
            """)
            lag = cursor.fetchone()[0]
            
            if lag > 60:
                print(f"WARNING: Replica lag is {lag:.1f} seconds")
            return lag
        finally:
            self.pool.putconn(conn)


# Typical analytics queries on read replica:
analytics = AnalyticsDB()

# Daily revenue report (would block production if run on primary)
revenue = analytics.execute_analytics_query("""
    SELECT 
        date_trunc('hour', created_at) AS hour,
        product_category,
        COUNT(*) AS order_count,
        SUM(total_amount) AS revenue,
        AVG(total_amount) AS avg_order_value
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1, 2
    ORDER BY 1 DESC, 4 DESC
""")

# Customer lifetime value calculation (heavy join + aggregation)
ltv = analytics.execute_analytics_query("""
    WITH customer_orders AS (
        SELECT 
            customer_id,
            MIN(created_at) AS first_order,
            MAX(created_at) AS last_order,
            COUNT(*) AS total_orders,
            SUM(total_amount) AS lifetime_value
        FROM orders
        WHERE status = 'completed'
        GROUP BY customer_id
    )
    SELECT 
        date_trunc('month', first_order) AS cohort_month,
        COUNT(*) AS customers,
        AVG(lifetime_value) AS avg_ltv,
        AVG(total_orders) AS avg_orders,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) AS median_ltv
    FROM customer_orders
    GROUP BY 1
    ORDER BY 1
""")
```

---

## Pattern 3: Aurora Global Database for Multi-Region DR

```python
# Architecture: Aurora PostgreSQL (us-east-1) → Global Database → (eu-west-1)
# Use case: Disaster recovery with <1 minute RTO and <1 second RPO

import boto3
import time

class GlobalDatabaseManager:
    """Manage Aurora Global Database for DR operations."""
    
    def __init__(self):
        self.rds_primary = boto3.client('rds', region_name='us-east-1')
        self.rds_secondary = boto3.client('rds', region_name='eu-west-1')
    
    def check_replication_health(self):
        """Monitor global database replication status."""
        response = self.rds_primary.describe_global_clusters(
            GlobalClusterIdentifier='production-global-db'
        )
        
        cluster = response['GlobalClusters'][0]
        members = cluster['GlobalClusterMembers']
        
        health = {}
        for member in members:
            region = member['DBClusterArn'].split(':')[3]
            health[region] = {
                'role': 'primary' if member['IsWriter'] else 'secondary',
                'lag_ms': member.get('GlobalWriteForwardingStatus', 'N/A'),
                'state': member.get('ReaderResourceStatus', 'available')
            }
        
        return health
    
    def planned_failover(self, target_region: str):
        """Execute planned failover (zero data loss)."""
        
        # Step 1: Verify target cluster is healthy
        health = self.check_replication_health()
        assert health[target_region]['state'] == 'available'
        
        # Step 2: Execute managed failover
        target_arn = self._get_cluster_arn(target_region)
        
        self.rds_primary.failover_global_cluster(
            GlobalClusterIdentifier='production-global-db',
            TargetDbClusterIdentifier=target_arn
        )
        
        # Step 3: Wait for completion
        while True:
            health = self.check_replication_health()
            if health[target_region]['role'] == 'primary':
                break
            time.sleep(10)
        
        return {'new_primary': target_region, 'status': 'complete'}
    
    def unplanned_failover(self, target_region: str):
        """Emergency failover when primary region is unavailable."""
        
        # Detach secondary (becomes standalone)
        rds_target = boto3.client('rds', region_name=target_region)
        target_arn = self._get_cluster_arn(target_region)
        
        rds_target.remove_from_global_cluster(
            GlobalClusterIdentifier='production-global-db',
            DbClusterIdentifier=target_arn
        )
        
        # Secondary is now an independent writable cluster
        # Update application endpoints (Route 53, parameter store, etc.)
        return {
            'new_primary': target_region,
            'data_loss': 'possible (last seconds of transactions)',
            'action_required': 'Update application connection strings'
        }


# Monitoring: CloudWatch alarm on replication lag
# AuroraGlobalDBReplicationLag > 5000ms → alert on-call
# AuroraGlobalDBRPOLag > 1s → potential data loss on failover
```

---

## Pattern 4: RDS Proxy for Lambda Connection Management

```python
# Architecture: Lambda (1000+ concurrent) → RDS Proxy (50 connections) → Aurora
# Use case: Serverless data API that queries relational data

import json
import boto3
import psycopg2

# RDS Proxy handles:
# 1. Connection pooling (Lambda concurrency → limited DB connections)
# 2. IAM authentication (no passwords in Lambda code)
# 3. Faster failover (maintains connections during Aurora failover)

PROXY_ENDPOINT = 'data-api-proxy.proxy-abc123.us-east-1.rds.amazonaws.com'

def get_iam_auth_token():
    """Generate IAM authentication token (15-minute validity)."""
    rds_client = boto3.client('rds')
    token = rds_client.generate_db_auth_token(
        DBHostname=PROXY_ENDPOINT,
        Port=5432,
        DBUsername='lambda_user',
        Region='us-east-1'
    )
    return token

def handler(event, context):
    """Lambda data API endpoint using RDS Proxy with IAM auth."""
    
    # IAM auth: no password stored anywhere
    token = get_iam_auth_token()
    
    conn = psycopg2.connect(
        host=PROXY_ENDPOINT,
        port=5432,
        dbname='production',
        user='lambda_user',
        password=token,
        sslmode='require',
        connect_timeout=5
    )
    
    try:
        cursor = conn.cursor()
        
        # Parse API request
        path = event.get('path', '')
        params = event.get('queryStringParameters', {}) or {}
        
        if path == '/api/orders/summary':
            cursor.execute("""
                SELECT order_date, COUNT(*) as orders, SUM(amount) as revenue
                FROM orders
                WHERE order_date >= %s AND order_date <= %s
                GROUP BY order_date
                ORDER BY order_date DESC
            """, (params.get('start_date', '2024-01-01'), 
                  params.get('end_date', '2024-01-31')))
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(results, default=str)
            }
    finally:
        conn.close()


# RDS Proxy configuration:
proxy_config = {
    'DBProxyName': 'data-api-proxy',
    'EngineFamily': 'POSTGRESQL',
    'Auth': [{
        'AuthScheme': 'SECRETS',
        'SecretArn': 'arn:aws:secretsmanager:us-east-1:123:secret:rds-creds',
        'IAMAuth': 'REQUIRED'  # Force IAM authentication
    }],
    'IdleClientTimeout': 1800,  # Close idle connections after 30 min
    'MaxConnectionsPercent': 80,  # Use up to 80% of max_connections
    'MaxIdleConnectionsPercent': 20,  # Keep 20% idle for bursts
    'ConnectionBorrowTimeout': 120,  # Wait up to 120s for a connection
}
```

---

## Production Operations Checklist

| Area | Check | Frequency |
|------|-------|-----------|
| Performance | Top 10 slow queries (Performance Insights) | Daily |
| Replication | Replica lag < 5 seconds | Continuous |
| Storage | Free space > 20% | Weekly |
| Connections | Active connections < 80% of max | Continuous |
| Backups | PITR enabled, 14+ day retention | Monthly verify |
| Security | SSL enforced, no public access | Monthly audit |
| Cost | Instance utilization > 40% (right-sized) | Monthly |
| Failover | Test Multi-AZ failover | Quarterly |
| DR | Global database replication lag | Continuous |
| Patches | Minor version within 2 of latest | Monthly |

```python
# Automated health check script:
def rds_health_check(instance_id: str):
    """Run production health checks on RDS instance."""
    rds = boto3.client('rds')
    cw = boto3.client('cloudwatch')
    
    # Instance status
    instance = rds.describe_db_instances(DBInstanceIdentifier=instance_id)['DBInstances'][0]
    
    checks = {
        'status': instance['DBInstanceStatus'],
        'multi_az': instance['MultiAZ'],
        'encrypted': instance['StorageEncrypted'],
        'auto_minor_upgrade': instance['AutoMinorVersionUpgrade'],
        'backup_retention': instance['BackupRetentionPeriod'],
        'storage_free_pct': get_metric(cw, instance_id, 'FreeStorageSpace') / 
                           (instance['AllocatedStorage'] * 1024**3) * 100,
        'cpu_avg': get_metric(cw, instance_id, 'CPUUtilization'),
        'connections': get_metric(cw, instance_id, 'DatabaseConnections'),
        'replica_lag': get_metric(cw, instance_id, 'ReplicaLag') if not instance['MultiAZ'] else 'N/A',
    }
    
    return checks
```

---

## Interview Tips

> **Tip 1:** "Design a real-time CDC pipeline from RDS to a data lake" — "RDS PostgreSQL with DMS reading the WAL (write-ahead log). DMS streams CDC events to Kinesis Data Streams (ordered, replayable). Lambda processes Kinesis batches: deserializes CDC records, adds metadata (operation type, timestamp), converts to Parquet, writes to S3 partitioned by table/date/hour. End-to-end latency is 5-30 seconds. For simpler setups, DMS can write directly to S3, but you lose ordering guarantees."

> **Tip 2:** "How do you handle Lambda to RDS connection exhaustion?" — "RDS Proxy. Without it, each Lambda invocation opens a new database connection. At 1000 concurrent Lambdas, you hit the max_connections limit and get connection refused errors. RDS Proxy pools connections: 1000 Lambda invocations share 50-100 actual database connections. Also use IAM auth through the proxy (no secrets in Lambda env vars) and benefit from faster Multi-AZ failover."

> **Tip 3:** "Aurora Global Database vs cross-region read replica — when to use each?" — "Aurora Global Database for true DR: automatic managed replication, <1 second lag, managed failover in under a minute. Cross-region read replica for read scaling in another region (serve reads locally, reduce latency). Global Database is the right choice when RTO and RPO are critical business requirements. Cross-region replica is simpler but manual failover and potential minutes of data loss."
