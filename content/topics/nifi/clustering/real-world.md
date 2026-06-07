---
title: "NiFi Clustering - Real-World Production Examples"
topic: nifi
subtopic: clustering
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [nifi, clustering, production, deployment, operations, monitoring]
---

# NiFi Clustering — Real-World Production Examples

## Example 1: AWS Production Cluster Architecture

```mermaid
graph TD
    subgraph "VPC (us-east-1)"
        subgraph "AZ-a"
            N1[NiFi Node 1<br>r5.2xlarge<br>8 CPU, 64GB RAM]
            ZK1[ZooKeeper 1<br>t3.medium]
        end
        subgraph "AZ-b"
            N2[NiFi Node 2<br>r5.2xlarge]
            ZK2[ZooKeeper 2]
        end
        subgraph "AZ-c"
            N3[NiFi Node 3<br>r5.2xlarge]
            ZK3[ZooKeeper 3]
        end
        
        ALB[Application Load Balancer<br>HTTPS:8443 → NiFi UI<br>Sticky sessions]
        EFS[EFS (shared storage)<br>NiFi Registry, drivers]
    end
    
    subgraph "External"
        KAFKA[MSK (Kafka)]
        S3[S3 Buckets]
        RDS[RDS PostgreSQL]
        SF[Snowflake]
    end
    
    ALB --> N1 & N2 & N3
    ZK1 & ZK2 & ZK3 --> N1 & N2 & N3
    N1 & N2 & N3 --> KAFKA & S3 & RDS & SF
    
    style ALB fill:#e1f5fe
    style N1 fill:#c8e6c9
```

### Infrastructure Configuration

```
# Instance sizing:
# r5.2xlarge: 8 vCPU, 64 GB RAM ($0.504/hr on-demand)
# Storage per node:
#   /opt/nifi/flowfile-repository: 100 GB gp3 SSD (IOPS: 10,000)
#   /opt/nifi/content-repository: 500 GB gp3 SSD (IOPS: 5,000)
#   /opt/nifi/provenance-repository: 200 GB gp3 (IOPS: 3,000)

# JVM settings (nifi-env.sh):
JAVA_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
# 16 GB heap (25% of 64 GB RAM)
# Remaining 48 GB: OS page cache for content repository reads

# nifi.properties key settings:
nifi.content.repository.directory.partition1=/mnt/nvme1/content-repo
nifi.content.repository.directory.partition2=/mnt/nvme2/content-repo
nifi.provenance.repository.max.storage.size=20 GB
nifi.provenance.repository.max.storage.time=7 days
```

## Example 2: Cluster Monitoring with Prometheus + Grafana

```yaml
# NiFi PrometheusReportingTask configuration:
# (Exposes /metrics endpoint for Prometheus scraping)

# Prometheus scrape config:
scrape_configs:
  - job_name: 'nifi'
    scheme: https
    tls_config:
      ca_file: /etc/prometheus/certs/nifi-ca.pem
    static_configs:
      - targets:
        - nifi-node-1:9092
        - nifi-node-2:9092
        - nifi-node-3:9092
    metrics_path: /metrics
```

```
# Key metrics to dashboard:

# Cluster health:
nifi_cluster_nodes_total
nifi_cluster_nodes_connected
nifi_cluster_nodes_disconnected

# Throughput:
nifi_processor_bytes_read_total{processor="ConsumeKafka"}
nifi_processor_bytes_written_total{processor="PutDatabaseRecord"}
nifi_processor_flowfiles_received_total
nifi_processor_flowfiles_sent_total

# Queue depths (back pressure indicator):
nifi_connection_queued_count{connection_name="*"}
nifi_connection_queued_bytes{connection_name="*"}
nifi_connection_backpressure_pct{connection_name="*"}

# JVM health per node:
nifi_jvm_heap_used_bytes
nifi_jvm_gc_pause_seconds
nifi_jvm_thread_count

# Processing latency:
nifi_processor_processing_nanos{quantile="0.99"}
```

### Grafana Alert Rules

```yaml
# Alert: Cluster node disconnected
- alert: NiFiNodeDisconnected
  expr: nifi_cluster_nodes_disconnected > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "NiFi cluster has disconnected nodes"

# Alert: Back pressure active
- alert: NiFiBackPressure
  expr: nifi_connection_backpressure_pct > 90
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "NiFi connection {{ $labels.connection_name }} at {{ $value }}% capacity"

# Alert: High GC pause
- alert: NiFiHighGC
  expr: rate(nifi_jvm_gc_pause_seconds_sum[5m]) > 0.1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "NiFi node {{ $labels.instance }} spending >10% time in GC"
```

## Example 3: Rolling Upgrade Procedure

```bash
#!/bin/bash
# Rolling upgrade: 1.24.0 → 1.25.0 (zero downtime)

NODES=("nifi-node-1" "nifi-node-2" "nifi-node-3")
NEW_VERSION="1.25.0"

for node in "${NODES[@]}"; do
    echo "=== Upgrading $node ==="
    
    # 1. Offload the node (drain queues to other nodes)
    echo "Offloading $node..."
    curl -X PUT "https://$node:8443/nifi-api/controller/cluster/nodes/${node_id}" \
        -H "Content-Type: application/json" \
        -d '{"node": {"status": "OFFLOADING"}}'
    
    # 2. Wait for offload to complete
    while true; do
        status=$(curl -s "https://$node:8443/nifi-api/controller/cluster/nodes/${node_id}" \
            | jq -r '.node.status')
        [ "$status" = "OFFLOADED" ] && break
        echo "  Waiting for offload... (status: $status)"
        sleep 10
    done
    
    # 3. Stop NiFi
    ssh "$node" "systemctl stop nifi"
    
    # 4. Upgrade binary
    ssh "$node" "
        mv /opt/nifi /opt/nifi-old
        tar xzf /tmp/nifi-${NEW_VERSION}.tar.gz -C /opt/
        mv /opt/nifi-${NEW_VERSION} /opt/nifi
        cp /opt/nifi-old/conf/nifi.properties /opt/nifi/conf/
        cp /opt/nifi-old/conf/state-management.xml /opt/nifi/conf/
        cp -r /opt/nifi-old/certs /opt/nifi/
    "
    
    # 5. Start NiFi (auto-joins cluster)
    ssh "$node" "systemctl start nifi"
    
    # 6. Wait for node to rejoin
    while true; do
        status=$(curl -s "https://nifi-lb:8443/nifi-api/controller/cluster" \
            | jq -r ".cluster.nodes[] | select(.address==\"$node\") | .status")
        [ "$status" = "CONNECTED" ] && break
        echo "  Waiting for $node to rejoin... (status: $status)"
        sleep 15
    done
    
    echo "=== $node upgraded successfully ==="
    echo "Waiting 60s before next node..."
    sleep 60
done

echo "=== All nodes upgraded to ${NEW_VERSION} ==="
```

## Example 4: Production Operational Runbook

```
# Common operational procedures:

# Add a node:
1. Provision instance (same specs as existing)
2. Install NiFi, copy cluster configs (nifi.properties, certs)
3. Start NiFi → auto-joins cluster via ZooKeeper
4. Verify in UI: Cluster page shows new node as CONNECTED
5. Load-balanced connections automatically include new node

# Remove a node (graceful):
1. UI → Cluster → Select node → "Offload"
2. Wait for queues to drain (FlowFiles move to other nodes)
3. Verify: node shows "OFFLOADED" status
4. Stop NiFi service on that node
5. Terminate instance

# Handle stuck node:
1. Check bulletin board for errors
2. Check JVM heap (GC issues?)
3. Check disk space (content repo full?)
4. If unresponsive: disconnect via cluster page → restart NiFi service
5. If data stuck: check connection queues → empty via provenance replay

# Recover from ZooKeeper failure:
1. NiFi nodes will show "DISCONNECTING" state
2. Fix ZooKeeper (restart, restore from snapshot)
3. NiFi nodes auto-reconnect when ZK is back
4. Verify: all nodes CONNECTED, processors resume
5. Check: no duplicate processing (Kafka offsets are safe)
```

## Interview Tips

> **Tip 1:** "How do you deploy NiFi in production on AWS?" — 3+ nodes (r5.2xlarge) across availability zones. ALB for UI access with sticky sessions. Dedicated EBS volumes: gp3 SSD for FlowFile repo (fast IOPS), larger gp3 for content repo. ZooKeeper ensemble (3 nodes) on separate instances. 16 GB JVM heap, rest for OS page cache. IAM roles for S3/services (no keys in config).

> **Tip 2:** "How do you perform zero-downtime upgrades?" — Rolling upgrade: one node at a time. (1) Offload node (drain queues). (2) Stop and upgrade binary. (3) Start (auto-joins cluster). (4) Wait for CONNECTED. (5) Repeat for next node. At all times: N-1 nodes processing → no downtime. NiFi handles mixed versions briefly during upgrade (backward compatible within minor versions).

> **Tip 3:** "What monitoring do you put on a NiFi cluster?" — Prometheus scraping each node's /metrics. Dashboard: cluster health (connected nodes), throughput (bytes/sec), queue depths (back pressure), JVM health (heap, GC). Alerts: node disconnected (P1), back pressure >90% for 5min (P2), GC >10% time (P2), disk >80% (P2). Plus NiFi's built-in bulletin board for processor errors.
