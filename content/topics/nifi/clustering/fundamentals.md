---
title: "NiFi Clustering - Fundamentals"
topic: nifi
subtopic: clustering
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [nifi, clustering, high-availability, scalability, zookeeper, data-engineering]
---

# NiFi Clustering — Fundamentals

## What is NiFi Clustering?

NiFi clustering allows multiple NiFi instances (nodes) to work together as a **single logical system**. Data flows are designed once and run across all nodes, providing horizontal scalability and high availability.

```mermaid
graph TD
    subgraph "NiFi Cluster (3 Nodes)"
        N1[Node 1<br>Cluster Coordinator<br>Primary Node]
        N2[Node 2<br>Worker Node]
        N3[Node 3<br>Worker Node]
    end
    
    ZK[ZooKeeper Ensemble<br>Coordination + Leader Election]
    UI[User Interface<br>Single entry point<br>Shows all nodes]
    
    ZK --> N1 & N2 & N3
    UI --> N1
    N1 <--> N2
    N1 <--> N3
    N2 <--> N3
    
    style N1 fill:#fff9c4
    style ZK fill:#e1f5fe
    style UI fill:#c8e6c9
```

## Key Cluster Roles

| Role | Description | How Many |
|------|-------------|----------|
| **Cluster Coordinator** | Manages cluster membership, handles join/leave | 1 (auto-elected) |
| **Primary Node** | Runs processors that should only run on one node | 1 (auto-elected) |
| **Worker Node** | Processes data (all nodes including coordinator/primary) | All nodes |

## Why Cluster?

| Benefit | Explanation |
|---------|-------------|
| **Scalability** | Add nodes to increase throughput |
| **High Availability** | If one node fails, others continue processing |
| **Load Distribution** | Data automatically distributed across nodes |
| **Single Management** | Design flows once, runs on all nodes |

## How Data Flows in a Cluster

```mermaid
graph LR
    subgraph "Node 1"
        K1[ConsumeKafka<br>Partitions 0-3]
        T1[Transform<br>Process locally]
    end
    
    subgraph "Node 2"
        K2[ConsumeKafka<br>Partitions 4-7]
        T2[Transform<br>Process locally]
    end
    
    subgraph "Node 3"
        K3[ConsumeKafka<br>Partitions 8-11]
        T3[Transform<br>Process locally]
    end
    
    K1 --> T1
    K2 --> T2
    K3 --> T3
    
    style K1 fill:#e1f5fe
    style K2 fill:#e1f5fe
    style K3 fill:#e1f5fe
```

**By default:** Each node runs its own instance of every processor independently. Data stays local to the node that ingested it (no unnecessary network transfer).

## Primary Node vs. All Nodes

Some processors should only run on ONE node (to avoid duplication):

```mermaid
graph TD
    subgraph "Primary Node Only"
        LIST[ListS3<br>Lists files ONCE<br>Distributes to all nodes]
        CRON[GenerateFlowFile<br>Triggers ONCE per schedule]
    end
    
    subgraph "All Nodes"
        FETCH[FetchS3Object<br>Downloads in parallel]
        TRANSFORM[ConvertRecord<br>Processes on every node]
        PUT[PutDatabaseRecord<br>Writes from every node]
    end
    
    LIST -->|"Distributes via<br>load-balanced connection"| FETCH
    
    style LIST fill:#fff9c4
    style FETCH fill:#c8e6c9
```

| Execution Strategy | Processors | Why |
|-------------------|-----------|-----|
| **Primary Node Only** | ListS3, ListSFTP, GenerateFlowFile | Avoid listing/generating duplicates |
| **All Nodes** | FetchS3Object, ConvertRecord, PutDatabaseRecord | Parallel processing for throughput |

## ZooKeeper's Role

ZooKeeper provides coordination services for the cluster:

```mermaid
graph TD
    ZK[ZooKeeper Ensemble<br>3 or 5 instances]
    
    ZK -->|"Leader election"| LE[Who is Coordinator?<br>Who is Primary?]
    ZK -->|"Cluster state"| CS[Which nodes are alive?<br>Health monitoring]
    ZK -->|"Flow synchronization"| FS[Ensure all nodes have<br>same flow definition]
    
    style ZK fill:#e1f5fe
```

```properties
# nifi.properties — ZooKeeper configuration:
nifi.cluster.is.node=true
nifi.zookeeper.connect.string=zk1:2181,zk2:2181,zk3:2181
nifi.zookeeper.root.node=/nifi
nifi.cluster.node.address=nifi-node-1
nifi.cluster.node.protocol.port=9876
```

## Basic Cluster Configuration

```properties
# nifi.properties on EACH node:

# Enable clustering:
nifi.cluster.is.node=true

# This node's identity:
nifi.cluster.node.address=nifi-node-1.company.com
nifi.cluster.node.protocol.port=9876

# ZooKeeper:
nifi.zookeeper.connect.string=zk-1:2181,zk-2:2181,zk-3:2181

# State management (cluster-wide):
nifi.state.management.provider.cluster=zk-provider

# Web UI (each node has its own port):
nifi.web.https.host=nifi-node-1.company.com
nifi.web.https.port=8443
```

## Load-Balanced Connections

To distribute work across cluster nodes:

```mermaid
graph TD
    subgraph "Node 1 (Primary)"
        LIST[ListS3<br>Primary Only<br>Finds 1000 files]
    end
    
    subgraph "Load-Balanced Connection"
        LB[Round Robin<br>Distributes FlowFiles<br>~333 per node]
    end
    
    subgraph "Node 1"
        F1[FetchS3Object<br>Fetches ~333 files]
    end
    subgraph "Node 2"
        F2[FetchS3Object<br>Fetches ~333 files]
    end
    subgraph "Node 3"
        F3[FetchS3Object<br>Fetches ~333 files]
    end
    
    LIST --> LB
    LB --> F1
    LB --> F2
    LB --> F3
    
    style LB fill:#fff9c4
```

```
# Connection → Configure → Load Balance Strategy:
Load Balance Strategy: Round Robin
Load Balance Compression: Compress Attributes and Content
# Distributes evenly across all connected nodes
# Compression reduces network traffic for inter-node transfer
```

## Cluster Node States

```mermaid
graph LR
    CONNECTING[CONNECTING<br>Joining cluster] --> CONNECTED[CONNECTED<br>Active member]
    CONNECTED -->|"Heartbeat timeout"| DISCONNECTED[DISCONNECTED<br>Removed from cluster]
    DISCONNECTED -->|"Reconnect"| CONNECTING
    CONNECTED -->|"Graceful shutdown"| OFFLOADING[OFFLOADING<br>Draining queues]
    OFFLOADING --> DISCONNECTED
    
    style CONNECTED fill:#c8e6c9
    style DISCONNECTED fill:#ffcdd2
    style OFFLOADING fill:#fff9c4
```

## Interview Tips

> **Tip 1:** "What is a NiFi cluster?" — Multiple NiFi instances working as one logical system. ZooKeeper coordinates. One node is Cluster Coordinator (manages membership), one is Primary Node (runs single-instance processors). All nodes process data in parallel. Design flows once — they run identically on all nodes.

> **Tip 2:** "Primary Node vs. All Nodes execution?" — Primary Node Only: for processors that should run once (ListS3, GenerateFlowFile) — avoids duplicate listings. All Nodes: for processing/output processors (FetchS3, ConvertRecord, PutDB) — maximizes parallelism. Use load-balanced connections to distribute work from primary-only processors to all nodes.

> **Tip 3:** "Why is ZooKeeper needed?" — Three functions: (1) Leader election (who is coordinator/primary). (2) Cluster membership (detecting node failures via heartbeats). (3) Flow synchronization (all nodes have the same flow version). Without ZooKeeper, nodes can't coordinate and may process data inconsistently.
