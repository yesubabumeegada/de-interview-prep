---
title: "NiFi Back Pressure - Fundamentals"
topic: nifi
subtopic: back-pressure
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [nifi, back-pressure, flow-control, connections, queues, data-engineering]
---

# NiFi Back Pressure — Fundamentals


## 🎯 Analogy

Think of NiFi back-pressure like a water reservoir with safety valves: when a connection queue fills up (10,000 FlowFiles or 1 GB default), NiFi stops the upstream processor from producing more — preventing memory exhaustion downstream.

---
## What is Back Pressure?

Back pressure is NiFi's **built-in flow control mechanism** that prevents a fast producer from overwhelming a slow consumer. When a connection queue fills up to its threshold, NiFi automatically **pauses the upstream processor** until the queue drains.

```mermaid
graph LR
    FAST[Fast Producer<br>10,000 FlowFiles/sec<br>GetKafka] -->|"Connection Queue<br>Threshold: 10,000<br>Current: 9,500"| SLOW[Slow Consumer<br>1,000 FlowFiles/sec<br>PutDatabaseRecord]
    
    style FAST fill:#c8e6c9
    style SLOW fill:#ffcdd2
```

**Without back pressure:** The queue grows infinitely → memory exhaustion → system crash.

**With back pressure:** When queue reaches threshold → upstream pauses → no memory overflow.

## How Back Pressure Works

```mermaid
graph TD
    subgraph "Normal Operation"
        N1[Upstream produces<br>FlowFiles flow freely]
        N2[Queue below threshold<br>✓ No back pressure]
        N3[Downstream consumes<br>at its own pace]
    end
    
    subgraph "Back Pressure Active"
        B1[Upstream PAUSED<br>Stops producing]
        B2[Queue at threshold<br>⚠ Back pressure engaged]
        B3[Downstream continues<br>consuming to drain queue]
    end
    
    subgraph "Recovery"
        R1[Upstream resumes<br>Produces again]
        R2[Queue drops below threshold<br>✓ Back pressure released]
        R3[Normal flow resumes]
    end
    
    N1 --> N2 --> N3
    N3 -->|"Queue fills"| B1
    B1 --> B2 --> B3
    B3 -->|"Queue drains"| R1
    R1 --> R2 --> R3
    
    style N2 fill:#c8e6c9
    style B2 fill:#ffcdd2
    style R2 fill:#c8e6c9
```

## Connection Settings

Every connection (queue) between processors has two back pressure thresholds:

| Setting | Default | Description |
|---------|---------|-------------|
| **Back Pressure Object Threshold** | 10,000 | Max FlowFiles in queue before pausing upstream |
| **Back Pressure Data Size Threshold** | 1 GB | Max data size in queue before pausing upstream |

Whichever threshold is hit **first** triggers back pressure.

```mermaid
graph LR
    subgraph "Connection Configuration"
        QUEUE["Queue Settings:<br>Object Threshold: 10,000 FlowFiles<br>Data Size Threshold: 1 GB<br>---<br>Current: 8,500 FlowFiles (200 MB)<br>Status: ✓ Normal"]
    end
    
    style QUEUE fill:#e1f5fe
```

## Visual Indicators in NiFi UI

```mermaid
graph LR
    P1[Processor A] -->|"Green bar<br>Queue OK"| P2[Processor B]
    P3[Processor C] -->|"Yellow/Red bar<br>⚠ Back pressure!"| P4[Processor D]
    
    style P1 fill:#c8e6c9
    style P2 fill:#c8e6c9
    style P3 fill:#fff9c4
    style P4 fill:#ffcdd2
```

In the NiFi UI:
- **Green connection**: Queue well below threshold
- **Yellow connection**: Queue approaching threshold (warning)
- **Red connection**: Back pressure active (upstream paused)
- Queue size shown on connection label: "5,000 / 10,000"

## Why Back Pressure Matters

| Without Back Pressure | With Back Pressure |
|----------------------|-------------------|
| Memory overflow (OOM) | Memory stays bounded |
| System crash | System stays healthy |
| Data loss possible | No data loss |
| Cascading failures | Graceful degradation |
| Unpredictable behavior | Predictable, controlled flow |

## Common Scenarios

### Scenario 1: Database Slower Than Kafka

```mermaid
graph LR
    K[ConsumeKafka<br>50,000 msg/sec] -->|"Queue: 10K threshold<br>Back pressure when full"| DB[PutDatabaseRecord<br>5,000 rows/sec]
    
    style K fill:#e1f5fe
    style DB fill:#ffcdd2
```

Kafka produces at 50K/sec, database handles only 5K/sec.
- Without BP: Queue grows to millions → memory crash
- With BP: Queue fills to 10K → Kafka consumer pauses → queue drains → resumes

### Scenario 2: API Rate Limiting

```mermaid
graph LR
    GEN[GenerateFlowFile<br>Continuous] -->|"Queue: 100 threshold"| API[InvokeHTTP<br>Rate limited to<br>60 calls/min]
    
    style GEN fill:#e1f5fe
    style API fill:#fff9c4
```

API allows only 60 requests/minute.
- Set Object Threshold = 100 (small queue, fast pause)
- Upstream pauses quickly, preventing API throttling errors

### Scenario 3: File Processing Burst

```mermaid
graph LR
    LIST[ListS3<br>Finds 100,000 files] -->|"Queue: 1000 threshold"| FETCH[FetchS3Object<br>Limited bandwidth]
    
    style LIST fill:#e1f5fe
    style FETCH fill:#c8e6c9
```

ListS3 finds 100K files instantly, but FetchS3Object downloads one at a time.
- Back pressure prevents 100K FlowFiles from overwhelming memory
- FetchS3Object processes at its own pace

## Configuring Back Pressure

### Per Connection

```
Right-click Connection → Configure:
  Back Pressure Object Threshold: 10000
  Back Pressure Data Size Threshold: 1 GB

# For small records (Kafka messages):
  Object Threshold: 50000        (many small FlowFiles OK)
  Data Size Threshold: 500 MB

# For large files (100MB+ each):
  Object Threshold: 100          (few files, each is large)
  Data Size Threshold: 10 GB

# For rate-limited targets:
  Object Threshold: 50           (small queue, fast back pressure)
  Data Size Threshold: 50 MB
```

### Default Settings (nifi.properties)

```properties
# Default back pressure for new connections:
nifi.backpressure.count.threshold=10000
nifi.backpressure.size.threshold=1 GB
```

## Relationship to Other Flow Control

```mermaid
graph TD
    subgraph "NiFi Flow Control Mechanisms"
        BP[Back Pressure<br>Connection-level<br>Pauses upstream processor]
        YIELD[Yield<br>Processor-level<br>Processor sleeps on no-work]
        PENALTY[Penalty<br>FlowFile-level<br>Individual FF delayed]
        SWAP[Swap<br>Memory-level<br>Overflow to disk]
    end
    
    style BP fill:#ffcdd2
    style YIELD fill:#c8e6c9
    style PENALTY fill:#fff9c4
    style SWAP fill:#e1f5fe
```

| Mechanism | Level | Trigger | Effect |
|-----------|-------|---------|--------|
| Back Pressure | Connection | Queue full | Pause upstream processor |
| Yield | Processor | No work available | Processor sleeps briefly |
| Penalty | FlowFile | Processing failure | Delay one FlowFile |
| Swap | System | Memory limit | Spill queue to disk |


## ▶️ Try It Yourself

```bash
# Back-pressure is configured per connection in NiFi:
# Back Pressure Object Threshold: 10000 (FlowFile count)
# Back Pressure Data Size Threshold: 1 GB

# When back-pressure triggers:
# 1. Upstream processor stops scheduling (no new FlowFiles created)
# 2. Downstream processor continues draining the queue
# 3. Upstream resumes when queue drops below threshold

# Monitor via NiFi API:
# GET /nifi-api/flow/process-groups/{id}/status
# Look for: "queuedCount" approaching "backPressureObjectThreshold"

# Tune back-pressure per connection based on FlowFile size:
# Small events (1KB): threshold=100000 objects, 100MB size
# Large files (1GB): threshold=10 objects, 20GB size

# Check current queue depths across all connections
# GET /nifi-api/process-groups/root/connections
# -> check status.aggregateSnapshot.queuedCount vs backPressureObjectThreshold

echo "High queue depth + back-pressure = downstream bottleneck — check slow processors"  
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is back pressure in NiFi?" — A flow control mechanism on connections (queues) between processors. When a queue reaches its configured threshold (FlowFile count or data size), the upstream processor is automatically paused until the queue drains. Prevents memory overflow and ensures the system doesn't crash when producers are faster than consumers.

> **Tip 2:** "How do you configure back pressure?" — Two thresholds per connection: Object Threshold (number of FlowFiles, default 10,000) and Data Size Threshold (total bytes, default 1 GB). Whichever is hit first triggers back pressure. Tune based on use case: low thresholds for rate-limited APIs, high thresholds for high-throughput batch processing.

> **Tip 3:** "What happens when back pressure is triggered?" — The upstream processor is paused (scheduler stops triggering it). It does NOT drop data — FlowFiles already in the queue continue processing. When the downstream consumer drains the queue below threshold, the upstream processor resumes automatically. Zero data loss, zero manual intervention.
