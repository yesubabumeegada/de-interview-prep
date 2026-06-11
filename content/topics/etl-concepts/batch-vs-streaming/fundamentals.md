---
title: "Batch vs Streaming - Fundamentals"
topic: etl-concepts
subtopic: batch-vs-streaming
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [etl, batch, streaming, real-time, data-pipeline, architecture]
---

# Batch vs Streaming — Fundamentals


## 🎯 Analogy

Think of batch vs streaming like grocery shopping vs a vending machine. Batch is buying a week of groceries in one trip (efficient, but you wait for the weekly run). Streaming is the vending machine — data is processed the moment it arrives.

---
## The Core Difference

```mermaid
flowchart LR
    subgraph "Batch Processing"
        A1[Collect Data<br>Over Time] --> B1[Process All<br>At Once] --> C1[Output Results<br>Hours Later]
    end
    subgraph "Stream Processing"
        A2[Events Arrive<br>Continuously] --> B2[Process Each<br>Immediately] --> C2[Results in<br>Seconds/Minutes]
    end
```

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Timing** | Process accumulated data on schedule | Process data as it arrives |
| **Latency** | Minutes to hours | Milliseconds to seconds |
| **Data scope** | Bounded (known start and end) | Unbounded (never-ending) |
| **Complexity** | Lower — simpler logic | Higher — ordering, late data, state |
| **Throughput** | Very high (optimized for bulk) | Moderate (per-record overhead) |
| **Cost** | Lower (run during off-peak) | Higher (always-on resources) |
| **Example** | Daily sales report | Real-time fraud detection |

## When to Use Each

### Use Batch When:
- Results don't need to be fresh (hourly/daily is fine)
- Processing requires complete data (e.g., "top 10 products this month")
- Input is naturally bounded (daily file drops, database snapshots)
- Cost efficiency is a priority
- Transformations are complex (large joins, ML models)

**Examples:**
- Nightly data warehouse ETL
- Monthly billing calculations
- Training ML models
- Historical analytics and reporting
- Data migrations

### Use Streaming When:
- Freshness matters (alerts, dashboards, recommendations)
- Events need immediate reaction (fraud, anomaly detection)
- Source is naturally continuous (clickstream, IoT sensors, logs)
- Downstream systems expect continuous updates

**Examples:**
- Real-time fraud detection
- Live dashboards (ops monitoring)
- Recommendation engines (personalized in-session)
- IoT sensor alerting
- Log monitoring and alerting

## The Lambda Architecture

A common pattern combining both approaches:

```mermaid
flowchart TD
    A[Data Source] --> B[Batch Layer<br>Complete, Accurate]
    A --> C[Speed Layer<br>Fast, Approximate]
    B --> D[Serving Layer]
    C --> D
    D --> E[Query Interface]
    
    style B fill:#e0f2fe
    style C fill:#fef3c7
```

- **Batch Layer:** Processes all historical data. Eventually consistent. Source of truth.
- **Speed Layer:** Processes recent data in real-time. Provides low-latency results.
- **Serving Layer:** Merges batch + speed results for queries.

**Drawback:** Dual code paths — same logic maintained in two systems (batch + stream).

## The Kappa Architecture

Simplification: use streaming for everything, replay history when needed.

```mermaid
flowchart LR
    A[Event Log<br>e.g., Kafka] --> B[Stream Processor]
    B --> C[Serving Layer]
    A -.->|Replay| B
```

- **Single code path** — one streaming job handles both real-time and reprocessing
- **Reprocessing:** Replay events from the beginning of the log
- **Tradeoff:** Requires a durable, replayable event log (Kafka with retention)

## Micro-Batch: The Middle Ground

Process data in small, frequent batches (every 1-5 minutes) rather than true event-at-a-time streaming.

```mermaid
flowchart LR
    A[Events] --> B[Buffer<br>1-5 min] --> C[Process Batch] --> D[Output]
    B --> B
```

**Examples:** Spark Structured Streaming (default mode), Apache Flink batch checkpoints

**Benefits:**
- Simpler than true streaming (no per-event state management)
- Lower latency than traditional batch (minutes vs hours)
- Exactly-once semantics easier to achieve
- Better throughput than true per-event processing

## Key Terminology

| Term | Definition |
|------|-----------|
| **Event time** | When the event actually happened (embedded in the data) |
| **Processing time** | When the system processes the event |
| **Ingestion time** | When the event enters the pipeline |
| **Watermark** | A threshold declaring "all events before this time have arrived" |
| **Windowing** | Grouping events into time-based buckets (tumbling, sliding, session) |
| **Late data** | Events arriving after their window has closed |
| **Backpressure** | Slowing down producers when consumers can't keep up |
| **Checkpoint** | Periodic snapshot of processing state for recovery |
| **Exactly-once** | Guarantee that each event is processed exactly one time |
| **At-least-once** | Events may be processed multiple times (duplicates possible) |

## Common Tools by Processing Type

| Batch | Streaming | Both |
|-------|-----------|------|
| Apache Spark (batch mode) | Apache Kafka Streams | Apache Spark (Structured Streaming) |
| Apache Hive | Apache Flink | Apache Beam |
| AWS Glue | Amazon Kinesis | Databricks |
| dbt | Apache Pulsar | Snowflake (Snowpipe + batch) |
| Airflow (orchestrator) | AWS Lambda | |

## Interview Tip 💡

> The most common interview question here is: "When would you choose streaming over batch?" The strong answer discusses **latency requirements** first (does the business need sub-minute freshness?), then **complexity and cost** (streaming is harder and more expensive — only use it when batch latency is insufficient). Bonus: mention that many "real-time" requirements are actually fine with micro-batch (5-minute delay).

## ▶️ Try It Yourself

```python
# Batch: process a day's worth of orders at once (runs at midnight)
import pandas as pd
from datetime import date

def batch_daily_revenue(date_str: str):
    df = pd.read_parquet(f"s3://data/orders/date={date_str}/")
    return df.groupby("region")["amount"].sum()

# Streaming: process each order as it arrives (Faust/Kafka)
# import faust
# app = faust.App("revenue", broker="kafka://localhost:9092")
# @app.agent(app.topic("orders"))
# async def process(stream):
#     async for order in stream:
#         print(f"Live: order {order['id']} = ${order['amount']}")

# For demo: simulate a streaming-style loop
import time
events = [{"id":1,"amount":100}, {"id":2,"amount":200}]
for event in events:
    print(f"Processing event {event['id']} immediately: ${event['amount']}")
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
