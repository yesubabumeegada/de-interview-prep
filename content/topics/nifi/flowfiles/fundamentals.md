---
title: "FlowFiles - Fundamentals"
topic: nifi
subtopic: flowfiles
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [nifi, flowfiles, data-flow, attributes, content, data-engineering]
---

# Apache NiFi FlowFiles — Fundamentals

## What is Apache NiFi?

Apache NiFi is a **data integration and dataflow automation platform** designed to move data between systems reliably at scale. It provides a web-based UI for designing, monitoring, and managing data pipelines visually.

```mermaid
graph LR
    subgraph "NiFi Core Concepts"
        FF[FlowFile<br>The data unit]
        P[Processor<br>Does work on FlowFiles]
        C[Connection<br>Queues between processors]
        PG[Process Group<br>Logical grouping]
    end
    
    FF --> P
    P --> C
    C --> P
    P --> PG
    
    style FF fill:#bbdefb
    style P fill:#c8e6c9
    style C fill:#fff9c4
    style PG fill:#e1bee7
```

## What is a FlowFile?

A FlowFile is the **fundamental data unit** in NiFi. Every piece of data moving through a NiFi pipeline is wrapped in a FlowFile.

A FlowFile has two parts:

| Component | What it is | Example |
|-----------|-----------|---------|
| **Content** | The actual data payload (bytes) | CSV rows, JSON document, image, Avro file |
| **Attributes** | Key-value metadata about the content | filename, path, mime.type, fileSize, uuid |

```mermaid
graph TD
    subgraph "FlowFile Structure"
        A[Attributes<br>Key-Value Pairs<br>filename: orders.csv<br>mime.type: text/csv<br>fileSize: 1048576<br>uuid: abc-123-def]
        B[Content<br>The actual data bytes<br>order_id,customer,amount<br>1001,Alice,99.99<br>1002,Bob,49.50<br>...]
    end
    
    style A fill:#fff9c4
    style B fill:#e1f5fe
```

## FlowFile Attributes

Every FlowFile automatically gets **core attributes** created by NiFi:

| Attribute | Description | Example |
|-----------|-------------|---------|
| `uuid` | Unique identifier for this FlowFile | `a1b2c3d4-e5f6-7890` |
| `filename` | Name of the file/data | `orders_2024-03-15.csv` |
| `path` | Directory path | `/data/incoming/` |
| `fileSize` | Size in bytes | `1048576` |
| `mime.type` | Content type | `application/json` |
| `entryDate` | When FlowFile entered the flow | `2024-03-15T10:30:00Z` |
| `lineageStartDate` | When FlowFile was originally created | `2024-03-15T10:30:00Z` |

You can also add **custom attributes** at any point in the flow:

```
source.system = "salesforce"
batch.id = "batch-20240315-001"
record.count = "5000"
processing.status = "validated"
```

## FlowFile Content

The content is the **raw bytes** — NiFi doesn't care what format it is. It could be:

- CSV, TSV, or fixed-width text
- JSON or XML documents
- Avro, Parquet, or ORC binary files
- Images, PDFs, or any binary data
- A single database row or an entire table dump

```mermaid
graph LR
    subgraph "Content Repository"
        CR[Stores actual bytes<br>on disk<br>Content-addressable<br>Efficient cloning]
    end
    
    subgraph "FlowFile References"
        FF1[FlowFile 1<br>→ Content Claim A]
        FF2[FlowFile 2<br>→ Content Claim B]
        FF3[FlowFile 3<br>→ Content Claim A<br>Same content!]
    end
    
    FF1 --> CR
    FF2 --> CR
    FF3 --> CR
    
    style CR fill:#e1f5fe
```

**Key concept:** FlowFiles don't contain the data directly — they hold a **reference (content claim)** to data stored in the Content Repository. Multiple FlowFiles can reference the same content (copy-on-write).

## FlowFile Lifecycle

```mermaid
graph TD
    CREATE[1. Created<br>Processor ingests data<br>e.g., GetFile, ConsumeKafka]
    QUEUE[2. Queued<br>Sits in Connection<br>waiting for next processor]
    PROCESS[3. Processed<br>Processor transforms<br>e.g., ConvertRecord, RouteOnAttribute]
    ROUTE[4. Routed<br>Sent to success/failure<br>relationship]
    DONE[5. Completed<br>Dropped or sent out<br>e.g., PutS3Object, PutDatabaseRecord]
    
    CREATE --> QUEUE --> PROCESS --> ROUTE
    ROUTE -->|"success"| QUEUE
    ROUTE -->|"failure"| QUEUE
    ROUTE -->|"terminal"| DONE
    
    style CREATE fill:#c8e6c9
    style PROCESS fill:#fff9c4
    style DONE fill:#e1bee7
```

## Common FlowFile Operations

| Operation | What happens | Example Processor |
|-----------|-------------|-------------------|
| **Create** | New FlowFile from external source | GetFile, ConsumeKafka, ListS3 |
| **Transform** | Modify content | ConvertRecord, ReplaceText, JoltTransformJSON |
| **Route** | Send to different paths based on condition | RouteOnAttribute, RouteOnContent |
| **Split** | One FlowFile → multiple FlowFiles | SplitJson, SplitRecord, SplitText |
| **Merge** | Multiple FlowFiles → one FlowFile | MergeContent, MergeRecord |
| **Enrich** | Add/update attributes | UpdateAttribute, EvaluateJsonPath |
| **Output** | Send FlowFile to external system | PutS3Object, PutDatabaseRecord |

## FlowFile Relationships

When a processor finishes with a FlowFile, it routes it to a **relationship** (like an output port):

```mermaid
graph LR
    P[Processor<br>ValidateRecord]
    S[success<br>Valid records]
    F[failure<br>Invalid records]
    O[original<br>Unchanged input]
    
    P -->|"valid"| S
    P -->|"invalid"| F
    P -->|"untouched"| O
    
    style P fill:#c8e6c9
    style S fill:#bbdefb
    style F fill:#ffcdd2
```

Common relationships:
- `success` — processing completed normally
- `failure` — processing encountered an error
- `original` — original FlowFile (when processor creates new ones)
- `matched` / `unmatched` — for routing processors
- Custom relationships defined per processor

## Connections (Queues)

Connections are the **queues** between processors. They hold FlowFiles waiting to be processed.

```mermaid
graph LR
    P1[Processor A] -->|"Connection<br>Queue: 5000 FlowFiles<br>Backpressure: 10000"| P2[Processor B]
    
    style P1 fill:#c8e6c9
    style P2 fill:#c8e6c9
```

Connection settings:
- **Back pressure threshold**: Max FlowFiles in queue before upstream pauses
- **Expiration**: Auto-drop FlowFiles older than X time
- **Prioritization**: FIFO, newest first, oldest first, priority attribute

## Interview Tips

> **Tip 1:** "What is a FlowFile?" — The fundamental data unit in NiFi. It has two parts: content (the actual data bytes stored in the Content Repository) and attributes (key-value metadata like filename, size, uuid). FlowFiles flow through processors connected by queues (connections).

> **Tip 2:** "How does NiFi handle large files efficiently?" — FlowFiles don't contain the data directly — they reference content stored in the Content Repository using content claims. Multiple FlowFiles can share the same content (copy-on-write). Content is streamed through processors, so even multi-GB files don't need to fit in memory.

> **Tip 3:** "What are FlowFile relationships?" — Output ports from processors. Each processor defines relationships (success, failure, original, etc.). You connect relationships to downstream processors or auto-terminate them. This enables conditional routing: valid records go to success, invalid to failure → different handling paths.
