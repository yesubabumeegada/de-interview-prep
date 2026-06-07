---
title: "FlowFiles - Intermediate Concepts"
topic: nifi
subtopic: flowfiles
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [nifi, flowfiles, content-repository, provenance, cloning, merging]
---

# Apache NiFi FlowFiles — Intermediate Concepts

## Content Repository Deep Dive

The Content Repository is where FlowFile content (data bytes) is physically stored on disk.

```mermaid
graph TD
    subgraph "NiFi Repositories"
        FR[FlowFile Repository<br>Tracks FlowFile metadata<br>Write-Ahead Log<br>WAL for crash recovery]
        CR[Content Repository<br>Stores actual data bytes<br>Content-addressable<br>Multiple partitions]
        PR[Provenance Repository<br>Tracks FlowFile history<br>Full audit trail<br>Searchable events]
    end
    
    FF[FlowFile] --> FR
    FF --> CR
    FF --> PR
    
    style FR fill:#fff9c4
    style CR fill:#e1f5fe
    style PR fill:#c8e6c9
```

### Content Claims

```mermaid
graph TD
    subgraph "Content Repository Partitions"
        P1[Partition 1<br>/content-repo/1/]
        P2[Partition 2<br>/content-repo/2/]
    end
    
    subgraph "FlowFiles"
        FF1[FlowFile A<br>Claim: P1, offset 0, size 1MB]
        FF2[FlowFile B<br>Claim: P1, offset 1MB, size 500KB]
        FF3[FlowFile C<br>Claim: P1, offset 0, size 1MB<br>Same claim as A!]
    end
    
    FF1 --> P1
    FF2 --> P1
    FF3 --> P1
    
    style FF3 fill:#fff9c4
```

**Key concepts:**
- Content is stored in **container files** (not one file per FlowFile)
- A **content claim** = container ID + offset + length
- **Copy-on-write**: Cloning a FlowFile copies only the claim reference, not the data
- Multiple partitions for I/O parallelism

## FlowFile Cloning

When a processor needs to create a copy (e.g., RouteOnAttribute sends to multiple relationships):

```mermaid
graph TD
    ORIG[Original FlowFile<br>Content Claim: X<br>Attrs: filename=orders.csv]
    
    CLONE1[Clone 1<br>Content Claim: X<br>Attrs: filename=orders.csv<br>route=success]
    CLONE2[Clone 2<br>Content Claim: X<br>Attrs: filename=orders.csv<br>route=archive]
    
    ORIG -->|"Clone operation"| CLONE1
    ORIG -->|"Clone operation"| CLONE2
    
    style ORIG fill:#e1f5fe
    style CLONE1 fill:#c8e6c9
    style CLONE2 fill:#c8e6c9
```

**Cloning is nearly free** — only metadata is duplicated, not the content bytes. The content claim reference count increments. Content is only garbage-collected when no FlowFiles reference it.

## FlowFile Splitting

Split one FlowFile into many (e.g., splitting a 10,000-record CSV into individual records):

```mermaid
graph LR
    INPUT[Input FlowFile<br>10,000 records<br>Content: full CSV]
    
    SPLIT[SplitRecord<br>Processor]
    
    OUT1[FlowFile 1<br>Records 1-1000]
    OUT2[FlowFile 2<br>Records 1001-2000]
    OUT3[...]
    OUT10[FlowFile 10<br>Records 9001-10000]
    
    INPUT --> SPLIT
    SPLIT --> OUT1
    SPLIT --> OUT2
    SPLIT --> OUT3
    SPLIT --> OUT10
    
    style INPUT fill:#ffcdd2
    style SPLIT fill:#fff9c4
```

```
# Split attributes automatically added:
fragment.identifier = "abc-123"     (groups fragments together)
fragment.index = "0"                (position in original)
fragment.count = "10"               (total fragments)
segment.original.filename = "orders.csv"
```

## FlowFile Merging

Combine multiple FlowFiles back into one (reverse of split, or batching):

```mermaid
graph LR
    IN1[FlowFile 1<br>100 records]
    IN2[FlowFile 2<br>100 records]
    IN3[FlowFile 3<br>100 records]
    
    MERGE[MergeRecord<br>Processor<br>Min: 1000 records<br>Max wait: 30s]
    
    OUT[Merged FlowFile<br>300 records<br>One batch]
    
    IN1 --> MERGE
    IN2 --> MERGE
    IN3 --> MERGE
    MERGE --> OUT
    
    style MERGE fill:#c8e6c9
    style OUT fill:#e1f5fe
```

**Merge strategies:**
- **Defragment**: Reassemble split FlowFiles (uses fragment.identifier)
- **Bin-Packing**: Combine by size/count thresholds
- **Record-based**: Merge by record count (MergeRecord)

## Attribute Management

### Updating Attributes

```
# UpdateAttribute processor — add or modify attributes:
Processing Rules:
  source.system = "salesforce"
  environment = "production"
  processed_date = "${now():format('yyyy-MM-dd')}"
  record_count = "${record.count}"  # From previous processor
```

### Extracting Attributes from Content

```
# EvaluateJsonPath — extract values from JSON content into attributes:
Input FlowFile content:
  {"customer_id": "C001", "name": "Alice", "amount": 99.99}

Configuration:
  customer_id = $.customer_id
  customer_name = $.name
  order_amount = $.amount

Result: FlowFile gets attributes:
  customer_id = "C001"
  customer_name = "Alice"
  order_amount = "99.99"
```

### Routing on Attributes

```mermaid
graph TD
    IN[Incoming FlowFile<br>Attributes:<br>priority=high<br>source=crm]
    
    ROUTE[RouteOnAttribute]
    
    HIGH["Connection: high_priority<br>Condition: priority == 'high'"]
    LOW["Connection: low_priority<br>Condition: priority == 'low'"]
    UNMATCH[Connection: unmatched<br>No condition matched]
    
    IN --> ROUTE
    ROUTE -->|"priority=high"| HIGH
    ROUTE -->|"priority=low"| LOW
    ROUTE -->|"else"| UNMATCH
    
    style ROUTE fill:#fff9c4
```

## FlowFile Content Manipulation

### Streaming (Preferred)

NiFi processors use **streaming** — content is never fully loaded into memory:

```java
// Processor reads content as a stream (never all in memory):
session.read(flowFile, inputStream -> {
    // Process byte-by-byte or line-by-line
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = reader.readLine()) != null) {
        // Process each line
    }
});

// This handles 100GB files with only MB of memory!
```

### Write-Back Pattern

```java
// Modify content: read old → write new
FlowFile newFlowFile = session.write(flowFile, (inputStream, outputStream) -> {
    // Read from input, transform, write to output
    // Old content claim released when done
});
```

## FlowFile Prioritization

Connections can prioritize which FlowFiles get processed first:

| Prioritizer | Order | Use Case |
|-------------|-------|----------|
| FirstInFirstOutPrioritizer | FIFO (default) | Normal ordering |
| NewestFlowFileFirstPrioritizer | Newest first | Process fresh data first |
| OldestFlowFileFirstPrioritizer | Oldest first | Prevent starvation |
| PriorityAttributePrioritizer | By `priority` attribute | Business-critical first |

```
# Set priority attribute for business-critical routing:
UpdateAttribute:
  priority = "${source.system:equals('payments'):ifElse('1', '5')}"
  
# Connection prioritizer: PriorityAttributePrioritizer
# Result: Payment FlowFiles (priority=1) processed before others (priority=5)
```

## FlowFile Provenance

Every operation on a FlowFile is tracked in the **Provenance Repository**:

| Event Type | What it Tracks |
|-----------|---------------|
| CREATE | FlowFile first enters the system |
| RECEIVE | Received from external system |
| SEND | Sent to external system |
| CLONE | FlowFile was duplicated |
| FORK | Split into multiple FlowFiles |
| JOIN | Multiple merged into one |
| CONTENT_MODIFIED | Content was changed |
| ATTRIBUTES_MODIFIED | Attributes were changed |
| ROUTE | Routed to specific relationship |
| DROP | FlowFile removed from flow |

## Interview Tips

> **Tip 1:** "How does NiFi handle large files without running out of memory?" — Streaming architecture. FlowFile content is stored in the Content Repository on disk, referenced by content claims. Processors read/write via streams (InputSteam/OutputStream), never loading full content into heap memory. A 100GB file uses the same memory as a 1KB file.

> **Tip 2:** "What happens when you clone a FlowFile?" — Only the metadata (attributes) is copied. Both the original and clone reference the SAME content claim in the Content Repository. The claim's reference count increments. Content is only written to disk again if one of them modifies it (copy-on-write). Cloning is O(1) in time and space.

> **Tip 3:** "How does NiFi track data lineage?" — Provenance Repository. Every operation (create, modify, route, send, drop) on every FlowFile is recorded with timestamps, processor IDs, and attribute snapshots. You can trace any piece of data from source to destination, see every transformation it went through, and replay it if needed.
