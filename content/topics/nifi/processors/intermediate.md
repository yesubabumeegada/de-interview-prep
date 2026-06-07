---
title: "NiFi Processors - Intermediate Concepts"
topic: nifi
subtopic: processors
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [nifi, processors, record-processors, lookup, custom-processors, scheduling]
---

# Apache NiFi Processors — Intermediate Concepts

## Record-Based Processors

Record processors work on **individual records within a FlowFile** (not the FlowFile as a whole). They use Record Reader + Record Writer services.

```mermaid
graph LR
    subgraph "Record Processing"
        IN[FlowFile<br>5000 records<br>CSV format]
        RR[Record Reader<br>Parses CSV into records]
        PROC[Record Processor<br>Operates on each record]
        RW[Record Writer<br>Outputs as JSON]
        OUT[FlowFile<br>5000 records<br>JSON format]
    end
    
    IN --> RR --> PROC --> RW --> OUT
    
    style IN fill:#e1f5fe
    style PROC fill:#fff9c4
    style OUT fill:#c8e6c9
```

### Key Record Processors

| Processor | Purpose | Example |
|-----------|---------|---------|
| ConvertRecord | Change format | CSV → JSON, JSON → Avro |
| QueryRecord | SQL on FlowFile content | `SELECT * WHERE amount > 100` |
| UpdateRecord | Modify field values | Set status = 'processed' |
| LookupRecord | Enrich from external source | Add customer_name from DB |
| ValidateRecord | Schema validation | Reject if missing required fields |
| SplitRecord | Split into smaller batches | 1M records → 100 × 10K |
| MergeRecord | Combine into larger batches | 100 × 10K → 1 × 1M |
| PartitionRecord | Split by field value | Group by region |

### QueryRecord (SQL on FlowFile Data)

```sql
-- QueryRecord lets you run SQL DIRECTLY on FlowFile content!
-- No database needed — processes in-memory.

-- Configuration:
--   Record Reader: CSVReader
--   Record Writer: JsonRecordSetWriter
--   Custom Properties (each becomes a relationship):

high_value_orders:
  SELECT * FROM FLOWFILE WHERE amount > 1000

us_only:
  SELECT order_id, customer, amount 
  FROM FLOWFILE 
  WHERE region = 'US'

summary:
  SELECT region, COUNT(*) as order_count, SUM(amount) as total
  FROM FLOWFILE
  GROUP BY region
```

### LookupRecord (Enrichment)

```mermaid
graph LR
    IN[FlowFile<br>customer_id: C001<br>amount: 99.99]
    
    LOOKUP[LookupRecord<br>Lookup: customer_id<br>Add: customer_name, segment]
    
    DB[(Lookup Service<br>Database or Cache<br>C001 → Alice, Enterprise)]
    
    OUT[FlowFile<br>customer_id: C001<br>customer_name: Alice<br>segment: Enterprise<br>amount: 99.99]
    
    IN --> LOOKUP
    DB --> LOOKUP
    LOOKUP --> OUT
    
    style LOOKUP fill:#fff9c4
    style DB fill:#e1f5fe
```

```
LookupRecord Configuration:
  Record Reader: JsonTreeReader
  Record Writer: JsonRecordSetWriter
  Lookup Service: DatabaseLookupService (or SimpleDatabaseLookupService)
  Result RecordPath: /customer_name    # Where to put lookup result
  Key: /customer_id                    # Field to lookup by
  
Lookup Service (SimpleDatabaseLookupService):
  Database Connection: PostgreSQL_Pool
  Table Name: dim_customer
  Lookup Key Column: customer_id
  Lookup Value Columns: customer_name, segment
```

### PartitionRecord

Split FlowFile content by field value (like GROUP BY):

```
PartitionRecord:
  Record Reader: JsonTreeReader
  Record Writer: JsonRecordSetWriter
  Partition Fields: region
  
# Input: 1 FlowFile with 10,000 records (mixed regions)
# Output: Multiple FlowFiles, one per region value:
#   FlowFile 1: region=US (3000 records), attribute: partition.region=US
#   FlowFile 2: region=EU (4000 records), attribute: partition.region=EU
#   FlowFile 3: region=APAC (3000 records), attribute: partition.region=APAC
```

## Processor Scheduling Deep Dive

### Timer Driven

```
Run Schedule: 5 sec
# Processor triggers every 5 seconds
# For polling sources: check for new data every 5 sec

Run Schedule: 0 sec  
# Run continuously (as fast as possible)
# For transformation processors: process immediately when FlowFiles arrive
```

### Cron Driven

```
Run Schedule: 0 0 6 * * ?
# Run at 6:00 AM every day (standard cron expression)
# For scheduled extractions: daily reports, hourly syncs

Run Schedule: 0 */15 * * * ?
# Every 15 minutes
```

### Event Driven (NiFi 2.x)

```
# Processor runs ONLY when a FlowFile arrives (no polling!)
# Most efficient for transformation processors
# Reduces unnecessary CPU usage
```

## Processor State Management

Some processors need to track state between executions:

```
# ListS3 tracks: "last file I listed" (to avoid re-listing)
# State stored locally (node-specific) or in cluster (shared)

ListS3 State:
  Scope: CLUSTER  (shared across NiFi cluster nodes)
  State:
    listing.timestamp = "1710489600000"
    # Only lists S3 objects newer than this timestamp

ConsumeKafka State:
  # Kafka manages offsets externally (consumer group)
  # NiFi doesn't need internal state for Kafka

ExecuteSQLRecord State:
  Scope: LOCAL
  State:
    maximum.value = "2024-03-15 10:30:00"
    # Incremental: SELECT * WHERE updated_at > ${maximum.value}
```

## Processor Groups

Logical containers for organizing related processors:

```mermaid
graph TD
    subgraph "Process Group: Order Ingestion"
        L[ListS3]
        F[FetchS3Object]
        V[ValidateRecord]
        L --> F --> V
    end
    
    subgraph "Process Group: Order Transformation"
        C[ConvertRecord]
        E[LookupRecord]
        U[UpdateRecord]
        C --> E --> U
    end
    
    subgraph "Process Group: Order Output"
        DB[PutDatabaseRecord]
        S3[PutS3Object]
        K[PublishKafka]
    end
    
    V -->|"Input Port"| C
    U -->|"Input Port"| DB
    U -->|"Input Port"| S3
    U -->|"Input Port"| K
```

**Benefits:**
- Visual organization (collapse/expand)
- Reusability (copy entire groups)
- Variable scoping (variables defined per group)
- Access control (permissions per group)
- Versioning (NiFi Registry integration)

## Error Handling Patterns

### Retry Pattern

```mermaid
graph TD
    PROC[PutDatabaseRecord]
    RETRY[RetryFlowFile<br>Max: 3 retries<br>Penalty: 10 sec]
    DLQ[PublishKafka<br>Dead Letter Queue]
    SUCCESS[Next Step]
    
    PROC -->|"success"| SUCCESS
    PROC -->|"failure"| RETRY
    RETRY -->|"retry"| PROC
    RETRY -->|"retries exhausted"| DLQ
    
    style PROC fill:#c8e6c9
    style RETRY fill:#fff9c4
    style DLQ fill:#ffcdd2
```

### Rollback Pattern

```
# For processors that support transactions:
PutDatabaseRecord:
  Rollback On Failure: true
  # If ANY record fails → entire batch rolled back
  # FlowFile sent to failure relationship (retryable)
  
# For non-transactional processors:
# Split into smaller batches first → limit blast radius
```

## Processor Performance Tips

| Tip | Explanation |
|-----|-------------|
| Set Concurrent Tasks wisely | Match to downstream capacity (DB connections, Kafka partitions) |
| Use Record processors | Process 1000s of records per FlowFile (not 1 FlowFile per record) |
| Batch before output | MergeRecord before PutDatabaseRecord (batch inserts are faster) |
| Avoid unnecessary copies | Use RouteOnAttribute instead of reading content when possible |
| Tune Run Schedule | 0 sec for transformers (react immediately), 5-30 sec for pollers |

## Interview Tips

> **Tip 1:** "What are record-based processors?" — Processors that operate on individual records WITHIN a FlowFile (not the FlowFile as a whole). They use Record Reader/Writer services for format-aware processing. Examples: ConvertRecord, QueryRecord, LookupRecord, PartitionRecord. Key advantage: one FlowFile can contain 100K records, all processed efficiently without splitting.

> **Tip 2:** "How does QueryRecord work?" — It lets you run SQL queries directly on FlowFile content (no external database needed!). Define multiple queries as properties — each becomes a relationship. Enables filtering (WHERE), projection (SELECT specific columns), aggregation (GROUP BY), and joining. Perfect for in-flow data transformation.

> **Tip 3:** "How do you handle processor failures?" — RetryFlowFile processor with exponential backoff (max retries + penalty duration). After exhausting retries → route to dead letter queue (PublishKafka or PutS3Object to DLQ). For transactional processors: enable rollback-on-failure to prevent partial writes. Always log error details in FlowFile attributes for debugging.
