---
title: "FlowFiles - Real-World Production Examples"
topic: nifi
subtopic: flowfiles
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [nifi, flowfiles, production, data-pipeline, patterns, best-practices]
---

# Apache NiFi FlowFiles — Real-World Production Examples

## Example 1: S3-to-Snowflake ETL Pipeline

```mermaid
graph TD
    LIST[ListS3<br>List new files in bucket<br>FlowFile per file reference]
    FETCH[FetchS3Object<br>Download content<br>Large files streamed]
    VALIDATE[ValidateRecord<br>Check schema compliance<br>Route valid/invalid]
    CONVERT[ConvertRecord<br>CSV → Parquet<br>Compress for Snowflake]
    PUT[PutS3Object<br>Stage to Snowflake S3<br>External stage location]
    SNOW[ExecuteSQL<br>COPY INTO command<br>Load staged files]
    
    LIST --> FETCH --> VALIDATE
    VALIDATE -->|"valid"| CONVERT
    VALIDATE -->|"invalid"| DLQ[PutS3Object<br>Dead letter queue]
    CONVERT --> PUT --> SNOW
    
    style LIST fill:#e1f5fe
    style VALIDATE fill:#fff9c4
    style CONVERT fill:#c8e6c9
    style SNOW fill:#bbdefb
```

### FlowFile Attribute Flow

```
After ListS3:
  filename = "orders_2024-03-15_001.csv.gz"
  s3.bucket = "data-lake-raw"
  s3.key = "landing/orders/2024/03/15/orders_2024-03-15_001.csv.gz"
  s3.lastModified = "1710489600000"
  
After FetchS3Object:
  (same attributes + content now contains the actual CSV data)
  fileSize = "52428800"  (50MB compressed)
  mime.type = "application/gzip"

After ValidateRecord:
  record.count = "125000"
  schema.validation = "valid"
  
After ConvertRecord:
  mime.type = "application/parquet"
  filename = "orders_2024-03-15_001.parquet"
  
After PutS3Object (staging):
  s3.bucket = "snowflake-stage"
  s3.key = "orders/orders_2024-03-15_001.parquet"
  s3.etag = "abc123..."
```

---

## Example 2: Kafka Consumer with Error Handling

```mermaid
graph TD
    KAFKA[ConsumeKafka_2_6<br>Topic: orders.events<br>Consumer Group: nifi-etl<br>12 concurrent tasks]
    
    MERGE[MergeRecord<br>Batch 5000 records<br>or 30 sec max wait]
    
    ENRICH[UpdateAttribute<br>Add processing metadata<br>batch.id, timestamp]
    
    TRANSFORM[JoltTransformJSON<br>Flatten nested structure<br>Rename fields]
    
    ROUTE[RouteOnAttribute<br>Route by event_type]
    
    DB[PutDatabaseRecord<br>Insert to PostgreSQL<br>Batch size: 1000]
    
    S3[PutS3Object<br>Archive to S3<br>Partitioned by date]
    
    RETRY[RetryFlowFile<br>Max 3 retries<br>Exponential backoff]
    
    DLQ[PublishKafka_2_6<br>Dead letter topic<br>orders.events.dlq]
    
    KAFKA --> MERGE --> ENRICH --> TRANSFORM --> ROUTE
    ROUTE -->|"order_created"| DB
    ROUTE -->|"order_updated"| DB
    ROUTE -->|"all"| S3
    DB -->|"failure"| RETRY
    RETRY -->|"retry < 3"| DB
    RETRY -->|"retry >= 3"| DLQ
    
    style KAFKA fill:#e1f5fe
    style MERGE fill:#c8e6c9
    style ROUTE fill:#fff9c4
    style DLQ fill:#ffcdd2
```

### Kafka FlowFile Attributes

```
After ConsumeKafka:
  kafka.topic = "orders.events"
  kafka.partition = "7"
  kafka.offset = "1589234"
  kafka.timestamp = "1710489600123"
  kafka.key = "order-12345"
  kafka.count = "1"
  
After MergeRecord (batched):
  record.count = "5000"
  merge.count = "5000"
  merge.bin.age = "12 seconds"
  fragment.identifier = "batch-uuid-456"
  
After UpdateAttribute:
  batch.id = "nifi-20240315-103000-001"
  processing.timestamp = "2024-03-15T10:30:00Z"
  environment = "production"
  
After RouteOnAttribute:
  (routed to appropriate relationship based on event_type attribute)
  
On Failure (RetryFlowFile):
  retry.count = "1"  → "2" → "3"
  last.error = "Connection refused: PostgreSQL"
  first.failure.time = "2024-03-15T10:30:05Z"
```

---

## Example 3: Multi-Source Data Integration

```mermaid
graph TD
    subgraph "Source Ingestion"
        API[InvokeHTTP<br>REST API<br>Every 5 min]
        SFTP[GetSFTP<br>Partner files<br>Polling]
        DB[ExecuteSQLRecord<br>Database CDC<br>Incremental]
    end
    
    subgraph "Standardization"
        TAG1[UpdateAttribute<br>source=api]
        TAG2[UpdateAttribute<br>source=sftp]
        TAG3[UpdateAttribute<br>source=database]
        FUNNEL[Funnel<br>Merge streams]
        SCHEMA[ConvertRecord<br>Normalize to Avro<br>Common schema]
    end
    
    subgraph "Quality & Routing"
        DQ[ValidateRecord<br>Schema + rules]
        DEDUP[DetectDuplicate<br>By business key hash]
        ROUTE2[RouteOnAttribute<br>By source + type]
    end
    
    subgraph "Output"
        LAKE[PutHDFS / PutS3<br>Data Lake raw zone]
        DWH[PutDatabaseRecord<br>Data Warehouse]
        ALERT[PutEmail<br>DQ alerts]
    end
    
    API --> TAG1 --> FUNNEL
    SFTP --> TAG2 --> FUNNEL
    DB --> TAG3 --> FUNNEL
    FUNNEL --> SCHEMA --> DQ
    DQ -->|"valid"| DEDUP
    DQ -->|"invalid"| ALERT
    DEDUP -->|"unique"| ROUTE2
    DEDUP -->|"duplicate"| LAKE
    ROUTE2 --> LAKE
    ROUTE2 --> DWH
    
    style FUNNEL fill:#e1bee7
    style DQ fill:#fff9c4
    style LAKE fill:#e1f5fe
    style DWH fill:#c8e6c9
```

---

## Example 4: Production FlowFile Monitoring

```mermaid
graph LR
    subgraph "Monitoring Attributes (added by processors)"
        A1["processing.start = timestamp"]
        A2["processing.end = timestamp"]
        A3["processing.duration.ms = calculated"]
        A4["record.count.in = 5000"]
        A5["record.count.out = 4998"]
        A6["error.count = 2"]
    end
    
    subgraph "Metrics Collection"
        SITE[SiteToSiteProvenanceReportingTask<br>Send provenance to monitoring NiFi]
        PROM[PrometheusReportingTask<br>Expose metrics endpoint]
        LOG[LogAttribute<br>Log attributes for debugging]
    end
```

### Production Attribute Convention

```
# Tracking through pipeline stages:
pipeline.name = "orders-etl-v2"
pipeline.stage = "3-transform"       # Current stage
pipeline.total.stages = "5"

# Timing:
stage.1.ingest.start = "2024-03-15T10:30:00Z"
stage.1.ingest.end = "2024-03-15T10:30:05Z"
stage.2.validate.start = "2024-03-15T10:30:06Z"
stage.2.validate.end = "2024-03-15T10:30:08Z"
stage.3.transform.start = "2024-03-15T10:30:09Z"

# Data quality embedded:
dq.input.records = "5000"
dq.valid.records = "4998"
dq.invalid.records = "2"
dq.null.rate.pct = "0.04"
dq.passed = "true"

# Error tracking:
error.processor = ""                 # Empty = no error
error.message = ""
error.timestamp = ""
retry.count = "0"
retry.max = "3"
```

---

## Best Practices Summary

| Practice | Why |
|----------|-----|
| Batch FlowFiles (1K-10K records each) | Avoid per-FlowFile overhead |
| Use attributes for routing, not content | Faster than parsing content |
| Name attributes with prefixes | `source.`, `dq.`, `routing.` — clarity |
| Keep content in standard formats | Avro/JSON for record-based processing |
| Archive FlowFile content (provenance) | Enables replay for debugging |
| Set back-pressure on every connection | Prevent memory issues |
| Use MergeRecord before external writes | Batch I/O to databases/APIs |
| Log attributes at key decision points | Debugging and audit trail |

## Interview Tips

> **Tip 1:** "Design a production NiFi pipeline for Kafka → Data Warehouse" — ConsumeKafka (concurrent tasks = partition count) → MergeRecord (batch 5000 records, 30s max) → ConvertRecord (to target format) → PutDatabaseRecord (batch size 1000). Error handling: RetryFlowFile (3 attempts, exponential backoff) → PublishKafka to DLQ after max retries. Add UpdateAttribute for tracking metadata throughout.

> **Tip 2:** "How do you handle multiple data sources with different formats?" — Each source gets its own ingestion processor. Add `source` attribute immediately (UpdateAttribute). Funnel to merge streams. ConvertRecord to a common schema (Avro with a unified schema). ValidateRecord ensures all sources conform. Route downstream based on attributes (source, type, priority) — not format-specific logic.

> **Tip 3:** "How do you monitor FlowFile processing in production?" — (1) Embed timing attributes at each stage for SLA tracking. (2) Use PrometheusReportingTask for metrics dashboards. (3) LogAttribute at key points for debugging. (4) Provenance for full audit trail. (5) Bulletin board for processor errors. (6) Monitor connection queue sizes for back-pressure alerts.
