---
title: "NiFi Expression Language - Real-World Production Examples"
topic: nifi
subtopic: expression-language
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [nifi, expression-language, production, patterns, dynamic-routing]
---

# NiFi Expression Language — Real-World Production Examples

## Example 1: Date-Partitioned Data Lake Paths

```
# Requirement: Write files to S3 with Hive-style partitioning
# Target: s3://lake/source=shopify/year=2024/month=03/day=15/hour=10/uuid.parquet

PutS3Object:
  Bucket: ${s3_bucket}
  Object Key: source=${source.system}/year=${now():format('yyyy')}/month=${now():format('MM')}/day=${now():format('dd')}/hour=${now():format('HH')}/${UUID()}.parquet

# For event-time partitioning (use event time, not processing time):
  Object Key: source=${source.system}/year=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss"):format('yyyy')}/month=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss"):format('MM')}/day=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss"):format('dd')}/${UUID()}.parquet
```

## Example 2: Multi-Tenant Dynamic Configuration

```
# Attributes set per-tenant via LookupRecord:
#   tenant_id = "acme"
#   tenant_bucket = "acme-data-lake"  
#   tenant_schema = "acme_raw"
#   tenant_retention_days = "90"

# All processors use tenant-specific config via EL:
PutS3Object:
  Bucket: ${tenant_bucket}
  Object Key: ${tenant_id}/raw/${now():format('yyyy/MM/dd')}/${filename}

PutDatabaseRecord:
  Table Name: ${tenant_schema}.${target_table}

# SLA check per tenant:
UpdateAttribute:
  sla_breached = "${processing.latency.ms:divide(60000):gt(${tenant_sla_minutes})}"

# Retention cleanup:
UpdateAttribute:
  retention_cutoff = "${now():toNumber():minus(${tenant_retention_days:multiply(86400000)})}"
```

## Example 3: Complex Error Context for DLQ

```
# When a FlowFile fails, build comprehensive error context:
UpdateAttribute (on failure path):
  error.context = "${source.system}|${filename}|${record.count}"
  error.summary = "Failed ${processing.stage} for ${source.system}: ${error.message}"
  error.alert.channel = "${source.system:equals('payments'):ifElse('#critical-alerts', '#data-alerts')}"
  error.priority = "${source.system:equals('payments'):ifElse('P1', ${retry.count:gt(2):ifElse('P2', 'P3')})}"
  error.runbook = "https://wiki.internal/runbooks/${source.system}/${processing.stage}"
  
# Slack alert with full context:
InvokeHTTP (POST to Slack):
  Request Body: {"channel": "${error.alert.channel}", "text": ":x: *${error.priority}*: ${error.summary}\nRunbook: ${error.runbook}\nRetries: ${retry.count}/${retry.max}"}
```

## Example 4: Dynamic Schema Versioning

```
# Source systems have versioned schemas that change over time
# Version detected from data or attributes:

UpdateAttribute:
  # Detect version from API response headers:
  schema_version = "${invokehttp.response.header.X-Schema-Version:replaceNull('1')}"
  
  # Or from filename convention:
  schema_version = "${filename:find('_v(\\d+)'):replaceAll('.*_v(\\d+).*', '$1'):replaceNull('1')}"
  
  # Build schema name dynamically:
  schema_name = "${source.system}_${event.type}_v${schema_version}"

# ConvertRecord uses this dynamic schema:
ConvertRecord:
  Schema Access Strategy: Schema Name
  Schema Name: ${schema_name}
  # Resolves to: "shopify_order_v2", "stripe_payment_v3", etc.
```

## Example 5: Incremental Processing State

```
# Track watermark for incremental extraction:
# After successful processing:
UpdateAttribute:
  last_processed_id = "${max.id}"
  last_processed_time = "${now():format("yyyy-MM-dd'T'HH:mm:ss'Z'")}"
  next_batch_start = "${max.id:plus(1)}"

# Next execution uses these attributes:
ExecuteSQLRecord:
  SQL: SELECT * FROM orders WHERE id > ${last_processed_id} AND created_at > '${last_processed_time}' LIMIT ${batch_size:replaceNull('10000')}
```

## Common EL Patterns Quick Reference

| Pattern | Expression |
|---------|-----------|
| Date partition path | `${now():format('yyyy/MM/dd')}` |
| UUID filename | `${UUID()}.${output_format}` |
| Null-safe default | `${attr:replaceNull('default_value')}` |
| File extension | `${filename:substringAfterLast('.')}` |
| File without extension | `${filename:substringBeforeLast('.')}` |
| Is CSV file? | `${filename:toLower():endsWith('.csv')}` |
| Size in MB | `${fileSize:divide(1048576)}` |
| Yesterday's date | `${now():toNumber():minus(86400000):format('yyyy-MM-dd')}` |
| Uppercase first letter | `${name:substring(0,1):toUpper():append(${name:substring(1)})}` |
| Env-aware URL | `https://api.${env}.company.com/v2/` |
| Dynamic Kafka topic | `${source:toLower()}.${event_type:toLower()}.events` |
| Retry delay (exponential) | `${retry.count:multiply(${retry.count}):multiply(1000)} ms` |

## Interview Tips

> **Tip 1:** "How do you handle date-partitioned output in NiFi?" — Use EL in the output path: `${source}/year=${event_time:format('yyyy')}/month=${event_time:format('MM')}/day=${event_time:format('dd')}/${UUID()}.parquet`. Key decision: partition by processing time (`${now()}`) or event time (from data attribute). Event time is better for analytics but requires parsing the data first.

> **Tip 2:** "How do you handle multi-tenant flows?" — Lookup tenant config from a database/cache → adds tenant-specific attributes (bucket, schema, SLA, retention). All downstream processors use `${tenant_*}` attributes via EL. One flow handles all tenants — config differences are in attributes, not processor settings. Adding a new tenant = adding a row to config table.

> **Tip 3:** "Production error handling with EL?" — Build comprehensive error context in attributes: source, filename, stage, error message, retry count, timestamp. Use EL to compute: alert priority (`ifElse` based on source criticality), alert channel (dynamic Slack channel), and runbook URL. Route to DLQ with ALL context preserved. This enables automated triage and replay.
