---
title: "NiFi Expression Language - Intermediate Concepts"
topic: nifi
subtopic: expression-language
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [nifi, expression-language, regex, record-path, advanced-functions]
---

# NiFi Expression Language — Intermediate Concepts

## Regular Expression Functions

```
# Given: filename = "orders_20240315_batch001.csv.gz"

# Extract with regex:
${filename:replaceAll('.*_(\\d{8})_.*', '$1')}     → "20240315"
${filename:replaceAll('.*batch(\\d+).*', '$1')}     → "001"

# Match test:
${filename:matches('.*\\d{8}.*')}                   → "true"

# Find (first match):
${filename:find('[0-9]{8}')}                        → "true" (match exists)

# Replace with regex:
${filename:replaceAll('[^a-zA-Z0-9.]', '_')}        → "orders_20240315_batch001_csv_gz"
${filename:replaceAll('\\.(csv|json|xml)\\.gz$', '.$1')}  → "orders_20240315_batch001.csv"
```

## Multi-Attribute Expressions

```
# Combine multiple attributes:
${path:append(${filename})}
# "/data/incoming/" + "orders.csv" = "/data/incoming/orders.csv"

# Conditional based on multiple attributes:
${source:equals('kafka'):and(${priority:equals('high')}):ifElse('fast-path', 'standard')}

# Null-safe chaining:
${customer_name:replaceNull('Unknown'):toUpper()}
# If customer_name is null → "UNKNOWN", else uppercase value

# Default values:
${batch_size:replaceNull('1000')}
# Use 1000 if batch_size attribute doesn't exist
```

## Advanced String Functions

```
# JSON path in EL (for simple extraction):
${flowfile.content:jsonPath('$.customer.id')}
# Note: Only works with EvaluateJsonPath, not general EL

# URL encoding:
${filename:urlEncode()}
# "my file (1).csv" → "my+file+%281%29.csv"

# Base64:
${secret:base64Encode()}
${encoded:base64Decode()}

# Padding:
${sequence:padLeft(5, '0')}
# "42" → "00042"

# Split and get field:
${filename:getDelimitedField(2, '_')}
# "orders_20240315_batch001.csv" → "20240315" (field 2, delimited by _)

# Repeat:
${literal('='):repeat(50)}
# "==================================================" (separator line)
```

## Record Path (for Record-Based Processors)

Record Path is a separate expression language used within record processors to reference fields:

```
# Record Path syntax (different from EL!):
/customer_id                    — Top-level field
/address/city                   — Nested field
/items[0]/sku                   — Array element
/items[*]/price                 — All array elements
/items[0..2]/name               — Array slice

# Used in processors like UpdateRecord, LookupRecord:
UpdateRecord:
  /status = "processed"                         # Set literal value
  /full_name = concat(/first_name, ' ', /last_name)  # Concatenate fields
  /amount_usd = multiply(/amount, /fx_rate)     # Math on fields
  /processed_at = now()                         # Current timestamp

# RecordPath functions:
concat(/first, ' ', /last)     — String concatenation
substring(/name, 0, 10)        — Substring
toUpperCase(/name)             — Case conversion
coalesce(/preferred_name, /name) — First non-null value
fieldName(.)                   — Get field name
toDate(/date_str, 'yyyy-MM-dd') — Parse date
format(toDate(/ts, 'epoch'), 'yyyy-MM-dd') — Format date
```

## Expression Language in Different Processor Contexts

### EvaluateJsonPath

```
# Extract JSON fields → FlowFile attributes:
EvaluateJsonPath:
  Destination: flowfile-attribute    # Extract to attributes
  customer_id = $.customer.id
  order_total = $.order.total
  item_count = $.order.items.length()
  first_item = $.order.items[0].name
```

### RouteOnAttribute (Complex Conditions)

```
# Multi-condition routing:
RouteOnAttribute:
  high_value_us = ${amount:gt(10000):and(${country:equals('US')})}
  
  stale_data = ${event.timestamp:toDate('yyyy-MM-dd'):toNumber():lt(${now():toNumber():minus(86400000)})}
  # event older than 24 hours
  
  needs_enrichment = ${customer_name:isEmpty():or(${email:isNull()})}
  
  weekend_file = ${now():format('u'):gt(5)}
  # Day of week > 5 = Saturday/Sunday
```

### Dynamic Processor Configuration

```
# InvokeHTTP with dynamic URL:
Remote URL: https://api.${environment}.company.com/v2/orders?since=${last_sync_time}&limit=${batch_size:replaceNull('1000')}

# PublishKafka with dynamic topic:
Topic Name: ${source.system:toLower()}.${event.type:toLower()}.events
# "Shopify" + "Order" → "shopify.order.events"

# PutDatabaseRecord with dynamic table:
Table Name: ${target_schema}.${target_table}
# From attributes: target_schema=silver, target_table=orders
# Result: "silver.orders"
```

## Variables vs. Attributes in EL

```
# Variables: defined at Process Group level, shared across all processors in group
# Access: ${variable_name} (same syntax, but resolved from variable registry)

# Variable Registry (Process Group → Variables):
#   environment = "production"
#   s3_bucket = "company-data-lake"
#   db_schema = "silver"

# Usage in processor config:
PutS3Object:
  Bucket: ${s3_bucket}                    # From variable
  Object Key: ${filename}                 # From FlowFile attribute
  
# Resolution order:
# 1. FlowFile attribute (highest priority)
# 2. Process Group variable
# 3. NiFi variable registry (nifi.properties)
# If attribute and variable have same name → attribute wins!
```

## Performance Considerations

```
# GOOD: Simple attribute reference (instant):
${filename}

# GOOD: String operations (fast):
${filename:substringBefore('.')}

# CAUTION: Regex (slower, OK for routing decisions):
${filename:matches('.*\\d{8}.*')}

# AVOID: Complex nested conditions (hard to debug):
${a:equals('x'):and(${b:gt(5):or(${c:isEmpty()})}):ifElse(${d:append('_yes')}, ${e:prepend('no_')})}

# BETTER: Break complex logic into multiple UpdateAttribute steps:
# Step 1: is_eligible = ${a:equals('x'):and(${b:gt(5)})}
# Step 2: result = ${is_eligible:ifElse('yes', 'no')}
```

## Interview Tips

> **Tip 1:** "What's the difference between Expression Language and Record Path?" — EL operates on FlowFile ATTRIBUTES (key-value metadata). Record Path operates on FlowFile CONTENT (individual fields within records). EL syntax: `${attribute:function()}`. Record Path syntax: `/field/nested_field`. EL is used in most processor configs; Record Path is used only in record-based processors (UpdateRecord, LookupRecord).

> **Tip 2:** "How do you handle date/time in NiFi EL?" — `${now()}` gives current epoch millis. `${now():format('yyyy-MM-dd')}` formats it. Date math: subtract millis (`${now():toNumber():minus(86400000)}` = yesterday). Parse strings: `${date_str:toDate('yyyy-MM-dd')}`. Common use: date-partitioned output paths, SLA checks, data freshness validation.

> **Tip 3:** "Variables vs Attributes?" — Variables are defined at Process Group level (shared, static per deployment). Attributes are per-FlowFile (dynamic, different for each data item). Same `${name}` syntax resolves attributes first, then variables. Use variables for: environment config (bucket names, URLs). Use attributes for: data-specific values (filenames, timestamps, record counts).
