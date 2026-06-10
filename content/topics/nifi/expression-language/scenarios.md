---
title: "NiFi Expression Language - Scenario Questions"
topic: nifi
subtopic: expression-language
content_type: scenario_question
tags: [nifi, expression-language, interview, scenarios]
---

# Scenario Questions — NiFi Expression Language

<article data-difficulty="junior">

## 🟢 Junior: Writing Basic Expressions

**Scenario:** You have FlowFiles with these attributes: `filename = "sales_report_20240315_US.csv"`, `fileSize = "5242880"`, `source.system = "Salesforce"`. Write NiFi Expression Language for: (1) Extract the date from the filename ("20240315"), (2) Calculate file size in MB, (3) Create an S3 path like "salesforce/2024/03/15/sales_report_20240315_US.csv", (4) Determine if it's a large file (>10MB).

<details>
<summary>💡 Hint</summary>
Use: `substringAfter`/`substringBefore` or `replaceAll` with regex for date extraction. `divide(1048576)` for MB. Combine `toLower()` + `format()` functions for the path. `gt()` for comparison.
</details>

<details>
<summary>✅ Solution</summary>

```
# (1) Extract date "20240315" from filename:
${filename:replaceAll('.*_(\\d{8})_.*', '$1')}
# Result: "20240315"

# Alternative (without regex):
${filename:substringAfter('report_'):substring(0, 8)}
# "sales_report_20240315_US.csv" → after "report_" = "20240315_US.csv" → first 8 chars = "20240315"

# (2) File size in MB:
${fileSize:divide(1048576)}
# 5242880 / 1048576 = "5" (5 MB)

# (3) S3 path (salesforce/2024/03/15/sales_report_20240315_US.csv):
${source.system:toLower()}/${filename:replaceAll('.*_(\\d{4})(\\d{2})(\\d{2})_.*', '$1/$2/$3')}/${filename}
# "Salesforce" → "salesforce"
# Extract date parts: 2024/03/15
# Append filename
# Result: "salesforce/2024/03/15/sales_report_20240315_US.csv"

# Simpler alternative:
${source.system:toLower()}/${file_date:substring(0,4)}/${file_date:substring(4,6)}/${file_date:substring(6,8)}/${filename}
# (Assumes file_date attribute = "20240315" was computed earlier)

# (4) Is it a large file (>10MB)?
${fileSize:gt(10485760)}
# 5242880 > 10485760? → "false"

# For routing:
# RouteOnAttribute property:
#   large_file = ${fileSize:gt(10485760)}
```

**Key Points:**
- `replaceAll` with regex captures groups for extraction (`$1`, `$2`, `$3`)
- Integer division: `divide(1048576)` converts bytes to MB
- `toLower()` normalizes system names for consistent paths
- `gt()` returns boolean string ("true"/"false") for routing decisions
- Always compute complex expressions in UpdateAttribute first, reference simple attributes downstream

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Dynamic Multi-Source Routing

**Scenario:** Build Expression Language for a flow that handles data from 5 different sources. Each source needs: (1) a different output S3 bucket (from a `target_bucket` attribute set by LookupRecord), (2) date-partitioned paths using the event's timestamp (not processing time), (3) a dynamic filename with format `{source}_{event_type}_{timestamp}_{uuid}.avro`, (4) routing to "fast-lane" if source is "payments" OR if a `priority` attribute equals "critical", (5) retry delay that doubles each attempt (starting at 5 seconds).

<details>
<summary>💡 Hint</summary>
Event timestamp: parse from attribute with `toDate()` then `format()`. Dynamic filename: concatenate attributes with `append()`. Routing: combine conditions with `or()`. Exponential backoff: use `multiply` on retry count (2^n pattern with `math.pow` not available, so use `multiply(2)` on previous delay stored in attribute).
</details>

<details>
<summary>✅ Solution</summary>

```
# ═══════════════════════════════════════
# (1) S3 Output Configuration
# ═══════════════════════════════════════

# PutS3Object:
Bucket: ${target_bucket}
# target_bucket attribute comes from LookupRecord lookup
# Each source maps to its own bucket (e.g., "payments-lake", "orders-lake")

# ═══════════════════════════════════════
# (2) Date-Partitioned Path (EVENT time)
# ═══════════════════════════════════════

# Given attribute: event.timestamp = "2024-03-15T14:30:00Z"
# Target: source=payments/year=2024/month=03/day=15/hour=14/{filename}

Object Key: source=${source.system:toLower()}/year=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss'Z'"):format('yyyy')}/month=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss'Z'"):format('MM')}/day=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss'Z'"):format('dd')}/hour=${event.timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss'Z'"):format('HH')}/${output_filename}

# ═══════════════════════════════════════
# (3) Dynamic Filename
# ═══════════════════════════════════════

# UpdateAttribute:
output_filename = "${source.system:toLower()}_${event.type:toLower()}_${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('yyyyMMddHHmmss')}_${UUID()}.avro"

# Result: "payments_transaction_20240315143000_a1b2c3d4-e5f6.avro"

# ═══════════════════════════════════════
# (4) Fast-Lane Routing
# ═══════════════════════════════════════

# RouteOnAttribute:
fast_lane = ${source.system:toLower():equals('payments'):or(${priority:equals('critical')})}

# Breakdown:
# source.system:toLower():equals('payments') → true if source is payments
# priority:equals('critical') → true if priority is critical
# :or() → true if EITHER condition is true

# Additional routing options:
standard_lane = ${source.system:toLower():equals('payments'):not():and(${priority:equals('critical'):not()})}
# Not payments AND not critical → standard lane

# ═══════════════════════════════════════
# (5) Exponential Retry Delay
# ═══════════════════════════════════════

# Initial state (first attempt):
# UpdateAttribute at flow start:
retry.count = "0"
retry.delay.ms = "5000"     # Start at 5 seconds

# On failure (in retry path) — UpdateAttribute:
retry.count = "${retry.count:plus(1)}"
retry.delay.ms = "${retry.delay.ms:multiply(2)}"

# Sequence:
# Attempt 1: retry.count=0, retry.delay.ms=5000 (5s)
# Attempt 2: retry.count=1, retry.delay.ms=10000 (10s)
# Attempt 3: retry.count=2, retry.delay.ms=20000 (20s)
# Attempt 4: retry.count=3, retry.delay.ms=40000 (40s)

# Cap the maximum delay:
retry.delay.ms = "${retry.delay.ms:multiply(2):replaceAll('(\\d+)', '${literal(\"$1\"):toNumber():gt(60000):ifElse(\"60000\", \"$1\")}')}"
# Simpler approach: just use gt() in routing to stop:
# RouteOnAttribute:
#   give_up = ${retry.count:gt(3)}

# ═══════════════════════════════════════
# COMPLETE UpdateAttribute (all together):
# ═══════════════════════════════════════

# UpdateAttribute processor properties:
source_lower = "${source.system:toLower()}"
event_date = "${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('yyyy-MM-dd')}"
event_year = "${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('yyyy')}"
event_month = "${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('MM')}"
event_day = "${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('dd')}"
event_hour = "${event.timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss'Z'\"):format('HH')}"
output_filename = "${source_lower}_${event.type:toLower()}_${event_date:replace('-','')}_${UUID()}.avro"
is_fast_lane = "${source_lower:equals('payments'):or(${priority:equals('critical')})}"
s3_key = "source=${source_lower}/year=${event_year}/month=${event_month}/day=${event_day}/hour=${event_hour}/${output_filename}"
```

**Key Points:**
- **Parse event time ONCE** in UpdateAttribute, reuse parts downstream
- **toLower()** normalizes source names (handles "Payments" vs "payments")
- **or()** for multi-condition fast-lane logic
- **multiply(2)** on stored delay attribute creates exponential backoff
- **Computed attributes** simplify downstream processor configs (just `${s3_key}`)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Self-Documenting Pipeline with EL

**Scenario:** Design an Expression Language strategy for a production NiFi flow that provides complete observability: (1) every FlowFile carries its full processing history as attributes (which processors touched it, when, duration), (2) SLA tracking (time since ingestion vs. configured SLA per source), (3) data quality scores computed from validation results, (4) automatic alert severity classification, and (5) all attributes formatted for direct ingestion into a monitoring system (Prometheus/Datadog format). Show the EL expressions for each requirement.

<details>
<summary>💡 Hint</summary>
Use UpdateAttribute at each pipeline stage to timestamp entry/exit. Compute duration = exit - entry. Accumulate processing history in a delimited attribute. SLA = current time - ingestion time vs threshold. DQ score = passed_checks / total_checks × 100. Alert severity from score + SLA combined. Format for Prometheus: metric_name{labels} value.
</details>

<details>
<summary>✅ Solution</summary>

```
# ═══════════════════════════════════════════════════
# (1) PROCESSING HISTORY TRACKING
# ═══════════════════════════════════════════════════

# At EACH stage, UpdateAttribute adds timing:

# Stage 1 - Ingestion:
UpdateAttribute:
  pipeline.stage = "1-ingest"
  stage.1.ingest.start = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")}"
  pipeline.ingestion.time = "${now()}"    # Epoch millis for SLA calc

# After Stage 1 completes:
UpdateAttribute:
  stage.1.ingest.end = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")}"
  stage.1.ingest.duration.ms = "${now():toNumber():minus(${stage.1.ingest.start:toDate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"):toNumber()})}"

# Stage 2 - Validation:
UpdateAttribute:
  pipeline.stage = "2-validate"
  stage.2.validate.start = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")}"

# Stage 2 complete:
UpdateAttribute:
  stage.2.validate.end = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")}"
  stage.2.validate.duration.ms = "${now():toNumber():minus(${stage.2.validate.start:toDate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"):toNumber()})}"

# Processing history (accumulated):
  processing.history = "${processing.history:replaceNull('')}${processing.history:isEmpty():ifElse('', '|')}${pipeline.stage}:${now():format('HH:mm:ss')}"
  # Result: "1-ingest:10:30:01|2-validate:10:30:03|3-transform:10:30:05"

# Total pipeline duration:
  pipeline.total.duration.ms = "${now():toNumber():minus(${pipeline.ingestion.time:toNumber()})}"

# ═══════════════════════════════════════════════════
# (2) SLA TRACKING
# ═══════════════════════════════════════════════════

# Given: sla_max_seconds attribute from source config lookup

UpdateAttribute:
  # Current processing age:
  sla.age.seconds = "${now():toNumber():minus(${pipeline.ingestion.time:toNumber()}):divide(1000)}"
  
  # SLA percentage consumed:
  sla.consumed.pct = "${sla.age.seconds:divide(${sla_max_seconds}):multiply(100)}"
  
  # SLA status:
  sla.status = "${sla.age.seconds:lt(${sla_max_seconds}):ifElse('ON_TRACK', ${sla.consumed.pct:lt(120):ifElse('AT_RISK', 'BREACHED')})}"
  # ON_TRACK: within SLA
  # AT_RISK: 100-120% of SLA consumed
  # BREACHED: >120% of SLA

  # Time remaining:
  sla.remaining.seconds = "${sla_max_seconds:toNumber():minus(${sla.age.seconds:toNumber()})}"

# ═══════════════════════════════════════════════════
# (3) DATA QUALITY SCORING
# ═══════════════════════════════════════════════════

# After validation processors set check results:
# dq.check.completeness = "pass"
# dq.check.uniqueness = "pass"  
# dq.check.format = "fail"
# dq.check.range = "pass"
# dq.check.referential = "pass"

UpdateAttribute:
  # Count passes:
  dq.passed.count = "${dq.check.completeness:equals('pass'):ifElse('1','0'):plus(${dq.check.uniqueness:equals('pass'):ifElse('1','0')}):plus(${dq.check.format:equals('pass'):ifElse('1','0')}):plus(${dq.check.range:equals('pass'):ifElse('1','0')}):plus(${dq.check.referential:equals('pass'):ifElse('1','0')})}"
  
  dq.total.checks = "5"
  
  # Quality score (0-100):
  dq.score = "${dq.passed.count:divide(5):multiply(100)}"
  # 4/5 passed → 80
  
  # Quality tier:
  dq.tier = "${dq.score:gt(95):ifElse('excellent', ${dq.score:gt(80):ifElse('good', ${dq.score:gt(60):ifElse('acceptable', 'poor')})})}"

# ═══════════════════════════════════════════════════
# (4) AUTOMATIC ALERT SEVERITY
# ═══════════════════════════════════════════════════

UpdateAttribute:
  alert.severity = "${sla.status:equals('BREACHED'):and(${dq.tier:equals('poor')}):ifElse('P1_CRITICAL', ${sla.status:equals('BREACHED'):or(${dq.tier:equals('poor')}):ifElse('P2_HIGH', ${sla.status:equals('AT_RISK'):ifElse('P3_MEDIUM', 'P4_LOW')})})}"
  
  # Severity matrix:
  # SLA BREACHED + DQ poor → P1_CRITICAL
  # SLA BREACHED OR DQ poor → P2_HIGH
  # SLA AT_RISK → P3_MEDIUM
  # Everything else → P4_LOW

  alert.channel = "${alert.severity:equals('P1_CRITICAL'):ifElse('#critical-oncall', ${alert.severity:equals('P2_HIGH'):ifElse('#data-alerts-urgent', '#data-alerts')})}"

# ═══════════════════════════════════════════════════
# (5) MONITORING SYSTEM FORMAT (Prometheus-style)
# ═══════════════════════════════════════════════════

UpdateAttribute:
  # Prometheus exposition format:
  metric.pipeline.duration = "nifi_pipeline_duration_seconds{source=\"${source.system}\",stage=\"${pipeline.stage}\",env=\"${env}\"} ${pipeline.total.duration.ms:divide(1000)}"
  
  metric.dq.score = "nifi_dq_score{source=\"${source.system}\",pipeline=\"${pipeline.name}\"} ${dq.score}"
  
  metric.sla.consumed = "nifi_sla_consumed_pct{source=\"${source.system}\",sla_seconds=\"${sla_max_seconds}\"} ${sla.consumed.pct}"
  
  metric.records.processed = "nifi_records_processed_total{source=\"${source.system}\",status=\"${dq.tier}\"} ${record.count}"

# These attributes can be:
# 1. Published to Kafka metrics topic via PublishKafka
# 2. Sent to Prometheus Pushgateway via InvokeHTTP
# 3. Logged via LogAttribute for Datadog agent pickup
```

**Key Points:**
- **Timing at every stage**: `start` + `end` + `duration.ms` attributes per stage
- **Processing history**: Accumulated delimited string shows full path through pipeline
- **SLA computation**: Age vs. threshold, with percentage and status levels
- **DQ scoring**: Count passing checks, compute percentage, classify into tiers
- **Alert severity matrix**: Combines SLA status + DQ tier into priority level
- **Monitoring format**: Pre-formatted for direct Prometheus/Datadog ingestion
- **All done in EL**: No custom code, no external systems — pure attribute computation
- **Debuggability**: Any FlowFile in the system carries its complete processing story

</details>

</article>

</content>

---

## ⚡ Quick-fire Q&A

**Q: What is NiFi Expression Language (EL) and where can it be used?**
A: NiFi EL is a domain-specific language embedded in `${...}` syntax that evaluates FlowFile attributes and system variables at runtime. It can be used in processor properties that display the EL indicator icon, enabling dynamic configuration without custom code.

**Q: How would you extract just the filename without extension from the `filename` attribute?**
A: `${filename:substringBeforeLast('.')}` uses the `substringBeforeLast` function to strip the extension. This is cleaner than regex for simple cases and more readable in property fields.

**Q: What is the difference between `${literal('text')}` and just writing `text` in a property?**
A: `${literal('text')}` explicitly invokes EL and returns the string "text". It is useful when you want to chain EL functions: `${literal('prefix_'):append(${filename})}`. Plain text is just a static value; EL is only evaluated when the expression markers are present.

**Q: How do you perform arithmetic in NiFi EL?**
A: Use numeric functions: `${fileSize:divide(1024):toWholeNumber()}` converts bytes to kilobytes. EL supports `plus`, `minus`, `multiply`, `divide`, `mod`, and type coercions like `toLong` and `toDecimal`.

**Q: How would you set a FlowFile attribute to the current timestamp formatted as `yyyy-MM-dd`?**
A: `${now():format('yyyy-MM-dd')}` — `now()` returns the current epoch millisecond and `format()` applies a Java SimpleDateFormat pattern.

**Q: What happens if an EL expression references an attribute that does not exist?**
A: EL returns an empty string, not an error. This can cause silent bugs (e.g., a route condition always evaluating false). Use `${attribute:isEmpty()}` or `${attribute:isNull()}` to guard against missing attributes.

**Q: How can you use EL to route FlowFiles conditionally in a RouteOnAttribute processor?**
A: Define a route property with an EL condition: `${status:equals('error')}` returns true/false, routing the FlowFile to the matching relationship. Multiple properties create multiple output relationships.

**Q: Can NiFi EL access environment variables or NiFi variables defined in the Variable Registry?**
A: Yes. Variables defined in the NiFi Variable Registry (process group or global) are accessible via `${variable_name}`. System environment variables are accessible via `${ENV_VAR}` if the property is EL-enabled and the administrator has not restricted environment access.

---

## 💼 Interview Tips

- Demonstrate function chaining fluency—interviewers often give a raw attribute value and ask you to transform it using EL. Practice `substring`, `replace`, `format`, `toDate`, and `split` chains.
- Explain *where* EL is supported: not all processor properties accept EL (look for the EL icon). Trying to use EL in a non-EL field is a common junior mistake.
- Senior interviewers probe edge cases: what happens with null/missing attributes, what happens with type mismatches in numeric functions. Show you know to validate and default.
- Mention performance: EL is evaluated per-FlowFile, so complex expressions in a high-throughput path add CPU cost. For heavy transformations, prefer a scripted processor or record-based approach.
- When discussing routing, clearly articulate the RouteOnAttribute match strategy (route to first match vs. all matches) and how EL boolean expressions drive it.
