---
title: "Streaming Data Quality - Scenario Questions"
topic: real-time-streaming
subtopic: streaming-data-quality
content_type: scenario_question
tags: [streaming, data-quality, scenarios, interview, dlq, schema, watermarks]
---

# Streaming Data Quality — Scenario Questions

<article data-difficulty="junior">

## Scenario: Null Values Flooding the Analytics Table

Your team runs a Flink pipeline that reads click events from Kafka and writes them to a Delta Lake table for analytics. The BI team reports that the dashboard is showing many rows where `user_id` is NULL, which breaks user-level metrics. The pipeline is running without errors.

**Questions:**
1. Why does a streaming pipeline write NULLs without throwing errors?
2. How would you add a null check in Flink to detect and route these records?
3. Where should the invalid records go, and what information should they contain?
4. How would you monitor the null rate and set up an alert?

<details>
<summary>✅ Solution</summary>

**1. Why NULLs don't cause errors:**
Flink's JSON deserializer (and most deserializers) treat missing or null fields as `null` in the resulting object — no exception is thrown. The pipeline happily processes the record, writes it to Delta with `user_id = NULL`, and moves on. There's no built-in DQ enforcement unless you explicitly add it.

**2. Adding a null check in Flink:**
```python
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import ProcessFunction
from pyflink.common.typeinfo import Types

dlq_tag = OutputTag("dlq", Types.STRING())

class NullUserIdFilter(ProcessFunction):
    def process_element(self, record: dict, ctx, collector):
        if record.get("user_id") is None:
            # Route to DLQ with context
            import json
            dlq_record = {
                "original": record,
                "violation": "NULL_USER_ID",
                "check": "completeness",
                "detected_at_ms": ctx.timestamp_of_current_processing_time()
            }
            collector.collect_side_output(dlq_tag, json.dumps(dlq_record))
        else:
            collector.collect(record)

validated_stream = stream.process(NullUserIdFilter())
good_stream = validated_stream  # main output: valid records
dlq_stream = validated_stream.get_side_output(dlq_tag)  # DLQ records
```

**3. DLQ destination and format:**
Route invalid records to a dedicated Kafka topic (e.g., `click-events-dlq`). Each DLQ record should include:
- The original raw record (for replay after fixing)
- The violation type and field name
- The source Kafka topic, partition, and offset (for traceability)
- Timestamp of detection
- The pipeline name and version

This allows the team to: (a) alert on DLQ volume, (b) investigate root cause using offset info, (c) fix and replay the records once the producer is corrected.

**4. Monitoring null rate and alerts:**
```python
# Flink: emit null rate metric
from pyflink.datastream.functions import RichFlatMapFunction

class NullRateMetricsEmitter(RichFlatMapFunction):
    def open(self, config):
        group = self.get_runtime_context().get_metrics_group()
        self.total = group.counter("total_records")
        self.null_user_id = group.counter("null_user_id_count")

    def flat_map(self, record: dict, collector):
        self.total.inc()
        if record.get("user_id") is None:
            self.null_user_id.inc()
        collector.collect(record)
```

Alert rule (Prometheus):
```
ALERT NullUserIdRateHigh
  IF (rate(null_user_id_count[5m]) / rate(total_records[5m])) > 0.01
  FOR 3m
  LABELS { severity = "warning" }
  ANNOTATIONS { summary = "NULL user_id rate > 1% in click events pipeline" }
```

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Streaming DQ for a Multi-Producer Kafka Topic

Your company has a Kafka topic `product-events` fed by three different microservices: the catalog service, the inventory service, and the pricing service. Each service has a slightly different event schema (different field names, some optional fields only from certain producers). You need to implement streaming DQ that:
- Validates each event against the appropriate schema based on its `event_type` field
- Routes invalid events to a DLQ with the producer identified
- Detects when a producer stops sending events (silence detection)
- Tracks DQ metrics per producer

**Questions:**
1. How would you design the schema validation to handle multiple schemas in one topic?
2. How would you detect producer silence in a streaming context?
3. What DQ metrics would you track per producer, and how would you aggregate them?
4. How should the DLQ records be structured to facilitate debugging per producer?

<details>
<summary>✅ Solution</summary>

**1. Multi-schema validation:**
```python
from pyflink.datastream.functions import ProcessFunction
import json

# Schema definitions per event_type
EVENT_SCHEMAS = {
    "product.created": {
        "required": ["product_id", "name", "category", "event_time"],
        "types": {"product_id": str, "name": str, "price": float}
    },
    "inventory.updated": {
        "required": ["product_id", "warehouse_id", "quantity", "event_time"],
        "types": {"quantity": int, "product_id": str}
    },
    "price.changed": {
        "required": ["product_id", "new_price", "old_price", "event_time"],
        "types": {"new_price": float, "old_price": float}
    }
}

class MultiSchemaValidator(ProcessFunction):
    def process_element(self, record: dict, ctx, collector):
        event_type = record.get("event_type")

        if event_type not in EVENT_SCHEMAS:
            collector.collect_side_output(dlq_tag, {
                "original": record,
                "error": f"UNKNOWN_EVENT_TYPE: {event_type}",
                "producer": record.get("source_service", "unknown")
            })
            return

        schema = EVENT_SCHEMAS[event_type]
        errors = []

        # Check required fields
        for field in schema["required"]:
            if record.get(field) is None:
                errors.append(f"Missing required field: {field}")

        # Check types
        for field, expected_type in schema.get("types", {}).items():
            value = record.get(field)
            if value is not None and not isinstance(value, expected_type):
                errors.append(
                    f"Wrong type for {field}: expected {expected_type.__name__}, "
                    f"got {type(value).__name__}"
                )

        if errors:
            collector.collect_side_output(dlq_tag, {
                "original": record,
                "errors": errors,
                "event_type": event_type,
                "producer": record.get("source_service", "unknown"),
                "detected_at": ctx.timestamp_of_current_processing_time()
            })
        else:
            collector.collect(record)
```

**2. Producer silence detection:**
```python
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class ProducerSilenceDetector(KeyedProcessFunction):
    """
    Key by producer/source_service.
    Register a timer for 5 minutes.
    If no events arrive before timer fires → emit silence alert.
    Reset timer on each event.
    """
    SILENCE_THRESHOLD_MS = 5 * 60 * 1000  # 5 minutes

    def open(self, config):
        self.last_event_time = self.get_runtime_context().get_state(
            ValueStateDescriptor("last_event_time", Types.LONG())
        )

    def process_element(self, record: dict, ctx, collector):
        # Cancel previous timer and set a new one
        if self.last_event_time.value() is not None:
            ctx.timer_service().delete_processing_time_timer(
                self.last_event_time.value() + self.SILENCE_THRESHOLD_MS
            )

        now = ctx.timestamp_of_current_processing_time()
        self.last_event_time.update(now)
        ctx.timer_service().register_processing_time_timer(
            now + self.SILENCE_THRESHOLD_MS
        )
        collector.collect(record)

    def on_timer(self, timestamp, ctx, collector):
        # Timer fired without being reset → producer is silent
        producer = ctx.get_current_key()
        collector.collect_side_output(silence_alert_tag, {
            "alert": "PRODUCER_SILENCE",
            "producer": producer,
            "silent_since_ms": self.last_event_time.value(),
            "detected_at_ms": timestamp
        })

stream.key_by(lambda r: r.get("source_service", "unknown")) \
      .process(ProducerSilenceDetector())
```

**3. Per-producer DQ metrics:**
```python
class PerProducerDQMetrics(RichMapFunction):
    def open(self, config):
        # Use metric group with producer label
        self.counters = {}  # producer → Counter

    def get_counter(self, producer: str, metric: str):
        key = f"{producer}:{metric}"
        if key not in self.counters:
            self.counters[key] = (
                self.get_runtime_context()
                .get_metrics_group()
                .add_group("producer", producer)
                .counter(metric)
            )
        return self.counters[key]

    def map(self, record: dict):
        producer = record.get("source_service", "unknown")
        self.get_counter(producer, "total_events").inc()
        if any(record.get(f) is None for f in ["product_id", "event_time"]):
            self.get_counter(producer, "null_events").inc()
        return record
```

**4. DLQ record structure for per-producer debugging:**
```json
{
  "dlq_metadata": {
    "pipeline": "product-events-validator",
    "pipeline_version": "2.1.0",
    "detected_at": "2024-01-15T14:30:00.123Z",
    "source_topic": "product-events",
    "source_partition": 2,
    "source_offset": 483920,
    "kafka_timestamp": "2024-01-15T14:29:59.000Z"
  },
  "producer": {
    "service": "catalog-service",
    "version": "1.4.2",
    "instance": "catalog-pod-abc123"
  },
  "violations": [
    {
      "check": "REQUIRED_FIELD",
      "field": "category",
      "severity": "CRITICAL"
    },
    {
      "check": "TYPE_MISMATCH",
      "field": "price",
      "expected_type": "float",
      "actual_type": "string",
      "actual_value": "\"19.99\""
    }
  ],
  "original_record": { ... }
}
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Designing a Self-Service Streaming DQ Platform

You are leading the data platform team at a company with 50+ data engineers running 200+ streaming Flink and Spark jobs. Currently:
- Each team implements DQ checks differently (or not at all)
- There is no centralized DQ visibility — nobody knows the overall DQ health
- DQ failures are discovered by downstream consumers hours or days later
- Schema changes from one team break another team's pipeline silently
- There is no SLA enforcement for data quality

Design a **self-service streaming DQ platform** that:
- Allows teams to define DQ rules without writing custom Flink code
- Provides centralized DQ observability
- Enforces data contracts at the Kafka layer
- Alerts in real-time on DQ violations

**Questions:**
1. Design the platform architecture. What components are needed?
2. How would you implement a declarative DQ rule engine that teams configure rather than code?
3. How do you enforce data contracts at the Kafka level to prevent silent schema changes?
4. Design the observability layer: what metrics, dashboards, and alerting are needed?
5. How do you handle the organizational challenge of 50 teams adopting the platform?

<details>
<summary>✅ Solution</summary>

**1. Platform architecture:**

```
Self-Service Streaming DQ Platform:

  ┌────────────────────────────────────────────────────────────┐
  │  Control Plane (Data Contract Registry)                     │
  │  - UI for teams to define DQ rules (YAML or web form)      │
  │  - Contract versioning and approval workflow                │
  │  - Schema registry integration (Avro/Protobuf schemas)     │
  │  - API for pipelines to fetch their contract at startup     │
  └────────────────────────────────────────────────────────────┘
                             ↓
  ┌────────────────────────────────────────────────────────────┐
  │  DQ Sidecar / Library (embedded in each pipeline)          │
  │  - Flink/Spark library: load contract → validate records   │
  │  - Schema validation (via schema registry)                 │
  │  - Rule engine: execute configured checks                  │
  │  - Route failures to DLQ                                   │
  │  - Emit metrics to DQ metrics Kafka topic                  │
  └────────────────────────────────────────────────────────────┘
                             ↓
  ┌────────────────────────────────────────────────────────────┐
  │  Observability Layer                                        │
  │  - Kafka: dq-metrics topic                                 │
  │  - Flink consumer: aggregate metrics per pipeline/topic    │
  │  - Prometheus: expose metrics                              │
  │  - Grafana: centralized DQ dashboard                       │
  │  - PagerDuty/Slack: alert routing                          │
  └────────────────────────────────────────────────────────────┘
                             ↓
  ┌────────────────────────────────────────────────────────────┐
  │  DLQ Management                                             │
  │  - DLQ catalog: search/browse failed records               │
  │  - Auto-healing: classify and replay fixable records       │
  │  - SLA tracking: alert on stale unresolved DLQ records     │
  └────────────────────────────────────────────────────────────┘
```

**2. Declarative DQ rule engine:**
```python
# Teams define DQ rules in YAML (checked into git, reviewed via PR)
# dq_contract.yaml for the orders team:
"""
pipeline: orders-processor
topic: orders
schema:
  format: avro
  subject: orders-value

checks:
  - name: order_id_not_null
    type: not_null
    field: order_id
    severity: CRITICAL

  - name: amount_positive
    type: range
    field: amount
    min: 0.01
    max: 1000000
    severity: CRITICAL

  - name: event_time_not_stale
    type: field_age
    field: event_time_ms
    max_age_seconds: 300
    severity: WARNING

  - name: order_id_unique
    type: unique
    field: order_id
    window: 2h
    severity: CRITICAL

  - name: volume_normal
    type: volume_anomaly
    window: 5m
    z_score_threshold: 3.0
    severity: WARNING
"""

# DQ library: load contract and generate validators at runtime
class DQRuleEngine:
    def __init__(self, contract_yaml: str):
        contract = yaml.safe_load(contract_yaml)
        self.checks = self._compile_checks(contract["checks"])

    def _compile_checks(self, check_configs: list) -> list:
        check_map = {
            "not_null": NotNullCheck,
            "range": RangeCheck,
            "field_age": FieldAgeCheck,
            "unique": UniqueCheck,
            "volume_anomaly": VolumeAnomalyCheck,
        }
        return [check_map[c["type"]](c) for c in check_configs]

    def validate(self, record: dict) -> list[DQViolation]:
        violations = []
        for check in self.checks:
            result = check.evaluate(record)
            if not result.passed:
                violations.append(result.violation)
        return violations
```

**3. Kafka-level contract enforcement:**

```
Schema Registry as the enforcement point:

  1. Teams register schemas via the contract registry UI (not directly to SR)
  2. Contract registry validates schema against DQ rules (e.g., required fields have non-null types)
  3. Contract registry submits approved schema to Confluent Schema Registry
  4. Producers must use the registered schema to write to Kafka
  5. Any incompatible schema change → SR rejects the write → deployment fails fast

  Additional: Kafka Topic Interceptors (custom):
    - Kafka client-side interceptor validates message against current schema
    - Interceptor is embedded in the producer library
    - Teams use the company's internal Kafka client → DQ enforced automatically

  For multi-language support:
    - Python: confluent-kafka with schema registry + custom validators
    - Java: Kafka client wrapper that bundles the interceptor
    - Go: Similar wrapper
```

**4. Observability design:**
```
Metrics hierarchy (Prometheus labels):
  {pipeline, topic, check_name, severity, passed}

Key dashboards:
  1. Platform-wide DQ health:
     - % of pipelines with DQ failures in last 1h
     - Top 10 checks failing across all pipelines
     - DLQ volume trend (total platform)

  2. Per-pipeline DQ dashboard:
     - Pass rate per check over time
     - Null rates per field
     - Volume anomaly events
     - DLQ records by violation type

  3. Topic-level health:
     - Producer silence alerts
     - Schema version distribution (detect if some consumers on old schema)
     - Consumer lag correlated with DQ error rate

Alert routing (by severity + team):
  CRITICAL failures → PagerDuty (on-call) + Slack #team-channel
  WARNING trends    → Slack #team-channel (daily digest)
  INFO metrics      → Dashboard only

SLA: DQ violations must be acknowledged within 4h of CRITICAL alert
     DLQ records must be resolved within 7 days (automated enforcement)
```

**5. Organizational adoption strategy:**

```
Phase 1 — Platform teams adopt first (months 1-2):
  - Core data platform team dogfoods the platform on internal pipelines
  - Find rough edges before exposing to 50 other teams
  - Build runbooks and documentation

Phase 2 — High-impact, willing teams (months 3-4):
  - Identify 3-5 teams with existing DQ pain points
  - Work closely with them to onboard (white-glove support)
  - Capture their DQ rules as platform templates
  - Build case studies from their DQ improvements

Phase 3 — Self-service rollout (months 5-8):
  - Mandate DQ contracts for all new topics (contractual, not retroactive)
  - Provide migration tooling for existing topics
  - Add DQ coverage to engineering KPIs ("% of pipelines with DQ contracts")
  - Incentive: teams with DQ contracts get faster incident support (SLA)

Phase 4 — Enforcement (months 9+):
  - All production pipelines must have DQ contracts
  - Automated compliance checks in CI/CD
  - Non-compliant pipelines flagged in platform catalog

Key success factors:
  - Zero-friction onboarding: one YAML file + one library import
  - Team autonomy: teams own their rules, platform owns the infrastructure
  - Visible value: dashboard shows DQ health improving over time
  - No blame: DQ violations are learning opportunities, not punishments
```

</details>
</article>
