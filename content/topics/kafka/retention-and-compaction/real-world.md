---
title: "Retention and Compaction - Real World"
topic: kafka
subtopic: retention-and-compaction
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [kafka, retention, compaction, storage-management, GDPR, operational-patterns]
---

# Kafka Retention and Compaction — Real World Patterns

## Pattern 1: GDPR Right-to-Erasure on Compacted Topics

Implementing "right to be forgotten" in Kafka requires sending tombstones to compacted topics.

```python
from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)

class GDPREraser:
    """Handles right-to-erasure requests for Kafka-stored personal data."""

    COMPACTED_TOPICS = [
        'user-profiles',
        'user-preferences',
        'user-sessions',
    ]

    def __init__(self, bootstrap: str):
        self.producer = Producer({
            'bootstrap.servers': bootstrap,
            'enable.idempotence': True,
            'acks': 'all',
        })

    def erase_user(self, user_id: str) -> dict:
        results = {}
        for topic in self.COMPACTED_TOPICS:
            try:
                # Tombstone: null value deletes this key from compacted topic
                self.producer.produce(
                    topic=topic,
                    key=user_id.encode(),
                    value=None,   # tombstone
                )
                results[topic] = 'tombstone_sent'
                logger.info("Tombstone sent for user %s on topic %s", user_id, topic)
            except Exception as e:
                results[topic] = f'error: {e}'
                logger.error("Failed to send tombstone for %s on %s: %s", user_id, topic, e)

        self.producer.flush()
        return results

    def verify_erasure(self, user_id: str, sr_url: str) -> dict:
        """Verify tombstone is present and no live records exist."""
        from confluent_kafka import Consumer, TopicPartition
        consumer = Consumer({
            'bootstrap.servers': 'broker:9092',
            'group.id': f'gdpr-verifier-{user_id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        results = {}
        for topic in self.COMPACTED_TOPICS:
            latest_value = None
            consumer.assign([TopicPartition(topic, 0)])  # simplified: check partition 0

            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.key() == user_id.encode():
                    latest_value = msg.value()   # None = tombstone

            results[topic] = 'erased' if latest_value is None else 'NOT_ERASED'

        consumer.close()
        return results
```

**Important caveat**: Kafka tombstones are not instant erasure. Records remain until compaction runs (which can take hours or days depending on `min.cleanable.dirty.ratio` and `max.compaction.lag.ms`). For true GDPR compliance, you may need to:
1. Send tombstones (for future compaction)
2. Track erasure requests in a separate compliance log
3. Attest completion after verifying compaction has run

## Pattern 2: Multi-Tier Retention Strategy

Different topics in the same cluster often have different retention needs. A tiered strategy:

```python
# retention_config.py
TOPIC_RETENTION_PROFILES = {
    # Profile: operational (short-lived events)
    'operational': {
        'cleanup.policy': 'delete',
        'retention.ms': str(24 * 3600 * 1000),   # 1 day
        'segment.ms': str(3600 * 1000),            # 1 hour segments for fast cleanup
        'retention.bytes': str(10 * 1024 ** 3),    # 10 GB per partition
    },
    # Profile: business events (medium-term replay)
    'business': {
        'cleanup.policy': 'delete',
        'retention.ms': str(30 * 24 * 3600 * 1000),  # 30 days
        'segment.bytes': str(256 * 1024 ** 2),         # 256 MB segments
    },
    # Profile: state store (compacted, indefinite)
    'state': {
        'cleanup.policy': 'compact',
        'min.cleanable.dirty.ratio': '0.1',
        'max.compaction.lag.ms': str(3600 * 1000),   # compact within 1h
        'delete.retention.ms': str(24 * 3600 * 1000),
    },
    # Profile: audit (long-term, compliance)
    'audit': {
        'cleanup.policy': 'delete',
        'retention.ms': str(365 * 24 * 3600 * 1000),  # 1 year
        'segment.ms': str(24 * 3600 * 1000),            # daily segments
    },
}

def apply_retention_profile(bootstrap: str, topic: str, profile: str):
    from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry
    admin = AdminClient({'bootstrap.servers': bootstrap})
    config = TOPIC_RETENTION_PROFILES[profile]

    resources = [ConfigResource(
        ConfigResource.Type.TOPIC,
        topic,
        set_config={k: v for k, v in config.items()}
    )]
    futures = admin.alter_configs(resources)
    for resource, future in futures.items():
        future.result()   # raises on error
    print(f"Applied '{profile}' retention profile to topic '{topic}'")
```

## Pattern 3: Monitoring Compaction Health

```python
import json
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge

compaction_lag = Gauge('kafka_topic_uncompacted_bytes', 'Uncompacted bytes per topic-partition',
                       ['topic', 'partition'])
oldest_dirty_ratio = Gauge('kafka_topic_dirty_ratio', 'Dirty ratio per topic-partition',
                           ['topic', 'partition'])

def collect_compaction_metrics(bootstrap: str):
    """Collect compaction health metrics from broker JMX or admin API."""
    # In practice, these metrics come from JMX:
    # kafka.log:type=LogCleaner,name=max-dirty-percent
    # kafka.log:type=LogCleaner,name=max-compaction-delay

    # Simplified: check log directory sizes via admin API
    admin = AdminClient({'bootstrap.servers': bootstrap})
    topics = admin.list_topics(timeout=10).topics

    for topic_name, topic_meta in topics.items():
        if topic_name.startswith('_') or topic_name.startswith('connect-'):
            continue
        for partition_id in topic_meta.partitions:
            # Fetch log start and end offset as proxy for log size
            pass   # Real implementation uses JMX or broker metrics endpoint
```

**Key JMX metrics for compaction health:**

| Metric | Description | Alert When |
|--------|-------------|-----------|
| `kafka.log:type=LogCleaner,name=max-dirty-percent` | Highest dirty ratio across all compacted partitions | > 80% |
| `kafka.log:type=LogCleaner,name=time-since-last-run-ms` | Time since log cleaner last ran | > 1h |
| `kafka.log:type=LogCleaner,name=max-compaction-delay-secs` | Oldest uncompacted record age | > `max.compaction.lag.ms` |
| `kafka.log:type=Log,name=Size,topic=X,partition=Y` | Partition log size in bytes | Exceeds expected |

## Pattern 4: Disk Full Prevention

```bash
#!/bin/bash
# disk_usage_monitor.sh — alert before disk fills

THRESHOLD=85  # percent
BOOTSTRAP="broker:9092"
ALERT_EMAIL="kafka-ops@example.com"

df -h /var/kafka/data | awk 'NR==2 {print $5}' | tr -d '%' | while read usage; do
  if [ "$usage" -gt "$THRESHOLD" ]; then
    echo "Kafka disk usage ${usage}% on $(hostname)" | \
      mail -s "ALERT: Kafka disk almost full" "$ALERT_EMAIL"

    # Emergency: reduce retention on highest-volume topics
    kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
      --alter \
      --add-config 'retention.ms=3600000' \  # emergency 1-hour retention
      --entity-type topics --entity-name high-volume-logs
  fi
done
```

**Root causes of unexpected disk growth:**
1. Slow consumers holding replication slot (Debezium) → WAL growth
2. `segment.ms` too large → active segments not rolling → cleanup blocked
3. Compaction not triggering → `log.cleaner.threads` too low or buffer too small
4. Topic creation without explicit retention → using broker default (may be 7 days)

## Pattern 5: Event Sourcing with Infinite Retention

For event-sourced systems, topics must be retained indefinitely. But this creates operational challenges:

```bash
# Infinite retention (careful — storage grows forever)
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter \
  --add-config 'retention.ms=-1,retention.bytes=-1' \
  --entity-type topics --entity-name domain-events

# Tiered storage makes infinite retention practical
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter \
  --add-config 'remote.storage.enable=true,
                local.retention.ms=86400000,
                retention.ms=-1' \
  --entity-type topics --entity-name domain-events
```

**Operational strategy for infinite retention without tiered storage:**
1. Keep recent (30 days) on broker SSD
2. Use MirrorMaker 2 to continuously replicate to a "cold" cluster with cheaper storage
3. Route recent queries to hot cluster; historical queries to cold cluster

## Interview Tips

> **Tip 1:** GDPR erasure on Kafka is a common advanced question. Tombstones initiate the deletion, but actual erasure happens only after compaction runs. For compliance, you must track tombstone sending + verify compaction completion. Some organizations use encryption per-user-key and rotate the key to achieve erasure without waiting for compaction.

> **Tip 2:** The segment rolling vs retention interaction is the most common misconfiguration. If `segment.ms=7d` and `retention.ms=1h`, no data is cleaned for 7 days because the active segment never rolls. Always set `segment.ms < retention.ms`.

> **Tip 3:** For event sourcing use cases, tiered storage is the enabler. Without it, infinite retention on a cluster with terabytes-per-day throughput is cost-prohibitive. Describe the local + S3 tiered approach when discussing long-term event retention.

> **Tip 4:** Disk full is a Kafka cluster killer — brokers stop accepting writes when disk hits 95%+ (configurable). Production clusters need disk usage alerts at 70%, 80%, and 85% with automated actions at 85% (reduce retention, expand storage, add broker).

> **Tip 5:** The `min.compaction.lag.ms` + `max.compaction.lag.ms` pair is important for compacted topics. Without `max.compaction.lag.ms`, low-write-rate topics may never compact (dirty ratio never reaches threshold). Setting it ensures compaction eventually happens.
