---
title: "Dataflow / Apache Beam — Intermediate"
topic: gcp
subtopic: dataflow
content_type: study_material
difficulty_level: mid-level
tags: [gcp, dataflow, interview]
---

# Dataflow / Apache Beam — Intermediate

Mid-level Dataflow interviews focus on the streaming semantics most engineers get wrong: triggers and accumulation modes, allowed lateness, the DoFn lifecycle, side inputs vs CoGroupByKey, autoscaling behavior, and template types.

## Triggers: When Windows Emit

The watermark decides the *default* firing, but triggers let you emit early (speculative) and late (corrections):

```python
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)

scored = (
    events
    | beam.WindowInto(
        window.FixedWindows(300),                       # 5-min windows
        trigger=AfterWatermark(
            early=AfterProcessingTime(60),              # preview every 60s
            late=AfterCount(1)                          # re-fire per late element
        ),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=3600                           # accept 1h of lateness
    )
    | beam.CombinePerKey(sum)
)
```

This produces, per window: early speculative results every minute → an **on-time** result when the watermark passes → corrected results for stragglers up to 1 hour late. After `allowed_lateness`, late data is **dropped** (count it with a metric!).

### Accumulating vs Discarding

| Mode | Each firing emits | Downstream must |
|------|-------------------|-----------------|
| ACCUMULATING | The full updated result | Overwrite/upsert previous value |
| DISCARDING | Only the delta since last firing | Sum the panes |

Classic interview trap: ACCUMULATING + a downstream that *sums* panes = double counting. Match the mode to the sink semantics (upsert ⇒ accumulating; additive sink ⇒ discarding).

## The DoFn Lifecycle

```python
class EnrichDoFn(beam.DoFn):
    def setup(self):
        # once per DoFn instance (per worker process): open clients
        self.client = create_api_client()

    def start_bundle(self):
        self.buffer = []

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        result = self.client.lookup(element["id"])
        yield {**element, "enriched": result}

    def finish_bundle(self):
        pass  # flush buffers; emit with window info if needed

    def teardown(self):
        self.client.close()
```

Why it matters:

- Expensive resources (DB connections, ML models) belong in `setup`, **never** in `process` — creating a client per element is the #1 performance bug.
- A **bundle** is the unit of commit/retry. If any element in a bundle fails, the whole bundle is retried — so side effects in `process` can happen more than once. **Exactly-once applies to state and sink integration, not to your arbitrary side effects.** Make external calls idempotent.
- Streaming retries failing bundles forever (poison pill stalls the pipeline); batch fails the job after 4 attempts of a bundle.

## Side Inputs vs CoGroupByKey

Two ways to "join":

| | Side input | CoGroupByKey |
|---|-----------|--------------|
| Shape | Small dataset broadcast to all workers | Full shuffle of both sides by key |
| Size limit | Must fit in worker memory (cached) | Arbitrarily large |
| Freshness | Recomputed per window (can use slowly-updating pattern) | Per window |
| Use | Dimension/config lookup | Large-large joins |

```python
rates = p | "ReadRates" >> beam.io.ReadFromText(...) | beam.Map(parse_rate)

priced = orders | beam.Map(
    lambda order, rates: {**order, "usd": order["amt"] * rates[order["ccy"]]},
    rates=beam.pvalue.AsDict(rates),
)
```

## Autoscaling Mechanics

- **Batch**: scales on throughput-based estimates of remaining work; dynamic work rebalancing splits hot ranges away from stragglers.
- **Streaming**: scales on backlog (Pub/Sub backlog seconds, stage backlog) and CPU utilization. Upscale when backlog grows; downscale when CPU is low and backlog is near zero.
- Key flags:

```bash
python pipeline.py \
  --runner=DataflowRunner \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --max_num_workers=50 \
  --num_workers=5 \
  --machine_type=n2-standard-4 \
  --region=us-central1
```

- **Streaming Engine**: moves shuffle/state off worker VMs into a Google backend service. Benefits: smaller/cheaper workers, faster and smoother autoscaling, better resilience. Enable with `--enable_streaming_engine` (default in many regions now).
- Streaming jobs scale keyed work: parallelism is ultimately bounded by key cardinality of GroupByKey stages — few hot keys ⇒ adding workers won't help.

## Late Data Handling — the Full Story

1. Element timestamp < watermark when it arrives ⇒ it's *late*.
2. If within `allowed_lateness` of its window ⇒ window re-fires per the late trigger.
3. Past allowed lateness ⇒ silently dropped (instrument with `Metrics.counter`).
4. Watermarks come from sources: Pub/Sub watermark is estimated; Kafka uses per-partition idle/lag heuristics. A stuck source partition holds the watermark back for the entire pipeline — windows stop closing. (Common production incident!)

```python
from apache_beam.metrics import Metrics

class TagLate(beam.DoFn):
    late_counter = Metrics.counter("quality", "late_dropped_candidates")

    def process(self, el, ts=beam.DoFn.TimestampParam,
                pane=beam.DoFn.PaneInfoParam):
        if pane.timing == beam.utils.windowed_value.PaneInfoTiming.LATE:
            self.late_counter.inc()
        yield el
```

## Templates: Classic vs Flex

| | Classic template | Flex template |
|---|------------------|---------------|
| Packaging | Staged graph file in GCS | Docker image + spec JSON |
| Runtime parameters | Only via `ValueProvider` (limited) | Any pipeline option; graph built at launch |
| Dynamic DAG per launch | No | Yes |
| Recommendation | Legacy | **Default choice today** |

```bash
# Build a Flex template
gcloud dataflow flex-template build gs://my-bucket/templates/my_job.json \
  --image-gcr-path us-central1-docker.pkg.dev/my-proj/repo/my-job:1.0 \
  --sdk-language PYTHON \
  --flex-template-base-image PYTHON3 \
  --py-path . \
  --env FLEX_TEMPLATE_PYTHON_PY_FILE=pipeline.py

# Run it
gcloud dataflow flex-template run my-job-$(date +%s) \
  --template-file-gcs-location gs://my-bucket/templates/my_job.json \
  --region us-central1 \
  --parameters input_topic=projects/p/topics/events
```

## Updating & Draining Streaming Jobs

Three ways to stop/replace a streaming job — know the difference:

| Action | In-flight data | Use |
|--------|----------------|-----|
| **Cancel** | Dropped | Emergencies |
| **Drain** | Windows closed & flushed, no new reads | Graceful shutdown |
| **Update** (`--update` + transform mapping) | State carried over to new job | Compatible code changes |

Update requires **compatibility**: same transform names (or an explicit `--transform_name_mapping`), compatible coders/state. Incompatible change ⇒ drain old job, start new one (accepting a results gap or replaying from the source).

## Common Pitfalls Checklist

- Creating clients/models inside `process()` instead of `setup()`.
- Assuming exactly-once covers external side effects — it doesn't; make sinks idempotent.
- ACCUMULATING mode into an additive sink ⇒ double counting.
- No `allowed_lateness`/late metrics ⇒ silent data loss you can't see.
- Hot keys: a single giant key serializes a GroupByKey; salt keys or use `Combine` (which pre-aggregates lifted on the mapper side) instead of raw GBK.
- Forgetting `--max_num_workers` ⇒ runaway autoscaling cost during a backlog spike.
- Logging per element at INFO ⇒ Cloud Logging bill rivaling the compute bill.
- Python-specific: heavyweight imports at module level get pickled/reimported badly — import inside `setup` when needed; prefer `--requirements_file`/custom containers for deps.

## Mini Practice

Sketch the windowing config for: "Per-product revenue every 5 minutes on a dashboard that upserts by (product, window), tolerate 2 hours of late mobile events, show a preview every 30 seconds."

```python
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(
        early=AfterProcessingTime(30),
        late=AfterCount(1),
    ),
    accumulation_mode=AccumulationMode.ACCUMULATING,  # sink upserts
    allowed_lateness=2 * 60 * 60,
)
```

Be ready to defend each line — that's exactly the shape of a mid-level Dataflow interview question.
