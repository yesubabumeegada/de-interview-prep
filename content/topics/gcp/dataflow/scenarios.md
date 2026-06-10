---
title: "Dataflow / Apache Beam — Interview Scenarios"
topic: gcp
subtopic: dataflow
content_type: scenario_question
tags: [gcp, dataflow, interview]
---

# Dataflow / Apache Beam — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Counts Per Minute From an Endless Stream

**Scenario:** Your team wants a count of orders per minute from a Pub/Sub topic, written to BigQuery. A teammate tried `beam.combiners.Count.PerKey()` directly on the stream and the pipeline failed with an error about applying a GroupByKey to an unbounded PCollection. Explain why this happens and write the corrected pipeline sketch.

<details>
<summary>💡 Hint</summary>

Think about what it means to "finish counting" a stream that never ends — when would the aggregation ever be able to emit a result? Consider what Beam construct chops an unbounded stream into finite pieces that can each be completed and emitted.

</details>

<details>
<summary>✅ Solution</summary>

**Why it fails:** `Count.PerKey` requires grouping all elements per key — but an unbounded PCollection never ends, so the grouping could never complete. Beam requires you to apply **windowing** (non-global windows, or a trigger on the global window) before any GroupByKey/Combine on an unbounded source.

**Corrected pipeline:**

```python
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

options = PipelineOptions(streaming=True)

with beam.Pipeline(options=options) as p:
    (
        p
        | "Read" >> beam.io.ReadFromPubSub(
              topic="projects/my-proj/topics/orders")
        | "Parse" >> beam.Map(parse_order)            # (product_id, 1)
        | "Window" >> beam.WindowInto(window.FixedWindows(60))
        | "Count" >> beam.combiners.Count.PerKey()
        | "ToRow" >> beam.Map(lambda kv: {
              "product_id": kv[0],
              "order_count": kv[1],
          })
        | "Write" >> beam.io.WriteToBigQuery(
              "my-proj:ds.orders_per_min",
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
```

Key points to say out loud:

- Fixed 60-second windows make each count finite; a window emits when the **watermark** passes its end.
- Windows are based on **event time** (Pub/Sub publish time by default, or a timestamp you extract), so out-of-order arrival still counts correctly.
- The same code with a bounded source would just run as batch — that's the unified model.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: One Bad Message Stalled the Whole Pipeline

**Scenario:** A streaming Dataflow job (Pub/Sub → transform → BigQuery) has stopped making progress. The Pub/Sub backlog is growing, and worker logs show the same JSON parse exception repeating every few seconds from the same element. Why does one malformed message stall a streaming pipeline, and how do you both fix the immediate incident and redesign so it never happens again?

<details>
<summary>💡 Hint</summary>

Recall what Dataflow does with a *bundle* whose processing throws an exception in streaming mode — how many times will it retry, and what happens to the messages sharing that bundle? For the redesign, think about catching failures inside the DoFn and routing them somewhere instead of throwing.

</details>

<details>
<summary>✅ Solution</summary>

**Why it stalls:** Streaming Dataflow retries a failing **bundle indefinitely**. The poison message fails, the bundle is retried, fails again, forever — and messages behind it in that key/bundle path back up. (Batch jobs fail after 4 bundle attempts; streaming never gives up.)

**Immediate mitigation:** ship a hotfix that catches the exception (or update the job with a guarded parser via `--update` if graph-compatible; otherwise drain and redeploy).

**Permanent design — dead-letter pattern with tagged outputs:**

```python
class SafeParse(beam.DoFn):
    OK, DEAD = "ok", "dead"

    def process(self, raw: bytes):
        try:
            yield beam.pvalue.TaggedOutput(self.OK, json.loads(raw))
        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.DEAD, {
                "raw": raw.decode("utf-8", errors="replace"),
                "error": str(e),
                "ts": datetime.utcnow().isoformat(),
            })

parsed = (
    p
    | beam.io.ReadFromPubSub(subscription=sub)
    | beam.ParDo(SafeParse()).with_outputs(SafeParse.DEAD, main=SafeParse.OK)
)

parsed[SafeParse.OK] | "Process" >> beam.ParDo(Transform()) | sink
parsed[SafeParse.DEAD] | "DLQ" >> beam.io.WriteToBigQuery(
    "proj:ops.dead_letters", ...)
```

Complete the answer with operational hardening:

| Layer | Measure |
|-------|---------|
| Metrics | `Metrics.counter("dlq", "parse_failures")` + alert on rate |
| BigQuery sink | Use `insert_retry_strategy` / failed-rows output to DLQ too |
| Pub/Sub | Note: Pub/Sub dead-letter topics also exist, but they act on *delivery* failures (nacks); inside Dataflow you DLQ at the *processing* level |
| Replay | DLQ rows are queryable — fix parser, backfill from the DLQ table |

Senior-flavored closing line: "Every ParDo that can throw on bad data gets a tagged dead-letter output — in streaming, an unhandled exception isn't a crash, it's an infinite retry loop."

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Exactly-Once Revenue Aggregation, Then Defend It vs Spark

**Scenario:** Design a Dataflow pipeline computing per-merchant revenue in 5-minute event-time windows from Pub/Sub payment events (~25k msg/s), with: results in BigQuery within ~30 s of window close, mobile events up to 6 hours late must eventually correct the numbers, and finance requires no double counting even across job restarts and replays. After your design, the interviewer pushes: "Why not Spark Structured Streaming on Dataproc?" Give the architecture and the comparison.

<details>
<summary>💡 Hint</summary>

You'll need to combine: dedup of producer retries (what ID does Dataflow dedupe on?), trigger/accumulation choices that let late corrections overwrite earlier results, and a sink write mode where re-emitting a window's result is safe. For the Spark question, focus on ops model, event-time/lateness ergonomics, and exactly-once boundaries rather than raw performance claims.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```text
Producers --(payment_id attr)--> Pub/Sub topic
    --> Dataflow streaming (Streaming Engine, exactly-once mode)
          1. ReadFromPubSub(id_label="payment_id")    # dedup producer retries
          2. SafeParse -> DLQ (tagged outputs)
          3. WindowInto(FixedWindows(300),
                trigger=AfterWatermark(late=AfterProcessingTime(60)),
                accumulation_mode=ACCUMULATING,
                allowed_lateness=6*3600)
          4. CombinePerKey(sum) keyed by merchant_id
          5. Write via BigQuery Storage Write API
    --> BigQuery table keyed (merchant_id, window_start)
          consumed through a view / MERGE into serving table
```

**Why each piece:**

1. **Dedup at ingestion**: `id_label="payment_id"` makes Dataflow dedupe Pub/Sub redeliveries and producer retries by business ID — covers at-least-once delivery from the source.
2. **ACCUMULATING + late firings**: each late firing re-emits the *full corrected sum* for (merchant, window). Downstream we **upsert** by `(merchant_id, window_start)` — re-emission is idempotent, so no double counting even on replay. (DISCARDING + additive sink is the fragile alternative — any replay double counts.)
3. **allowed_lateness=6h** keeps window state alive for corrections; pair with a counter for data arriving even later (dropped) so finance knows the residual error rate.
4. **Sink exactly-once**: Storage Write API; final serving via scheduled `MERGE` keyed on (merchant, window) — the upsert is the last line of defense and also covers job drain/replace gaps.
5. **Restart story**: compatible changes via `--update` (state carried); incompatible ⇒ drain, redeploy, and rely on Pub/Sub replay (seek to timestamp) + idempotent MERGE to heal overlap.

**Latency check:** watermark-driven on-time firing + Write API gives seconds-level delivery; 30 s post-window-close is comfortable at 25k msg/s with Streaming Engine autoscaling.

**"Why not Spark on Dataproc?" — the defensible comparison:**

| Concern | Dataflow | Spark SS on Dataproc |
|---------|----------|----------------------|
| Ops | Per-job serverless workers, built-in autoscaling | You run/patch/size the cluster; autoscaling is coarser |
| Event time & lateness | Triggers, allowed lateness, per-pane accumulation are first-class | Watermarking exists, but no per-window late re-firing semantics this rich (micro-batch; `withWatermark` drops late data past threshold) |
| Exactly-once | Bundle-commit dedup + Pub/Sub ID dedup + Write API offsets | Achievable, but you own checkpoint mgmt + idempotent/transactional sink wiring; Pub/Sub connector maturity is weaker than Kafka's |
| Latency | Sub-second to seconds | Micro-batch: seconds — fine here, honestly |
| When Spark wins | — | Team already Spark-expert, heavy ML/library reuse, multi-cloud portability, Kafka-centric stack |

Close with: "For this requirement set — late-data corrections plus financial exactly-once on GCP with Pub/Sub — Dataflow's semantics map one-to-one. I'd pick Spark if we were Kafka-based, multi-cloud, or consolidating onto an existing Spark platform; correctness is achievable there too, just with more glue I'd have to own."

</details>

</article>

## Interview Tips

> **Tip 1:** "Explain windowing, watermarks, and triggers" is the canonical Dataflow question — structure it as the four What/Where/When/How questions of the Beam model, then give one concrete config (fixed window + AfterWatermark with early/late). Structure beats trivia.

> **Tip 2:** When asked about exactly-once, draw the boundary explicitly: exactly-once for state/shuffle inside the pipeline, at-least-once for external side effects unless the sink coordinates (Storage Write API, idempotent upserts). Interviewers use this to separate people who've run streaming in prod from those who haven't.

> **Tip 3:** For "Dataflow vs Spark," never answer with a winner — answer with a decision rule (ops ownership, event-time semantics depth, existing ecosystem, cloud portability). Conceding where Spark wins makes the rest of your answer credible.

## ⚡ Quick-fire Q&A

**Q:** PCollection in one sentence?
A: An immutable, distributed, possibly unbounded dataset whose elements each carry a timestamp and window.

**Q:** What breaks fusion between ParDos?
A: A shuffle boundary — GroupByKey/Combine or an explicit `Reshuffle`.

**Q:** What happens to data later than `allowed_lateness`?
A: Silently dropped — so instrument a metric counter on late panes.

**Q:** Drain vs cancel?
A: Drain stops reading, closes windows and flushes results; cancel kills the job and drops in-flight data.

**Q:** Why can adding workers fail to increase streaming throughput?
A: Per-key processing is serialized — hot keys or low key cardinality cap parallelism regardless of worker count.

**Q:** Accumulating vs discarding panes?
A: Accumulating re-emits the full updated result per firing (pair with upsert sinks); discarding emits only deltas (pair with additive sinks).

**Q:** What does Streaming Engine change?
A: Moves shuffle/state off worker VMs into a managed backend — smaller workers, faster autoscaling, easier updates.

**Q:** How do batch and streaming differ in bundle retries?
A: Batch fails the job after 4 failed attempts of a bundle; streaming retries the bundle forever — hence dead-letter outputs are mandatory.
