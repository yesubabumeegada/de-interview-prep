---
title: "Dataflow / Apache Beam — Real-World Case Studies"
topic: gcp
subtopic: dataflow
content_type: study_material
layer: real-world
difficulty_level: mid-level
tags: [gcp, dataflow, interview]
---

# Dataflow / Apache Beam — Real-World Case Studies

Three production incidents/projects with the kind of detail interviewers probe for: what broke, how you found it, what you changed, and what it cost before and after.

## Case Study 1: The Streaming Job That Autoscaled to 90 Workers and Stayed There

### Context

Ad-tech company. Streaming pipeline: Pub/Sub (bid events, ~40k msg/s) → parse → enrich against a campaign API → window 1 min → write to BigQuery. `max_num_workers=100`, n2-standard-4.

### Symptom

After a product launch, the job scaled from ~12 workers to 90 and never came back down. Monthly Dataflow cost projection jumped from ~$4,500 to ~$33,000. Backlog stayed near zero — so why 90 workers?

### Investigation

- Worker CPU: only ~35%. Autoscaler held workers high because **stage throughput** was limited, not CPU.
- Dataflow UI stage metrics: the enrichment ParDo showed high "processing time per element" — ~45 ms each.
- Worker logs + code review: the DoFn created an HTTPS client **per element** and called the campaign API synchronously, one element at a time.

```python
# BEFORE (the bug)
class Enrich(beam.DoFn):
    def process(self, el):
        client = CampaignClient()          # TLS handshake per element!
        el["campaign"] = client.get(el["campaign_id"])
        yield el
```

### Fix

```python
# AFTER
class Enrich(beam.DoFn):
    def setup(self):
        self.client = CampaignClient(pool_size=8)   # reuse, keep-alive
        self.cache = TTLCache(maxsize=50_000, ttl=300)

    def start_bundle(self):
        self.batch = []

    def process(self, el):
        cid = el["campaign_id"]
        if cid in self.cache:
            el["campaign"] = self.cache[cid]
            yield el
        else:
            self.batch.append(el)
            if len(self.batch) >= 100:
                yield from self._flush()

    def finish_bundle(self):
        for wv in self._flush_windowed():
            yield wv

    def _flush(self):
        ids = {e["campaign_id"] for e in self.batch}
        results = self.client.get_many(ids)          # one RPC for 100 ids
        self.cache.update(results)
        for e in self.batch:
            e["campaign"] = results[e["campaign_id"]]
            yield e
        self.batch = []
```

Plus a 5-minute refreshing side input for the 2,000 most active campaigns.

### Outcome

| Metric | Before | After |
|--------|--------|-------|
| Workers (steady state) | 90 | 8 |
| Enrich latency/element | ~45 ms | ~0.4 ms amortized |
| Monthly cost | ~$33,000 projected | ~$3,100 |
| Campaign API QPS | 40,000 | ~300 |

Interview soundbite: "Autoscaling can't fix per-element I/O. Client in `setup()`, batch the RPCs, cache hot keys — the campaign API team also stopped paging."

## Case Study 2: Windows Stopped Closing — the Stuck Watermark Incident

### Context

IoT platform: Kafka (200 partitions) → Dataflow streaming → sessionized device metrics → BigQuery. Fixed 5-min windows, allowed lateness 30 min.

### Symptom

At 02:10, BigQuery freshness alerts fired: no new windowed aggregates for 25 minutes. The job was "running" green, workers healthy, CPU normal, no errors in logs. Raw passthrough records (un-windowed branch) still flowed.

### Investigation — the debugging story

1. Un-windowed branch healthy + windowed branch frozen ⇒ suspect **watermark**, not processing.
2. Dataflow UI: "data watermark" for the windowing stage frozen at 02:08.
3. Per-partition Kafka lag dashboard: 199 partitions current; **partition 113 had no new messages and a stalled consumer** — a broker had been replaced and one producer pinned to a stale metadata view kept failing to publish to 113's new leader.
4. Beam's Kafka watermark = min over partitions; one silent partition with unknown idle status held the global watermark, so **every** window in the pipeline stayed open. No data loss — just no results.

### Fix

- Immediate: bounced the broken producer; partition 113 resumed; watermark jumped forward; 25 minutes of windows fired in one burst (downstream upserts absorbed it — accumulating panes + MERGE by window key, which is why idempotent sinks matter).
- Permanent:
  1. Producer client upgraded (metadata refresh bug).
  2. Enabled idle-partition watermark advancement in the Kafka source config (treat partitions idle > 60 s as not holding the watermark):

```python
from apache_beam.io.kafka import ReadFromKafka

ReadFromKafka(
    consumer_config={"bootstrap.servers": brokers},
    topics=["device-metrics"],
    # advance watermark past idle partitions
    # (KafkaIO: withCreateTime + idle advancement in Java;
    #  in Python via expansion service params)
)
```

  3. Alert added on `data_watermark_age > 10 min` per stage — catching it in minutes, not via downstream freshness.

### Outcome

Time-to-detect for the same failure class went from 25 min (downstream alert) to 3 min (watermark age alert). Zero data lost in the incident.

Interview soundbite: "A streaming pipeline can be perfectly healthy and produce nothing — the watermark is a min over all source partitions, so one silent partition freezes every window. Alert on watermark age, not just throughput."

## Case Study 3: Migrating a 6-Hour Spark Batch to Dataflow FlexRS

### Context

Retail company ran a nightly Spark job on a long-lived Dataproc cluster (15 × n2-highmem-8, ~$5,800/month amortized) to build product recommendation features from ~2 TB of GCS Parquet. Cluster sat idle ~16 h/day. Team wanted lower cost and less cluster babysitting; deadline was "ready by 07:00," start anytime after 23:00.

### Approach

Rewrote the job in Beam Python (mostly mechanical: DataFrame ops → schema'd PCollections + `Combine`/`CoGroupByKey`), and ran it with **FlexRS** (flexible resource scheduling: delayed start, mix of preemptible workers, ~40% discount) since the 8-hour window had slack:

```bash
python build_features.py \
  --runner=DataflowRunner \
  --region=us-central1 \
  --flexrs_goal=COST_OPTIMIZED \
  --max_num_workers=60 \
  --machine_type=n2-standard-8 \
  --temp_location=gs://retail-tmp/df
```

### Two debugging detours worth telling

1. **Gzip trap**: a third of the input was `.json.gz` — unsplittable, so 60 workers sat idle while 20 ground through big files. Fix: a tiny upstream Dataflow job (and producer change) re-landing those as Snappy Parquet. Wall-clock dropped 70 minutes.
2. **Skewed CoGroupByKey**: `product_id = "UNKNOWN"` held 4% of all events; one worker ran 40 minutes longer than the rest. Fix: filtered the junk key into a separate branch and pre-aggregated it with `Combine` (no join needed for unknowns).

### Outcome

| Metric | Dataproc (before) | Dataflow FlexRS (after) |
|--------|-------------------|--------------------------|
| Compute cost / month | ~$5,800 (24/7 cluster) | ~$1,150 (per-job, discounted) |
| Wall-clock | 6 h 10 m | 2 h 35 m |
| Ops effort | Patching, resizing, init actions | None (per-job workers) |
| Failure recovery | Manual rerun | Rerun job; dynamic rebalancing handles stragglers |

Honest trade-offs they accepted (good interview material): lost the Spark ecosystem (a small MLlib step moved to BigQuery ML), FlexRS jobs can be queued up to ~6 hours before starting (fine for a 23:00–07:00 window, wrong for tight SLAs), and the team had to learn Beam idioms — `Combine` over naive GroupByKey was the big one.

## Patterns Across All Three Cases

1. **Throughput problems are usually inside the DoFn** (per-element clients, blocking I/O) or in the data shape (hot keys, unsplittable files) — rarely fixed by more workers.
2. **Watch the watermark like a vital sign** — alert on watermark age per stage; a green job can be a frozen job.
3. **Idempotent sinks turn incidents into non-events** — burst replays after recovery are safe when the sink upserts by key+window.
4. **Per-job ephemeral compute (and FlexRS for slack deadlines) beats idle clusters** on both cost and ops — the recurring argument for Dataflow over self-managed Spark on GCP.
