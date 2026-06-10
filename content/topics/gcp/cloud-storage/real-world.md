---
title: "Cloud Storage — Real-World Applications"
topic: gcp
subtopic: cloud-storage
content_type: study_material
difficulty_level: mid-level
tags: [gcp, cloud-storage, gcs, case-study, interview]
---

# Cloud Storage — Real-World Applications

Three production stories you can adapt when an interviewer asks "tell me about a time you worked with cloud storage."

## Case Study 1: The $40K/Month Bucket Nobody Audited

**Context:** An e-commerce company's GCS bill grew from $8K to $40K/month over a year. Nobody knew why — "storage is cheap, we just keep everything."

**Investigation:**

```bash
# Step 1: which buckets hold the bytes?
for b in $(gcloud storage buckets list --format="value(name)"); do
  echo "== $b"; gcloud storage du gs://$b --summarize --readable-sizes
done

# Step 2: enable Storage Insights inventory export to BigQuery
```

```sql
-- Step 3: age and class distribution of bytes
SELECT
  storage_class,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(time_created), DAY) > 365 THEN '>1y'
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(time_created), DAY) > 90  THEN '90d-1y'
    ELSE '<90d'
  END AS age_bucket,
  ROUND(SUM(size)/POW(10,12), 2) AS tb,
  COUNT(*) AS objects
FROM `ops.gcs_inventory`
GROUP BY 1, 2
ORDER BY tb DESC;
```

**Findings:**
1. 60% of bytes were **Standard-class objects older than a year**, never read (confirmed via data access logs) — mostly raw JSON event dumps.
2. A versioned config bucket had **2.1 million noncurrent versions** of the same 400 files — a CI job rewrote them every 5 minutes for 3 years.
3. A `tmp/` prefix held 180 TB of abandoned Spark scratch data.

**Fixes:**

```json
{
  "lifecycle": { "rule": [
    { "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"}, "condition": {"age": 30} },
    { "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"}, "condition": {"age": 120} },
    { "action": {"type": "Delete"}, "condition": {"numNewerVersions": 5} },
    { "action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["tmp/"]} }
  ]}
}
```

Plus: converted raw JSON to Parquet+ZSTD during a one-time backfill (3.8× size reduction on the hot zone).

**Result:** $40K → $14K/month. The lifecycle JSON took an afternoon; the inventory analysis was the real work.

**Interview takeaway:** quantify before optimizing — Storage Insights + BigQuery turns "storage is expensive" into three line items with owners.

## Case Study 2: Event-Driven Ingestion Replacing a Polling Cron

**Context:** A logistics company received ~3,000 CSV files/day from partners via SFTP-to-GCS bridge, landing in `gs://partner-landing/`. A cron job listed the bucket every 5 minutes and processed new files. Problems: 5-minute worst-case latency, duplicate processing after crashes, and list operations grew with bucket size.

**Redesign — Pub/Sub notifications + idempotent consumer:**

```bash
gcloud storage buckets notifications create gs://partner-landing \
  --topic=partner-files --event-types=OBJECT_FINALIZE \
  --payload-format=json
```

```python
# Cloud Run service triggered by Pub/Sub push
import json, base64
from google.cloud import bigquery, storage

bq = bigquery.Client()

def handle(event):
    msg = json.loads(base64.b64decode(event["message"]["data"]))
    bucket, name, generation = msg["bucket"], msg["name"], msg["generation"]

    # Idempotency: object generation is unique per upload
    file_id = f"{bucket}/{name}#{generation}"
    if already_processed(file_id):          # check in a small Firestore/BQ table
        return "duplicate", 200

    job = bq.load_table_from_uri(
        f"gs://{bucket}/{name}",
        "ingest.partner_shipments_raw",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
        ),
    )
    job.result()
    mark_processed(file_id)
    return "ok", 200
```

**Key decisions:**
- **`OBJECT_FINALIZE` + generation number** as the idempotency key — re-uploads of the same filename get a new generation, retried Pub/Sub deliveries reuse the same one. Exactly the dedup semantics needed.
- Dead-letter topic on the subscription: malformed files don't block the queue; they land in a DLQ reviewed daily.
- Files move to `processed/` prefix via lifecycle-managed copy? No — they're left in place with a 30-day delete rule; state lives in the processing log, not the object layout.

**Result:** end-to-end latency 5 min → ~8 seconds; duplicate loads eliminated; list-operation costs dropped to zero.

## Case Study 3: Cross-Region Egress Surprise on a Dataproc Migration

**Context:** A team migrated Spark workloads from on-prem to Dataproc in `europe-west1`... but the historical data had been bulk-loaded months earlier into a **multi-region `US` bucket**. Jobs worked. The first full month's bill showed **$11K of network egress**.

**Diagnosis:**

```bash
# Billing export -> BigQuery
SELECT service.description, sku.description, ROUND(SUM(cost),2) AS cost
FROM `billing.gcp_billing_export_v1`
WHERE invoice.month = '202405'
GROUP BY 1,2 ORDER BY cost DESC LIMIT 10;
-- "Network Data Transfer GCP Inter Region ..." topped the list
```

Every Spark read pulled data from US storage to EU compute.

**Fix — Storage Transfer Service, then repoint:**

```bash
gcloud transfer jobs create gs://us-lake-events gs://eu-lake-events \
  --source-creds-file=... \
  --overwrite-when=different

# Validate object counts + bytes, then flip the Spark configs/catalog
# to gs://eu-lake-events and delete the US copy after a 30-day safety window.
```

**Hard-won lessons (great interview material):**
1. **Egress is invisible until the invoice.** Always check bucket location vs compute region during migration planning — it's one `gcloud storage buckets describe` call.
2. The transfer itself cost one final egress payment (~$2K) — pitch it as "one month's bleed to stop a permanent leak."
3. They added a CI check: Terraform module refuses to create a Dataproc cluster whose default bucket region differs from the cluster region.

## Patterns Worth Quoting in Interviews

| Pattern | One-liner |
|---|---|
| Inventory before optimizing | "Storage Insights → BigQuery told us 60% of Standard bytes hadn't been read in a year." |
| Generation-based idempotency | "GCS object generation is a free, unique upload ID — perfect dedup key for event-driven ingestion." |
| Notifications over polling | "OBJECT_FINALIZE → Pub/Sub cut latency from minutes to seconds and removed list costs entirely." |
| Co-locate storage and compute | "Bucket region must match cluster region — we enforce it in Terraform now." |
| Raw zone as backup | "Curated tables are rebuildable from immutable raw; that's our real DR story." |
