---
title: "BigQuery Advanced — Senior Deep Dive"
topic: gcp
subtopic: bigquery-advanced
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [gcp, bigquery, slots, reservations, optimization, interview]
---

# BigQuery Advanced — Senior Deep Dive

At the senior level, you architect the BigQuery estate for an organization: designing the slot reservation strategy, building the cost chargeback model, hardening the security posture, and knowing which query anti-patterns silently destroy performance at scale. This layer covers the internals and architecture decisions interviewers probe at staff/senior engineer rounds.

---

## Slot Reservation Architecture

BigQuery's capacity model (Standard/Enterprise/Enterprise Plus editions) replaces the legacy flat-rate pricing. Understanding the hierarchy is essential for multi-team cost isolation.

### Reservation Hierarchy

```
Organization
└─ Admin Project (holds the reservation resource)
    └─ RESERVATION: "prod" (1000 slots, Enterprise, 1-year)
        ├─ ASSIGNMENT: project "data-warehouse" (job_type=QUERY)
        ├─ ASSIGNMENT: project "ml-team" (job_type=QUERY)
        └─ ASSIGNMENT: project "etl-jobs" (job_type=BATCH)
    └─ RESERVATION: "dev" (200 slots, Standard, flex)
        └─ ASSIGNMENT: project "data-warehouse-dev"
```

**Key behaviors:**
- Slots not used by one assignment are **shared** with other assignments in the same reservation (idle slot sharing within reservation).
- Baseline slots are guaranteed. Autoscaling (in Enterprise+) can burst beyond baseline.
- `job_type=BATCH` jobs run at lower priority and cost less — use for non-SLA-critical loads.

```bash
# Create a reservation
bq mk --project_id=admin-project \
  --location=US \
  --slots=1000 \
  --edition=ENTERPRISE \
  --commitment_plan=ANNUAL \
  reservation prod-reservation

# Assign a project to the reservation
bq mk --project_id=admin-project \
  --location=US \
  --reservation_id=prod-reservation \
  --assignee_id=data-warehouse \
  --assignee_type=PROJECT \
  assignment
```

### Autoscaler Behavior

BigQuery's autoscaler monitors queue depth and slot utilization. When demand exceeds baseline, it adds slots (up to `max_slots`). The autoscaler has a ~1-minute lag — this is relevant for bursty workloads where the first query in a burst may be slow.

**Interview question**: "How do you handle a workload where 50 analysts all run dashboards at 9am Monday?" → Answer: pre-warm with a dummy query at 8:55am, use BI Engine for dashboard reads, and consider reservation bursting + slot autoscaling with a high enough `max_slots` ceiling.

---

## Query Execution Internals: Dremel and Capacitor

BigQuery's execution engine (Dremel) uses a **tree of servers** — a root server that dispatches work to intermediate mixers, which dispatch to leaf servers that actually read data from Colossus (Google's distributed file system). Understanding this helps explain performance characteristics:

- **Columnar storage (Capacitor format)**: columns are stored separately with aggressive compression and run-length encoding. This is why `SELECT *` is so expensive — you deserialize every column.
- **Shuffle**: joins and GROUP BY require shuffling data between leaf servers. Large shuffles are the #1 cause of slow complex queries.
- **Slot milliseconds**: each slot running for 1 ms = 1 slot-ms. A query using 100 slots for 10 seconds = 1,000,000 slot-ms. This metric in INFORMATION_SCHEMA tells you true compute cost.

```sql
-- Analyze slot usage for recent queries
SELECT
  job_id,
  total_slot_ms,
  total_bytes_processed,
  -- slot efficiency: bytes per slot-second
  SAFE_DIVIDE(total_bytes_processed, total_slot_ms / 1000) AS bytes_per_slot_second,
  ARRAY_LENGTH(referenced_tables) AS tables_referenced
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND statement_type = 'SELECT'
ORDER BY total_slot_ms DESC
LIMIT 20;
```

---

## Join Optimization Strategies

### Broadcast vs. Shuffle Joins

BigQuery automatically chooses join strategy:
- **Broadcast join** (map-side join): one table is small enough to be replicated to all workers. Fast, no shuffle.
- **Shuffle join**: both tables are large, data is redistributed by join key. Expensive, but necessary.

You cannot force join type in standard SQL, but you can hint it via query annotations or restructure the query.

**Common anti-pattern**: joining two large tables on a non-integer key (like a UUID string) causes expensive string hashing in the shuffle. Integer keys are faster.

### Skew Handling

Data skew occurs when one join key has disproportionately many rows (e.g., a `NULL` key, or one customer with 90% of orders). Signs: one worker runs for 10x longer than others; query hangs at 99% completion.

```sql
-- Detect skew: check key distribution before joining
SELECT
  customer_id,
  COUNT(*) AS row_count
FROM `project.dataset.orders`
GROUP BY customer_id
ORDER BY row_count DESC
LIMIT 20;

-- Mitigation 1: exclude nulls
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.customer_id IS NOT NULL;

-- Mitigation 2: salt the key for extremely skewed distributions
-- (Split the hot key into N sub-keys, join in two phases)
```

---

## Advanced Materialized View Patterns

### Cross-Dataset Materialized Views

Materialized views can reference base tables in different datasets (even different projects in the same region). This enables centralized pre-aggregation for multi-team consumption.

```sql
-- MV in analytics dataset referencing raw events in data_lake dataset
CREATE MATERIALIZED VIEW `analytics.daily_active_users`
PARTITION BY event_date
CLUSTER BY app_version
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 120,
  allow_non_incremental_definition = FALSE  -- fail if BQ can't do incremental
)
AS
SELECT
  DATE(event_timestamp) AS event_date,
  app_version,
  COUNT(DISTINCT user_id) AS dau
FROM `data_lake.raw_events`
WHERE event_type = 'app_open'
GROUP BY 1, 2;
```

**Incremental refresh limitations**: BQ can only do incremental refresh if the MV query meets specific criteria — no LIMIT, no window functions (OVER), no non-deterministic functions (RAND, CURRENT_TIMESTAMP in certain positions), no UNION ALL mixing aggregated and non-aggregated. The `allow_non_incremental_definition = FALSE` flag makes violations fail fast rather than silently falling back to full refresh.

---

## Terraform for BigQuery Infrastructure

Production BigQuery estates are managed as code:

```hcl
resource "google_bigquery_dataset" "warehouse" {
  dataset_id  = "data_warehouse"
  location    = "US"
  description = "Production data warehouse"

  access {
    role          = "OWNER"
    user_by_email = "data-platform@project.iam.gserviceaccount.com"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  default_partition_expiration_ms = 7776000000  # 90 days
}

resource "google_bigquery_table" "orders" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "orders"

  time_partitioning {
    type  = "DAY"
    field = "order_date"
  }

  clustering = ["customer_id", "status"]

  schema = file("schemas/orders.json")

  deletion_protection = true
}

resource "google_bigquery_reservation" "prod" {
  name     = "prod-reservation"
  location = "US"
  slot_capacity = 1000
  edition  = "ENTERPRISE"
}
```

---

## Cost Attribution and Chargeback

In multi-team environments, you need to attribute BigQuery costs per team. The canonical approach:

1. **Labels on queries**: teams add `labels = {"team": "marketing"}` to their job configs. INFORMATION_SCHEMA.JOBS exposes these labels.
2. **Separate projects per team**: each team's project has its own billing account or budget alert.
3. **Billing export to BigQuery**: export Cloud Billing data to a BigQuery dataset, join with INFORMATION_SCHEMA.JOBS for query-level cost breakdown.

```sql
-- Cost chargeback: join billing export with job metadata
SELECT
  j.labels['team'] AS team,
  DATE(j.creation_time) AS date,
  SUM(j.total_bytes_processed) / POW(1024, 4) * 5 AS estimated_cost_usd
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
WHERE j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1, 2
ORDER BY date DESC, estimated_cost_usd DESC;
```

---

## Security Architecture Summary

A complete BigQuery security design for a senior interview involves:

| Layer | Mechanism | Scope |
|---|---|---|
| Dataset access | IAM roles (dataViewer, dataEditor) | All tables in dataset |
| Table access | Table-level IAM bindings | Specific table |
| Column access | Policy Tags + Fine-Grained Reader | Specific columns |
| Row access | Row Access Policies (RLS) | Filtered rows per user/group |
| Network | VPC Service Controls | Prevent data exfiltration |
| Audit | Data Access audit logs | Who accessed what, when |

The key interview insight: these layers are **additive and independent**. A user must satisfy ALL applicable restrictions — dataset IAM AND column policy tags AND row access policies. The most restrictive combination wins.
