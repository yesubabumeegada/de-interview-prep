---
title: "Data Mesh Design - Scenario Questions"
topic: system-design
subtopic: data-mesh-design
content_type: scenario_question
tags: [system-design, data-mesh, scenarios, interview, data-products, domain-ownership]
---

# Scenario Questions — Data Mesh Design

<article data-difficulty="junior">

## 🟢 Junior: Identify What Makes a Good Data Product

**Scenario:** Your company is starting to talk about "data products." Your manager asks you to review three proposed data products and tell them which one is well-designed and which ones need work:

- **Option A:** A Redshift table `raw_orders` that's a direct copy of the production MySQL `orders` table, updated every night.
- **Option B:** A Delta table `fct_customer_orders` with documentation, an owner (the orders team), a defined schema, freshness SLA of 4 hours, and column-level descriptions. PII fields are masked for analytics consumers.
- **Option C:** An S3 folder `s3://data-lake/orders_data_2024/` containing CSV files uploaded manually by a developer each month, with no schema documentation.

<details>
<summary>✅ Solution</summary>

**Option B is the well-designed data product.** Options A and C are anti-patterns.

**Option A problems — raw copy ≠ data product:**
- No transformation or business logic applied — raw tables leak internal implementation details
- If the MySQL schema changes (column renamed, type changed), all consumers break
- No access control — everyone sees everything including PII
- No SLA — consumers don't know how fresh it is
- No documentation — what does `order_status = 7` mean?

**Option C problems — manual CSV ≠ data product:**
- Manual process = unreliable delivery (what if the developer is on vacation?)
- No schema enforcement — CSV column order might change silently
- No monitoring, no alerts on failure
- Monthly updates = stale for most use cases
- No ownership model — what happens when that developer leaves?

**Option B qualities — this is a data product:**
```
Discoverable   ✓ — documented with descriptions and owner
Addressable    ✓ — stable Delta table location with versioning
Trustworthy    ✓ — 4-hour freshness SLA with monitoring
Self-describing ✓ — column-level descriptions, schema registered
Interoperable  ✓ — Delta format, readable by Spark/Databricks/Athena
Secure         ✓ — PII masked, access controlled per consumer type
```

**What to tell your manager:**
- Option B: ship it — it's a proper data product
- Option A: needs transformation layer (staging → domain model), PII masking, and SLA definition
- Option C: needs to be automated (Airflow/dbt), schema formalized, and moved to a governed location

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Design a Data Product for the Orders Domain

**Scenario:** You're a DE on the Orders team at a 500-person e-commerce company. The central data team is overwhelmed and has a 6-week backlog. Your manager asks you to take ownership of the `order_events` data product that 5 downstream teams (Finance, Marketing, Data Science, Fraud, Customer Support) consume. Design the data product: schema, access tiers, SLA, freshness monitoring, and schema evolution strategy.

<details>
<summary>✅ Solution</summary>

**Step 1: Define the schema with access tiers**

```yaml
# order_events data product schema
columns:
  - name: order_id
    type: string
    access: all_consumers
    description: "Unique order identifier"

  - name: order_status
    type: string
    enum: [pending, processing, shipped, delivered, cancelled, refunded]
    access: all_consumers

  - name: total_amount_usd
    type: decimal(10,2)
    access: all_consumers

  - name: product_category
    type: string
    access: all_consumers

  - name: customer_id_hashed     # SHA-256 of customer_id
    type: string
    access: [marketing, data_science, customer_support]
    pii: false                   # hashed — not raw PII

  - name: customer_id            # raw, identifiable
    type: string
    access: [fraud_detection]    # approved for PII
    pii: true
    masking: hash_unless_approved

  - name: shipping_address
    type: string
    access: [customer_support, fraud_detection]  # operational need
    pii: true
    masking: redact_unless_approved
```

**Step 2: SLA definition**

```yaml
sla:
  freshness:
    batch: "< 4 hours"           # Airflow pipeline runs every 2 hours
    streaming: "< 60 seconds"    # Kafka CDC for fraud team
  completeness: "> 99.9%"        # order_id and order_status never null
  availability: "99.5%"          # table readable, not under maintenance
  schema_notice: "30 days"       # breaking changes announced 30 days ahead
```

**Step 3: Freshness monitoring**

```python
# dbt source freshness check (runs in CI and as scheduled Airflow task)
# sources.yml
sources:
  - name: orders
    tables:
      - name: order_events
        loaded_at_field: event_timestamp
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 4, period: hour}
```

```python
# Airflow alert on SLA breach
def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_slack(
        channel="#dp-orders-order-events",
        message=(
            f"🚨 SLA breach: order_events freshness > 4h\n"
            f"Blocking tasks: {blocking_task_list}\n"
            f"Last successful run: {get_last_run_time()}"
        )
    )
```

**Step 4: Schema evolution strategy**

```python
# Use Delta schema evolution with backward compatibility rule:
# ALLOWED: add new columns (consumers ignore unknown columns)
# ALLOWED: widen types (INT → BIGINT)
# NOT ALLOWED: rename columns (breaks all consumers)
# NOT ALLOWED: remove columns (breaks consumers using them)
# NOT ALLOWED: narrow types (BIGINT → INT)

# For breaking changes: version the data product
# v3 adds new column, v2 stays available for 30 days

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

df.write \
    .format("delta") \
    .option("mergeSchema", "true") \  # allows additive changes
    .mode("append") \
    .save("s3://orders-domain/order_events/v3/")

# Breaking change: publish v4 alongside v3
# Announce to consumers in #data-changes Slack
# Deprecate v3 after 30 days
```

**Step 5: Consumer onboarding**

```bash
# Any team that wants access opens a PR:
# consumers/order_events_consumers.yaml

# PR is reviewed by orders team lead → auto-provisions Unity Catalog grants
```

**Trade-offs to discuss:**
- Streaming vs batch: fraud needs < 60 seconds → Kafka CDC. Finance needs daily aggregates → batch is cheaper and more reliable. Both from the same source (Debezium CDC → Kafka → Flink for streaming, Spark batch for daily snapshot).
- PII tiers add complexity but are non-negotiable for GDPR compliance.
- Self-serve access via PR workflow adds a few hours of latency vs instant — acceptable for non-urgent access.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Data Mesh for a 20-Domain Enterprise

**Scenario:** You're hired as the Head of Data Engineering at a fintech company with 1,200 engineers across 20 product teams (Payments, Lending, KYC, Fraud, Cards, Rewards, Customer, Notifications, Risk, Compliance, and 10 more). The current state: 1 central data team of 25 engineers with a 3-month backlog, 200 data pipelines all owned by the central team, no documentation, no data catalog, PII everywhere, and 3 regulatory audits pending. You have 12 months and a team of 8 to transform this. Design the data mesh strategy.

<details>
<summary>✅ Solution</summary>

**Month 1-2: Assessment and foundation**

```
Audit current state:
  - Map all 200 pipelines to producing domain (which team's system is the source?)
  - Identify top 10 most-consumed datasets (quick wins for data products)
  - Find all PII flows (urgent — 3 audits pending)
  - Survey domain teams: do they have DEs? Do they want data ownership?

Findings example:
  - 60% of pipelines come from 4 domains: Payments, Customer, Lending, Cards
  - Top consumed: transactions, customer_profile, loan_status, card_events
  - PII in 47 tables, no masking, 12 teams have access they shouldn't
  - 8 of 20 domains already have embedded DEs (surprise resource!)
```

**Month 2-4: Platform foundation (your 8-person team builds this)**

```
Priority 1: PII Remediation (regulatory compliance — can't wait)
  - Unity Catalog column masking on all 47 PII tables
  - Audit log review → revoke inappropriate access
  - Timeline: 6 weeks

Priority 2: Self-serve scaffold
  - Data product template (Terraform + GitHub Actions)
  - Unity Catalog as governance layer
  - DataHub for catalog + lineage
  - Airflow templates for common pipeline patterns (CDC → Delta, S3 → dbt)
  - Timeline: 8 weeks (parallel to PII work)

Priority 3: Data product contracts
  - YAML spec for data products (schema, SLA, consumers, owner)
  - PR-based access workflow → auto-provisions Unity Catalog grants
  - Timeline: 4 weeks
```

**Month 4-8: Migrate to domain ownership (pilot with 4 domains)**

```
Selection criteria for pilot domains:
  1. Has embedded DEs (can own pipelines)
  2. High-value data (proves ROI quickly)
  3. Willing team lead (cultural fit)

Pilot: Payments, Customer, Lending, Cards

Migration pattern per domain:
  Week 1: Domain team shadows central team on their own pipelines
  Week 2: Domain team takes over with central team as backup
  Week 3: Domain team fully independent, central team on-call
  Week 4: Graduation — domain team owns data products, central team moves on

Central team offloads 4 domains × avg 15 pipelines = 60 pipelines
Remaining: 140 pipelines for 16 domains → queue drops 30%
```

**Month 8-12: Scale to all 20 domains**

```
Remaining 16 domains — phased by readiness:
  Batch 2 (months 8-9): Fraud, Risk, KYC (high regulatory priority)
  Batch 3 (months 9-10): Cards, Rewards, Notifications
  Batch 4 (months 10-11): Remaining 10 domains

Central team role transforms:
  Before: 25 engineers building pipelines
  After:  8 engineers running platform + governance

  Freed 17 engineers → 1-2 per domain as embedded DEs
  (This is the headcount argument that gets exec buy-in)
```

**Federated governance model:**

```yaml
# Global policies (enforced, no exceptions):
global:
  pii_masking: required          # column masking via Unity Catalog
  data_retention: 7_years_max   # legal requirement
  audit_logging: all_reads       # regulatory compliance
  schema_registration: required  # all products must register
  sla_definition: required       # freshness + availability

# Domain autonomy (domains decide):
domain:
  pipeline_technology: choice   # Airflow, dbt, Spark, Flink — their call
  internal_naming: free         # internal tables can use any convention
  storage_format: [delta, parquet, avro]  # must be in approved list
  quality_rules: minimum_plus   # can add rules beyond global minimums
  deprecation_schedule: 30_day_notice  # but can choose longer
```

**12-month success metrics:**

```
Metric                              Target        Measurement
Time-to-data-product                < 1 week      (was 3 months)
Central team queue length           < 10 items    (was 200+)
Data products in catalog            100+          (was 0 documented)
PII compliance violations           0             (was open)
Domain team satisfaction (NPS)      > 40          (quarterly survey)
Pipeline failure rate               < 1%          (was 8%)
```

**The exec pitch (30-second version):**
"We have 3 regulatory audits pending, a 3-month backlog causing business decisions to be made on stale data, and 25 engineers building pipes instead of strategic products. Data mesh gives each product team ownership of their own data. My 8-person platform team enables this. We free 17 engineers to join domain teams. In 12 months: audit risk eliminated, backlog gone, time-to-data drops from months to days. Cost-neutral — we're redistributing headcount, not adding it."

</details>
</article>
