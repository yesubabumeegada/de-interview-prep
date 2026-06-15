---
title: "Data Modeling in the Real World — Case Studies and Interview Presentation"
description: "Real-world case studies of poor data modeling choices, whiteboard interview techniques, stakeholder questions, normalization trade-offs, and OLTP-to-dimensional migration"
content_type: study_material
topic: data-modeling
subtopic: interview-scenarios
layer: real-world
difficulty_level: senior
tags: [data-modeling, case-studies, whiteboard-interview, normalization, denormalization, OLTP-migration, stakeholder-questions, trade-offs, interview]
---

# Data Modeling in the Real World

Senior interviews increasingly test whether you can connect academic modeling principles to real engineering consequences. This guide covers what happens when schemas go wrong, how to present a schema in an interview setting, and how to navigate the practical politics and trade-offs of production data modeling.

---

## 1. Real Case Studies of Poor Data Modeling Choices

### Case Study 1: The Everything-in-JSON Trap

**What happened:** A fast-moving SaaS startup stored all user event attributes in a single JSONB column in PostgreSQL. Initially convenient — no migrations needed for new event types.

```sql
-- Original "flexible" schema
CREATE TABLE user_events (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT,
    event_type  VARCHAR(100),
    occurred_at TIMESTAMP,
    properties  JSONB    -- "just put everything here"
);
```

Two years later, the table had 40 billion rows. Common queries like `SELECT properties->>'campaign_id'` were scanning 40B rows because JSON keys are not indexable the same way as columns. Analysts couldn't use standard BI tools because the schema was unpredictable. New engineers had no documentation on what keys existed.

**Consequences:**
- Dashboard queries took 45+ minutes
- Data engineers spent 30% of their time answering "what's in the JSON?" questions
- Migrating to structured columns required a full historical reprocess of 40B rows

**Lessons:**
- Use JSON for genuinely variable data (feature flags, third-party payloads). For well-understood analytics events, use typed columns.
- Semi-structured JSON plus materialized views is a compromise: store raw, materialize common fields as columns.
- Establish a schema registry early. Every event type should have a defined schema before it goes to production.

---

### Case Study 2: The Missing Grain Declaration

**What happened:** A retail analytics team built a `fact_sales` table without documenting the grain. Some engineers loaded data at the order level; others loaded at the line-item level. Both pipelines wrote to the same table.

```sql
-- Records at different grains mixed in the same table
fact_sales:
| sale_id | customer_id | product_id | amount |
| 1001    | 500         | NULL       | 250.00 |  ← order-level row (no product)
| 1002    | 500         | 789        | 150.00 |  ← line-item row
| 1002    | 500         | 790        | 100.00 |  ← line-item row (same order!)
```

`SUM(amount) WHERE customer_id = 500` returned `500.00` — double-counting the order total.

**Consequences:**
- Revenue figures were inflated by 15–40% depending on the query
- CFO presented incorrect YTD numbers to the board
- Four weeks of engineer time spent diagnosing and remediating

**Lessons:**
- Declare the grain in the table's comment/documentation before the first row is loaded
- Use dbt or another testing layer to assert grain uniqueness: `unique` + `not_null` tests on the grain key
- Separate pipelines writing to the same fact table need governance and a single owner

---

### Case Study 3: Natural Keys in the Data Warehouse

**What happened:** An e-commerce company used `customer_email` as the primary key in `dim_customer` and as the foreign key in `fact_orders`. Customers could update their email address.

When a customer changed from `alice@gmail.com` to `alice@company.com`, the team ran `UPDATE dim_customer SET email = 'alice@company.com'`. But `fact_orders` still referenced the old email. Join integrity broke. Some orders were orphaned.

**Consequences:**
- 8% of orders had no matching customer dimension row — dropped from all customer-level reports
- Customer lifetime value calculations were silently understated
- Required a multi-hour backfill and production freeze

**Lessons:**
- Always use surrogate keys in the warehouse. Natural keys belong as attributes in the dimension, not as join keys.
- If you inherit a system using natural keys, write a reconciliation query (`fact LEFT JOIN dim ON natural_key`) to detect orphan facts before they cause reporting errors.

---

### Case Study 4: The Unpartitioned Fact Table

**What happened:** A media company's fact table for video views grew to 8 trillion rows over 5 years. No partitioning was applied. A standard weekly engagement report scanned 8 trillion rows even though it only needed the last 7 days.

**Consequences:**
- Weekly report that should take 30 seconds took 22 minutes
- BigQuery costs exceeded $40,000/month on this one report
- Analysts stopped running exploratory queries because they feared the cost

**Lessons:**
- Partition by event date from day one. Retrofitting partitioning on a multi-trillion-row table is expensive and operationally risky.
- Use query cost estimation (`EXPLAIN`, BigQuery dry-run, Snowflake query profile) before running against large fact tables in production.
- Apply table expiration policies for raw event data older than a retention window.

---

### Case Study 5: Over-Normalized Dimensions Killing Analytics Performance

**What happened:** An Inmon-style enterprise DW team built a fully normalized schema. To answer "How much did we sell by product category?" required joining 11 tables. A simple executive dashboard ran a query that generated a 14-table join plan.

```sql
-- Simplified version of what was actually happening
SELECT pc.category_name, SUM(f.revenue)
FROM fact_sales f
JOIN product_sku ps ON f.sku_key = ps.sku_key
JOIN product p ON ps.product_id = p.product_id
JOIN product_subcategory psc ON p.subcategory_id = psc.subcategory_id
JOIN product_category pc ON psc.category_id = pc.category_id
GROUP BY 1;
```

**Consequences:**
- Dashboard load time: 8 minutes
- BI tool connection timeouts for ad-hoc queries
- Analysts built local Excel copies of data to avoid the slow DW — creating shadow data silos

**Lessons:**
- Analytical layers should be denormalized for query performance. Keep the normalized EDW as the integration layer; build star schema marts on top for consumption.
- Monitor query plans regularly. A join count above 4–5 on a BI dashboard query is a modeling red flag.

---

## 2. How to Present a Schema Design in a Whiteboard Interview

The whiteboard portion of a data modeling interview is as much a communication exercise as a technical one.

### The Framework: Think Aloud, Then Draw

**Step 1: Restate the prompt** (30 seconds)
Paraphrase the problem to confirm alignment. "So we're building a schema that supports reporting on customer orders, product inventory, and maybe marketing campaign attribution — does that sound right?"

**Step 2: Ask 2–3 clarifying questions** (1–2 minutes)
Do not skip this. Interviewers award points for asking the right questions:
- "What's the primary analytical question — is it revenue by product, or customer retention, or something else?"
- "Are there any regulatory requirements — do we need to retain changes to customer attributes?"
- "What's the rough data volume we're designing for?"

**Step 3: State the grain out loud before drawing** (30 seconds)
"The grain of my fact table will be one row per order line item — each product on each order gets its own row."

**Step 4: Draw fact table first, then dimensions** (3–5 minutes)
Start with the center of the star. Label each column with its type (FK, measure, degenerate dim). Then draw dimension boxes around it with connecting lines.

```
┌─────────────────────────────────────────┐
│             fact_order_line             │
│  - order_line_key (PK, bigint)          │
│  - date_key (FK → dim_date)             │
│  - customer_key (FK → dim_customer)     │
│  - product_key (FK → dim_product)       │
│  - order_number (degenerate dim)        │
│  - quantity (int)                       │
│  - unit_price_usd (decimal)             │
│  - discount_usd (decimal)               │
│  - net_revenue_usd (decimal)            │
└─────────────────────────────────────────┘
```

**Step 5: Annotate decisions as you draw**
Say things like:
- "I'm using a surrogate key here, not the natural order_id, to decouple us from source system changes."
- "I'm making order_number a degenerate dimension because there's nothing to say about the order number beyond its value."
- "Customer attributes like segment and city would be SCD2 because we want to analyze historical behavior by the segment the customer was in at the time."

**Step 6: Address trade-offs proactively**
Before the interviewer asks, say: "One trade-off here is that storing unit_price in the fact table means if the product's price is corrected retroactively, historical facts won't be updated — is that acceptable for this use case, or do we need price audit trails?"

### What Interviewers Are Evaluating

| Behavior | What It Signals |
|---|---|
| Asks clarifying questions before drawing | Requirements-driven thinking |
| States grain explicitly | Understands the most critical modeling decision |
| Uses surrogate keys | Understands warehouse independence from source systems |
| Mentions SCD types appropriately | Knows how to handle changing dimensions |
| Identifies role-playing dimensions | Practical experience with real schemas |
| Discusses trade-offs | Senior-level architectural judgment |
| Checks back with interviewer | Collaborative working style |

---

## 3. Stakeholder Questions to Ask Before Designing

The best data models are designed collaboratively with the people who will query them. These are the questions to ask:

### Question Set 1: Query Patterns

- "What are the top 5 questions you need this model to answer?"
- "Do you need this at a daily, hourly, or real-time granularity?"
- "Will analysts write SQL directly, or are they using a BI tool that generates SQL?"

Why it matters: A BI tool like Tableau generates many small queries with `IN` filters. A data science team writes long window functions. These have very different physical design implications.

### Question Set 2: Business Definitions

- "How do you define a 'customer'? Someone who placed any order, or someone who completed checkout?"
- "What counts as 'revenue'? Gross before discounts? Net after returns?"
- "Is a cancelled order a sale that failed, or should it never appear in the sales fact?"

Why it matters: Undefined business terms lead to models that different teams interpret differently, producing conflicting numbers.

### Question Set 3: Data Retention and History

- "Do you need to analyze customer behavior before and after a plan upgrade?"
- "If a product's category changes, should historical sales reflect the old or new category?"
- "How far back does reporting need to go — 1 year, 5 years, forever?"

Why it matters: Determines whether SCD2 is needed, and for how long dimension history needs to be retained.

### Question Set 4: Volume and SLA

- "How many transactions per day? What's the peak?"
- "What's the acceptable query latency for dashboards — 5 seconds or 30 seconds?"
- "Are there scheduled reports that must complete by a specific time?"

Why it matters: Dictates partitioning strategy, whether pre-aggregations are needed, and whether a materialized view layer is warranted.

---

## 4. Trade-off Discussion: Normalization vs Denormalization at Scale

This is the most common architectural debate in data engineering. Here is a principled framework for the interview:

### When to Normalize

- **Source of truth layer:** Raw integration layer should be normalized to avoid update anomalies.
- **Data that changes frequently:** If product attributes change weekly, a normalized lookup table is easier to maintain than updating millions of fact rows.
- **Storage is genuinely constrained:** Less common on modern cloud, but relevant for on-prem or cost-sensitive environments.
- **Multiple fact tables share dimension data:** Conformed dimensions in a star schema are already a form of normalization.

### When to Denormalize

- **Query performance is the primary concern:** Every join adds latency. At petabyte scale, removing joins from the query path can mean seconds vs minutes.
- **BI tools with limited SQL capability:** Some embedded analytics platforms can't handle multi-table joins. OBT is necessary.
- **Known, stable query patterns:** If 90% of queries filter and group by the same 5 attributes, materializing those into the fact table eliminates repeated join cost.
- **Columnar storage neutralizes redundancy cost:** In BigQuery or Snowflake, storing `product_category` in 5 billion fact rows costs pennies because of compression. Redundancy is cheap; join cost is not.

### The Practical Spectrum

```
More Normalized                                           More Denormalized
     │                                                           │
     ▼                                                           ▼
  Data Vault        Kimball Star Schema         OBT / Activity Schema
(Hub/Link/Sat)    (Fact + Dimension tables)    (One wide table)
     │                      │                           │
  High audit         Balanced — best for         Best for BI speed,
  auditability       most analytics use          worst for maintenance
```

**Interview position:**
> "I don't treat this as binary. In practice, I build a normalized integration layer (or Data Vault for complex enterprises) as the source of truth. On top of that, I build denormalized data marts optimized for specific analytical domains. High-traffic query patterns get further materialized into OBTs or pre-aggregated summary tables. The key is that the normalized layer is always the authoritative source — denormalized layers are derived."

---

## 5. Migrating from an OLTP Schema to a Dimensional Model

This is a common real-world engineering challenge and a favorite interview topic.

### The OLTP Schema (Source)

A typical e-commerce OLTP database looks like:

```sql
-- Normalized for transactional efficiency
orders (order_id, customer_id, order_date, status, total_amount)
order_items (item_id, order_id, product_id, quantity, unit_price)
customers (customer_id, first_name, last_name, email, created_at, city)
products (product_id, name, category_id, cost_price, list_price)
categories (category_id, name, parent_category_id)
```

### The Migration Approach

**Step 1: Design the target dimensional model first**

Define grain, facts, and dimensions before touching the source data.

**Step 2: Map source columns to target schema**

```
OLTP Source                →  DW Target
────────────────────────────────────────────────────
orders.order_date          →  fact_order_line.date_key (via dim_date lookup)
order_items.quantity       →  fact_order_line.quantity_ordered
order_items.unit_price     →  fact_order_line.unit_price_usd
customers.city             →  dim_customer.city (SCD2 tracked)
categories.name +
parent category hierarchy  →  dim_product.category + dim_product.subcategory (denormalized)
```

**Step 3: Build the historical load**

Load all historical data from the OLTP. For dimensions:
- Reconstruct SCD2 history from audit tables, change logs, or slowly changing values in the OLTP if they exist
- If no history is available, load all customers at their current state as "SCD2 version 1" with `effective_date = epoch`

**Step 4: Build the incremental pipeline**

Switch from full historical load to CDC (Change Data Capture) for ongoing updates:
- Use Debezium, AWS DMS, or Fivetran to capture OLTP changes
- Apply SCD2 logic for dimension changes
- Append-only insert for new fact rows

**Step 5: Validate before cutover**

```sql
-- Validate revenue totals match between OLTP and DW
-- OLTP
SELECT SUM(oi.quantity * oi.unit_price) AS oltp_revenue
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- DW
SELECT SUM(net_revenue_usd) AS dw_revenue
FROM fact_order_line
WHERE date_key BETWEEN 20240101 AND 20241231;
```

Reconcile totals within acceptable tolerance (< 0.01% difference). Never cut over on unreconciled numbers.

**Step 6: Parallel operation period**

Run both OLTP reporting and DW reporting in parallel for 2–4 weeks. Surface any discrepancies before decommissioning OLTP-based reports.

### Common Migration Pitfalls

- **Grain mismatch:** Source system stores at order level, DW design is at line-item level — need to explode records during ETL
- **Missing historical SCD2 data:** OLTP systems often have no audit trail — agree with business on what "best effort" historical reconstruction looks like
- **Timezone inconsistencies:** OLTP stores timestamps in local time, DW expects UTC — convert at the ETL layer, never in the application layer
- **NULL handling differences:** OLTP allows NULL product prices (draft orders); DW fact tables need a strategy (default to 0, exclude, or use a special row)

---

## 6. Practice Scenarios

1. A product manager tells you that the previous data warehouse reported $120M in annual revenue, but the new DW you built reports $115M. Both are supposedly correct. How do you investigate and resolve this discrepancy?

2. You join a company and discover that every data model was designed by a different team, using different naming conventions and no conformed dimensions. A "customer" in the marketing model is defined differently from a "customer" in the finance model. What is your approach to standardizing this?

3. An engineering manager asks whether to build a new feature's analytics on the existing star schema or start fresh with an activity schema approach. What factors influence your recommendation?

4. A GDPR request comes in to delete all data for a specific customer. Your `fact_order_line` table has 50 million rows referencing this customer over 8 years. What is your deletion strategy, and what are the implications for historical aggregates?

5. The business wants "real-time" revenue reporting (< 1 minute latency). Your current batch pipeline runs every hour. What options do you present, and what are the modeling trade-offs of each?
