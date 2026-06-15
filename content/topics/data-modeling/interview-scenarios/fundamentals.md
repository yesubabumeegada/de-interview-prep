---
title: "Data Modeling Interview Fundamentals"
description: "Core patterns and concepts every junior data engineer must know for data modeling interview questions"
content_type: study_material
topic: data-modeling
subtopic: interview-scenarios
layer: fundamentals
difficulty_level: junior
tags: [data-modeling, star-schema, snowflake-schema, fact-tables, dimension-tables, SCD, surrogate-keys, interview]
---

# Data Modeling Interview Fundamentals

Data modeling questions are among the most common in data engineering interviews. Interviewers use them to assess whether you understand how data is organized for analytical workloads, whether you can translate business requirements into schemas, and whether you know the trade-offs between different design patterns.

This guide covers the foundational patterns you will encounter at the junior level.

---

## 1. How to Approach "Design a Schema for X"

When an interviewer asks "Design a schema for X," they are testing your structured thinking as much as your technical knowledge. Use this framework:

### Step 1: Clarify Requirements Before Drawing Anything

Always ask questions first. Jumping straight to a diagram signals that you do not think about requirements before coding.

Good clarifying questions:
- "What are the main business questions this schema needs to answer?"
- "What is the expected data volume — millions of rows or billions?"
- "Are there any latency requirements for queries?"
- "What system will produce this data — OLTP, event streams, flat files?"
- "Who are the consumers — BI dashboards, data scientists, ad-hoc SQL analysts?"

### Step 2: Identify the Business Process

Every data warehouse design starts with identifying the business process being measured. Examples:
- Retail sales → the act of a customer purchasing a product
- Ride-sharing → the act of completing a trip
- Streaming → the act of a user watching content

### Step 3: Identify the Grain

The **grain** is the most important decision in dimensional modeling. It defines what a single row in your fact table represents.

Examples of grain declarations:
- "One row per line item on a sales order"
- "One row per completed ride"
- "One row per daily snapshot of an account balance"

Finer grain = more flexibility but larger table. Coarser grain = faster queries but loss of detail.

### Step 4: Identify Dimensions

Ask: "What context describes each event?" Dimensions answer the who, what, where, when, and why.

Common dimensions:
- `dim_date` — when did this happen?
- `dim_customer` — who performed this action?
- `dim_product` — what was involved?
- `dim_location` — where did it happen?

### Step 5: Identify Facts (Measures)

Facts are the numeric, additive measurements. Ask: "What do we want to measure?"

Common facts:
- `quantity_sold`
- `revenue_usd`
- `trip_duration_minutes`
- `discount_amount`

Facts should be additive across all dimensions (sum across customer + product + date makes sense) or at least semi-additive (sum across some dimensions).

---

## 2. Star Schema vs Snowflake Schema

This is one of the most common comparison questions. Know the difference cold.

### Star Schema

```
                    dim_date
                       |
dim_customer --- fact_sales --- dim_product
                       |
                  dim_store
```

- Dimension tables are **denormalized** — all attributes stored in one flat table
- Joins are simple: fact table joins directly to each dimension
- Query performance is better because fewer joins
- Storage is slightly larger due to redundant data in dimensions
- Easier for BI tools and analysts to understand

**When to use:** Most OLAP workloads, especially on cloud MPP databases (Redshift, BigQuery, Snowflake). The default choice for a Kimball-style data warehouse.

### Snowflake Schema

```
dim_product_category
        |
    dim_product
        |
dim_customer --- fact_sales --- dim_product
        |
    dim_city
        |
   dim_state
        |
  dim_country
```

- Dimension tables are **normalized** — hierarchies split into separate tables
- More joins required per query
- Saves storage by removing redundant data
- Harder to query and explain to non-technical stakeholders

**When to use:** When storage is at a premium (rare on modern cloud), or when the data engineering team maintains strict normalization standards. Less common in practice for analytics.

### Interview Tip

If asked "which is better?" always answer with trade-offs:
> "Star schema is generally preferred for analytics because simpler queries run faster and are easier to maintain. Snowflake schema saves storage and is more normalized, but the additional joins add query complexity. On modern columnar databases like BigQuery or Snowflake, storage cost differences are minimal, so I default to star schema unless there's a specific reason not to."

---

## 3. Fact Table vs Dimension Table — How to Tell the Difference

A common trick question: "Is X a fact or a dimension?"

| Characteristic | Fact Table | Dimension Table |
|---|---|---|
| Contains | Numeric measurements | Descriptive attributes |
| Rows represent | Events or transactions | Things/entities |
| Cardinality | Very high (billions of rows) | Lower (millions or fewer) |
| Changes | Append-only (typically) | Can change over time |
| Examples | `fact_sales`, `fact_page_views` | `dim_customer`, `dim_product` |

**Tricky case — "Is revenue a fact?"** Yes. Revenue, quantity, duration are all facts.

**Tricky case — "Is date a fact or dimension?"** Date is always a dimension (`dim_date`). Never store raw timestamps as your only date reference — pre-build a calendar dimension so analysts can filter by fiscal year, quarter, day of week, holiday flag, etc.

**Tricky case — "What about a status flag?"** Status (e.g., `order_status = 'shipped'`) is a dimension attribute, not a fact. It describes the event, not measures it.

---

## 4. Slowly Changing Dimensions (SCD Types 1, 2, 3)

SCDs describe how to handle changes to dimension attributes over time. This is a very high-frequency interview topic.

### The Problem

A customer moves from New York to Los Angeles. Historical sales should reflect their old address at the time of purchase. How do you handle this?

### SCD Type 1 — Overwrite (No History)

Simply update the existing row. Old value is lost.

```sql
UPDATE dim_customer
SET city = 'Los Angeles'
WHERE customer_id = 12345;
```

**Use when:** History doesn't matter. Example: correcting a typo in a customer's name.

**Drawback:** Cannot analyze historical behavior by old attribute value.

### SCD Type 2 — Add a New Row (Full History)

Insert a new row for the changed record. Track validity with effective/expiration dates and an `is_current` flag.

```sql
-- dim_customer after SCD2 update
| surrogate_key | customer_id | city         | effective_date | expiry_date  | is_current |
|---------------|-------------|--------------|----------------|--------------|------------|
| 1001          | 12345       | New York     | 2020-01-01     | 2024-06-14   | false      |
| 1002          | 12345       | Los Angeles  | 2024-06-15     | 9999-12-31   | true       |
```

The fact table stores the `surrogate_key` (1001 or 1002), so historical sales correctly link to "New York."

**Use when:** You need to analyze behavior by the attribute value at the time of the event. Most common SCD type.

**Drawback:** Dimension table grows over time. Queries must filter for `is_current = true` for current snapshots.

### SCD Type 3 — Add a New Column (Limited History)

Add a `previous_value` column alongside the current value. Only preserves one level of history.

```sql
| customer_id | current_city | previous_city |
|-------------|--------------|---------------|
| 12345       | Los Angeles  | New York      |
```

**Use when:** You only ever need to compare "current" vs "one change ago." Example: tracking the most recent territory reassignment for a sales rep.

**Drawback:** Cannot track more than one prior value. Rarely used in practice.

### SCD Type 4 (Bonus — Mini-Dimension)

Split rapidly changing attributes (age band, income bracket) into a separate mini-dimension to avoid massive SCD2 churn on the main customer table.

---

## 5. Surrogate Keys vs Natural Keys

### Natural Key

The business identifier — `customer_email`, `product_sku`, `order_number`. Comes from the source system.

**Problem:** Natural keys can change. A customer can change their email. A product SKU can be reused. Source systems can have duplicates. Natural keys may not be stable across source systems in an enterprise.

### Surrogate Key

A system-generated integer (or UUID) with no business meaning — `customer_key INT IDENTITY`.

**Benefits:**
- Stable — never changes even if the business identifier changes
- Enables SCD2 — each version of a dimension record gets its own surrogate key
- Faster joins — integer comparisons are faster than string comparisons
- Decouples the warehouse from source system key changes

**Interview answer:**
> "I always use surrogate keys in the data warehouse. Natural keys go in the dimension as a business key attribute for traceability, but the fact table foreign keys always point to the surrogate key. This protects us from source system changes and enables SCD2 versioning."

---

## 6. Common Interview Mistakes to Avoid

### Mistake 1: Not Declaring the Grain

Jumping to table design without stating what a single fact row represents. Always say "the grain of this fact table is one row per ___" before drawing your schema.

### Mistake 2: Putting Descriptive Attributes in the Fact Table

`customer_city` in `fact_sales` is wrong. It belongs in `dim_customer`. Facts should only contain foreign keys, dates, and numeric measures.

### Mistake 3: Ignoring the Date Dimension

Storing just a `timestamp` column in the fact table is insufficient. Join to a `dim_date` table so analysts can filter by fiscal periods, holidays, and day-of-week without string parsing.

### Mistake 4: Forgetting Surrogate Keys

Using `customer_email` as the foreign key in `fact_sales`. Email can change; a surrogate key cannot.

### Mistake 5: Making Everything SCD2

SCD2 adds complexity. Only apply it to attributes where historical analysis matters. If a customer's loyalty tier changes and analysts only care about the current tier, SCD1 is simpler and sufficient.

### Mistake 6: Ignoring NULL Handling in Dimensions

When a dimension is unknown at the time of the fact (e.g., a customer checked out as a guest), use a special "Unknown" dimension row with `surrogate_key = -1` rather than NULL foreign keys. NULL foreign keys break GROUP BY and cause fact rows to be silently dropped in inner joins.

---

## 7. Key Vocabulary Cheat Sheet

| Term | Definition |
|---|---|
| Grain | What one row in the fact table represents |
| Additive fact | Can be summed across all dimensions (revenue, quantity) |
| Semi-additive fact | Can be summed across some dimensions (account balance — sum across accounts, not across time) |
| Non-additive fact | Cannot be summed (ratios, percentages) |
| Conformed dimension | A dimension shared across multiple fact tables (e.g., `dim_date` used by fact_sales and fact_inventory) |
| Degenerate dimension | A dimension attribute stored in the fact table with no dimension table (e.g., `order_number`) |
| Factless fact table | A fact table with no numeric measures — used to record events (student enrolled in course) |
| Junk dimension | A dimension that groups together low-cardinality flags and indicators from the fact table |
| Role-playing dimension | A single dimension table used multiple times in a fact table under different aliases (dim_date as order_date and ship_date) |

---

## 8. Practice Questions

1. Design a star schema for a hotel booking system. What is the grain? What are the dimensions? What are the facts?
2. A product's price changes monthly. Should you use SCD1, SCD2, or SCD3? Why?
3. What is the difference between an additive and a semi-additive fact? Give an example of each.
4. A customer checks out as a guest — no account created. How do you handle the customer dimension foreign key in the fact table?
5. You have two dates in the same fact row: `order_date` and `ship_date`. How do you model this?
6. What is a conformed dimension, and why does it matter for reporting?
7. When would you choose a snowflake schema over a star schema?
8. What is a junk dimension? Give an example.
