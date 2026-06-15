---
title: "Data Modeling Interview — Intermediate Patterns"
description: "Mid-level data modeling interview topics including e-commerce schema design, bridge tables, role-playing dimensions, and ride-sharing walkthroughs"
content_type: study_material
topic: data-modeling
subtopic: interview-scenarios
layer: intermediate
difficulty_level: mid-level
tags: [data-modeling, e-commerce, bridge-tables, factless-fact, role-playing-dimensions, degenerate-dimensions, junk-dimensions, SCD, ride-sharing, interview]
---

# Data Modeling Interview — Intermediate Patterns

At the mid-level, interviewers expect you to move beyond definitions and demonstrate applied design skills. You should be able to walk through a complete schema design for a business domain, explain trade-offs confidently, and handle edge cases like many-to-many relationships and late-arriving data.

---

## 1. Designing a Data Warehouse for an E-Commerce Company

This is one of the most common mid-level design prompts. Let's walk through it systematically.

**Interview prompt:** "Design a data warehouse schema for an e-commerce company. It needs to support reporting on orders, products, customers, and inventory."

### Step 1: Clarify Requirements

Before drawing anything:
- What is the primary reporting need — sales performance, inventory health, customer behavior, or all three?
- What is the transaction volume — thousands of orders per day or millions?
- Do we need real-time or daily batch reporting?
- Are there multiple sales channels (web, mobile, marketplace)?

### Step 2: Identify Business Processes

For e-commerce, there are typically three fact tables covering distinct business processes:

1. **Sales (Order Line Items)** — the core transactional fact
2. **Inventory Snapshots** — periodic snapshot of stock levels
3. **Customer Sessions / Events** — web/app engagement (optional)

### Step 3: Design the Sales Fact Table

**Grain:** One row per order line item (one product on one order).

```sql
CREATE TABLE fact_order_line (
    order_line_key      BIGINT PRIMARY KEY,     -- surrogate key
    order_date_key      INT REFERENCES dim_date(date_key),
    ship_date_key       INT REFERENCES dim_date(date_key),  -- role-playing dim
    customer_key        INT REFERENCES dim_customer(customer_key),
    product_key         INT REFERENCES dim_product(product_key),
    store_key           INT REFERENCES dim_store(store_key),
    promotion_key       INT REFERENCES dim_promotion(promotion_key),
    order_number        VARCHAR(50),             -- degenerate dimension
    quantity_ordered    INT,
    unit_price_usd      DECIMAL(10,2),
    discount_amount_usd DECIMAL(10,2),
    gross_revenue_usd   DECIMAL(10,2),
    net_revenue_usd     DECIMAL(10,2)
);
```

Key design decisions:
- `order_date_key` and `ship_date_key` both reference `dim_date` — this is a **role-playing dimension**
- `order_number` is a **degenerate dimension** — lives in the fact table, no separate dimension table needed
- `promotion_key` can be `-1` (unknown) if no promotion applied — avoid NULLs

### Step 4: Design Dimension Tables

```sql
CREATE TABLE dim_customer (
    customer_key        INT PRIMARY KEY,          -- surrogate key
    customer_id         VARCHAR(50),              -- natural/business key
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(50),
    customer_segment    VARCHAR(50),              -- e.g. 'VIP', 'Standard'
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
);
-- SCD2 applied to customer_segment and address fields
```

```sql
CREATE TABLE dim_product (
    product_key         INT PRIMARY KEY,
    product_id          VARCHAR(50),
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    unit_cost_usd       DECIMAL(10,2),
    is_active           BOOLEAN,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
);
```

```sql
CREATE TABLE dim_date (
    date_key            INT PRIMARY KEY,       -- YYYYMMDD integer
    full_date           DATE,
    day_of_week         VARCHAR(10),
    day_number_in_week  INT,
    month_name          VARCHAR(10),
    month_number        INT,
    quarter             INT,
    year                INT,
    fiscal_year         INT,
    fiscal_quarter      INT,
    is_holiday          BOOLEAN,
    is_weekend          BOOLEAN
);
```

### Step 5: Design the Inventory Snapshot Fact

**Grain:** One row per product per store per day.

```sql
CREATE TABLE fact_inventory_snapshot (
    snapshot_date_key   INT REFERENCES dim_date(date_key),
    product_key         INT REFERENCES dim_product(product_key),
    store_key           INT REFERENCES dim_store(store_key),
    units_on_hand       INT,
    units_on_order      INT,
    reorder_threshold   INT,
    is_in_stock         BOOLEAN
);
```

Note: `units_on_hand` is a **semi-additive fact** — you can sum it across stores and products, but summing across dates gives a meaningless number. Instead, you take the latest snapshot (or average) across time.

---

## 2. Many-to-Many Relationships — Bridge Tables and Factless Fact Tables

### The Problem

A single customer can have multiple loyalty program memberships. A loyalty program can have many customers. How do you model this?

**Wrong approach:** Comma-separated list in a column. Never do this.

**Right approach:** Bridge table (also called an associative table).

### Bridge Table Pattern

```sql
CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_name   VARCHAR(100)
);

CREATE TABLE dim_loyalty_program (
    program_key     INT PRIMARY KEY,
    program_name    VARCHAR(100),
    tier            VARCHAR(50)
);

-- Bridge table resolves the many-to-many
CREATE TABLE bridge_customer_loyalty (
    customer_key    INT REFERENCES dim_customer(customer_key),
    program_key     INT REFERENCES dim_loyalty_program(program_key),
    enrollment_date DATE,
    is_primary      BOOLEAN,   -- optional: mark the primary membership
    PRIMARY KEY (customer_key, program_key)
);
```

When querying, join `fact_sales` → `bridge_customer_loyalty` → `dim_loyalty_program`. Be careful of fan-out: a customer with 3 memberships will appear 3 times in a join, inflating aggregates. Use a weighting factor or filter to `is_primary = true` depending on the use case.

### Factless Fact Tables

A factless fact table records the occurrence of an event that has no numeric measure.

**Example:** Student course enrollment.

```sql
CREATE TABLE fact_enrollment (
    enrollment_date_key INT REFERENCES dim_date(date_key),
    student_key         INT REFERENCES dim_student(student_key),
    course_key          INT REFERENCES dim_course(course_key),
    instructor_key      INT REFERENCES dim_instructor(instructor_key)
    -- no numeric facts — the row itself is the fact
);
```

Query: "How many students enrolled in each course this semester?" → `COUNT(*)` on this table.

**Example 2:** Coverage fact — "Which products were on promotion each day?"

```sql
CREATE TABLE fact_product_promotion_coverage (
    date_key        INT,
    product_key     INT,
    promotion_key   INT
);
```

---

## 3. Handling Late-Arriving Dimensions

**The problem:** An event arrives in the fact table before its dimension record exists. A new customer places an order before their registration is fully processed and loaded into `dim_customer`.

### Strategy 1: Use an "Unknown" Placeholder

Insert a row with `customer_key = -1` and description "Unknown Customer" in `dim_customer`. When the fact arrives with no matching customer, use `-1` as the foreign key. When the dimension record later arrives, update the fact rows.

**Pros:** No NULL foreign keys. Query won't silently drop rows.
**Cons:** Requires a reconciliation job to patch up late-arriving facts.

### Strategy 2: Infer a Dimension Record Immediately

When the fact arrives, create a minimal dimension record with what you know (e.g., just the `customer_id`). Later, when the full dimension record arrives, update the existing row (SCD1 on inferred attributes).

**Pros:** No placeholder rows. Facts immediately link to real dimension records.
**Cons:** Dimension table may temporarily have incomplete records.

### Strategy 3: Hold the Fact in a Landing/Staging Area

Delay loading the fact until the dimension is confirmed. Use a reconciliation queue.

**Pros:** Cleanest approach — no partial data in production.
**Cons:** Introduces latency. Not acceptable for near-real-time requirements.

**Interview framing:**
> "I handle late-arriving dimensions with a combination of an 'Unknown' placeholder for immediate loading and a nightly reconciliation job that patches foreign keys once the dimension record arrives. This ensures no fact rows are dropped while eventual consistency is guaranteed within 24 hours."

---

## 4. Role-Playing Dimensions

A single dimension table used multiple times in the same fact table, each alias representing a different role.

**Example:** In `fact_order_line`, dates appear twice:
- `order_date_key` — when the order was placed
- `ship_date_key` — when it was shipped

Both foreign keys point to the **same** `dim_date` table, but they represent different roles.

```sql
-- Query using role-playing dimension
SELECT
    o.full_date         AS order_date,
    s.full_date         AS ship_date,
    DATEDIFF('day', o.full_date, s.full_date) AS days_to_ship,
    SUM(f.gross_revenue_usd) AS revenue
FROM fact_order_line f
JOIN dim_date o ON f.order_date_key = o.date_key
JOIN dim_date s ON f.ship_date_key  = s.date_key
GROUP BY 1, 2, 3;
```

In some tools, you create **views** to alias the dimension:

```sql
CREATE VIEW dim_order_date AS SELECT * FROM dim_date;
CREATE VIEW dim_ship_date  AS SELECT * FROM dim_date;
```

This prevents confusion in BI tools that auto-generate joins.

---

## 5. Degenerate Dimensions

A dimension attribute that lives in the fact table itself — there is no separate dimension table for it because it provides no additional descriptive attributes.

**Classic example:** `order_number` in a retail sales fact.

The order number is useful for drilling down or grouping, but there's nothing to say about the order number itself beyond its value. Creating a `dim_order_number` table with one column would be pointless.

```sql
-- order_number is a degenerate dimension — it stays in the fact table
fact_order_line (
    order_line_key    BIGINT,
    order_number      VARCHAR(50),    -- degenerate dimension
    product_key       INT,
    quantity          INT,
    revenue           DECIMAL
);
```

Other examples: invoice number, ticket number, transaction ID.

---

## 6. Junk Dimensions

A junk dimension is a table that bundles together low-cardinality flags and indicators that would otherwise clutter the fact table.

**Without junk dimension:**
```sql
fact_order_line (
    ...
    is_gift_wrapped     BOOLEAN,
    is_express_shipping BOOLEAN,
    is_return_eligible  BOOLEAN,
    payment_method_flag VARCHAR(20),  -- 'credit', 'debit', 'paypal'
    channel_flag        VARCHAR(20)   -- 'web', 'mobile', 'store'
)
```

Five extra columns in the fact table, each with low cardinality. This wastes space because every row stores the same repeated values.

**With junk dimension:**
```sql
CREATE TABLE dim_order_flags (
    order_flags_key     INT PRIMARY KEY,
    is_gift_wrapped     BOOLEAN,
    is_express_shipping BOOLEAN,
    is_return_eligible  BOOLEAN,
    payment_method      VARCHAR(20),
    channel             VARCHAR(20)
);
-- Pre-populate all combinations: 2 * 2 * 2 * 3 * 3 = 72 rows

fact_order_line (
    ...
    order_flags_key     INT REFERENCES dim_order_flags(order_flags_key)
)
```

Now the fact table has one compact foreign key instead of five flag columns. The junk dimension has at most `2^n * m` rows (the cross product of all flag values) — still tiny.

---

## 7. Interview Walkthrough: Design a Schema for a Ride-Sharing App

**Common interview prompt:** "You're a data engineer at a ride-sharing company. Design a schema to support analytics on trip performance, driver earnings, and rider behavior."

### Clarify First

- "Is this for batch analytics or near-real-time?"
- "Do we need to model surge pricing? Driver ratings? Cancellations?"
- "Are drivers also sometimes riders?"

### Identify Business Processes

1. **Completed trips** — the primary transactional fact
2. **Driver shifts / availability** — when drivers are online (factless or snapshot)
3. **Ratings** — driver rated by rider and rider rated by driver

### Core Fact Table

**Grain:** One row per completed trip.

```sql
CREATE TABLE fact_trip (
    trip_key            BIGINT PRIMARY KEY,
    request_date_key    INT REFERENCES dim_date(date_key),
    pickup_date_key     INT REFERENCES dim_date(date_key),       -- role-playing
    dropoff_date_key    INT REFERENCES dim_date(date_key),       -- role-playing
    rider_key           INT REFERENCES dim_rider(rider_key),
    driver_key          INT REFERENCES dim_driver(driver_key),
    pickup_zone_key     INT REFERENCES dim_zone(zone_key),
    dropoff_zone_key    INT REFERENCES dim_zone(zone_key),       -- role-playing
    vehicle_key         INT REFERENCES dim_vehicle(vehicle_key),
    trip_flags_key      INT REFERENCES dim_trip_flags(trip_flags_key),  -- junk dim
    trip_id             VARCHAR(50),                             -- degenerate dim
    distance_miles      DECIMAL(8,2),
    duration_minutes    INT,
    base_fare_usd       DECIMAL(8,2),
    surge_multiplier    DECIMAL(4,2),
    surge_amount_usd    DECIMAL(8,2),
    total_fare_usd      DECIMAL(8,2),
    driver_earnings_usd DECIMAL(8,2),
    platform_fee_usd    DECIMAL(8,2),
    tip_amount_usd      DECIMAL(8,2)
);
```

### Junk Dimension for Trip Flags

```sql
CREATE TABLE dim_trip_flags (
    trip_flags_key      INT PRIMARY KEY,
    is_surge_active     BOOLEAN,
    is_shared_ride      BOOLEAN,
    is_scheduled        BOOLEAN,
    payment_method      VARCHAR(20),  -- 'card', 'cash', 'wallet'
    vehicle_type        VARCHAR(20)   -- 'standard', 'xl', 'luxury'
);
```

### Role-Playing Zone Dimension

```sql
CREATE TABLE dim_zone (
    zone_key        INT PRIMARY KEY,
    zone_id         VARCHAR(50),
    zone_name       VARCHAR(100),
    city            VARCHAR(100),
    neighborhood    VARCHAR(100),
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6)
);
-- pickup_zone_key and dropoff_zone_key both reference dim_zone
```

### Key Points to Mention in the Interview

1. Grain is one row per **completed** trip — cancelled trips go in a separate fact or are tracked with a flag
2. `dim_zone` is role-played for pickup and dropoff
3. `dim_date` is role-played for request time, pickup time, and dropoff time
4. Driver earnings and platform fees decompose total fare — this is useful for finance reporting
5. A separate `fact_driver_shift` could track when drivers go online/offline — this is a **periodic snapshot** or **factless fact**

---

## 8. Practice Questions

1. You need to report on which products were on sale on any given day. No revenue is recorded — the sale either happened or didn't. How do you model this?
2. A product can belong to multiple categories (e.g., a tablet is in both "Electronics" and "Computing"). How do you handle this in a star schema?
3. What is the difference between a degenerate dimension and a junk dimension?
4. In the ride-sharing schema, why is `surge_multiplier` stored as a fact rather than a dimension attribute?
5. A new customer signs up after placing their first order. How do you handle the order in your fact table before the customer dimension record exists?
6. You need to track both the date a policy was written and the date a claim was filed. How do you model these two dates?
7. When would a factless fact table be more appropriate than a regular fact table with a binary flag?
