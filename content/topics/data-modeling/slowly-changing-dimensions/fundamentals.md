---
title: "Slowly Changing Dimensions - Fundamentals"
topic: data-modeling
subtopic: slowly-changing-dimensions
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [data-modeling, scd, dimensions, type1, type2, type3, warehouse]
---

# Slowly Changing Dimensions — Fundamentals

## What Are Slowly Changing Dimensions?

A Slowly Changing Dimension (SCD) is a dimension table attribute that **changes over time** — like a customer's address, an employee's department, or a product's price. The "slowly" means it doesn't change every transaction — but it DOES change occasionally.

**The problem:** When a customer moves from New York to San Francisco, should historical orders show the OLD address or the NEW address? The SCD type determines the answer.

> **Why SCD matters for DE:** This is one of the most common interview questions AND one of the most complex ETL patterns to implement. Every data warehouse deals with changing dimensions.

---

## The Three Main SCD Types

| Type | Strategy | History? | Complexity | Use Case |
|------|----------|:---:|:---:|----------|
| **Type 1** | Overwrite the old value | No | Simple | Corrections, non-critical attributes |
| **Type 2** | Add new row with date range | Full history | Complex | Audit trail, historical reporting |
| **Type 3** | Add column for previous value | Limited (1 version) | Medium | "What was it before?" queries |

---

## SCD Type 1 — Overwrite (No History)

Simply UPDATE the dimension record when a value changes.

**Before change:**

| customer_key | customer_id | name | city | segment |
|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | New York | Gold |

**After Alice moves:**

| customer_key | customer_id | name | city | segment |
|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | **San Francisco** | Gold |

```sql
-- SCD Type 1: simple UPDATE
UPDATE dim_customer 
SET city = 'San Francisco', updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 'C-100';
```

**Impact on historical facts:** ALL historical orders for Alice now show "San Francisco" as her city — even orders placed when she lived in New York.

**When to use Type 1:**
- Data corrections (typo fixes)
- Attributes that don't matter historically (phone number, email)
- When you WANT the latest value everywhere (simplified reporting)

---

## SCD Type 2 — Add New Row (Full History)

Create a NEW row for each version, with effective date range and a current flag.

**Before change:**

| customer_key | customer_id | name | city | effective_from | effective_to | is_current |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | New York | 2020-01-01 | 9999-12-31 | TRUE |

**After Alice moves (Jan 15, 2024):**

| customer_key | customer_id | name | city | effective_from | effective_to | is_current |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | New York | 2020-01-01 | **2024-01-14** | **FALSE** |
| **1002** | C-100 | Alice | **San Francisco** | **2024-01-15** | 9999-12-31 | TRUE |

```sql
-- Step 1: Close the current record
UPDATE dim_customer 
SET effective_to = '2024-01-14', is_current = FALSE
WHERE customer_id = 'C-100' AND is_current = TRUE;

-- Step 2: Insert new version
INSERT INTO dim_customer (customer_key, customer_id, name, city, effective_from, effective_to, is_current)
VALUES (NEXT_KEY(), 'C-100', 'Alice', 'San Francisco', '2024-01-15', '9999-12-31', TRUE);
```

**Impact on historical facts:**
- Orders placed before Jan 15 → join to key 1001 (New York) ✓
- Orders placed after Jan 15 → join to key 1002 (San Francisco) ✓
- **History is preserved perfectly!**

**Joining facts to SCD Type 2:**

```sql
-- Point-in-time lookup: which version was active when the order was placed?
SELECT f.order_id, f.amount, d.city
FROM fact_orders f
JOIN dim_customer d 
    ON f.customer_key = d.customer_key;
-- The fact table stores the SURROGATE KEY that was active at order time

-- OR: join by natural key + date range
SELECT f.order_id, f.amount, d.city
FROM fact_orders f
JOIN dim_customer d 
    ON f.customer_id = d.customer_id
    AND f.order_date >= d.effective_from
    AND f.order_date < d.effective_to;
```

**When to use Type 2:**
- Regulatory/compliance requirements (audit trail)
- Historical reporting ("revenue by customer segment AT THE TIME of purchase")
- Any attribute where the historical value matters for analysis

---

## SCD Type 3 — Add Column for Previous Value

Store the current AND previous value in separate columns.

**Before change:**

| customer_key | customer_id | name | current_city | previous_city |
|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | New York | NULL |

**After Alice moves:**

| customer_key | customer_id | name | current_city | previous_city |
|:---:|:---:|:---:|:---:|:---:|
| 1001 | C-100 | Alice | **San Francisco** | **New York** |

```sql
UPDATE dim_customer 
SET previous_city = current_city, 
    current_city = 'San Francisco'
WHERE customer_id = 'C-100';
```

**Limitation:** Only keeps ONE previous value. If Alice moves again, "New York" is lost — only "San Francisco" → new city would be tracked.

**When to use Type 3:**
- Only need "before and after" comparison
- Limited history is acceptable (one level back)
- Simpler than Type 2 (no date ranges, no multiple rows)

---

## Comparison Table

| Aspect | Type 1 | Type 2 | Type 3 |
|--------|--------|--------|--------|
| Storage overhead | None | High (N rows per entity) | Low (extra columns) |
| History preserved | No | Complete | One previous value only |
| ETL complexity | Simple (UPDATE) | Complex (close + insert) | Medium (UPDATE 2 cols) |
| Fact table impact | None (same key) | Must store version-specific key | None (same key) |
| Query complexity | Simple | Date-range joins needed | Simple |
| Recovery possible | No (original lost) | Yes (all versions available) | Partial (one version back) |

---

## Hybrid Approach (Most Common in Practice)

Real warehouses use DIFFERENT types for different attributes on the SAME dimension:

```sql
CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,        -- Surrogate key (new per Type 2 version)
    customer_id     VARCHAR(20),            -- Natural key (same across versions)
    
    -- Type 2 attributes (history tracked):
    segment         VARCHAR(20),            -- Changes trigger new row
    city            VARCHAR(50),            -- Changes trigger new row
    
    -- Type 1 attributes (overwritten):
    email           VARCHAR(100),           -- Just overwrite (no history needed)
    phone           VARCHAR(20),            -- Just overwrite
    
    -- Type 2 metadata:
    effective_from  DATE NOT NULL,
    effective_to    DATE NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);
```

> **Rule:** Track history (Type 2) only for attributes that matter for historical analysis. Use Type 1 for everything else (simpler ETL, less storage).

---

## Interview Tips

> **Tip 1:** "Explain SCD Type 2" — "When a tracked attribute changes, we close the current record (set effective_to and is_current=FALSE) and insert a new row with the new values, a new surrogate key, and effective_from = today. Historical facts keep their old surrogate key, so they always join to the correct historical version."

> **Tip 2:** "How do you join facts to a Type 2 dimension?" — "Two approaches: (1) Store the surrogate key in the fact table at insert time (fastest, but requires lookup during ETL). (2) Join on natural key + date range (f.order_date BETWEEN d.effective_from AND d.effective_to) — more flexible but slower."

> **Tip 3:** "When would you NOT use Type 2?" — "When history doesn't matter for business analysis (corrections, typo fixes), when storage/ETL complexity cost exceeds the value of history, or when the attribute changes too frequently (would create millions of versions). Use Type 1 for non-critical attributes."
