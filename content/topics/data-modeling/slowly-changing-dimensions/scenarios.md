---
title: "Slowly Changing Dimensions - Scenario Questions"
topic: data-modeling
subtopic: slowly-changing-dimensions
content_type: scenario_question
tags: [data-modeling, scd, interview, scenarios]
---

# Scenario Questions — Slowly Changing Dimensions

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement SCD Type 2 with MERGE

**Scenario:** Write the SQL MERGE statement that implements SCD Type 2 for a `dim_customer` table. The source is a daily snapshot of all customers. Detect changes in `segment` and `city` columns, close old records, and insert new versions.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Close changed records
MERGE INTO dim_customer t
USING daily_customer_snapshot s
ON t.customer_id = s.customer_id AND t.is_current = TRUE
WHEN MATCHED AND (t.segment != s.segment OR t.city != s.city) THEN
    UPDATE SET 
        t.is_current = FALSE,
        t.effective_to = CURRENT_DATE - 1;

-- Step 2: Insert new versions for changed + new customers
INSERT INTO dim_customer (customer_key, customer_id, name, segment, city, email,
                         effective_from, effective_to, is_current)
SELECT 
    NEXT_SURROGATE_KEY(),
    s.customer_id,
    s.name,
    s.segment,
    s.city,
    s.email,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM daily_customer_snapshot s
LEFT JOIN dim_customer t 
    ON s.customer_id = t.customer_id AND t.is_current = TRUE
WHERE t.customer_key IS NULL  -- Brand new customer
   OR t.segment != s.segment  -- Changed segment
   OR t.city != s.city;       -- Changed city

-- Step 3: Type 1 update for non-tracked attributes (email, phone)
UPDATE dim_customer t
SET t.email = s.email, t.phone = s.phone
FROM daily_customer_snapshot s
WHERE t.customer_id = s.customer_id AND t.is_current = TRUE
  AND (t.email != s.email OR t.phone != s.phone);
```

**Key design decisions:**
- Separate Type 2 (close + insert) from Type 1 (simple update) — different attributes get different treatment
- `effective_to = CURRENT_DATE - 1` for closed records (yesterday was the last day of the old version)
- `effective_from = CURRENT_DATE` for new version (today is the first day)
- Include both "brand new" customers (LEFT JOIN NULL) AND changed customers in the insert

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Handle Late-Arriving Dimension Changes

**Scenario:** Your ETL processes facts at 2 AM. At 8 AM, the source system sends a correction: customer C-100 changed segment from "Gold" to "Platinum" YESTERDAY (not today). Facts loaded at 2 AM used the old "Gold" segment. How do you handle this retroactive dimension change in a Type 2 SCD?

<details>
<summary>✅ Solution</summary>

**The problem:** The fact table already loaded orders from yesterday joining to the "Gold" version of the customer. Now we learn the change actually happened yesterday, not today.

**Solution: "Mini-dimension" approach for the correction**

```sql
-- Step 1: Retroactively adjust the Type 2 timeline
-- The current "Gold" record should have ended YESTERDAY, not still be current

-- Close the Gold record as of yesterday (the actual change date)
UPDATE dim_customer 
SET effective_to = '2024-01-14', is_current = FALSE
WHERE customer_id = 'C-100' AND segment = 'Gold' AND is_current = TRUE;

-- Insert the Platinum version starting from yesterday
INSERT INTO dim_customer (customer_key, customer_id, name, segment, city,
                         effective_from, effective_to, is_current)
VALUES (NEXT_KEY(), 'C-100', 'Alice', 'Platinum', 'San Francisco',
        '2024-01-14', '9999-12-31', TRUE);

-- Step 2: Fix the fact table (repoint yesterday's facts to the new dimension key)
UPDATE fact_orders f
SET customer_key = (
    SELECT customer_key FROM dim_customer 
    WHERE customer_id = 'C-100' AND is_current = TRUE
)
WHERE f.customer_id = 'C-100' 
  AND f.order_date = '2024-01-14'
  AND f.customer_key = (
    SELECT customer_key FROM dim_customer 
    WHERE customer_id = 'C-100' AND segment = 'Gold'
  );
```

**Alternative: Don't update facts (accept minor inaccuracy)**

In many real systems, retroactive corrections are handled by:
1. Applying the change as of TODAY (not retroactively)
2. Documenting that the effective_from is approximate
3. Only fixing facts for critical business cases (financial reporting)

**Prevention: Inferred Members**

If facts arrive BEFORE dimensions, use the "inferred member" technique:
```sql
-- Insert a placeholder when fact arrives with unknown dimension
INSERT INTO dim_customer (customer_key, customer_id, name, is_inferred)
VALUES (NEXT_KEY(), 'C-NEW-999', 'Unknown', TRUE);

-- When real dimension data arrives: update the inferred record
UPDATE dim_customer 
SET name = 'Bob', segment = 'Silver', city = 'Chicago', is_inferred = FALSE
WHERE customer_id = 'C-NEW-999' AND is_inferred = TRUE;
-- No fact table update needed — it already points to the correct key!
```

</details>

</article>
