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

---

## ⚡ Quick-fire Q&A

**Q: What is a slowly changing dimension (SCD) and why does it exist?**
A: An SCD is a dimension whose attributes change over time but not at a high frequency (e.g., customer address, product category). The SCD pattern defines how to handle these changes — whether to overwrite history, preserve it, or track it partially.

**Q: What is the difference between SCD Type 1 and Type 2?**
A: Type 1 overwrites the current value with no history retained — useful when history is irrelevant or incorrect data needs correction. Type 2 inserts a new row with a new surrogate key, preserving the full change history and enabling point-in-time accurate joins.

**Q: How do you implement SCD Type 2 in a dbt model?**
A: Use dbt's snapshot feature with a `strategy: timestamp` or `strategy: check` configuration. dbt automatically manages effective date columns (`dbt_valid_from`, `dbt_valid_to`) and the `is_current` flag, inserting new rows on change and closing out old ones.

**Q: What is SCD Type 3 and what are its limitations?**
A: Type 3 adds a "previous value" column alongside the current value column, allowing one level of history. Its limitation is that it only supports a fixed number of historical versions — typically just the most recent change — making it unsuitable for attributes that change frequently.

**Q: What is SCD Type 6 (hybrid SCD)?**
A: Type 6 combines Types 1, 2, and 3 by adding a new row for each change (Type 2), overwriting current value columns across all rows for an entity (Type 1), and maintaining a "previous value" column (Type 3). This gives both full history and easy access to the current value.

**Q: How do you handle SCD in a streaming pipeline?**
A: Use upsert/merge operations on the target table (e.g., Delta Lake MERGE) triggered by change events. Each event carries the new attribute values; the merge closes the previous record's validity window and inserts the new version with the current event timestamp.

**Q: What columns are typically added to a Type 2 dimension table?**
A: `effective_date` (or `valid_from`) marks when the record became active. `expiry_date` (or `valid_to`) marks when it was superseded (NULL or a far-future date for current records). An `is_current` boolean flag simplifies filtering for the current version.

**Q: What problems arise when joining fact tables to Type 2 dimensions?**
A: If the fact table stores only the dimension's natural key, you must join on both the natural key and the fact event date falling within the dimension's effective/expiry window — a range join that is expensive at scale. Storing the surrogate key in the fact table at load time avoids this.

---

## 💼 Interview Tips

- Know all SCD types (1–6) and be ready to explain them with a concrete example like customer address or product price changes.
- The most common interview mistake is not accounting for how the fact table joins to a Type 2 dimension — always discuss surrogate key assignment at load time.
- dbt snapshots are the modern standard for SCD Type 2 implementation; mention them to show practical, production-level knowledge.
- Discuss the operational overhead of SCD Type 2 at scale — billions of rows with many SCD columns can cause significant storage and query costs.
- Senior interviewers will ask about late-arriving data in SCD pipelines — be prepared to explain how to retroactively insert or correct dimension versions.
- Show awareness that not every changing attribute needs Type 2 — the decision should be driven by whether historical accuracy of that attribute affects business analysis.
