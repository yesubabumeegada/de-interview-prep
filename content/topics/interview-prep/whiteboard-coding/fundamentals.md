---
title: "Whiteboard Coding - Fundamentals"
topic: interview-prep
subtopic: whiteboard-coding
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [interview-prep, whiteboard-coding, sql, python, fundamentals]
---

# Whiteboard Coding — Fundamentals

## What Companies Actually Test for Data Engineers

Live coding interviews for data engineers are different from software engineering interviews. You will rarely be asked to implement a binary search tree or solve a graph traversal problem. The focus is on the tools and thought patterns data engineers use every day: SQL, Python data manipulation, and occasionally small algorithm problems relevant to data processing.

Understanding this distinction matters. Over-preparing for LeetCode-hard algorithm problems while neglecting SQL is a common and costly mistake for DE candidates.

---

## What DE Live Coding Interviews Actually Cover

### Category 1: SQL (Most Common)

SQL is tested at nearly every company that interviews data engineers. Specific patterns:

- **Window functions**: running totals, rank within group, lag/lead
- **Aggregations with filters**: WHERE vs. HAVING, conditional aggregation
- **Joins**: INNER, LEFT, self-joins, anti-joins
- **Advanced patterns**: top-N per group, gaps and islands, session windowing, consecutive events
- **CTEs**: structuring complex queries for readability

Most companies provide a simple schema and ask you to write a query. Some ask you to explain what a given query does. A few ask you to optimize a slow query.

### Category 2: Python Data Manipulation

Python problems test your ability to process data programmatically. Common problems:

- Flatten a nested JSON structure
- Deduplicate events while keeping the latest per key
- Implement a simple ETL (read, transform, write)
- Group and aggregate data with standard library or pandas
- Parse and transform log lines

Note: You will rarely be asked to use pandas in a whiteboard setting unless the job specifically involves data science. Pure Python solutions using dictionaries and lists are usually preferred because they're more universally readable.

### Category 3: Small Algorithm Problems

These occasionally appear, but are almost always data-adjacent:

- Find the most frequent element in a list
- Merge two sorted arrays (relevant to merge-sort-based data processing)
- Find duplicates in a dataset
- Implement a simple sliding window counter

---

## The Live Coding Approach: Think Aloud

The single most important skill in a live coding interview is **narrating your thinking**. The interviewer is not just evaluating the final solution — they're evaluating how you problem-solve. A candidate who writes a working solution in silence is harder to evaluate than a candidate who talks through an imperfect approach.

### The 5-Step Approach

**Step 1: Restate the problem in your own words**

"So if I'm understanding correctly, I need to find the top 3 customers by total spend for each region, and if there are ties, I should include all tied customers. Is that right?"

This confirms understanding and buys time to think.

**Step 2: Clarify ambiguities**

- "Should I handle NULLs in the amount column?"
- "Is the input sorted or unsorted?"
- "What should happen with duplicate records — deduplicate first or include them in the calculation?"

**Step 3: Sketch the approach before writing code**

"I'll start by computing total spend per customer per region, then rank them within each region, then filter for rank ≤ 3. I'll use a CTE to make the steps readable."

Say this before writing a single line. If your approach is wrong, the interviewer will redirect you now rather than after you've written 20 lines.

**Step 4: Write the code, narrate as you go**

"I'm using `DENSE_RANK` here instead of `RANK` because the problem says to include all tied customers — `RANK` would skip the next rank after a tie, which is what I want if I were numbering positions, but `DENSE_RANK` keeps consecutive values."

**Step 5: Test with an example**

Walk through your solution with a small mental test case before declaring it done. Say: "Let me trace through this. If customer A has $100 and customer B has $100 and customer C has $50, all in the East region, DENSE_RANK gives A=1, B=1, C=2 — so both A and B would be included in the top 3 even though they both have rank 1. That's correct."

---

## SQL Fundamentals: The Patterns You Must Know

### Window Functions

Window functions are tested in nearly every SQL interview. You must be able to write and explain:

```sql
-- Running total
SELECT
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
FROM orders;

-- Rank within group (top N per group)
SELECT *
FROM (
    SELECT
        region,
        customer_id,
        SUM(amount) AS total_spend,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rnk
    FROM orders
    GROUP BY region, customer_id
) ranked
WHERE rnk <= 3;

-- Lag/Lead for period-over-period comparison
SELECT
    month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY month) AS prev_month_revenue,
    revenue - LAG(revenue, 1) OVER (ORDER BY month) AS mom_change
FROM monthly_revenue;
```

### Conditional Aggregation

```sql
-- Pivot-style aggregation without a PIVOT clause
SELECT
    user_id,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN event_type = 'refund' THEN 1 ELSE 0 END) AS refund_count,
    SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS total_purchased,
    SUM(CASE WHEN event_type = 'refund' THEN amount ELSE 0 END) AS total_refunded
FROM events
GROUP BY user_id;
```

---

## Python Fundamentals: Core Patterns

### Flatten a Nested JSON

```python
def flatten_json(obj, parent_key='', sep='_'):
    """Flatten a nested dict into a single-level dict with compound keys."""
    items = {}
    for k, v in obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

# Example
nested = {"user": {"id": 1, "address": {"city": "NYC", "zip": "10001"}}, "score": 95}
print(flatten_json(nested))
# {'user_id': 1, 'user_address_city': 'NYC', 'user_address_zip': '10001', 'score': 95}
```

**What to say**: "I'd handle the case where a value is a list separately — in practice, list values in JSON events need a decision: do we expand them into multiple rows, or serialize them as strings? I'd ask the interviewer before assuming."

### Deduplicate Events Keeping Latest

```python
def deduplicate_keep_latest(events, key_field, timestamp_field):
    """Keep only the most recent event per key."""
    latest = {}
    for event in events:
        key = event[key_field]
        if key not in latest or event[timestamp_field] > latest[key][timestamp_field]:
            latest[key] = event
    return list(latest.values())

events = [
    {"id": 1, "ts": "2024-01-01T10:00:00", "status": "pending"},
    {"id": 1, "ts": "2024-01-01T10:05:00", "status": "shipped"},
    {"id": 2, "ts": "2024-01-01T09:00:00", "status": "delivered"},
]
print(deduplicate_keep_latest(events, "id", "ts"))
# [{"id": 1, "ts": "...", "status": "shipped"}, {"id": 2, ...}]
```

---

## Data Engineering-Specific Interview Tips

**Always mention NULLs**: In any SQL solution, say "I'm assuming there are no NULLs in the join key — if there are, they'd be excluded from an INNER JOIN, and I'd want to confirm that's correct."

**Think about data types**: "I'm treating `amount` as a float here, but if this is a currency field in production, I'd want to use DECIMAL(10,2) to avoid floating-point precision errors."

**Mention scale**: "This solution works for thousands of rows. If this were running on a billion-row table, I'd want to make sure the window function can partition-prune effectively, and I'd check the execution plan for sort operations."

**Handle edge cases aloud**: "What if the input list is empty? My loop returns an empty dict — that's the right behavior."

These habits signal that you think like a production engineer, not just a puzzle solver.
