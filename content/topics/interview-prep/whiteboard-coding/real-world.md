---
title: "Whiteboard Coding - Real-World Patterns"
topic: interview-prep
subtopic: whiteboard-coding
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [interview-prep, whiteboard-coding, sql, python, real-world, production]
---

# Whiteboard Coding — Real-World Patterns

## How Live Coding Interviews Map to Real DE Work

The best preparation for DE live coding is understanding which problems companies draw from actual production work. This section maps common interview patterns to real-world scenarios, so your practice has context and the "why" behind each technique.

---

## SQL: Real Production Patterns That Appear in Interviews

### Pattern: Late-Arriving Data / Backfill Detection

In production, data often arrives late. A common analytics task is identifying which days have complete data vs. incomplete data.

```sql
-- Find days where data volume is abnormally low (possible late arrival)
WITH daily_volumes AS (
    SELECT
        DATE(event_time) AS event_date,
        COUNT(*) AS event_count
    FROM events
    WHERE event_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(event_time)
),
stats AS (
    SELECT
        AVG(event_count) AS avg_count,
        STDDEV(event_count) AS stddev_count
    FROM daily_volumes
    WHERE event_date < CURRENT_DATE  -- Exclude today (still filling)
)
SELECT
    dv.event_date,
    dv.event_count,
    s.avg_count,
    CASE
        WHEN dv.event_count < s.avg_count - 2 * s.stddev_count
        THEN 'ANOMALOUS_LOW'
        ELSE 'NORMAL'
    END AS volume_status
FROM daily_volumes dv
CROSS JOIN stats s
ORDER BY event_date DESC;
```

**Why this appears in interviews**: Interviewers at data platform companies ask this because it reflects the kind of monitoring queries DEs write in production.

### Pattern: Calculating Retention Cohorts

Cohort retention analysis is one of the most requested SQL problems at consumer tech companies.

```sql
-- D7 retention: what % of users active in week 0 returned in week 1?
WITH cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('week', MIN(event_date)) AS cohort_week
    FROM user_activity
    GROUP BY user_id
),
activity_weeks AS (
    SELECT
        a.user_id,
        c.cohort_week,
        DATE_TRUNC('week', a.event_date) AS activity_week,
        DATEDIFF('week', c.cohort_week, DATE_TRUNC('week', a.event_date)) AS weeks_since_cohort
    FROM user_activity a
    JOIN cohorts c ON a.user_id = c.user_id
)
SELECT
    cohort_week,
    COUNT(DISTINCT CASE WHEN weeks_since_cohort = 0 THEN user_id END) AS cohort_size,
    COUNT(DISTINCT CASE WHEN weeks_since_cohort = 1 THEN user_id END) AS retained_week_1,
    COUNT(DISTINCT CASE WHEN weeks_since_cohort = 4 THEN user_id END) AS retained_week_4,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN weeks_since_cohort = 1 THEN user_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN weeks_since_cohort = 0 THEN user_id END), 0),
        1
    ) AS week_1_retention_pct
FROM activity_weeks
GROUP BY cohort_week
ORDER BY cohort_week;
```

**Technique to highlight**: Using `NULLIF(..., 0)` in the denominator prevents division-by-zero errors for empty cohorts. This is a production habit that interviewers notice.

### Pattern: SCD Type 2 Query

Slowly Changing Dimensions Type 2 is common in data warehouse work. A frequent interview question is querying point-in-time data from an SCD2 table.

```sql
-- Find what plan a customer was on at a specific date
-- SCD2 customers table: (customer_id, plan, valid_from, valid_to, is_current)
SELECT
    c.customer_id,
    c.plan,
    c.valid_from,
    c.valid_to
FROM customers_scd2 c
WHERE c.customer_id = 12345
  AND '2024-06-15'::DATE BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31'::DATE);
```

**What to explain**: "SCD2 stores history by giving each version a validity range. `valid_to` is NULL for the current record, so I use `COALESCE` to replace NULL with a far-future date for the range check. This query gives me the plan as it was on June 15, 2024, regardless of what changes happened after."

---

## Python: Real ETL Patterns

### Pattern: Schema Validation at Ingestion

```python
from dataclasses import dataclass
from typing import Optional
import json

REQUIRED_FIELDS = {"user_id", "event_type", "timestamp"}
VALID_EVENT_TYPES = {"purchase", "refund", "view", "click"}

def validate_and_parse_events(raw_lines: list[str]):
    """
    Parse JSON event lines, validate schema, separate valid from invalid.
    Returns (valid_events, invalid_events) for dead-letter queue handling.
    """
    valid = []
    invalid = []

    for line_num, line in enumerate(raw_lines, 1):
        try:
            event = json.loads(line.strip())
        except json.JSONDecodeError as e:
            invalid.append({"line": line_num, "error": f"JSON parse error: {e}", "raw": line})
            continue

        # Check required fields
        missing = REQUIRED_FIELDS - set(event.keys())
        if missing:
            invalid.append({"line": line_num, "error": f"Missing fields: {missing}", "raw": line})
            continue

        # Validate event_type
        if event["event_type"] not in VALID_EVENT_TYPES:
            invalid.append({
                "line": line_num,
                "error": f"Invalid event_type: {event['event_type']}",
                "raw": line
            })
            continue

        valid.append(event)

    return valid, invalid

# Usage
valid, invalid = validate_and_parse_events(raw_lines)
print(f"Processed: {len(valid)} valid, {len(invalid)} invalid")
# In production: write invalid to a dead-letter S3 prefix or alert on invalid count
```

**What to narrate**: "In a production ETL, I'd write invalid records to a dead-letter queue rather than silently dropping them. I'd also emit a metric for invalid record rate — if it spikes, something upstream changed. The parse/validate split here mirrors what Great Expectations or Soda does, but implemented from scratch."

### Pattern: Merge/Upsert Logic in Python

```python
def upsert_records(existing: dict, new_records: list[dict], key_field: str) -> dict:
    """
    Merge new_records into existing dict, updating existing keys
    and adding new ones. Simulates a SQL MERGE/UPSERT operation.
    """
    result = dict(existing)  # Don't mutate the input
    for record in new_records:
        key = record[key_field]
        if key in result:
            # Update: merge fields, prefer new values
            result[key] = {**result[key], **record}
        else:
            # Insert: new key
            result[key] = record
    return result

# Example
existing_users = {
    "u1": {"user_id": "u1", "name": "Alice", "plan": "free"},
    "u2": {"user_id": "u2", "name": "Bob", "plan": "pro"},
}
updates = [
    {"user_id": "u1", "plan": "pro"},   # Update Alice's plan
    {"user_id": "u3", "name": "Carol", "plan": "free"},  # New user
]
merged = upsert_records(existing_users, updates, "user_id")
# u1 now has plan="pro", u3 is added
```

---

## DE-Specific Interview Tips That Actually Matter

### Think About Data Volume at Every Step

The habit that distinguishes experienced DEs in interviews: annotating every operation with its data implications.

When writing a JOIN, say: "This join is between a 10M-row fact table and a 100K-row dimension table. The join should be efficient because the dimension table fits in memory for a broadcast join."

When writing a GROUP BY, say: "The group-by result set is much smaller than the input — maybe 10K unique users from 100M events. That's a big reduction."

### Mention Index Usage (Even if You Can't Add Indexes)

"If this were running on a database with proper indexes, the WHERE clause on `user_id` would use the index and avoid a full scan. For a query like this in production, I'd check the EXPLAIN ANALYZE output to confirm the planner is using it."

### Handle NULLs Explicitly

Every SQL interview should include NULL handling. Common patterns to volunteer:

```sql
-- Safe division
NULLIF(numerator, 0)  -- Return NULL instead of divide-by-zero

-- Treat NULL as 0 in sums
COALESCE(amount, 0)

-- Exclude NULLs from COUNT (COUNT(*) counts all rows; COUNT(col) excludes NULLs)
SELECT COUNT(*) AS total_rows, COUNT(amount) AS rows_with_amount
FROM orders;

-- NULL-safe comparison (instead of = which returns NULL for NULL = NULL)
WHERE a IS NOT DISTINCT FROM b  -- Standard SQL
WHERE a <=> b                   -- MySQL syntax
```

### Edge Cases to Volunteer Without Being Asked

- **Empty input**: What does your function return for an empty list or empty table?
- **All NULLs**: What does SUM return for a column that's all NULL? (NULL, not 0)
- **Ties**: Does your ranking handle ties correctly?
- **Date boundaries**: Is your date filter inclusive or exclusive on the end?
- **Large data**: Would your Python solution work for 100M records? (If using a list in memory, probably not — mention this.)

### The "What Would You Do Differently in Production?" Question

Senior interviewers often end a coding question with "how would you make this production-ready?" Common answers:

- Add logging and error handling around each step
- Write unit tests for edge cases (empty input, NULLs, duplicates)
- Add monitoring: row count checks before and after transforms
- Parameterize hardcoded values (dates, thresholds)
- Handle schema drift: what if a new field appears in the input?

Volunteering these without being asked is a strong positive signal.
