---
title: "NULL Handling - Real-World"
topic: sql
subtopic: null-handling
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, null, production-bugs, not-in, avg, group-by, data-quality, interview-tips]
---

# NULL Handling — Real-World

These are real NULL bugs that ship to production, cost hours to debug, and are hard to catch in code review because the SQL looks syntactically correct.

---

## Production Bug #1: NOT IN Subquery Returns 0 Rows Due to One NULL

**Scenario**: A fraud team maintains a `flagged_users` table of user IDs that should be excluded from a marketing campaign. The campaign query uses `NOT IN`:

```sql
-- Campaign targeting query
SELECT user_id, email
FROM users
WHERE user_id NOT IN (SELECT user_id FROM flagged_users);
```

A data engineer accidentally inserted a row with `user_id = NULL` into `flagged_users` when backfilling from an upstream source that had a schema mismatch. The next morning, the campaign sends 0 emails.

**Why it breaks**:
```sql
-- NOT IN expands to:
WHERE user_id != 5 AND user_id != 12 AND user_id != NULL
-- user_id != NULL → UNKNOWN
-- TRUE AND TRUE AND UNKNOWN → UNKNOWN
-- UNKNOWN rows are excluded → 0 rows pass
```

**Fix — option 1**: filter NULLs out of the subquery
```sql
WHERE user_id NOT IN (SELECT user_id FROM flagged_users WHERE user_id IS NOT NULL)
```

**Fix — option 2**: use NOT EXISTS (immune to NULLs in the subquery)
```sql
SELECT u.user_id, u.email
FROM users u
WHERE NOT EXISTS (
    SELECT 1 FROM flagged_users f WHERE f.user_id = u.user_id
);
```

**Prevention**: add a NOT NULL constraint to `flagged_users.user_id` at the table level.

---

## Production Bug #2: AVG Off by 15% Due to Unexpected NULLs

**Scenario**: A data analyst builds a weekly revenue report that tracks average order value (AOV). For three weeks the numbers look correct. Then an upstream order management system starts sending orders with `revenue = NULL` for promotional orders (previously they sent 0).

```sql
-- Old behavior: promotional orders had revenue = 0
-- AVG(revenue) = SUM/COUNT(*) = correct

-- New behavior: promotional orders have revenue = NULL
-- AVG(revenue) = SUM / COUNT(revenue) -- excludes NULL rows from denominator!
-- If 15% of orders are promotional, AOV inflates by ~18% (100/85)
```

The report shows AOV jumped from $75 to $88 — everyone assumes a business success, but it's a data bug.

**Fix**: decide the semantics explicitly and document them:

```sql
-- Option A: "AOV including only orders with known revenue"
SELECT AVG(revenue) AS aov_known_orders
FROM orders
WHERE revenue IS NOT NULL;

-- Option B: "AOV treating promotional orders as $0 revenue"
SELECT AVG(COALESCE(revenue, 0)) AS aov_all_orders
FROM orders;

-- Option C: Monitor the NULL rate to detect the change
SELECT
    COUNT(*)                                                    AS total_orders,
    COUNT(revenue)                                              AS orders_with_revenue,
    ROUND(100.0 * (COUNT(*) - COUNT(revenue)) / COUNT(*), 2)  AS null_revenue_pct,
    AVG(revenue)                                               AS aov_excl_null,
    AVG(COALESCE(revenue, 0))                                  AS aov_incl_null_as_zero
FROM orders
WHERE order_week = '2024-03-11';
```

**Prevention**: add a data quality check that alerts when `null_revenue_pct` increases more than 2% week-over-week.

---

## Production Bug #3: GROUP BY Treating NULLs as Equal Creates Wrong Aggregations

**Scenario**: A logistics team tracks shipments by `carrier_id`. Some shipments have no assigned carrier yet (`carrier_id = NULL`). A weekly summary query:

```sql
SELECT carrier_id, COUNT(*) AS shipments
FROM shipments
WHERE shipped_date BETWEEN '2024-03-01' AND '2024-03-07'
GROUP BY carrier_id;
```

The output includes a row `(NULL, 847)`. This looks like 847 shipments have no carrier. But it's masking two different problems: some NULLs are "carrier not yet assigned" (pending shipments) and some are "carrier data lost in migration" (a data quality issue). Both are bucketed together under NULL.

**Fix**: tag NULLs by their root cause before aggregating:

```sql
-- Add a source tag to distinguish NULL types
SELECT
    COALESCE(carrier_id::VARCHAR, 'UNKNOWN') AS carrier_id,
    status,
    COUNT(*) AS shipments
FROM shipments
WHERE shipped_date BETWEEN '2024-03-01' AND '2024-03-07'
GROUP BY carrier_id, status;
-- Now you can see: status='pending' with NULL carrier vs status='delivered' with NULL carrier

-- Or use a data quality layer
SELECT
    carrier_id,
    COUNT(*) FILTER (WHERE status = 'pending')                      AS pending_no_carrier,
    COUNT(*) FILTER (WHERE status = 'delivered' AND carrier_id IS NULL) AS delivered_missing_carrier,
    COUNT(*) FILTER (WHERE carrier_id IS NOT NULL)                   AS with_carrier
FROM shipments
WHERE shipped_date BETWEEN '2024-03-01' AND '2024-03-07'
GROUP BY carrier_id;
```

---

## Interview Tips for NULL Questions

### The NULL Diagnostic Framework

When an interview problem involves data that "seems off," always run through the NULL checklist:

1. **Does the table have nullable columns involved in the filter?** Check `WHERE col != value` — NULLs pass neither side
2. **Does any aggregate look too high?** Check if `AVG` is excluding NULLs from denominator
3. **Does an anti-join return 0 rows?** Check for NULLs in the `NOT IN` subquery
4. **Does the GROUP BY have an unexpected NULL bucket?** Decide if it's intentional or a data quality issue
5. **Does a window function return NULL for recent rows?** Check if `IGNORE NULLS` is needed

### Common Interview Questions About NULLs

**"Why does `SELECT * FROM t WHERE col NOT IN (1, 2, NULL)` return no rows?"**

Walk through the logic: `NOT IN (1, 2, NULL)` expands to `col != 1 AND col != 2 AND col != NULL`. The last condition is always UNKNOWN. UNKNOWN AND anything is UNKNOWN or FALSE (never TRUE). So no rows pass.

**"How would you find rows where a column changed between two snapshots?"**

Standard approach breaks on NULLs:
```sql
-- Broken: misses NULL → value transitions
WHERE t1.col != t2.col

-- Correct: NULL-safe comparison
WHERE t1.col IS DISTINCT FROM t2.col

-- Or verbose but portable:
WHERE (t1.col != t2.col)
   OR (t1.col IS NULL AND t2.col IS NOT NULL)
   OR (t1.col IS NOT NULL AND t2.col IS NULL)
```

**"How do you implement a slowly-changing dimension where no-change means no update?"**

```sql
-- Detect actual changes (NULL-safe)
INSERT INTO dim_customer_history
SELECT c.customer_id, c.tier, c.region, CURRENT_TIMESTAMP AS effective_date
FROM customers_staging c
LEFT JOIN dim_customer d ON c.customer_id = d.customer_id AND d.is_current
WHERE c.tier IS DISTINCT FROM d.tier     -- catches NULL transitions
   OR c.region IS DISTINCT FROM d.region;
```

### Saying the Right Things in an Interview

- "I'd check if any NULL values are in the subquery before using NOT IN"
- "AVG ignores NULLs — I'd check whether that's the intended behavior or a bug"
- "I'd use IS DISTINCT FROM instead of != to handle NULL-to-value transitions in SCD logic"
- "I'd add a data quality monitor to alert if the NULL rate for this column exceeds X%"

---

## NULL Handling Checklist for Code Review

When reviewing SQL that touches nullable columns:

- [ ] All `NOT IN (subquery)` have NULL filtered from the subquery, or converted to `NOT EXISTS`
- [ ] `AVG(col)` semantics are documented — does excluding NULLs match business intent?
- [ ] `CASE WHEN` statements have an explicit `ELSE` (not relying on implicit NULL)
- [ ] JOIN conditions on nullable columns use `COALESCE` or `IS NOT DISTINCT FROM` if NULL matching is needed
- [ ] `ORDER BY` on nullable columns has explicit `NULLS FIRST`/`NULLS LAST`
- [ ] Data quality monitor exists for NULL rate on critical columns
