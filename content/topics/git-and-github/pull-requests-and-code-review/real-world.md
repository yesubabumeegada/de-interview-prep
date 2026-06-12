---
title: "Pull Requests and Code Review - Real World"
topic: git-and-github
subtopic: pull-requests-and-code-review
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [git, github, code-review, real-world]
---

# Pull Requests and Code Review — Real World

## Case Study: PR Review Caught a $1.2M Revenue Bug

### Background

A DE team at a marketplace company merged a dbt model change that modified how orders with applied coupons were counted. The PR was "just a cleanup" — removing what looked like dead code.

### What Happened

```sql
-- Before (correct):
SELECT 
    order_id,
    amount - COALESCE(coupon_value, 0) AS net_revenue
FROM orders
WHERE status = 'completed'

-- "Cleanup" PR removed COALESCE (author thought coupon_value was always populated):
SELECT
    order_id,
    amount - coupon_value AS net_revenue    -- ← NULL propagation!
FROM orders
WHERE status = 'completed'
```

For orders without a coupon (`coupon_value IS NULL`), `amount - NULL = NULL`. About 15% of orders silently became `NULL` revenue. Finance reported a $1.2M revenue drop in Q3.

**The PR had been merged in 8 minutes with a one-word approval: "LGTM."**

### What They Changed

**1. PR template added explicit checklist:**
```markdown
## Data Correctness
- [ ] Tested with NULL values in all relevant columns
- [ ] Verified row counts before and after match expectations
- [ ] Run: SELECT COUNT(*), COUNT(net_revenue) FROM staging.orders — both should equal
```

**2. Mandatory dbt test added:**
```yaml
# schema.yml
columns:
  - name: net_revenue
    tests:
      - not_null    # Would have caught this immediately
      - dbt_utils.accepted_range:
          min_value: 0
```

**3. Review quality guidance:**
```
New team norm: "LGTM" reviews not accepted.
Every review must include at least:
- What you checked (specific logic, edge cases)
- OR a specific question about something unclear
```

**4. SQL-specific review checklist:**
```markdown
SQL Review Checklist:
- [ ] Every arithmetic operation handles NULL (COALESCE, NULLIF, ISNULL)
- [ ] JOIN type is intentional (INNER vs LEFT — can rows be silently dropped?)
- [ ] GROUP BY includes all non-aggregated columns
- [ ] WHERE clause is correct (especially status filter logic)
```

### Result

The dbt `not_null` test on `net_revenue` would have failed in CI and blocked the merge. Three people on the team independently said "I assumed it was always populated" — the test is the only reliable check.

**Key lesson:** A review checklist changes what reviewers look for. Without explicit prompts, humans default to reviewing style over correctness.
