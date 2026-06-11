---
title: "Pull Requests and Code Review — Scenarios"
topic: git-and-github
subtopic: pull-requests-and-code-review
content_type: scenario_question
tags: [git, github, pull-requests, code-review, interview, scenarios]
---

# Pull Requests and Code Review — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Write a Good PR Description

**Scenario:** You added a dbt model `fct_orders_daily` that aggregates daily order metrics. Write a PR description that follows best practices.

<details>
<summary>💡 Hint</summary>

Cover: what changed (the model, what it computes), why (business need/ticket), how you tested it (dbt test, sample output), and any impact on downstream models. Keep it concise but complete — reviewers shouldn't have to read the code to understand the purpose.

</details>

<details>
<summary>✅ Solution</summary>

```markdown
## feat(orders): add daily order metrics aggregation (DE-412)

## Summary
Adds `fct_orders_daily` — a daily aggregation of order volume and revenue
by channel. This is the source for the Finance dashboard's daily order report.

## Changes
- New model: `dbt/models/gold/fct_orders_daily.sql`
- New schema tests: not_null, unique on date+channel

## Why
Finance team needs daily granularity for the Q4 dashboard (DE-412).
Previously they were aggregating in Looker from the order-level fact table,
causing slow queries and inconsistent numbers.

## Testing
- [x] `dbt compile` passes
- [x] `dbt test --select fct_orders_daily` passes (not_null, unique)
- [x] Sample output verified against Finance's existing manual calculation:

| date       | channel | orders | revenue   |
|------------|---------|--------|-----------|
| 2024-01-01 | organic | 1,243  | $45,231   |
| 2024-01-01 | paid    | 892    | $32,150   |

✓ Matches Finance spreadsheet to within 0.01%.

## Downstream Impact
None — new model, no existing models modified.

## Rollback
This model is new — revert the PR. No downstream models depend on it yet.
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Review a SQL Model PR

**Scenario:** You're reviewing this dbt SQL model PR. Identify any issues.

```sql
-- fct_revenue_by_customer.sql
SELECT
    customer_id,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count,
    SUM(amount) / COUNT(*) AS avg_order_value
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY 1
```

<details>
<summary>💡 Hint</summary>

Check for: (1) Missing status filter — are cancelled/pending orders included? (2) Division by zero — COUNT(*) is always ≥1 so this is safe, but check if there's a row per customer even with 0 orders (no, there isn't with WHERE + GROUP BY). (3) Hardcoded date — should this be parameterized or is it intentional? (4) Missing not_null test for customer_id. (5) Schema tests — are there any?

</details>

<details>
<summary>✅ Solution</summary>

```markdown
**Review: fct_revenue_by_customer**

[blocking] Status filter missing: This includes cancelled and pending orders
in revenue. Add `AND status = 'completed'` to the WHERE clause.
Result: current numbers overcount revenue.

[blocking] Hardcoded date '2024-01-01' will silently stop updating as data ages.
Use `{{ var('start_date', '2020-01-01') }}` or remove the filter entirely
if this is meant to be a full historical aggregate.

[suggestion] customer_id should have a not_null test — a NULL customer_id
would group all anonymous orders together. Add to schema.yml:
columns:
  - name: customer_id
    tests:
      - not_null

[question] Is the intention to include one row per customer ever seen,
or only customers with orders in 2024? If the former, a dim_customers
LEFT JOIN would be cleaner. If the latter, document it in the model
description so future readers understand the scope.

[nit] Consider NULLIF(COUNT(*), 0) for avg_order_value for safety,
even though GROUP BY prevents it currently — defensive coding.
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Code Review Process for 30 DE Contributors

**Scenario:** Your company has 30 data engineers contributing to a monorepo. Reviews are inconsistent — some PRs get 4 reviewers, some get none. Some merge in 10 minutes, some block for 2 weeks. Design a systematic code review process.

<details>
<summary>💡 Hint</summary>

The problem is lack of structure, not lack of reviewers. Fix with: CODEOWNERS for automatic routing, tiered review requirements (gold models need more eyes than staging models), PR size limits enforced in CI, response time SLAs (not enforced by a bot, but agreed by the team), and automated checks to handle the non-human work (syntax, style, secrets) so human reviewers focus on correctness and design.

</details>

<details>
<summary>✅ Solution</summary>

**Structure:**
```
Tier 1 (auto-approved if CI passes): Docs, test additions, chore
Tier 2 (1 reviewer from owning domain team): Feature model changes
Tier 3 (1 domain + 1 platform): Gold model changes, shared macros
Tier 4 (2 reviewers + architect): Schema contracts, breaking changes
```

**CODEOWNERS routing:**
```gitattributes
dbt/models/staging/    @domain-teams     # any domain team can review
dbt/models/gold/       @platform-de      # platform required
dbt/models/shared/     @data-architect   # architect required
airflow/dags/          @platform-de
```

**SLAs (team agreement, not enforced by bots):**
```
- First review response: within 1 business day
- Blocking review must include specific fix, not just "this is wrong"
- Author must respond to blocking comments within 1 business day
- PRs open > 5 days → auto-comment asking for status update
```

**Automated CI handles:**
```yaml
- dbt compile + dbt test
- Python linting (ruff)
- Secret scanning (detect-secrets)
- PR title format (conventional commits)
- PR size warning (> 500 lines)
```

**Review quality:**
```markdown
Team norm: reviews must say WHAT was checked.
Not acceptable: "LGTM"
Acceptable: "Reviewed JOIN logic and NULL handling. LGTM."
```

**Metrics tracked monthly:**
```
- Median time to first review
- Median time to merge
- % PRs with blocking comments
- % PRs merged without review (should be 0)
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What makes a good PR size and why does it matter?**
A: Under 400 lines is ideal. Large PRs get rubber-stamped because reviewers can't hold the full context. Small PRs are easier to reason about, faster to review, and easier to revert if they cause issues. If a feature is large, split it into sequential PRs each of which is deployable.

**Q: What is CODEOWNERS and how does it work?**
A: CODEOWNERS is a file in `.github/CODEOWNERS` that maps file paths to GitHub teams/users. When a PR touches a path covered by CODEOWNERS, GitHub automatically adds the listed owners as required reviewers. Combined with branch protection requiring CODEOWNERS approval, it enforces that every change is reviewed by the right people.

**Q: What is the difference between a blocking and a non-blocking review comment?**
A: A blocking comment (request for changes) must be addressed before the PR can merge. A non-blocking comment (suggestion/nit) is advisory — the author can choose to address it or leave it. Clearly labeling comments as [blocking], [suggestion], or [nit] prevents confusion about what must be done.

**Q: How do you handle a PR where the reviewer and author disagree on approach?**
A: Escalate to a team decision, not a war of attrition. Document both approaches and the trade-offs, bring it to a team sync or async discussion with a third opinion. The reviewer shouldn't block indefinitely on stylistic preference; the author shouldn't dismiss architectural concerns. Agree on criteria for the decision.

**Q: What automated checks should run on every PR before human review?**
A: Linting and formatting (ruff, flake8, sqlfluff), type checking (mypy), unit tests, dbt compile, secret scanning (detect-secrets), and PR title format validation. These handle objective correctness mechanically — human reviewers should focus on logic, data correctness, and design.

**Q: What is a squash merge and when is it appropriate?**
A: Squash merge combines all commits from a branch into one commit on main. It produces clean history (one commit = one feature) at the cost of losing intermediate commit detail. Appropriate for feature PRs; not appropriate for long-running branches where the commit history tells a story.

---

## 💼 Interview Tips

- Lead with the purpose of code review — not just bug-catching but knowledge sharing and standards enforcement. Senior interviewers want to hear you think about the team, not just the code.
- CODEOWNERS is a practical implementation detail most candidates don't mention — naming it specifically distinguishes you from generic answers about "requiring reviews."
- For data-specific reviews, mention NULL handling, JOIN types, and status filter correctness as the first checks — these are DE-specific bugs that pure software engineers miss.
- Frame PR size as a team health metric: small PRs = fast feedback = frequent deploys = less risk. Connect it to delivery philosophy, not just preference.
- For senior design questions, propose metrics (time to first review, % unreviewed) — it shows you manage review quality as a system, not just a practice.
- Avoid describing review as blocking productivity — frame it as reducing the cost of bugs (finding a bug in review is 10x cheaper than in production).
