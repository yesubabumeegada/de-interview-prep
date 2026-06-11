---
title: "Pull Requests and Code Review - Fundamentals"
topic: git-and-github
subtopic: pull-requests-and-code-review
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [git, github, pull-requests, code-review]
---

# Pull Requests and Code Review — Fundamentals


## 🎯 Analogy

Think of pull requests like a peer review for code: they create a checkpoint where a second engineer verifies correctness, checks for security issues, and confirms tests pass — before anything reaches the main branch (and production).

---
## The Peer Review Analogy

A pull request is like submitting a research paper for peer review before publication. You don't publish scientific findings without other experts checking your methods, logic, and conclusions. Code review is the same: a second set of eyes catches bugs you can't see (you're too close to your own code), ensures the approach is sound, shares knowledge across the team, and maintains code quality standards. The PR is the mechanism that makes peer review systematic and tracked.

---

## Anatomy of a Good PR

```markdown
## What Changed
- Added daily revenue aggregation model `fct_revenue_daily`
- Refactored extract function to handle null order_ids
- Added unit tests for the revenue calculation logic

## Why
Ticket DE-423: Finance team needs daily revenue by channel for their dashboard.

## Testing
- [ ] dbt compile passes
- [ ] dbt test passes on dev target
- [ ] Unit tests pass (`pytest tests/test_revenue.py`)
- [ ] Verified output matches expected row counts

## Screenshots / Outputs
| date       | channel  | revenue  |
| 2024-01-01 | organic  | 45,230   |
| 2024-01-01 | paid     | 12,450   |

## Rollback Plan
Revert this PR — `fct_revenue_daily` is new, no downstream breakage.
```

---

## PR Best Practices

```bash
# Keep PRs small (< 400 lines is ideal)
# One logical change per PR
# Don't mix refactoring with feature work

# Good PR titles (conventional commits)
feat(revenue): add daily revenue aggregation by channel
fix(orders): handle null order_id in extract function
test(revenue): add unit tests for revenue calculation
chore: upgrade dbt from 1.6 to 1.7

# Bad PR titles
"fixes"
"WIP"
"updates"
```

---

## Code Review Checklist for DE

```markdown
## Reviewer Checklist

**Correctness**
- [ ] Does the logic match the stated requirement?
- [ ] Are edge cases handled? (nulls, empty datasets, duplicates)
- [ ] Is the SQL filter correct? (check WHERE clauses carefully)

**Data Quality**
- [ ] Are there dbt tests for not_null, unique, accepted_values?
- [ ] Does this break any downstream models?

**Performance**
- [ ] No full table scans without WHERE clause
- [ ] No unnecessary cross joins
- [ ] Appropriate use of partitioning/clustering

**Security**
- [ ] No hardcoded credentials
- [ ] No PII in logs or debug output

**Maintainability**
- [ ] Clear model/function names
- [ ] Comments for non-obvious logic only
- [ ] Tests added for new behavior
```

---

## Giving Feedback Well

```markdown
# Types of review comments:

# ❌ Unhelpful — vague, no direction
"This is wrong"
"I don't like this approach"

# ✅ Helpful — specific, actionable
"This WHERE clause will silently exclude orders with NULL status.
Add: `WHERE status IS NOT NULL AND status = 'completed'`
or handle nulls explicitly with COALESCE."

# Prefix your comment type:
[blocking]: This will cause a production bug — must fix before merge
[suggestion]: Consider using COALESCE here for clarity
[nit]: Minor style preference — can ignore
[question]: Why did you choose UNION over UNION ALL here?
```

---

## GitHub PR Features

```bash
# Request specific reviewers
gh pr create --reviewer @senior-de,@data-platform-team

# Add to a specific team for review
# GitHub: Settings → Teams → CODEOWNERS file

# Draft PR (not ready for review)
gh pr create --draft --title "WIP: revenue v2"

# Convert to ready
gh pr ready

# Check CI status
gh pr checks

# Merge strategies:
# Merge commit: preserves all commits (useful for audit trails)
# Squash and merge: one commit per feature (clean history)
# Rebase and merge: linear history, no merge commit
```

## ▶️ Try It Yourself

```bash
# Open a PR with the GitHub CLI
gh pr create   --title "feat: add incremental load to orders pipeline"   --body "## Changes
- Added watermark-based incremental loading
- Added tests for edge cases (empty batch, late data)
- Updated README with new env vars

## Testing
- Unit tests: pytest tests/ -v
- dbt test: dbt test --select orders_pipeline+"   --assignee @me   --label "data-pipeline"

# Review a PR
gh pr review 42 --approve --body "LGTM — good test coverage"
gh pr review 42 --request-changes --body "Please add test for null amounts"

# Check PR status
gh pr status
gh pr view 42

# Merge after approval
gh pr merge 42 --squash --delete-branch
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
