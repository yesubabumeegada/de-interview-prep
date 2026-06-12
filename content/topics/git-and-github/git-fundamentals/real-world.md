---
title: "Git Fundamentals - Real World"
topic: git-and-github
subtopic: git-fundamentals
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [git, github, real-world, data-engineering, incident]
---

# Git Fundamentals — Real World

## Case Study: The Schema Change That Broke Production

### Background

A data engineering team at a fintech company managed 80+ dbt models in a shared Git repository. The team of 12 engineers all committed directly to `main` with no branch protection. Hotfixes and feature work were mixed in the same branch.

### The Incident

A senior engineer renamed a column `customer_id` → `client_id` across 3 dbt models to match a new naming convention. They tested locally with `dbt run`, it passed, and they pushed to main. Within 20 minutes:

- 6 downstream Airflow DAGs failed with `KeyError: 'customer_id'`
- 3 Looker dashboards showed errors
- Finance's daily close report failed to generate

The change was valid individually but broke everything that depended on the old column name downstream.

**Root cause:** No branch protection. No PR review. No CI that tested downstream impact.

---

### The Fix: Git Workflow Overhaul

**Step 1: Branch protection (GitHub settings)**
```
main branch rules:
✓ Require pull request before merging
✓ Require 1 approving review
✓ Require status checks to pass:
  - dbt-compile-and-test
  - downstream-impact-check
✓ Require branches to be up to date
✓ Do not allow bypassing (including admins)
```

**Step 2: Standardized branching**
```bash
# Naming convention enforced by pre-push hook
# feat/<ticket>-<description>
# fix/<ticket>-<description>
# chore/<description>

git checkout -b feat/DE-423-rename-customer-id-column
# Work, commit, push
git push origin feat/DE-423-rename-customer-id-column
# Open PR → triggers CI → requires review → merge
```

**Step 3: CI impact check added**
```yaml
# .github/workflows/dbt-check.yml
- name: Check downstream impact
  run: |
    # List changed models
    CHANGED=$(git diff origin/main --name-only | grep "models/" | sed 's|.sql||')
    
    # Find all downstream dependencies
    dbt ls --select "$CHANGED+" --output name > downstream.txt
    
    echo "Models affected by this PR:"
    cat downstream.txt
    
    # Fail if >10 downstream models affected (requires senior review)
    COUNT=$(wc -l < downstream.txt)
    if [ $COUNT -gt 10 ]; then
      echo "::error::$COUNT downstream models affected. Add @data-platform to review."
      exit 1
    fi
```

---

### Results After 3 Months

| Metric | Before | After |
|---|---|---|
| Production incidents from schema changes | 3/month | 0/month |
| Average PR review time | N/A (none) | 45 minutes |
| Downstream impact visible before merge | Never | Always |
| Time to revert a bad deploy | 30-60 min | `git revert` in 5 min |

**Key takeaway:** Git is not just version control — it's an audit trail, a coordination mechanism, and a deployment gate. For data teams, the PR process is where data contracts get enforced.
