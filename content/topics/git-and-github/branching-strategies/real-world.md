---
title: "Branching Strategies - Real World"
topic: git-and-github
subtopic: branching-strategies
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [git, branching, real-world, data-engineering, gitflow]
---

# Branching Strategies — Real World

## Case Study: Migrating from Gitflow to Trunk-Based

### Background

A 12-person DE team at a healthcare analytics company used Gitflow. They had `main`, `develop`, and quarterly `release` branches. In theory this gave them stability. In practice it created pain.

### The Problem

**Merge hell every sprint end:** Feature branches 3-4 weeks long. Merging into develop took an entire day resolving conflicts from 5 engineers' simultaneous changes to overlapping dbt models.

**"Develop branch" diverged from production:** The develop branch had 6 weeks of untested feature work that had never run in staging. Nobody knew which parts actually worked.

**Release branch drift:** By the time a quarterly release was prepared, main and develop had diverged by 200+ commits. The release branch merge was a week-long manual effort.

### The Migration

**Phase 1: Shorten branch lifetimes (no tooling changes)**
```
Policy change: "No feature branch lives longer than 3 days."
If a feature takes longer → break it into smaller mergeable pieces.
Use feature flags to merge code without enabling it in prod.
```

**Phase 2: Add CI on every PR**
```yaml
# If CI catches it → merge safely → shorten feedback loop
on: pull_request
jobs:
  dbt-compile: ...  # catches broken models immediately
  dag-import: ...   # catches Airflow syntax errors
```

**Phase 3: Delete develop branch**
```bash
# Sunset the develop branch over 4 weeks
# Week 1: main IS the integration branch — all PRs go to main
# Week 2: staging deploys from main automatically
# Week 3: monitor for 2 weeks — zero incidents
# Week 4: delete develop branch
git push origin --delete develop
```

**Phase 4: Feature flags for long-running work**
```python
# models/config.py
ENABLE_NEW_PATIENT_COHORT_V2 = os.getenv("FF_COHORT_V2", "false") == "true"

# Work merged to main for 3 weeks, disabled by default
# Enable in staging: FF_COHORT_V2=true
# Enable in prod when ready
```

### Results

| Metric | Gitflow | Trunk-Based |
|---|---|---|
| Sprint-end merge time | 6-8 hours | 0 (no sprint-end merges) |
| Longest branch lifetime | 4 weeks | 2 days |
| Conflicts per PR | 8-12 | 1-2 |
| Time from code → staging | 2-3 weeks | Same day |
| Production incidents | 2-3/quarter | 1/quarter |
| Engineer satisfaction | Low | High |

### What They Kept from Gitflow

- Signed tags for every production release (`git tag -a v2024-Q2 -m "Q2 release"`)
- A `release-notes.md` updated in each PR description
- The hotfix branch pattern (still needed for urgent patches)

**Key insight:** They didn't need Gitflow to have release discipline. They got it from tagged deployments + feature flags + mandatory PR review. Gitflow added ceremony without adding safety.
