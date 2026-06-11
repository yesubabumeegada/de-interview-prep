---
title: "GitOps for Data - Intermediate"
topic: git-and-github
subtopic: gitops-for-data
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, github, gitops-for-data]
---

# GitOps for Data — Intermediate

```python
# Audit trail: every pipeline change traceable to a PR
# Git blame shows who changed what and why

import subprocess

def get_last_change_info(file_path: str) -> dict:
    """Get git info for the last change to a file."""
    result = subprocess.run(
        ["git", "log", "-1", "--format=%H|%an|%ae|%s|%ai", "--", file_path],
        capture_output=True, text=True
    )
    sha, author, email, message, date = result.stdout.strip().split("|")
    return {"sha": sha, "author": author, "email": email, "message": message, "date": date}
```

## Environment Promotion via GitOps

```
main branch → auto-deploys to staging
Release tag v2024.02.01 → triggers production deploy
```

```yaml
# Promote to production on tag
on:
  push:
    tags: ['v*']

jobs:
  deploy-prod:
    environment: production  # requires manual approval
    steps:
      - run: dbt run --target prod
      - run: dbt test --target prod
```

## Drift Detection

```bash
# Detect if production dbt state matches what's in git
dbt source freshness --target prod  # check source freshness
dbt ls --target prod > current_models.txt
dbt ls --select '*' > expected_models.txt
diff current_models.txt expected_models.txt  # any drift?
```
