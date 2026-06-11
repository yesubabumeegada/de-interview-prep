---
title: "GitOps for Data - Fundamentals"
topic: git-and-github
subtopic: gitops-for-data
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [git, github, gitops-for-data]
---

# GitOps for Data — Fundamentals

## ▶️ Try It Yourself

```yaml
# GitOps: merge to main = deploy to production (no manual steps)
# .github/workflows/gitops_dbt.yml

name: dbt GitOps
on:
  push:
    branches: [main]
    paths: [models/**, seeds/**, macros/**]

jobs:
  dbt-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup dbt
        run: pip install dbt-snowflake
      - name: dbt deps
        run: dbt deps
      - name: dbt run (changed models only)
        run: dbt build --select state:modified+ --defer --state ./manifest
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SF_ACCOUNT }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SF_PASSWORD }}
      - name: Update production manifest
        run: aws s3 cp target/manifest.json s3://dbt-artifacts/prod/manifest.json
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
