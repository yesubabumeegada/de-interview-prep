---
title: "dbt Cloud & CI/CD - Scenarios"
topic: dbt
subtopic: dbt-cloud-and-ci-cd
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, cicd, deployment, interview, scenarios]
---

# dbt Cloud & CI/CD — Scenario Questions

## Scenario 1 (Junior): CI Takes 45 Minutes

**Situation:** Every PR triggers a CI pipeline that runs `dbt build` on all 150 models. It takes 45 minutes, slowing down the team. How do you fix it?

**Answer:**

**Current (slow):**
```bash
dbt build  # Runs all 150 models every PR
```

**Solution: Slim CI — only build what changed**

```bash
# Step 1: Store prod manifest in S3 after each production run
aws s3 cp target/manifest.json s3://my-bucket/prod/manifest.json

# Step 2: Download prod manifest in CI
aws s3 cp s3://my-bucket/prod/manifest.json ./prod-state/

# Step 3: Only build modified models + downstream
dbt build \
  --select state:modified+ \
  --defer \            # Read unchanged upstream from prod
  --state ./prod-state
```

**Result:** If a PR changes 3 models, only those 3 + their downstream are built and tested. CI drops from 45 min → 2-5 min.

**Additional optimizations:**
- Cache `dbt_packages/` in GitHub Actions
- Skip CI for documentation-only changes
- Run tests in parallel with `--threads 8`

---

## Scenario 2 (Mid-Level): Production Job Fails at 5am

**Situation:** You're on-call. The 5am production dbt job fails. The executive dashboard shows no data for today. You have 2 hours before business opens. Walk through your response.

**Answer:**

**0:00 — Alert received:** PagerDuty fires

**0:02 — Check run results:**
```bash
# dbt Cloud: check job run logs
# Look for: error model name, error message
# Common errors: warehouse timeout, source table missing, schema drift
```

**0:05 — Identify root cause:**
```
Error: Object 'RAW_DB.shopify.orders' does not exist
```
→ Fivetran connector failed overnight, table is missing

**0:07 — Check Fivetran:**
- Fivetran dashboard → connector shows "Broken"
- Error: Shopify API rate limit hit at 2am

**0:10 — Trigger Fivetran re-sync:**
```bash
# Trigger via Fivetran API
curl -X POST https://api.fivetran.com/v1/connectors/shopify_prod/force \
  -H "Authorization: Bearer $FIVETRAN_KEY"
```

**0:15 — Communicate:**
```
Slack #data-ops: "5am dbt job failed — Shopify data delayed ~1hr. 
Root cause: Fivetran rate limit. ETA for fix: 6:30am. 
Dashboard will be current by 7am."
```

**0:45 — Fivetran sync completes**

**0:50 — Rerun failed dbt job manually:**
```bash
# Only run models that depend on Shopify source
dbt build --select source:shopify+ --target prod
```

**1:10 — Verify + close:**
```bash
dbt test --select tag:smoke_test
# ✅ All tests pass
```

Post-incident: Add Fivetran sync check as first step in dbt job. Add `dbt source freshness` before `dbt build`.

---

## Scenario 3 (Senior): Design CI/CD Strategy for dbt Mesh

**Situation:** You're migrating from a 300-model monolithic dbt project to a dbt Mesh with 5 separate projects (platform, finance, marketing, operations, data-science). Design the CI/CD strategy.

**Answer:**

**Architecture:**
```
platform_project (foundation — shared dims/facts)
  ├── finance_project (depends on platform)
  ├── marketing_project (depends on platform)
  ├── operations_project (depends on platform)
  └── data_science_project (depends on platform + finance)
```

**CI/CD Rules:**

**Rule 1: Each project has independent CI**
```yaml
# platform_project/.github/workflows/ci.yml
# finance_project/.github/workflows/ci.yml
# etc. — each repo has its own workflow
```

**Rule 2: Platform project changes trigger downstream validation**
```yaml
# When platform_project deploys to prod:
on-deploy:
  - trigger: finance_project CI (against new platform artifacts)
  - trigger: marketing_project CI
  - trigger: operations_project CI
  # Only deploy downstream if all pass
```

**Rule 3: Cross-project testing**
```bash
# finance_project CI uses latest platform prod artifacts
aws s3 cp s3://artifacts/platform-project/prod/manifest.json ./upstream-state/
dbt build --select state:modified+ --defer --state ./upstream-state
```

**Rule 4: Deployment order matters**
```
Deploy sequence (Airflow DAG):
  1. dbt snapshot (all projects)
  2. platform_project build + test
  3. (parallel) finance, marketing, operations build + test
  4. data_science_project build + test
  5. All-projects smoke tests
  6. Notify stakeholders
```

**Rule 5: Rollback is per-project**
```bash
# If finance_project fails, only roll back finance — platform stays deployed
./rollback.sh finance_project
# Platform data is still good — only finance marts rolled back
```
