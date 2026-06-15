---
title: "dbt Orchestration - Scenario Questions"
topic: dbt
subtopic: dbt-orchestration
content_type: scenario_question
tags: [dbt, orchestration, scenarios, interview]
---

# dbt Orchestration — Scenario Questions

<article data-difficulty="junior">

## Scenario: Setting Up a dbt Cloud Job

Your team just migrated to dbt Cloud. You need to schedule the full dbt project to run every day at 6 AM UTC. The job should also test all models after running and generate fresh docs.

**What steps do you take in dbt Cloud to configure this?**

<details>
<summary>✅ Solution</summary>

**In dbt Cloud UI: Jobs → New Job**

**Configuration:**

1. **Job Name:** `Production Daily Build`

2. **Environment:** Select your production environment (linked to prod warehouse credentials)

3. **Commands (in order):**
   ```
   dbt source freshness
   dbt build
   dbt docs generate
   ```
   Note: `dbt build` runs both `dbt run` and `dbt test` together. Alternatively use `dbt run` then `dbt test` as separate commands.

4. **Schedule:**
   - Enable: ✓
   - Cron: `0 6 * * *` (6 AM UTC daily)

5. **Advanced Settings:**
   - Generate docs on run: ✓ (or include `dbt docs generate` as a command)
   - Run source freshness: ✓

6. **Notifications:**
   - Add email/Slack for failure alerts

**What `dbt build` does:** Runs models, snapshots, seeds, and tests in DAG order. If a test fails, downstream models that depend on the tested model are blocked (controlled by `+on_test_failure`).

**Verify the job ran:** Jobs → Run History → inspect logs per command.

**Important:** Always set `--target prod` or configure the environment to point to the production target so models materialize in the correct schema.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Implementing Slim CI in GitHub Actions

Your team's dbt CI runs `dbt build` on every PR — it rebuilds everything and takes 45 minutes. PRs are waiting hours for CI to pass. Implement slim CI that only builds changed models.

**Write the GitHub Actions workflow. What are the edge cases to handle?**

<details>
<summary>✅ Solution</summary>

```yaml
# .github/workflows/dbt_slim_ci.yml
name: dbt Slim CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-slim-ci:
    runs-on: ubuntu-latest
    env:
      DBT_TARGET: ci
      SNOWFLAKE_SCHEMA: dbt_ci_pr${{ github.event.pull_request.number }}

    steps:
      - uses: actions/checkout@v3

      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.2

      - name: dbt deps
        run: dbt deps

      - name: Download production manifest
        env:
          DBT_API_TOKEN: ${{ secrets.DBT_API_TOKEN }}
          DBT_ACCOUNT_ID: ${{ secrets.DBT_ACCOUNT_ID }}
          DBT_JOB_ID: ${{ secrets.DBT_PROD_JOB_ID }}
        run: |
          mkdir -p prod_state
          curl -fL \
            -H "Authorization: Token ${DBT_API_TOKEN}" \
            "https://cloud.getdbt.com/api/v2/accounts/${DBT_ACCOUNT_ID}/jobs/${DBT_JOB_ID}/artifacts/manifest.json" \
            -o prod_state/manifest.json

      - name: dbt build (slim CI with defer)
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          dbt build \
            --select "state:modified+" \
            --defer \
            --state prod_state/ \
            --target ci

      - name: Cleanup CI schema
        if: always()   # Run even if build fails
        run: dbt run-operation drop_schema --args '{"schema_name": "${{ env.SNOWFLAKE_SCHEMA }}"}'
```

**Edge cases to handle:**

1. **New project (no prod manifest):** If the manifest download fails (first run ever), fall back to `dbt build` without state selection:
   ```bash
   curl ... -o prod_state/manifest.json || echo "No prod manifest — running full build"
   if [ -f prod_state/manifest.json ]; then
     dbt build --select "state:modified+" --defer --state prod_state/
   else
     dbt build
   fi
   ```

2. **Macro-only changes:** `state:modified` detects macro changes if they affect compiled model SQL. Always use `state:modified+` to catch downstream impact.

3. **`dbt_project.yml` changes:** Global config changes may affect all models. In that case, fall back to full build:
   ```bash
   if git diff --name-only origin/main...HEAD | grep -q 'dbt_project.yml'; then
     echo "dbt_project.yml changed — running full build"
     dbt build
   else
     dbt build --select "state:modified+" --defer --state prod_state/
   fi
   ```

4. **packages.yml changes:** `dbt deps` may pull in new macros. Always run `dbt deps` before state comparison.

5. **Concurrent PRs:** Use unique schema per PR number to avoid conflicts (`dbt_ci_pr${PR_NUMBER}`).

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Orchestration Architecture for a Growing Data Team

A 30-person company is growing to 80. Currently they have a single dbt project with 300 models run nightly via a BashOperator in Airflow. Problems: CI takes 60 minutes, ownership of models is unclear, the marketing team and finance team step on each other in PRs, and when any model fails the whole pipeline fails.

**Design a new orchestration architecture. Justify your choices.**

<details>
<summary>✅ Solution</summary>

**Diagnosis of current problems:**

| Problem | Root Cause |
|---|---|
| 60-min CI | Full rebuild every PR; no slim CI |
| Unclear ownership | Single project, no team-level structure |
| PR conflicts | All teams editing same repo |
| Cascade failures | BashOperator — one failure fails everything |

---

**Proposed Architecture:**

**1. Split into domain projects (dbt Mesh)**

```
platform/     ← Core entities owned by platform team
marketing/    ← Owned by marketing analysts  
finance/      ← Owned by finance analysts
product/      ← Owned by product analysts
```

Each project has its own GitHub repo, CI/CD pipeline, and dbt Cloud environment. Teams release independently.

**2. Migrate Airflow to Cosmos (model-level tasks)**

```python
# Each domain gets its own DAG
marketing_dag = DbtDag(
    dag_id="marketing_daily",
    render_config=RenderConfig(select=["tag:marketing"]),
    schedule_interval="0 7 * * *",
)
```

Benefits: model-level failures, retry individual models, clear visibility per team.

**3. Implement slim CI per project**

```yaml
# Each project's CI workflow
dbt build \
  --select "state:modified+" \
  --defer \
  --state prod_state/
```

CI time drops from 60 minutes to 3-8 minutes for small PRs.

**4. Coordinate across projects via Airflow task dependencies**

```python
# Master orchestration DAG
platform_dag_run >> marketing_dag_run
platform_dag_run >> finance_dag_run
platform_dag_run >> product_dag_run
```

The platform project runs first (source of truth). Domain projects run in parallel after it.

**5. Failure handling**

- Cosmos: model-level Slack alerts with exact failing model + log link
- `store_failures: true` on key tests so failed rows are persisted
- Dead letter process: failed models write to `dbt_failures` schema for investigation

**6. Migration path (to avoid big bang)**

```
Week 1-2: Add Cosmos to existing single-project Airflow DAG (immediate model-level visibility)
Week 3-4: Add slim CI (immediate CI time improvement)
Month 2: Extract platform models into separate repo
Month 3-4: Extract domain projects one by one
```

**Tradeoffs acknowledged:**
- dbt Mesh requires dbt Cloud (cost); alternative is Airflow with separate project runs and shared artifact storage
- More repos = more DevOps overhead; mitigate with cookiecutter templates
- Cross-project `ref()` adds coupling; govern via public model contracts

</details>
</article>
