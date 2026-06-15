---
title: "dbt Orchestration - Intermediate"
topic: dbt
subtopic: dbt-orchestration
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, orchestration, airflow, cosmos, dagster, slim-ci, defer]
---

# dbt Orchestration — Intermediate

## Airflow + Cosmos: Model-Level Tasks

[Cosmos](https://astronomer.github.io/astronomer-cosmos/) (by Astronomer) converts your dbt project into individual Airflow tasks — one per dbt model. This gives fine-grained retry, alerting, and monitoring.

```bash
pip install astronomer-cosmos
```

```python
# dags/dbt_cosmos.py
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime

profile_config = ProfileConfig(
    profile_name="default",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"database": "ANALYTICS", "schema": "DBT_PROD"},
    ),
)

dbt_dag = DbtDag(
    dag_id="dbt_project_dag",
    project_config=ProjectConfig("/opt/dbt/my_project"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Optional: only include specific models
    # select=["tag:daily"],
    # exclude=["tag:experimental"],
)
```

**What Cosmos creates:**
- Each dbt model → Airflow task
- dbt `ref()` dependencies → Airflow task dependencies
- Test tasks appended after each model task
- Retry/alert at model granularity

## Slim CI: `--select state:modified+`

### How State Comparison Works

dbt compares `manifest.json` files — one from production (the "state") and one from the current branch compile.

```bash
# CI workflow:
# 1. Download production manifest
dbt ls --target prod > /dev/null  # or copy from artifact storage
# Actually: download the manifest from dbt Cloud artifacts or S3

# 2. Compile current branch
dbt compile

# 3. Run only modified + downstream
dbt build \
  --select "state:modified+" \
  --state ./prod-manifest/    # directory containing production manifest.json
```

### State Selector Options

```bash
# Models that changed (code or config)
--select "state:modified"

# Changed models + their downstream dependents
--select "state:modified+"

# Changed models + all their ancestors (rarely needed)
--select "+state:modified"

# New models only (didn't exist in prod)
--select "state:new"

# Models where upstream sources changed
--select "state:modified.upstream_normalized"
```

### Fetching the Production Manifest

```bash
# From dbt Cloud via API
curl -L \
  -H "Authorization: Token ${DBT_API_TOKEN}" \
  "https://cloud.getdbt.com/api/v2/accounts/${ACCOUNT_ID}/jobs/${JOB_ID}/artifacts/manifest.json" \
  -o prod-manifest/manifest.json
```

## `--defer` to Production

`--defer` tells dbt: if an upstream model hasn't been built in the current environment, use the production version instead of failing.

```bash
dbt run \
  --select my_new_model \
  --defer \
  --state ./prod-manifest/
```

**Without `--defer`:** Running `my_new_model` in CI fails because `stg_orders` doesn't exist in the CI schema.

**With `--defer`:** dbt automatically substitutes the production `stg_orders` for any upstream models not present in CI. Only `my_new_model` is actually built.

### Combined Slim CI + Defer Pattern

```bash
dbt build \
  --select "state:modified+" \
  --defer \
  --state ./prod-manifest/
```

This is the canonical production slim CI pattern: build only changed models, defer upstream deps to prod.

## Dagster + dbt: Asset-Based Integration

Dagster treats dbt models as **software-defined assets**. Each model is an asset; the dependency graph is derived from `ref()` relationships.

```bash
pip install dagster-dbt
```

```python
# my_project/assets/dbt_assets.py
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

```python
# my_project/definitions.py
from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets.dbt_assets import my_dbt_assets, DBT_PROJECT_DIR

defs = Definitions(
    assets=[my_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
```

**Key Dagster + dbt advantages:**
- dbt models visible as assets in the Dagster UI
- Asset lineage traversal (what data produced this model?)
- Partitioned assets (run for specific date ranges)
- Easy integration with upstream ingestion (Fivetran, dlt) as Dagster assets

## dbt Mesh: Cross-Project References

dbt Mesh (dbt 1.6+) allows multiple dbt projects to share models via **cross-project refs**.

```yaml
# In project B's packages.yml or dependencies.yml
projects:
  - name: project_a
```

```sql
-- In project B, reference a model from project A
select * from {{ ref('project_a', 'fct_orders') }}
```

**Requirements:**
- Both projects must be registered in dbt Cloud (dbt Cloud feature)
- The upstream model must be declared as `public` access

```yaml
# In project_a/models/schema.yml
models:
  - name: fct_orders
    access: public   # Required for cross-project ref
    config:
      contract:
        enforced: true  # Recommended: enforce column contracts
```

**Access levels:**
- `private` (default) — only accessible within the same dbt project group
- `protected` — accessible across groups within the same project  
- `public` — accessible from other dbt projects

## Orchestration Comparison Matrix

| Feature | dbt Cloud | Airflow + BashOp | Airflow + Cosmos | Dagster |
|---|---|---|---|---|
| Setup complexity | Low | Low | Medium | Medium |
| Model-level visibility | No | No | Yes | Yes |
| Native dbt DAG | Yes | No | Yes | Yes |
| External dependencies | Limited | Yes (via DAG) | Yes | Yes |
| Cost | Paid | Self-hosted | Self-hosted | Self-hosted/Cloud |
| Slim CI support | Yes | Manual | Yes | Yes |
| Asset-based | No | No | No | Yes |

## Interview Questions

**Q: What is the difference between `state:modified` and `state:modified+`?**
A: `state:modified` runs only the changed models. `state:modified+` also runs all downstream dependents of those models, ensuring end-to-end consistency is tested.

**Q: How does `--defer` work in practice?**
A: dbt reads the production `manifest.json` to know what models exist in production. For any upstream dependency not built in the current environment, it substitutes the production relation. Your model builds against production upstream data, not a missing table.

**Q: What advantage does Cosmos provide over a simple BashOperator?**
A: Cosmos converts each dbt model into an individual Airflow task. This gives model-level retry, alerting, and success/failure visibility. With BashOperator, a single model failure fails the entire job with no visibility into which model was the problem.

**Q: What is dbt Mesh?**
A: dbt Mesh is a feature (dbt Cloud) enabling multiple dbt projects to reference each other's models via `ref('project_name', 'model_name')`. It supports domain-oriented data architecture where different teams own separate dbt projects that share data via published, versioned interfaces.
