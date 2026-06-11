---
title: "Monorepo Patterns - Intermediate"
topic: git-and-github
subtopic: monorepo-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, github, monorepo-patterns]
---

# Monorepo Patterns — Intermediate

```python
# Selective CI: detect which services changed
import subprocess

def get_changed_services(base_branch: str = "origin/main") -> list[str]:
    result = subprocess.run(
        ["git", "diff", base_branch, "--name-only"],
        capture_output=True, text=True
    )
    changed_files = result.stdout.strip().split("
")
    
    services = set()
    for file in changed_files:
        if file.startswith("dbt/"):
            services.add("dbt")
        elif file.startswith("pipelines/revenue/"):
            services.add("revenue-pipeline")
        elif file.startswith("airflow/dags/"):
            services.add("airflow")
    
    return list(services)

# In CI: only test what changed
changed = get_changed_services()
if "dbt" in changed:
    run_dbt_tests()
if "revenue-pipeline" in changed:
    run_pipeline_tests()
```

## Shared Library Management

```toml
# pyproject.toml — workspace dependencies
[tool.uv.workspace]
members = ["shared", "pipelines/*"]

# shared/pyproject.toml
[project]
name = "de-shared"

# pipelines/revenue/pyproject.toml
[project]
dependencies = [
    "de-shared",   # local workspace dependency
    "pandas>=2.0",
]
```
