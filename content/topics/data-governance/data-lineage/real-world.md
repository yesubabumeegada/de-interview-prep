---
title: "Data Lineage — Real World"
topic: data-governance
subtopic: data-lineage
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-lineage, openlineage, marquez, impact-analysis, production]
---

# Data Lineage — Real World Patterns

## Pattern 1: Lineage-Driven Safe Deprecation

Before deleting a table, use lineage to check all consumers:

```python
import requests
import sqlalchemy as sa
from datetime import datetime, timedelta

def safe_deprecate_table(
    table_name: str,
    marquez_url: str,
    engine,
    target_delete_date: str,
) -> dict:
    """
    Safely deprecate a table by:
    1. Checking downstream lineage (who uses it)
    2. Checking recent query logs (is it actively used)
    3. Notifying owners of all downstream assets
    4. Marking as deprecated in the catalog
    """
    
    report = {"table": table_name, "safe_to_delete": False, "blockers": []}
    
    # Step 1: Check lineage for downstream dependencies
    lineage_resp = requests.get(
        f"{marquez_url}/api/v1/lineage",
        params={"nodeId": f"dataset:snowflake://prod:{table_name}", "depth": 5}
    )
    graph = lineage_resp.json().get("graph", [])
    
    downstream_tables = [
        node["data"]["name"]
        for node in graph
        if node["type"] == "DATASET" and node["data"]["name"] != table_name
    ]
    
    downstream_dashboards = [
        node["data"]["title"]
        for node in graph
        if node["type"] == "DASHBOARD"
    ]
    
    if downstream_tables or downstream_dashboards:
        report["blockers"].append({
            "type": "active_downstream",
            "downstream_tables": downstream_tables,
            "downstream_dashboards": downstream_dashboards,
        })
    
    # Step 2: Check recent query activity
    with engine.connect() as conn:
        recent_queries = conn.execute(sa.text("""
            SELECT COUNT(DISTINCT user_email) AS unique_users, COUNT(*) AS query_count
            FROM query_logs
            WHERE table_name = :table
              AND queried_at >= NOW() - INTERVAL '30 days'
        """), {"table": table_name}).fetchone()
    
    if recent_queries.query_count > 0:
        report["blockers"].append({
            "type": "recent_queries",
            "query_count": recent_queries.query_count,
            "unique_users": recent_queries.unique_users,
            "message": f"Table queried {recent_queries.query_count}x by {recent_queries.unique_users} users in past 30 days",
        })
    
    report["safe_to_delete"] = len(report["blockers"]) == 0
    report["downstream_count"] = len(downstream_tables) + len(downstream_dashboards)
    report["target_delete_date"] = target_delete_date
    
    return report


# Usage
result = safe_deprecate_table(
    "legacy.orders_v1",
    "http://marquez:5000",
    engine,
    "2024-03-01",
)

print(f"Safe to delete: {result['safe_to_delete']}")
if result["blockers"]:
    print(f"Blockers: {result['blockers']}")
```

---

## Pattern 2: Lineage-Aware Incident Impact Analysis

When an incident occurs, use lineage to scope impact instantly:

```python
def analyze_incident_impact(
    failing_table: str,
    lineage_store,  # Neo4j or Marquez client
    catalog,        # DataHub or similar
) -> dict:
    """
    Called at incident start: instantly determine what's affected.
    Used to notify the right teams and set correct severity.
    """
    
    downstream = lineage_store.get_impact(failing_table, max_hops=5)
    
    # Classify by asset type
    affected_tables = [d for d in downstream if "." in d["dataset"]]
    affected_dashboards = [d for d in downstream if d["dataset"].endswith("-dashboard")]
    
    # Get owners for affected tables
    owners_to_notify = set()
    for asset in affected_tables:
        owner = catalog.get_owner(asset["dataset"])
        if owner:
            owners_to_notify.add(owner)
    
    # Estimate severity
    p1_tables = [t for t in affected_tables if catalog.get_tag(t["dataset"], "tier") == "tier-1"]
    
    severity = "P1" if p1_tables else ("P2" if affected_dashboards else "P3")
    
    return {
        "failing_table": failing_table,
        "severity": severity,
        "affected_tables": len(affected_tables),
        "affected_dashboards": len(affected_dashboards),
        "owners_to_notify": list(owners_to_notify),
        "p1_tables_affected": [t["dataset"] for t in p1_tables],
        "full_impact": downstream,
    }

# Used in Airflow on_failure_callback
def on_failure_with_lineage(context):
    failing_table = context["task_instance"].xcom_pull("output_table")
    impact = analyze_incident_impact(failing_table, lineage_store, catalog)
    
    message = (
        f":fire: *INCIDENT* — `{failing_table}` pipeline failed\n"
        f"*Severity:* {impact['severity']}\n"
        f"*Affected:* {impact['affected_tables']} tables, {impact['affected_dashboards']} dashboards\n"
        f"*Notify:* {', '.join(impact['owners_to_notify'])}"
    )
    slack.post("#data-incidents", message)
```

---

## Pattern 3: Lineage Validation in CI/CD

Ensure new pipelines correctly emit lineage before merging:

```python
# tests/test_lineage.py — runs in CI against staging environment
import pytest
import requests
import time

MARQUEZ_URL = "http://marquez-staging:5000/api/v1"

def wait_for_lineage_event(dataset_name: str, timeout_seconds: int = 60) -> bool:
    """Poll Marquez until the expected lineage event appears."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp = requests.get(f"{MARQUEZ_URL}/datasets/{dataset_name}")
        if resp.status_code == 200:
            return True
        time.sleep(5)
    return False

def test_orders_pipeline_lineage():
    """After pipeline run, verify lineage was emitted to Marquez."""
    
    # Trigger pipeline
    import subprocess
    subprocess.run(["python", "pipelines/orders_transform.py", "--date", "2024-01-15"], check=True)
    
    # Wait for lineage events to appear in Marquez
    assert wait_for_lineage_event("SILVER.ORDERS_CLEANED"), "No lineage event for SILVER.ORDERS_CLEANED"
    
    # Verify lineage edges
    resp = requests.get(
        f"{MARQUEZ_URL}/lineage",
        params={"nodeId": "dataset:snowflake://staging:SILVER.ORDERS_CLEANED", "depth": 2}
    )
    graph = resp.json()["graph"]
    node_names = {n["data"].get("name") for n in graph}
    
    assert "BRONZE.ORDERS_RAW" in node_names, "Expected upstream lineage to BRONZE.ORDERS_RAW"
```

---

## Lineage Gotchas

| Gotcha | Impact | Fix |
|---|---|---|
| Spark dynamic partition pruning confuses lineage | Wrong input tables captured | Use explicit `spark.openlineage.dataset.namespace` overrides |
| dbt ephemeral models don't appear in lineage | Hidden joins/transforms | Document ephemeral model logic as column-level lineage in schema.yml |
| Table renames break existing lineage edges | Orphaned nodes in graph | Re-emit lineage for all downstream jobs after rename |
| Cross-schema views don't emit lineage | Lineage gaps at view boundaries | Add explicit lineage events when creating views |
| Lineage lag (events arrive after dashboards load) | Stale lineage during incident | Buffer 5-10 min; don't make real-time decisions on lineage freshness |
