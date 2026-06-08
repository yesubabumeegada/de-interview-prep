---
title: "Data Lineage — Intermediate"
topic: data-governance
subtopic: data-lineage
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-lineage, openlineage, marquez, spark, airflow-lineage]
---

# Data Lineage — Intermediate

## OpenLineage with Spark

Spark has native OpenLineage integration via the `openlineage-spark` library:

```python
# spark_session_with_lineage.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("orders_transform") \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "http") \
    .config("spark.openlineage.transport.url", "http://marquez:5000") \
    .config("spark.openlineage.namespace", "spark-prod") \
    .config("spark.openlineage.parentJobName", "orders_pipeline") \
    .config("spark.openlineage.parentRunId", "{{ dag_run.run_id }}") \
    .getOrCreate()

# All reads/writes are automatically tracked as lineage events
df = spark.read.table("bronze.orders_raw")               # → captured as INPUT
df_cleaned = df.filter(df.amount > 0).dropDuplicates(["order_id"])
df_cleaned.write.mode("overwrite").saveAsTable("silver.orders_cleaned")  # → captured as OUTPUT
```

---

## Airflow + OpenLineage

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from openlineage.airflow import DAG as OpenLineageDAG  # drop-in DAG replacement
from datetime import datetime

# Replace standard DAG with OpenLineageDAG for automatic lineage tracking
with OpenLineageDAG(
    "orders_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    # OpenLineage config read from environment:
    # OPENLINEAGE_URL=http://marquez:5000
    # OPENLINEAGE_NAMESPACE=airflow-prod
) as dag:
    
    def ingest_orders(**context):
        # Emit manual lineage event for non-SQL work
        from openlineage.client import OpenLineageClient
        from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
        
        client = OpenLineageClient.from_environment()
        
        client.emit(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=context["run_id"]),
            job=Job(namespace="airflow-prod", name="ingest_orders"),
            inputs=[Dataset(namespace="jdbc://source-db", name="orders")],
            outputs=[Dataset(namespace="snowflake://myaccount", name="BRONZE.ORDERS_RAW")],
        ))
    
    ingest = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders)
```

---

## Marquez — Lineage Backend

Marquez is an open-source OpenLineage-compatible metadata service:

```python
import requests

MARQUEZ_URL = "http://marquez:5000/api/v1"

def get_lineage_graph(namespace: str, job_name: str, depth: int = 3) -> dict:
    """Get the full lineage graph for a job."""
    resp = requests.get(
        f"{MARQUEZ_URL}/lineage",
        params={"nodeId": f"job:{namespace}:{job_name}", "depth": depth},
    )
    resp.raise_for_status()
    return resp.json()

def get_dataset_lineage(namespace: str, dataset_name: str) -> dict:
    """Get lineage for a specific dataset (upstream + downstream)."""
    resp = requests.get(
        f"{MARQUEZ_URL}/lineage",
        params={"nodeId": f"dataset:{namespace}:{dataset_name}", "depth": 5},
    )
    resp.raise_for_status()
    return resp.json()

def find_affected_datasets(changed_dataset: str, namespace: str = "snowflake://myaccount") -> list[str]:
    """Impact analysis: find all datasets downstream of a changed dataset."""
    graph = get_dataset_lineage(namespace, changed_dataset)
    
    affected = []
    for node in graph.get("graph", []):
        if node["type"] == "DATASET" and node["id"] != f"dataset:{namespace}:{changed_dataset}":
            affected.append(node["data"]["name"])
    
    return affected

# Example
affected = find_affected_datasets("SILVER.ORDERS_CLEANED")
print(f"Changing SILVER.ORDERS_CLEANED affects: {affected}")
# → ['GOLD.ORDERS', 'GOLD.REVENUE_DAILY', 'GOLD.CUSTOMER_LTV']
```

---

## Column-Level Lineage with dbt

dbt provides column-level lineage out of the box — parse it from the manifest:

```python
import json
from typing import Dict, List

def extract_column_lineage_from_dbt(manifest_path: str) -> Dict[str, List[dict]]:
    """
    Extract column-level lineage from dbt manifest.
    Returns: {target_column_fqn: [{source_column, source_model}]}
    """
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    column_lineage = {}
    
    for node_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        
        model_name = node["name"]
        
        # dbt exposes column-level depends_on in newer versions
        for col_name, col_meta in node.get("columns", {}).items():
            target_fqn = f"{model_name}.{col_name}"
            
            # Parse column lineage from meta (if teams set it)
            sources = col_meta.get("meta", {}).get("lineage_from", [])
            if sources:
                column_lineage[target_fqn] = sources
    
    return column_lineage

# More practical: query dbt's compiled SQL to find column origins
def find_column_in_upstream(model_name: str, column_name: str, manifest: dict) -> list[str]:
    """Find where a column is referenced in upstream models."""
    node = manifest["nodes"].get(f"model.project.{model_name}")
    if not node:
        return []
    
    depends_on_nodes = node.get("depends_on", {}).get("nodes", [])
    sources = []
    
    for upstream_id in depends_on_nodes:
        upstream = manifest["nodes"].get(upstream_id) or manifest["sources"].get(upstream_id)
        if upstream and column_name in upstream.get("columns", {}):
            sources.append(f"{upstream['schema']}.{upstream['name']}.{column_name}")
    
    return sources
```

---

## Lineage for Compliance: PII Flow Tracking

```python
import sqlalchemy as sa

def trace_pii_column_flow(engine, pii_table: str, pii_column: str) -> list[dict]:
    """
    Find all downstream tables that contain data derived from a PII column.
    Used for GDPR data mapping / DSAR (Data Subject Access Requests).
    """
    with engine.connect() as conn:
        # Recursive CTE: follow lineage edges where the column is referenced
        result = conn.execute(sa.text("""
            WITH RECURSIVE pii_flow AS (
                -- Start: PII column source
                SELECT 
                    :source_table AS table_name,
                    :source_col AS column_name,
                    0 AS depth,
                    :source_table AS path
                
                UNION ALL
                
                -- Traverse downstream
                SELECT
                    e.target_table,
                    cl.target_column,
                    pf.depth + 1,
                    pf.path || ' → ' || e.target_table
                FROM lineage_edges e
                JOIN column_lineage cl ON cl.source_table = e.source_table 
                    AND cl.source_column = pf.column_name
                    AND cl.target_table = e.target_table
                JOIN pii_flow pf ON pf.table_name = e.source_table
                WHERE pf.depth < 5
            )
            SELECT DISTINCT table_name, column_name, depth, path
            FROM pii_flow
            WHERE depth > 0
            ORDER BY depth, table_name
        """), {"source_table": pii_table, "source_col": pii_column}).fetchall()
    
    return [{"table": r.table_name, "column": r.column_name, "depth": r.depth, "path": r.path} for r in result]

# Example: trace where customer.email flows
pii_flow = trace_pii_column_flow(engine, "silver.customers", "email")
for item in pii_flow:
    print(f"[depth {item['depth']}] {item['table']}.{item['column']} via: {item['path']}")
```

---

## Interview Tips

> **Tip 1:** "How does Airflow integrate with OpenLineage?" — Replace the standard DAG import with `openlineage.airflow.DAG`. Set `OPENLINEAGE_URL` env var to your Marquez or DataHub endpoint. Airflow automatically emits START/COMPLETE/FAIL events with input/output datasets for each task. Manual emission needed for tasks that don't use standard SQL operators.

> **Tip 2:** "How do you implement column-level lineage in practice?" — Three approaches: (1) dbt: parses SQL refs and exposes column lineage in manifest. (2) SQL parsing: tools like SQLGlot or SQLFluff parse SELECT statements to extract column dependencies. (3) OpenLineage column facets: emit per-column lineage in your OpenLineage events. dbt is the easiest for most teams.

> **Tip 3:** "How do you use lineage for GDPR compliance?" — Trace PII column flows using the lineage graph. Identify all tables that contain derived PII. For a Data Subject Access Request, query all identified tables filtered by subject ID. For right-to-erasure, delete/mask in all tables at the source — upstream propagation via pipeline rerun.
