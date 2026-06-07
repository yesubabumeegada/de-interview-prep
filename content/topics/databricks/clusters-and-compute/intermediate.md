---
title: "Clusters and Compute - Intermediate"
topic: databricks
subtopic: clusters-and-compute
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, clusters, compute, optimization, policies, sizing, photon]
---

# Clusters and Compute — Intermediate

## Cluster Sizing Methodology

### Right-Sizing Based on Workload

```python
def recommend_cluster_size(data_size_gb: float, complexity: str, sla_minutes: int) -> dict:
    """Recommend cluster configuration based on workload characteristics."""
    
    # Rule of thumb: 1 worker can process ~50-100 GB/hour for standard ETL
    # Complex joins/aggregations: ~20-50 GB/hour per worker
    # Simple reads/writes: ~100-200 GB/hour per worker
    
    processing_rates = {"simple": 150, "moderate": 75, "complex": 30}  # GB/hr/worker
    rate = processing_rates[complexity]
    
    # Workers needed to meet SLA
    hours_available = sla_minutes / 60
    workers_needed = max(2, int(data_size_gb / (rate * hours_available)))
    
    # Instance type selection
    if complexity == "complex":  # Shuffle-heavy
        instance = "i3.xlarge"  # Local SSD for spill
    elif data_size_gb > 500:  # Large data, memory-intensive
        instance = "r5.2xlarge"
    else:
        instance = "m5.xlarge"  # General purpose
    
    return {
        "min_workers": max(2, workers_needed // 2),  # Start smaller, autoscale
        "max_workers": workers_needed + 2,  # Headroom for spikes
        "instance_type": instance,
        "driver": "r5.xlarge" if workers_needed > 8 else instance,
    }

# Examples:
# 100 GB, moderate complexity, 30 min SLA → 3 workers, m5.xlarge
# 1 TB, complex joins, 60 min SLA → 34 workers, i3.xlarge
# 50 GB, simple, 15 min SLA → 2 workers, m5.xlarge
```

### Observing and Adjusting

```python
# After running the job, check Spark UI metrics:

# 1. Task duration distribution (Stages tab)
# If 95% of tasks finish in 2 min but 5% take 20 min → data skew
# Fix: salting, broadcast, more partitions

# 2. Shuffle spill (Stages tab → shuffle write/spill)
# If spill > 0 → not enough memory for shuffle
# Fix: more workers (distribute data) or i3 instances (fast local SSD)

# 3. Executor GC time (Executors tab)
# If GC > 10% of task time → memory pressure
# Fix: more memory (r5 instances) or more workers (less data per executor)

# 4. CPU utilization (cluster metrics)
# If < 50% average → over-provisioned (reduce workers)
# If > 90% sustained → under-provisioned (add workers or use c5)
```

---

## Cluster Policies

Cluster policies let admins control what users can configure:

```python
# Platform admin creates policy:
POLICY = {
    "name": "standard-etl-policy",
    "definition": {
        # Restrict instance types (no expensive GPU instances)
        "node_type_id": {
            "type": "allowlist",
            "values": ["i3.xlarge", "i3.2xlarge", "m5.xlarge", "m5.2xlarge", "r5.xlarge"],
        },
        # Limit cluster size
        "autoscale.max_workers": {
            "type": "range",
            "maxValue": 16,
        },
        # Force spot instances
        "aws_attributes.availability": {
            "type": "fixed",
            "value": "SPOT_WITH_FALLBACK",
        },
        # Force auto-termination
        "autotermination_minutes": {
            "type": "range",
            "minValue": 10,
            "maxValue": 120,
            "defaultValue": 30,
        },
        # Required tags
        "custom_tags.team": {
            "type": "fixed",
            "value": "",  # User must provide
        },
        # Force Photon
        "runtime_engine": {
            "type": "fixed",
            "value": "PHOTON",
        },
    },
}

# Benefits:
# - Users can't accidentally create expensive clusters
# - Cost governance (max workers limited, spot enforced)
# - Standardization (everyone uses approved instance types)
# - Auto-termination guaranteed (no forgotten running clusters)
```

---

## Photon Engine

Photon is Databricks' vectorized query engine written in C++ (replaces JVM-based Spark SQL execution):

```python
# Enable Photon:
"spark_version": "14.3.x-photon-scala2.12"
# OR in cluster config: "runtime_engine": "PHOTON"

# What Photon accelerates:
# - Scan (reading Parquet/Delta files): 2-3x faster
# - Filter (WHERE clauses): 3-5x faster
# - Aggregation (GROUP BY, SUM, COUNT): 2-4x faster
# - Join (hash joins): 2-3x faster
# - Sort: 2-3x faster

# What Photon does NOT accelerate:
# - Python UDFs (still runs in Python, not C++)
# - Complex Spark ML operations
# - RDD operations (only DataFrame/SQL)
# - Custom Java/Scala code
```

```sql
-- Check if Photon is being used for your query:
EXPLAIN EXTENDED SELECT ... ;
-- Look for "PhotonExec" in the plan (indicates Photon is active)

-- Photon is most beneficial for:
-- 1. Large table scans with filters (data warehouse queries)
-- 2. Heavy aggregations (GROUP BY with many groups)
-- 3. Wide tables with many columns (columnar vectorized reads)
-- 4. Delta Lake operations (MERGE, UPDATE, DELETE)
```

---

## Serverless Compute

Databricks Serverless removes cluster management entirely:

```python
# Serverless SQL Warehouse: no cluster config needed
# Just pick a size (Small, Medium, Large, etc.)
# Databricks manages: instance selection, scaling, optimization

# Serverless for Jobs (preview):
# No job cluster config needed — Databricks picks optimal resources
{
    "task_key": "transform",
    "notebook_task": {"notebook_path": "/pipelines/transform"},
    "environment_key": "default",  # Serverless environment
}

# Benefits of serverless:
# - Zero startup time (cluster always warm, shared across customers)
# - No instance type decisions (Databricks optimizes automatically)
# - Pay-per-use (no idle time cost)
# - Auto-scales without configuration

# Trade-offs:
# - Higher per-DBU cost (premium for zero management)
# - Less control (can't tune Spark configs)
# - Network isolation limitations (shared infrastructure)
```

---

## Multi-Cluster Architecture

```python
# Production architecture: different clusters for different workloads

COMPUTE_ARCHITECTURE = {
    "etl_clusters": {
        "purpose": "Batch ETL pipelines (Workflows)",
        "type": "Job clusters",
        "instance": "i3.xlarge",
        "scaling": "Autoscale 4-16",
        "spot": True,
        "runtime": "Photon",
        "cost_per_hour": "~$3.50",
    },
    "streaming_cluster": {
        "purpose": "Continuous streaming (Auto Loader, DLT)",
        "type": "Job cluster (long-running)",
        "instance": "m5.xlarge",
        "scaling": "Fixed 4 workers",
        "spot": False,  # Stability for streaming
        "cost_per_hour": "~$2.80",
    },
    "sql_warehouses": {
        "purpose": "BI queries, dashboards, ad-hoc SQL",
        "type": "SQL Warehouse (Serverless)",
        "sizes": {"analysts": "Small", "dashboards": "Medium"},
        "auto_stop": "10 min",
        "cost": "Per-query (serverless)",
    },
    "interactive": {
        "purpose": "Development, exploration",
        "type": "All-purpose (shared)",
        "instance": "m5.xlarge",
        "scaling": "Autoscale 1-4",
        "auto_terminate": "30 min",
        "policy": "standard-dev-policy",
    },
}
```

---

## Init Scripts and Libraries

```python
# Install libraries at cluster startup:

# Method 1: Cluster libraries (UI or API)
# Install PyPI, Maven, or DBFS-hosted packages
"libraries": [
    {"pypi": {"package": "great-expectations==0.18.0"}},
    {"pypi": {"package": "slack-sdk==3.27.0"}},
    {"maven": {"coordinates": "io.delta:delta-core_2.12:2.4.0"}},
]

# Method 2: Init scripts (run shell commands at startup)
"init_scripts": [
    {"dbfs": {"destination": "dbfs:/init-scripts/install_tools.sh"}},
]

# install_tools.sh:
#!/bin/bash
pip install great-expectations==0.18.0
apt-get install -y jq  # System packages

# Method 3: requirements.txt in Repos
# If notebook is in a Repo, dependencies from requirements.txt auto-install

# Best practices:
# - Pin versions (great-expectations==0.18.0, not great-expectations)
# - Use cluster libraries for production (faster, cached)
# - Use init scripts for system-level installs (apt-get, OS tools)
# - Test library compatibility before updating in production
```

---

## Spark Configuration Tuning

```python
# Common Spark configurations for Databricks:

SPARK_CONF = {
    # Shuffle partitions (default: 200)
    "spark.sql.shuffle.partitions": "auto",  # Databricks auto-tunes!
    
    # Delta optimizations
    "spark.databricks.delta.optimizeWrite.enabled": "true",  # Coalesce small files
    "spark.databricks.delta.autoCompact.enabled": "true",    # Auto-OPTIMIZE
    
    # Memory tuning
    "spark.executor.memory": "8g",          # Per-executor memory
    "spark.executor.memoryOverhead": "2g",  # Off-heap memory (Python, I/O buffers)
    
    # Parallelism
    "spark.default.parallelism": "auto",    # Based on cluster size
    "spark.sql.files.maxPartitionBytes": "128m",  # Max size per partition when reading
    
    # Adaptive Query Execution (always enable)
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Broadcast join threshold
    "spark.sql.autoBroadcastJoinThreshold": "100m",  # Broadcast tables up to 100 MB
}
```

---

## Interview Tips

> **Tip 1:** "How do you right-size a cluster?" — Start with the workload: data size, complexity (joins vs simple transforms), and SLA. Rule of thumb: 50-100 GB/hour per worker for moderate ETL. Run the job, check Spark UI (spill? GC? idle workers?). Adjust: more workers for parallelism, larger instances for memory, i3 for shuffle-heavy.

> **Tip 2:** "What's the difference between Photon and standard runtime?" — Photon is a C++ vectorized engine that replaces JVM execution for SQL/DataFrame operations. It's 2-5x faster for scans, filters, aggregations, and joins. It does NOT help with Python UDFs or RDD operations. Use Photon for all production workloads (only slight cost premium via DBU rate).

> **Tip 3:** "How do cluster policies help with cost governance?" — Policies restrict what users can configure: instance type allowlists, max worker limits, forced spot instances, required auto-termination, and mandatory tags for cost tracking. Platform team sets policies, users create clusters within those boundaries. Prevents: forgotten running clusters, oversized configs, expensive instance types.
