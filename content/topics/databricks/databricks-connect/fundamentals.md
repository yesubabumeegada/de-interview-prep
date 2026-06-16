---
title: "Databricks Connect v2: Local IDE Development Against Remote Clusters"
description: "Learn what Databricks Connect v2 is, how it differs from legacy v1, installation, authentication, and basic usage for local PySpark development"
content_type: study_material
topic: databricks
subtopic: databricks-connect
layer: fundamentals
difficulty_level: junior
tags: [databricks, databricks-connect, pyspark, local-development, ide, spark-connect, authentication]
---

# Databricks Connect v2: Fundamentals

## What Is Databricks Connect?

Databricks Connect is a client library that lets you run Apache Spark code from your local IDE — VS Code, JetBrains PyCharm/IntelliJ, Jupyter, or any Python environment — while the actual computation executes on a remote Databricks cluster. You write and debug code locally with full IDE support (autocomplete, breakpoints, linting), but Spark jobs run on your Databricks cluster in the cloud, with access to all cluster-mounted data and Delta tables.

Think of it as a thin client: your laptop sends the logical plan of your Spark transformations to the cluster over the network, the cluster computes results, and the results stream back to your local process.

---

## Databricks Connect v2 vs. Legacy v1

Databricks Connect has existed for a while, but v2 (introduced with Databricks Runtime 13.0 in 2023) is a complete rewrite with a fundamentally different architecture.

### Legacy v1 (DBR 5.x–12.x)
- Based on injecting JAR files and Py4J bridge into the local Python process
- Required matching exact Scala/Java versions between local env and cluster
- Notoriously fragile: version mismatches caused cryptic errors
- Large installation footprint (downloaded cluster JARs locally)
- Limited IDE integration; interactive debugging was difficult
- Did not support Databricks Runtime 13.0+

### Databricks Connect v2 (DBR 13.0+)
- Built on **Spark Connect**, the open-source Apache Spark project that standardizes remote Spark execution via gRPC
- Lightweight Python client — no JARs required locally, no Java/Scala version matching
- Uses **Apache Arrow** for efficient columnar data transfer back to the client
- Full IDE integration: local breakpoints work while code runs remotely
- Supports `databricks-connect` pip package that matches DBR version
- Cleaner authentication via PAT tokens, OAuth, or unified Databricks CLI config
- Smaller, more predictable set of supported APIs (some limitations compared to full cluster access)

**Rule of thumb:** If your cluster runs DBR 13.0 or later, use v2. Legacy v1 is deprecated and unsupported on modern runtimes.

---

## Supported Environments

Databricks Connect v2 works anywhere Python runs:

| Environment | Integration Quality |
|---|---|
| VS Code + Python extension | Excellent — full breakpoint debugging |
| JetBrains PyCharm / IntelliJ | Excellent — full debugging |
| Jupyter Notebooks (local) | Good — great for exploration |
| Jupyter in VS Code | Excellent |
| Command line / pytest | Good — ideal for CI/CD |
| Any Python 3.8+ environment | Works |

VS Code with the Databricks extension provides the richest experience: it can automatically configure the connection to your workspace, manage cluster selection, and integrate with Databricks Asset Bundles.

---

## Cluster Requirements

Not every cluster type supports Databricks Connect v2. The requirements are:

### Cluster Requirements Checklist
- **Databricks Runtime (DBR) 13.0 or higher** — v2 does not work with earlier runtimes
- **Cluster access mode must be "Single User" or "Shared"** — "No Isolation Shared" clusters are not supported
- **Python version** — cluster Python version must match your local Python version (major.minor must match, e.g., both Python 3.10)
- **Cluster must be running** — Connect cannot start a terminated cluster automatically (though you can use the Databricks CLI or REST API to start it before connecting)
- **Databricks Connect package version must match DBR** — `databricks-connect==13.3.*` for DBR 13.3, for example

### Cluster Access Modes
- **Single User** — cluster is dedicated to one user; full feature support
- **Shared** — cluster shared across users with process isolation; supported but with some restrictions (no arbitrary UDFs with native code, no `%run`, no SparkContext)

---

## Installation

Installation is a single pip command, but version pinning matters.

```bash
# Install matching your DBR version
# For DBR 13.3
pip install databricks-connect==13.3.*

# For DBR 14.3 LTS
pip install databricks-connect==14.3.*

# For DBR 15.4 LTS (latest LTS as of 2024)
pip install databricks-connect==15.4.*

# Always recommended: install in a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate  # Windows
pip install databricks-connect==14.3.*
```

### Important: Uninstall pyspark First
`databricks-connect` includes its own PySpark distribution. If you have a standalone `pyspark` package installed, it will conflict.

```bash
# Remove conflicting package if present
pip uninstall pyspark

# Then install databricks-connect
pip install databricks-connect==14.3.*
```

### Verify Installation
```bash
databricks-connect --version
# Should print: Databricks Connect 14.3.x
```

---

## Authentication

Databricks Connect needs to know your workspace URL and credentials. There are three main approaches.

### Option 1: Databricks CLI Configuration (Recommended)

Install the Databricks CLI and run `databricks configure`:

```bash
# Install Databricks CLI
pip install databricks-cli
# OR use the newer standalone CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configure default profile
databricks configure

# You will be prompted:
# Databricks Host: https://your-workspace.azuredatabricks.net
# Token: dapi...your-PAT-token...
```

This writes credentials to `~/.databrickscfg`:

```ini
[DEFAULT]
host = https://your-workspace.azuredatabricks.net
token = dapi1234567890abcdef
```

Databricks Connect automatically reads from `~/.databrickscfg` when you create a SparkSession.

### Option 2: Environment Variables

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi1234567890abcdef
export DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh
```

### Option 3: Personal Access Tokens (PAT)

Generate a PAT in your Databricks workspace:
1. Click your username in the top-right → **User Settings**
2. Go to **Developer** → **Access tokens**
3. Click **Generate new token**, give it a name and optional expiry
4. Copy the token (starts with `dapi`) — you only see it once

PAT tokens are the simplest approach for personal development. For CI/CD, use OAuth service principals instead (covered in senior-deep-dive).

### Option 4: OAuth (Recommended for Teams)

```bash
# Login with OAuth (browser-based, no token required)
databricks auth login --host https://your-workspace.azuredatabricks.net
```

This opens a browser for SSO login and caches OAuth credentials locally. Tokens refresh automatically.

---

## Basic Usage: Creating a SparkSession

With Databricks Connect v2, you use `DatabricksSession` instead of `SparkSession.builder`:

```python
from databricks.connect import DatabricksSession

# Simplest approach — reads from ~/.databrickscfg or environment variables
spark = DatabricksSession.builder.getOrCreate()

# Explicit cluster ID
spark = DatabricksSession.builder.clusterId("0123-456789-abcdefgh").getOrCreate()

# Explicit config
from databricks.sdk.config import Config

config = Config(
    host="https://your-workspace.azuredatabricks.net",
    token="dapi1234567890abcdef",
    cluster_id="0123-456789-abcdefgh"
)
spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
```

### Running a Simple DataFrame Operation

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Read a Delta table from Unity Catalog
df = spark.read.table("main.sales.transactions")

# Run a transformation — this executes on the remote cluster
result = df.filter("amount > 1000").groupBy("region").sum("amount")

# .show() triggers execution and brings results to your local process
result.show(10)

# Collect to a Pandas DataFrame locally
import pandas as pd
pandas_df = result.toPandas()
print(pandas_df.head())
```

When you call `.show()` or `.collect()` or `.toPandas()`, the Spark plan is sent to the cluster, executed there, and results are streamed back via Arrow format. Your local machine never loads the full dataset — only the result rows you request.

### Reading CSV and Writing Delta

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Read from DBFS or cloud storage accessible to the cluster
df = spark.read.csv("dbfs:/mnt/raw/customers.csv", header=True, inferSchema=True)

# Transform
cleaned = df.dropna(subset=["customer_id", "email"]) \
            .withColumnRenamed("cust_id", "customer_id")

# Write back to Delta (runs on cluster)
cleaned.write.format("delta").mode("overwrite").saveAsTable("main.staging.customers_clean")

print("Write complete")
```

---

## Limitations vs Full Cluster Access

Databricks Connect v2 is powerful but does not support all Spark APIs. Understanding limitations prevents frustrating errors.

### What Does NOT Work with Databricks Connect v2

| Feature | Status | Reason |
|---|---|---|
| `SparkContext` (sc) APIs | Not supported | SC-level APIs not available via Spark Connect protocol |
| UDFs with native code (Pandas UDFs with C extensions) | Limited | Native code must be on cluster |
| Spark Streaming (`readStream`) | Not supported | Streaming not yet in Spark Connect protocol |
| `dbutils.notebook.run()` | Not supported | Notebook orchestration not available via Connect |
| `%run` magic | Not supported | Notebook-only feature |
| `spark.sql()` with `CREATE TABLE` DDL | Partially supported | Some DDL works, complex DDL may not |
| RDD APIs | Not supported | RDD layer below DataFrame API |
| `MLlib` (some parts) | Partially supported | DataFrame-based ML works; RDD-based ML does not |
| `display()` function | Not supported | Databricks notebook-only |

### What DOES Work

- All DataFrame/Dataset transformations and actions
- SQL queries via `spark.sql("SELECT ...")`
- Reading/writing Delta tables, Parquet, CSV, JSON, ORC
- Delta Lake operations (MERGE, DELETE, UPDATE via SQL)
- Unity Catalog table access
- `spark.catalog` operations (list databases, tables)
- `dbutils.fs` (list, copy files on DBFS) — available via `RemoteDbUtils`
- Structured UDFs (Python UDFs defined in Python, without native code extensions)

---

## dbutils with Databricks Connect

`dbutils` is not automatically available. Use `RemoteDbUtils`:

```python
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils  # SDK dbutils

spark = DatabricksSession.builder.getOrCreate()

# File system operations
files = dbutils.fs.ls("dbfs:/mnt/raw/")
for f in files:
    print(f.name, f.size)

# Secrets (read from Databricks Secrets)
secret_value = dbutils.secrets.get(scope="my-scope", key="my-key")
```

Note: `dbutils.widgets` and `dbutils.notebook` are not available via Connect.

---

## When to Use Databricks Connect vs. Alternatives

Choosing the right tool depends on your situation:

| Scenario | Best Tool |
|---|---|
| Developing and testing PySpark logic in VS Code with breakpoints | **Databricks Connect v2** |
| Quick ad-hoc data exploration and visualization | **Databricks Notebooks** |
| Running automated unit tests that don't need a real cluster | **Local PySpark** (with Delta Standalone for Delta reads) |
| Running integration tests against real data | **Databricks Connect v2** |
| Deploying production jobs | **Databricks Workflows / Jobs** |
| Learning Spark basics, no Databricks subscription | **Local PySpark** |
| Real-time streaming development | **Databricks Notebooks** (Connect lacks streaming) |

### Connect Is Best For:
- Engineers who prefer IDE-based development over notebooks
- Teams that want version-controlled Python files (`.py`) instead of notebooks (`.ipynb`)
- Writing testable, modular code with pytest
- Debugging complex transformations step-by-step with breakpoints

### Notebooks Are Better When:
- You need interactive visualization (`display()`, Databricks charts)
- You're doing streaming development
- You need to use `%run` to chain notebooks
- The task is exploratory and iterative (notebooks' inline output is convenient)

---

## Quick Reference: Common Commands

```bash
# Install
pip install databricks-connect==14.3.*

# Configure
databricks configure

# Test connection
python -c "from databricks.connect import DatabricksSession; spark = DatabricksSession.builder.getOrCreate(); print(spark.range(10).count())"
```

```python
# Minimal working script
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
print(spark.sql("SELECT current_timestamp()").collect())
```

---

## Summary

- Databricks Connect v2 uses gRPC + Arrow for lightweight, reliable remote Spark execution from local IDEs
- Requires DBR 13.0+, matching Python versions, and Single User or Shared cluster access mode
- Install with `pip install databricks-connect==<dbr-version>.*`, authenticate via `databricks configure`
- Use `DatabricksSession.builder.getOrCreate()` — not `SparkSession.builder`
- Limitations: no streaming, no SparkContext/RDD, no native UDFs, no `display()`
- Best for IDE-based development, testable Python files, and integration testing against real cluster data
