---
title: "Databricks Connect Interview Scenarios"
description: "Hands-on scenarios for local Databricks development with Databricks Connect v2"
content_type: scenario_question
topic: databricks
subtopic: databricks-connect
tags: [databricks, databricks-connect, local-development, pyspark, ide]
---

# Databricks Connect Interview Scenarios

<article data-difficulty="junior">

## Scenario: Setting Up Databricks Connect for a New Engineer

**Context:** You're a senior data engineer onboarding a new team member. They've been assigned their first task: write PySpark transformations to clean and aggregate raw customer event data. They're comfortable with Python and VS Code but have only used Databricks notebooks before. You want to set them up with Databricks Connect so they can develop locally with breakpoints and version-controlled `.py` files.

**Your task:** Walk through the complete setup process for getting this new engineer up and running with Databricks Connect v2 in VS Code.

**Constraints:**
- The workspace runs Databricks Runtime 14.3 LTS
- The engineer's laptop runs Python 3.10
- They have a Databricks account with access to a Single User cluster
- They should be able to run a simple DataFrame operation against a Delta table in the dev catalog

**Discussion points:**
1. What are the installation steps, in order?
2. What cluster requirements must be verified before connecting?
3. How do you configure authentication? What's the simplest approach for personal development?
4. What's the first test to verify the connection works?
5. What limitations should you warn them about upfront?

<details>
<summary>✅ Solution</summary>

### Step 1: Verify Cluster Requirements

Before installing anything, check the cluster:
- Go to Compute in the Databricks UI
- Confirm the cluster runs **DBR 14.3** (not an older version)
- Confirm the cluster **access mode is "Single User"** (not "No Isolation Shared")
- Note the **cluster ID** (visible in the cluster URL: `...#/compute/clusters/0123-456789-abcdef`)
- Check the **Python version** on the cluster: go to cluster → Libraries or Configuration → see Python version (should be 3.10 for DBR 14.3)

### Step 2: Install databricks-connect

```bash
# Create a virtual environment (avoids polluting global Python)
python -m venv .venv
source .venv/bin/activate       # Mac/Linux
# .venv\Scripts\activate        # Windows

# IMPORTANT: remove pyspark if installed (conflicts with databricks-connect)
pip uninstall pyspark -y

# Install databricks-connect matching the DBR version
pip install databricks-connect==14.3.*

# Verify
databricks-connect --version
# Output: Databricks Connect 14.3.x
```

### Step 3: Configure Authentication

The simplest approach for personal development: PAT token + `databricks configure`.

1. In Databricks UI: click username top-right → User Settings → Developer → Access Tokens → Generate New Token
2. Name it "local-dev", set expiry to 90 days, copy the token (starts with `dapi`)

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure (enter host and token when prompted)
databricks configure
# Databricks Host: https://your-workspace.azuredatabricks.net
# Token: dapi...
```

Verify authentication:
```bash
databricks clusters list
# Should print the list of clusters including their cluster IDs
```

### Step 4: Write a Test Script

```python
# test_connect.py
import os
os.environ["DATABRICKS_CLUSTER_ID"] = "0123-456789-abcdef"   # Their cluster ID

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Simple test — should return 10
count = spark.range(10).count()
print(f"Connection successful! range(10).count() = {count}")

# Test Delta table access
df = spark.read.table("dev_catalog.default.sample_events")
df.printSchema()
df.show(5)
```

Run it:
```bash
python test_connect.py
# Should print: Connection successful! range(10).count() = 10
```

### Step 5: Set Up VS Code Debugging

1. Install VS Code extension: **Databricks** (publisher: Databricks)
2. Create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Debug with Databricks Connect",
      "type": "debugpy",
      "request": "launch",
      "program": "${file}",
      "env": {
        "DATABRICKS_CLUSTER_ID": "0123-456789-abcdef"
      }
    }
  ]
}
```

3. Set a breakpoint in `test_connect.py` and press F5 — the breakpoint should hit.

### Step 6: Warn About Limitations

Tell the new engineer what doesn't work:
- **No `display()`**: use `.show()` instead
- **No streaming** (`readStream`): use notebooks for streaming development
- **No SparkContext** (`sc.`): avoid SC-level APIs
- **No `dbutils.notebook.run()`**: orchestrate from Databricks Workflows instead
- **Python versions must match**: if the cluster is updated to a new Python version, update local too
- **Cluster must be running**: Connect cannot auto-start a terminated cluster

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Designing an Isolated Local Dev Workflow for 10 Engineers

**Context:** Your team has grown to 10 data engineers, all working on the same Databricks workspace. Currently, everyone develops in shared notebooks on a single shared all-purpose cluster. You're experiencing constant conflicts:
- Engineers accidentally overwrite each other's notebook changes
- Long-running jobs from one engineer slow down another's interactive queries
- It's hard to tell which version of the code is "current"
- Testing requires running entire notebooks manually

**Your task:** Design a local development workflow using Databricks Connect that gives each engineer full isolation while sharing a common Unity Catalog Delta Lake.

**Requirements:**
- Each engineer should be able to develop independently without interfering with others
- Code should be version-controlled in Git with readable diffs
- Engineers should be able to run tests before pushing changes
- The shared Delta Lake (Unity Catalog) should be accessible from all environments
- Cost should not increase dramatically vs. the current single shared cluster

**Discussion points:**
1. How do you structure clusters for 10 engineers?
2. How do you enforce cost controls and prevent runaway clusters?
3. How do you handle schema/table isolation during development?
4. What does the testing strategy look like?
5. What is the migration plan from shared notebooks?

<details>
<summary>✅ Solution</summary>

### Cluster Strategy: Per-Engineer Dev Clusters

Give each engineer their own dedicated dev cluster with strict cost controls enforced via a cluster policy.

**Create a cluster policy** (Databricks Admin → Compute → Cluster Policies):

```json
{
  "cluster_type": { "type": "fixed", "value": "all-purpose" },
  "autotermination_minutes": {
    "type": "fixed",
    "value": 60,
    "hidden": false
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2"],
    "defaultValue": "Standard_DS3_v2"
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 2,
    "defaultValue": 1
  },
  "spark_version": {
    "type": "fixed",
    "value": "14.3.x-scala2.12"
  },
  "custom_tags.team": { "type": "fixed", "value": "data-engineering" },
  "custom_tags.purpose": { "type": "fixed", "value": "dev" }
}
```

Key controls:
- **60-minute auto-termination**: cluster shuts down after 1 hour of inactivity → no idle billing
- **Max 2 workers**: prevents accidental large cluster creation
- **Fixed DBR version**: ensures Connect compatibility for everyone
- **Tags**: enables cost attribution per engineer

With 10 engineers and 60-min auto-termination, realistically only 3-5 clusters are active at any given time. Cost is similar to the previous always-on shared cluster.

### Schema Isolation via Personal Dev Schemas

Each engineer works in their own schema to avoid conflicts:

```python
# Convention: dev_catalog.<engineer_username>
# e.g., dev_catalog.alice, dev_catalog.bob

import os
MY_SCHEMA = f"dev_catalog.{os.environ.get('DATABRICKS_SCHEMA', os.environ.get('USER', 'dev'))}"
```

Create personal schemas once per engineer:
```sql
-- Run once per engineer in the dev catalog
CREATE SCHEMA IF NOT EXISTS dev_catalog.alice;
GRANT ALL PRIVILEGES ON SCHEMA dev_catalog.alice TO `alice@company.com`;
```

Engineers read from shared tables (read-only in `prod_catalog` or `main`), but write their outputs to their personal schema:

```python
# Read shared source data (read-only)
df = spark.read.table("main.raw.transactions")

# Write to personal schema
result.write.format("delta").mode("overwrite") \
      .saveAsTable(f"{MY_SCHEMA}.transactions_cleaned")
```

### Project Structure with Connect

```
data-engineering/
├── databricks.yml
├── pyproject.toml
├── src/
│   └── pipelines/
│       ├── clean_transactions.py
│       └── aggregate_revenue.py
├── tests/
│   ├── unit/
│   │   └── test_clean_transactions.py   # Local SparkSession
│   └── integration/
│       └── test_clean_transactions_int.py  # Connect
└── .env.template                         # Template for engineers to copy
```

```bash
# .env.template (copy to .env and fill in)
DATABRICKS_CONFIG_PROFILE=dev
DATABRICKS_CLUSTER_ID=<your-cluster-id-here>
DATABRICKS_SCHEMA=<your-username>
TEST_CATALOG=dev_catalog
```

### Testing Strategy

**Layer 1 — Unit tests (local, fast, no cluster):**

```python
# tests/unit/test_clean_transactions.py
from pyspark.sql import SparkSession
import pytest
from pipelines.clean_transactions import clean_df  # Pure transformation function

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

def test_removes_nulls(spark):
    data = [(1, None, 100.0), (2, "C2", 200.0)]
    df = spark.createDataFrame(data, ["id", "customer_id", "amount"])
    result = clean_df(df)
    assert result.count() == 1
```

**Layer 2 — Integration tests (Connect, real cluster):**

```python
# tests/integration/test_clean_transactions_int.py
import pytest, os
from databricks.connect import DatabricksSession
from pipelines.clean_transactions import clean_df

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()

def test_against_real_data(spark):
    schema = os.environ.get("DATABRICKS_SCHEMA", "dev")
    df = spark.read.table(f"dev_catalog.{schema}.test_transactions")
    result = clean_df(df)
    assert result.count() > 0
    assert result.filter("customer_id IS NULL").count() == 0
```

**Running tests:**
```bash
# Fast — run all the time
pytest tests/unit/ -v

# Before pushing — verify against real cluster
pytest tests/integration/ -v
```

### Migration Plan from Notebooks

1. **Week 1**: Set up project structure and dev environment for 2 volunteer engineers
2. **Week 2**: Migrate one non-critical pipeline to `.py` files; document the process
3. **Week 3-4**: Run a "setup day" — pair with each engineer to set up their local environment
4. **Ongoing**: New pipelines developed as `.py` files; existing notebooks migrated when they need changes
5. **Keep notebooks** for ad-hoc exploration and visualization (don't force-migrate everything)

### Cost Analysis

Old approach: 1 shared cluster, 8 workers, running 10 hours/day = 80 DBU-hours/day

New approach: 10 dev clusters, 1-2 workers each, avg 3 hours/day active (auto-terminate) = 30-60 DBU-hours/day

Result: lower or equal cost with better isolation.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Complete CI/CD Pipeline with Connect, DABs, and GitHub Actions

**Context:** Your team is building a production data platform on Databricks. You need to design a complete CI/CD pipeline that:
- Runs unit tests on every PR (fast, no cluster)
- Runs integration tests using Databricks Connect (real cluster, real data)
- Deploys to staging using Databricks Asset Bundles after merging to main
- Deploys to production after staging validation passes
- Handles authentication securely (no PAT tokens in CI)
- Manages cluster lifecycle in CI to control costs
- Provides test isolation so parallel CI runs don't interfere

**Your task:** Design the complete architecture and walk through the key implementation decisions.

**Constraints:**
- GitHub is the SCM; GitHub Actions for CI/CD
- Two Databricks workspaces: `dev` (for CI integration tests) and `prod` (for staging/prod deploy)
- Unity Catalog in both workspaces
- Service principal already exists in Azure AD
- Budget: CI integration tests should cost <$10/day
- Test runs should be isolated from each other

**Discussion points:**
1. How do you handle authentication for CI without PAT tokens?
2. How do you manage the CI cluster lifecycle (start, test, stop)?
3. How do you isolate parallel CI runs from each other?
4. What does the DABs deploy pipeline look like for staging/prod?
5. What are the cost controls?
6. How do you handle test failures — do you still clean up?

<details>
<summary>✅ Solution</summary>

### Authentication Architecture

**Never use PAT tokens for CI.** Use **OAuth Machine-to-Machine (M2M)** with a service principal.

**Setup (one-time):**
1. Create Service Principal in Azure AD: `databricks-ci-sp`
2. Add to Databricks dev workspace: Admin → Service Principals → Add → `databricks-ci-sp`
3. Grant permissions:
   - Cluster: "Can Attach To" on the CI cluster
   - Unity Catalog: USE CATALOG, USE SCHEMA, CREATE TABLE on `ci_catalog`
   - Secrets (if tests use secrets): READ on relevant secret scopes

4. Store in GitHub repository secrets:
   ```
   DATABRICKS_DEV_HOST      = https://dev-workspace.azuredatabricks.net
   DATABRICKS_PROD_HOST     = https://prod-workspace.azuredatabricks.net
   DATABRICKS_SP_CLIENT_ID  = <service-principal-app-id>
   DATABRICKS_SP_SECRET     = <service-principal-client-secret>
   DATABRICKS_CI_CLUSTER_ID = 0123-456789-ci-cluster
   ```

**Why OAuth M2M over PAT:**
- PAT tokens have a fixed expiry; long CI runs (>token-lifetime) fail mid-run
- OAuth tokens auto-refresh every hour transparently via the Databricks SDK
- Service principal identity enables fine-grained audit logging
- Secrets rotation is managed at the identity provider, not individually per token

### Cluster Lifecycle Management

**Option A: Persistent CI Cluster (simpler, slightly higher cost)**

Create a dedicated CI cluster with auto-termination set to 30 minutes. The CI pipeline starts it if stopped, runs tests, and lets auto-termination handle shutdown.

```python
# scripts/ensure_cluster_running.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State
import time, argparse, os

def ensure_running(cluster_id: str, timeout: int = 600) -> None:
    w = WorkspaceClient()
    state = w.clusters.get(cluster_id).state
    
    if state == State.RUNNING:
        return
    if state in (State.TERMINATED, State.TERMINATING):
        if state == State.TERMINATING:
            w.clusters.wait_get_cluster_terminated(cluster_id)
        w.clusters.start(cluster_id)
    
    deadline = time.time() + timeout
    while time.time() < deadline:
        if w.clusters.get(cluster_id).state == State.RUNNING:
            print("Cluster running")
            return
        time.sleep(15)
    raise TimeoutError("Cluster did not start in time")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--cluster-id", required=True)
    ensure_running(p.parse_args().cluster_id)
```

**Option B: Job Cluster per CI Run (cleanest isolation, lower idle cost)**

Each CI run creates a job cluster, runs tests, cluster auto-terminates on job completion.

```python
# scripts/run_integration_tests_as_job.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowRequest, PythonWheelTask
import os, sys

def run_tests_as_job():
    w = WorkspaceClient()
    run = w.jobs.run_now(
        job_id=int(os.environ["DATABRICKS_CI_JOB_ID"]),
        python_params=["--run-id", os.environ["GITHUB_RUN_ID"]]
    )
    result = run.result()
    if result.state.result_state.value != "SUCCESS":
        print(f"Tests failed: {result.state.state_message}")
        sys.exit(1)
    print("Tests passed")

if __name__ == "__main__":
    run_tests_as_job()
```

Recommended: Option A for simplicity; Option B for cost-critical or high-PR-volume scenarios.

### Test Isolation for Parallel CI Runs

The key: use a **unique schema per CI run** derived from `GITHUB_RUN_ID`.

```python
# tests/conftest.py
import pytest, os
from databricks.connect import DatabricksSession
from databricks.sdk.config import Config

@pytest.fixture(scope="session")
def spark():
    config = Config(
        host=os.environ["DATABRICKS_HOST"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
        cluster_id=os.environ["DATABRICKS_CLUSTER_ID"]
    )
    return DatabricksSession.builder.sdkConfig(config).getOrCreate()

@pytest.fixture(scope="session")
def test_schema(spark):
    """Create an isolated schema for this CI run, drop it after."""
    run_id = os.environ.get("GITHUB_RUN_ID", f"local_{os.getpid()}")
    schema = f"ci_catalog.ci_{run_id}"
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

@pytest.fixture
def transactions_table(spark, test_schema):
    """Create test fixture data in the isolated schema."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {test_schema}.transactions AS
        SELECT * FROM VALUES
            (1, 'C1', 100.0, 'US', '2024-01-15'),
            (2, 'C2', 200.0, 'EU', '2024-01-16'),
            (3, NULL, 300.0, 'US', '2024-01-17')
        AS t(id, customer_id, amount, region, event_date)
    """)
    return f"{test_schema}.transactions"
```

This ensures:
- Two concurrent CI runs on different PRs each get their own `ci_catalog.ci_12345` schema
- No data interference between runs
- Schema is always dropped, even on test failure (because of `autouse=True` on session fixture with `yield`)

### Complete GitHub Actions Workflow

```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install -e ".[dev]" pyspark==3.5.*
      - run: pytest tests/unit/ -v --tb=short

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    environment: databricks-dev
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install databricks-connect==14.3.* -e ".[dev]"
      - name: Start CI cluster
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_SP_SECRET }}
        run: python scripts/ensure_cluster_running.py --cluster-id ${{ secrets.DATABRICKS_CI_CLUSTER_ID }}
      - name: Run integration tests
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_SP_SECRET }}
          DATABRICKS_CLUSTER_ID: ${{ secrets.DATABRICKS_CI_CLUSTER_ID }}
          GITHUB_RUN_ID: ${{ github.run_id }}
        run: pytest tests/integration/ -v --tb=short -m integration

  deploy-staging:
    runs-on: ubuntu-latest
    needs: integration-tests
    if: github.ref == 'refs/heads/main'
    environment: databricks-staging
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install databricks-cli
      - name: Deploy to staging
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_SP_SECRET }}
        run: |
          databricks bundle deploy --target staging
          databricks bundle run smoke_test --target staging
          
  deploy-prod:
    runs-on: ubuntu-latest
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    environment: databricks-prod  # Requires manual approval in GitHub
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install databricks-cli
      - name: Deploy to production
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_SP_SECRET }}
        run: databricks bundle deploy --target prod
```

### Cost Controls

| Control | Implementation | Effect |
|---|---|---|
| CI cluster auto-termination | 30 minutes in cluster policy | Cluster shuts down after tests, no idle billing |
| CI cluster size | Max 2 workers, DS3_v2 | Small cluster, ~$0.50/hr |
| Concurrent run limit | GitHub: max 3 concurrent CI runs | At most 3 clusters active simultaneously |
| Schema cleanup | `DROP SCHEMA ... CASCADE` on session teardown | No stale test data accumulating cost |
| Job clusters for heavy tests | Python wheel job instead of Connect for expensive tests | Job clusters are 40% cheaper than all-purpose |

Estimated cost: 3 concurrent CI runs × $0.50/hr × 30min avg = ~$0.75 per CI run. At 20 PRs/day, ~$15/day. If budget is $10/day, reduce concurrent run limit to 2.

### Failure Handling

Critical: cleanup must run even on test failure.

In pytest, use `yield` fixtures (cleanup runs regardless of test outcome):
```python
@pytest.fixture(scope="session")
def test_schema(spark):
    schema = f"ci_catalog.ci_{os.environ['GITHUB_RUN_ID']}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")  # Always runs
```

In GitHub Actions, use `if: always()` for cleanup steps:
```yaml
- name: Cleanup
  if: always()   # Runs even if previous steps failed
  run: python scripts/cleanup.py --schema ci_${{ github.run_id }}
```

</details>
</article>
