---
title: "Airflow Dynamic DAGs - Senior Deep Dive"
topic: airflow
subtopic: dynamic-dags
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, dynamic-dags, parse-performance, lazy-evaluation, testing, versioning, production]
---

# Airflow Dynamic DAGs — Senior Deep Dive

## DAG File Parsing: The Hidden Performance Tax

Every DAG file in the `dags/` directory is **parsed repeatedly** by the scheduler. Understanding this cycle is essential for production dynamic DAGs.

### Parse Cycle Mechanics

```
Default parse cycle:
  - scheduler_heartbeat_sec = 5s (how often the scheduler loops)
  - min_file_process_interval = 30s (minimum time between DAG file parses)
  - dag_file_processor_timeout = 50s (kill parser if it takes this long)

For a 30-DAG deployment:
  - Each file parsed every ~30s
  - 1 parse/30s × 60s/min × 60min/hr = 120 parses/hour per file
  - Over 24 hours: 2,880 parse events per file
```

**Any top-level code in the DAG file runs 2,880 times per day per file.**

```python
# This runs 2,880 times/day — EXPENSIVE for DB calls
with open('/config/tables.json') as f:          # OK: filesystem is fast
    tables = json.load(f)

tables = Variable.get('tables_config', ...)     # ACCEPTABLE: Airflow caches this
tables = requests.get('http://api/tables').json()  # DANGEROUS: HTTP call on every parse
tables = db_cursor.execute('SELECT...').fetchall() # VERY DANGEROUS: DB query on every parse
```

### Measuring Parse Time

```bash
# Check how long your DAG file takes to parse
airflow dags report

# Or time it directly
time python /opt/airflow/dags/my_dynamic_dag.py

# The scheduler's parse duration is logged and visible in Airflow UI:
# Browse → DAG Dependencies → File parsing stats (Airflow 2.3+)
```

```sql
-- Query parse durations from metadata DB
SELECT
    fileloc,
    AVG(duration) as avg_parse_seconds,
    MAX(duration) as max_parse_seconds,
    COUNT(*) as parse_count
FROM dag_parsing_results
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY fileloc
ORDER BY avg_parse_seconds DESC;
```

**Target:** DAG file parse time < 1 second. > 30 seconds risks parse timeouts and missed schedules.

---

## Lazy Evaluation vs Eager Generation

### Eager (Traditional Loop) — Problems at Scale

```python
# Eager: all tasks created at parse time
# If the list changes, the DAG must be re-parsed to pick up changes
# 1000 items = 1000 task objects created on every parse

tables = load_all_tables_from_db()  # runs at parse time

for table in tables:
    PythonOperator(task_id=f'process_{table}', ...)
# Problem: 1000 DB calls × 2880 parses/day = 2,880,000 DB queries/day
```

### Lazy (Dynamic Task Mapping) — Production Pattern

```python
# Lazy: the list is NOT evaluated at parse time
# The DAG structure has fixed task nodes; the count is determined at runtime

def get_tables(**context) -> list[str]:
    """This runs at task execution time, NOT parse time."""
    return db_cursor.execute('SELECT table_name FROM changed_tables').fetchall()

with DAG('lazy_dynamic_dag', ...) as dag:

    # At parse time: ONE task node "get_tables"
    discover = PythonOperator(task_id='get_tables', python_callable=get_tables)

    # At parse time: ONE mapped task node "process_table"
    # The expand() is evaluated at RUNTIME when discover completes
    process = PythonOperator.partial(
        task_id='process_table',
        python_callable=process_table_fn,
    ).expand(
        op_kwargs=discover.output.map(lambda t: {'table': t})
    )

    discover >> process

# Parse cost: 2 task objects per parse (regardless of list size)
# Runtime cost: N task instances only when the DAG actually runs
```

**Impact comparison for 200 tables:**

| Approach | Task objects at parse time | DB queries/day |
|----------|--------------------------|----------------|
| Eager loop | 200 | 200 × 2,880 = 576,000 |
| Dynamic mapping | 2 (fixed) | 1 per DAG run × runs_per_day |

---

## Testing Dynamic DAGs

### 1. Parse Correctness Test

```python
import pytest
from airflow.models import DagBag

def test_dynamic_dag_parses_without_error():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert 'my_dynamic_dag' in dagbag.dags, f"DAG not found. Errors: {dagbag.import_errors}"
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

def test_dynamic_dag_task_count():
    """Verify DAG generates expected number of tasks for a known config."""
    # Patch the config source so we control what the DAG sees
    import json
    test_config = {'tables': ['orders', 'customers', 'products']}
    
    with patch('builtins.open', mock_open(read_data=json.dumps(test_config))):
        dagbag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dagbag.get_dag('config_driven_dag')
        
        task_ids = [t.task_id for t in dag.tasks]
        assert 'load_orders' in task_ids
        assert 'load_customers' in task_ids
        assert 'load_products' in task_ids
        assert len([t for t in task_ids if t.startswith('load_')]) == 3

def test_no_duplicate_task_ids():
    """Ensure dynamic generation doesn't create duplicate task IDs."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.get_dag('my_dynamic_dag')
    
    task_ids = [t.task_id for t in dag.tasks]
    assert len(task_ids) == len(set(task_ids)), \
        f"Duplicate task IDs found: {[t for t in task_ids if task_ids.count(t) > 1]}"
```

### 2. Testing Dynamic Task Mapping Output

```python
from airflow.models import TaskInstance, DagRun
from airflow.utils.state import State
from unittest.mock import patch

def test_discover_returns_expected_structure():
    """Test that the discovery task returns the correct format for expand()."""
    from dags.my_dynamic_dag import discover_changed_tables
    
    # Mock the data source
    with patch('dags.my_dynamic_dag.query_metadata') as mock_query:
        mock_query.return_value = [
            {'table': 'orders', 'partition': '2024-01-15'},
            {'table': 'customers', 'partition': '2024-01-15'},
        ]
        
        result = discover_changed_tables(ds='2024-01-15', **mock_context)
        
        assert isinstance(result, list), "Must return a list for expand()"
        assert len(result) == 2
        assert all('table' in item for item in result), "Each item must have 'table' key"
        assert all('partition' in item for item in result), "Each item must have 'partition' key"
```

### 3. Dependency Structure Test

```python
def test_task_dependencies_in_dynamic_dag():
    """Verify the dependency graph is correct regardless of how many tasks are generated."""
    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('dynamic_pipeline')
    
    discover_task = dag.get_task('discover_tables')
    
    # Verify that all dynamically generated tasks depend on discover
    for task in dag.tasks:
        if task.task_id.startswith('process_'):
            assert discover_task.task_id in [t.task_id for t in task.upstream_list], \
                f"Task {task.task_id} must depend on discover_tables"
```

---

## Versioning Dynamic DAG Configs Safely

Dynamic DAG configs (JSON/YAML files, Airflow Variables) are data that drives structure. Versioning them requires care to avoid breaking existing DAG runs.

### Config File Versioning

```
dags/
├── config/
│   ├── tables_v1.json         # original config
│   ├── tables_v2.json         # new config (added 2 tables)
│   └── tables_current.json    # symlink to active version
└── my_pipeline.py
```

```python
# my_pipeline.py — reads from current version
import json
import os

CONFIG_VERSION = os.environ.get('PIPELINE_CONFIG_VERSION', 'current')
config_path = f'/opt/airflow/dags/config/tables_{CONFIG_VERSION}.json'

with open(config_path) as f:
    config = json.load(f)
```

**Deployment process:**
1. Deploy `tables_v2.json` to all Airflow nodes
2. Test with `PIPELINE_CONFIG_VERSION=v2` on a dev instance
3. Update environment variable to switch active version atomically
4. New parses pick up v2; old in-flight runs are unaffected (they've already been instantiated)

### Airflow Variable Versioning

```python
# Store config with a version key
import json
from airflow.models import Variable

# Write
Variable.set('pipeline_config_v2', json.dumps({
    'tables': ['orders', 'customers', 'products', 'new_table'],
    'version': 2,
}))

# Activate (swap the current pointer)
Variable.set('pipeline_config_current', 'v2')

# DAG reads
current_version = Variable.get('pipeline_config_current', default_var='v1')
config = Variable.get(f'pipeline_config_{current_version}', deserialize_json=True)
```

### Handling Config Changes Mid-Run

**Problem:** If a config change removes a table between when the DAG is scheduled and when it's parsed, the DAG structure changes mid-lifecycle — Airflow may show tasks as deleted in the UI.

**Solution: Config snapshot at run start**

```python
def snapshot_config(**context) -> dict:
    """
    Snapshot the current config at the START of the DAG run.
    Store in XCom so all downstream tasks see the same config,
    even if the config file changes during the run.
    """
    with open('/config/tables.json') as f:
        config = json.load(f)
    # Stored in XCom automatically via return value
    return config

with DAG('safe_dynamic_dag', ...) as dag:
    
    snapshot = PythonOperator(
        task_id='snapshot_config',
        python_callable=snapshot_config,
    )
    
    # All processing uses the snapshotted config, not live config
    process = PythonOperator.partial(
        task_id='process_table',
        python_callable=process_fn,
    ).expand(
        op_kwargs=snapshot.output.map(lambda c: [{'table': t} for t in c['tables']])
    )
    
    snapshot >> process
```

---

## Production Checklist for Dynamic DAGs

```
Parse performance:
  ☐ No database calls at module level
  ☐ No HTTP calls at module level
  ☐ Config loaded from file (not network) or Airflow Variables
  ☐ Parse time < 1 second (verified with `time python dag_file.py`)

Task structure:
  ☐ Task IDs are unique (tested)
  ☐ Task count < 250 (or dynamic task mapping used instead of loops)
  ☐ TaskGroups used for tasks > 30 for UI readability
  ☐ `max_active_tis_per_dag` set on mapped tasks

Testing:
  ☐ Parse correctness test exists (DagBag loads without errors)
  ☐ Task count verified for known config input
  ☐ Dependency structure tested programmatically
  ☐ Discovery function tested independently with mocked data source

Config management:
  ☐ Config is versioned (not mutable in place)
  ☐ Config changes go through review (structure impacts DAG)
  ☐ Rollback procedure documented
  ☐ Deployment process ensures all nodes see same config version
```

---

## Interview Tips

> **Tip 1:** "What's the parse-time performance concern with dynamic DAGs?" — "DAG files are parsed every 30 seconds. Any top-level code runs on every parse cycle — 2,880 times per day. If your dynamic config comes from a database query, that's potentially millions of queries per day that have nothing to do with your actual pipeline runs. The fix is to read config from files (fast) or use lazy evaluation via dynamic task mapping, where the list is determined at runtime, not parse time."

> **Tip 2:** "How do you test a dynamic DAG?" — "Three levels: (1) parse test — load the DAG via DagBag and assert no import errors; (2) structure test — verify task IDs, dependencies, and trigger rules are correct for a known config input (mock the config source); (3) function test — test the discovery/mapping functions independently, ensuring they return the correct data format for expand()."

> **Tip 3:** "What happens if a config changes while a DAG run is in progress?" — "Airflow uses the task structure that was valid when the DAG run was created. Mid-run config changes to loop-based DAGs can cause confusion in the UI (tasks may appear as 'removed'). For dynamic task mapping, the mapped instances are determined when the upstream task completes — so a config change mid-run only affects runs that start after the change. The safest pattern is to snapshot the config at the start of each DAG run via XCom, so all downstream tasks work from the same snapshot."

## ⚡ Cheat Sheet

**Parse Cycle Numbers**
- Default `min_file_process_interval` = 30s → every DAG file parsed **2,880 times/day**
- `dag_file_processor_timeout` = 50s → parse killed if longer
- Target: parse time **< 1 second**; > 30s risks missed schedules
- Verify: `time python /opt/airflow/dags/my_dag.py`

**What's Safe vs Dangerous at Module Level**
| Code | Safety | Why |
|---|---|---|
| `open('config.json')` | ✅ OK | Filesystem is fast |
| `Variable.get('tables')` | ✅ Acceptable | Airflow caches Variables |
| `requests.get('http://api/...')` | ❌ Dangerous | HTTP call × 2,880/day |
| `db_cursor.execute('SELECT...')` | ❌ Very dangerous | DB query × 2,880/day |

**Eager vs Lazy Task Generation**
| | Eager (loop at parse) | Lazy (dynamic task mapping) |
|---|---|---|
| Task objects at parse | N per parse cycle | 2 (fixed) |
| DB queries/day | N × 2,880 | ~runs_per_day |
| Config change | Requires re-parse | Evaluated at runtime |
| Airflow version | All | 2.3+ |

**Dynamic Task Mapping Pattern**
```python
discover = PythonOperator(task_id='get_list', python_callable=get_list)
process = PythonOperator.partial(task_id='process', python_callable=fn).expand(
    op_kwargs=discover.output.map(lambda t: {'item': t})
)
```

**Production Checklist**
- ☐ No DB/HTTP calls at module level
- ☐ Parse time < 1s (`time python dag_file.py`)
- ☐ Task IDs unique (tested via DagBag)
- ☐ Task count < 250 per DAG (or use mapping)
- ☐ `max_active_tis_per_dag` set on mapped tasks
- ☐ Config is versioned (not mutable in place)

**Config Snapshot Pattern (Safe Mid-Run Changes)**
```python
# First task snapshots config → returns via XCom
# All downstream tasks use XCom snapshot, not live config file
# Config change mid-run only affects runs that start AFTER the change
```

**Testing Dynamic DAGs — Three Levels**
1. **Parse test**: `DagBag.import_errors == 0`
2. **Structure test**: task IDs, dependency ordering, trigger rules — with mocked config
3. **Function test**: discovery/mapping functions return correct format for `expand()`
