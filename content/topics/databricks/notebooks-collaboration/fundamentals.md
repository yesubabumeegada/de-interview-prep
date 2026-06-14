---
title: "Notebooks & Collaboration - Fundamentals"
topic: databricks
subtopic: notebooks-collaboration
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, notebooks, collaboration, widgets, magic-commands, workspace]
---

# Notebooks & Collaboration — Fundamentals

## 🎯 Analogy

Databricks notebooks are like Google Docs for data engineers — real-time collaborative editing, versioned, and executable. Unlike Jupyter, they run on your cluster (or serverless compute) and can be connected to Git. They're the primary workspace for exploration, prototyping, and light production workloads.

---

## Notebook Basics

**Cell types:**
- `%python` — PySpark / Python
- `%sql` — SQL (use Spark SQL directly)
- `%scala` — Scala
- `%r` — R
- `%md` — Markdown documentation
- `%sh` — Shell commands on the driver node

```python
# Python cell (default)
df = spark.table("prod.sales.orders")
df.display()

# Switch languages with magic commands
```

```sql
%sql
SELECT region, SUM(amount) AS revenue
FROM prod.sales.orders
WHERE order_date >= '2024-01-01'
GROUP BY region
ORDER BY revenue DESC
```

```python
%md
## Analysis Results

This section explains the revenue breakdown by region for Q1 2024.
Methodology: sum of `amount` column from the orders table.
```

---

## The `display()` Function

```python
# Spark DataFrames
df = spark.table("prod.sales.orders")
display(df)       # interactive table with sort, filter, charts

# Pandas DataFrames
import pandas as pd
pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
display(pdf)

# Matplotlib plots
import matplotlib.pyplot as plt
plt.plot([1, 2, 3], [4, 5, 6])
plt.title("Sample Plot")
display(plt.gcf())   # render in notebook output
```

---

## Widgets for Parameterized Notebooks

Widgets make notebooks interactive — users can change parameters without editing code:

```python
# Text widget
dbutils.widgets.text("date", "2024-01-01", "Analysis Date")

# Dropdown widget
dbutils.widgets.dropdown("region", "us-east", ["us-east", "us-west", "eu", "apac"], "Region")

# Combobox (dropdown + custom input)
dbutils.widgets.combobox("table", "orders", ["orders", "transactions", "events"], "Table Name")

# Read widget values
date = dbutils.widgets.get("date")
region = dbutils.widgets.get("region")

# Use in query
df = spark.sql(f"""
    SELECT * FROM prod.sales.orders
    WHERE order_date = '{date}'
      AND region = '{region}'
""")
display(df)

# Remove all widgets
dbutils.widgets.removeAll()
```

---

## Databricks Utilities (dbutils)

```python
# File system operations
dbutils.fs.ls("dbfs:/data/")           # list files
dbutils.fs.cp("src/", "dst/")          # copy
dbutils.fs.rm("dbfs:/tmp/old/", True)  # recursive delete

# Notebook control
dbutils.notebook.run("./child_notebook", timeout_seconds=600,
                     arguments={"param1": "value1"})

# Return a value from a notebook (for workflow use)
dbutils.notebook.exit("success: 1000 rows processed")

# Secrets (never hardcode credentials)
token = dbutils.secrets.get(scope="my-scope", key="api-token")

# Get current user/cluster info
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
```

---

## Real-Time Collaboration

Databricks notebooks support simultaneous editing:
- Multiple users see each other's cursors (like Google Docs)
- Changes are auto-saved continuously
- Cell outputs are shared immediately — useful for pair debugging

**Workspace organization:**
```
Workspace/
  Shared/              ← visible to all users
    data-platform/
      pipelines/
      utilities/
  Users/
    alice@company.com/ ← personal space
    bob@company.com/
  Repos/               ← Git-connected notebooks (preferred for production)
    my-org/
      de-pipelines/
```

---

## Notebook Results and Comments

```python
# Add a persistent comment to a notebook (shows in sidebar)
# Right-click any cell → Add Comment

# Tag cells with metadata
# Cell → ... menu → Edit Tags
# Tag: "data-validation", "deprecated", "review-needed"

# Pin outputs — results persist even if kernel restarts
# Run cell → right-click output → Pin output
```

---

## Interview Tips

> **Tip 1:** "What is the difference between `display()` and `show()` in Databricks?" — "`show()` is a standard PySpark method that prints a text table to stdout — limited, no interactivity. `display()` is Databricks-specific — it renders an interactive table with filtering, sorting, and charting capabilities. It also works on Pandas DataFrames and Matplotlib figures. Always use `display()` in notebooks."

> **Tip 2:** "What are widgets used for?" — "Widgets make notebooks parameterizable without editing code. You define a widget (text, dropdown, slider), and users can change its value via a UI dropdown at the top of the notebook. The notebook re-runs using the new parameter value. Useful for date-range exploratory notebooks, regional dashboards, or any notebook that Databricks Workflows passes parameters to."

> **Tip 3:** "How do you pass parameters to a notebook from a Databricks Workflow?" — "In the Workflow task configuration, set 'Parameters' as key-value pairs. In the notebook, access them with `dbutils.widgets.get('param_name')`. This pattern lets one notebook serve multiple use cases — e.g., the same data quality notebook run for different tables by passing the table name as a parameter."
