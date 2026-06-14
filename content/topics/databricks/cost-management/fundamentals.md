---
title: "Cost Management - Fundamentals"
topic: databricks
subtopic: cost-management
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, cost, dbu, auto-termination, spot-instances, cluster-policies]
---

# Cost Management — Fundamentals

## 🎯 Analogy

Databricks cost is like a taxi meter — it runs while the engine is on. Auto-termination is like the car automatically turning off when parked. DBUs (Databricks Units) are the fuel — different car types (cluster sizes) burn fuel at different rates. Spot instances are like carpooling — cheaper but someone might need the car back unexpectedly.

---

## DBU Pricing Model

DBU (Databricks Unit) = the unit of compute consumption.

```
Cost = DBU rate × number of DBUs consumed
DBUs consumed = cluster DBU/hour × time running

Example:
  8-node cluster: 8 × 1 DBU/hr = 8 DBU/hr
  Running for 3 hours: 8 × 3 = 24 DBUs
  At $0.40/DBU: 24 × $0.40 = $9.60
```

**DBU rate varies by:**
- Workload type: Jobs (cheaper) vs All-Purpose (more expensive)
- Cluster tier: Standard vs Premium
- Cloud provider / region

**Plus:** Cloud compute costs (EC2/Azure VMs) are charged separately by your cloud provider.

---

## Auto-Termination: #1 Cost Lever

The most common source of wasted cost: clusters left running when not in use.

```python
# In cluster settings: Auto Termination → 30 minutes
# After 30 minutes of inactivity → cluster shuts down automatically

# Via API:
{
    "autotermination_minutes": 30,
    "cluster_name": "my-cluster",
    ...
}

# Via Databricks CLI:
databricks clusters edit --cluster-id <id> \
  --json '{"autotermination_minutes": 30}'
```

**Rule:** Interactive clusters should terminate after 30-60 minutes. Production job clusters terminate automatically when the job finishes.

---

## Spot Instances (Save 50-70%)

Spot instances (AWS) / Spot VMs (Azure) are preemptible — the cloud provider can reclaim them:

```python
# Cluster config with spot workers (on-demand driver for stability)
{
    "driver_node_type_id": "i3.xlarge",    # on-demand (stable)
    "node_type_id": "i3.xlarge",            # workers
    "num_workers": 8,
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",   # spot first, fall back to on-demand
        "spot_bid_price_percent": 100,           # bid up to 100% of on-demand price
        "first_on_demand": 1                     # keep 1 worker on-demand as backup
    }
}
```

**When to use spot:** Fault-tolerant batch jobs, dev/test, ML training. Avoid for: streaming jobs (interruption = data loss risk), interactive user clusters (interruption = session lost).

---

## Job Clusters vs All-Purpose Clusters

```
Job Clusters:
  - Created per-job run → auto-terminated when job completes
  - ~50% cheaper DBU rate
  - No shared compute — no cold start conflicts

All-Purpose (Interactive) Clusters:
  - Long-running, shared by multiple users
  - More expensive DBU rate
  - Good for: Databricks SQL warehouses, interactive notebooks

Rule: Production pipelines should use job clusters, not all-purpose clusters.
```

---

## Cluster Policies: Prevent Runaway Costs

```json
{
    "name": "engineer-standard",
    "definition": {
        "autotermination_minutes": {
            "type": "range",
            "minValue": 10,
            "maxValue": 60,
            "defaultValue": 30
        },
        "num_workers": {
            "type": "range",
            "minValue": 1,
            "maxValue": 10
        },
        "node_type_id": {
            "type": "allowlist",
            "values": ["i3.xlarge", "i3.2xlarge"],
            "defaultValue": "i3.xlarge"
        },
        "aws_attributes.availability": {
            "type": "fixed",
            "value": "SPOT_WITH_FALLBACK"
        }
    }
}
```

**Effect:** Engineers can't spin up a 50-node on-demand cluster — the policy limits them to 10 spot workers and forces auto-termination.

---

## Key Cost Concepts

| Concept | Impact |
|---------|--------|
| **Auto-termination** | Eliminates idle cluster cost |
| **Spot instances** | 50-70% compute savings |
| **Job clusters** | 50% cheaper DBU rate vs all-purpose |
| **Cluster policies** | Prevent oversized cluster creation |
| **Photon** | Faster queries = fewer DBUs for same work |

---

## Interview Tips

> **Tip 1:** "What's the biggest Databricks cost mistake teams make?" — "Forgetting to set auto-termination on interactive clusters. A developer starts a 20-node cluster, leaves for a meeting, and the cluster runs for 8 hours over the weekend — $400+ wasted. Set auto-termination to 30-60 minutes at the org policy level via cluster policies."

> **Tip 2:** "When should you use spot instances?" — "For fault-tolerant batch workloads: ETL pipelines, ML training, scheduled reports. Use `SPOT_WITH_FALLBACK` — spot first, falls back to on-demand if spot is unavailable. Keep the driver on-demand. Avoid spot for streaming jobs (interruption = trigger reprocessing) and interactive user sessions (interruption = losing work)."

> **Tip 3:** "What's the difference in cost between a job cluster and an all-purpose cluster?" — "Job clusters use the Jobs Compute DBU rate, which is roughly 50% cheaper than the All-Purpose rate. Additionally, job clusters terminate immediately when the job finishes — no idle time. For a pipeline that runs 1 hour/day, an all-purpose cluster running all day costs ~24x more in idle time alone."
