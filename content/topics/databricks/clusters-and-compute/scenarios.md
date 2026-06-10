---
title: "Clusters and Compute - Scenario Questions"
topic: databricks
subtopic: clusters-and-compute
content_type: scenario_question
tags: [databricks, clusters, compute, interview, scenarios]
---

# Scenario Questions — Clusters and Compute

<article data-difficulty="junior">

## 🟢 Junior: Choosing an Instance Type

**Scenario:** Your ETL job processes 200GB of data with multiple large shuffle joins. It currently OOMs on m5.xlarge (16 GB RAM) workers. Should you switch to r5.xlarge (32 GB) for more memory or i3.xlarge (30 GB + local SSD) for faster spill?

<details>
<summary>💡 Hint</summary>
Shuffle joins spill data to disk when memory is insufficient. The key question: is the bottleneck RAM (data doesn't fit) or disk I/O (spill is slow)?
</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: i3.xlarge**

```python
# Analysis:
# m5.xlarge: 16 GB RAM, EBS storage (network-attached, slow for spill)
# r5.xlarge: 32 GB RAM, EBS storage (more RAM, but spill still slow if it happens)
# i3.xlarge: 30 GB RAM + 950 GB NVMe SSD (fast local disk for shuffle spill)

# For shuffle-heavy workloads:
# - Some spill is inevitable with 200 GB of joins across workers
# - i3's NVMe SSD reads/writes at 1.6 GB/s (10x faster than EBS)
# - Even with 32 GB RAM (r5), large shuffles will still spill
# - i3 makes spill FAST (acceptable) rather than trying to avoid it entirely

# Configuration:
{
    "node_type_id": "i3.xlarge",  # 30 GB RAM + 950 GB NVMe SSD
    "autoscale": {"min_workers": 4, "max_workers": 12},
    "spark_conf": {
        "spark.sql.shuffle.partitions": "400",  # More partitions = smaller per-task memory
        "spark.sql.adaptive.enabled": "true",   # AQE optimizes shuffle
    }
}

# If i3 still OOMs: the data per executor is too large
# Fix: add more workers (distribute data further) rather than bigger instances
```

**Key Points:**
- i3 instances are the default choice for shuffle-heavy ETL on Databricks
- Local NVMe SSD handles spill 10x faster than EBS (network-attached storage)
- More workers (parallelism) is often better than bigger instances (RAM)
- r5 is for when data must fit in memory (caching, broadcast, collect_list)
- i3 is for when spill is acceptable but must be fast (joins, groupBy, sort)
- AQE + more shuffle partitions reduces per-task memory needs

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Spot Instance Configuration

**Scenario:** Your daily ETL job runs for 45 minutes on 8 workers. The budget is tight. Configure spot instances to reduce cost while ensuring the job completes reliably.

<details>
<summary>💡 Hint</summary>
Use SPOT_WITH_FALLBACK (tries spot, falls back to on-demand). Keep the driver on-demand (losing it kills the job). Workers on spot (Spark handles worker loss).
</details>

<details>
<summary>✅ Solution</summary>

```python
{
    "node_type_id": "i3.xlarge",
    "num_workers": 8,
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",  # Use spot when available
        "spot_bid_price_percent": 100,          # Bid at on-demand price (max availability)
        "first_on_demand": 1,                    # First node = driver (always on-demand)
        "zone_id": "auto",                       # Let AWS pick best zone for spot
    },
}

# Cost calculation:
# On-demand i3.xlarge: $0.312/hr
# Spot i3.xlarge: ~$0.094/hr (70% cheaper)
# 
# Before (all on-demand): 8 workers × $0.312 × 0.75 hr = $1.87 per run
# After (spot + fallback): 8 workers × $0.094 × 0.75 hr = $0.56 per run
# Monthly savings: (1.87 - 0.56) × 30 = $39.30/month for this one job
# 
# For 10 daily jobs: ~$393/month savings

# Why it's safe:
# - Spark is fault-tolerant: if a spot worker is reclaimed, its tasks reschedule
# - first_on_demand=1 protects the driver (job survives worker loss)
# - SPOT_WITH_FALLBACK guarantees capacity (uses on-demand if no spot available)
# - 45-min job: low chance of spot reclamation (most reclamations happen after hours)
```

**Key Points:**
- `SPOT_WITH_FALLBACK`: best of both (cheap when available, guaranteed when not)
- `first_on_demand=1`: driver MUST be on-demand (losing driver = job fails)
- Workers on spot: Spark gracefully handles worker loss (re-schedules tasks)
- `spot_bid_price_percent=100`: bid at full on-demand price for maximum spot availability
- 70% cost savings with minimal risk for batch ETL jobs
- For streaming (always-on): spot is riskier (frequent reclamation → scaling delays)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Auto-Termination

**Scenario:** Your team has 5 data scientists who each start an all-purpose cluster in the morning and forget to stop it at night. Monthly cost: $12K (clusters running 24/7 but used only 8 hours). Fix this.

<details>
<summary>💡 Hint</summary>
Set auto-termination (idle timeout). After N minutes of no activity, the cluster automatically stops. Use cluster policies to enforce this for all users.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Fix 1: Set auto-termination on existing clusters
{
    "autotermination_minutes": 30,  # Stop after 30 min idle
    # "Idle" = no notebooks running, no jobs executing, no active connections
}

# Fix 2: Enforce via cluster policy (users can't disable it)
POLICY = {
    "name": "data-science-policy",
    "definition": {
        "autotermination_minutes": {
            "type": "range",
            "minValue": 15,      # At least 15 min (not too aggressive)
            "maxValue": 60,      # At most 60 min
            "defaultValue": 30,  # Default: 30 min
        },
        # Users CANNOT set autotermination > 60 min or disable it
    }
}

# Cost impact:
# Before: 5 clusters × 24 hr/day × $2.50/hr = $300/day = $9,000/month
# After:  5 clusters × 8 hr used + 2.5 hr idle × $2.50/hr = $131/day = $3,937/month
# Savings: ~$5,000/month (56% reduction) from ONE config change!

# Additional improvement: single shared cluster instead of 5 individual
# 1 shared cluster (larger) instead of 5 personal clusters:
# Before: 5 × m5.xlarge × 24/7 = $9,000/month
# After: 1 × m5.2xlarge × 10 hr/day with auto-term = $600/month (93% savings!)
```

**Key Points:**
- Auto-termination is the #1 cost-saving config for interactive clusters
- Policy enforcement ensures NO user can disable it (governance)
- 30-minute timeout: balances cost savings vs user inconvenience (restart takes 3-4 min)
- Consider shared clusters: 5 users rarely need 5 separate clusters simultaneously
- Monitor: check system tables for clusters that are running but unused
- Alternative: encourage SQL Warehouse for queries (serverless, auto-stops faster)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Autoscaling Behavior

**Scenario:** Your cluster has `min_workers=2, max_workers=10`. The job starts with a simple read (needs 2 workers), then does a massive join (needs 8 workers), then a small write (needs 2 workers). Describe the autoscaling behavior.

<details>
<summary>💡 Hint</summary>
Autoscaling reacts to pending tasks. When tasks queue up (join phase), it adds workers. When tasks complete and workers are idle (write phase), it removes them.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Timeline of autoscaling behavior:

# T=0 min: Job starts
# Workers: 2 (min_workers)
# Phase: Reading data from S3 (parallel reads, 2 workers handle it fine)

# T=5 min: Join begins
# Workers: 2 → pending tasks pile up (only 2 workers, but 200 shuffle tasks!)
# Autoscaler detects: pending tasks >> available task slots
# T=7 min: Adds 2 workers → 4 total (takes ~2-3 min to provision)
# T=9 min: Still tasks pending → adds 2 more → 6 total
# T=11 min: Still tasks pending → adds 2 more → 8 total
# Now processing at full speed with 8 workers

# T=20 min: Join completes, write begins
# Workers: 8, but only 2 are active (write is simple)
# Autoscaler detects: 6 workers idle
# T=22 min: Removes 2 idle workers → 6
# T=24 min: Removes 2 more → 4
# T=26 min: Removes 2 more → 2 (back to min_workers)

# T=28 min: Write completes, job done. Cluster at 2 workers.

# KEY OBSERVATIONS:
# 1. Scale-UP takes ~2-4 min per batch of workers (VM provisioning)
# 2. Scale-DOWN takes ~2 min per batch (idle detection + removal)
# 3. The join phase has a "ramp-up" period where it's under-provisioned
# 4. Total: join takes longer than if we had 8 workers from the start
# 5. But we save money during read/write phases (only 2 workers)
```

**Key Points:**
- Scale-up is NOT instant (~2-4 min to add workers)
- During scale-up: tasks queue, causing temporary slowdown
- Scale-down happens ~2 min after workers become idle
- For predictable workloads: fixed cluster avoids the scaling delay
- For variable workloads: autoscaling saves 30-50% vs always-at-max
- Tip: if your job has a known "heavy" phase, consider starting at a higher min_workers

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Reading Cluster Metrics

**Scenario:** Your job took 45 minutes (SLA: 30 min). The Spark UI shows: 200 tasks, most complete in 2 minutes, but 5 tasks take 20 minutes each. What's the problem and how do you fix it?

<details>
<summary>💡 Hint</summary>
Most tasks (195) finish in 2 min but 5 take 20 min = classic data skew. A few partitions have much more data than others, causing stragglers.
</details>

<details>
<summary>✅ Solution</summary>

```python
# DIAGNOSIS: Data Skew
# 195 tasks: 2 minutes each (normal)
# 5 tasks: 20 minutes each (straggler tasks — these set the stage duration!)
# Stage duration = max task duration = 20 min (not 2 min)
# The 5 stragglers have partitions with 10x more data than normal

# FIX 1: Enable AQE skew join (automatic, no code change)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE detects skewed partitions at runtime and splits them into smaller pieces
# Result: 20-min tasks become multiple 2-min tasks

# FIX 2: If AQE doesn't fully resolve it — identify the skew key
skewed_keys = spark.sql("""
    SELECT join_key, COUNT(*) as cnt 
    FROM large_table 
    GROUP BY join_key 
    ORDER BY cnt DESC LIMIT 10
""")
# Shows: key "NULL" has 50M rows, others have 500K

# FIX 3: Handle NULL keys separately
null_data = df.filter(col("join_key").isNull())
non_null_data = df.filter(col("join_key").isNotNull())
# Process null_data with broadcast join (or special handling)
# Process non_null_data normally (balanced)
result = non_null_data.join(dim, "join_key").union(null_data.crossJoin(broadcast(default_dim)))

# RESULT:
# Before: 45 min (5 straggler tasks at 20 min each)
# After: 12 min (all tasks ~2 min, no stragglers)
# Within SLA: 12 min < 30 min ✓
```

**Key Points:**
- Data skew = a few partitions much larger than others → straggler tasks
- Stage duration = longest single task (not average!)
- First fix: enable AQE (spark.sql.adaptive.skewJoin.enabled = true)
- AQE splits skewed partitions automatically at runtime
- If AQE isn't enough: identify the hot key (often NULL or a popular value)
- Handle hot keys separately (broadcast, filter + special path)
- Adding more workers does NOT fix skew (the straggler still takes 20 min)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cost Optimization Audit

**Scenario:** Your team's Databricks monthly bill is $15K. Audit reveals: (A) 3 all-purpose clusters running 24/7 ($6K), (B) ETL on all-purpose compute ($4K), (C) SQL warehouse always-on Classic ($3K), (D) Other ($2K). Cut the bill to under $6K without impacting functionality.

<details>
<summary>💡 Hint</summary>
All three major cost centers have clear optimizations: auto-terminate idle clusters, switch ETL to job compute, and switch SQL warehouse to serverless.
</details>

<details>
<summary>✅ Solution</summary>

```python
OPTIMIZATION_PLAN = {
    "A_idle_clusters": {
        "current": "$6,000 (3 clusters × 24/7 on-demand)",
        "problem": "Clusters run overnight and weekends with zero usage",
        "fix": "Auto-terminate after 30 min idle + cluster policy enforcement",
        "after": "$1,200 (same clusters, ~8 hrs active/day × 20 workdays)",
        "savings": "$4,800",
    },
    "B_etl_compute": {
        "current": "$4,000 (ETL using all-purpose DBU rate $0.40)",
        "problem": "Production jobs paying interactive pricing",
        "fix": "Switch all ETL to Workflows with Job clusters ($0.15/DBU) + spot instances",
        "after": "$700 (same jobs: $0.15 DBU + spot instances)",
        "savings": "$3,300",
    },
    "C_sql_warehouse": {
        "current": "$3,000 (Classic Medium warehouse, always on)",
        "problem": "Paying for idle warehouse (queries only 8 hrs/day)",
        "fix": "Switch to Serverless SQL warehouse with 10-min auto-stop",
        "after": "$1,200 (pay only during active queries)",
        "savings": "$1,800",
    },
    "D_other": {
        "current": "$2,000",
        "fix": "Right-size remaining clusters based on utilization metrics",
        "after": "$1,500",
        "savings": "$500",
    },
    "total_after": "$4,600 ✓ (under $6K target)",
    "total_savings": "$10,400/month (69% reduction!)",
}

# Implementation timeline:
# Week 1: Auto-terminate policies (immediate, no disruption)
# Week 2: Migrate ETL to Workflows + job clusters (needs testing)
# Week 3: Switch SQL warehouse to serverless (immediate)
# Week 4: Right-size remaining (analyze utilization, reduce max_workers)
```

**Key Points:**
- Auto-terminate: the single biggest quick win ($4,800/month from one config)
- Job compute: 62% cheaper DBU rate for scheduled pipelines
- Spot instances: additional 70% off on top of job compute pricing
- Serverless SQL: eliminates idle warehouse cost (pay per query only)
- These are STANDARD optimizations — every Databricks deployment should apply them
- No functionality loss: same code, same results, dramatically less cost

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cluster Pool Design

**Scenario:** You have 20 hourly ETL jobs, each needing 4-8 i3.xlarge workers for 5-10 minutes. Without pools, each job waits 4 minutes for cluster startup. Design an instance pool that minimizes both startup time and idle cost.

<details>
<summary>💡 Hint</summary>
The pool should keep enough idle instances to serve typical concurrent demand. Analyze: how many jobs run simultaneously? That determines min_idle.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Analysis:
# 20 jobs/hour, each 5-10 min, needing 4-8 workers
# Concurrent jobs at any given time: ~3-4 (since 20 jobs × 7.5 avg min / 60 min = ~2.5)
# Peak concurrent workers needed: 4 jobs × 6 avg workers = 24 workers

# Pool configuration:
POOL_CONFIG = {
    "instance_pool_name": "hourly-etl-pool",
    "node_type_id": "i3.xlarge",
    
    "min_idle_instances": 8,     # Keep 8 VMs warm (covers 1-2 concurrent jobs instantly)
    "max_capacity": 32,          # Can burst to 32 (covers 4 concurrent jobs + headroom)
    
    "idle_instance_autotermination_minutes": 20,  # Remove excess idle after 20 min
    # But always maintain min_idle (those don't get terminated)
    
    "preloaded_spark_versions": ["14.3.x-photon-scala2.12"],  # Pre-install runtime
    # Further reduces startup: runtime already loaded in pool VMs
}

# Job cluster using the pool:
{
    "instance_pool_id": "pool-abc123",
    "autoscale": {"min_workers": 4, "max_workers": 8},
    # Workers come from pool: startup goes from 4 min → 30-60 seconds!
}

# Cost analysis:
# Pool idle cost: 8 instances × $0.312/hr × 24 hr = $59.90/day
# Startup time savings: 20 jobs × 4 min saved = 80 min/day compute saved
# 80 min × 6 workers × $0.312/60 = $24.96/day saved on compute alone
# Net cost: $59.90 - $24.96 = $34.94/day extra
# But: SLA improvement (jobs start 4 min faster) is worth it for hourly cadence

# Optimization: during off-hours (midnight-6 AM), only 2-3 jobs run
# Pool auto-reduces to min_idle=8 (excess VMs terminate after 20 min)
# Effective cost: pool is efficient during business hours, minimal during off-hours
```

**Key Points:**
- min_idle = typical concurrent workers needed (not peak — those scale from pool's available capacity)
- max_capacity = peak concurrent workers + headroom for burst
- idle_timeout: keep it aligned with job frequency (20 min for hourly jobs)
- Preloaded spark version eliminates runtime download time
- Pool cost is the IDLE instances × hourly rate (actively used instances cost the same regardless)
- Trade-off: pool cost vs faster startup. Worth it if jobs run frequently (every 15-60 min)
- NOT worth it for daily jobs (paying 24h of idle for a 4-min startup savings)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Photon vs Standard Runtime

**Scenario:** Your daily ETL job runs in 60 minutes on standard runtime (14.3.x-scala2.12). Switching to Photon runtime costs 2x more per DBU but the vendor claims 2-5x speedup. Is it worth it for your workload (heavy aggregations + Delta writes)?

<details>
<summary>💡 Hint</summary>
Photon costs ~2x per DBU but if it finishes in half the time, total cost is the same (or less). For aggregations + Delta writes, Photon typically gives 2-3x speedup. Do the math.
</details>

<details>
<summary>✅ Solution</summary>

```python
# STANDARD RUNTIME:
# Duration: 60 minutes
# Workers: 8 × i3.xlarge
# DBU rate: $0.15/DBU (Jobs compute)
# DBUs: 8 workers × 1 DBU × 1 hour = 8 DBUs
# DBU cost: 8 × $0.15 = $1.20
# AWS cost: 8 × $0.312 × 1 hr = $2.50
# Total per run: $3.70

# PHOTON RUNTIME (estimated 2.5x speedup for aggregation-heavy workload):
# Duration: 60 / 2.5 = 24 minutes
# Workers: 8 × i3.xlarge (same)
# DBU rate: $0.30/DBU (Photon premium, roughly 2x)
# DBUs: 8 × 1 × 0.4 hr = 3.2 DBUs
# DBU cost: 3.2 × $0.30 = $0.96
# AWS cost: 8 × $0.312 × 0.4 hr = $1.00
# Total per run: $1.96

# COMPARISON:
# Standard: $3.70/run × 30 days = $111/month, 60 min duration
# Photon: $1.96/run × 30 days = $58.80/month, 24 min duration
# 
# Photon SAVES: $52/month (47% cheaper!) AND finishes 36 min faster

# RECOMMENDATION: Switch to Photon
# - Cost savings: 47% (DBU premium is more than offset by reduced runtime)
# - SLA improvement: 60 min → 24 min (40% of current duration)
# - Risk: minimal (same code, same results, just faster execution)
# - Verify: run one day on Photon, compare results and duration
```

**Key Points:**
- Photon DBU rate is ~2x standard, but execution is 2-5x faster
- For aggregation + Delta writes: typically 2-3x speedup (Photon's sweet spot)
- Net effect: usually CHEAPER with Photon despite higher DBU rate (less total runtime)
- Run a comparison: same job, same data, standard vs Photon — measure actual speedup
- Photon doesn't help Python UDFs (only SQL/DataFrame operations)
- If your job is 80% Python UDFs: Photon won't help much (skip the premium)
- If your job is 80% SQL aggregations/joins/Delta operations: Photon is a no-brainer

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cluster Right-Sizing

**Scenario:** System tables show your ETL cluster (max_workers=16) never uses more than 6 workers. The 16-worker peak was configured 6 months ago when data was 3x larger before a migration. Right-size it.

<details>
<summary>💡 Hint</summary>
Check peak utilization over the last 30 days. Set max_workers to 1.5-2x the observed peak (headroom for growth). Also check if min_workers can be reduced.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Analyze actual worker utilization over last 30 days
SELECT 
    DATE(start_time) as run_date,
    MAX(state.metrics.max_active_executors) as peak_workers_used
FROM system.lakeflow.job_run_timeline
WHERE job_id = 12345  -- Your ETL job
  AND start_time >= current_date() - 30
GROUP BY DATE(start_time)
ORDER BY peak_workers_used DESC;

-- Results:
-- | run_date | peak_workers_used |
-- | 2024-03-10 | 6 |  ← absolute peak
-- | 2024-03-05 | 5 |
-- | most days  | 4 |  ← typical
```

```python
# Step 2: Right-size based on data

# Observed: peak = 6 workers, typical = 4 workers
# Current config: max_workers = 16 (2.7x over-provisioned!)

# New config:
{
    "autoscale": {
        "min_workers": 3,   # Typical need (was probably 4, but 3 handles light phases)
        "max_workers": 8,   # 1.3x observed peak (headroom for growth)
    }
}

# Cost impact:
# Before: could burst to 16 workers (paying for 16 during scale-up attempts)
# After: max 8 workers (half the max spend during heavy phases)
# Typical run: uses 4-6 workers (no change in duration)
# The 10 unused workers were NEVER helping — just available but idle

# Step 3: Set up monitoring to alert if we approach new max
# If peak_workers_used > max_workers * 0.8 for 3 consecutive days:
# Alert: "Consider increasing max_workers — approaching capacity limit"

# Step 4: Review quarterly (data volumes change!)
# Add a monthly check to system tables query
# If growth trend shows peak increasing, proactively increase max_workers
```

**Key Points:**
- Check ACTUAL utilization before right-sizing (don't guess from config)
- Set max_workers to 1.3-2x observed peak (headroom but not wasteful)
- Over-provisioning wastes money on UNUSED capacity
- Under-provisioning causes: SLA breaches, task queuing, slow jobs
- Review quarterly: data volumes change, cluster needs change
- Monitor after right-sizing: alert if approaching new limits
- The best cluster size is one that's 70-80% utilized at peak

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise Compute Governance

**Scenario:** You manage Databricks for 200 users across 8 teams. Monthly spend: $50K. Problems: teams create oversized clusters, nobody uses spot, some clusters run 24/7, and there's no cost attribution. Design a governance system.

<details>
<summary>💡 Hint</summary>
Four pillars: cluster policies (control creation), mandatory tags (attribution), automated monitoring (detect waste), and chargeback reports (accountability). Technical controls + financial incentives.
</details>

<details>
<summary>✅ Solution</summary>

```python
GOVERNANCE_FRAMEWORK = {
    "pillar_1_policies": {
        "purpose": "Control what users CAN create",
        "policies": {
            "standard-dev": {
                "max_workers": 4,
                "allowed_instances": ["m5.large", "m5.xlarge"],
                "auto_terminate": {"min": 15, "max": 60, "default": 30},
                "spot": "SPOT_WITH_FALLBACK (forced)",
                "tags_required": ["team", "purpose"],
            },
            "standard-etl": {
                "max_workers": 16,
                "allowed_instances": ["i3.xlarge", "i3.2xlarge", "r5.xlarge"],
                "spot": "SPOT_WITH_FALLBACK (forced)",
                "tags_required": ["team", "pipeline_name", "sla_tier"],
            },
            "large-etl": {
                "max_workers": 32,
                "requires_approval": True,  # Platform team must approve
                "tags_required": ["team", "justification"],
            },
            "gpu-ml": {
                "max_workers": 8,
                "allowed_instances": ["g5.xlarge", "g5.2xlarge"],
                "requires_approval": True,
                "tags_required": ["team", "experiment_name"],
            },
        },
    },
    
    "pillar_2_tagging": {
        "purpose": "Attribution — who spends what",
        "mandatory_tags": ["team", "cost_center", "environment"],
        "enforcement": "Cluster policy rejects creation without required tags",
        "reporting": "Monthly chargeback by team (system.billing.usage + tags)",
    },
    
    "pillar_3_monitoring": {
        "purpose": "Detect and eliminate waste",
        "automated_checks": [
            "Clusters running > 12 hours with no activity → auto-terminate",
            "Jobs using all-purpose compute → alert team lead",
            "Clusters at < 30% CPU utilization for 7 days → right-size recommendation",
            "Monthly spend > team budget × 80% → budget warning to team lead",
        ],
        "dashboard": "Real-time: running clusters, daily cost, utilization",
    },
    
    "pillar_4_chargeback": {
        "purpose": "Financial accountability drives optimization",
        "model": "Per-team monthly cost report (compute + storage)",
        "granularity": "Per-job cost visible in team dashboard",
        "incentive": "Teams that reduce spend by 20% get budget back for new projects",
        "review": "Quarterly cost review with team leads",
    },
}

# IMPLEMENTATION:
# Month 1: Deploy policies (block oversized clusters, enforce spot + auto-terminate)
# Month 2: Deploy monitoring (automated waste detection + alerts)
# Month 3: Launch chargeback reports (monthly emails to team leads)
# Month 4: Review results — expect 30-50% cost reduction organically

# Expected outcome:
# $50K → $25-30K (40-50% reduction) within 3 months
# Teams self-optimize once they see their own costs
# Platform team no longer needs to manually police cluster usage
```

**Key Points:**
- Policies: prevent the problem (users CAN'T create wasteful clusters)
- Tags: attribute the cost (know WHO is spending WHAT)
- Monitoring: detect the waste (automated alerts faster than manual review)
- Chargeback: create incentive (teams that own their cost optimize proactively)
- Approval gates for expensive resources (GPU, >16 workers) prevent accidental overspend
- Financial incentive ("save money = get budget for new projects") drives culture change
- Expected 40-50% reduction within 3 months from governance alone

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Workload Cluster Architecture

**Scenario:** Design the compute architecture for a data platform with: (A) 30 hourly ETL pipelines, (B) 10 always-on streaming jobs, (C) 100 analysts using SQL, (D) 20 data scientists training models, (E) real-time feature serving. Budget: $30K/month.

<details>
<summary>💡 Hint</summary>
Each workload type has different compute needs (instance type, lifecycle, availability). Group by: batch ETL (job clusters, spot), streaming (fixed, on-demand), SQL (serverless warehouse), ML (GPU, spot), serving (low-latency, on-demand).
</details>

<details>
<summary>✅ Solution</summary>

```python
COMPUTE_ARCHITECTURE = {
    "A_batch_etl": {
        "workload": "30 hourly ETL pipelines",
        "compute": "Job clusters from instance pool",
        "config": {
            "pool": {"instance_type": "i3.xlarge", "min_idle": 8, "max": 32},
            "clusters": "Auto-scale 4-8 workers per job, spot instances",
            "runtime": "14.3 Photon (2-3x faster for Delta operations)",
        },
        "monthly_cost": "$5,500",
        "rationale": "Pool eliminates 4-min startup for hourly jobs. Spot + Photon minimizes per-run cost. i3 handles shuffle-heavy ETL well.",
    },
    
    "B_streaming": {
        "workload": "10 always-on streaming jobs (DLT continuous)",
        "compute": "Dedicated job clusters, on-demand",
        "config": {
            "instances": "m5.xlarge, fixed 4 workers per pipeline",
            "availability": "ON_DEMAND (spot reclamation disrupts streaming)",
            "runtime": "14.3 standard (streaming doesn't benefit much from Photon)",
        },
        "monthly_cost": "$8,000",
        "rationale": "Streaming needs stability (on-demand). Fixed workers for predictable throughput. One cluster per pipeline for isolation.",
    },
    
    "C_sql_analytics": {
        "workload": "100 analysts, ad-hoc SQL + dashboards",
        "compute": "Serverless SQL Warehouses",
        "config": {
            "warehouses": [
                {"name": "analysts-small", "size": "Small", "auto_stop": "10 min"},
                {"name": "dashboards-medium", "size": "Medium", "auto_stop": "15 min"},
            ],
        },
        "monthly_cost": "$7,000",
        "rationale": "Serverless: zero management, auto-scales per query, pay-per-use. Two warehouses: separate analyst ad-hoc from dashboard scheduled refresh.",
    },
    
    "D_ml_training": {
        "workload": "20 data scientists, model training",
        "compute": "GPU job clusters, spot instances",
        "config": {
            "instances": "g5.xlarge (A10G GPU)",
            "workers": "4-8 per training job",
            "availability": "SPOT_WITH_FALLBACK",
            "runtime": "14.3 GPU ML",
        },
        "monthly_cost": "$4,500",
        "rationale": "GPU spot (60% savings). Training is checkpointed (spot-safe). ML runtime includes PyTorch/TF. 4 hrs/day average training time.",
    },
    
    "E_feature_serving": {
        "workload": "Real-time feature serving (online inference)",
        "compute": "Model Serving endpoint (serverless)",
        "config": {
            "type": "Serverless Model Serving",
            "scale_to_zero": True,
            "concurrency": "auto",
        },
        "monthly_cost": "$3,000",
        "rationale": "Serverless serving: scales with request volume, no cluster management. Pay per request + compute time.",
    },
    
    "monitoring_and_governance": {
        "monthly_cost": "$2,000",
        "includes": "Cluster policies, monitoring dashboard, automated governance checks",
    },
    
    "TOTAL": "$30,000/month ✓ (within budget)",
}

# KEY DESIGN DECISIONS:
# 1. Separate compute per workload type (different needs, different SLAs)
# 2. Spot for batch + ML (fault-tolerant, significant savings)
# 3. On-demand for streaming (stability required)
# 4. Serverless for SQL (zero management, pay-per-use ideal for bursty BI)
# 5. Instance pool for hourly ETL (startup time matters at hourly cadence)
# 6. Photon for ETL (faster = cheaper despite DBU premium)
```

**Key Points:**
- Each workload type gets optimized compute (don't use one-size-fits-all)
- Spot instances for fault-tolerant workloads (ETL, ML) = 60-70% savings
- On-demand for streaming (stability) and serving (latency)
- Serverless SQL eliminates warehouse management for analysts
- Instance pools for frequent jobs (hourly ETL needs fast startup)
- Budget fits: $30K supports batch + streaming + SQL + ML + serving
- This architecture supports hundreds of users across different workload patterns

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cluster Performance Debugging

**Scenario:** Your daily ETL job suddenly takes 3x longer (was 30 min, now 90 min). No code changes. The cluster config hasn't changed. Data volume is the same. Diagnose the root cause.

<details>
<summary>💡 Hint</summary>
If code and data haven't changed, look at: infrastructure (spot reclamation, degraded instances), upstream data quality (new skew pattern), Delta table degradation (small files accumulated), or Spark config changes (runtime upgrade side effect).
</details>

<details>
<summary>✅ Solution</summary>

```python
# DEBUGGING CHECKLIST (when code and data volume are unchanged):

# 1. CHECK: Spot instance interruptions
# If spot workers were reclaimed and replaced, tasks had to restart
# Look at: Spark UI → Executors tab → any executors with short uptime?
# Fix: check if availability changed (spot capacity in your region)

# 2. CHECK: Small files in Delta table (table degradation)
spark.sql("DESCRIBE DETAIL production.silver.orders").show()
# numFiles: if it jumped from 100 to 10,000 → small file problem!
# Fix: OPTIMIZE production.silver.orders
# Prevention: enable autoCompact + optimizeWrite

# 3. CHECK: Data skew (new pattern in source data)
# Even with same volume, distribution may have changed
# Look at: Spark UI → Stages → task duration distribution
# If 95% tasks: 1 min, 5% tasks: 20 min → new skew appeared
# Fix: enable AQE skew join, investigate the hot key

# 4. CHECK: Stale table statistics
# Spark optimizer uses statistics for join order, broadcast decisions
# If stats are stale, optimizer may choose a bad plan
spark.sql("ANALYZE TABLE production.silver.orders COMPUTE STATISTICS")
# Fix: run ANALYZE TABLE to refresh statistics

# 5. CHECK: Runtime/library version change
# Did someone update the Databricks runtime or a library?
# New runtime versions occasionally have performance regressions
# Fix: pin to the specific known-good version

# 6. CHECK: Concurrent workload interference
# Is another heavy job running at the same time (sharing cloud resources)?
# Network bandwidth, S3 throttling, or EBS limits can degrade performance
# Fix: schedule jobs at different times, or use dedicated VPCs

# 7. CHECK: Cloud provider degradation
# Rare but possible: AWS experiencing issues in your AZ
# Check AWS Service Health Dashboard
# Fix: wait, or move to a different AZ

# MOST COMMON ROOT CAUSES for "nothing changed but it's slow":
# 1. Small files accumulated (70% of cases) → OPTIMIZE
# 2. Data skew pattern shifted (15% of cases) → AQE
# 3. Spot capacity issues (10% of cases) → switch to on-demand temporarily
# 4. Stale statistics (5% of cases) → ANALYZE TABLE

# IMMEDIATE FIX:
spark.sql("OPTIMIZE production.silver.orders ZORDER BY (customer_id, order_date)")
spark.sql("ANALYZE TABLE production.silver.orders COMPUTE STATISTICS FOR ALL COLUMNS")
# Then re-run the job — likely back to 30 min
```

**Key Points:**
- "Nothing changed but it's slow" almost always has a hidden change
- #1 cause: Delta table file accumulation (streaming writes create thousands of small files)
- #2 cause: data distribution shift (same volume but different key distribution = skew)
- #3 cause: infrastructure (spot issues, degraded instances, cloud provider problems)
- OPTIMIZE + ANALYZE is the first thing to try (fixes 70% of mystery slowdowns)
- Enable `autoCompact` + `optimizeWrite` to prevent small file problems permanently
- If problem persists: compare Spark UI between a fast run and slow run (side-by-side)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Capacity Planning for Growth

**Scenario:** Your company's data volume is growing 15% monthly. Current platform: 50TB, $8K/month compute. In 12 months you'll have 250TB. Plan the compute scaling to handle 5x data while keeping costs proportional (not 5x the bill).

<details>
<summary>💡 Hint</summary>
Linear data growth doesn't require linear compute growth if you: use incremental processing (only process new data), optimize table layout (data skipping), upgrade to Photon (faster per GB), and tier storage (hot/cold).
</details>

<details>
<summary>✅ Solution</summary>

```python
# PROJECTION: 50 TB → 250 TB over 12 months (15% monthly growth)
# NAIVE: 5x data = 5x compute = $8K → $40K/month (unsustainable!)
# TARGET: keep compute growth to 2x max ($8K → $16K)

SCALING_STRATEGY = {
    "1_incremental_processing": {
        "current": "Full table scans for silver/gold refresh",
        "target": "Only process NEW data since last run (CDF, watermarks)",
        "impact": "Process 500K new rows instead of 250M total (500x less work!)",
        "compute_scaling": "Near-zero growth (new data per day stays constant)",
        "savings": "Prevents 80% of potential compute growth",
    },
    
    "2_table_optimization": {
        "current": "Tables grow without maintenance",
        "target": "Liquid clustering + regular OPTIMIZE",
        "impact": "Queries skip 95% of data via data skipping stats",
        "compute_scaling": "Read amplification stays constant even as table grows",
        "how": "CLUSTER BY (event_date, customer_id) — queries only touch relevant files",
    },
    
    "3_photon_upgrade": {
        "current": "Standard runtime for all jobs",
        "target": "Photon for all ETL (2-3x faster)",
        "impact": "Same work in half the time = half the DBUs",
        "compute_scaling": "Offset growth: 2x data but 2x faster = same cost",
    },
    
    "4_storage_tiering": {
        "current": "All data in active Delta tables",
        "target": "Hot (30 days) in Delta, Warm (90 days) in S3 IA, Cold (365+) in Glacier",
        "impact": "Only hot data participates in queries (150 GB/day × 30 = 4.5 TB active)",
        "compute_scaling": "Cluster only processes hot data; historical in cheap cold storage",
    },
    
    "5_right_sizing_automation": {
        "current": "Static cluster configs set 6 months ago",
        "target": "Monthly auto-review of utilization + auto-adjust max_workers",
        "impact": "Clusters grow with data needs, shrink when optimizations take effect",
    },
}

# PROJECTED COST AT 250 TB (with all strategies):
# Incremental processing: ETL processes ~same daily volume regardless of total size
# Table optimization: queries scan 5% of data (same as today)
# Photon: 2x faster → offset data growth
# Storage tiering: compute only touches 5 TB of hot data (not 250 TB)
# 
# Result: $8K → ~$12K (50% growth, not 5x)
# The remaining growth is from: more streaming throughput + more users
# Without strategies: would be $40K+ (5x growth)
```

**Key Points:**
- Incremental processing is the #1 defense against compute cost scaling with data
- If you only process NEW data, compute stays flat even as total data grows 5x
- Table optimization (Liquid Clustering, Z-ORDER) keeps query performance constant
- Photon offsets growth: data doubles but execution halves = flat cost
- Storage tiering keeps ACTIVE data (that compute touches) small
- Combined: 5x data growth → 1.5x compute growth (not 5x)
- Key insight: compute scales with DAILY PROCESSING VOLUME, not TOTAL DATA SIZE
- Design pipelines to be incremental from day 1 (much harder to retrofit later)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are the main cluster types in Databricks and when do you use each?**
A: All-Purpose Clusters are long-running, shared clusters used for interactive development and collaboration. Job Clusters are ephemeral clusters launched specifically for a job run and terminated afterward — they are cheaper and more isolated. SQL Warehouses are optimized for Databricks SQL workloads.

**Q: What is the difference between Standard, High Concurrency, and Single Node cluster modes?**
A: Standard mode supports multiple users but has limited isolation. High Concurrency mode enables fine-grained sharing with table ACLs and is optimized for many simultaneous users. Single Node mode runs Spark with no worker nodes — useful for small datasets, ML training on a single GPU, or development.

**Q: How does Databricks cluster autoscaling work?**
A: Autoscaling monitors the pending task queue and scales workers up when tasks are backlogged and down when workers are idle beyond the idle timeout. It reduces cost for variable workloads but can introduce latency when scaling up — not ideal for strict latency SLAs.

**Q: What is a Databricks SQL Warehouse and how does it differ from a compute cluster?**
A: A SQL Warehouse is a managed compute resource optimized for SQL analytics workloads, using the Photon execution engine. Unlike general-purpose clusters, it starts faster, scales per-query, and is designed for BI tool connectivity via JDBC/ODBC.

**Q: What is instance pooling in Databricks and what is its benefit?**
A: Instance pools pre-provision and maintain idle cloud VM instances that clusters can claim immediately on startup. They dramatically reduce cluster start times (from 5-7 minutes to seconds) by eliminating VM provisioning latency — critical for job clusters and interactive workflows.

**Q: What are Databricks Runtime versions and why do they matter?**
A: Databricks Runtime (DBR) is the set of software packages pre-installed on clusters (Spark version, Delta Lake version, ML libraries). The ML Runtime adds pre-installed ML frameworks. Choosing the correct DBR version ensures compatibility with library versions and Delta features your code requires.

**Q: How do you right-size a Databricks cluster for a production job?**
A: Profile the job with a representative data volume: check CPU utilization, shuffle spill to disk, and GC overhead in the Spark UI. Increase memory if spilling, add cores if CPU-bound, and use storage-optimized instances if shuffle-heavy. Set autoscaling bounds based on observed peak demand.

**Q: What are spot/preemptible instances and when should you use them in Databricks?**
A: Spot instances are discounted cloud VMs that can be reclaimed by the cloud provider with short notice. Use them for fault-tolerant batch workloads (not streaming or interactive) where cost savings outweigh the risk of interruption. Databricks supports mixed instance fleets with on-demand drivers and spot workers.

---

## 💼 Interview Tips

- Know when to use Job Clusters vs. All-Purpose Clusters — always recommending Job Clusters for production workloads shows cost awareness and operational discipline.
- Instance pools are a high-value optimization that many candidates overlook — mentioning them signals practical production experience.
- Be ready to walk through cluster sizing methodology: show that you use Spark UI metrics to drive decisions, not guesswork.
- Senior interviewers will probe cost optimization strategies — discuss spot instances, autoscaling, cluster policies, and job cluster lifecycle management.
- Know Databricks Runtime versions relevant to your target role (ML Runtime, GPU Runtime, photon-enabled runtimes) — version awareness signals active practitioner knowledge.
- Common mistake: using All-Purpose Clusters for production jobs — explain why Job Clusters are preferred for isolation, cost, and clean state.
