---
title: "Spark Deployment & Ops — Real World"
topic: spark
subtopic: deployment-and-ops
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, deployment, production, incident, cost, spot, migration, real-world]
---

# Spark Deployment & Ops — Real World

## War Story: Spot Fleet Causing Job Failures at 3 AM

**Scenario:** Production ETL on EMR Spot fleet ran fine for 3 weeks, then started failing consistently at 3 AM. Jobs aborted mid-run with `ExecutorLostFailure` on 60% of executors simultaneously.

**Investigation:**
```
3 AM → EC2 Spot prices spike for r5.4xlarge in us-east-1b
EMR Spot interruption: 60% of task nodes terminated simultaneously

Spark configuration:
  spark.task.maxFailures = 4 (default)
  ESS: NOT enabled
  → Shuffle data lost with executor
  → Stages failed, retried, failed again (no ESS to recover shuffle)
  → Job aborted after 4 stage failures
```

**Fix:**
```python
# 1. Enable ESS to survive executor loss
spark.conf.set("spark.shuffle.service.enabled", "true")
# Configure in EMR: bootstrap action to start ESS on all nodes

# 2. Increase retry tolerance
spark.conf.set("spark.task.maxFailures", "10")
spark.conf.set("spark.stage.maxConsecutiveAttempts", "8")

# 3. Diversify instance types (reduce simultaneous interruption risk)
# EMR: Use instance fleet instead of instance group
# Mix: r5.4xlarge, r5a.4xlarge, m5.8xlarge → different Spot pools
# If one pool is interrupted, others keep running

# 4. Checkpoint long jobs at natural boundaries
# Between major ETL phases: write intermediate results to S3
# On restart: skip completed phases
```

---

## War Story: K8s Executor Pods Pending for 20 Minutes

**Scenario:** New K8s cluster for Spark. Jobs submitted but executor pods stayed in `Pending` state for 15-20 minutes before eventually starting. 

**Investigation:**
```bash
kubectl describe pod spark-job-exec-1 -n spark-jobs
# Events:
# Warning  FailedScheduling  0/12 nodes are available:
#   12 Insufficient cpu.

# Check node resources:
kubectl top nodes
# NAME       CPU(cores)  CPU%  MEMORY   MEMORY%
# node-01    3950m/4000m 98%   14.2Gi   89%
```

**Root cause:** Executor pods requested `spark.executor.request.cores = 4` (same as limit). All 12 nodes were already at 98% CPU utilization because previous jobs hadn't released their executor pods (idle executors not scaling down).

**Fix:**
```python
# 1. Enable dynamic allocation on K8s (requires Remote Shuffle Service)
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.shuffle.manager",
    "org.apache.spark.shuffle.celeborn.SparkShuffleManager")

# 2. Set CPU requests < limits (allow bursting, better bin-packing)
spark.conf.set("spark.kubernetes.executor.request.cores", "2")
spark.conf.set("spark.kubernetes.executor.limit.cores", "4")

# 3. Use cluster autoscaler + node groups with proper labels
# Cluster autoscaler adds nodes when pods are Pending
# -- add annotation to namespace:
# cluster-autoscaler.kubernetes.io/safe-to-evict: "false" (driver)
# cluster-autoscaler.kubernetes.io/safe-to-evict: "true"  (executors)
```

---

## Migration Playbook: YARN to Kubernetes

```
Phase 1: Assessment (2 weeks)
  □ Inventory all spark-submit invocations
  □ Identify external dependencies (HDFS, Hive Metastore, Kerberos)
  □ Assess Python/JAR dependency management
  □ Estimate container image count

Phase 2: Foundation (3 weeks)
  □ Set up K8s cluster with node autoscaler
  □ Deploy Remote Shuffle Service (Celeborn)
  □ Build base Spark Docker image
  □ Set up container registry + CI/CD for image builds
  □ Configure RBAC, namespaces, resource quotas

Phase 3: Pilot (2 weeks)
  □ Migrate 3-5 non-critical jobs
  □ Compare performance vs YARN baseline
  □ Test dynamic allocation and autoscaling
  □ Validate shuffle service behavior

Phase 4: Migration (6-8 weeks)
  □ Migrate jobs team by team
  □ Run YARN and K8s in parallel with checksums
  □ Decommission YARN after 2-week stability window

Phase 5: Optimization (ongoing)
  □ Tune resource requests/limits per job
  □ Implement spot instance strategy
  □ Set up Prometheus/Grafana monitoring
```

---

## Interview Tips

> **Tip 1:** "How do you handle simultaneous Spot interruptions?" — Use an instance fleet with 5+ instance types across 2+ availability zones — different Spot pools have uncorrelated interruption events. Enable External Shuffle Service (YARN) or Remote Shuffle Service (K8s) so executor death doesn't lose shuffle data. Increase `spark.task.maxFailures` to 10. For critical jobs, pin the core nodes (HDFS/shuffle storage) to On-Demand and use Spot only for compute task nodes.

> **Tip 2:** "What's your approach to migrating from YARN to Kubernetes?" — The key challenges are: (1) Dynamic allocation — YARN's ESS has no K8s equivalent; use Celeborn/Uniffle. (2) Hive Metastore access — deploy a standalone HMS that both can reach. (3) Python dependencies — Docker images replace YARN's distributed archive; need CI/CD for image builds. (4) Kerberos — K8s sidecar or keytab secret injection. Run both in parallel during migration with result checksums to validate correctness before decommissioning YARN.
