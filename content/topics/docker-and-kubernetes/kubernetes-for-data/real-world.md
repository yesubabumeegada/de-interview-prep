---
title: "Kubernetes for Data - Real World"
topic: docker-and-kubernetes
subtopic: kubernetes-for-data
content_type: real_world_example


tags: [docker, kubernetes, kubernetes-for-data]
---

# Kubernetes for Data — Real World

## Case Study: Spark on K8s Reduces Costs by 65%

### Background

A data analytics company ran a dedicated EMR cluster for Spark processing. The cluster ran 24/7 with 20 nodes (r5.2xlarge). Most of the time, only 20-30% of capacity was used — pipelines ran in windows.

### The Migration

Moved to Spark on EKS (Kubernetes) with Karpenter auto-scaling:
- Zero nodes when no Spark jobs running
- Spot instances for batch jobs (70% cost savings)
- On-demand only for jobs with strict SLAs

```bash
# Before: 20 x r5.2xlarge running 24/7 = ~$4,800/month

# After: Karpenter scales 0 → 20 nodes in 90 seconds when jobs arrive
# Average: 4 nodes × 8 hours/day = ~$800/month (83% reduction)
```

### Results

| Metric | EMR Static | Spark on K8s |
|---|---|---|
| Monthly cost | $4,800 | $800 |
| Cold start time | 0 (always on) | 90 seconds |
| Resource utilization | 25% | 85% |
| Team independence | Low (shared cluster) | High (isolated namespaces) |

**Trade-off:** 90-second cold start vs. always-on. Acceptable for batch jobs; not for interactive/streaming workloads.
