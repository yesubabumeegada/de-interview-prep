---
title: "Deployment Strategies - Real World"
topic: ci-cd
subtopic: deployment-strategies
content_type: real_world_example
tags: [ci-cd, deployment,blue-green,canary,rollback, real-world]
---

# Deployment Strategies — Real World

## Case Study: Canary Deploy Prevented a Revenue Data Outage

A fintech company switched their revenue pipeline to canary deployments after a bad deploy caused 6 hours of wrong revenue numbers. With canary, v2 first served 5% of pipeline runs. Within 10 minutes, automated checks detected a 15% error rate on the canary (vs 0.1% baseline). The canary was automatically rolled back. Total impact: 5% of one pipeline run — vs 6 hours of wrong data in the old approach.

**The key setup:** Error rate alert in Prometheus. If canary error rate > 5% for 5 minutes, automatically revert the service selector to the stable deployment. Zero manual intervention.
