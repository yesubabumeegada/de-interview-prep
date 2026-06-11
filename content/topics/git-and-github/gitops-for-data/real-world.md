---
title: "GitOps for Data - Real World"
topic: git-and-github
subtopic: gitops-for-data
content_type: real_world_example
tags: [git, github, gitops-for-data, real-world]
---

# GitOps for Data — Real World

## Case Study: Eliminating 'Shadow Deploys'

A DE team at an insurance company deployed Airflow DAGs by SSHing into the server and copying files. Several engineers did this, and over time the server had DAG versions that didn't match what was in git. A bug was fixed in git but the old version was still on the server — no one noticed for 3 weeks.

**The fix:** GitOps via GitHub Actions. Merging to main automatically synced DAGs to MWAA S3 bucket. SSHing to the server for deployments was removed from everyone's access. After 6 months: zero drift incidents, full audit trail, on-call engineers could see exactly what was running by checking git.
