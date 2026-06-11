---
title: "Monorepo Patterns - Real World"
topic: git-and-github
subtopic: monorepo-patterns
content_type: real_world_example
tags: [git, github, monorepo-patterns, real-world]
---

# Monorepo Patterns — Real World

## Case Study: Moving from 8 Repos to 1

A DE team maintained separate repos for dbt, Airflow, Spark jobs, and utilities. Making a coordinated change (rename a column in dbt, update the downstream Spark job, update the DAG) required 3 PRs across 3 repos, coordinated merges, and hoping nothing deployed in the wrong order.

**The migration to monorepo:** Created one repo with path-filtered CI. Coordinated changes became one PR. Selective CI kept build times under 5 minutes. Cross-component changes dropped from 3 PRs + 3 merges to 1 PR + 1 merge.
