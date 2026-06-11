---
title: "Monorepo Patterns — Scenarios"
topic: git-and-github
subtopic: monorepo-patterns
content_type: scenario_question
tags: [git, github, monorepo-patterns, interview, scenarios]
---

# Monorepo Patterns — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Getting Started

**Scenario:** Your team asks you to set up a monorepo for the DE team that has separate repos for dbt, Airflow, and Python pipelines. Describe your approach.

<details>
<summary>💡 Hint</summary>

Decide on directory structure first. Migrate repos one at a time. Set up path-filtered CI immediately so each team only has their tests triggered. CODEOWNERS assigns each directory to its owning team.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Create monorepo structure
mkdir de-monorepo
cd de-monorepo
git init

# Move existing repos as directories
git remote add dbt-old https://github.com/org/dbt-repo.git
git fetch dbt-old
git read-tree --prefix=dbt/ -u dbt-old/main

# Directory structure:
# de-monorepo/
# ├── dbt/
# ├── airflow/
# ├── pipelines/
# └── .github/
#     ├── CODEOWNERS
#     └── workflows/
```

```yaml
# Path-filtered CI (each team's tests run independently)
on:
  pull_request:
    paths: ['dbt/**']

# .github/CODEOWNERS
dbt/        @dbt-team
airflow/    @airflow-team
pipelines/  @pipeline-team
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling a Real Problem

**Scenario:** Your monorepo CI takes 25 minutes because every PR runs all tests for all services. How do you fix it?

<details>
<summary>💡 Hint</summary>

Detect which services changed (git diff) and only run their tests. Path-filtered triggers and selective test execution are the key. Also cache per-service dependencies separately.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Detect changed services in CI
jobs:
  detect-changes:
    outputs:
      dbt: ${{ steps.changes.outputs.dbt }}
      pipelines: ${{ steps.changes.outputs.pipelines }}
    steps:
      - uses: dorny/paths-filter@v3
        id: changes
        with:
          filters: |
            dbt:
              - 'dbt/**'
            pipelines:
              - 'pipelines/**'

  test-dbt:
    needs: detect-changes
    if: needs.detect-changes.outputs.dbt == 'true'
    steps:
      - run: dbt test

  test-pipelines:
    needs: detect-changes
    if: needs.detect-changes.outputs.pipelines == 'true'
    steps:
      - run: pytest pipelines/
```

Result: PRs touching only dbt skip pipeline tests entirely. 25 min → 4 min for typical PRs.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design at Scale

**Scenario:** Your company has decided to merge 15 separate repos (dbt, 5 Airflow DAG repos, 6 pipeline repos, shared libraries, infra) into one monorepo. Design the migration and ongoing CI strategy.

<details>
<summary>💡 Hint</summary>

Migrate one repo at a time using git subtree or git filter-repo (preserve history). Set up CI incrementally. Start with CODEOWNERS and path-filtered CI from day one. Use workspace tools (uv workspaces, npm workspaces) for shared dependency management.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Migration plan (15 repos → 1):
# Week 1-2: Setup monorepo structure, CI, CODEOWNERS
# Week 3-6: Migrate repos one at a time (preserve git history)

# Migrate with history preserved:
git subtree add --prefix=dbt https://github.com/org/dbt-repo.git main --squash

# Or with full history (larger but traceable):
git remote add dbt-remote https://github.com/org/dbt-repo.git
git fetch dbt-remote
git merge -s ours --no-commit dbt-remote/main
git read-tree --prefix=dbt/ -u dbt-remote/main
git commit -m 'migration: import dbt repo with full history'

# CI: selective testing from day one
# Cache: separate per-service cache keys
# CODEOWNERS: each directory owned by its team
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a monorepo and what are its main advantages for DE teams?**
A: A monorepo is a single git repository containing multiple projects (dbt, Airflow, Spark, shared utilities). Advantages: atomic cross-component changes in one PR, shared tooling and CI configuration, easier code sharing, and unified dependency management.

**Q: What is path-filtered CI and why is it critical for monorepos?**
A: Path-filtered CI only runs tests for services whose files changed in a PR. Without it, every PR runs all tests for all services — making CI slow and annoying. GitHub Actions paths filter (`paths:`) or tools like Turborepo/Nx implement this.

**Q: What is CODEOWNERS and how does it help in a monorepo?**
A: CODEOWNERS maps directory paths to teams/individuals who automatically become required reviewers. In a monorepo, it ensures each team reviews changes to their own code without manual reviewer assignment, even though everyone shares the same repo.

**Q: What are the main challenges of monorepos at large scale?**
A: CI time (mitigated by path filtering + caching), merge conflicts when many teams modify shared code (mitigated by CODEOWNERS + clear ownership boundaries), git performance with large histories (mitigated by shallow clones in CI), and dependency management complexity.

**Q: When would you choose polyrepo over monorepo?**
A: Polyrepo makes sense when teams need fully independent release cycles, deploy on different cadences, use fundamentally different technology stacks, or when security requires strict code isolation between teams.

**Q: How do you share code between services in a monorepo?**
A: Use local workspace packages (uv workspaces, npm workspaces). A shared library lives in `shared/` and other services declare it as a local dependency. Changes to the shared library trigger tests for all dependent services via the dependency graph in CI.

**Q: What is Turborepo/Nx and when would a DE team use it?**
A: Turborepo and Nx are monorepo build systems that add intelligent caching and dependency graph-aware task running. They cache test results and only re-run tasks whose inputs changed. Useful when a DE team has complex JavaScript/Node tooling, but overkill for pure Python/SQL monorepos.

---

## 💼 Interview Tips

- Lead with path-filtered CI as the key enabler of monorepo scalability — without it, monorepos create unbearably slow CI.
- Connect CODEOWNERS to team ownership at scale — it's the practical mechanism that makes team-based ownership work in a shared repo.
- Be prepared to argue for or against monorepo vs polyrepo — know both sides. Monorepo wins on coordination; polyrepo wins on independence and isolation.
- For migration questions, mention git subtree or git filter-repo as the tools for preserving history — history preservation is often non-negotiable for compliance and debugging.
- Shared library management (local workspace packages) is an intermediate topic that separates candidates with monorepo experience from those who've only read about it.
- Avoid claiming monorepos are always better — they require tooling discipline (path filtering, caching, CODEOWNERS) to work well. Without that infrastructure, they're just polyrepos in a trench coat.
