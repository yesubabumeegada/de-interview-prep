---
title: "GitHub Actions — Scenarios"
topic: ci-cd
subtopic: github-actions
content_type: scenario_question
tags: [ci-cd, github-actions, interview, scenarios]
---

# GitHub Actions — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Add CI to a dbt Project

**Scenario:** Your team's dbt project has no CI. On every PR, models sometimes have syntax errors that only get caught after manual deploy. How do you set up a GitHub Actions workflow to automatically compile and test dbt on every PR?

<details>
<summary>💡 Hint</summary>

You need a workflow triggered on `pull_request`, that installs dbt, runs `dbt compile` (catches syntax/config errors), and optionally runs `dbt test` on the changed models. Use secrets for database credentials. Run on `ubuntu-latest`.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths: ["dbt/**"]

jobs:
  dbt-check:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Cache pip
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: pip-${{ hashFiles('requirements.txt') }}
      
      - name: Install dbt
        run: pip install -r requirements.txt
      
      - name: dbt compile
        working-directory: dbt/
        run: dbt compile --profiles-dir profiles/
        env:
          DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
      
      - name: dbt test (changed models)
        working-directory: dbt/
        run: dbt test --profiles-dir profiles/ --target ci
        env:
          DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
```

Add `DBT_SNOWFLAKE_PASSWORD` in: GitHub repo → Settings → Secrets and variables → Actions.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: CI is Too Slow

**Scenario:** Your GitHub Actions CI pipeline takes 18 minutes. The main bottlenecks are pip install (6 min) and dbt package install (4 min). How do you speed it up to under 5 minutes?

<details>
<summary>💡 Hint</summary>

The main tools are caching and running only what changed. Cache pip dependencies keyed on the requirements.txt hash — on cache hit, pip install skips. Cache dbt packages keyed on packages.yml hash. Then add path filtering so the CI only runs when relevant files change. Finally, run dbt only on changed models + their downstream dependencies.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
on:
  pull_request:
    branches: [main]
    paths:
      - "dbt/**"
      - "pipelines/**"
      - "requirements*.txt"

jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # need full history for diff

      # Cache pip — key changes only when requirements.txt changes
      - uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: pip-${{ runner.os }}-${{ hashFiles('requirements*.txt') }}
          restore-keys: pip-${{ runner.os }}-

      - run: pip install -r requirements.txt -r requirements-dev.txt

      # Cache dbt packages — key changes only when packages.yml changes
      - uses: actions/cache@v4
        with:
          path: dbt/dbt_packages/
          key: dbt-packages-${{ hashFiles('dbt/packages.yml') }}

      - run: dbt deps
        working-directory: dbt/

      # Only compile + test CHANGED models and their downstream
      - name: Run dbt on changed models only
        working-directory: dbt/
        run: |
          CHANGED=$(git diff origin/main --name-only | grep "dbt/models/" | \
            sed 's|dbt/models/||;s|.sql||' | tr '\n' ' ')
          if [ -n "$CHANGED" ]; then
            echo "Changed models: $CHANGED"
            dbt compile --select $CHANGED
            dbt test --select $CHANGED+
          else
            echo "No dbt model changes detected"
          fi
```

**Result:** 18 min → 3-4 min (cache hits + selective runs).

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Secure Multi-Environment Deployment Pipeline

**Scenario:** Design a GitHub Actions deployment pipeline that deploys to three environments (dev → staging → prod). Dev auto-deploys on merge to main. Staging requires automated smoke tests to pass. Production requires a manual approval from a senior engineer. All deployments must use OIDC (no long-lived AWS secrets).

<details>
<summary>💡 Hint</summary>

Use GitHub Environments with protection rules — dev has no protection (auto-deploy), staging has required status checks, production requires a named reviewer and uses a separate OIDC role with tighter IAM permissions. Use `needs` to chain jobs sequentially. Store environment-specific config in GitHub environment variables, not workflow-level variables.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
name: Deploy Pipeline

on:
  push:
    branches: [main]

jobs:
  deploy-dev:
    runs-on: ubuntu-latest
    environment: dev          # no protection — auto-deploys
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::111:role/DeployDev
          aws-region: us-east-1
      - run: ./deploy.sh dev

  smoke-test-dev:
    needs: deploy-dev
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/smoke/ --target dev

  deploy-staging:
    needs: smoke-test-dev      # only if smoke tests pass
    runs-on: ubuntu-latest
    environment: staging       # requires smoke-test status check
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::222:role/DeployStaging
          aws-region: us-east-1
      - run: ./deploy.sh staging

  smoke-test-staging:
    needs: deploy-staging
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/smoke/ --target staging

  deploy-prod:
    needs: smoke-test-staging
    runs-on: ubuntu-latest
    environment: production    # requires manual approval from senior-engineers group
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::333:role/DeployProd
          aws-region: us-east-1
      - run: ./deploy.sh prod
```

**GitHub environment protection config:**
```
production environment:
  ✓ Required reviewers: @senior-engineers (1 required)
  ✓ Prevent self-review
  ✓ Wait timer: 5 minutes (allows cancellation)
  ✓ Environment secrets: PROD-specific
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `on: push` and `on: pull_request` triggers?**
A: `push` fires when commits are pushed to a branch (used for deploy pipelines). `pull_request` fires when a PR is opened, updated, or synchronized (used for CI validation). PRs should use `pull_request` — it runs against the merge result, not just the branch tip.

**Q: What are GitHub Actions secrets and how do you use them securely?**
A: Secrets are encrypted values stored in GitHub (repo or org level) accessible to workflows as `${{ secrets.NAME }}`. Never echo them or include in log output. Use environment-level secrets for environment-specific credentials. Rotate regularly.

**Q: What is dependency caching in GitHub Actions and how does it work?**
A: The `actions/cache` action saves a directory (e.g., `~/.cache/pip`) keyed by a hash (e.g., `hashFiles('requirements.txt')`). On subsequent runs with the same key, the cache is restored instead of re-downloading. Cache misses fall back to a `restore-keys` prefix match.

**Q: What is OIDC authentication in GitHub Actions and why is it better than secrets?**
A: OIDC lets GitHub Actions prove its identity to cloud providers (AWS, GCP, Azure) without storing long-lived credentials as secrets. GitHub issues a short-lived OIDC token; the cloud provider exchanges it for temporary credentials. Eliminates credential rotation and reduces breach surface.

**Q: What is a GitHub Environment and how does it enable deployment gates?**
A: A GitHub Environment is a named deployment target (dev/staging/prod) with configurable protection rules: required reviewers, wait timers, and required status checks. Jobs targeting an environment pause and wait for approval before running.

**Q: How do you run different steps only on specific branches?**
A: Use the `if` condition: `if: github.ref == 'refs/heads/main'` for main-only steps. Or use `if: github.event_name == 'push'` to distinguish push from PR runs.

**Q: What is a reusable workflow and when would you create one?**
A: A reusable workflow is a workflow called by other workflows via `uses: org/repo/.github/workflows/file.yml`. Create one when you need the same multi-job CI process across multiple repositories — for example, a shared dbt test workflow used by 5 different data repos.

---

## 💼 Interview Tips

- Mention OIDC authentication as your first choice for cloud credentials in CI — it signals awareness of modern secretless auth patterns and distinguishes you from candidates who only know long-lived access keys.
- When discussing slow CI, lead with caching and path filtering as the two highest-ROI fixes — interviewers at large companies care deeply about CI speed as a developer productivity metric.
- For multi-environment deployment questions, specifically describe the GitHub Environments feature with required reviewers — it's the practical implementation detail most candidates don't know.
- Connect GitHub Actions to the broader DE workflow: PR triggers dbt compile → merge triggers data quality tests → scheduled triggers for nightly backfill jobs. Show you think of it as a platform, not just a test runner.
- Avoid claiming you'd store database credentials directly in workflow files or as unencrypted environment variables — it's a hard red flag for security-conscious teams.
- For senior roles, discuss composite actions or reusable workflows as the mechanism for sharing CI logic across repositories — showing you think about platform standardization.
