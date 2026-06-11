---
title: "Deployment Strategies — Scenarios"
topic: ci-cd
subtopic: deployment-strategies
content_type: scenario_question
tags: [ci-cd, deployment,blue-green,canary,rollback, interview, scenarios]
---

# Deployment Strategies — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Basic Scenario

**Scenario:** Your manager asks you to deploy a new version of the revenue pipeline to production. The current version is running fine. What deployment strategy do you use and why?

<details>
<summary>💡 Hint</summary>

Use a rolling deployment for a stateless pipeline worker — it's the default K8s strategy and gives you gradual rollout + automatic rollback. Always verify the rollout status and have `kubectl rollout undo` ready.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Rolling deploy — safe default for stateless pipelines
kubectl set image deployment/revenue-pipeline   container=registry/revenue-pipeline:v2.0.0

# Monitor the rollout
kubectl rollout status deployment/revenue-pipeline
# → Waiting for deployment "revenue-pipeline" rollout to finish: 1 of 4 updated

# If something looks wrong
kubectl rollout undo deployment/revenue-pipeline
# → deployment.apps/revenue-pipeline rolled back

# Verify rollback
kubectl get pods -l app=revenue-pipeline
kubectl logs <pod-name> --tail=50
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Intermediate Challenge

**Scenario:** You need to deploy a dbt model change that renames a column used by 3 downstream pipelines. How do you deploy this with zero downtime?

<details>
<summary>💡 Hint</summary>

Use a 3-phase migration: add the new column alongside the old, deploy code that reads both, then remove the old column. Never rename in one step.

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Phase 1: Add new column (backward compatible — both coexist)
ALTER TABLE orders ADD COLUMN client_id INT;
UPDATE orders SET client_id = customer_id;

-- Phase 2: Deploy all consumers to read new column
-- (each downstream pipeline updated to use client_id)
-- Both columns exist — no breaking change

-- Phase 3: After all consumers deployed, drop old column
ALTER TABLE orders DROP COLUMN customer_id;
```

```bash
# In dbt: add column alias for transition period
# models/staging/stg_orders.sql
SELECT
    customer_id,
    customer_id AS client_id,  # alias — both available downstream
    amount, status, order_date
FROM {{ source('raw', 'orders') }}
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Challenge

**Scenario:** Design a progressive delivery system for a DE platform where 50 pipelines deploy daily. Deployments must be safe, fast to roll back, and provide data quality gates before production traffic.

<details>
<summary>💡 Hint</summary>

Progressive delivery = automated gates between stages. Build an image → deploy to staging → run data quality checkpoint → canary to 10% prod → monitor for 15 min → promote to 100% or auto-rollback. Feature flags handle long-running changes.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Progressive delivery pipeline:
# 1. Build + scan image
# 2. Deploy to staging
# 3. Run data quality gate (GE checkpoint)
# 4. If passes: deploy canary (10% of pipeline runs)
# 5. Monitor error rate for 15 minutes
# 6. If clean: promote to 100%
# 7. If degraded: auto-rollback

deploy:
  needs: [quality-gate]
  steps:
    - name: Deploy canary (10%)
      run: |
        kubectl scale deployment pipeline-canary --replicas=1
        kubectl scale deployment pipeline-stable --replicas=9

    - name: Monitor canary (15 min)
      run: |
        sleep 900
        ERROR_RATE=$(query_prometheus 'rate(pipeline_errors[15m])')
        if (( $(echo "$ERROR_RATE > 0.05" | bc -l) )); then
          echo "Canary error rate too high — rolling back"
          kubectl scale deployment pipeline-canary --replicas=0
          kubectl scale deployment pipeline-stable --replicas=10
          exit 1
        fi

    - name: Promote canary to 100%
      run: |
        kubectl scale deployment pipeline-canary --replicas=10
        kubectl scale deployment pipeline-stable --replicas=0
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a rolling deployment and a blue-green deployment?**
A: Rolling updates one pod at a time (both versions run briefly in parallel). Blue-green maintains two complete environments and switches traffic atomically — instant rollback by reverting the traffic switch. Blue-green is safer but more expensive (double infrastructure cost during switch).

**Q: What is a canary deployment and when is it appropriate?**
A: Canary routes a small percentage of traffic (5-10%) to the new version before full rollout. Use it when you want to validate behavior on real traffic at low risk. Best for changes where errors are detectable in metrics within minutes.

**Q: What is `kubectl rollout undo` and when would you use it?**
A: It reverts a Deployment to its previous revision. Use it immediately when a deploy causes errors — it's faster than deploying a previous image tag and preserves K8s rollout history.

**Q: What is a feature flag and how does it relate to deployment?**
A: A feature flag is a conditional that enables/disables a feature without code deployment. It decouples code deploy from feature release — code can be in production (dark) and activated later by flipping the flag. Enables trunk-based development for long-running features.

**Q: What is a schema migration and why is it the hardest part of deploying data pipelines?**
A: Schema migrations change database structure (add/rename/drop columns). They're hard because: old code still runs during deploy (can't break it), new code expects new schema, and some changes can't be easily rolled back. The solution is backward-compatible migrations (add before drop).

**Q: How do you roll back a dbt model change in production?**
A: `git revert` the change, merge to main, and trigger the CI/CD pipeline to redeploy the reverted dbt models. If data was already corrupted, also run `dbt run --full-refresh` on affected models to recompute from source.

**Q: What is `dbt defer` and how does it enable safer deployments?**
A: `dbt defer` lets you run only changed models in CI, using the production state for unchanged upstream dependencies. This enables slim CI: `dbt run --select state:modified+ --defer --state prod-artifacts/`. Only changed models run — faster, cheaper, and focused.

---

## 💼 Interview Tips

- Lead with "every deploy needs a tested rollback path" — it frames you as someone who thinks about operational safety, not just shipping features.
- Know the three main strategies (rolling, blue-green, canary) and when each is appropriate — interviewers often ask directly.
- For dbt-specific deployment questions, mention state:modified+ and defer — they're the practical tools for efficient dbt CI and show tool-specific expertise.
- Schema migrations are the hardest part of DE deployment — discuss the 3-phase pattern (add → dual-read → drop) specifically. Many candidates miss this entirely.
- Feature flags are the answer to "how do you deploy a long-running feature" — have a concrete implementation example (dbt variable, environment variable).
- Avoid describing deployment as just `kubectl apply` — show the full lifecycle: build, test, canary, monitor, promote.
