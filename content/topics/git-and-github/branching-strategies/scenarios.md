---
title: "Branching Strategies — Scenarios"
topic: git-and-github
subtopic: branching-strategies
content_type: scenario_question
tags: [git, branching, interview, scenarios]
---

# Branching Strategies — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Choosing a Branch Strategy for a New Team

**Scenario:** You're joining a 4-person DE team starting a new dbt + Airflow project. The tech lead asks you to recommend a branching strategy. What do you recommend and why?

<details>
<summary>💡 Hint</summary>

For a small team (4 people) working on continuous data pipelines, Gitflow's complexity is overhead without benefit. Trunk-based development with short-lived branches and required PR reviews is the right fit. The key arguments: small team means fewer parallel branches = fewer conflicts; DE pipelines should deploy frequently; Gitflow's release branch model adds latency that doesn't fit continuous pipeline delivery.

</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: Trunk-Based Development**

```
Rationale:
1. Small team (4 people) → low conflict risk, no need for develop branch buffer
2. Data pipelines should be deployable anytime → continuous delivery mindset
3. Gitflow overhead (release branches, hotfix process) > benefit at this scale
4. Short branches = less merge pain
```

**Setup:**
```bash
# main is the single source of truth
# All work in short-lived feature branches

# Naming convention
feat/DE-101-add-revenue-model
fix/DE-202-null-handling
chore/update-dependencies

# Branch protection (GitHub settings)
# ✓ Require PR
# ✓ 1 reviewer required
# ✓ CI must pass (dbt compile + dbt test)
# ✓ Auto-delete branch after merge
```

**When to revisit:** If the team grows to 15+ people or if you need a formal quarterly release cycle with a compliance hold period, reconsider Gitflow or a scaled trunk approach.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Long-Running Feature Needs to Ship Partially

**Scenario:** You're building a new customer segmentation model in dbt that will take 6 weeks to complete. Your tech lead doesn't want to keep a 6-week feature branch. How do you handle this?

<details>
<summary>💡 Hint</summary>

Feature flags let you merge incomplete work to main without activating it. Split the 6-week work into smaller mergeable chunks: schema changes first, then incremental logic, then the final activation. Each chunk merges to main (and thus deploys) but the feature is inactive until the flag is turned on. This keeps branches short (2-3 days max) while the feature develops over 6 weeks.

</details>

<details>
<summary>✅ Solution</summary>

**Week 1-2: Schema and infrastructure (merge early)**
```bash
git checkout -b feat/DE-501-add-segmentation-schema
# Add empty table, create columns — no logic yet
# Merge to main: safe, nothing runs yet
git push && gh pr create
```

**Week 2-4: Core logic behind feature flag**
```python
# dbt: conditional model activation
{% if var('enable_customer_segmentation', false) %}
SELECT customer_id, rfm_score, segment
FROM {{ ref('int_customer_rfm') }}
{% else %}
SELECT null::INT AS customer_id, null::FLOAT AS rfm_score, null::TEXT AS segment
WHERE 1=0  -- empty model when disabled
{% endif %}
```

```bash
# Each piece merges to main (2-3 day branches)
git checkout -b feat/DE-502-rfm-scoring
# ... work, test locally ...
git push && gh pr create  # merge after 2 days

git checkout -b feat/DE-503-segmentation-labels
# ... merge after 2 days ...
```

**Week 5-6: Testing and activation**
```bash
# Enable in staging first
DBT_VAR_ENABLE_CUSTOMER_SEGMENTATION=true dbt run --target staging

# After 1 week of validation → enable in production
# No code change needed — just environment variable
```

This pattern is called "branch by abstraction" — the code is always in main, but the behavior is toggled.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Branching Strategy for 30-Team Data Mesh

**Scenario:** Your company has 30 domain teams, each with their own dbt models in a monorepo. Teams release on different cadences (some daily, some weekly). Design a branching strategy that lets teams work independently without blocking each other.

<details>
<summary>💡 Hint</summary>

In a monorepo with 30 teams, the key problem is blast radius — one team's bad merge shouldn't break another team's CI. Use trunk-based development at the monorepo level (all PRs go to main), but add CI path filtering so each PR only tests its own changed domain. Add a CODEOWNERS file so each domain team owns their directory and auto-reviews their own PRs. Release independence is achieved through feature flags, not separate branches.

</details>

<details>
<summary>✅ Solution</summary>

**CODEOWNERS for domain isolation:**
```gitattributes
# .github/CODEOWNERS
dbt/models/finance/           @finance-de-team
dbt/models/marketing/         @marketing-de-team
dbt/models/operations/        @operations-de-team
dbt/models/shared/            @platform-de-team  # platform owns shared models

# Airflow DAGs by domain
dags/finance/                 @finance-de-team
dags/marketing/               @marketing-de-team
```

**CI: Only test affected domain on PR**
```yaml
jobs:
  detect-domain:
    outputs:
      domains: ${{ steps.detect.outputs.domains }}
    steps:
      - id: detect
        run: |
          DOMAINS=$(git diff origin/main --name-only | \
            grep "dbt/models/" | cut -d'/' -f3 | sort -u | jq -R -s -c 'split("\n")[:-1]')
          echo "domains=$DOMAINS" >> $GITHUB_OUTPUT

  test-domain:
    needs: detect-domain
    strategy:
      matrix:
        domain: ${{ fromJSON(needs.detect-domain.outputs.domains) }}
    steps:
      - run: dbt test --select models/${{ matrix.domain }}+
```

**Release independence:**
```python
# Feature flags per domain team
FEATURE_FLAGS = {
    f"finance.{model}": bool(os.getenv(f"FF_FINANCE_{model.upper()}", "")),
    f"marketing.{model}": bool(os.getenv(f"FF_MARKETING_{model.upper()}", "")),
}
# Each team controls their own flags in their environment config
# No team blocks another team's release
```

**Platform team owns:**
```
- Branch protection rules (no one bypasses)
- Shared CODEOWNERS enforcement
- Cross-domain model contracts (schema validation)
- main branch CI health dashboard
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between Gitflow and trunk-based development?**
A: Gitflow uses multiple long-lived branches (main, develop, release, feature, hotfix) with formal merge ceremonies between them. Trunk-based development keeps one primary branch (main/trunk) and uses very short-lived feature branches (1-3 days) that merge frequently. Trunk-based reduces merge conflicts and delivers faster.

**Q: What is a feature flag and how does it enable trunk-based development for long features?**
A: A feature flag is a conditional in code (e.g., `if os.getenv("FF_NEW_FEATURE") == "true"`) that controls whether a feature is active. It allows merging incomplete work to main without activating it, so branches stay short while features take weeks to develop.

**Q: What are branch protection rules and why are they essential?**
A: Branch protection rules on the main branch enforce: required PR before merge, required CI checks passing, and required reviewer approvals. They prevent direct commits to main, ensuring all changes go through review and testing. Essential for team safety.

**Q: When would you use a hotfix branch?**
A: When production is broken and you need to patch it immediately without merging incomplete work from develop. Hotfix branches from main, is fixed and tested quickly, then merged back to both main AND develop (or the current feature branch in trunk-based). In trunk-based, a hotfix is just a very short branch.

**Q: What is squash-and-merge and when is it appropriate?**
A: Squash-and-merge combines all commits in a PR into one commit on the target branch. It keeps main's history clean (one commit per feature/fix) at the cost of losing the granular commit history from the branch. Appropriate for small to medium features; less appropriate for large long-running features where history detail matters.

**Q: How do you handle a situation where two teams modified the same dbt model on separate branches?**
A: The second team to merge gets a merge conflict. They should fetch latest main, rebase their branch, read both changes carefully, combine them if both are valid (e.g., one added a column, one fixed a filter), run dbt compile + dbt test, then merge. Communication between teams before both modify the same model is better than fixing conflicts.

---

## 💼 Interview Tips

- Lead with trunk-based development as your recommendation for DE teams unless asked about a specific context — it shows you know modern delivery practices.
- Justify your recommendation with DE-specific arguments: pipelines deploy frequently, short feedback loops matter, long branches mean late integration testing.
- Feature flags are the answer to "but what if a feature takes 6 weeks?" — have a concrete explanation of how to implement them in dbt (variables) or Python (env vars).
- For monorepo scenarios, bring up CODEOWNERS and path-filtered CI as the mechanisms that give teams independence — most candidates don't know these details.
- Avoid recommending Gitflow for small teams (< 10 engineers) working on continuously-deployed pipelines — it signals you're applying a pattern without evaluating fit.
- Know the hotfix pattern regardless of which main strategy you use — it shows operational maturity and that you've thought about emergency production scenarios.
