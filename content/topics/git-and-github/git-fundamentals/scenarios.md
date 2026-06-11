---
title: "Git Fundamentals — Scenarios"
topic: git-and-github
subtopic: git-fundamentals
content_type: scenario_question
tags: [git, github, interview, scenarios, version-control]
---

# Git Fundamentals — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: You Committed a Secret to Git

**Scenario:** You accidentally committed a file containing a database password to your GitHub repository (public repo). The commit was pushed 10 minutes ago. What do you do?

<details>
<summary>💡 Hint</summary>

Assume the secret is already compromised — it was exposed for 10 minutes on a public repo and could have been indexed by GitHub's secret scanning or by bots. Your first action is to rotate the secret (change the password), not to remove it from git. Removing from git history is secondary — it limits future exposure but doesn't undo past exposure. Never assume "nobody saw it" just because the repo is small.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Rotate the credential immediately**
```
→ Log into your database and change the password NOW
→ Revoke any API keys that were exposed
→ The old credential is compromised regardless of git history cleanup
```

**Step 2: Remove from git history**
```bash
# Remove the file from git history entirely
git filter-repo --path secrets.yaml --invert-paths

# Or for a specific string in a file:
git filter-repo --replace-text <(echo 'password=supersecret==>password=REDACTED')

# Force push the rewritten history
git push origin main --force
```

**Step 3: Add to .gitignore**
```bash
echo "secrets.yaml" >> .gitignore
echo ".env" >> .gitignore
git add .gitignore
git commit -m "chore: ignore secrets files"
```

**Step 4: Prevent recurrence**
```bash
# Add detect-secrets pre-commit hook
pip install detect-secrets
detect-secrets scan > .secrets.baseline
# Add to .pre-commit-config.yaml:
# - repo: https://github.com/Yelp/detect-secrets
#   hooks: [id: detect-secrets]
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Merge Conflict in a dbt Model

**Scenario:** You and a colleague both modified `models/gold/revenue_daily.sql` on separate branches. When you merge your branch into main, you get a merge conflict. Walk through resolving it safely.

<details>
<summary>💡 Hint</summary>

First understand what both versions are trying to do — don't just accept "yours" or "theirs" blindly. Open the conflicted file and read both versions. Often the right resolution is to keep both changes (e.g., you added a new column, they fixed a filter). After resolving, run `dbt compile` and `dbt test` to verify the model is valid before committing.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# After git pull or git merge creates conflict:
git status
# → both modified: models/gold/revenue_daily.sql

# Open the file — conflict markers look like:
```

```sql
SELECT
  order_date,
<<<<<<< HEAD
  SUM(amount) AS total_revenue,
  COUNT(*) AS order_count
=======
  SUM(amount * (1 - discount)) AS total_revenue,
  COUNT(*) AS order_count,
  AVG(amount) AS avg_order_value
>>>>>>> feature/add-avg-order-value
FROM orders
WHERE status = 'completed'
GROUP BY order_date
```

```sql
-- Correct resolution: keep BOTH changes
SELECT
  order_date,
  SUM(amount * (1 - discount)) AS total_revenue,  -- colleague's fix
  COUNT(*) AS order_count,
  AVG(amount) AS avg_order_value                   -- my addition
FROM orders
WHERE status = 'completed'
GROUP BY order_date
```

```bash
# After editing:
git add models/gold/revenue_daily.sql

# Verify it compiles correctly
dbt compile --select revenue_daily
dbt test --select revenue_daily

# Commit the resolution
git commit -m "merge: resolve conflict in revenue_daily — keep discount + avg_order_value"
```

**Communication:** Message your colleague: "Merged your discount fix with my avg_order_value addition — please verify the resolution looks correct."

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Git Workflow for a 20-Person DE Team

**Scenario:** Your company has 20 data engineers working on a shared dbt + Airflow monorepo. Currently everyone commits to main. There are frequent conflicts, broken pipelines from half-finished work, and no audit trail for what changed when. Design a Git workflow.

<details>
<summary>💡 Hint</summary>

For a team this size, trunk-based development (with very short-lived branches) works better than Gitflow — it minimizes long-lived branches and merge conflicts while still protecting main. The key controls are: branch protection (PRs required), CI that runs on every PR, small frequent commits (not week-long feature branches), and feature flags for work-in-progress so code can merge without going live. The audit trail problem is solved by enforcing conventional commit messages and linking PRs to tickets.

</details>

<details>
<summary>✅ Solution</summary>

**Trunk-based development for DE:**
```
main ← protected, always deployable
  ↑
feat/DE-101-revenue-v2  (max 2-3 days, then merge)
fix/DE-234-null-handling (hours, same-day merge)
```

**Branch protection rules (GitHub settings):**
```
✓ Require pull request (1 review minimum)
✓ Required status checks: dbt-compile, dbt-test, dag-import-test
✓ Require branches to be up to date before merging
✓ Restrict who can push to main: only CI service account
✓ Delete head branches on merge (keep list clean)
```

**PR checklist template (.github/pull_request_template.md):**
```markdown
## Changes
- [ ] What changed?
- [ ] Why?

## Testing
- [ ] dbt tests pass locally (`dbt test --select <changed_models>+`)
- [ ] DAG import check passes
- [ ] Downstream impact reviewed (list affected models)

## Rollback
- How to revert if this breaks production?
```

**Conventional commits via commit-msg hook:**
```bash
# Enforces: feat|fix|chore|refactor|test|ci: message
commit_pattern='^(feat|fix|chore|refactor|test|ci|docs)\(.+\)?: .{10,72}$'
if ! echo "$1" | grep -qE "$commit_pattern"; then
  echo "Commit message must match: type(scope): description"
  exit 1
fi
```

**Deployment audit trail:**
```bash
# Tag every production deploy
git tag -a prod-2024-01-15-v2 -m "Deploy: revenue v2 — DE-101"
git push --tags

# Query history by tag range
git log prod-2024-01-14-v1..prod-2024-01-15-v2 --oneline
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `git merge` and `git rebase`?**
A: Merge creates a new merge commit preserving both branch histories. Rebase replays your commits on top of the target branch creating a linear history. Use merge to integrate feature branches into main; use rebase to update your feature branch with latest main. Never rebase shared/public branches.

**Q: What does `git revert` do and how does it differ from `git reset`?**
A: `git revert` creates a new commit that undoes a previous commit — it's safe for shared branches because it doesn't rewrite history. `git reset` moves the branch pointer backwards, rewriting history — only safe on local/private branches.

**Q: How would you recover a branch you accidentally deleted?**
A: Use `git reflog` to find the SHA of the last commit on that branch (reflog records every HEAD movement), then `git checkout -b recovered-branch <sha>` to recreate it.

**Q: What is the purpose of `git stash`?**
A: Stash temporarily shelves uncommitted changes so you can switch branches. `git stash push` saves current changes; `git stash pop` restores them. Useful when you need to switch context urgently without making a work-in-progress commit.

**Q: What information should a good commit message contain?**
A: A type (feat/fix/chore), a short imperative description (<72 chars), and optionally a body explaining why (not what). Example: `fix(revenue): exclude pending orders from daily aggregate`. The why matters for future readers looking at git blame.

**Q: What is `.gitignore` and what should DE projects always include in it?**
A: `.gitignore` lists files and patterns Git should not track. DE projects should always ignore: `.env` / secrets files, `__pycache__/`, virtual environments (`.venv/`), large data files (`*.csv`, `*.parquet`), dbt `target/` and `dbt_packages/`, Jupyter `.ipynb_checkpoints/`, and Airflow `logs/`.

**Q: What is `git bisect` and when would you use it?**
A: `git bisect` performs a binary search through commit history to find which commit introduced a bug. Mark the current commit as `bad` and an older known-good commit as `good` — Git checks out the midpoint for you to test, and you repeat until the culprit commit is identified.

---

## 💼 Interview Tips

- When asked about merge conflicts, always emphasize understanding both changes before resolving — accepting blindly is worse than the conflict itself.
- Connect credential leak scenarios to rotation first, then cleanup — interviewers testing security awareness want to hear rotation before git history rewriting.
- For senior workflow questions, mention trunk-based development as your default recommendation with reasoning — it signals awareness of modern practices beyond Gitflow.
- Mention branch protection rules specifically (required PR, required CI checks, required reviews) — they are the practical enforcement mechanism most interviewers haven't heard candidates describe in detail.
- For DE-specific git practices, bring up: `.gitignore` for data files, Git LFS for large assets, conventional commits for audit trails, and tagging production deployments.
- Avoid describing "everyone commits to main" as acceptable for any team larger than 2 people — it signals low maturity in delivery practices.
