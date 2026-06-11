---
title: "Git Hooks and Automation — Scenarios"
topic: git-and-github
subtopic: git-hooks-and-automation
content_type: scenario_question
tags: [git, github, git-hooks-and-automation, interview, scenarios]
---

# Git Hooks and Automation — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Getting Started

**Scenario:** Your team asks you to set up pre-commit hooks to prevent secrets and enforce code formatting on the team's DE monorepo. Describe your approach.

<details>
<summary>💡 Hint</summary>

Start with detect-secrets (highest business value) and ruff (formatting). Use the pre-commit framework to manage hooks. Add to CI as well so it catches bypasses.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.3.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
```

```bash
# Team setup (one time):
pip install pre-commit
detect-secrets scan > .secrets.baseline
git add .pre-commit-config.yaml .secrets.baseline
git commit -m 'ci: add pre-commit hooks'
pre-commit install
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling a Real Problem

**Scenario:** A developer bypassed pre-commit hooks using `--no-verify` and committed a database password. How do you respond and prevent future bypasses?

<details>
<summary>💡 Hint</summary>

Rotate the credential immediately (it's exposed). Review who has repo access. Then enforce hooks in CI (pre-commit run --all-files in GitHub Actions) so bypassing local hooks still fails in CI.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Rotate the credential immediately
# Login to database, change password, update secrets manager

# Step 2: Remove from git history
git filter-repo --replace-text <(echo 'old_password==>REDACTED')
git push --force-with-lease  # after team coordination

# Step 3: CI enforcement (can't be bypassed by --no-verify)
# .github/workflows/security.yml
- name: Detect secrets
  run: |
    pip install detect-secrets
    detect-secrets scan --baseline .secrets.baseline
    detect-secrets audit .secrets.baseline
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design at Scale

**Scenario:** Design an automated code quality and security enforcement system for a DE platform team with 40 engineers across 10 teams, some of whom regularly bypass local hooks.

<details>
<summary>💡 Hint</summary>

Local hooks are the first line but not the last. CI must enforce the same checks — hooks are local opt-in, CI is mandatory. Also add branch protection requiring CI checks to pass. For secrets: periodic scanning of all branches, not just on commit.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Three layers of enforcement:
# 1. Local hooks (fast, developer-friendly) — pre-commit
# 2. CI checks (mandatory, can't bypass) — GitHub Actions
# 3. Periodic scanning (catches drift) — scheduled workflow

# Layer 2: CI enforcement (mandatory)
name: Security and Quality
on: [push, pull_request]
jobs:
  quality:
    steps:
      - run: pre-commit run --all-files   # same hooks as local

  secrets-scan:
    steps:
      - uses: gitleaks/gitleaks-action@v2  # scans entire git history
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

# Layer 3: Scheduled full-history scan
on:
  schedule:
    - cron: '0 6 * * *'  # Daily 6 AM
jobs:
  full-scan:
    steps:
      - uses: gitleaks/gitleaks-action@v2
        with:
          config: .gitleaks.toml
          # Scans ALL commits, not just HEAD
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the pre-commit framework and how does it differ from writing hooks manually?**
A: The pre-commit framework manages hook installation, versioning, and execution across a team. Instead of writing bash scripts in `.git/hooks/` (not tracked in git, not shareable), you declare hooks in `.pre-commit-config.yaml` (committed to git, same for everyone).

**Q: What is detect-secrets and how does it prevent credential leaks?**
A: detect-secrets scans staged files for patterns that match secrets (AWS keys, passwords, API tokens). It maintains a `.secrets.baseline` file of known false positives. New secrets trigger a hook failure and block the commit. It runs in under 1 second locally.

**Q: What is the difference between a pre-commit hook and a pre-push hook?**
A: Pre-commit runs before each commit is created — fast checks (lint, format, detect-secrets). Pre-push runs before `git push` — can be slower (unit tests, dbt compile). Pre-push catches issues before they reach the remote, where others might see them.

**Q: How do you prevent developers from bypassing hooks with `--no-verify`?**
A: You can't prevent it locally — it's a developer's machine. Instead, run the same hooks in CI (GitHub Actions): `pre-commit run --all-files`. CI can't be bypassed and blocks the PR. Local hooks are the fast developer experience; CI is the enforcement.

**Q: What is SQLFluff and how does it relate to git hooks?**
A: SQLFluff is a SQL linter and formatter. In a DE team's pre-commit config, `sqlfluff-fix` automatically formats SQL files and `sqlfluff-lint` enforces SQL style rules. This keeps dbt SQL models consistently formatted without manual style reviews.

**Q: What is a commit-msg hook used for?**
A: A commit-msg hook validates the commit message format before the commit is created. Use it to enforce conventional commit format (`feat:`, `fix:`, `chore:`) which enables automated changelog generation and semantic versioning.

**Q: How do you roll out pre-commit hooks to a team that has never used them?**
A: Add `.pre-commit-config.yaml` and `.secrets.baseline` to the repo. Update the README with setup instructions (`pip install pre-commit && pre-commit install`). Run `pre-commit run --all-files` in CI from day one — this enforces the rules even for developers who haven't set up hooks locally. Address initial violations together in a cleanup PR.

---

## 💼 Interview Tips

- Lead with detect-secrets as the highest-value hook for DE teams — it prevents incidents that require credential rotation, security audits, and breach notifications.
- Always mention that local hooks must be paired with CI enforcement — `--no-verify` bypasses can't be stopped locally, but CI ensures the same checks run on every PR.
- SQLFluff for SQL formatting is a DE-specific detail that shows you've thought about code quality beyond Python — many DE teams don't know about it.
- For the pre-commit framework specifically, mention that hooks are defined in a committed YAML file — this makes them shareable and version-controlled, unlike raw `.git/hooks/` scripts.
- Frame hooks as developer experience, not just enforcement — fast feedback (lint error in 0.5 seconds locally vs. 4 minutes in CI) is what makes developers keep hooks installed.
- Avoid framing `--no-verify` as a big problem — it's a known escape hatch. The right response is CI enforcement, not trying to block local bypasses (which are sometimes legitimately needed).
