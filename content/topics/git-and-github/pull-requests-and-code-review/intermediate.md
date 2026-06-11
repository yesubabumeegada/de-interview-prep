---
title: "Pull Requests and Code Review - Intermediate"
topic: git-and-github
subtopic: pull-requests-and-code-review
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, github, pull-requests, codeowners, review-automation]
---

# Pull Requests and Code Review — Intermediate

## CODEOWNERS

```gitattributes
# .github/CODEOWNERS
# These owners are automatically added as reviewers

# Platform team owns all shared infrastructure
*                         @data-platform-team

# Domain teams own their models
dbt/models/finance/       @finance-de @data-platform-team
dbt/models/marketing/     @marketing-de @data-platform-team

# Senior sign-off required for governance changes
dbt/models/gold/          @data-platform-lead
dbt/macros/               @data-platform-lead

# Schema contracts require architecture review
dbt/models/shared/        @data-architect
```

---

## Automated PR Checks

```yaml
# .github/workflows/pr-checks.yml
name: PR Checks

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  validate-pr-title:
    runs-on: ubuntu-latest
    steps:
      - name: Check conventional commit format
        run: |
          TITLE="${{ github.event.pull_request.title }}"
          PATTERN="^(feat|fix|chore|refactor|test|ci|docs)(\(.+\))?: .{10,72}$"
          if ! echo "$TITLE" | grep -qE "$PATTERN"; then
            echo "PR title must follow: type(scope): description"
            echo "Got: $TITLE"
            exit 1
          fi

  check-pr-size:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Check PR size
        run: |
          LINES=$(git diff origin/main --stat | tail -1 | grep -o '[0-9]* insertion' | grep -o '[0-9]*')
          if [ "${LINES:-0}" -gt 600 ]; then
            echo "::warning::This PR adds $LINES lines. Consider splitting into smaller PRs."
          fi
```

---

## PR Templates

```markdown
<!-- .github/pull_request_template.md -->
## Summary
<!-- 1-3 sentences: what changed and why -->


## Type of Change
- [ ] Bug fix
- [ ] New feature / pipeline
- [ ] Refactoring (no behavior change)
- [ ] Infrastructure / CI change

## Testing Done
- [ ] Unit tests pass (`pytest tests/ -v`)
- [ ] dbt compile passes
- [ ] dbt tests pass on dev target
- [ ] Manually verified output on sample data

## Downstream Impact
<!-- List models/dashboards affected by this change -->
- None / List them here

## Rollback Instructions
<!-- How to revert if this causes issues -->
`git revert <sha>` / specific steps

## Screenshots
<!-- Add query results or dashboard screenshots if relevant -->
```

---

## Review Automation with GitHub Actions

```yaml
# Auto-assign reviewers based on changed files
- name: Auto-assign reviewer
  uses: actions/github-script@v7
  with:
    script: |
      const changedFiles = await github.rest.pulls.listFiles({
        owner: context.repo.owner,
        repo: context.repo.repo,
        pull_number: context.issue.number,
      });
      
      const hasFinanceModels = changedFiles.data
        .some(f => f.filename.startsWith('dbt/models/finance/'));
      
      if (hasFinanceModels) {
        await github.rest.pulls.requestReviewers({
          owner: context.repo.owner,
          repo: context.repo.repo,
          pull_number: context.issue.number,
          team_reviewers: ['finance-de-team'],
        });
      }
```

---

## Stale PR Management

```yaml
# .github/workflows/stale.yml
name: Mark stale PRs
on:
  schedule:
    - cron: "0 9 * * 1"  # Monday 9 AM

jobs:
  stale:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/stale@v9
        with:
          stale-pr-message: "This PR has been inactive for 14 days. Please update or close it."
          close-pr-message: "Closing PR after 7 more days of inactivity."
          days-before-stale: 14
          days-before-close: 7
          exempt-pr-labels: "do-not-close,in-progress"
```
