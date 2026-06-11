---
title: "Git Fundamentals - Intermediate"
topic: git-and-github
subtopic: git-fundamentals
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, github, rebase, cherry-pick, reflog, bisect]
---

# Git Fundamentals — Intermediate

## Rebase Deep Dive

```bash
# Interactive rebase: edit, squash, reorder last 3 commits
git rebase -i HEAD~3

# In the editor:
# pick abc1234 feat: add extract step
# squash def5678 fix typo in extract step     ← squash into above
# pick ghi9012 feat: add transform step

# Result: 2 clean commits instead of 3

# Rebase feature branch onto updated main
git checkout feature/new-transform
git fetch origin
git rebase origin/main
# If conflicts:
git status                    # see conflicted files
# Edit files to resolve
git add resolved_file.py
git rebase --continue         # continue after each conflict
# Or abort:
git rebase --abort
```

**Golden rule:** Never rebase commits already pushed to a shared branch.

---

## Cherry-Pick: Take One Commit

```bash
# Scenario: hotfix on main, need it in your feature branch too
git log main --oneline
# → a1b2c3d fix: correct tax calculation bug
# → ...

git checkout feature/my-work
git cherry-pick a1b2c3d
# Applies that one commit onto your branch

# Cherry-pick a range
git cherry-pick a1b2c3d..e5f6g7h
```

---

## Stash: Temporary Shelving

```bash
# You're mid-work, need to switch branches urgently
git stash                         # save current changes
git stash push -m "wip: revenue transform"  # named stash

git checkout hotfix/urgent-bug    # switch branch
# ... do urgent work, commit ...
git checkout feature/revenue      # back to your branch

git stash list                    # see all stashes
git stash pop                     # restore latest stash (removes it)
git stash apply stash@{1}         # restore specific (keeps in list)
git stash drop stash@{0}          # delete a stash
```

---

## Reflog: The Safety Net

```bash
# Accidentally deleted a branch? Lost commits after hard reset?
# Reflog records every HEAD movement
git reflog

# Output:
# abc1234 HEAD@{0}: reset: moving to HEAD~1
# def5678 HEAD@{1}: commit: feat: add revenue DAG
# ghi9012 HEAD@{2}: checkout: moving from main to feature

# Recover the "lost" commit
git checkout -b recovery-branch def5678
# or
git reset --hard def5678
```

---

## Git Bisect: Find the Bug Commit

```bash
# Your pipeline was working 2 weeks ago, broken now.
# 50 commits in between. Which one broke it?

git bisect start
git bisect bad                    # current commit is broken
git bisect good v2.3.0            # this tag was good

# Git checks out middle commit
# Test your pipeline, then:
git bisect good   # or
git bisect bad

# Git narrows it down (log2(50) ≈ 6 checks)
# At the end:
# "abc1234 is the first bad commit"
git bisect reset                  # back to HEAD
```

---

## Git Worktree: Multiple Branches at Once

```bash
# Work on a hotfix without losing current working state
git worktree add ../hotfix-work hotfix/urgent-fix

# Now you have two working directories:
# ~/project/           ← your feature branch
# ~/hotfix-work/       ← hotfix branch

# After hotfix done:
git worktree remove ../hotfix-work
```

---

## Advanced .gitattributes for DE Projects

```gitattributes
# .gitattributes

# Normalize line endings
* text=auto

# Treat large SQL files as binary (no diff noise)
*.sql diff=sql

# Ensure notebooks are diffable
*.ipynb diff=jupyternotebook

# Lock binary files — no merge conflicts
*.parquet binary
*.xlsx binary
*.png binary

# Use custom merge driver for dbt manifest
dbt_project.yml merge=ours
```

---

## Common Intermediate Mistakes

| Mistake | Correct approach |
|---|---|
| `git add .` always | Stage specific files; review `git diff --staged` |
| Huge commits | Commit one logical change at a time |
| Rebase shared branches | Only rebase local/unshared branches |
| Hardcoded credentials committed | Use pre-commit hook with `detect-secrets` |
| Resolving merge conflicts by accepting all theirs | Read each conflict carefully |
| Force-pushing to main | Protect main branch; never force push |
