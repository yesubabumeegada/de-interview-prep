---
title: "Git Hooks and Automation - Real World"
topic: git-and-github
subtopic: git-hooks-and-automation
content_type: real_world_example
tags: [git, github, git-hooks-and-automation, real-world]
---

# Git Hooks and Automation — Real World

## Case Study: Secrets Committed 47 Times in One Year

A startup DE team committed AWS credentials, database passwords, and API keys to git 47 times in one year. Each required emergency key rotation, audit of who accessed the repo, and security incident documentation.

**After adding detect-secrets pre-commit hook:** Zero credential commits in the following 12 months. The hook ran in under 1 second and blocked commits before they ever reached the remote. Setup time: 30 minutes for the whole team.

```bash
# Setup that eliminated the problem:
pip install pre-commit detect-secrets
detect-secrets scan > .secrets.baseline  # baseline existing secrets
pre-commit install  # hooks on every commit

# What engineers see when they try to commit a secret:
# Detect secrets...............................Failed
# Secret detected: AWS Access Key in pipeline.py
# Blocked — rotate the key, don't commit it.
```
