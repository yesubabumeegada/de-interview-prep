---
title: "Git Hooks and Automation - Intermediate"
topic: git-and-github
subtopic: git-hooks-and-automation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, github, git-hooks-and-automation]
---

# Git Hooks and Automation — Intermediate

```python
# commit-msg hook: enforce conventional commit format
#!/usr/bin/env python3
import sys, re

commit_msg = open(sys.argv[1]).read().strip()

PATTERN = r'^(feat|fix|chore|refactor|test|ci|docs)(\(.+\))?: .{10,72}$'
EXAMPLES = [
    'feat(revenue): add daily aggregation model',
    'fix(orders): handle null customer_id',
    'chore: update dbt to 1.7.0',
]

if not re.match(PATTERN, commit_msg.split('
')[0]):
    print(f'❌ Commit message does not follow conventional commits.')
    print(f'   Got: {commit_msg}')
    print(f'   Examples: {chr(10).join(EXAMPLES)}')
    sys.exit(1)
```

## pre-push Hook: Run Tests

```bash
#!/bin/bash
# .git/hooks/pre-push

echo "Running unit tests before push..."
pytest tests/ -m unit -q

if [ 0 -ne 0 ]; then
    echo "Tests failed. Push aborted."
    exit 1
fi

echo "Tests passed. Proceeding with push."
```

## CI Also Runs pre-commit

```yaml
# Run the same hooks in CI — catches bypassed hooks
- name: Run pre-commit
  run: |
    pip install pre-commit
    pre-commit run --all-files
```
