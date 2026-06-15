---
title: "Data Pipeline Versioning - Senior Deep Dive"
topic: ci-cd
subtopic: data-pipeline-versioning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ci-cd, versioning, data-contracts, changelog-automation, breaking-changes, immutable-datasets, pipeline-registry]
---

# Data Pipeline Versioning — Senior Deep Dive

## Breaking vs. Non-Breaking Pipeline Changes: A Taxonomy

Senior engineers must have a precise mental model of what constitutes a breaking change in a data pipeline context. Unlike API versioning where a breaking change is purely syntactic, data pipeline breaking changes can be semantic (the data means something different even if the schema is unchanged).

### Schema-Level Breaking Changes

| Change Type | Breaking? | Mitigation Strategy |
|-------------|-----------|---------------------|
| Remove column | Yes — hard break | Never remove; deprecate → sunset period → remove |
| Rename column | Yes — hard break | Add new column, deprecate old, copy data, remove after migration |
| Change data type (incompatible) | Yes — hard break | Add new column with new type, migrate consumers |
| Change data type (widening) | No | INT → BIGINT is safe |
| Add nullable column | No | Backward compatible |
| Add non-nullable column without default | Yes | Add with a default, then backfill |
| Change primary key | Yes | New table version |
| Reorder columns (positional) | Context-dependent | Parquet/Delta: no. CSV with positional reads: yes |

### Semantic Breaking Changes

These are invisible in the schema but break consumer expectations:

```sql
-- V1 — revenue counts ALL orders
-- Revenue metric: $10M/month

-- V2 — "fix": exclude internal test orders
-- Revenue metric: $9.8M/month
-- Dashboards show a sudden 2% drop — was it a real decline or a pipeline change?
SELECT
    SUM(order_total) as revenue
FROM orders
WHERE is_test_order = false  -- Added in V2 — SEMANTIC BREAKING CHANGE
```

Semantic changes must be treated as breaking changes even when the schema is identical. The data contract must capture the business logic, not just the schema:

```yaml
# data-contracts/revenue-v3.0.0.yaml
version: 3.0.0
changelog:
  - version: "3.0.0"
    type: major  # BREAKING: semantic change in revenue calculation
    description: "Exclude test orders from revenue. Historical data reprocessed for 2023+."
    migration_guide: |
      If you're comparing v2.x and v3.x revenue figures, note that v3.x
      excludes internal test orders (~2% of order volume). Compare v3.x
      against reprocessed v3.x history, not raw v2.x figures.
```

## Changelog Automation with Conventional Commits

**Conventional Commits** is a specification for commit message formatting that enables automated changelog generation and semantic version bumping:

```
<type>[optional scope]: <description>

feat(orders): add discount_amount field to orders model
fix(customers): correct null handling in email normalization
feat(revenue)!: exclude test orders — BREAKING CHANGE in calculation logic
perf(spark): optimize join strategy in large_orders model
docs(ltv): add documentation for customer LTV calculation
refactor(staging): move staging models to staging schema
```

- `feat`: new feature → bumps MINOR version
- `fix`: bug fix → bumps PATCH version  
- `feat!` or `BREAKING CHANGE` in footer: → bumps MAJOR version
- `perf`, `refactor`, `docs`, `chore`: no version bump (unless configured)

### Automated Changelog with release-please

```yaml
# .github/workflows/release.yml
name: Release Please

on:
  push:
    branches: [main]

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: google-github-actions/release-please-action@v4
        id: release
        with:
          release-type: simple
          package-name: data-platform
          changelog-types: |
            [
              {"type":"feat","section":"Features","hidden":false},
              {"type":"fix","section":"Bug Fixes","hidden":false},
              {"type":"perf","section":"Performance","hidden":false},
              {"type":"feat!","section":"BREAKING CHANGES","hidden":false}
            ]

      # On release, upload dbt manifest to versioned S3 path
      - name: Tag production manifest
        if: ${{ steps.release.outputs.release_created }}
        run: |
          VERSION=${{ steps.release.outputs.tag_name }}
          aws s3 cp ./target/manifest.json \
            s3://dbt-artifacts/releases/${VERSION}/manifest.json
```

This creates GitHub releases automatically with changelogs like:

```markdown
## [3.0.0] - 2024-01-15

### BREAKING CHANGES
* exclude test orders from revenue calculation (#234)

### Features  
* add discount_amount field to orders model (#231)

### Bug Fixes
* correct null handling in email normalization (#229)
```

## Immutable Dataset Versioning

Rather than mutating tables in-place, the immutable dataset pattern creates versioned snapshots:

```python
# Versioned output pattern: every pipeline run writes to a versioned path
def write_versioned_output(df, dataset_name: str, version: str, date: str):
    """
    Write to versioned path:
    s3://curated/dataset=orders/version=2.1.0/date=2024-01-15/
    """
    output_path = (
        f"s3://curated/"
        f"dataset={dataset_name}/"
        f"version={version}/"
        f"date={date}/"
    )
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(output_path)

    # Update "latest" pointer for consumers who don't pin versions
    latest_path = f"s3://curated/dataset={dataset_name}/latest/"
    df.write.format("parquet").mode("overwrite").save(latest_path)

# Consumers can pin to a specific version or use latest
# Pinned: s3://curated/dataset=orders/version=2.0.0/date=2024-01-15/
# Latest: s3://curated/dataset=orders/latest/date=2024-01-15/
```

Delta Lake makes this elegant with time travel:

```python
# Producers write normally — Delta tracks history automatically
df.write.format("delta").mode("append").save("s3://curated/orders/")

# Consumers can read any historical version
# By version number
df_v10 = spark.read.format("delta") \
    .option("versionAsOf", 10) \
    .load("s3://curated/orders/")

# By timestamp — "what did the data look like yesterday at 6am?"
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-14 06:00:00") \
    .load("s3://curated/orders/")

# Check table history
spark.sql("DESCRIBE HISTORY delta.`s3://curated/orders/`").show()
```

## Pipeline Registry Pattern

For large organizations with dozens of data products, a pipeline registry provides discoverability, ownership, and version tracking:

```python
# registry/pipeline_registry.py
from dataclasses import dataclass
from typing import List

@dataclass
class PipelineVersion:
    version: str
    git_sha: str
    deployed_at: str
    breaking_change: bool
    changelog: str

@dataclass
class PipelineRegistration:
    pipeline_id: str
    owner_team: str
    schedule: str
    outputs: List[str]           # Dataset names this pipeline produces
    inputs: List[str]            # Dataset names this pipeline consumes
    current_version: PipelineVersion
    data_contract_ref: str       # Path to data contract YAML

# registry/register.py — called from CI on deployment
import json
import boto3

def register_pipeline_deployment(pipeline_id: str, version: str, git_sha: str):
    """Register a new pipeline deployment in the central registry."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('pipeline-registry')

    table.put_item(Item={
        'pipeline_id': pipeline_id,
        'version': version,
        'git_sha': git_sha,
        'deployed_at': datetime.utcnow().isoformat(),
        'deployed_by': os.environ.get('GITHUB_ACTOR', 'unknown'),
        'environment': os.environ.get('ENVIRONMENT', 'prod'),
    })

    # Notify downstream consumers of new version
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=f"arn:aws:sns:us-east-1:123456789:pipeline-updates-{pipeline_id}",
        Message=json.dumps({
            'pipeline_id': pipeline_id,
            'new_version': version,
            'git_sha': git_sha
        }),
        Subject=f"Pipeline {pipeline_id} deployed version {version}"
    )
```

## Managing Breaking Changes Safely: The Sunset Protocol

When a breaking schema change is unavoidable, follow a structured migration:

```
Week 1: Deploy V2 alongside V1 (both run simultaneously)
         V2 writes to: curated.orders_v2
         V1 writes to: curated.orders (unchanged)
         Announce breaking change to all downstream consumers

Week 2-4: Consumer migration period
          - Update consumer A to read from orders_v2
          - Update consumer B to read from orders_v2
          - Track migration in a GitHub tracking issue

Week 4: Cutover
         - Add alias: CREATE VIEW curated.orders AS SELECT * FROM curated.orders_v2
         - This is a backward-compatible cutover for consumers that haven't updated yet

Week 8: Sunset V1
         - Verify all consumers have migrated (query information_schema for views/queries reading orders_v1)
         - Drop curated.orders_v1
         - Remove the view alias
```

```sql
-- Track who is querying the old table using Snowflake query history
SELECT
    query_text,
    user_name,
    role_name,
    start_time
FROM snowflake.account_usage.query_history
WHERE
    query_text ILIKE '%orders_v1%'
    AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
-- If this query returns results after Week 4, those consumers haven't migrated yet
```

## Key Senior Interview Takeaways

- **Semantic breaking changes** (logic changes, filter changes) are as dangerous as schema breaking changes — both require major version bumps and migration guides
- **Conventional Commits + release-please** automates changelogs and version bumping — no manual CHANGELOG.md editing
- **Delta time travel** is the elegant solution for immutable dataset versioning — consumers can pin to any version or timestamp
- **Pipeline registry** provides the discoverability and impact analysis needed when dozens of pipelines have interdependencies
- The **sunset protocol** is how you safely remove breaking changes — announce, parallel run, migrate consumers, then cut over with an alias before dropping the old version
- Know the difference: renaming a column is a hard breaking change; adding a nullable column is not
