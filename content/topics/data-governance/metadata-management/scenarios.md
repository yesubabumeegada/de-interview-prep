---
title: "Metadata Management — Scenarios"
topic: data-governance
subtopic: metadata-management
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [metadata, interview, scenarios, catalog, glossary]
---

# Metadata Management — Interview Scenarios

## Scenario 1 (Junior): Inconsistent Metric Definitions

**Question:** Finance says Q4 revenue is $12M. Sales says it's $14M. Both are reading from "gold.revenue" but getting different numbers. How does metadata management help here?

**Answer:**

**Root cause:** No agreed definition of "revenue" — each team is applying different filters.

**Immediate investigation:**
```sql
-- What exactly does each team's query look like?
-- Finance query:
SELECT SUM(amount) FROM gold.orders WHERE status = 'completed' AND DATE_TRUNC('quarter', order_date) = '2024-10-01';

-- Sales query:
SELECT SUM(amount) FROM gold.revenue WHERE quarter = '2024-Q4';

-- Difference: Finance filters by status='completed', Sales includes 'pending'
-- Also: Sales uses a different table (gold.revenue vs gold.orders)
```

**Governance fix:**

**Step 1: Create an official glossary term**
```yaml
# governance/glossary/revenue.yaml
term_id: GT-001
name: Revenue
definition: >
  Total monetary value of orders with status='completed' in a given period.
  EXCLUDES: cancelled orders, pending orders, refunds, internal test orders.
  INCLUDES: taxes and shipping fees.
  Reference table: gold.orders, column: amount WHERE status = 'completed'
owner: cfo@company.com
approved_by: cfo@company.com
```

**Step 2: Link to the canonical column**
```python
catalog.link_glossary_term("GT-001", "gold.orders.amount")
# Now in DataHub: gold.orders.amount shows linked glossary term "Revenue"
```

**Step 3: Add description to the table**
```yaml
# In dbt schema.yml
models:
  - name: orders
    columns:
      - name: amount
        description: >
          Order value in USD. See glossary term 'Revenue' for definition.
          Only use WHERE status = 'completed' for revenue reporting.
        meta:
          glossary_term: Revenue
```

**Prevention:** Any "revenue" metric must link to this glossary term. Finance and Sales must use the canonical definition. Disputes escalated to CFO (term owner).

---

## Scenario 2 (Mid-level): Metadata Drift After Acquisition

**Question:** Your company acquired a startup. Their data is being merged into your data lake. They have 80 tables with no documentation, no owners, mixed naming conventions. How do you onboard their data?

**Answer:**

**Week 1: Automated discovery**
```python
# 1. Run technical metadata discovery
for table in acquired_tables:
    # Auto-ingest schema, row counts, sample values
    catalog_client.ingest_table(table, platform="snowflake")
    
    # Run PII classifier
    findings = pii_classifier.classify_table(engine, table)
    if findings:
        # Tag PII immediately — don't wait for manual review
        catalog_client.tag_columns(table, findings)

# 2. Run usage analysis on their historical query logs (if available)
# Identify which tables are actually used vs. unused
```

**Week 2: Prioritization and owner assignment**
```python
# Sort by usage (most-used tables first)
priority_tables = sorted(
    acquired_tables,
    key=lambda t: query_logs.get_monthly_queries(t),
    reverse=True,
)

# Assign to acquiring team's domain leaders for ownership
# Top 20 most-used tables: assign to business owners immediately
# Remaining: assign to data platform team as temporary steward
```

**Week 3-4: Documentation sprint**
```
- Hold metadata hackathon: block 4 hours, acquisition team + domain leads
- Each team documents their critical tables (goal: top 20 by usage)
- Template: 3-sentence description, owner, domain, sensitivity
- Gate: no table can be used by analytics team without description + owner
```

**Automation:**
```python
# Block queries to undocumented acquired tables (soft block via warning)
def pre_query_check(user_email: str, table_name: str) -> dict:
    metadata = catalog.get(table_name)
    if not metadata.get("description"):
        return {
            "allowed": True,  # Don't hard-block, but warn
            "warning": f"Table {table_name} has no documentation. "
                       "Contact data-governance@company.com to request docs.",
        }
    return {"allowed": True}
```

---

## Scenario 3 (Senior): Metadata at Scale — 10,000 Tables

**Question:** You're the data platform lead at a large company with 10,000 tables. Your metadata quality score is 45% (many tables undocumented, no owners). Leadership wants it at 85% in 6 months. How?

**Answer:**

**Baseline analysis (Week 1)**
```python
# Score current state
scores = compute_metadata_quality_score(engine)
print(f"Current: {scores['overall']:.0%} overall")
# → Current: 45% overall

# Break down by problem type
# Description: 40% coverage
# Owner: 55% coverage
# Classification: 30% coverage
```

**Strategy: Tiered approach**

**Tier 1 (top 200 tables by query count): Month 1**
```
- 200 tables = 80% of all user queries
- Assign each to a domain team owner
- Mandate description + classification in 2 weeks
- DEs pair with analysts to write descriptions
- Incentive: team with best metadata score gets recognition in all-hands
```

**Tier 2 (next 1,800 tables): Month 2-3**
```
- Run auto-classification (handles classification quickly)
- Auto-generate description stubs from column names + table name using LLM
- Humans review and approve AI-generated stubs (much faster than writing)
- Assign to domain leads as batch
```

**Tier 3 (remaining 8,000 tables): Month 4-6**
```
- Auto-classify (sensitivity) using scanner
- Auto-assign owner = last committer of pipeline writing to table
- Auto-generate brief description from schema + dbt model name
- Human curation for tables that get >100 monthly queries
```

**Enforcement from Month 2 onward:**
```
- CI gate: new tables must have description + owner before deploy
- Weekly digest to domain leads: their metadata score
- Monthly governance review: bottom 3 domains get platform team support
- Quarterly: publish metadata scorecard publicly (by domain)
```

**Month 6 outcome:**
```
Tier 1 (200 tables): ~98% coverage (human-curated)
Tier 2 (1,800 tables): ~88% coverage (AI-assisted + human review)
Tier 3 (8,000 tables): ~82% coverage (auto-generated)
Overall: ~84% — target achieved
```

**Key lesson:** Don't wait for perfect descriptions. A 2-sentence auto-generated description reviewed by a human is infinitely better than none.
