---
title: "Metadata Management — Scenarios"
topic: data-governance
subtopic: metadata-management
content_type: scenario_question
tags: [metadata, interview, scenarios, catalog, glossary]
---

# Metadata Management — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Inconsistent Metric Definitions

**Scenario:** Finance says Q4 revenue is $12M. Sales says it's $14M. Both are reading from "gold.revenue" but getting different numbers. How does metadata management help here?

<details>
<summary>💡 Hint</summary>

**Root cause:** No agreed definition of "revenue" — each team is applying different filters.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Metadata Drift After Acquisition

**Scenario:** Your company acquired a startup. Their data is being merged into your data lake. They have 80 tables with no documentation, no owners, mixed naming conventions. How do you onboard their data?

<details>
<summary>💡 Hint</summary>

Start with automated discovery to understand what you're dealing with before asking humans to document anything: ingest schema, row counts, sample values, and PII signals automatically. Then do one interview per domain (not per table) with the acquired team's engineers to assign owners and get business descriptions for the 10–15 most important tables — owners cascade documentation to related tables. Rename and reclassify in a staging catalog before merging, so your production catalog stays clean during onboarding.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Metadata at Scale — 10,000 Tables

**Scenario:** You're the data platform lead at a large company with 10,000 tables. Your metadata quality score is 45% (many tables undocumented, no owners). Leadership wants it at 85% in 6 months. How?

<details>
<summary>💡 Hint</summary>

Going from 45% to 85% across 10,000 tables in 6 months is impossible through manual documentation alone. The strategy is: score each table on a metadata quality rubric (owner, description, classification tag, freshness SLA), then focus human effort on the top 500 most-queried tables (these have the most impact and interested owners), use LLM-assisted description generation for the rest, and enforce a "no deploy without metadata" gate in CI for all new tables. Track progress weekly with a quality dashboard so teams can see their scores and compete.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is metadata and what are the main types relevant to data engineering?**
A: Metadata is data about data. Key types include: technical metadata (schema, data types, row counts, partitions), operational metadata (pipeline run times, freshness, SLAs), business metadata (descriptions, ownership, business glossary terms), and governance metadata (classification, lineage, access policies).

**Q: What is the difference between a data dictionary and a business glossary?**
A: A data dictionary documents the technical details of fields in a specific dataset — column name, type, allowed values, and a brief description. A business glossary defines enterprise-wide business terms and their canonical meanings, independent of any specific table or system.

**Q: How does metadata management support data discovery?**
A: Rich metadata — descriptions, tags, ownership, related terms — makes datasets findable in a catalog. Data consumers can search by business term, owner, or topic rather than having to know exact table names, reducing the time to find trustworthy data and reducing ad-hoc requests to data engineering teams.

**Q: What is schema evolution and how should metadata management handle it?**
A: Schema evolution is the change of a dataset's structure over time (adding, renaming, or removing columns, changing types). Metadata management should version schemas (e.g., using Schema Registry for Kafka or Delta Lake schema history), track what changed and when, and propagate change notifications to downstream consumers.

**Q: What is active metadata and how does it differ from passive metadata?**
A: Passive metadata is manually authored (descriptions, tags, ownership). Active metadata is automatically derived from system activity — query frequency, last access time, data quality scores, and pipeline run statistics. Active metadata keeps the catalog accurate without relying solely on human maintenance.

**Q: How do you enforce metadata completeness at dataset publication time?**
A: Define a metadata contract that new datasets must satisfy before being published — required fields like owner, description, sensitivity classification, and data quality rules. Enforce it as a CI/CD check in your data pipeline framework (e.g., dbt tests, Great Expectations, or a custom catalog API validation step).

**Q: What is a schema registry and when would you use one?**
A: A schema registry (e.g., Confluent Schema Registry) stores and versions schemas for event streams (Avro, Protobuf, JSON Schema). It enforces schema compatibility rules (backward, forward, full) so producers and consumers can evolve schemas without breaking each other, which is essential for Kafka-based pipelines.

---

## 💼 Interview Tips

- Distinguish active from passive metadata fluently — it is a relatively advanced concept and demonstrates that you follow current thinking in the catalog and governance space.
- Mention schema registry specifically for streaming pipelines; many candidates describe batch schema management but overlook the Kafka streaming context, which matters for real-time DE roles.
- Frame metadata management as an enabler of self-service — the more complete the metadata, the less time engineers spend answering "what does this column mean?" questions.
- For senior roles, describe governance metadata (classification, lineage, policies) as a first-class type alongside technical and business metadata — it shows you think about the full governance stack.
- Discuss metadata quality metrics (completeness, accuracy, freshness) and how you would monitor them — interviewers want to hear that you would treat metadata as a product with its own quality standards.
- Avoid describing metadata management as purely a documentation project; emphasize automation, active metadata collection, and enforcement at pipeline boundaries to show operational scale thinking.
