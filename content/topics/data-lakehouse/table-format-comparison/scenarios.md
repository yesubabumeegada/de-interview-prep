---
title: "Table Format Comparison — Scenarios"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: scenario_question
tags: [iceberg, delta, hudi, comparison, scenarios]
---

# Table Format Comparison — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Delta Lake, Iceberg, and Hudi — Core Differences

**Scenario:** A startup is building their first data lakehouse. The engineering manager asks you to recommend a table format. They use Databricks for ETL, Trino for ad-hoc SQL, and may add Flink later. What do you recommend and why?

<details>
<summary>💡 Hint</summary>

Consider engine compatibility. Delta Lake has tight Databricks integration but historically weaker multi-engine support. Iceberg is engine-agnostic. Hudi excels at CDC. The multi-engine requirement (Databricks + Trino + Flink) is the key constraint.

</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: Apache Iceberg**

**Reasoning:**

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---------|-----------|----------------|-------------|
| Databricks support | Native | Via Delta-Iceberg or UniForm | Connector |
| Trino support | Limited (Delta connector) | First-class | Limited |
| Flink support | Limited | First-class | First-class |
| Open spec | Yes (open-sourced 2023) | Yes (Apache) | Yes (Apache) |
| CDC support | Basic | Via Flink/Spark | Best-in-class |
| Multi-catalog | Limited | REST catalog standard | Limited |

**For this team:**
- Databricks now supports Iceberg natively via **UniForm** (Universal Format), which lets a Delta table be read as Iceberg by external engines
- Trino has a mature Iceberg connector
- Flink has first-class Iceberg sink support

**If already on Databricks:** Use Delta with UniForm enabled — get native Databricks performance while allowing Trino and Flink to read via Iceberg protocol.

```sql
-- Enable UniForm on a Delta table (Databricks)
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

**If greenfield (no Databricks lock-in):** Go pure Iceberg with a REST catalog (Polaris, Nessie, or AWS Glue).

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Migrating from Delta Lake to Iceberg

**Scenario:** Your company is migrating from Databricks Delta Lake to an open lakehouse on EMR with Iceberg. You have 200 Delta tables ranging from 10GB to 50TB. Design a migration strategy that minimizes downtime and risk.

<details>
<summary>💡 Hint</summary>

Consider shadow migration (write to both, validate, cut over), in-place conversion vs copy, and the Delta-to-Iceberg migration tools. Test with small tables first. Plan for rollback.

</details>

<details>
<summary>✅ Solution</summary>

**Migration Strategy: Phased Shadow Migration**

**Phase 1: Assessment (Week 1)**
```python
# Inventory all Delta tables
tables_info = spark.sql("""
  SELECT table_name,
         round(sum(size)/1e12, 2) as size_tb,
         count(*) as file_count,
         max(modificationTime) as last_modified
  FROM (
    DESCRIBE DETAIL delta.`s3://bucket/tables/*`
  )
  GROUP BY table_name
  ORDER BY size_tb DESC
""")

# Categorize: small (<100GB), medium, large (>10TB)
```

**Phase 2: In-Place Conversion (Small Tables)**

Using the Delta-Iceberg migration tool:
```python
# Use iceberg-delta-lake-compat or rewrite via Spark
from delta import DeltaTable

# Option A: Read Delta, write as Iceberg
delta_df = spark.read.format("delta").load("s3://bucket/small_table/")
delta_df.write.format("iceberg")     .option("write.format.default", "parquet")     .saveAsTable("prod.small_table")

# Verify row counts
delta_count = delta_df.count()
iceberg_count = spark.table("prod.small_table").count()
assert delta_count == iceberg_count
```

**Phase 3: Dual-Write for Large Tables**
```python
def write_to_both(batch_df, batch_id):
    # Write to Delta (existing)
    batch_df.write.format("delta")         .mode("append").save("s3://bucket/large_table/")
    
    # Write to Iceberg (new)
    batch_df.write.format("iceberg")         .mode("append").saveAsTable("prod_iceberg.large_table")

stream.writeStream.foreachBatch(write_to_both).start()
```

**Phase 4: Validation**
```python
import great_expectations as ge

# Statistical validation
def validate_migration(delta_path, iceberg_table):
    delta_df = spark.read.format("delta").load(delta_path)
    ice_df = spark.table(iceberg_table)
    
    checks = {
        "row_count_match": delta_df.count() == ice_df.count(),
        "null_counts_match": all(
            delta_df.filter(col(c).isNull()).count() ==
            ice_df.filter(col(c).isNull()).count()
            for c in delta_df.columns
        ),
        "schema_match": delta_df.schema == ice_df.schema
    }
    return checks
```

**Phase 5: Cutover**
- Update all pipeline configs to point to Iceberg tables
- Keep Delta tables in read-only mode for 30 days
- Monitor query performance and data quality
- Decommission Delta after validation period

**Rollback Plan:**
- Keep Delta tables intact during migration
- Feature flag in pipeline configs to switch back
- Delta retained for 30 days post-cutover

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Choosing a Table Format at Scale — Architecture Decision Record

**Scenario:** You are the principal data engineer at a financial services company processing 50TB/day. The data platform team needs to decide on a table format for the next 5 years. You must evaluate Delta Lake, Iceberg, and Hudi across: regulatory compliance (data retention, audit trails), multi-cloud strategy, query performance, operational complexity, and ecosystem lock-in. Produce an architecture decision record (ADR).

<details>
<summary>💡 Hint</summary>

ADRs have: context, decision drivers, options considered, decision, consequences. For financial services, consider: immutable audit logs, fine-grained access control, column-level encryption, cross-cloud portability, and regulatory requirements (GDPR right-to-erasure vs immutability).

</details>

<details>
<summary>✅ Solution</summary>

**Architecture Decision Record: Table Format Selection**

**Status:** Proposed  
**Date:** 2024-Q1  
**Deciders:** Principal DE, Platform Architect, CTO

---

**Context:**
50TB/day ingestion across 3 cloud providers (AWS primary, GCP DR, Azure for EU data residency). 200 analysts on Trino, 15 data scientists on Spark, real-time fraud detection on Flink. SEC/FINRA audit requirements: 7-year immutable retention, complete lineage.

---

**Decision Drivers:**

| Driver | Weight |
|--------|--------|
| Regulatory compliance | Critical |
| Multi-engine/multi-cloud | High |
| Query performance | High |
| Operational maturity | Medium |
| Vendor lock-in risk | Medium |

---

**Options Evaluated:**

**Delta Lake**
- ✅ Best Databricks integration, DeltaSharing for cross-org sharing
- ✅ VACUUM with retention controls
- ❌ Multi-cloud catalog story weak (Databricks-centric)
- ❌ Trino integration limited vs Iceberg

**Apache Iceberg**
- ✅ True multi-engine: Trino, Spark, Flink, Hive all first-class
- ✅ REST catalog is cloud-agnostic
- ✅ Row-level deletes (GDPR erasure) with `DELETE WHERE`
- ✅ Snapshot retention for audit (immutable snapshot history)
- ❌ CDC (upsert) slightly more complex than Hudi

**Apache Hudi**
- ✅ Best CDC/upsert support, timeline-based audit
- ✅ MoR for high-frequency updates
- ❌ Weaker Trino support
- ❌ Smaller community than Iceberg/Delta

---

**Decision: Apache Iceberg with Polaris REST Catalog**

**Rationale:**
1. **Compliance:** Iceberg's snapshot model provides immutable point-in-time audit (required for SEC rule 17a-4). `expire_snapshots` is explicitly controlled, never automatic.
2. **GDPR Erasure:** Row-level deletes update metadata without rewriting all files. Deleted rows become unreachable without physical scan.
3. **Multi-cloud:** REST catalog abstracts cloud-specific metastores. Same catalog API on AWS (S3), GCP (GCS), Azure (ADLS).
4. **Multi-engine:** Trino (analytics), Spark (ETL), Flink (streaming) all have mature Iceberg connectors.

**Catalog Architecture:**
```
┌──────────────────────────────────────────────┐
│           Apache Polaris (REST Catalog)       │
│  - OAuth2 per-engine credentials             │
│  - Namespace-level access policies           │
│  - Cross-cloud namespace federation          │
└──────────────────┬───────────────────────────┘
                   │
        ┌──────────┼──────────┐
        │          │          │
     AWS S3     GCP GCS   Azure ADLS
   (primary)    (DR)     (EU residency)
```

**GDPR Erasure Procedure:**
```sql
-- Soft delete (metadata only, fast)
DELETE FROM prod.customers WHERE customer_id = 12345;

-- After legal hold expires: physical removal
CALL prod.system.expire_snapshots(
    table => 'prod.customers',
    older_than => TIMESTAMP '2024-01-01'
);
CALL prod.system.rewrite_data_files('prod.customers');
CALL prod.system.remove_orphan_files('prod.customers');
```

**Consequences:**
- Positive: Engine flexibility, no Databricks dependency
- Negative: Higher operational complexity vs managed Delta (Databricks)
- Mitigation: Platform team maintains Polaris catalog; runbooks for maintenance procedures

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do Delta Lake, Iceberg, and Hudi handle schema evolution?" — All three support adding/removing columns. Iceberg and Hudi use column IDs internally so renames don't break reads. Delta uses column names and is more strict about rename operations.
> **Tip 2:** "What is ACID in the context of table formats?" — Atomicity (all-or-nothing commits via metadata swap), Consistency (schema enforcement), Isolation (snapshot isolation prevents dirty reads), Durability (committed snapshots are permanent on object storage).
> **Tip 3:** "What's the difference between hidden partitioning and Hive partitioning?" — Hive partitioning requires users to include partition columns in queries. Iceberg's hidden partitioning uses transforms (e.g., `months(event_time)`) and automatically prunes partitions without requiring users to know the physical layout.
