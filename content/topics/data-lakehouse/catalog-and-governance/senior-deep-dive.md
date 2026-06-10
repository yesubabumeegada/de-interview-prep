---
title: "Catalog & Governance — Senior Deep Dive"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [governance, data-mesh, data-contracts, privacy-engineering, compliance]
---

# Catalog & Governance — Senior Deep Dive

## Data Mesh and Federated Governance

```
Data Mesh principle: domain ownership of data products
  Each business domain (orders, payments, customers) owns its data
  Domain publishes data as a "data product" with a contract
  Central team: sets standards (schemas, quality, access), doesn't own data

Federated governance model:
  Central team defines:
    - Column naming conventions (customer_id, not cust_id, uid, or customerID)
    - PII classification taxonomy (PII_HIGH, PII_MED, PII_LOW)
    - Minimum SLA standards (Gold tables: 99.9% freshness within SLA)
    - Access request process
  
  Domain teams own:
    - Their tables and pipelines
    - Data quality for their domain
    - Column descriptions and business glossary entries
    - Incident response for their data

Catalog role in Data Mesh:
  The catalog IS the data product registry
  Each data product = catalog entry with:
    - Owner (domain team)
    - SLA (freshness, availability)
    - Schema (contract)
    - Quality score (pass/fail on defined checks)
    - Consumers (who uses this product)
    - Changelog (what changed, when, who)
```

---

## Data Contracts Implementation

```yaml
# Data contract: formal agreement between data producer and consumers
# Defines: schema, quality expectations, SLA, versioning

# data_contract_orders_v2.yaml
id: orders-v2
name: "Orders Data Product"
version: "2.1.0"
owner: "order-platform-team@company.com"
status: active

schema:
  - name: order_id
    type: BIGINT
    nullable: false
    description: "Globally unique order identifier"
    pii: false
    
  - name: customer_id
    type: BIGINT
    nullable: false
    description: "Foreign key to customers table"
    pii: true
    pii_class: PII_MED
    
  - name: amount
    type: DECIMAL(18,2)
    nullable: false
    description: "Order total in USD, inclusive of tax, exclusive of delivery fee"
    
  - name: status
    type: STRING
    nullable: false
    description: "Order lifecycle status"
    allowed_values: [pending, processing, shipped, delivered, cancelled, refunded]

quality:
  - check: "no nulls on order_id"
    type: not_null
    column: order_id
    fail_threshold: 0  # zero nulls allowed
    
  - check: "amount must be positive"
    type: custom_sql
    sql: "SELECT COUNT(*) FROM {table} WHERE amount <= 0"
    fail_threshold: 0
    
  - check: "status in allowed set"
    type: accepted_values
    column: status
    values: [pending, processing, shipped, delivered, cancelled, refunded]

sla:
  freshness: "PT1H"      # updated within 1 hour
  availability: "99.5%"  # table available 99.5% of the time
  row_count_min: 1000    # at least 1K rows expected daily

versioning:
  breaking_change_policy: "Major version bump required for column type narrowing, drops"
  deprecation_notice: "14 days written notice before breaking change"
  
consumers:
  - team: analytics
    since: "2023-01-15"
    usage: "daily revenue dashboard"
  - team: finance
    since: "2022-06-01"
    usage: "billing reconciliation"
```

---

## Privacy Engineering: PII at Scale

```python
# Automated PII detection and classification

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# PII detection patterns
PII_PATTERNS = {
    "email":   r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "phone":   r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
    "ssn":     r"\b\d{3}-\d{2}-\d{4}\b",
    "credit_card": r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b",
    "ip_addr": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
}

def detect_pii_columns(spark, table_path: str, sample_size: int = 1000) -> dict:
    """Scan a sample of data to detect likely PII columns."""
    df = spark.read.format("delta").load(table_path).limit(sample_size)
    pii_columns = {}
    
    for column in df.columns:
        # Sample values for the column
        sample = [str(row[column]) for row in df.select(column).collect() if row[column]]
        sample_str = " ".join(sample[:100])
        
        for pii_type, pattern in PII_PATTERNS.items():
            matches = len(re.findall(pattern, sample_str))
            if matches > 5:  # threshold: 5+ matches in sample → likely PII
                pii_columns[column] = pii_type
                break
    
    return pii_columns

# Dynamic masking function (hash-based pseudonymization)
from pyspark.sql.functions import sha2, concat_ws, lit

def pseudonymize_pii(df, pii_columns: list, salt: str = "secret_salt"):
    """Replace PII columns with consistent pseudonyms (same input → same hash)."""
    for col_name in pii_columns:
        df = df.withColumn(
            col_name,
            sha2(concat_ws("|", col(col_name), lit(salt)), 256)
        )
    return df

# GDPR tokenization with lookup table (reversible for authorized users)
def tokenize_pii(df, pii_column: str, token_table_path: str):
    """Replace PII with opaque token; store mapping in secure token table."""
    from pyspark.sql.functions import monotonically_increasing_id, uuidv4
    
    # Generate tokens
    df_with_token = df.withColumn("_token", uuidv4())
    
    # Store PII → token mapping in encrypted token table
    token_mapping = df_with_token.select(
        col(pii_column).alias("pii_value"),
        col("_token").alias("token"),
    )
    token_mapping.write.format("delta") \
        .mode("append") \
        .option("encryption.key", "arn:aws:kms:us-east-1:123:key/abc") \
        .save(token_table_path)
    
    # Replace PII with token in output
    return df_with_token.drop(pii_column).withColumnRenamed("_token", pii_column)
```

---

## Governance Maturity Model

```
Level 1 — Ad Hoc:
  No catalog, no access control beyond S3 bucket policies
  Schema in engineers' heads, data quality unknown
  Risk: data breaches, incorrect analysis, no regulatory compliance
  
Level 2 — Reactive:
  Technical catalog (Glue/Hive Metastore) for engine discovery
  Role-based S3 access (read/write at folder level)
  Basic data quality checks in Airflow
  No column-level security, no lineage
  
Level 3 — Proactive:
  Business catalog (DataHub/Atlan) with descriptions and ownership
  Column-level security + row-level security on sensitive tables
  Automated PII detection + column masking
  Table-level lineage (which tables feed which)
  Data quality SLAs defined and monitored
  
Level 4 — Managed:
  Column-level lineage (full pipeline impact analysis)
  Data contracts with versioning and consumer SLAs
  Automated compliance scanning (find all tables with unmasked PII)
  Audit trail for all data access (who read what, when)
  Data product catalog with certified, deprecation, and draft states
  
Level 5 — Optimized:
  Self-service governance: domain teams manage their own governance
  Federated governance: central policy, domain execution
  Privacy by design: PII tokenization at ingestion (never stored raw)
  Automated access provisioning from catalog (request → approved → granted)
  Cross-cloud, multi-catalog unified governance (Polaris/Unity Catalog)

Most companies: Level 2-3. Level 4+ requires dedicated governance team.
```

---

## Interview Tips

> **Tip 1:** "How do you prevent data governance from becoming a bottleneck?" — The anti-pattern: central team approves every data access request → 5-day SLA → teams bypass governance. The solution: self-service governance via catalog. Tag data with classification (PII/Public/Internal). Pre-approve roles: "analyst_ro" can access all Internal data automatically. Only PII_HIGH requires manual approval. Access is provisioned automatically after approval (no manual IAM changes). Data engineers in domains manage their own table descriptions and quality checks. Central team: sets policy and audits; doesn't touch every access request.

> **Tip 2:** "A compliance audit asks: 'Show us everywhere customer SSN is stored and who can access it.'" — Without lineage: multi-week manual investigation. With mature governance: (1) DataHub search for tag `pii_class=ssn` → shows 3 tables. (2) Unity Catalog / Glue: list all IAM roles and users with SELECT on those tables. (3) Lineage graph: show the SSN flows from Bronze (raw) → Silver (masked) → Gold (hashed, only analysts with PII role). (4) Audit log: pull all SELECT queries on SSN tables from CloudTrail for last 90 days. Entire response: 2 hours, not 2 weeks.

> **Tip 3:** "What's a data contract and how do you enforce it?" — A data contract is a formal schema + quality + SLA agreement between a data producer (pipeline team) and data consumers (analysts, downstream pipelines). Enforce it via: (1) schema validation at write time (Delta/Iceberg schema enforcement); (2) dbt tests that run on every pipeline execution (not_null, accepted_values, relationships); (3) Great Expectations checkpoints that gate pipeline promotion to Silver; (4) version the contract file in Git — breaking changes require major version bump and consumer notification. Contracts turn implicit assumptions into explicit, testable commitments.

## ⚡ Cheat Sheet

**Catalog types**
| Catalog | Strengths | Use case |
|---|---|---|
| Hive Metastore | Broad compatibility | Legacy |
| AWS Glue | Serverless, integrates Athena/EMR | AWS-native |
| Unity Catalog | Fine-grained access + lineage | Databricks |
| Iceberg REST | Open standard, multi-engine | Cross-platform |
| Nessie | Git-like branching | Data version control |

**Iceberg REST catalog config**
```python
spark.conf.set("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.prod.type", "rest")
spark.conf.set("spark.sql.catalog.prod.uri", "https://catalog.company.com/api/catalog")
```

**Nessie branching (data version control)**
```python
nessie_client.create_branch("feature/experiment-x")
spark.conf.set("spark.sql.catalog.nessie.ref", "feature/experiment-x")
# Run pipeline on branch — doesn't affect main
nessie_client.merge("feature/experiment-x", "main")
```

**Governance layers**
```
Catalog:  who can discover tables (metadata access)
Storage:  S3 bucket policies + IAM (file access)
Engine:   column masking, row filtering at query time
Lineage:  auto-track data flow (OpenLineage)
Audit:    log all access for compliance
```

**Key interview points**
- Catalog holds metadata; data stays in object storage
- Multi-engine: one Iceberg table readable by Spark, Trino, Flink, DuckDB simultaneously
- Schema registry = streaming (Kafka); catalog = batch tables (different tools)
- Data products: catalog entry = contract (owner, SLA, schema, access policy)
