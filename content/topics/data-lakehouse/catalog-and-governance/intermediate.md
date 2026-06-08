---
title: "Catalog & Governance — Intermediate"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [unity-catalog, glue, datahub, lineage, access-control]
---

# Catalog & Governance — Intermediate

## Unity Catalog Deep Dive

```
Unity Catalog (Databricks): three-level namespace
  catalog.schema.table
  prod_lakehouse.silver.orders
  
  Hierarchical permissions:
    Metastore → Catalogs → Schemas → Tables → Columns/Rows

  Key capabilities:
  1. Fine-grained access control:
     GRANT SELECT ON TABLE prod_lakehouse.silver.orders TO analytics_group;
     GRANT MODIFY ON SCHEMA prod_lakehouse.silver TO etl_writers;
  
  2. Column-level masking:
     CREATE FUNCTION mask_email(email STRING)
     RETURNS STRING
     RETURN IF(is_member('pii_admin'), email, REGEXP_REPLACE(email, '(.).*@', '$1***@'));
     
     ALTER TABLE silver.customers ALTER COLUMN email SET MASK mask_email;
  
  3. Row filters (row-level security):
     CREATE FUNCTION tenant_filter(tenant_id STRING)
     RETURNS BOOLEAN
     RETURN is_member(CONCAT('tenant_', tenant_id)) OR is_member('admin');
     
     ALTER TABLE silver.orders ADD ROW FILTER tenant_filter ON (tenant_id);
  
  4. Automated lineage:
     -- Unity Catalog auto-tracks table reads/writes
     -- Column-level lineage: tracks which source column feeds which target
     -- View in Databricks UI: table → Lineage tab
  
  5. Tags for classification:
     ALTER TABLE silver.customers
     ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
```

---

## AWS Glue Data Catalog

```python
import boto3

glue = boto3.client("glue", region_name="us-east-1")

# Register a table in Glue catalog
glue.create_table(
    DatabaseName="silver",
    TableInput={
        "Name": "orders",
        "Description": "Cleansed order records, updated hourly via CDC",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "order_id",     "Type": "bigint", "Comment": "Unique order identifier"},
                {"Name": "customer_id",  "Type": "bigint", "Comment": "FK to customers table"},
                {"Name": "amount",       "Type": "decimal(18,2)", "Comment": "Order total, USD"},
                {"Name": "status",       "Type": "string", "Comment": "pending|processing|shipped|delivered"},
                {"Name": "order_date",   "Type": "date", "Comment": "Date order was placed"},
            ],
            "Location": "s3://bucket/silver/orders",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            },
        },
        "PartitionKeys": [{"Name": "order_date", "Type": "date"}],
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
            "table_type": "ICEBERG",
            "metadata_location": "s3://bucket/silver/orders/metadata/v1.metadata.json",
            "owner": "data-engineering@company.com",
            "sla_freshness": "1 hour",
        }
    }
)

# Run Glue Crawler to auto-discover tables
glue.create_crawler(
    Name="silver_crawler",
    Role="arn:aws:iam::123456789:role/GlueCrawlerRole",
    DatabaseName="silver",
    Targets={"S3Targets": [{"Path": "s3://bucket/silver/"}]},
    Schedule="cron(0 * * * ? *)",  # hourly
    SchemaChangePolicy={
        "UpdateBehavior": "UPDATE_IN_DATABASE",
        "DeleteBehavior": "LOG",
    }
)
```

---

## DataHub for Business Catalog

```yaml
# DataHub ingestion recipe: ingest metadata from multiple sources

source:
  type: glue
  config:
    aws_region: us-east-1
    extract_owners: true
    extract_transforms: true
    
# After ingestion: tables visible in DataHub UI with:
# - Schema (from Glue)
# - Ownership (from Glue table parameters)
# - Lineage (if using DataHub lineage emitter in Spark jobs)

---
# Add business metadata via DataHub Python SDK
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass, OwnerClass, OwnershipClass, OwnershipTypeClass
)

emitter = DatahubRestEmitter("http://datahub-gms:8080")

# Add description and owner
dataset_urn = make_dataset_urn("glue", "silver.orders")
emitter.emit_mce({
    "entityUrn": dataset_urn,
    "entityType": "dataset",
    "aspectName": "datasetProperties",
    "aspect": DatasetPropertiesClass(
        description="Silver-layer orders table. Cleansed and deduplicated from Bronze. "
                    "Updated hourly via CDC from Postgres. PII: customer_id only.",
        customProperties={
            "freshness_sla": "1 hour",
            "owner": "data-engineering",
            "classification": "internal",
        }
    )
})
```

---

## Column-Level Lineage

```python
# Column-level lineage tracks: which source column feeds which target column
# Critical for: impact analysis (if source column changes, what breaks?)
#               compliance (where does this PII data flow?)

# OpenLineage (open standard for lineage emission)
# Supported by: Spark (openlineage-spark), Airflow, Flink, dbt

# Spark with OpenLineage:
spark = SparkSession.builder \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "http") \
    .config("spark.openlineage.transport.url", "http://datahub:4318") \
    .config("spark.openlineage.namespace", "my_lakehouse") \
    .getOrCreate()

# Every Spark job now emits lineage events:
# Input datasets → Transformations → Output datasets
# Column-level: if you SELECT customer_id, amount FROM source_table,
#               DataHub/Marquez records: target.customer_id ← source.customer_id

# dbt lineage (automatic):
# dbt emits lineage for all models (which source tables feed which target tables)
# Column-level lineage: dbt 1.7+ with --select and column_lineage

# View lineage in DataHub:
# Table page → Lineage tab → Upstream graph
# Click any node to trace data from Salesforce → Fivetran → Bronze → Silver → Gold
```

---

## Interview Tips

> **Tip 1:** "How does column-level lineage help with compliance?" — When a regulator asks "who has access to customer SSN data?", column-level lineage lets you trace: SSN comes from PII Bronze table → flows to Silver customers table → used in Gold customer_360 → accessed by BI tool X. You can see every table SSN touched, every consumer, and apply column masking at each hop. Without lineage, answering this question requires manual code review of every pipeline.

> **Tip 2:** "What's the difference between Unity Catalog and AWS Glue?" — Glue is a technical catalog: it stores schema metadata for Athena, EMR, and Glue ETL to discover tables. It has no column masking, no row filters, limited business metadata. Unity Catalog is a governance platform: it adds fine-grained RBAC (column masking, row filters), automated lineage, tags, audit logs, and cross-workspace sharing — built on top of Delta Lake. Glue is part of the infrastructure stack; Unity Catalog is a governance product.

> **Tip 3:** "A new analyst joins and asks 'where do I find the revenue table?' What does good governance look like?" — Good governance: analyst goes to data catalog (Atlan/DataHub), searches "revenue", finds 2 tables: `gold.daily_revenue` (certified, fresh, "use this") and `gold.legacy_revenue` (deprecated, "do not use"). Each table has a description, owner contact, freshness SLA, data quality score, and sample data preview. Column descriptions explain the difference between `gross_revenue` and `net_revenue`. This is governance maturity. The analyst finds the right table in 5 minutes, not after asking 3 Slack channels.
