---
title: "AWS Lake Formation - Senior Deep Dive"
topic: aws-services
subtopic: lake-formation
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, lake-formation, data-mesh, governance-at-scale]
---

# AWS Lake Formation — Senior-Level Deep Dive

## Data Mesh Implementation with Lake Formation

Lake Formation enables a federated data mesh where domain teams own their data products while central governance enforces policies:

```python
import boto3

lf = boto3.client('lakeformation')

# Central governance team defines tag taxonomy
lf.create_lf_tag(TagKey='domain', TagValues=['orders', 'customers', 'payments', 'inventory'])
lf.create_lf_tag(TagKey='data_product_tier', TagValues=['bronze', 'silver', 'gold'])
lf.create_lf_tag(TagKey='pii_level', TagValues=['none', 'low', 'high'])

# Domain teams tag their own tables
# Orders domain team tags their gold-tier product
lf.add_lf_tags_to_resource(
    Resource={'Table': {'DatabaseName': 'orders_domain', 'Name': 'fact_orders_gold'}},
    LFTags=[
        {'TagKey': 'domain', 'TagValues': ['orders']},
        {'TagKey': 'data_product_tier', 'TagValues': ['gold']},
        {'TagKey': 'pii_level', 'TagValues': ['none']}
    ]
)

# Cross-domain access: marketing can read gold-tier from orders and customers
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/MarketingDomain'}},
    Resource={
        'LFTagPolicy': {
            'ResourceType': 'TABLE',
            'Expression': [
                {'TagKey': 'domain', 'TagValues': ['orders', 'customers']},
                {'TagKey': 'data_product_tier', 'TagValues': ['gold']},
                {'TagKey': 'pii_level', 'TagValues': ['none']}
            ]
        }
    },
    Permissions=['SELECT']
)
```

**Data mesh architecture with LF:**
- Each domain team owns an AWS account with their data products
- Central governance account manages LF-Tag taxonomy and org-level policies
- Cross-account sharing via RAM + LF enables consumption without data copying
- Domain teams can grant within their scope (PermissionsWithGrantOption)

---

## Governed Tables — ACID Transactions on S3

```python
# Create a governed table (ACID support on S3)
glue = boto3.client('glue')

glue.create_table(
    DatabaseName='curated',
    TableInput={
        'Name': 'customer_profiles',
        'TableType': 'GOVERNED',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'customer_id', 'Type': 'string'},
                {'Name': 'email', 'Type': 'string'},
                {'Name': 'segment', 'Type': 'string'}
            ],
            'Location': 's3://lake-governed/customer_profiles/',
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        }
    }
)
```

### Transaction API

```python
# Start a transaction
tx = lf.start_transaction(TransactionType='READ_AND_WRITE')
transaction_id = tx['TransactionId']

try:
    # Perform operations within transaction
    # (operations reference the transaction_id)
    
    # Commit if all succeeded
    lf.commit_transaction(TransactionId=transaction_id)
except Exception as e:
    # Rollback on failure
    lf.cancel_transaction(TransactionId=transaction_id)
    raise e
```

> **Note:** Governed tables are being superseded by Apache Iceberg tables in modern architectures. Iceberg provides better ecosystem compatibility and performance. Consider governed tables for legacy setups only.

---

## Data Cell Filtering — Fine-Grained Access

```python
# Complex filter: restrict both rows AND columns
lf.create_data_cells_filter(
    TableData={
        'TableCatalogId': '123456789',
        'DatabaseName': 'curated',
        'TableName': 'transactions',
        'Name': 'eu-customers-no-pii',
        'RowFilter': {
            'FilterExpression': "region IN ('eu-west-1', 'eu-central-1') AND transaction_date >= '2024-01-01'"
        },
        'ColumnNames': ['transaction_id', 'amount', 'category', 'transaction_date', 'region'],
        'ColumnWildcard': {
            'ExcludedColumnNames': ['customer_email', 'card_last_four', 'ip_address']
        }
    }
)

# Multiple filters for different access patterns
filters = [
    {'name': 'full-access', 'row_filter': 'TRUE', 'excluded_cols': []},
    {'name': 'regional-eu', 'row_filter': "region LIKE 'eu-%'", 'excluded_cols': ['customer_email']},
    {'name': 'aggregate-only', 'row_filter': 'TRUE', 'excluded_cols': ['customer_email', 'card_last_four', 'ip_address', 'transaction_id']}
]
```

---

## Centralized vs Federated Governance

| Aspect | Centralized | Federated (Data Mesh) |
|--------|------------|----------------------|
| Who manages tags | Central team | Central defines taxonomy, domains assign |
| Who grants access | Central admin | Domain leads (with GrantOption) |
| Account structure | Single account | Multi-account (per domain) |
| Scalability | Bottleneck at central team | Scales with org size |
| Consistency | High (single authority) | Requires policy enforcement |
| Onboarding speed | Slow (tickets to central) | Fast (domain self-service) |
| Audit | Simpler (one account) | Aggregated from multiple accounts |

**Recommended hybrid approach:**
- Central team: tag taxonomy, org-wide SCPs, cross-domain policies
- Domain teams: tag assignment, intra-domain grants, data product publishing
- Automated: tag validation (Lambda), compliance checks, drift detection

---

## Migration from IAM-Only to Lake Formation

```python
# Phase 1: Enable LF in hybrid mode (existing access unchanged)
lf.put_data_lake_settings(
    DataLakeSettings={
        'DataLakeAdmins': [
            {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/LakeAdmin'}
        ],
        'CreateDatabaseDefaultPermissions': [],  # Don't auto-grant to IAMAllowedPrincipals
        'CreateTableDefaultPermissions': []
    }
)

# Phase 2: Register S3 locations
lf.register_resource(
    ResourceArn='arn:aws:s3:::data-lake-curated',
    UseServiceLinkedRole=True
)

# Phase 3: Grant LF permissions (parallel to existing IAM)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/AnalystRole'}},
    Resource={'Table': {'DatabaseName': 'curated', 'Name': 'orders'}},
    Permissions=['SELECT']
)

# Phase 4: Remove IAMAllowedPrincipals (cuts over to LF-only)
lf.revoke_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}},
    Resource={'Database': {'Name': 'curated'}},
    Permissions=['ALL']
)

# Phase 5: Validate access (test with each role)
# If something breaks, re-grant IAMAllowedPrincipals as rollback
```

**Migration risks:**
- Revoking IAMAllowedPrincipals immediately breaks all access not explicitly granted in LF
- Test in dev/staging first with a single database
- Keep CloudTrail enabled to identify who accesses what before migration
- Have a rollback plan (re-grant IAMAllowedPrincipals)

---

## Audit Logging and Compliance

```python
# CloudTrail captures all LF API calls
# Key events to monitor:
audit_events = [
    'GrantPermissions',       # Who granted what to whom
    'RevokePermissions',      # Access removed
    'GetTemporaryGluePartitionCredentials',  # Data access (query-time)
    'GetTemporaryGlueTableCredentials',      # Table-level access
    'BatchGrantPermissions',  # Bulk changes
    'CreateDataCellsFilter',  # New filter creation
]

# Athena query to audit access patterns (CloudTrail → S3 → Athena)
audit_query = """
SELECT 
    userIdentity.arn AS who,
    eventName AS action,
    requestParameters AS what,
    eventTime AS when
FROM cloudtrail_logs
WHERE eventSource = 'lakeformation.amazonaws.com'
  AND eventName IN ('GrantPermissions', 'RevokePermissions')
  AND eventTime > DATE_ADD('day', -30, NOW())
ORDER BY eventTime DESC
"""
```

---

## Interview Tips

> **Tip 1:** "How would you implement a data mesh on AWS?" — "Use Lake Formation with multi-account architecture. Each domain team owns an AWS account with their data products. Central governance account defines the LF-Tag taxonomy (domain, classification, tier). Domain teams tag their tables and can grant access within their scope. Cross-account sharing via RAM enables consumption without data copying. Central team enforces org-wide policies via SCPs and tag validation."

> **Tip 2:** "How do you migrate from IAM-based to Lake Formation access control?" — "Phased approach: (1) Enable LF in hybrid mode (both IAM and LF checked). (2) Register S3 locations. (3) Create LF grants that mirror existing IAM access. (4) Test thoroughly. (5) Revoke IAMAllowedPrincipals one database at a time. Key risk: revoking IAMAllowedPrincipals breaks all access not explicitly granted in LF. Always have a rollback plan."

> **Tip 3:** "How do you handle fine-grained access at scale with hundreds of tables?" — "LF-Tags. Define a tag taxonomy (domain, classification, PII level) and assign tags to tables. Grant access to tag expressions instead of individual tables. When new tables are tagged, permissions apply automatically. This scales from 10 to 10,000 tables without changing any grant statements. Combine with data cell filters for row/column restrictions on sensitive tables."
