---
title: "AWS Lake Formation - Intermediate"
topic: aws-services
subtopic: lake-formation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, lake-formation, governance, permissions, cross-account]
---

# AWS Lake Formation — Intermediate Concepts

## LF-Tags — Tag-Based Access Control

LF-Tags replace per-table/column grants with tag-based policies. Instead of granting access to 500 tables individually, tag them and grant access to the tag:

```python
import boto3

lf = boto3.client('lakeformation')

# Step 1: Create LF-Tag keys
lf.create_lf_tag(TagKey='classification', TagValues=['public', 'internal', 'confidential', 'restricted'])
lf.create_lf_tag(TagKey='domain', TagValues=['finance', 'marketing', 'engineering', 'hr'])

# Step 2: Assign tags to resources
lf.add_lf_tags_to_resource(
    Resource={
        'Table': {
            'DatabaseName': 'curated',
            'Name': 'customer_orders'
        }
    },
    LFTags=[
        {'TagKey': 'classification', 'TagValues': ['internal']},
        {'TagKey': 'domain', 'TagValues': ['finance']}
    ]
)

# Step 3: Grant access based on tags (not resource names)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/FinanceAnalyst'}},
    Resource={
        'LFTagPolicy': {
            'ResourceType': 'TABLE',
            'Expression': [
                {'TagKey': 'domain', 'TagValues': ['finance']},
                {'TagKey': 'classification', 'TagValues': ['public', 'internal']}
            ]
        }
    },
    Permissions=['SELECT'],
    PermissionsWithGrantOption=[]
)
```

**Benefit:** When new finance tables are created and tagged `domain=finance`, the FinanceAnalyst role automatically gains access. No permission update needed.

---

## Row and Column Level Filtering

### Column-Level Security

```python
# Grant access to specific columns only (hide PII)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/Analyst'}},
    Resource={
        'TableWithColumns': {
            'DatabaseName': 'curated',
            'Name': 'customers',
            'ColumnNames': ['customer_id', 'segment', 'region', 'signup_date']
            # Excluded: email, phone, ssn, address
        }
    },
    Permissions=['SELECT']
)
```

### Row-Level Security (Data Filters)

```python
# Create a filter that restricts rows by region
lf.create_data_cells_filter(
    TableData={
        'TableCatalogId': '123456789',
        'DatabaseName': 'curated',
        'TableName': 'orders',
        'Name': 'us-east-only',
        'RowFilter': {
            'FilterExpression': "region = 'us-east-1'"
        },
        'ColumnNames': ['order_id', 'amount', 'order_date', 'region']
    }
)

# Grant the filter to a principal
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/RegionalAnalyst'}},
    Resource={
        'DataCellsFilter': {
            'TableCatalogId': '123456789',
            'DatabaseName': 'curated',
            'TableName': 'orders',
            'Name': 'us-east-only'
        }
    },
    Permissions=['SELECT']
)
```

---

## Cross-Account Data Sharing

```python
# Account A (Data Producer): Share database with Account B

# Step 1: Grant permissions to external account
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': '987654321'}},  # Account B ID
    Resource={
        'Database': {'Name': 'curated_shared'}
    },
    Permissions=['DESCRIBE'],
    PermissionsWithGrantOption=['DESCRIBE']
)

lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': '987654321'}},
    Resource={
        'Table': {'DatabaseName': 'curated_shared', 'Name': 'fact_orders'}
    },
    Permissions=['SELECT'],
    PermissionsWithGrantOption=['SELECT']
)
```

```python
# Account B (Consumer): Accept and create resource link

# Step 1: Accept RAM share (automatic if within same org)
# Step 2: Create resource link in local catalog
glue = boto3.client('glue')
glue.create_database(
    DatabaseInput={
        'Name': 'shared_from_account_a',
        'TargetDatabase': {
            'CatalogId': '123456789',  # Account A
            'DatabaseName': 'curated_shared'
        }
    }
)
# Now Account B can query: SELECT * FROM shared_from_account_a.fact_orders
```

---

## Integration with Glue Catalog

Lake Formation wraps and extends the Glue Data Catalog:

| Feature | Glue Catalog Only | Glue + Lake Formation |
|---------|-------------------|----------------------|
| Schema registry | Yes | Yes |
| IAM-based access | Yes | Yes (legacy mode) |
| Column-level security | No | Yes |
| Row-level filtering | No | Yes |
| Tag-based access | No | Yes (LF-Tags) |
| Cross-account sharing | Manual S3 policies | Native via RAM |
| Audit trail | CloudTrail only | CloudTrail + LF audit |

---

## Data Location Registration

```python
# Register S3 locations that Lake Formation manages
lf.register_resource(
    ResourceArn='arn:aws:s3:::data-lake-curated',
    UseServiceLinkedRole=True  # LF service role accesses S3 on behalf of users
)

# Users don't need direct S3 permissions!
# LF provides temporary credentials scoped to permitted tables/columns
```

**How it works:**
1. User queries Athena: `SELECT * FROM curated.orders`
2. Athena asks Lake Formation: "Can this user read this table?"
3. LF checks grants → issues temporary S3 credentials (scoped to specific prefixes)
4. Athena reads only the permitted data from S3

---

## Permission Model — Grant and Revoke

```python
# Grant with delegation (PermissionsWithGrantOption)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/DataLead'}},
    Resource={'Database': {'Name': 'curated'}},
    Permissions=['CREATE_TABLE', 'DESCRIBE'],
    PermissionsWithGrantOption=['DESCRIBE']  # DataLead can grant DESCRIBE to others
)

# Revoke access
lf.revoke_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/FormerEmployee'}},
    Resource={'Table': {'DatabaseName': 'curated', 'Name': 'customers'}},
    Permissions=['SELECT']
)

# Batch grant (multiple tables at once)
lf.batch_grant_permissions(
    Entries=[
        {
            'Id': '1',
            'Principal': {'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/Analyst'}},
            'Resource': {'Table': {'DatabaseName': 'curated', 'Name': 'orders'}},
            'Permissions': ['SELECT']
        },
        {
            'Id': '2',
            'Principal': {'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/Analyst'}},
            'Resource': {'Table': {'DatabaseName': 'curated', 'Name': 'products'}},
            'Permissions': ['SELECT']
        }
    ]
)
```

---

## Hybrid IAM + Lake Formation Permissions

When migrating to LF, you run in hybrid mode where both IAM and LF permissions apply:

```
Access decision = IAM allows AND Lake Formation allows

Transition path:
1. Enable LF (settings → "Use only IAM access control" for existing databases)
2. New databases: LF-managed from the start
3. Migrate existing databases one at a time (revoke IAMAllowedPrincipals, add LF grants)
4. Eventually: all databases under LF control
```

---

## Interview Tips

> **Tip 1:** "How does Lake Formation differ from just using IAM for S3?" — "IAM controls S3 at the bucket/prefix level — you'd need hundreds of policies for table-level access. Lake Formation adds a semantic layer: grant access to tables, columns, and even rows. LF issues temporary, scoped S3 credentials so users never need direct S3 policies. Plus LF-Tags allow automatic permission inheritance when new tables are tagged."

> **Tip 2:** "How would you implement PII protection across a data lake?" — "Use Lake Formation column-level security. Tag columns containing PII (email, SSN, phone). Create data cell filters that exclude PII columns for general analysts. Grant full access only to specific roles (data stewards). Row-level filters can further restrict access by region or business unit. All access is auditable via CloudTrail."

> **Tip 3:** "Explain cross-account data sharing with Lake Formation" — "Producer account grants permissions to consumer's account ID (or org ID). AWS RAM creates the share automatically. Consumer creates a resource link in their Glue Catalog pointing to the shared database. Queries run in the consumer account, but data stays in the producer's S3. Producer pays for storage, consumer pays for compute (Athena/Redshift). No data copying needed."
