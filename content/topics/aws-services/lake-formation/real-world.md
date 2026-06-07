---
title: "AWS Lake Formation - Real-World Production Examples"
topic: aws-services
subtopic: lake-formation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, lake-formation, production, multi-team]
---

# AWS Lake Formation — Real-World Production Examples

## Pattern 1: Multi-Team Data Platform with LF-Tags

```python
import boto3

lf = boto3.client('lakeformation')

# Enterprise tag taxonomy (defined by central data governance team)
tags = {
    'domain': ['sales', 'marketing', 'finance', 'engineering', 'hr'],
    'sensitivity': ['public', 'internal', 'confidential', 'restricted'],
    'environment': ['dev', 'staging', 'prod'],
    'data_product_status': ['draft', 'certified', 'deprecated']
}

for key, values in tags.items():
    lf.create_lf_tag(TagKey=key, TagValues=values)

# Tag assignment automation (triggered when Glue Crawler registers new tables)
def auto_tag_table(database_name, table_name, table_metadata):
    """Auto-assign tags based on naming conventions and metadata"""
    tags_to_assign = []
    
    # Domain from database prefix
    domain = database_name.split('_')[0]  # e.g., sales_curated → sales
    tags_to_assign.append({'TagKey': 'domain', 'TagValues': [domain]})
    
    # Sensitivity from column names
    pii_columns = {'email', 'ssn', 'phone', 'address', 'dob'}
    table_columns = {col['Name'] for col in table_metadata['Columns']}
    if table_columns & pii_columns:
        tags_to_assign.append({'TagKey': 'sensitivity', 'TagValues': ['confidential']})
    else:
        tags_to_assign.append({'TagKey': 'sensitivity', 'TagValues': ['internal']})
    
    lf.add_lf_tags_to_resource(
        Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}},
        LFTags=tags_to_assign
    )

# Team access policies (one-time setup, applies to all current + future tables)
team_policies = {
    'SalesAnalyst': {'domain': ['sales'], 'sensitivity': ['public', 'internal']},
    'FinanceTeam': {'domain': ['sales', 'finance'], 'sensitivity': ['public', 'internal', 'confidential']},
    'DataEngineers': {'domain': ['sales', 'marketing', 'finance', 'engineering'], 'sensitivity': ['public', 'internal']},
    'DataStewards': {'domain': ['sales', 'marketing', 'finance', 'engineering', 'hr'], 'sensitivity': ['public', 'internal', 'confidential', 'restricted']}
}

for role, policy in team_policies.items():
    lf.grant_permissions(
        Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': f'arn:aws:iam::123456789:role/{role}'}},
        Resource={
            'LFTagPolicy': {
                'ResourceType': 'TABLE',
                'Expression': [
                    {'TagKey': 'domain', 'TagValues': policy['domain']},
                    {'TagKey': 'sensitivity', 'TagValues': policy['sensitivity']}
                ]
            }
        },
        Permissions=['SELECT']
    )
```

---

## Pattern 2: PII Protection with Column-Level Masking

```python
# Strategy: Create tiered access to the same table
# Tier 1: Full access (data stewards only)
# Tier 2: Masked PII (analysts see hashed emails, no SSN)
# Tier 3: No PII (aggregation-only access)

# Full table schema: customer_id, name, email, ssn, phone, segment, region, ltv

# Tier 3: No PII filter
lf.create_data_cells_filter(
    TableData={
        'TableCatalogId': '123456789',
        'DatabaseName': 'curated',
        'TableName': 'customers',
        'Name': 'no-pii-access',
        'RowFilter': {'FilterExpression': 'TRUE'},
        'ColumnNames': ['customer_id', 'segment', 'region', 'ltv']
    }
)

# Tier 2: Partial PII (name + hashed identifiers via view)
lf.create_data_cells_filter(
    TableData={
        'TableCatalogId': '123456789',
        'DatabaseName': 'curated',
        'TableName': 'customers',
        'Name': 'partial-pii-access',
        'RowFilter': {'FilterExpression': 'TRUE'},
        'ColumnNames': ['customer_id', 'name', 'email', 'segment', 'region', 'ltv']
        # SSN and phone excluded
    }
)

# Grant filters to roles
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/JuniorAnalyst'}},
    Resource={'DataCellsFilter': {'DatabaseName': 'curated', 'TableName': 'customers', 'Name': 'no-pii-access', 'TableCatalogId': '123456789'}},
    Permissions=['SELECT']
)

lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/SeniorAnalyst'}},
    Resource={'DataCellsFilter': {'DatabaseName': 'curated', 'TableName': 'customers', 'Name': 'partial-pii-access', 'TableCatalogId': '123456789'}},
    Permissions=['SELECT']
)
```

---

## Pattern 3: Cross-Account Data Sharing for Partner Access

```python
# Architecture: 3 accounts
# Account A (Producer): owns raw + curated data
# Account B (Internal Consumer): analytics team in separate account
# Account C (External Partner): limited read access

# Account A: Share curated tables with Account B (full access)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': '111111111111'}},  # Account B
    Resource={'Database': {'Name': 'curated'}},
    Permissions=['DESCRIBE'],
    PermissionsWithGrantOption=['DESCRIBE']
)
lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': '111111111111'}},
    Resource={'Table': {'DatabaseName': 'curated', 'Name': 'fact_orders', 'TableWildcard': {}}},
    Permissions=['SELECT'],
    PermissionsWithGrantOption=['SELECT']
)

# Account A: Share filtered view with Account C (partner - restricted)
lf.create_data_cells_filter(
    TableData={
        'TableCatalogId': '123456789',
        'DatabaseName': 'curated',
        'TableName': 'partner_metrics',
        'Name': 'partner-c-filter',
        'RowFilter': {'FilterExpression': "partner_id = 'PARTNER_C'"},
        'ColumnNames': ['metric_date', 'impressions', 'clicks', 'conversions']
    }
)

lf.grant_permissions(
    Principal={'DataLakePrincipal': {'DataLakePrincipalIdentifier': '222222222222'}},  # Account C
    Resource={'DataCellsFilter': {'DatabaseName': 'curated', 'TableName': 'partner_metrics', 'Name': 'partner-c-filter', 'TableCatalogId': '123456789'}},
    Permissions=['SELECT']
)
```

---

## Pattern 4: Compliance Audit Trail

```python
# Automated compliance reporting using CloudTrail + Athena

# Query: Who accessed PII tables in the last 30 days?
pii_audit_query = """
SELECT 
    userIdentity.arn AS accessor,
    requestParameters.resource.table.name AS table_name,
    eventTime,
    sourceIPAddress,
    COUNT(*) AS access_count
FROM cloudtrail_logs
WHERE eventSource = 'lakeformation.amazonaws.com'
  AND eventName = 'GetTemporaryGlueTableCredentials'
  AND requestParameters.resource.table.databaseName = 'curated'
  AND requestParameters.resource.table.name IN ('customers', 'employees', 'payments')
  AND year = '2024' AND month = '01'
GROUP BY 1, 2, 3, 4
ORDER BY eventTime DESC
"""

# Query: Permission changes audit
permission_changes_query = """
SELECT 
    userIdentity.arn AS granted_by,
    eventName AS action,
    json_extract_scalar(requestParameters, '$.principal.dataLakePrincipal.dataLakePrincipalIdentifier') AS granted_to,
    json_extract_scalar(requestParameters, '$.resource') AS resource,
    json_extract_scalar(requestParameters, '$.permissions') AS permissions,
    eventTime
FROM cloudtrail_logs
WHERE eventSource = 'lakeformation.amazonaws.com'
  AND eventName IN ('GrantPermissions', 'RevokePermissions', 'BatchGrantPermissions')
  AND year = '2024'
ORDER BY eventTime DESC
"""

# Automated alert: detect privilege escalation
escalation_detection = """
SELECT userIdentity.arn, requestParameters
FROM cloudtrail_logs
WHERE eventSource = 'lakeformation.amazonaws.com'
  AND eventName = 'GrantPermissions'
  AND json_extract_scalar(requestParameters, '$.permissionsWithGrantOption') != '[]'
  AND userIdentity.arn NOT LIKE '%/LakeAdmin%'
"""
```

---

## Migration Guide: IAM to Lake Formation

| Phase | Action | Risk Level | Rollback |
|-------|--------|-----------|----------|
| 1. Audit | Map all IAM S3 policies to table-level access | None | N/A |
| 2. Enable LF | Set data lake admins, register locations | Low | Remove admins |
| 3. Hybrid grants | Create LF grants matching IAM access | None | Delete grants |
| 4. Test | Verify access with all roles in staging | Low | N/A |
| 5. Cutover | Revoke IAMAllowedPrincipals per database | High | Re-grant IAMAllowed |
| 6. Validate | Confirm all services still function | High | Re-grant IAMAllowed |
| 7. Cleanup | Remove old S3 IAM policies | Medium | Restore policies |

```python
# Automated migration script: discover existing access
def audit_current_access():
    """Analyze IAM policies to identify who accesses which S3 paths"""
    iam = boto3.client('iam')
    
    # Get all roles with S3 access
    roles = iam.list_roles()['Roles']
    access_map = []
    
    for role in roles:
        policies = iam.list_attached_role_policies(RoleName=role['RoleName'])
        for policy in policies['AttachedPolicies']:
            policy_doc = iam.get_policy_version(
                PolicyArn=policy['PolicyArn'],
                VersionId=iam.get_policy(PolicyArn=policy['PolicyArn'])['Policy']['DefaultVersionId']
            )['PolicyVersion']['Document']
            
            for statement in policy_doc.get('Statement', []):
                resources = statement.get('Resource', [])
                if isinstance(resources, str):
                    resources = [resources]
                s3_resources = [r for r in resources if 'data-lake' in r]
                if s3_resources:
                    access_map.append({
                        'role': role['RoleName'],
                        's3_paths': s3_resources,
                        'actions': statement.get('Action', [])
                    })
    return access_map
```

---

## Interview Tips

> **Tip 1:** "How would you set up a multi-team data platform with proper governance?" — "Lake Formation with LF-Tags. Define a tag taxonomy (domain, sensitivity, tier). Auto-tag tables via Lambda when crawlers register them. Grant access using tag expressions — one policy per team role covers all current and future tables. Central governance owns the taxonomy; domain teams assign tags to their tables. This scales from 5 tables to 5,000 without per-table grants."

> **Tip 2:** "How do you ensure PII compliance in a shared data lake?" — "Three layers: (1) LF-Tags mark sensitivity level on every table. (2) Data cell filters create tiered access — full, partial-PII, no-PII views of the same table. (3) CloudTrail audit queries detect who accessed PII tables and when. Automated alerts fire if non-approved roles access restricted data. Column-level security means analysts query the same table name but see different columns based on their role."

> **Tip 3:** "How do you share data across AWS accounts securely?" — "Lake Formation cross-account sharing via RAM. Producer grants table-level (or filtered) access to consumer account ID. Consumer creates a resource link in their Glue Catalog. Data stays in producer's S3 — no copying. For partners, add data cell filters to restrict rows and columns before sharing. All access auditable in CloudTrail. Revocation is instant (revoke permission, access stops immediately)."
