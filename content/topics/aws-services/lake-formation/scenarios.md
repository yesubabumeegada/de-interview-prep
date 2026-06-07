---
title: "AWS Lake Formation - Scenario Questions"
topic: aws-services
subtopic: lake-formation
content_type: scenario_question
tags: [aws, lake-formation, interview, scenarios, governance]
---

# Scenario Questions — AWS Lake Formation

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement Column-Level Security for PII

**Scenario:** Your data lake has a `customers` table with columns: `customer_id`, `name`, `email`, `ssn`, `purchase_history`. Analysts should see all columns EXCEPT `ssn` and `email`. The compliance team should see everything. Implement using Lake Formation.

<details>
<summary>✅ Solution</summary>

```python
import boto3
lakeformation = boto3.client('lakeformation')

# Grant analysts access to non-PII columns only
lakeformation.grant_permissions(
    Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123:role/AnalystRole'},
    Resource={
        'TableWithColumns': {
            'DatabaseName': 'curated',
            'Name': 'customers',
            'ColumnNames': ['customer_id', 'name', 'purchase_history']
            # Excludes: ssn, email
        }
    },
    Permissions=['SELECT']
)

# Grant compliance team full access
lakeformation.grant_permissions(
    Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123:role/ComplianceRole'},
    Resource={
        'Table': {'DatabaseName': 'curated', 'Name': 'customers'}
    },
    Permissions=['SELECT', 'ALTER', 'DROP']
)
```

**Result:** When analysts query the table via Athena, they only see 3 columns. If they try `SELECT ssn`, they get "Access Denied." Compliance team sees all 5 columns.

**Why Lake Formation over IAM policies:** IAM can't do column-level restrictions on S3 data. Lake Formation provides SQL-database-style fine-grained access control on top of S3/Glue Catalog data.

</details>

</article>
