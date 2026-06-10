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

---

## ⚡ Quick-fire Q&A

**Q: What is AWS Lake Formation and what problem does it solve?**
A: Lake Formation is a service that simplifies building, securing, and managing data lakes on S3. It solves the problem of managing fine-grained access control across multiple services (Athena, Glue, Redshift Spectrum, EMR) by providing a centralized permission model on top of the Glue Data Catalog — replacing complex S3 bucket policies and IAM policies with a unified governance layer.

**Q: What is the difference between Lake Formation permissions and IAM/S3 permissions?**
A: Lake Formation adds a layer of fine-grained permissions (database, table, column, row-level) on top of IAM. Both must allow access for a principal to read data. Lake Formation enables column-level security and row-level filters that S3/IAM alone cannot enforce, without exposing raw S3 paths.

**Q: What is column-level security in Lake Formation?**
A: Column-level security restricts which columns a principal can query in a Glue table. For example, a data analyst can query a customer table but the `ssn` and `credit_card` columns are excluded from their grants — Athena and Redshift Spectrum enforce these restrictions automatically.

**Q: What are Lake Formation Data Filters (row-level security)?**
A: Data Filters define row-level access conditions (a WHERE clause) on a table. When granted to a principal, their queries on that table automatically apply the filter — for example, a regional analyst only sees rows where `region = 'us-east-1'`, enforced transparently by Athena or Redshift Spectrum.

**Q: What are Lake Formation governed tables?**
A: Governed tables are an evolution of Glue tables that support ACID transactions on S3 data, automatic compaction, and time-travel queries. They allow concurrent read/write without conflicts — similar to Delta Lake or Apache Iceberg — within the Lake Formation governance framework.

**Q: How do you share data across AWS accounts using Lake Formation?**
A: Use AWS RAM (Resource Access Manager) to share Glue Data Catalog databases and tables with another account. The recipient account's Lake Formation administrator then grants permissions to their principals. This enables secure data mesh and cross-account analytics without copying data.

**Q: What is the Lake Formation blueprint and workflow feature?**
A: Blueprints are templates for common ingestion patterns (database snapshot, incremental database, CloudTrail logs) that auto-generate Glue crawlers and jobs to ingest data into the data lake. Workflows orchestrate these crawlers and jobs in a dependency graph.

**Q: How does Lake Formation integrate with Athena for data governance?**
A: When Lake Formation is enabled for a Glue Data Catalog database, Athena queries automatically respect Lake Formation table, column, and row-level permissions. Users without column access receive errors when querying those columns, and row filters are transparently applied — no changes needed to Athena queries.

---

## 💼 Interview Tips

- Articulate Lake Formation's core value proposition clearly: it decouples data governance from physical S3 access, enabling fine-grained table/column/row permissions managed centrally rather than through complex bucket policies.
- Senior interviewers want to hear about the data mesh use case: Lake Formation cross-account sharing via RAM enables domain teams to own and govern their data while sharing it securely with consumers — a real pattern in modern data platforms.
- Avoid the mistake of thinking Lake Formation replaces IAM: both must grant access. Lake Formation adds a permission layer on top of IAM; a principal needs both IAM permissions and Lake Formation permissions to access data.
- Mention the transition from the old S3-based permission model (using IAM and bucket policies) to the Lake Formation model — organizations migrating to it must explicitly opt registered data locations into Lake Formation governance.
- Demonstrate column-level security knowledge with a concrete example: PII columns (SSN, phone, email) excluded from analyst grants while data scientists get full access — this resonates as a real compliance use case.
- Know that Lake Formation permissions are enforced at query time by Athena and Redshift Spectrum, but EMR requires additional configuration (Lake Formation EMR integration) — a common gotcha interviewers probe.
