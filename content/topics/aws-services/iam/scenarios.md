---
title: "AWS IAM - Scenario Questions"
topic: aws-services
subtopic: iam
content_type: scenario_question
tags: [aws, iam, interview, scenarios, security, roles]
---

# Scenario Questions — AWS IAM

<article data-difficulty="junior">

## 🟢 Junior: Create a Least-Privilege Glue Role

**Scenario:** Create an IAM role for a Glue ETL job that needs to: read from `s3://raw-data/orders/`, write to `s3://curated-data/orders/`, read/write the Glue Catalog, and write CloudWatch logs. Follow least-privilege principle.

<details>
<summary>✅ Solution</summary>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadSourceData",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                "arn:aws:s3:::raw-data",
                "arn:aws:s3:::raw-data/orders/*"
            ]
        },
        {
            "Sid": "WriteTargetData",
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:DeleteObject"],
            "Resource": "arn:aws:s3:::curated-data/orders/*"
        },
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
                "glue:CreatePartition", "glue:BatchCreatePartition",
                "glue:UpdateTable"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:123456789:catalog",
                "arn:aws:glue:us-east-1:123456789:database/raw_data",
                "arn:aws:glue:us-east-1:123456789:database/curated",
                "arn:aws:glue:us-east-1:123456789:table/raw_data/*",
                "arn:aws:glue:us-east-1:123456789:table/curated/*"
            ]
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": "arn:aws:logs:us-east-1:123456789:log-group:/aws-glue/*"
        }
    ]
}
```

**Key least-privilege principles applied:**
- Read-only on source bucket (no write/delete)
- Write-only on target path (can't read other data)
- Specific Glue Catalog databases (not `*`)
- No `s3:*` or `glue:*` wildcard permissions
- Trust policy: only Glue service can assume this role

</details>

</article>
