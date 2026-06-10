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

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between an IAM Role and an IAM User?**
A: An IAM User has permanent long-term credentials (access key + secret). An IAM Role has no credentials — it's assumed by trusted entities (EC2, Lambda, another AWS account) to get temporary credentials via STS. Roles are the best practice for AWS services and cross-account access; users are for humans with programmatic access needs.

**Q: What is the principle of least privilege and how do you apply it in data pipelines?**
A: Least privilege means granting only the permissions needed to perform a task. For a Glue ETL job, this means the job's IAM role should have `s3:GetObject` only on the input prefix and `s3:PutObject` only on the output prefix — not `s3:*` on all buckets.

**Q: What is an IAM Policy and what are the different policy types?**
A: IAM Policies are JSON documents defining allow/deny permissions. Types include: Identity-based policies (attached to users, groups, roles), Resource-based policies (attached to resources like S3 buckets, allowing cross-account access), Permission Boundaries (max permissions a role can have), SCPs (Service Control Policies in AWS Organizations), and Session Policies (temporary scope reductions).

**Q: How does IAM policy evaluation work when multiple policies apply?**
A: AWS evaluates all applicable policies with an explicit deny overriding everything. The default is deny. A request is allowed only if there is an explicit allow and no explicit deny from any applicable policy (identity-based, resource-based, SCP, permission boundary).

**Q: What is IAM assume role and how does cross-account access work?**
A: To access resources in Account B from Account A: create a role in Account B with a trust policy allowing Account A's principal to assume it. In Account A, attach a policy allowing `sts:AssumeRole` on that role's ARN. The caller assumes the role via STS and receives temporary credentials scoped to Account B.

**Q: What are IAM Permission Boundaries?**
A: Permission Boundaries set the maximum permissions an identity-based policy can grant to a role or user. Even if a role has `AdministratorAccess`, a permission boundary restricting it to S3 means the effective permissions are only S3 actions. They are used to safely delegate IAM role creation to developers.

**Q: What is the difference between `aws:PrincipalOrgID` and `aws:SourceAccount` condition keys?**
A: `aws:PrincipalOrgID` restricts access to principals within your AWS Organization — useful for org-wide S3 bucket policies. `aws:SourceAccount` restricts resource-based policy actions to a specific account — commonly used in Lambda resource policies to prevent confused deputy attacks.

**Q: How do you audit IAM permissions in a large AWS environment?**
A: Use IAM Access Analyzer to identify resource policies granting external access, IAM Credentials Report for user credential status, AWS Config rules for policy compliance, CloudTrail for API call audit, and Access Advisor to see last-used service permissions for right-sizing roles.

---

## 💼 Interview Tips

- Always advocate for roles over long-term access keys — this is the single most important IAM best practice and interviewers expect you to state it clearly and explain why (key rotation burden, accidental exposure risk).
- Senior interviewers probe cross-account access deeply: be able to walk through the exact trust policy and permissions policy needed for a Lambda in Account A to read from an S3 bucket in Account B.
- Demonstrate understanding of the confused deputy problem: a Lambda with broad permissions being triggered by untrusted sources can act on behalf of the attacker. Mitigate with `aws:SourceAccount` and `aws:SourceArn` conditions.
- Avoid vague answers about "giving access" — always specify the exact policy element: which actions (e.g., `s3:GetObject`), which resources (specific ARN with prefix), and which conditions (e.g., `aws:RequestedRegion`).
- Mention IAM Access Analyzer for proactive security: it continuously monitors resource policies for external access and generates findings automatically, which shows operational maturity.
- Know SCPs in AWS Organizations context: even if a root account has `AdministratorAccess`, an SCP denying `s3:DeleteBucket` on the OU prevents deletion — important for data governance in multi-account data lake architectures.
