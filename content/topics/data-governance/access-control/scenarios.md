---
title: "Access Control & RBAC — Scenarios"
topic: data-governance
subtopic: access-control
content_type: scenario_question
tags: [access-control, rbac, interview, scenarios, iam]
---

# Access Control & RBAC — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Setting Up Access for a New Analyst

**Scenario:** A new revenue analyst joins the team. Walk through how you grant them appropriate data access.

<details>
<summary>💡 Hint</summary>

Follow the *principle of least privilege*: grant only the access needed for the role, not the broadest access that would be convenient. For an analyst: read-only on the gold layer, no access to raw tables or sensitive schemas, and PII should be masked (not visible). Use a role (not direct user grants) so when the next analyst joins, you just add them to the group — not repeat the individual grant process. Ask "what tables do they need for their job?" not "what can we give them?"

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Determine what access they need**
```
Revenue analyst needs:
- READ access to gold.orders, gold.revenue_daily, gold.customers
- NO access to gold.payroll, finance.sensitive_*, or raw bronze tables
- Masked PII (email should be hashed, not visible)
```

**Step 2: Use the existing analyst_revenue role**
```sql
-- Don't create custom access per user — use roles
-- Grant the existing role to the new user
GRANT ROLE ANALYST_REVENUE TO USER jane.new_analyst;

-- Verify what they'll see
USE ROLE ANALYST_REVENUE;
SELECT email FROM gold.customers LIMIT 5;
-- → 'e3b0c44...' (hashed, PII masked as expected)
```

**Step 3: Submit access request for any additional access**
```
If they need PII access:
→ Submit access request via governance portal
→ Business justification required
→ DPO review and approval
→ Time-limited (90 days), not permanent
```

**Step 4: Verify and document**
```sql
-- Confirm grants
SHOW GRANTS TO USER jane.new_analyst;
-- Should show: ANALYST_REVENUE only

-- No overprivileged access:
USE ROLE ANALYST_REVENUE;
SELECT * FROM gold.payroll;  -- Should fail: "Insufficient privileges"
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Access Breach Investigation

**Scenario:** Your anomaly detection system flags that a data analyst ran 500 queries on PII tables in a single day — their normal is 5-10. How do you investigate?

<details>
<summary>💡 Hint</summary>

**Step 1: Gather facts before assuming the worst**

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Gather facts before assuming the worst**
```sql
-- What queries did they run?
SELECT query_text, table_name, rows_returned, queried_at
FROM query_logs
WHERE user_email = 'suspect@company.com'
  AND DATE(queried_at) = '2024-01-15'
  AND table_name IN (SELECT table_name FROM data_catalog.assets WHERE 'pii' = ANY(tags))
ORDER BY queried_at;
```

**Step 2: Check if the queries are legitimate**
```
Scenarios:
A) Analyst ran a loop in Python that sent 500 identical queries → coding error, not malicious
B) Analyst was building a new report requiring many small queries → legitimate, but fix to batch
C) Analyst downloaded entire customer table → potential data exfiltration, escalate to security
D) Queries all from unusual IP or at 3AM → potential account compromise
```

**Step 3: Determine next action based on evidence**
```
If A or B: Coach on best practices (use LIMIT, batch queries, use Spark for large analysis)
If C: 
  1. Immediately revoke access
  2. Escalate to Information Security
  3. Check if data was copied to external location
  4. Notify DPO (GDPR breach notification may be required)
If D:
  1. Lock account
  2. Force password reset
  3. Review all actions taken during suspicious window
```

**Step 4: Prevention**
```sql
-- Add row limit policy for non-admin roles
CREATE OR REPLACE ROW ACCESS POLICY pii_row_limit AS (val VARCHAR) RETURNS BOOLEAN ->
  -- Enforce max rows via query limit at session level
  -- (use Snowflake Resource Monitor or query tag enforcement)
  TRUE;

-- Better: alert on excessive downloads
-- If rows_returned > 10000 on PII table → alert security immediately
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing RBAC for 200 Teams

**Scenario:** Your company has 200 domain teams each with analysts, engineers, and data scientists. How do you design an RBAC system that scales without needing per-team role configuration?

<details>
<summary>💡 Hint</summary>

For 200 teams, you can't configure individual per-team roles — that's 600 roles (200 × 3 job types) that nobody can audit. Instead, design a *hierarchy*: a small set of platform base roles (analyst_read, engineer_write, scientist_read, pii_approved) that are attribute-driven — membership is controlled by IdP group attributes (team=revenue, function=analyst, pii_approved=false). The platform role grants access to the appropriate schemas by tier, and PII masking is enforced at the column level regardless of role. New teams get access automatically by being added to the right IdP groups — no per-team role configuration needed.

</details>

<details>
<summary>✅ Solution</summary>

**Design: Attribute-driven role hierarchy**

```
Base roles (platform-defined):
  analyst_read:     SELECT on gold tables (masked PII)
  engineer_write:   read + write bronze/silver, read gold
  scientist_gpu:    analyst_read + access to ML datasets
  admin_platform:   full access
  pii_approved:     SELECT on PII columns unmasked (requires DPO approval)

Domain modifier roles (auto-generated per domain):
  domain_sales:     grants access to sales/* tables only
  domain_finance:   grants access to finance/* tables only
  domain_marketing: grants access to marketing/* tables only

Combined via role grants:
  ANALYST_SALES = analyst_read + domain_sales
  ENGINEER_FINANCE = engineer_write + domain_finance
```

```python
# Terraform generator: auto-create domain roles from config
def generate_domain_roles(domains: list[str], base_roles: list[str]) -> str:
    """Generate Terraform for all domain×base_role combinations."""
    tf_blocks = []
    
    for domain in domains:
        for base_role in base_roles:
            combined_role = f"{base_role.upper()}_{domain.upper()}"
            tf_blocks.append(f"""
resource "snowflake_role" "{combined_role.lower()}" {{
  name    = "{combined_role}"
  comment = "Auto-generated: {base_role} access for {domain} domain"
}}

resource "snowflake_role_grants" "{combined_role.lower()}_base" {{
  role_name = snowflake_role.{combined_role.lower()}.name
  roles     = [snowflake_role.{base_role}.name, snowflake_role.domain_{domain}.name]
}}
""")
    
    return "\n".join(tf_blocks)

# Result: 200 domains × 4 base roles = 800 roles, all auto-managed
# User assignment: HR system webhook assigns role on hire based on team/role attributes
```

**Key scalability principles:**
```
1. Never create per-user grants — always via roles
2. Role names encode function + domain (parseable by automation)
3. Terraform manages all role definitions (PR review = access review)
4. HR system auto-provisions on hire, auto-revokes on departure
5. Quarterly review: anomaly detection flags unused grants for cleanup
```

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is the difference between authentication and authorization?**
A: Authentication verifies who a user is (identity), while authorization determines what that user is allowed to do (permissions). Both are required for secure data access control — authentication comes first, authorization follows.

**Q: What is Role-Based Access Control (RBAC) and how does it differ from Attribute-Based Access Control (ABAC)?**
A: RBAC assigns permissions to roles and users are assigned roles, keeping management simple and scalable. ABAC grants access based on attributes of the user, resource, and environment (e.g., department, data sensitivity, time of day), enabling finer-grained policies at the cost of complexity.

**Q: What is the principle of least privilege and why does it matter in data platforms?**
A: Least privilege means granting users and services only the minimum permissions needed to perform their tasks. It limits blast radius when credentials are compromised and reduces the risk of accidental data modification or exfiltration.

**Q: How do you implement column-level security in a data warehouse?**
A: Most modern warehouses (Snowflake, BigQuery, Redshift) support column masking policies — you define a policy that returns the real value for authorized roles and a masked/null value for others. Alternatively, create views that expose only allowed columns and grant access to the view rather than the base table.

**Q: What is row-level security (RLS) and when would you use it?**
A: RLS restricts which rows a user can see in a table based on their identity or role. Use it for multi-tenant data models where different customers or business units should only see their own data, without maintaining separate tables per tenant.

**Q: How do service accounts differ from user accounts in data platform access control?**
A: Service accounts are non-human identities used by applications, pipelines, and automated jobs. They should have tightly scoped permissions, no interactive login, and their credentials should be managed via a secrets manager and rotated regularly.

**Q: What is the purpose of data access auditing?**
A: Auditing records who accessed what data, when, and from where. It enables compliance reporting, breach investigation, anomaly detection, and demonstrating due diligence to regulators. Audit logs should be immutable and stored separately from the data they protect.

---

## 💼 Interview Tips

- Lead with least privilege as a design principle before diving into implementation details — it frames your answer around security-first thinking that senior interviewers respect.
- Be specific about tools: mention Unity Catalog, AWS Lake Formation, BigQuery IAM, or Snowflake RBAC depending on the stack, rather than speaking only in abstract terms.
- Discuss service account credential rotation and secrets management proactively — it shows you think about access control as an ongoing operational practice, not a one-time setup.
- For senior roles, connect access control to data mesh principles: explain how domain teams own their data products and grant access through a catalog, rather than a central team managing all permissions.
- Mention audit logging when discussing access control — interviewers at regulated companies (finance, healthcare) will specifically probe whether you know compliance requirements like SOC 2 and HIPAA.
- Avoid describing overly permissive designs like "give the team admin access for now" — even in hypothetical scenarios it signals poor security instincts.
