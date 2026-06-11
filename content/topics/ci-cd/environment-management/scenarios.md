---
title: "Environment Management — Scenarios"
topic: ci-cd
subtopic: environment-management
content_type: scenario_question
tags: [ci-cd, environments,secrets,config,parity, interview, scenarios]
---

# Environment Management — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Basic Scenario

**Scenario:** You joined a team and noticed the pipeline code has `DB_PASSWORD = 'hardcoded123'` in the source file committed to GitHub. What do you do?

<details>
<summary>💡 Hint</summary>

Treat the password as compromised (it's in git history). Rotate it immediately. Remove from code. Add to secrets manager. Add `.env` to `.gitignore`. Consider using detect-secrets pre-commit hook to prevent recurrence.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Rotate the password (it's compromised in git history)
# Log into database → change password

# Step 2: Remove from code
# pipeline.py:
import os
DB_PASSWORD = os.environ["DB_PASSWORD"]  # read from environment

# Step 3: Add to .gitignore
echo ".env" >> .gitignore
echo "*.env" >> .gitignore

# Step 4: Add to GitHub Actions secrets
# GitHub → Settings → Secrets → New: DB_PASSWORD = <new-password>

# Step 5: Scrub git history (optional — password already rotated)
git filter-repo --replace-text <(echo 'hardcoded123==>REDACTED')
git push --force-with-lease
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Intermediate Challenge

**Scenario:** Your staging environment uses different database types than production (SQLite vs PostgreSQL), and you're seeing bugs that only appear in production. How do you fix environment parity?

<details>
<summary>💡 Hint</summary>

Provision staging with the same database type as production. Use Docker Compose or Terraform to define the staging environment identically. Anonymize production data for staging rather than using SQLite with fake data.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# docker-compose.staging.yml — PostgreSQL in staging (not SQLite)
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: staging_db
      POSTGRES_USER: staging
      POSTGRES_PASSWORD: ${STAGING_DB_PASSWORD}
    volumes:
      - staging-data:/var/lib/postgresql/data

  pipeline:
    image: my-pipeline:latest
    environment:
      ENVIRONMENT: staging
      DB_URL: postgresql://staging:${STAGING_DB_PASSWORD}@postgres:5432/staging_db
    depends_on:
      - postgres
```

```python
# Anonymize prod data for staging (never use raw prod data)
def create_staging_snapshot(prod_engine, staging_engine):
    df = pd.read_sql("SELECT * FROM orders LIMIT 100000", prod_engine)
    df["email"] = df["customer_id"].apply(lambda x: f"test_{x}@example.com")
    df["name"] = df["customer_id"].apply(lambda x: f"Test User {x}")
    df.to_sql("orders", staging_engine, if_exists="replace", index=False)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Challenge

**Scenario:** Design the environment management strategy for a DE platform with 20 teams, each needing isolated dev environments, a shared staging, and production. Include secrets management.

<details>
<summary>💡 Hint</summary>

Personal dev environments via K8s namespaces or ephemeral Docker Compose stacks. Shared staging mirrors prod. Secrets via AWS Secrets Manager with IAM role-based access (teams access only their own secrets). Terraform for consistent infrastructure provisioning.

</details>

<details>
<summary>✅ Solution</summary>

```
Environment architecture:

dev:
  - Per-engineer namespace in K8s (or docker-compose locally)
  - Spun up on-demand via GitHub Action or script
  - Ephemeral test database (SQLite or small Postgres)
  - Secrets: personal test credentials in local .env

staging:
  - Shared, always-on, mirrors prod infrastructure
  - Anonymized production data snapshot (weekly refresh)
  - Secrets: staging-specific in AWS Secrets Manager
    path: staging/pipeline/<service>/db-creds

production:
  - Secrets: prod-specific in AWS Secrets Manager
    path: prod/pipeline/<service>/db-creds
  - IAM role per service (least privilege)
  - Secret rotation: automatic, 30-day cycle
```

```python
# Teams access only their own secrets
# IAM policy: allow GetSecretValue on path matching team prefix
# arn:aws:secretsmanager:*:*:secret:prod/finance/*  → finance-team-role
# arn:aws:secretsmanager:*:*:secret:prod/marketing/* → marketing-team-role
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the principle of environment parity and why does it matter?**
A: Environment parity means dev, staging, and production use the same infrastructure stack (same database type, same OS, same runtime versions). Without it, bugs appear only in production because the environments behave differently — defeating the purpose of staging.

**Q: What is a secrets manager and why should DE teams use one?**
A: A secrets manager (AWS Secrets Manager, HashiCorp Vault, Azure Key Vault) stores credentials securely, provides audit logs of access, supports automatic rotation, and controls access via IAM. It replaces storing passwords in environment files, code, or CI variables that can be exposed.

**Q: What should and should not be in a .env file?**
A: .env files should contain local development credentials and config — never production credentials. They must be in .gitignore and never committed to git. For CI/CD, use the CI platform's secret store (GitHub Actions Secrets). For production, use a dedicated secrets manager.

**Q: How do you prevent using production data in non-production environments?**
A: Network isolation (staging can't reach prod database). Anonymized data exports for staging (hash/pseudonymize PII, shuffle sensitive values). Synthetic data generation for dev. Periodic audits to verify staging data is not real PII.

**Q: What is infrastructure as code and how does it support environment parity?**
A: IaC (Terraform, CloudFormation, Pulumi) defines infrastructure in code, enabling identical provisioning across environments. Same Terraform modules create dev, staging, and prod — differences only in variables (instance size, replica count). This guarantees structural parity.

**Q: How do you handle secrets rotation without downtime?**
A: Use short-lived credentials (Vault dynamic secrets, IAM roles) that rotate automatically. For static credentials, use a dual-secret pattern: both old and new credentials are valid during rotation window. Applications read the current secret at startup; rotation updates the secret manager value and triggers a rolling restart.

**Q: What environment variables are always required for a data pipeline in production?**
A: ENVIRONMENT (dev/staging/production), database connection strings, cloud credentials (via IAM role, not env var), logging configuration, feature flags, and service URLs. Never hardcode any of these — always environment-injected.

---

## 💼 Interview Tips

- Lead with secrets management as a security-first answer — never commit credentials, use a secrets manager in production. It signals security awareness immediately.
- Mention environment parity explicitly and give a concrete example of what breaks without it (SQLite vs PostgreSQL query behavior differences).
- AWS Secrets Manager or HashiCorp Vault by name — tool-specific knowledge distinguishes you from generic answers.
- The non-prod data safety point (anonymize, don't use real PII in staging) is often missed by candidates and is critical for regulated industries.
- For senior architecture questions, map secrets to IAM roles (per-service, least privilege) — show you think about access control as part of environment design.
- Avoid describing .env files as the solution for staging/production — they're development tools only.
