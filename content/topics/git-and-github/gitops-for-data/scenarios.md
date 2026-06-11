---
title: "GitOps for Data — Scenarios"
topic: git-and-github
subtopic: gitops-for-data
content_type: scenario_question
tags: [git, github, gitops-for-data, interview, scenarios]
---

# GitOps for Data — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Getting Started

**Scenario:** Your team asks you to set up a GitOps process for deploying Airflow DAGs — currently everyone SSHes to the server manually. Describe your approach.

<details>
<summary>💡 Hint</summary>

Start simple: GitHub Actions workflow on push to main that syncs the dags/ folder to S3 (for MWAA) or restarts the Airflow pods. Remove SSH deploy access. Document the new process clearly.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# .github/workflows/deploy-dags.yml
name: Deploy DAGs to MWAA
on:
  push:
    branches: [main]
    paths: ["dags/**"]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123:role/GitHubActions
          aws-region: us-east-1
      - name: Sync DAGs to MWAA S3
        run: aws s3 sync dags/ s3://my-mwaa-bucket/dags/ --delete
```

Remove SSH access to the server. Every DAG change now goes through PR → CI → merge → auto-deploy.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling a Real Problem

**Scenario:** A production DAG was manually edited on the server by an on-call engineer during an incident. Now git and production are out of sync. How do you reconcile this and prevent it happening again?

<details>
<summary>💡 Hint</summary>

Sync git to match production (pull the manual changes into git, not the other way). Then remove write access to the server. For incidents: establish a runbook that says 'make temporary fix, then immediately open a PR with the same change.'

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Get the manual fix into git
ssh prod-server cat /opt/airflow/dags/revenue.py > /tmp/revenue_manual_fix.py
diff dags/revenue.py /tmp/revenue_manual_fix.py  # see what changed

# Create PR with the same fix
git checkout -b fix/revenue-incident-fix
cp /tmp/revenue_manual_fix.py dags/revenue.py
git commit -m 'fix: apply incident hotfix for null order handling (from manual fix during incident)'
gh pr create

# Step 2: Prevent future out-of-sync
# Remove SSH write access to DAG directory
# Use GitOps: auto-sync from git on every merge to main
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design at Scale

**Scenario:** Design a GitOps architecture for a DE platform where 20 domain teams independently deploy their pipelines to shared infrastructure, with full audit trail and self-service.

<details>
<summary>💡 Hint</summary>

Teams deploy via PRs to a manifests repo. ArgoCD or Flux watches the manifests repo and applies changes. Teams have write access to their directory only (CODEOWNERS). Platform team owns shared infra. Audit trail = git history + ArgoCD deployment events.

</details>

<details>
<summary>✅ Solution</summary>

Platform team maintains 'k8s-manifests' repo. Each domain team has a directory. ArgoCD watches and syncs. Teams open PRs to their directory for any deployment change. Full git history = full audit trail. Platform team CODEOWNERS their shared infra.

```
k8s-manifests/
├── .github/CODEOWNERS
│   finance-de/   @finance-de-team
│   platform/     @platform-team
├── finance-de/
│   ├── pipelines/
│   └── cronjobs/
└── platform/
    ├── airflow/
    └── spark-operator/
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is GitOps and how does it differ from traditional deployment?**
A: GitOps makes git the single source of truth for infrastructure and deployments. Changes are made through PRs (reviewed, tested), then automatically applied. Traditional deployment often involves manual CLI commands or SSH — with no audit trail and risk of drift between git and production.

**Q: What is 'configuration drift' and how does GitOps prevent it?**
A: Drift is when the live environment diverges from what's in git — someone made a manual change that wasn't committed. GitOps prevents drift by making git the authoritative source and continuously reconciling (ArgoCD, Flux) or deploying on every merge, leaving no mechanism for out-of-band changes.

**Q: How do you roll back in a GitOps workflow?**
A: `git revert <commit>` creates a new commit that undoes the bad change. Pushing to main triggers auto-deploy of the reverted state. Alternatively, tag a known-good git SHA and deploy that tag. Either way, the rollback itself is a git operation — tracked, reviewed, and auditable.

**Q: What is the difference between push-based and pull-based GitOps?**
A: Push-based GitOps (GitHub Actions): CI/CD pushes changes to the target environment on merge. Pull-based GitOps (ArgoCD, Flux): an agent in the cluster continuously pulls from git and reconciles. Pull-based is more secure (no outbound credentials from CI) and handles drift better.

**Q: How do you handle secrets in a GitOps workflow?**
A: Never commit secrets to git. Use sealed secrets (Bitnami Sealed Secrets), External Secrets Operator (reads from AWS Secrets Manager/Vault), or environment-specific secrets injected at deploy time. The GitOps repo contains references to secrets, not the secrets themselves.

**Q: How do you manage different environments (dev/staging/prod) in GitOps?**
A: Use directory-based (separate dir per environment), branch-based (separate branch per environment), or Kustomize overlays. Directory-based is most common — main branch syncs to staging, tags sync to prod. Different ArgoCD applications point to different directories.

**Q: What tools implement GitOps for Kubernetes?**
A: ArgoCD and Flux are the two main tools. ArgoCD provides a UI and declarative Application CRDs. Flux is more lightweight and GitOps-native. For non-Kubernetes GitOps (Airflow DAGs, dbt), GitHub Actions workflows that sync on merge are the common pattern.

---

## 💼 Interview Tips

- Connect GitOps to audit trail requirements — in regulated industries, proving who deployed what and when is often a compliance requirement, and git history provides this automatically.
- Mention ArgoCD or Flux by name when discussing K8s GitOps, and GitHub Actions workflows when discussing Airflow/dbt GitOps — tool-specific knowledge distinguishes you.
- Frame GitOps as solving the 'configuration drift' and 'who deployed this?' problems — these are pain points every experienced DE has felt.
- For senior roles, discuss self-service GitOps: domain teams deploy via PRs to their directory, platform team reviews shared infra only — it scales without bottlenecking on a central ops team.
- Avoid describing GitOps as just 'using git for deployment' — emphasize the automated reconciliation aspect (ArgoCD continuously syncing) and the drift prevention it provides.
- Connect GitOps to rollback: `git revert` is the rollback mechanism, and it's safer than running ad-hoc commands because it goes through the same review and test process.
