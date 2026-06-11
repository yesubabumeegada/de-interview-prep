---
title: "Helm and Operators - Scenarios"
topic: docker-and-kubernetes
subtopic: helm-and-operators
content_type: scenario_question


tags: [docker, kubernetes, helm-and-operators]
---

# Helm and Operators — Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Install Airflow with Helm

**Scenario:** Your team wants to run Airflow on Kubernetes. Walk through installing it with Helm.

<details>
<summary>💡 Hint</summary>

Add the Airflow Helm repo, create a namespace, write a values.yaml with your configuration, then `helm install`. Key settings: executor, database, DAG source.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# 1. Add repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# 2. Create namespace
kubectl create namespace airflow

# 3. Write values.yaml (minimal)
cat > airflow-values.yaml << 'YAML'
executor: KubernetesExecutor
dags:
  gitSync:
    enabled: true
    repo: https://github.com/org/dags
    branch: main
YAML

# 4. Install
helm install airflow apache-airflow/airflow   -n airflow   -f airflow-values.yaml

# 5. Verify
kubectl get pods -n airflow
helm status airflow -n airflow
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Upgrade Airflow Without Downtime

**Scenario:** Airflow is running in production on 2.7.3. You need to upgrade to 2.8.0. How do you do it safely with Helm?

<details>
<summary>💡 Hint</summary>

Use `helm upgrade --atomic` which rolls back automatically if the upgrade fails. Test in staging first with the same values.yaml. Review the Airflow 2.8.0 release notes for breaking changes. Upgrade scheduler first (stateless), then webserver.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# 1. Review release notes for breaking changes
# https://airflow.apache.org/docs/2.8.0/release_notes.html

# 2. Test in staging first
helm upgrade airflow apache-airflow/airflow   -n airflow-staging   -f values.yaml   --set airflowVersion=2.8.0   --set defaultAirflowTag=2.8.0   --dry-run    # see what would change

helm upgrade airflow apache-airflow/airflow   -n airflow-staging   -f values.yaml   --set airflowVersion=2.8.0

# 3. Verify staging works
kubectl get pods -n airflow-staging
kubectl exec -it <airflow-pod> -n airflow-staging -- airflow version

# 4. Upgrade production
helm upgrade airflow apache-airflow/airflow   -n airflow   -f values.yaml   --set airflowVersion=2.8.0   --atomic \        # auto-rollback if pods don't become healthy
  --timeout 10m     # wait up to 10 minutes

# 5. Rollback if needed
helm rollback airflow -n airflow
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Helm-Based Platform for 20 DE Teams

**Scenario:** Design a Helm-based deployment system where 20 data teams can deploy their own pipelines to Kubernetes without needing K8s expertise, while the platform team maintains standards.

<details>
<summary>💡 Hint</summary>

Create a company-standard Helm chart that all teams use (as a library chart or base chart). Teams provide only their values.yaml (image, schedule, resources). Platform team maintains the chart templates (security, resource limits, monitoring). Helmfile or ArgoCD manages deploying all releases. Teams never write K8s YAML directly.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Company chart: de-batch-pipeline (maintained by platform team)
# Templates enforce: non-root, resource limits, monitoring labels, network policies
# Teams only provide values:

# finance-revenue/values.yaml (finance team writes this)
pipeline:
  name: revenue-daily
  schedule: "0 6 * * *"
  image: registry/finance-revenue:abc1234
  resources:
    requests:
      memory: 2Gi
      cpu: "500m"
  env:
    - name: ENVIRONMENT
      value: production
```

```bash
# Platform team's helmfile.yaml manages all team deployments
releases:
  - name: finance-revenue
    namespace: finance-de
    chart: ./de-batch-pipeline   # company standard chart
    values:
      - teams/finance/revenue-values.yaml

  - name: marketing-attribution
    namespace: marketing-de
    chart: ./de-batch-pipeline
    values:
      - teams/marketing/attribution-values.yaml
```

Teams open PRs to update their values.yaml. Platform reviews chart template changes. ArgoCD syncs Helmfile state to cluster.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Helm and what problem does it solve?**
A: Helm is a Kubernetes package manager. It bundles related K8s resources into "charts" with configurable values, enabling complex applications (Airflow, Kafka) to be installed with one command and upgraded/rolled back consistently.

**Q: What is the difference between `helm install` and `helm upgrade`?**
A: `helm install` creates a new release. `helm upgrade` updates an existing release with new values or chart version. Add `--install` to upgrade or install in one command: `helm upgrade --install`.

**Q: What does `--atomic` do in a Helm upgrade?**
A: With `--atomic`, if pods don't become healthy within the timeout, Helm automatically rolls back to the previous release. Prevents partial upgrades from leaving the cluster in a broken state.

**Q: What is a Kubernetes Operator and how does it differ from Helm?**
A: An Operator is a controller that encodes operational knowledge about a specific application (Kafka, Spark) as code running in your cluster. It watches custom resources and manages the application's full lifecycle. Helm installs resources; an Operator actively manages them (handles failures, upgrades, backups).

**Q: What is Strimzi and when would a DE team use it?**
A: Strimzi is the Kafka Operator for Kubernetes. Use it when running Kafka on K8s — it handles Kafka broker lifecycle, rolling upgrades, TLS certificate management, and topic management via K8s custom resources.

**Q: What is values.yaml and how does Helm templating work?**
A: values.yaml contains configuration defaults for a Helm chart. Templates (in `templates/`) reference values with `{{ .Values.myKey }}` syntax. When installing, values from values.yaml are substituted into templates to produce final K8s manifests. Override defaults with `-f custom-values.yaml` or `--set key=value`.

---

## 💼 Interview Tips

- Connect Helm to operational repeatability — the ability to install/upgrade/rollback complex applications (Airflow, Kafka) consistently is the core value.
- Know `helm upgrade --atomic` as the production-safe upgrade command — it shows awareness of rollback safety.
- Distinguish between Helm (install/upgrade) and Operators (continuous management) — interviewers test whether you understand the difference in operational model.
- Mention Strimzi for Kafka on K8s specifically — it's the standard and shows you know the ecosystem beyond basic Helm.
- For platform/senior questions, the company-standard chart pattern (platform team owns templates, teams own values) is the scalable answer.
- Avoid describing Helm as "just YAML templating" — it's a deployment lifecycle management tool (install, upgrade, rollback, history) that happens to use templates.
