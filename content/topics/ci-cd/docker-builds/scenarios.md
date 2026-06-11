---
title: "Docker Builds — Scenarios"
topic: ci-cd
subtopic: docker-builds
content_type: scenario_question
tags: [ci-cd, docker, builds, interview, scenarios]
---

# Docker Builds — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Add Docker Build to CI

**Scenario:** Your Python pipeline script is working locally. You need to add Docker building to your GitHub Actions CI so that every PR builds and verifies the image. How do you do it?

<details>
<summary>💡 Hint</summary>

Add a job that uses `docker/build-push-action` with `push: false` on PRs (build to verify, don't push). Use `docker/setup-buildx-action` for efficient builds with caching. Tag the image with the git SHA.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
name: CI
on:
  pull_request:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: docker/setup-buildx-action@v3
      
      - name: Build (verify, don't push on PR)
        uses: docker/build-push-action@v6
        with:
          context: .
          push: false
          tags: my-pipeline:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
      
      - name: Basic smoke test
        run: docker run --rm my-pipeline:${{ github.sha }} python -c "import pipeline; print('OK')"
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Build Is Slow — 12 Minutes

**Scenario:** Your Docker build in CI takes 12 minutes because pip install runs from scratch every time. How do you optimize it to under 3 minutes?

<details>
<summary>💡 Hint</summary>

Two fixes: (1) Ensure `requirements.txt` is copied before application code in the Dockerfile so pip install is cached unless requirements change. (2) Use GitHub Actions cache or registry cache with `docker/build-push-action`. Also check that `--no-cache` is not accidentally set in the build command.

</details>

<details>
<summary>✅ Solution</summary>

**Fix 1: Dockerfile layer order**
```dockerfile
# WRONG — code change invalidates pip layer
COPY . .
RUN pip install -r requirements.txt

# RIGHT — pip cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
```

**Fix 2: Enable build cache in GitHub Actions**
```yaml
- uses: docker/setup-buildx-action@v3

- uses: docker/build-push-action@v6
  with:
    context: .
    push: false
    tags: my-pipeline:${{ github.sha }}
    cache-from: type=gha              # restore from GHA cache
    cache-to: type=gha,mode=max       # save all layers
```

**Result:** Requirements unchanged PR: 12 min → 45 seconds (cache hit). Requirements changed PR: 12 min → 3-4 min (cache miss for pip only).

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Secure Build Pipeline for Regulated Environment

**Scenario:** Your company handles financial data under SOC 2. Security requires: every image must be scanned before push, no critical CVEs allowed in production, all images signed, and audit trail for every deployed image. Design the CI build pipeline.

<details>
<summary>💡 Hint</summary>

Use OIDC for registry auth (no secrets), Trivy for CVE scanning with exit code 1 on CRITICAL, Cosign for signing via OIDC (keyless), and tag by git SHA for traceability. Store scan results as artifacts. The audit trail is the combination of: git history (what code), image SHA (what binary), Cosign transparency log (signed when/by whom), and K8s deployment events (deployed when/where).

</details>

<details>
<summary>✅ Solution</summary>

```yaml
name: Secure Build Pipeline
on:
  push:
    branches: [main]

jobs:
  build-scan-sign:
    runs-on: ubuntu-latest
    permissions:
      id-token: write     # OIDC for registry + cosign
      contents: read
      security-events: write  # upload sarif results

    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3

      # OIDC auth — no stored secrets
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123:role/GitHubActions
          aws-region: us-east-1

      - name: Login to ECR
        id: ecr
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build
        uses: docker/build-push-action@v6
        id: build
        with:
          context: .
          push: true
          provenance: true
          sbom: true
          tags: ${{ steps.ecr.outputs.registry }}/pipeline:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

      # Scan — block on CRITICAL
      - name: Scan for CVEs
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: ${{ steps.ecr.outputs.registry }}/pipeline:${{ github.sha }}
          format: sarif
          output: trivy.sarif
          exit-code: '1'
          severity: CRITICAL,HIGH

      - name: Upload scan results
        uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: trivy.sarif

      # Sign with keyless Cosign — OIDC proves it came from this workflow
      - name: Install Cosign
        uses: sigstore/cosign-installer@v3

      - name: Sign image
        run: |
          cosign sign --yes \
            ${{ steps.ecr.outputs.registry }}/pipeline:${{ github.sha }}@${{ steps.build.outputs.digest }}

      # Audit artifact — who built what when
      - name: Record build audit
        run: |
          cat > build-audit.json << AUDIT
          {
            "image": "${{ steps.ecr.outputs.registry }}/pipeline:${{ github.sha }}",
            "digest": "${{ steps.build.outputs.digest }}",
            "git_sha": "${{ github.sha }}",
            "git_ref": "${{ github.ref }}",
            "actor": "${{ github.actor }}",
            "workflow": "${{ github.workflow }}",
            "run_id": "${{ github.run_id }}",
            "timestamp": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
          }
          AUDIT
          aws s3 cp build-audit.json s3://audit-bucket/builds/${{ github.sha }}.json
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: Why should you never rebuild a Docker image when promoting from staging to production?**
A: Rebuilding means the production image is different from the staging-tested image — a new `FROM` pull or package update can silently change behavior. The tested binary should be promoted as-is: pull from dev registry, retag, push to prod registry.

**Q: What is image provenance and why does it matter for compliance?**
A: Provenance is cryptographic proof of how and where an image was built — which workflow, which repo, which commit, which runner. It lets you prove to auditors that production images were built by your CI pipeline from reviewed code, not built locally by an engineer.

**Q: What tagging strategy should production images use?**
A: Tag by git SHA (full or short). Never use `:latest` in production — it's mutable and ambiguous. A SHA tag is immutable and traceable: you can always find exactly which code and which CI run produced it.

**Q: What does `cosign sign` do and why is it used?**
A: Cosign creates a cryptographic signature for a Docker image and stores it in the OCI registry alongside the image. When deploying, you verify the signature to confirm the image was built and signed by your CI pipeline — protecting against image tampering or substitution.

**Q: What is the difference between `cache-from: type=gha` and `type=registry`?**
A: `type=gha` stores cache in GitHub Actions cache storage (free, tied to the repo, expires after 7 days). `type=registry` stores cache layers in your container registry (costs registry storage, available indefinitely, accessible across runners). GHA cache is simpler; registry cache is better for self-hosted runners.

**Q: How do you scan Docker images for vulnerabilities in CI?**
A: Use Trivy (`aquasecurity/trivy-action`), Snyk, or Docker Scout. Run the scan against the built image with `--exit-code 1` and `--severity CRITICAL` to fail CI when critical vulnerabilities are found. Upload results as SARIF to GitHub Security tab.

---

## 💼 Interview Tips

- The "build once, promote" principle is the single most important concept in Docker CI — state it clearly and explain why rebuilding is dangerous.
- Image tagging by git SHA is the production standard — know how to generate it (`${{ github.sha }}` in Actions) and why `:latest` is insufficient.
- For security-focused interviews, mention the full chain: OIDC auth → scan → sign → SBOM — it shows you think about the supply chain holistically.
- Distinguish between image scanning (finding CVEs in the image) and code scanning (SAST in source code) — they're complementary, not alternatives.
- Mention BuildKit and build caching as the performance story — show that you've felt the pain of slow CI builds and know the practical fix.
- For regulated environments, the audit trail (who built what when, signed by what pipeline) is often more important to the interviewer than the technical build steps.
