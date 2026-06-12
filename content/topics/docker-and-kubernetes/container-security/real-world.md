---
title: "Container Security - Real World"
topic: docker-and-kubernetes
subtopic: container-security
content_type: study_material
difficulty_level: senior
layer: real-world


tags: [docker, kubernetes, container-security]
---

# Container Security — Real World

## Case Study: Security Scan Prevented a Supply Chain Attack

### Background

A data engineering team used an open-source Python package `data-utils-helper` in their pipeline image. The package had 50k weekly downloads and appeared legitimate.

### The Incident (Prevented)

A security researcher discovered the package was typosquatting `data-utils-helpers` (with an 's'). The malicious version contained code that exfiltrated environment variables (including credentials) to an external server.

The team's Trivy scan in CI flagged:
```
CRITICAL: data-utils-helper 1.2.3 — malicious package (supply chain attack)
Source: GitHub Advisory Database GHSA-xxxx-xxxx-xxxx
```

The CI pipeline blocked the PR. The package was immediately removed.

**Without the scan:** The malicious code would have deployed to production, exfiltrating AWS credentials from every pipeline run.

### The Security Stack They Had

```yaml
# GitHub Actions: scan on every PR
- uses: aquasecurity/trivy-action@master
  with:
    image-ref: ${{ env.IMAGE_NAME }}:${{ github.sha }}
    exit-code: '1'
    severity: 'CRITICAL,HIGH'
    format: 'sarif'
    output: 'trivy-results.sarif'

- uses: github/codeql-action/upload-sarif@v3
  if: always()
  with:
    sarif_file: trivy-results.sarif
```

**Cost of implementing:** 30 minutes. **Cost of not implementing:** credential breach + incident response + customer notification.
