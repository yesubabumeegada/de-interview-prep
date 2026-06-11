---
title: "Dockerfile Best Practices — Scenarios"
topic: docker-and-kubernetes
subtopic: dockerfile-best-practices
content_type: scenario_question
tags: [docker, dockerfile, interview, scenarios, optimization]
---

# Dockerfile Best Practices — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Fix a Poorly Written Dockerfile

**Scenario:** A colleague wrote this Dockerfile. It works, but it's 2.8 GB and rebuilds pip dependencies every time any code changes. Identify the problems and fix them.

```dockerfile
FROM python:3.11
COPY . /app
RUN pip install -r /app/requirements.txt
WORKDIR /app
CMD python pipeline.py
```

<details>
<summary>💡 Hint</summary>

There are 3 main problems: (1) Using full `python:3.11` instead of `python:3.11-slim`, (2) Copying all code before pip install — so any code change invalidates the pip cache layer, (3) No pip cache cleanup (`--no-cache-dir`). Also: no non-root user, no WORKDIR before COPY. Fix all of these.

</details>

<details>
<summary>✅ Solution</summary>

```dockerfile
# ✅ Fixed Dockerfile
FROM python:3.11-slim

# Non-root user for security
RUN useradd -m -u 1000 appuser

WORKDIR /home/appuser/app

# Install system deps first (rarely change)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements FIRST for layer caching
COPY --chown=appuser:appuser requirements.txt .
# --no-cache-dir: don't bake pip cache into layer
RUN pip install --no-cache-dir -r requirements.txt

# Copy code last (changes most often)
COPY --chown=appuser:appuser . .

USER appuser

CMD ["python", "pipeline.py"]
```

**Issues fixed:**
1. `python:3.11-slim` instead of `python:3.11` → ~800 MB savings
2. `requirements.txt` copied before code → pip cached unless requirements change
3. `--no-cache-dir` → pip cache not stored in layer
4. Non-root user → security baseline
5. `--no-install-recommends` + apt-get cleanup → smaller apt layer

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Credentials Appear in Docker Image

**Scenario:** A security scan flagged that your Docker image contains database credentials. The Dockerfile has `RUN pip install -r requirements.txt --extra-index-url https://user:password@private.pypi.com/simple/`. The credentials are baked into the image layer. How do you fix this?

<details>
<summary>💡 Hint</summary>

Docker layers are immutable — even if you overwrite the credential in a later layer, it's still readable in the earlier layer via `docker history` or by pulling the layer directly. The fix is to never write the credential to any layer. Use BuildKit's `--mount=type=secret` to mount the credential at build time without it appearing in any layer. The credential is only available during that specific `RUN` instruction and never written to the image.

</details>

<details>
<summary>✅ Solution</summary>

```dockerfile
# ❌ Wrong — credential in image layer (visible in docker history)
RUN pip install -r requirements.txt \
    --extra-index-url https://user:password@private.pypi.com/simple/

# ✅ Fixed — use BuildKit secret mount
# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

RUN --mount=type=secret,id=pip_conf,target=/root/.pip/pip.conf \
    pip install --no-cache-dir -r requirements.txt
# pip.conf contains: [global]\nextra-index-url = https://user:pass@private.pypi.com/simple/
# The credential is NEVER written to any layer
```

```bash
# Build with secret
DOCKER_BUILDKIT=1 docker build \
  --secret id=pip_conf,src=pip.conf \
  -t my-pipeline:v1 .

# Verify credential is NOT in image
docker history my-pipeline:v1 | grep -i password
# → (empty — no credentials in any layer)

docker run --rm my-pipeline:v1 cat /root/.pip/pip.conf
# → cat: /root/.pip/pip.conf: No such file or directory
# (secret is not in the final image)
```

**In CI (GitHub Actions):**
```yaml
- name: Build image (with secret)
  run: |
    echo "[global]
    extra-index-url = https://${{ secrets.PYPI_USER }}:${{ secrets.PYPI_PASS }}@private.pypi.com/simple/" > pip.conf
    
    DOCKER_BUILDKIT=1 docker build \
      --secret id=pip_conf,src=pip.conf \
      -t my-pipeline:v1 .
    
    rm pip.conf  # clean up local file
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Define Base Image Strategy for a DE Platform

**Scenario:** Your DE platform team supports 20 teams each building Docker images for their pipelines. Currently every team uses a different base image, different Python versions, and different dependency sets. Security scans find 50+ CVEs distributed across images. Design a base image strategy.

<details>
<summary>💡 Hint</summary>

Create a curated base image hierarchy: a company-maintained base image that all pipelines inherit from. The platform team owns the base image (security patches, Python version, common system deps). Individual pipeline images only add what they uniquely need. This centralizes security patching — fix the base image, rebuild all children automatically via CI. Enforce the hierarchy with a policy check (PR check that rejects images not inheriting from the approved base).

</details>

<details>
<summary>✅ Solution</summary>

**Image hierarchy:**
```
python:3.11.4-slim-bookworm (public — pinned digest)
  └── company/de-base:2024.02 (platform team owns)
       ├── company/de-python:2024.02 (Python DE common deps)
       │    └── team-revenue/pipeline:abc1234 (revenue team)
       │    └── team-marketing/pipeline:def5678 (marketing team)
       └── company/de-spark:2024.02 (Spark + Java)
            └── team-data-science/features:ghi9012
```

**Base image (platform team maintains):**
```dockerfile
# de-base/Dockerfile
FROM python:3.11.4-slim-bookworm@sha256:<pinned>

# Common system deps (curl, libpq for postgres)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev=15.* curl=7.88.* \
    && rm -rf /var/lib/apt/lists/*

# Non-root user standard
RUN useradd -m -u 1000 deuser
USER deuser
WORKDIR /home/deuser/app

LABEL org.opencontainers.image.vendor="company"
LABEL org.opencontainers.image.base="de-base"
```

**Enforcement: CI policy check for all pipeline PRs:**
```python
# Check that Dockerfile inherits from approved base
import subprocess, sys

result = subprocess.run(
    ["grep", "-E", "^FROM", "Dockerfile"],
    capture_output=True, text=True
)
first_from = result.stdout.strip().split("\n")[0]
APPROVED_BASES = ["company/de-base:", "company/de-python:", "company/de-spark:"]

if not any(base in first_from for base in APPROVED_BASES):
    print(f"❌ Dockerfile must inherit from an approved base image.")
    print(f"   Found: {first_from}")
    print(f"   Approved: {APPROVED_BASES}")
    sys.exit(1)
```

**Security patch process:**
```bash
# Platform team patches base image monthly (or on critical CVE)
docker build -t company/de-base:2024.03 .
docker push company/de-base:2024.03

# Renovate bot opens PRs on all child images to bump base version
# Teams merge PRs → their images are patched
# Total patching time: 1 day vs 2 weeks of individual patching
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Docker layer caching and how do you optimize for it?**
A: Each Dockerfile instruction creates a layer. If a layer's inputs haven't changed, Docker reuses the cached layer. Optimize by ordering instructions from least to most frequently changing: system deps → Python deps (requirements.txt COPY + pip install) → application code.

**Q: Why use `--no-cache-dir` with pip in a Dockerfile?**
A: pip caches downloaded wheels by default. In a Dockerfile, this cache is baked into the image layer but never used again (next build starts fresh). `--no-cache-dir` prevents this wasted space — can save 100-500 MB in heavy dependency images.

**Q: What does `COPY --chown=appuser:appuser` do?**
A: It copies files and immediately sets their ownership to the specified user:group. Without it, files copied by root are owned by root, which can cause permission errors when the container runs as a non-root user.

**Q: Why is using `:latest` in a Dockerfile dangerous?**
A: `:latest` is a mutable tag — `python:latest` today may be Python 3.13, but next year it's 3.14 with breaking changes. It makes builds non-reproducible and can introduce unexpected breaking changes silently. Always pin to a specific version like `python:3.11.4-slim`.

**Q: How do multi-stage builds improve security?**
A: Build tools (gcc, make, pip build cache, dev dependencies) needed to compile native extensions are only in the build stage — they don't appear in the final runtime image. This dramatically reduces the attack surface (no shell injection via build tools, fewer CVEs).

**Q: What should always be in a .dockerignore for DE projects?**
A: At minimum: `.git/`, `data/` (large datasets), `.env` (secrets), `__pycache__/`, `.venv/`, `tests/` (don't ship test code), `*.csv` / `*.parquet` (large files), and `notebooks/`.

**Q: How do you verify that credentials are not baked into a Docker image?**
A: Run `docker history image:tag` to inspect each layer's command. Run `docker run --rm image:tag env` to see all environment variables. Use `docker inspect image:tag` to check labels and config. For thorough scanning, use `trivy` or `docker scout` which scan for embedded secrets.

---

## 💼 Interview Tips

- Lead with layer caching order as the most practical and highest-ROI optimization — it's immediately actionable and shows you understand how Docker actually works.
- Non-root user and `--no-cache-dir` should be mentioned in any Dockerfile you describe — they signal awareness of basic production and security standards.
- For credential questions, always describe BuildKit secret mounts as the correct solution, not just "use environment variables" — environment variables appear in `docker inspect`.
- Multi-stage builds are the answer to "how do you keep images small" — practice explaining the builder/runtime split in a DE context (Spark needing JDK to compile, not to run).
- For platform/senior questions, the base image hierarchy and automated patching story shows systems thinking — you're not just writing one Dockerfile, you're designing for 20 teams.
- Avoid describing `.dockerignore` as optional — in real projects, missing it causes builds to include gigabytes of data files and the `.git/` directory. Treat it as mandatory.
