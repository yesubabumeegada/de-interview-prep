---
title: "Helm and Operators - Intermediate"
topic: docker-and-kubernetes
subtopic: helm-and-operators
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker, kubernetes, helm-and-operators]
---

# Helm and Operators — Intermediate

## Creating a Helm Chart for a DE Pipeline

```bash
# Generate chart scaffold
helm create de-pipeline

# Structure:
# de-pipeline/
# ├── Chart.yaml
# ├── values.yaml
# ├── templates/
# │   ├── deployment.yaml
# │   ├── service.yaml
# │   ├── configmap.yaml
# │   └── _helpers.tpl
```

```yaml
# templates/cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ .Release.Name }}-{{ .Values.pipeline.name }}
  labels:
    {{- include "de-pipeline.labels" . | nindent 4 }}
spec:
  schedule: {{ .Values.pipeline.schedule | quote }}
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: pipeline
              image: {{ .Values.image.repository }}:{{ .Values.image.tag }}
              resources:
                requests:
                  memory: {{ .Values.resources.requests.memory }}
                  cpu: {{ .Values.resources.requests.cpu }}
              env:
                - name: ENVIRONMENT
                  value: {{ .Values.environment }}
```

```yaml
# values.yaml
pipeline:
  name: revenue-daily
  schedule: "0 6 * * *"

image:
  repository: registry/revenue-pipeline
  tag: latest

resources:
  requests:
    memory: 2Gi
    cpu: "500m"

environment: production
```

## Helm Chart Testing

```bash
# Dry-run: see what would be deployed
helm install --dry-run --debug my-pipeline ./de-pipeline -f prod-values.yaml

# Lint chart
helm lint ./de-pipeline

# Template: render and inspect
helm template my-pipeline ./de-pipeline -f prod-values.yaml | head -50

# Test after install
helm test my-pipeline  # runs helm test pods
```
