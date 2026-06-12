---
title: "AI - Model Serving"
topic: ai
subtopic: model-serving
content_type: scenario_question
difficulty_level: junior
tags: [ai, model-serving, scenarios, latency, traffic-spikes, rollback]
---

# Model Serving — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: High Latency Debugging

Your recommendation model API has a p99 latency of 850ms, far above the 200ms SLA. The service is a FastAPI app running a PyTorch model with a single worker. Here's the profiling output:

```
Request breakdown:
  Deserialize JSON:         5ms
  Load model from disk:   600ms  ← !!
  Preprocess features:     15ms
  Model inference:         40ms
  Serialize response:      10ms
  Total:                  670ms
```

You also notice that the model file is 450MB and stored on an NFS mount. What's wrong and what are your fixes in order of priority?

<details>
<summary>💡 Hint</summary>

The "Load model from disk" time of 600ms is the smoking gun. When should a model be loaded in a serving application? Also consider: NFS adds latency to every disk read. What would happen if you had multiple workers — would this be 600ms * N?

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause: Model Loaded Per Request

The model is being loaded from disk on every request instead of being loaded once at startup.

```python
# WRONG: loading per request
@app.post("/predict")
async def predict(request: dict):
    model = torch.load("/nfs/models/recommendation.pt")  # 600ms EVERY TIME
    output = model(features)
    return output
```

### Fix 1 (Immediate): Load Model at Startup

```python
from contextlib import asynccontextmanager
import torch
import time

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load once at startup — not per request
    print("Loading model...")
    start = time.monotonic()
    app.state.model = torch.load("/nfs/models/recommendation.pt")
    app.state.model.eval()
    load_time = (time.monotonic() - start) * 1000
    print(f"Model loaded in {load_time:.0f}ms")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/predict")
async def predict(request: dict):
    # Uses pre-loaded model — 0ms model load time
    model = app.state.model
    output = model(features)
    return output
```

After this fix: p99 latency drops from 850ms to ~70ms (5+15+40+10ms).

### Fix 2 (Durable): Move Model from NFS to Local Storage

NFS adds ~1-5ms per read vs local SSD. For a 450MB model, this is significant.

```dockerfile
# Dockerfile: Copy model into container image
FROM python:3.11-slim

# Copy model into container (loaded from local disk, not NFS)
COPY models/recommendation.pt /app/models/recommendation.pt
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

Or alternatively, store in container's ephemeral storage and download from S3 at startup:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Download from S3 to /tmp (local disk) if not cached
    local_path = "/tmp/recommendation.pt"
    if not os.path.exists(local_path):
        import boto3
        s3 = boto3.client("s3")
        s3.download_file("models-bucket", "recommendation.pt", local_path)
    
    app.state.model = torch.load(local_path)
    app.state.model.eval()
    yield
```

### Fix 3 (Performance): Convert to TorchScript for Faster Inference

```python
import torch

# TorchScript: eliminates Python overhead for inference
model = torch.load("/tmp/recommendation.pt")
model.eval()

# Script the model
scripted_model = torch.jit.script(model)
scripted_model.save("/tmp/recommendation_scripted.pt")

# Load scripted model — 30-40% faster inference
app.state.model = torch.jit.load("/tmp/recommendation_scripted.pt")
```

### Fix 4 (Scale): Multiple Workers

```bash
# Run with multiple workers to handle concurrent requests
uvicorn main:app --host 0.0.0.0 --port 8080 --workers 4
# Each worker loads the model once at startup — 4 models in memory
# Memory: 450MB * 4 = 1.8GB — ensure sufficient RAM
```

### After All Fixes: Expected Performance

| Fix | Expected p99 |
|-----|-------------|
| Baseline | 850ms |
| Fix 1: Load at startup | 70ms |
| Fix 2: Local disk | 65ms |
| Fix 3: TorchScript | 45ms |
| Fix 4: 4 workers | 45ms (4x throughput) |

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Handling Traffic Spikes

Your fraud detection API normally handles 500 RPS. During a Black Friday sale, traffic spikes to 8,000 RPS within minutes. The service crashes — pods are OOMKilled and new pods can't start fast enough because each pod takes 90 seconds to cold-start (model loading from S3).

Design a strategy to handle 16x traffic spikes without service degradation.

<details>
<summary>💡 Hint</summary>

Think about three time horizons: (1) immediate — what can you do right now to survive the spike? (2) medium-term — how do you scale faster? (3) long-term — how do you architect for predictable high traffic? Also consider: does every request need the full model, or can you shed load gracefully?

</details>

<details>
<summary>✅ Solution</summary>

### Immediate Mitigation: Load Shedding

```python
from fastapi import FastAPI, HTTPException
import asyncio
from asyncio import Semaphore
import time
from collections import deque

class LoadShedder:
    """
    Rejects excess requests rather than queueing them indefinitely.
    A fast "no" is better than a slow "yes" during overload.
    """
    
    def __init__(self, max_concurrent: int = 100, max_queue_depth: int = 200):
        self.semaphore = Semaphore(max_concurrent)
        self.current_queue = 0
        self.max_queue = max_queue_depth
        self.recent_latencies = deque(maxlen=100)
    
    async def __aenter__(self):
        if self.current_queue >= self.max_queue:
            raise HTTPException(
                status_code=503,
                headers={"Retry-After": "5"},
                detail={
                    "error": "Service temporarily overloaded",
                    "retry_after_seconds": 5,
                }
            )
        
        self.current_queue += 1
        await self.semaphore.acquire()
        return self
    
    async def __aexit__(self, *args):
        self.current_queue -= 1
        self.semaphore.release()

load_shedder = LoadShedder(max_concurrent=100, max_queue_depth=200)

@app.post("/score")
async def score_transaction(transaction: dict):
    async with load_shedder:
        return await fraud_service.score(transaction)
```

### Fast Scaling: Pre-Warmed Instance Pool

```yaml
# Kubernetes HPA with pre-warming
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: fraud-api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: fraud-api
  minReplicas: 10       # Keep 10 pods warm (vs 3 normally)
  maxReplicas: 100
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60    # Scale earlier (vs 80%)
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0   # Scale up immediately
      policies:
        - type: Pods
          value: 20                   # Add 20 pods at a time
          periodSeconds: 30
    scaleDown:
      stabilizationWindowSeconds: 300  # Scale down slowly
```

### Fix Cold-Start: Model in Container Image

```dockerfile
# Pack model into the container image
# Cold-start goes from 90s → 15s (just app startup, no S3 download)
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Model baked into image (~200MB compressed)
COPY models/fraud_model.json /app/models/fraud_model.json

COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
```

Or use Kubernetes init containers to pre-load the model:

```yaml
initContainers:
  - name: model-downloader
    image: amazon/aws-cli
    command: ["aws", "s3", "cp", "s3://models/fraud_model.json", "/models/fraud_model.json"]
    volumeMounts:
      - name: model-volume
        mountPath: /models
containers:
  - name: fraud-api
    image: fraud-api:latest
    env:
      - name: MODEL_PATH
        value: /models/fraud_model.json
    volumeMounts:
      - name: model-volume
        mountPath: /models
volumes:
  - name: model-volume
    emptyDir: {}
```

### Architecture for Predictable High Traffic

```python
# Tiered response: fast path for simple cases
class TieredFraudScorer:
    """
    Tier 1 (< 0.5ms): Rule-based fast path — block obvious fraud
    Tier 2 (< 5ms): Lightweight model — fast approximate scoring
    Tier 3 (< 50ms): Full XGBoost — only for uncertain cases
    """
    
    def __init__(self, fast_model, full_model):
        self.fast_model = fast_model    # Logistic regression, < 0.1ms
        self.full_model = full_model    # XGBoost, ~2ms
    
    async def score(self, tx: dict) -> dict:
        # Tier 1: Rule-based (no model)
        if tx["amount"] > 50_000 and tx.get("is_new_device"):
            return {"score": 0.99, "tier": "rules", "action": "block"}
        
        if tx.get("ip_country") in BLOCKED_COUNTRIES:
            return {"score": 0.95, "tier": "rules", "action": "block"}
        
        # Tier 2: Fast model
        fast_score = self.fast_model.predict_proba([[...]])[0][1]
        
        if fast_score < 0.1:  # Clearly legitimate
            return {"score": fast_score, "tier": "fast", "action": "approve"}
        
        if fast_score > 0.85:  # Clearly fraud
            return {"score": fast_score, "tier": "fast", "action": "block"}
        
        # Tier 3: Full model for uncertain cases (10-30% of traffic)
        full_score = await self._run_full_model(tx)
        return {"score": full_score, "tier": "full", "action": self._decide(full_score)}
```

### Capacity Planning

```python
def estimate_capacity(
    current_rps: float,
    peak_multiplier: float,
    model_inference_ms: float,
    concurrency_per_pod: int,
    pod_startup_seconds: float,
):
    peak_rps = current_rps * peak_multiplier
    requests_per_pod_per_second = 1000 / model_inference_ms * concurrency_per_pod
    
    pods_needed = peak_rps / requests_per_pod_per_second
    time_to_scale = pod_startup_seconds * (pods_needed / 20)  # Add 20 pods at a time
    
    print(f"Peak RPS: {peak_rps:.0f}")
    print(f"Pods needed: {pods_needed:.0f}")
    print(f"Time to scale (without pre-warming): {time_to_scale:.0f}s")
    
    # With pre-warming: start with 50% of peak capacity
    min_warm_pods = pods_needed * 0.5
    scale_up_needed = pods_needed - min_warm_pods
    time_with_prewarming = pod_startup_seconds * (scale_up_needed / 20)
    print(f"Time to scale (with {min_warm_pods:.0f} warm pods): {time_with_prewarming:.0f}s")

estimate_capacity(
    current_rps=500,
    peak_multiplier=16,
    model_inference_ms=5,
    concurrency_per_pod=50,
    pod_startup_seconds=15,  # After container image bake
)
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Emergency Model Rollback

At 2am, your on-call engineer gets paged: the fraud model was updated 3 hours ago and chargeback rates have increased 40% (from 0.5% to 0.7%). The model was promoted automatically after passing A/B test metrics (AUC improved from 0.87 to 0.89). Revenue impact is $50K/hour. Design a rollback strategy and a post-mortem process.

<details>
<summary>💡 Hint</summary>

Think about: (1) how to rollback immediately with minimal downtime, (2) what checks should have caught this before promotion, (3) what monitoring should trigger automatic rollback, and (4) how to investigate root cause — why did AUC improve but chargebacks increase?

</details>

<details>
<summary>✅ Solution</summary>

### Immediate Rollback (Goal: < 5 minutes)

```python
import mlflow
from mlflow.tracking import MlflowClient

class EmergencyRollback:
    """
    Rollback procedure for production model incidents.
    Designed to execute in < 5 minutes including verification.
    """
    
    def __init__(self, model_name: str):
        self.client = MlflowClient()
        self.model_name = model_name
    
    def get_model_versions(self) -> dict:
        """Show all available model versions."""
        versions = self.client.search_model_versions(f"name='{self.model_name}'")
        return {
            v.version: {
                "stage": v.current_stage,
                "run_id": v.run_id,
                "created": v.creation_timestamp,
                "tags": v.tags,
            }
            for v in versions
        }
    
    def rollback_to_version(self, target_version: str):
        """
        Atomic rollback:
        1. Load target model (don't disturb production yet)
        2. Verify target model works
        3. Atomically swap production model
        """
        # Step 1: Verify target version exists and is loadable
        print(f"Loading rollback target version {target_version}...")
        target_model = mlflow.sklearn.load_model(
            f"models:/{self.model_name}/{target_version}"
        )
        
        # Step 2: Smoke test on held-out validation data
        import numpy as np
        test_features = np.random.randn(100, 64)
        test_scores = target_model.predict_proba(test_features)[:, 1]
        assert test_scores.min() >= 0 and test_scores.max() <= 1, "Model output sanity check failed"
        print(f"Smoke test passed: {len(test_scores)} samples scored")
        
        # Step 3: Transition current Production to Archived
        current_production = self.client.get_latest_versions(
            self.model_name, stages=["Production"]
        )
        if current_production:
            current_version = current_production[0].version
            self.client.transition_model_version_stage(
                name=self.model_name,
                version=current_version,
                stage="Archived",
                archive_existing_versions=False,
            )
            print(f"Archived current production version: {current_version}")
        
        # Step 4: Promote target to Production
        self.client.transition_model_version_stage(
            name=self.model_name,
            version=target_version,
            stage="Production",
        )
        print(f"Promoted version {target_version} to Production")
        
        # Step 5: Signal serving layer to reload
        # In practice: update config map, trigger rolling restart, or hot-reload
        self._trigger_hot_reload()
        
        print(f"Rollback complete. Monitoring metrics...")
    
    def _trigger_hot_reload(self):
        """Signal model servers to reload Production model."""
        import redis
        r = redis.Redis.from_url("redis://control-plane:6379")
        r.publish("model_updates", f'{{"model": "{self.model_name}", "action": "reload"}}')


# Execute rollback
rollback = EmergencyRollback("fraud-detector")
print(rollback.get_model_versions())

# Rollback to previous production version
rollback.rollback_to_version(target_version="47")  # Previous good version
```

### Why AUC Improved But Chargebacks Increased

```
Root Cause Analysis Framework:

1. AUC measures ranking ability — does the model rank fraudulent
   transactions higher than legitimate ones? YES (AUC 0.87 → 0.89)

2. Chargeback rate depends on the DECISION at a fixed threshold.
   If the new model scores some fraud at 0.48 (below 0.5 threshold),
   they get approved despite having high fraud probability.

3. The new model may have been trained on:
   - Different time period (seasonal fraud patterns shifted)
   - Different feature set (a feature was inadvertently excluded)
   - Different sampling (undersampling changed the score distribution)
   - Different threshold calibration

Hypothesis: The model's score distribution shifted. The old model
scored true fraud at 0.7-0.9. The new model scores the same transactions
at 0.4-0.6 (below the 0.5 decision threshold), causing them to be approved.
```

```python
def investigate_score_distribution_shift(
    old_model,
    new_model,
    validation_data,
    validation_labels,
):
    """Check if score distributions shifted at the decision threshold."""
    import numpy as np
    import matplotlib.pyplot as plt
    
    old_scores = old_model.predict_proba(validation_data)[:, 1]
    new_scores = new_model.predict_proba(validation_data)[:, 1]
    
    threshold = 0.5
    
    # Compare at threshold
    print("Score distribution analysis:")
    print(f"Old model — fraud scores at threshold=0.5:")
    fraud_mask = validation_labels == 1
    
    old_fraud_approved = (old_scores[fraud_mask] < threshold).mean()
    new_fraud_approved = (new_scores[fraud_mask] < threshold).mean()
    
    print(f"  Old: {old_fraud_approved:.1%} of fraud approved (FNR)")
    print(f"  New: {new_fraud_approved:.1%} of fraud approved (FNR)")
    
    # This explains the chargeback increase!
    if new_fraud_approved > old_fraud_approved * 1.2:
        print("FINDING: New model approves significantly more fraud at threshold=0.5")
        print("RECOMMENDATION: Recalibrate threshold for new model score distribution")
```

### Automatic Rollback System

```python
class AutomaticRollbackGuard:
    """
    Monitors business metrics post-deployment.
    Triggers automatic rollback if metrics degrade.
    """
    
    def __init__(
        self,
        rollback_handler: EmergencyRollback,
        previous_version: str,
        chargeback_rate_threshold_pct: float = 0.6,   # 0.5% baseline + 20% buffer
        false_negative_rate_threshold: float = 0.15,
        monitoring_window_minutes: int = 30,
    ):
        self.rollback = rollback_handler
        self.previous_version = previous_version
        self.thresholds = {
            "chargeback_rate": chargeback_rate_threshold_pct,
            "false_negative_rate": false_negative_rate_threshold,
        }
        self.window_minutes = monitoring_window_minutes
        self.rolled_back = False
    
    async def monitor_and_guard(self):
        """
        Post-deployment monitoring loop.
        Runs for 4 hours after deployment.
        """
        import asyncio
        
        end_time = time.time() + 4 * 3600  # Monitor for 4 hours
        
        while time.time() < end_time and not self.rolled_back:
            metrics = await self._fetch_current_metrics()
            
            violations = []
            for metric, threshold in self.thresholds.items():
                current_value = metrics.get(metric, 0)
                if current_value > threshold:
                    violations.append(f"{metric}={current_value:.3f} > threshold={threshold}")
            
            if violations:
                print(f"ALERT: Metric violations detected: {violations}")
                print("Triggering automatic rollback...")
                
                self.rollback.rollback_to_version(self.previous_version)
                self.rolled_back = True
                
                await self._notify_oncall(violations)
                break
            
            await asyncio.sleep(60)  # Check every minute
    
    async def _fetch_current_metrics(self) -> dict:
        """Fetch real-time business metrics from data warehouse."""
        # In practice: query a metrics store or data warehouse
        return {
            "chargeback_rate": 0.007,  # Simulated
            "false_negative_rate": 0.12,
        }
    
    async def _notify_oncall(self, violations: list):
        """Page on-call engineer via PagerDuty."""
        import httpx
        async with httpx.AsyncClient() as client:
            await client.post(
                "https://events.pagerduty.com/v2/enqueue",
                json={
                    "routing_key": "PAGERDUTY_KEY",
                    "event_action": "trigger",
                    "payload": {
                        "summary": f"Automatic model rollback triggered: {violations}",
                        "severity": "critical",
                        "source": "fraud-model-guard",
                    }
                }
            )
```

### Post-Mortem Process

```markdown
## Incident Post-Mortem: Fraud Model Deployment 2024-01-15

### Timeline
- 23:00: Model v50 promoted to production (automated)
- 23:30: Chargeback rate begins rising
- 02:00: PagerDuty alert fires (2.5h detection gap!)
- 02:05: Manual rollback initiated
- 02:10: Rollback complete, chargeback rate normalizing

### Root Cause
New model's score distribution shifted downward for borderline cases.
Fraud transactions previously scored 0.65-0.80 are now scored 0.35-0.55,
below the 0.5 decision threshold. AUC improved because the relative
ranking was better, but the absolute calibration was wrong.

### Why Wasn't This Caught?
1. A/B test used AUC as the gate — not calibrated probability accuracy
2. Chargeback rate has a 24-48h lag (chargebacks are filed after dispute)
3. No automatic rollback threshold was configured
4. Shadow test ran for only 24h — not enough to sample seasonal patterns

### Action Items
1. Add Expected Calibration Error (ECE) to promotion gate (Week 1)
2. Set automatic rollback threshold: chargeback_rate > 0.6% over 30min (Week 1)
3. Add score distribution comparison to promotion checklist (Week 2)
4. Extend shadow testing to 7 days minimum (Week 2)
5. Page on-call when chargeback rate rises > 15% relative in any 1h window (Week 1)
```

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between batch inference and online inference, and when would you choose each?**
A: Batch inference processes large datasets asynchronously on a schedule — ideal for pre-computed recommendations, risk scores, or reporting where low latency isn't required. Online inference serves predictions in real-time (milliseconds to seconds) — required for user-facing features, fraud detection, and content ranking.

**Q: What is model serving latency and what are the main contributors to it?**
A: Total latency includes: network round-trip time, preprocessing/feature retrieval, model forward pass, and postprocessing. For deep learning models, the model forward pass often dominates. Feature retrieval from a remote store can be a major bottleneck in online serving.

**Q: What is a canary deployment for ML models and what does it protect against?**
A: A canary deployment routes a small percentage of traffic (e.g., 5-10%) to the new model while the rest continues hitting the production model. It protects against bad model deployments by limiting blast radius — if the new model degrades, only a fraction of users are affected before rollback.

**Q: What is model quantization and how does it affect serving performance?**
A: Quantization reduces model weight precision (e.g., float32 → int8), shrinking model size and speeding up inference — often 2-4x with minimal accuracy loss. It's especially impactful on edge devices and CPUs. Tradeoff: small accuracy degradation and potential precision loss for certain numerical outputs.

**Q: What is the difference between TensorFlow Serving, Triton Inference Server, and TorchServe?**
A: TF Serving is optimized for TensorFlow models with gRPC/REST APIs and versioning built in. Triton (NVIDIA) supports multiple frameworks (TF, PyTorch, ONNX) with GPU optimization and concurrent model execution. TorchServe is PyTorch-native with handler customization and multi-model serving.

**Q: What is a model ensemble in serving and what are the latency challenges?**
A: An ensemble combines predictions from multiple models (averaging, stacking, voting) for improved accuracy. Serving challenges include: running multiple inference calls serially adds latency, or parallel calls require orchestration. Optimize with async fan-out and caching of base model outputs where possible.

**Q: How do you handle model version rollback in production?**
A: Keep previous model versions active in the serving infrastructure (multi-version support in TF Serving, SageMaker endpoints). Maintain a traffic routing layer (API Gateway or load balancer) that can instantly shift 100% of traffic back to the previous version. Automate rollback triggers on metric degradation alerts.

**Q: What is ONNX and what problem does it solve for model serving?**
A: ONNX (Open Neural Network Exchange) is an open format for representing ML models across frameworks. It solves framework portability — a model trained in PyTorch can be exported to ONNX and served via any ONNX-compatible runtime (e.g., ONNX Runtime), enabling framework-agnostic serving infrastructure.

---

## 💼 Interview Tips

- Always distinguish the serving pattern (batch vs. real-time vs. streaming) before discussing infrastructure — the right stack is entirely dependent on latency requirements and throughput.
- Mention the three optimization levers for serving: hardware (GPU vs. CPU, instance type), model optimization (quantization, pruning, ONNX export), and serving infrastructure (batching, caching, async). Senior candidates know all three.
- When discussing deployment strategies, go beyond "blue-green" and "canary" — explain how you'd define success criteria and rollback triggers, not just the mechanics of traffic splitting.
- Feature retrieval latency is often the hidden bottleneck in online serving — mentioning the online feature store and its p99 latency requirements shows you've operated production ML systems.
- Avoid describing serving as just "deploy a Docker container with the model." Discuss autoscaling policies, cost vs. latency tradeoffs, and multi-model serving for efficiency.
- Senior interviewers care about the gap between offline and online performance — be ready to explain how you validate that a model serving via REST/gRPC gives the same predictions as the training-time evaluation.
