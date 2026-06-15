---
title: "Cost & Latency Tradeoffs - Senior Deep Dive"
topic: rag-llm
subtopic: cost-latency-tradeoffs
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [rag, llm, cost, quantization, distillation, kv-cache, sla, production]
---

# Cost & Latency Tradeoffs — Senior Deep Dive

## KV Cache Internals

The KV (Key-Value) cache stores the attention keys and values computed for the prompt, avoiding recomputation on subsequent tokens. Understanding it helps you optimize prompts for throughput.

```
Prefill phase: compute KV for all prompt tokens (expensive, parallelizable)
Decode phase: generate one token at a time, reusing KV cache (memory-bound)

KV cache size = 2 × layers × heads × head_dim × seq_len × bytes_per_element
             = 2 × 32 × 32 × 128 × seq_len × 2 (fp16)
             = ~0.5 MB per 1K tokens for a 7B model
             = ~8 MB per 1K tokens for a 70B model

At 128K context: ~1 GB of KV cache per request for a 70B model
→ Determines max concurrent requests per GPU
```

**Production implication**: Long prompts = large KV cache = fewer concurrent users. Optimize prompt length to maximize throughput.

---

## Quantization for Self-Hosted Models

When running models on-premise, quantization dramatically reduces memory footprint and increases throughput:

| Format | Precision | Memory (7B model) | Quality loss | Use case |
|--------|-----------|-------------------|-------------|---------|
| FP32 | 32-bit float | 28 GB | None | Training |
| FP16/BF16 | 16-bit float | 14 GB | Minimal | Production serving |
| AWQ (INT4) | 4-bit adaptive | 4-5 GB | Very small | Production serving |
| GPTQ (INT4) | 4-bit static | 4-5 GB | Small | Batch inference |
| GGUF (Q4_K_M) | 4-bit + mixed | 4-5 GB | Small | CPU + consumer GPU |
| GGUF (Q2_K) | 2-bit | 2.5 GB | Significant | Constrained hardware |

```python
# Using vLLM for quantized serving (AWQ recommended for quality)
# pip install vllm

from vllm import LLM, SamplingParams

# Load AWQ quantized model — 4x memory reduction vs FP16
llm = LLM(
    model="TheBloke/Mistral-7B-Instruct-v0.2-AWQ",
    quantization="awq",
    dtype="float16",
    max_model_len=4096,
    gpu_memory_utilization=0.85,  # leave 15% for KV cache
    tensor_parallel_size=1        # set to N for multi-GPU
)

sampling_params = SamplingParams(
    temperature=0,
    max_tokens=512,
    stop=["</s>", "[INST]"]  # model-specific stop tokens
)

# Batch inference — vLLM handles KV cache sharing across requests
prompts = [f"[INST] {q} [/INST]" for q in ["Question 1", "Question 2", "Question 3"]]
outputs = llm.generate(prompts, sampling_params)

for output in outputs:
    print(output.outputs[0].text)

# Throughput: AWQ 7B on A10G → ~1500-2000 tokens/sec
# vs FP16 7B on A10G → ~800-1000 tokens/sec
```

### Comparing Quantization Methods

```python
# Benchmark: measure quality vs speed tradeoff
import time
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

def benchmark_model(model_id: str, test_prompts: list[str]) -> dict:
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForCausalLM.from_pretrained(model_id, device_map="auto", torch_dtype=torch.float16)

    latencies = []
    for prompt in test_prompts:
        inputs = tokenizer(prompt, return_tensors="pt").to("cuda")
        t0 = time.time()
        with torch.no_grad():
            outputs = model.generate(**inputs, max_new_tokens=200)
        latencies.append((time.time() - t0) * 1000)

    import statistics
    return {
        "model": model_id,
        "p50_latency_ms": statistics.median(latencies),
        "p95_latency_ms": sorted(latencies)[int(0.95 * len(latencies))],
        "memory_gb": torch.cuda.max_memory_allocated() / 1e9
    }
```

---

## Knowledge Distillation for Fine-Tuning

Distillation trains a smaller student model to mimic a larger teacher model. The student is 10-50x cheaper to run:

```python
# Distillation pipeline: generate teacher outputs → fine-tune student

from openai import OpenAI
import json

client = OpenAI()

def generate_teacher_dataset(
    training_inputs: list[str],
    teacher_model: str = "gpt-4o",
    task_system_prompt: str = ""
) -> list[dict]:
    """
    Step 1: Use expensive teacher model to generate high-quality outputs.
    These become the training data for the student model.
    """
    dataset = []
    for input_text in training_inputs:
        response = client.chat.completions.create(
            model=teacher_model,
            messages=[
                {"role": "system", "content": task_system_prompt},
                {"role": "user", "content": input_text}
            ],
            temperature=0  # deterministic for reproducibility
        )
        dataset.append({
            "messages": [
                {"role": "system", "content": task_system_prompt},
                {"role": "user", "content": input_text},
                {"role": "assistant", "content": response.choices[0].message.content}
            ]
        })
    return dataset

def fine_tune_student(dataset: list[dict], base_model: str = "gpt-4o-mini") -> str:
    """
    Step 2: Fine-tune small student model on teacher outputs.
    Result: gpt-4o quality on specific task, at gpt-4o-mini price.
    """
    import json

    # Write JSONL
    with open("/tmp/training_data.jsonl", "w") as f:
        for item in dataset:
            f.write(json.dumps(item) + "\n")

    # Upload training file
    with open("/tmp/training_data.jsonl", "rb") as f:
        training_file = client.files.create(file=f, purpose="fine-tune")

    # Create fine-tuning job
    job = client.fine_tuning.jobs.create(
        training_file=training_file.id,
        model=base_model,
        hyperparameters={"n_epochs": 3, "batch_size": 8, "learning_rate_multiplier": 1.0},
        suffix="de-classifier"  # model name: gpt-4o-mini-de-classifier-2024-01-15
    )
    print(f"Fine-tuning job: {job.id}")
    return job.id

# After fine-tuning: cost goes from $2.50/M (gpt-4o) to $0.30/M (fine-tuned mini) = 8x cheaper
```

---

## Latency SLA Design

Define and measure SLAs at each percentile:

```python
from dataclasses import dataclass
import statistics, time, threading
from collections import deque

@dataclass
class LatencySLA:
    p50_ms: float    # median
    p95_ms: float    # most users
    p99_ms: float    # tail latency
    p999_ms: float   # extreme tail

# Recommended SLAs by use case
SLAS = {
    "interactive_chat":   LatencySLA(p50_ms=800, p95_ms=2000, p99_ms=4000, p999_ms=8000),
    "search_augmented":   LatencySLA(p50_ms=400, p95_ms=1200, p99_ms=2500, p999_ms=5000),
    "batch_enrichment":   LatencySLA(p50_ms=5000, p95_ms=15000, p99_ms=30000, p999_ms=60000),
    "real_time_pipeline": LatencySLA(p50_ms=200, p95_ms=500, p99_ms=1000, p999_ms=2000),
}

class LatencyTracker:
    """Rolling window latency tracking for SLA monitoring."""

    def __init__(self, window_size: int = 1000):
        self.samples = deque(maxlen=window_size)
        self.lock = threading.Lock()

    def record(self, latency_ms: float):
        with self.lock:
            self.samples.append(latency_ms)

    def percentile(self, p: float) -> float:
        with self.lock:
            if not self.samples:
                return 0.0
            sorted_samples = sorted(self.samples)
            idx = int(len(sorted_samples) * p / 100)
            return sorted_samples[min(idx, len(sorted_samples) - 1)]

    def check_sla(self, sla: LatencySLA) -> dict:
        return {
            "p50_ok": self.percentile(50) <= sla.p50_ms,
            "p95_ok": self.percentile(95) <= sla.p95_ms,
            "p99_ok": self.percentile(99) <= sla.p99_ms,
            "p50_actual": self.percentile(50),
            "p95_actual": self.percentile(95),
            "p99_actual": self.percentile(99),
        }

tracker = LatencyTracker()

def instrumented_llm_call(prompt: str) -> str:
    t0 = time.time()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    tracker.record((time.time() - t0) * 1000)
    return response.choices[0].message.content
```

---

## Cost Optimization Decision Framework

```
Is your workload latency-sensitive?
├── YES → Can you use a smaller model?
│    ├── YES → Use gpt-4o-mini / claude-haiku + semantic cache
│    └── NO → Use streaming + prompt caching for repeated prefixes
└── NO (batch workload)
     ├── Use Batch API (50% cost reduction)
     ├── If on-premise: quantize to AWQ INT4 (4x throughput)
     └── If task-specific: distill teacher → fine-tuned mini model

Is your prompt long (>2000 tokens)?
├── YES → Is the prefix static across calls?
│    ├── YES → Use prompt caching (Anthropic cache_control or OpenAI auto)
│    └── NO → Summarize/compress dynamic context
└── NO → Optimize output length: use structured output instead of verbose prose

Are you paying too much per query?
├── Classify queries by complexity
│    ├── Simple → gpt-4o-mini ($0.15/M)
│    ├── Moderate → gpt-4o ($2.50/M)
│    └── Complex → claude-opus ($15/M) — only for hard problems
└── Add semantic cache (hits save 100% of LLM cost)
```

---

## Interview Q&A

**Q: Our LLM pipeline costs $30K/month. Where do you start cutting costs?**

A: Start with a cost breakdown by (1) model tier — are you using GPT-4o for tasks gpt-4o-mini handles equally well? (2) cache hit rate — are you recomputing identical or similar prompts? (3) output token count — are responses verbose? (4) batch vs real-time ratio — can workloads shift to batch API?

Then: implement semantic cache (typical 30-60% hit rate on repeat-pattern workloads), add model routing (route 70% of queries to mini model), enable prompt caching for static prefixes, move batch workloads to batch API. Together these typically reduce cost by 60-80%.

**Q: What's the difference between quantization formats GGUF, GPTQ, and AWQ?**

A: All are INT4 quantization methods:
- **GGUF**: CPU-friendly, used by llama.cpp/Ollama; can run on CPU with GPU offload; `Q4_K_M` is the sweet spot for quality vs size
- **GPTQ**: Post-training quantization; calibrated on a dataset; GPU-only; good quality; older standard
- **AWQ** (Activation-aware Weight Quantization): identifies salient weights and protects them from aggressive quantization; better quality than GPTQ at same bit-width; used in vLLM production serving

For production serving on GPU: AWQ. For local development on Mac/CPU: GGUF.
