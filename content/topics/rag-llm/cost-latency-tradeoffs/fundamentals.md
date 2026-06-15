---
title: "Cost & Latency Tradeoffs - Fundamentals"
topic: rag-llm
subtopic: cost-latency-tradeoffs
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [rag, llm, cost, latency, tokens, pricing, optimization]
---

# Cost & Latency Tradeoffs — Fundamentals

## Why This Matters for Data Engineers

LLM API costs can easily reach $10K+/month at scale. A data pipeline calling GPT-4o 100K times/day with 2K tokens per call costs ~$500/day. Understanding token economics and optimization techniques is critical for building cost-effective LLM pipelines.

---

## Token Economics

Everything in LLM pricing is denominated in **tokens**. A token is roughly 4 characters or 0.75 words in English.

```
"Hello, world!" → 4 tokens
"The data engineer wrote an optimized SQL query for the data warehouse" → 13 tokens
1,000 words ≈ 1,333 tokens
```

### 2024 Pricing Reference (per 1M tokens)

| Model | Input | Output | Context Window |
|-------|-------|--------|---------------|
| **GPT-4o** | $2.50 | $10.00 | 128K |
| **GPT-4o-mini** | $0.15 | $0.60 | 128K |
| **Claude 3.5 Sonnet** | $3.00 | $15.00 | 200K |
| **Claude 3.5 Haiku** | $0.80 | $4.00 | 200K |
| **Gemini 1.5 Pro** | $1.25 | $5.00 | 1M |
| **Gemini 1.5 Flash** | $0.075 | $0.30 | 1M |

**Key insight**: Output tokens cost 4-5x more than input tokens. Minimize output verbosity.

---

## Cost Calculation

```python
def calculate_cost(
    input_tokens: int,
    output_tokens: int,
    model: str = "gpt-4o"
) -> float:
    """Calculate cost in USD for a single LLM call."""
    pricing = {
        "gpt-4o":         {"input": 2.50,  "output": 10.00},
        "gpt-4o-mini":    {"input": 0.15,  "output": 0.60},
        "claude-sonnet":  {"input": 3.00,  "output": 15.00},
        "claude-haiku":   {"input": 0.80,  "output": 4.00},
        "gemini-pro":     {"input": 1.25,  "output": 5.00},
        "gemini-flash":   {"input": 0.075, "output": 0.30},
    }
    rates = pricing[model]
    cost = (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1_000_000
    return cost

def estimate_daily_cost(
    requests_per_day: int,
    avg_input_tokens: int,
    avg_output_tokens: int,
    model: str = "gpt-4o"
) -> dict:
    """Project daily and monthly cost."""
    cost_per_request = calculate_cost(avg_input_tokens, avg_output_tokens, model)
    return {
        "cost_per_request_usd": round(cost_per_request, 6),
        "daily_cost_usd": round(cost_per_request * requests_per_day, 2),
        "monthly_cost_usd": round(cost_per_request * requests_per_day * 30, 2)
    }

# Example: Data quality pipeline, 10K checks/day, 500 input + 200 output tokens
print(estimate_daily_cost(10000, 500, 200, "gpt-4o"))
# {'cost_per_request_usd': 0.003250, 'daily_cost_usd': 32.50, 'monthly_cost_usd': 975.0}

print(estimate_daily_cost(10000, 500, 200, "gpt-4o-mini"))
# {'cost_per_request_usd': 0.000196, 'daily_cost_usd': 1.96, 'monthly_cost_usd': 58.8}
# → 16x cheaper with mini model
```

---

## Streaming vs Non-Streaming

**Streaming** sends tokens to the client as they're generated. **Non-streaming** waits for the full response.

| | Streaming | Non-Streaming |
|-|-----------|--------------|
| **Time to first token** | Fast (~200ms) | Slow (wait for full response) |
| **Total latency** | Same | Same |
| **UX** | Feels responsive | Appears frozen |
| **Cost** | Identical | Identical |
| **Use case** | Interactive apps | Batch pipelines |

```python
from openai import OpenAI
client = OpenAI()

# Streaming — for interactive use
def stream_response(prompt: str):
    stream = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True
    )
    for chunk in stream:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)

# Non-streaming — for batch pipelines (simpler, same cost)
def batch_response(prompt: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content
```

---

## Latency Components

```
Total latency = Network RTT + Queue wait + TTFT + Generation time
                    │              │          │           │
                  ~50ms         0-2000ms    ~200ms    tokens × ~15ms/token
```

- **TTFT** (Time to First Token): ~200-500ms for cloud models
- **Generation speed**: GPT-4o generates ~50-80 tokens/second
- **p50 vs p99**: Median latency is often 1-2s; p99 can be 5-10s due to queue spikes

```python
import time

def measure_latency(prompt: str, n_samples: int = 10) -> dict:
    """Measure TTFT and total latency across multiple calls."""
    ttfts = []
    total_latencies = []

    for _ in range(n_samples):
        t_start = time.time()
        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            stream=True
        )
        first_token = True
        for chunk in stream:
            if first_token and chunk.choices[0].delta.content:
                ttfts.append((time.time() - t_start) * 1000)
                first_token = False
        total_latencies.append((time.time() - t_start) * 1000)

    import statistics
    return {
        "ttft_p50_ms": statistics.median(ttfts),
        "ttft_p95_ms": sorted(ttfts)[int(0.95 * len(ttfts))],
        "total_p50_ms": statistics.median(total_latencies),
        "total_p95_ms": sorted(total_latencies)[int(0.95 * len(total_latencies))]
    }
```

---

## Key Terms

| Term | Definition |
|------|-----------|
| **Token** | Unit of text ~4 chars; all LLM pricing is per token |
| **TTFT** | Time to First Token — how fast streaming starts |
| **Prompt caching** | Re-using a static prompt prefix to reduce input token cost |
| **Batch API** | Send many requests at once for ~50% cost reduction, accepts higher latency |
| **Model routing** | Use cheap model by default, escalate to expensive model when needed |
| **Quantization** | Reducing model weight precision (float32 → int4) to save memory/speed up inference |
