---
title: "Cost & Latency Tradeoffs - Intermediate"
topic: rag-llm
subtopic: cost-latency-tradeoffs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, llm, cost, caching, batch-api, semantic-cache, model-routing]
---

# Cost & Latency Tradeoffs — Intermediate

## Prompt Caching

Both Anthropic and OpenAI support caching a portion of the prompt to avoid re-processing it on repeated calls. This is ideal for large system prompts, schemas, or reference documents that don't change per request.

### Anthropic cache_control

```python
import anthropic

client = anthropic.Anthropic()

# Large static context (schema, runbook, examples) — mark for caching
STATIC_CONTEXT = """You are a data engineering assistant with access to this schema:
[...5000 tokens of schema documentation...]
""" * 10  # simulate large context

def query_with_cache(user_question: str):
    """Use cache_control to cache the static prefix."""
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": STATIC_CONTEXT,
                "cache_control": {"type": "ephemeral"}  # Cache this block
            },
            {
                "type": "text",
                "text": "Answer concisely. Always cite the specific table and column names."
            }
        ],
        messages=[{"role": "user", "content": user_question}]
    )

    usage = response.usage
    print(f"Cache read tokens: {usage.cache_read_input_tokens}")    # billed at 10% of normal
    print(f"Cache write tokens: {usage.cache_creation_input_tokens}") # 25% premium on first call
    print(f"Regular input tokens: {usage.input_tokens}")
    return response.content[0].text

# First call: cache_creation_input_tokens → 25% premium to write cache
# Subsequent calls within 5min: cache_read_input_tokens → 90% discount
# Net savings: >80% on input tokens for repeated calls with same prefix
```

**Cost math for Anthropic cache:**
- Cache write: 1.25× normal input rate (one-time cost)
- Cache read: 0.10× normal input rate (90% discount)
- Break-even: 2nd call is already profitable

### OpenAI Prompt Caching

OpenAI automatically caches prefixes ≥ 1024 tokens, no explicit API configuration needed:

```python
from openai import OpenAI
client = OpenAI()

# OpenAI caches automatically when:
# 1. prompt is identical prefix (same system + start of messages)
# 2. prefix is >= 1024 tokens
# 3. cache TTL: ~5-10 minutes of inactivity

LARGE_SYSTEM_PROMPT = "..." * 300  # > 1024 tokens

def optimized_batch_calls(questions: list[str]) -> list[str]:
    """All calls share the same system prompt — cached after first call."""
    responses = []
    for q in questions:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": LARGE_SYSTEM_PROMPT},
                {"role": "user", "content": q}  # only this changes per call
            ]
        )
        # Check cached_tokens in usage
        cached = resp.usage.prompt_tokens_details.cached_tokens
        print(f"Cached: {cached}/{resp.usage.prompt_tokens} input tokens")
        responses.append(resp.choices[0].message.content)
    return responses
```

---

## Semantic Caching

Cache responses based on semantic similarity of inputs, not exact string match:

```python
import redis
import numpy as np
from openai import OpenAI
import json, hashlib

client = OpenAI()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def embed(text: str) -> list[float]:
    resp = client.embeddings.create(model="text-embedding-3-small", input=text)
    return resp.data[0].embedding

def cosine_similarity(a: list[float], b: list[float]) -> float:
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

class SemanticCache:
    """Cache LLM responses by semantic similarity. Avoids redundant API calls."""

    def __init__(self, similarity_threshold: float = 0.92, ttl_seconds: int = 3600):
        self.threshold = similarity_threshold
        self.ttl = ttl_seconds
        self.cache_key = "semantic_cache"

    def get(self, query: str) -> tuple[str | None, float]:
        """Return (cached_response, similarity_score) or (None, 0.0)."""
        query_embedding = embed(query)
        entries = r.hgetall(self.cache_key)

        best_sim = 0.0
        best_response = None

        for key, value in entries.items():
            entry = json.loads(value)
            sim = cosine_similarity(query_embedding, entry["embedding"])
            if sim > best_sim:
                best_sim = sim
                best_response = entry["response"]

        if best_sim >= self.threshold:
            return best_response, best_sim
        return None, best_sim

    def set(self, query: str, response: str):
        """Cache a query-response pair."""
        embedding = embed(query)
        key = hashlib.md5(query.encode()).hexdigest()
        r.hset(self.cache_key, key, json.dumps({"embedding": embedding, "response": response, "query": query}))
        r.expire(self.cache_key, self.ttl)

cache = SemanticCache(similarity_threshold=0.92)

def cached_llm_call(query: str) -> dict:
    """LLM call with semantic cache."""
    cached_response, sim = cache.get(query)
    if cached_response:
        return {"response": cached_response, "source": "cache", "similarity": sim}

    # Cache miss — call LLM
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": query}]
    )
    answer = response.choices[0].message.content
    cache.set(query, answer)

    return {"response": answer, "source": "llm", "similarity": 0.0}

# "What is the revenue for Q1?" and "Show me Q1 revenue" → same cache hit
```

---

## Batch Inference API

OpenAI and Anthropic offer batch APIs with ~50% cost reduction for non-real-time workloads:

```python
from openai import OpenAI
import json, time

client = OpenAI()

def run_batch_job(prompts: list[dict]) -> list[dict]:
    """
    OpenAI Batch API: 50% cost reduction, 24h completion window.
    Ideal for: nightly data enrichment, bulk classification, offline eval.
    """
    # Prepare JSONL file
    requests = []
    for i, prompt_data in enumerate(prompts):
        requests.append({
            "custom_id": f"request-{i}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": prompt_data.get("system", "")},
                    {"role": "user", "content": prompt_data["user"]}
                ],
                "max_tokens": 500,
                "temperature": 0
            }
        })

    # Write JSONL
    batch_file = "\n".join(json.dumps(r) for r in requests)
    with open("/tmp/batch_requests.jsonl", "w") as f:
        f.write(batch_file)

    # Upload file
    with open("/tmp/batch_requests.jsonl", "rb") as f:
        batch_input_file = client.files.create(file=f, purpose="batch")

    # Create batch
    batch = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    print(f"Batch created: {batch.id}")

    # Poll for completion (in production: use webhook or scheduled check)
    while True:
        batch = client.batches.retrieve(batch.id)
        print(f"Status: {batch.status}, Completed: {batch.request_counts.completed}/{batch.request_counts.total}")
        if batch.status in ("completed", "failed", "cancelled"):
            break
        time.sleep(60)  # poll every minute

    if batch.status != "completed":
        raise RuntimeError(f"Batch failed with status: {batch.status}")

    # Retrieve results
    result_file = client.files.content(batch.output_file_id)
    results = []
    for line in result_file.text.strip().split("\n"):
        item = json.loads(line)
        results.append({
            "custom_id": item["custom_id"],
            "response": item["response"]["body"]["choices"][0]["message"]["content"],
            "input_tokens": item["response"]["body"]["usage"]["prompt_tokens"],
            "output_tokens": item["response"]["body"]["usage"]["completion_tokens"]
        })
    return results

# Usage for nightly product categorization
products = [{"user": f"Classify product: {name}"} for name in product_names]
results = run_batch_job(products)
# Cost: $1.25/M input + $5/M output (vs $2.50/$10 standard = 50% cheaper)
```

---

## Model Routing by Complexity

Route requests to small/cheap models by default; escalate to large models only when needed:

```python
from openai import OpenAI
from anthropic import Anthropic
import json

openai_client = OpenAI()
anthropic_client = Anthropic()

ROUTING_PROMPT = """Classify the complexity of this question for a data engineering assistant.
Return JSON: {"complexity": "simple"|"moderate"|"complex", "reason": "brief explanation"}

simple: factual lookup, definition, simple calculation
moderate: multi-step reasoning, code generation, data analysis
complex: architecture design, debugging ambiguous issues, multi-system reasoning"""

def classify_complexity(query: str) -> str:
    """Use cheapest model to classify query complexity."""
    resp = openai_client.chat.completions.create(
        model="gpt-4o-mini",  # $0.15/M input — tiny cost for routing
        messages=[
            {"role": "system", "content": ROUTING_PROMPT},
            {"role": "user", "content": query[:500]}
        ],
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=100
    )
    return json.loads(resp.choices[0].message.content)["complexity"]

MODEL_ROUTING = {
    "simple":   ("gpt-4o-mini",           0.15),  # cost per 1M input tokens
    "moderate": ("gpt-4o",                2.50),
    "complex":  ("claude-opus-4-5",       15.00)  # most capable for hard problems
}

def routed_llm_call(query: str, system_prompt: str) -> dict:
    """Route to the appropriate model based on query complexity."""
    complexity = classify_complexity(query)
    model, rate = MODEL_ROUTING[complexity]

    if model.startswith("claude"):
        resp = anthropic_client.messages.create(
            model=model, max_tokens=2048,
            messages=[{"role": "user", "content": query}],
            system=system_prompt
        )
        response_text = resp.content[0].text
        tokens_in, tokens_out = resp.usage.input_tokens, resp.usage.output_tokens
    else:
        resp = openai_client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": query}]
        )
        response_text = resp.choices[0].message.content
        tokens_in, tokens_out = resp.usage.prompt_tokens, resp.usage.completion_tokens

    cost = (tokens_in * rate + tokens_out * (rate * 4)) / 1_000_000

    return {
        "response": response_text,
        "model_used": model,
        "complexity": complexity,
        "estimated_cost_usd": cost
    }
```

---

## Context Window Optimization

```python
def optimize_context(system_prompt: str, chat_history: list[dict], user_message: str,
                     max_context_tokens: int = 4000) -> list[dict]:
    """
    Truncate history to fit within context budget.
    Strategy: always keep system prompt + last N turns + current message.
    """
    import tiktoken
    enc = tiktoken.encoding_for_model("gpt-4o")

    def count_tokens(messages: list[dict]) -> int:
        return sum(len(enc.encode(m.get("content", "") or "")) for m in messages)

    system_msg = [{"role": "system", "content": system_prompt}]
    current_msg = [{"role": "user", "content": user_message}]

    overhead = count_tokens(system_msg) + count_tokens(current_msg)
    remaining_budget = max_context_tokens - overhead

    # Add history from most recent, stop when budget exceeded
    selected_history = []
    for turn in reversed(chat_history):
        turn_tokens = count_tokens([turn])
        if remaining_budget - turn_tokens < 0:
            break
        selected_history.insert(0, turn)
        remaining_budget -= turn_tokens

    return system_msg + selected_history + current_msg
```
