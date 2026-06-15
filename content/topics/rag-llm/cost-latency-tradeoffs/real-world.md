---
title: "Cost & Latency Tradeoffs - Real World"
topic: rag-llm
subtopic: cost-latency-tradeoffs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, cost, production, optimization, monitoring, case-study]
---

# Cost & Latency Tradeoffs — Real World

## Case Study 1: Reducing a $45K/Month LLM Bill by 72%

**Situation:** An e-commerce DE team had an LLM pipeline classifying 2M product listings/month using GPT-4o. Cost was $45K/month. SLA: results within 4 hours of listing creation.

### Before: Naive Implementation

```python
# Original: every listing → GPT-4o synchronously
def classify_product(title: str, description: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o",            # expensive
        messages=[
            {"role": "system", "content": TAXONOMY_PROMPT},  # 3000 tokens, repeated
            {"role": "user", "content": f"Title: {title}\nDescription: {description}"}
        ],
        temperature=0
    )
    return json.loads(response.choices[0].message.content)

# Cost: 2M × (3200 input + 150 output) × $0.00001025 = ~$68K/month
# (Note: TAXONOMY_PROMPT repeated for every call = wasteful)
```

### After: Three Optimizations Applied

```python
from openai import OpenAI
import json, hashlib
import redis

client = OpenAI()
r = redis.Redis()

# Optimization 1: Batch API (50% cost reduction)
def submit_classification_batch(listings: list[dict]) -> str:
    """Submit nightly batch for 50% cost savings."""
    requests = []
    for listing in listings:
        requests.append({
            "custom_id": listing["id"],
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": TAXONOMY_PROMPT},
                    {"role": "user", "content": f"Title: {listing['title']}\nDesc: {listing['description'][:500]}"}
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0,
                "max_tokens": 100  # Optimization 2: limit output tokens (was 512)
            }
        })

    with open("/tmp/batch.jsonl", "w") as f:
        f.write("\n".join(json.dumps(r) for r in requests))

    with open("/tmp/batch.jsonl", "rb") as f:
        file_obj = client.files.create(file=f, purpose="batch")

    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    return batch.id

# Optimization 3: Semantic cache for repeated/similar titles
EMBED_CACHE = {}  # In production: Redis with TTL

def get_embedding(text: str) -> list[float]:
    cache_key = hashlib.md5(text.encode()).hexdigest()
    if cache_key not in EMBED_CACHE:
        resp = client.embeddings.create(model="text-embedding-3-small", input=text)
        EMBED_CACHE[cache_key] = resp.data[0].embedding
    return EMBED_CACHE[cache_key]

def find_similar_classification(title: str, threshold: float = 0.95) -> dict | None:
    """Check if we've classified a nearly-identical product before."""
    query_emb = get_embedding(title)
    # In production: vector DB query (Pinecone/Redis with vector support)
    # For demo: check Redis hash of past embeddings
    cached = r.hgetall("product_classifications")
    import numpy as np
    for key, value in cached.items():
        entry = json.loads(value)
        sim = np.dot(query_emb, entry["embedding"]) / (np.linalg.norm(query_emb) * np.linalg.norm(entry["embedding"]))
        if sim >= threshold:
            return entry["classification"]
    return None

# Result breakdown:
# Batch API: $45K × 0.5 = $22.5K
# Max tokens 100 vs 512: $22.5K × 0.7 = $15.75K (output tokens = 4x cost of input)
# Semantic cache 30% hit rate: $15.75K × 0.7 = $11K
# Final: $11K/month — 76% reduction, within 4h SLA via nightly batch
```

---

## Case Study 2: Latency Optimization for Real-Time RAG

**Situation:** A data platform team had a RAG chatbot for analysts. p50 latency was 3.2s, p95 was 7.8s. User satisfaction was low. Target: p50 < 1s, p95 < 2.5s.

### Latency Profiling

```python
import time
from dataclasses import dataclass

@dataclass
class LatencyBreakdown:
    embedding_ms: float
    vector_search_ms: float
    context_assembly_ms: float
    llm_ttft_ms: float
    llm_generation_ms: float

    @property
    def total_ms(self):
        return (self.embedding_ms + self.vector_search_ms +
                self.context_assembly_ms + self.llm_ttft_ms + self.llm_generation_ms)

def profiled_rag_call(query: str) -> tuple[str, LatencyBreakdown]:
    # Step 1: Embed query
    t0 = time.time()
    query_embedding = get_embedding(query)
    embed_ms = (time.time() - t0) * 1000

    # Step 2: Vector search
    t0 = time.time()
    chunks = vector_db.query(query_embedding, top_k=5)
    search_ms = (time.time() - t0) * 1000

    # Step 3: Assemble context
    t0 = time.time()
    context = "\n\n".join(c["text"] for c in chunks)
    assembly_ms = (time.time() - t0) * 1000

    # Step 4: LLM call with streaming for TTFT measurement
    t0 = time.time()
    ttft_ms = None
    full_response = ""

    stream = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": f"Context:\n{context}\n\nAnswer based on context only."},
            {"role": "user", "content": query}
        ],
        stream=True
    )

    for chunk in stream:
        if ttft_ms is None and chunk.choices[0].delta.content:
            ttft_ms = (time.time() - t0) * 1000
        full_response += chunk.choices[0].delta.content or ""

    generation_ms = (time.time() - t0) * 1000 - ttft_ms

    breakdown = LatencyBreakdown(embed_ms, search_ms, assembly_ms, ttft_ms, generation_ms)
    return full_response, breakdown

# Profiling results:
# embedding_ms: 85ms → bottleneck #1
# vector_search_ms: 320ms → bottleneck #2
# context_assembly_ms: 2ms
# llm_ttft_ms: 450ms
# llm_generation_ms: 2340ms → bottleneck #3
# Total p50: 3200ms
```

### Optimizations Applied

```python
import asyncio
import aiohttp

# Fix 1: Parallelize embedding + any pre-fetch
async def parallel_embed_and_prefetch(query: str) -> tuple[list[float], dict]:
    embed_task = asyncio.create_task(async_get_embedding(query))
    prefetch_task = asyncio.create_task(prefetch_user_context())
    embedding, user_ctx = await asyncio.gather(embed_task, prefetch_task)
    return embedding, user_ctx

# Fix 2: Cache embeddings for common analytical queries
from functools import lru_cache

@lru_cache(maxsize=10000)
def cached_embedding(query_normalized: str) -> tuple:
    """Cache embedding for normalized query strings."""
    result = get_embedding(query_normalized)
    return tuple(result)

def normalize_query(query: str) -> str:
    """Normalize for cache key: lowercase, strip punctuation, sort words."""
    import re
    return " ".join(sorted(re.sub(r'[^\w\s]', '', query.lower()).split()))

# Fix 3: Reduce vector search latency — tune HNSW ef_search
# Pinecone: no direct control; use fewer top_k results (5→3)
# Qdrant: tune hnsw_ef at query time
from qdrant_client import QdrantClient
from qdrant_client.models import SearchParams

qdrant = QdrantClient(host="localhost", port=6333)

def fast_vector_search(query_vector: list[float], top_k: int = 3) -> list[dict]:
    results = qdrant.search(
        collection_name="knowledge_base",
        query_vector=query_vector,
        limit=top_k,
        search_params=SearchParams(hnsw_ef=64)  # default 128 → 64 = 40% faster, minor recall drop
    )
    return [{"text": r.payload["text"], "score": r.score} for r in results]

# Fix 4: Reduce output tokens — constrain response length
response = client.chat.completions.create(
    model="gpt-4o",
    messages=messages,
    max_tokens=300,    # was unlimited → cuts generation time proportionally
    stream=True        # stream so TTFT drives perceived latency
)

# Results after optimization:
# embedding_ms: 85ms → 12ms (cache hit)
# vector_search_ms: 320ms → 190ms (ef=64 + top_k=3)
# llm_ttft_ms: 450ms (unchanged — network bound)
# llm_generation_ms: 2340ms → 920ms (max_tokens=300)
# Total p50: 1572ms → with parallel embed+prefetch → ~1100ms
# p95: 2200ms (target met)
```

---

## Cost Monitoring Dashboard

```python
# Track cost metrics in Prometheus/Grafana
from prometheus_client import Counter, Histogram, Gauge

llm_tokens = Counter("llm_tokens_total", "Total tokens consumed", ["model", "type", "team"])
llm_cost = Counter("llm_cost_usd_total", "Total LLM cost in USD", ["model", "team"])
llm_latency = Histogram("llm_latency_ms", "LLM call latency", ["model"], buckets=[100,300,500,1000,2000,5000])
cache_ops = Counter("semantic_cache_ops_total", "Cache operations", ["result"])  # hit/miss

PRICING = {"gpt-4o": (2.50, 10.0), "gpt-4o-mini": (0.15, 0.60), "claude-opus-4-5": (15.0, 75.0)}

def record_llm_metrics(model: str, input_tokens: int, output_tokens: int,
                       latency_ms: float, team: str = "platform"):
    rates = PRICING.get(model, (5.0, 15.0))
    cost = (input_tokens * rates[0] + output_tokens * rates[1]) / 1_000_000

    llm_tokens.labels(model=model, type="input", team=team).inc(input_tokens)
    llm_tokens.labels(model=model, type="output", team=team).inc(output_tokens)
    llm_cost.labels(model=model, team=team).inc(cost)
    llm_latency.labels(model=model).observe(latency_ms)

# Alert rules (Alertmanager):
# - llm_cost_usd_total > $1000/hour → page immediately
# - rate(llm_latency_ms_bucket{le="1000"}[5m]) < 0.8 → warn (p80 < 1s violated)
# - cache_ops{result="hit"} / cache_ops_total < 0.2 → warn (cache not working)
```

---

## Lessons Learned

1. **Output tokens cost 4x input** — always set `max_tokens` explicitly. Verbose system prompts that generate long explanations are expensive; use structured output (JSON) to constrain output size.

2. **Batch API is underutilized** — most teams don't realize you can shift 60-70% of LLM workloads to batch (nightly enrichment, classification, eval runs) at half cost. 24h SLA is acceptable for most non-interactive pipelines.

3. **Profile before optimizing** — always measure where latency actually is. In most RAG systems it's the LLM generation phase, not embedding or vector search. Many teams optimize the wrong thing.

4. **Semantic cache requires tuning** — threshold of 0.95+ is safe for factual queries; 0.90 may be appropriate for more flexible tasks. Always A/B test cache quality before deploying at scale.

5. **Prompt caching requires cache-friendly prompt structure** — the static prefix must come first, user content last. Mixing static and dynamic content breaks caching.
