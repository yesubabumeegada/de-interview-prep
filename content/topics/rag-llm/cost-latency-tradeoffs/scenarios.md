---
title: "Cost & Latency Tradeoffs - Scenario Questions"
topic: rag-llm
subtopic: cost-latency-tradeoffs
content_type: scenario_question
tags: [rag, llm, cost, latency, optimization, scenarios, interview]
---

# Scenario Questions — Cost & Latency Tradeoffs

<article data-difficulty="junior">

## 🟢 Junior: Calculate and Optimize LLM Pipeline Cost

**Scenario:** You're building a pipeline that uses GPT-4o to generate a data quality summary for each of your 500 tables, run once per day. Each call uses roughly 800 input tokens (table schema + sample data) and 200 output tokens (the summary). What does this cost per day and per month? The team wants to cut costs by 80%. What's your approach?

<details>
<summary>💡 Hint</summary>
Calculate: (input_tokens × input_rate + output_tokens × output_rate) × calls_per_day. GPT-4o is $2.50/M input and $10/M output. Then think about: (1) is this task simple enough for gpt-4o-mini? (2) does the summary change every day, or could you cache it? (3) is this interactive or batch?
</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Calculate current cost**

```python
def calculate_daily_cost():
    tables = 500
    input_tokens_per_call = 800
    output_tokens_per_call = 200

    # GPT-4o pricing (per million tokens)
    input_rate = 2.50   # $2.50/M
    output_rate = 10.00  # $10.00/M

    cost_per_call = (
        input_tokens_per_call * input_rate +
        output_tokens_per_call * output_rate
    ) / 1_000_000

    daily_cost = cost_per_call * tables
    monthly_cost = daily_cost * 30

    print(f"Cost per call: ${cost_per_call:.4f}")   # $0.0040
    print(f"Daily cost: ${daily_cost:.2f}")          # $2.00
    print(f"Monthly cost: ${monthly_cost:.2f}")      # $60.00
    return daily_cost

calculate_daily_cost()
# $0.0040/call × 500 = $2.00/day = $60/month
```

**Step 2: 80% reduction strategy**

```python
from openai import OpenAI
import json, hashlib
import redis

client = OpenAI()
r = redis.Redis()

# Strategy 1: Switch to gpt-4o-mini (16x cheaper for this task)
# gpt-4o-mini: $0.15/M input, $0.60/M output
# New cost: (800 × 0.15 + 200 × 0.60) / 1M = $0.000240/call
# 500 × $0.000240 = $0.12/day = $3.60/month → 94% reduction!

# Strategy 2: Cache results (schema changes are rare)
def get_table_schema_hash(table_name: str) -> str:
    """Hash the schema to detect changes."""
    # In production: query information_schema
    schema = get_schema(table_name)  # your existing function
    return hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest()

def get_dq_summary_with_cache(table_name: str, schema: dict) -> str:
    schema_hash = get_table_schema_hash(table_name)
    cache_key = f"dq_summary:{table_name}:{schema_hash}"

    # Check cache
    cached = r.get(cache_key)
    if cached:
        return cached.decode()  # No API call needed — 0 cost

    # Cache miss: call LLM
    response = client.chat.completions.create(
        model="gpt-4o-mini",    # cheaper model
        messages=[
            {"role": "system", "content": "Generate a concise data quality summary (3 sentences max)."},
            {"role": "user", "content": f"Table: {table_name}\nSchema: {json.dumps(schema)}"}
        ],
        max_tokens=200,         # constrain output (was unlimited)
        temperature=0
    )
    summary = response.choices[0].message.content

    # Cache with 7-day TTL (schema rarely changes)
    r.setex(cache_key, 7 * 24 * 3600, summary)
    return summary

# Strategy 3: Use batch API for 50% additional reduction
# (combine with mini model: 94% reduction → batch gives another 50% on LLM cost)
# Final estimate: ~97% cost reduction vs original

# Summary of options:
print("\nCost comparison (500 tables/day):")
print(f"GPT-4o, no cache:        $60.00/month")
print(f"GPT-4o-mini, no cache:   $3.60/month  (94% reduction)")
print(f"GPT-4o-mini + cache:     ~$0.50/month (99% reduction — only pays for schema changes)")
print(f"GPT-4o-mini + batch API: $1.80/month  (97% reduction — no cache needed)")
```

**Interview answer summary:**
- Switch to `gpt-4o-mini`: immediate 94% cost reduction — most impactful single change
- Add schema-based caching: only regenerate when schema changes (tables are stable); expected cache hit rate ~85%
- Set `max_tokens=200` to cap output verbosity
- If interactive deadline is flexible: batch API for additional 50% off LLM cost
- Combined effect: ~97-99% cost reduction

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid: Implement Semantic Caching for a High-Traffic RAG Pipeline

**Scenario:** Your RAG chatbot serves 50K queries/day. Monitoring shows that analysts ask very similar questions repeatedly: "What is Q1 revenue?", "Show me Q1 2024 revenue", "Q1 revenue total?" These should all hit the same cache entry. Implement a semantic cache using Redis and OpenAI embeddings that: (1) caches by semantic similarity (not exact match), (2) uses a configurable similarity threshold, (3) tracks cache hit rate as a metric, (4) expires entries after 1 hour.

<details>
<summary>💡 Hint</summary>
Embed the query using `text-embedding-3-small`. Store (embedding, response) in Redis as a hash. On cache lookup, compute cosine similarity between the query embedding and all stored embeddings. Return the cached response if similarity > threshold. Use `EXPIRE` for TTL. Track hits/misses in a separate Redis counter.
</details>

<details>
<summary>✅ Solution</summary>

```python
import redis
import numpy as np
import json
import hashlib
import time
from openai import OpenAI
from typing import Optional

client = OpenAI()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

CACHE_NAMESPACE = "semantic_cache:v1"
METRICS_NAMESPACE = "semantic_cache:metrics"
DEFAULT_TTL = 3600          # 1 hour
DEFAULT_THRESHOLD = 0.92    # cosine similarity threshold

def get_embedding(text: str) -> list[float]:
    """Embed text using text-embedding-3-small."""
    resp = client.embeddings.create(
        model="text-embedding-3-small",
        input=text.strip().lower()  # normalize before embedding
    )
    return resp.data[0].embedding

def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two embedding vectors."""
    a_arr, b_arr = np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)
    norm_a, norm_b = np.linalg.norm(a_arr), np.linalg.norm(b_arr)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a_arr, b_arr) / (norm_a * norm_b))

def cache_get(query: str, threshold: float = DEFAULT_THRESHOLD) -> Optional[dict]:
    """
    Look up cache for semantically similar query.
    Returns: {"response": str, "similarity": float, "cached_query": str} or None
    """
    query_embedding = get_embedding(query)

    # Get all cache entries
    entries = r.hgetall(CACHE_NAMESPACE)

    best_sim = 0.0
    best_entry = None

    for key, raw_value in entries.items():
        entry = json.loads(raw_value)
        sim = cosine_similarity(query_embedding, entry["embedding"])
        if sim > best_sim:
            best_sim = sim
            best_entry = entry

    if best_entry and best_sim >= threshold:
        # Track hit
        r.incr(f"{METRICS_NAMESPACE}:hits")
        r.incr(f"{METRICS_NAMESPACE}:total")
        return {
            "response": best_entry["response"],
            "similarity": best_sim,
            "cached_query": best_entry["original_query"]
        }

    # Track miss
    r.incr(f"{METRICS_NAMESPACE}:misses")
    r.incr(f"{METRICS_NAMESPACE}:total")
    return None

def cache_set(query: str, response: str, ttl: int = DEFAULT_TTL):
    """Store a query-response pair in the semantic cache."""
    embedding = get_embedding(query)
    cache_key = hashlib.sha256(query.encode()).hexdigest()[:16]

    entry = {
        "original_query": query,
        "embedding": embedding,
        "response": response,
        "stored_at": time.time()
    }

    r.hset(CACHE_NAMESPACE, cache_key, json.dumps(entry))
    r.expire(CACHE_NAMESPACE, ttl)  # TTL on the whole hash

def get_cache_hit_rate() -> dict:
    """Return current cache performance metrics."""
    hits = int(r.get(f"{METRICS_NAMESPACE}:hits") or 0)
    misses = int(r.get(f"{METRICS_NAMESPACE}:misses") or 0)
    total = hits + misses

    return {
        "hit_rate": hits / total if total > 0 else 0.0,
        "hits": hits,
        "misses": misses,
        "total": total,
        "cache_size": r.hlen(CACHE_NAMESPACE)
    }

def rag_with_semantic_cache(query: str, system_prompt: str) -> dict:
    """Full RAG call with semantic caching."""
    t0 = time.time()

    # Check semantic cache first
    cached = cache_get(query)
    if cached:
        return {
            "response": cached["response"],
            "source": "semantic_cache",
            "similarity": cached["similarity"],
            "cached_query": cached["cached_query"],
            "latency_ms": (time.time() - t0) * 1000,
            "cost_usd": 0.0  # cache hit is free!
        }

    # Cache miss — call LLM
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
    )
    answer = response.choices[0].message.content
    tokens_in = response.usage.prompt_tokens
    tokens_out = response.usage.completion_tokens
    cost = (tokens_in * 2.50 + tokens_out * 10.0) / 1_000_000

    # Store in cache for future similar queries
    cache_set(query, answer)

    return {
        "response": answer,
        "source": "llm",
        "similarity": 0.0,
        "cached_query": None,
        "latency_ms": (time.time() - t0) * 1000,
        "cost_usd": cost
    }

# Test semantic similarity grouping
queries = [
    "What is Q1 2024 revenue?",
    "Show me Q1 revenue",           # should hit cache (sim ~0.95)
    "Q1 2024 total revenue",        # should hit cache (sim ~0.94)
    "What were Q3 sales?",          # should miss cache (different quarter)
]

system_prompt = "You are a data analyst. Answer based on available data."

for q in queries:
    result = rag_with_semantic_cache(q, system_prompt)
    print(f"Q: {q[:40]:<40} | Source: {result['source']:<6} | Sim: {result['similarity']:.3f}")

print("\nCache metrics:", get_cache_hit_rate())
```

**Production notes for the interview:**
- In production: replace the `hgetall` linear scan with a vector DB (Redis with `redis-py` vector extensions, or a dedicated store like Qdrant) — linear scan doesn't scale past ~10K entries
- Use `text-embedding-3-small` for the cache key embedding (cheap at $0.02/M), not `text-embedding-3-large` — quality is sufficient for cache lookup
- Threshold tuning: 0.92+ for factual queries, 0.88+ for conversational/creative — test on your query distribution
- Cache invalidation: when underlying data changes, flush entries by topic (use prefix keys per data domain)
- Expected hit rate: 30-60% for analytical dashboards where analysts repeatedly check the same metrics

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design an Adaptive Model Router with Cost Budgets and SLA Guarantees

**Scenario:** You're the lead DE for an internal data assistant serving 200 engineers. The team has a monthly LLM budget of $5,000. Users expect p95 latency < 3 seconds. Queries range from simple ("What does this table do?") to complex ("Debug why this dbt model has 30% null values and suggest a fix"). Design and implement an adaptive routing system that: (1) classifies query complexity, (2) routes to the cheapest model that can handle the query, (3) tracks budget in real-time and degrades gracefully when near the limit, (4) monitors p95 latency and routes away from slow endpoints, (5) has a circuit breaker for model API failures.

<details>
<summary>💡 Hint</summary>
Design as a state machine: NORMAL → BUDGET_WARNING → BUDGET_CRITICAL → BUDGET_EXHAUSTED. In each state, route to progressively cheaper models. Track latency with a rolling percentile tracker. Use a circuit breaker pattern: after N consecutive failures, mark the endpoint as OPEN (bypass) for 60 seconds. Use Redis for shared state across multiple router instances.
</details>

<details>
<summary>✅ Solution</summary>

```python
import time, json, threading
import redis
from openai import OpenAI
from anthropic import Anthropic
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
from typing import Optional

openai_client = OpenAI()
anthropic_client = Anthropic()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# --- Model Registry ---

@dataclass
class ModelConfig:
    name: str
    provider: str           # "openai" | "anthropic"
    input_cost_per_m: float
    output_cost_per_m: float
    max_complexity: str     # "simple" | "moderate" | "complex"
    target_p95_ms: float    # target p95 latency for this model

MODELS = {
    "simple":   ModelConfig("gpt-4o-mini",    "openai",    0.15,  0.60,  "simple",   800),
    "moderate": ModelConfig("gpt-4o",         "openai",    2.50,  10.00, "moderate", 2000),
    "complex":  ModelConfig("claude-opus-4-5","anthropic", 15.00, 75.00, "complex",  4000),
}

# --- Budget State Machine ---

class BudgetState(str, Enum):
    NORMAL   = "normal"         # all models available
    WARNING  = "warning"        # >80% budget used — avoid complex model
    CRITICAL = "critical"       # >95% budget used — simple only
    EXHAUSTED = "exhausted"     # budget hit — block all calls

def get_budget_state(monthly_budget: float = 5000.0) -> tuple[BudgetState, float]:
    spent = float(r.get("llm:budget:spent_usd") or 0)
    pct = spent / monthly_budget
    if pct >= 1.0:
        return BudgetState.EXHAUSTED, spent
    elif pct >= 0.95:
        return BudgetState.CRITICAL, spent
    elif pct >= 0.80:
        return BudgetState.WARNING, spent
    return BudgetState.NORMAL, spent

def record_cost(cost_usd: float):
    r.incrbyfloat("llm:budget:spent_usd", cost_usd)
    # Expire at end of month (simplified: 30d TTL reset monthly by cron)

# --- Latency Tracker (rolling window, Redis-backed) ---

class DistributedLatencyTracker:
    def __init__(self, model: str, window: int = 100):
        self.key = f"llm:latency:{model}"
        self.window = window

    def record(self, latency_ms: float):
        r.lpush(self.key, latency_ms)
        r.ltrim(self.key, 0, self.window - 1)

    def percentile(self, p: float) -> float:
        samples = [float(v) for v in r.lrange(self.key, 0, -1)]
        if not samples:
            return 0.0
        sorted_samples = sorted(samples)
        idx = int(len(sorted_samples) * p / 100)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

latency_trackers = {name: DistributedLatencyTracker(name) for name in MODELS}

# --- Circuit Breaker ---

class CircuitState(str, Enum):
    CLOSED = "closed"   # normal
    OPEN   = "open"     # tripped — bypass model
    HALF   = "half"     # testing if recovered

class CircuitBreaker:
    def __init__(self, model: str, failure_threshold: int = 5, reset_timeout_s: int = 60):
        self.model = model
        self.failure_key = f"llm:circuit:{model}:failures"
        self.tripped_at_key = f"llm:circuit:{model}:tripped_at"
        self.failure_threshold = failure_threshold
        self.reset_timeout_s = reset_timeout_s

    def state(self) -> CircuitState:
        tripped_at = r.get(self.tripped_at_key)
        if not tripped_at:
            return CircuitState.CLOSED
        elapsed = time.time() - float(tripped_at)
        if elapsed > self.reset_timeout_s:
            return CircuitState.HALF
        return CircuitState.OPEN

    def record_failure(self):
        failures = r.incr(self.failure_key)
        r.expire(self.failure_key, 120)
        if failures >= self.failure_threshold:
            r.set(self.tripped_at_key, time.time())

    def record_success(self):
        r.delete(self.failure_key)
        r.delete(self.tripped_at_key)

    def is_available(self) -> bool:
        return self.state() in (CircuitState.CLOSED, CircuitState.HALF)

breakers = {name: CircuitBreaker(name) for name in MODELS}

# --- Query Complexity Classifier ---

def classify_complexity(query: str) -> str:
    resp = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": """Classify query complexity:
simple: lookup, definition, yes/no, < 3 steps of reasoning
moderate: multi-step, code generation, analysis
complex: architecture, debugging ambiguous issues, cross-system reasoning
Return only: simple | moderate | complex"""},
            {"role": "user", "content": query[:300]}
        ],
        temperature=0,
        max_tokens=10
    )
    return resp.choices[0].message.content.strip().lower()

# --- The Adaptive Router ---

def call_model(model_config: ModelConfig, query: str, system_prompt: str) -> tuple[str, int, int]:
    """Call the specified model. Returns (response, input_tokens, output_tokens)."""
    if model_config.provider == "openai":
        resp = openai_client.chat.completions.create(
            model=model_config.name,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": query}],
            max_tokens=1000
        )
        return resp.choices[0].message.content, resp.usage.prompt_tokens, resp.usage.completion_tokens

    elif model_config.provider == "anthropic":
        resp = anthropic_client.messages.create(
            model=model_config.name, max_tokens=1000,
            system=system_prompt,
            messages=[{"role": "user", "content": query}]
        )
        return resp.content[0].text, resp.usage.input_tokens, resp.usage.output_tokens

def adaptive_route(query: str, system_prompt: str, monthly_budget: float = 5000.0) -> dict:
    """
    Route query to optimal model based on:
    - Query complexity
    - Current budget state
    - Model latency health
    - Circuit breaker status
    """
    # 1. Check budget state
    budget_state, spent = get_budget_state(monthly_budget)

    if budget_state == BudgetState.EXHAUSTED:
        return {
            "response": "Monthly LLM budget exhausted. Service will resume next month.",
            "model": None, "cost_usd": 0, "blocked": True, "reason": "budget_exhausted"
        }

    # 2. Classify query (always affordable — uses mini model)
    complexity = classify_complexity(query)

    # 3. Determine target model based on complexity + budget state
    if budget_state == BudgetState.CRITICAL:
        target_complexity = "simple"  # force cheapest model
    elif budget_state == BudgetState.WARNING:
        target_complexity = "simple" if complexity == "simple" else "moderate"  # cap at moderate
    else:
        target_complexity = complexity

    # 4. Build fallback chain: try target, fall back to cheaper models
    model_priority = ["complex", "moderate", "simple"]
    target_idx = model_priority.index(target_complexity)
    candidates = model_priority[target_idx:]  # target and cheaper

    selected_model = None
    for candidate in candidates:
        config = MODELS[candidate]
        breaker = breakers[candidate]
        tracker = latency_trackers[candidate]

        # Check circuit breaker
        if not breaker.is_available():
            continue

        # Check p95 latency health (if we have data)
        p95 = tracker.percentile(95)
        if p95 > 0 and p95 > config.target_p95_ms * 1.5:  # 50% over target → skip
            continue

        selected_model = candidate
        break

    if not selected_model:
        return {"response": "All models unavailable.", "model": None, "cost_usd": 0,
                "blocked": True, "reason": "all_circuits_open"}

    config = MODELS[selected_model]
    breaker = breakers[selected_model]
    tracker = latency_trackers[selected_model]

    # 5. Execute call with telemetry
    t0 = time.time()
    try:
        response, tokens_in, tokens_out = call_model(config, query, system_prompt)
        latency_ms = (time.time() - t0) * 1000
        breaker.record_success()
        tracker.record(latency_ms)

        cost = (tokens_in * config.input_cost_per_m + tokens_out * config.output_cost_per_m) / 1_000_000
        record_cost(cost)

        return {
            "response": response,
            "model": config.name,
            "complexity": complexity,
            "target_complexity": target_complexity,
            "budget_state": budget_state,
            "cost_usd": cost,
            "latency_ms": latency_ms,
            "blocked": False
        }

    except Exception as e:
        breaker.record_failure()
        latency_ms = (time.time() - t0) * 1000
        tracker.record(latency_ms)

        # Retry with next cheaper model
        return adaptive_route.__wrapped__(query, system_prompt, monthly_budget)  # simplified — in prod: explicit retry

# --- Usage ---
result = adaptive_route(
    query="Why does the orders table have 30% null values in the revenue column?",
    system_prompt="You are a data engineering assistant with knowledge of our Snowflake warehouse."
)
print(f"Model: {result['model']}, Complexity: {result['complexity']}, Cost: ${result['cost_usd']:.5f}")
print(f"Budget state: {result['budget_state']}, Latency: {result['latency_ms']:.0f}ms")
```

**Senior design points to articulate:**
1. **State machine for budget degradation**: explicit states with defined fallback behavior — avoids ad-hoc conditionals
2. **Circuit breaker prevents cascading failures**: OPEN state bypasses unavailable endpoints; HALF state tests recovery without full exposure
3. **Distributed state via Redis**: multiple router instances share budget counters and circuit breaker state — critical for horizontal scaling
4. **Latency-aware routing**: don't route to models with degraded p95, even if budget says it's OK — preserves SLA
5. **Complexity classifier cost**: using gpt-4o-mini for routing costs ~$0.0001/query — negligible vs routing savings
6. **Graceful degradation hierarchy**: complex → moderate → simple, never the reverse — ensures budget protection
7. **Alerting thresholds**: 80% budget → warning, 95% → critical, 100% → hard stop; alerts at 80% give 2 weeks to react

</details>
</article>
