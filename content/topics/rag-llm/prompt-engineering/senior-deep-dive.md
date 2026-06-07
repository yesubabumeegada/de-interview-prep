---
title: "Prompt Engineering - Senior Deep Dive"
topic: rag-llm
subtopic: prompt-engineering
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [prompt-engineering, dspy, prompt-injection, caching, multi-model, optimization, production]
---

# Prompt Engineering — Senior Deep Dive

## Production-Scale Prompt Systems

At the senior level, prompt engineering becomes **systems design**. You're optimizing for cost at 10M+ calls/month, defending against injection attacks, and building automated prompt optimization pipelines.

---

## Prompt Optimization at Scale with DSPy

DSPy replaces hand-crafted prompts with **learned prompts** — the framework optimizes prompts automatically based on training examples.

```python
import dspy
from dspy.datasets import DataLoader

# Configure DSPy with your LLM
lm = dspy.LM("openai/gpt-4o-mini", temperature=0.0)
dspy.configure(lm=lm)

# Define a signature (input → output contract)
class TextToSQL(dspy.Signature):
    """Convert natural language to SQL given a database schema."""
    schema: str = dspy.InputField(desc="Database schema DDL")
    question: str = dspy.InputField(desc="Natural language question")
    sql: str = dspy.OutputField(desc="Valid SQL query")

# Simple module using Chain of Thought
class SQLGenerator(dspy.Module):
    def __init__(self):
        self.generate = dspy.ChainOfThought(TextToSQL)
    
    def forward(self, schema: str, question: str) -> dspy.Prediction:
        return self.generate(schema=schema, question=question)

# Compile (optimize) the module with training examples
from dspy.teleprompt import BootstrapFewShot

# Training data: (input, expected_output) pairs
trainset = [
    dspy.Example(
        schema="orders(id, customer_id, amount, date)",
        question="total revenue in January",
        sql="SELECT SUM(amount) FROM orders WHERE date >= '2024-01-01' AND date < '2024-02-01'"
    ).with_inputs("schema", "question"),
    # ... more examples
]

# Metric: does the SQL parse and produce correct results?
def sql_metric(example, pred, trace=None):
    try:
        # Validate SQL syntax (use sqlparse or run against test DB)
        import sqlparse
        parsed = sqlparse.parse(pred.sql)
        return len(parsed) > 0 and parsed[0].get_type() == "SELECT"
    except Exception:
        return False

# Optimize: finds the best few-shot examples and instructions
optimizer = BootstrapFewShot(metric=sql_metric, max_bootstrapped_demos=4)
compiled_generator = optimizer.compile(SQLGenerator(), trainset=trainset)

# Use the optimized module
result = compiled_generator(
    schema="products(id, name, category, price)\norders(id, product_id, quantity, date)",
    question="top 5 products by total quantity sold"
)
print(result.sql)
```

### Advanced DSPy: Multi-Stage Optimization

```python
from dspy.teleprompt import MIPROv2

class DataQualityPipeline(dspy.Module):
    """Multi-step data quality analysis with optimized prompts."""
    
    def __init__(self):
        self.classify_issue = dspy.ChainOfThought("record, schema -> issue_type, severity")
        self.explain_issue = dspy.ChainOfThought("record, issue_type -> explanation, fix_suggestion")
        self.prioritize = dspy.ChainOfThought("issues: list -> prioritized_actions")
    
    def forward(self, records: list, schema: str):
        issues = []
        for record in records:
            classification = self.classify_issue(record=str(record), schema=schema)
            if classification.issue_type != "none":
                explanation = self.explain_issue(
                    record=str(record), 
                    issue_type=classification.issue_type
                )
                issues.append({
                    "record": record,
                    "type": classification.issue_type,
                    "severity": classification.severity,
                    "explanation": explanation.explanation,
                    "fix": explanation.fix_suggestion,
                })
        
        if issues:
            priorities = self.prioritize(issues=str(issues))
            return dspy.Prediction(issues=issues, actions=priorities.prioritized_actions)
        
        return dspy.Prediction(issues=[], actions="No issues found")

# MIPRO optimizer: optimizes instructions AND few-shot examples jointly
optimizer = MIPROv2(metric=quality_metric, num_candidates=10, max_bootstrapped_demos=3)
optimized_pipeline = optimizer.compile(DataQualityPipeline(), trainset=trainset, valset=valset)

# Save compiled module for production
optimized_pipeline.save("optimized_dq_pipeline.json")
```

---

## Prompt Caching

Both OpenAI and Anthropic offer prompt caching to reduce cost and latency for repeated prompt prefixes.

```python
# OpenAI Prompt Caching (automatic for identical prefixes > 1024 tokens)
# The system message + schema context gets cached across calls

LARGE_SCHEMA_CONTEXT = """...(2000+ tokens of schema DDL)..."""

def cached_sql_generation(questions: list[str]) -> list[str]:
    """All calls share the same prefix → automatic cache hits after first call."""
    results = []
    
    for question in questions:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                # This prefix (system + context) is cached after first call
                {"role": "system", "content": f"You are a SQL expert.\n\nSchema:\n{LARGE_SCHEMA_CONTEXT}"},
                # Only this part changes
                {"role": "user", "content": f"Generate SQL: {question}"}
            ],
            temperature=0.0,
        )
        results.append(response.choices[0].message.content)
        
        # Check cache usage in response
        # response.usage.prompt_tokens_details.cached_tokens shows cache hits
    
    return results

# Anthropic explicit caching with cache_control
import anthropic

anthropic_client = anthropic.Anthropic()

def anthropic_cached_call(question: str, schema: str):
    """Explicitly mark content for caching with Anthropic."""
    response = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": f"You are a SQL expert. Schema:\n{schema}",
                "cache_control": {"type": "ephemeral"}  # Cache this block
            }
        ],
        messages=[{"role": "user", "content": question}]
    )
    return response.content[0].text
```

### Custom Caching Layer for Pipelines

```python
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

class PromptCache:
    """Application-level prompt cache for deterministic LLM calls."""
    
    def __init__(self, backend: str = "redis", ttl_hours: int = 24):
        self.ttl = timedelta(hours=ttl_hours)
        if backend == "redis":
            import redis
            self.store = redis.Redis(decode_responses=True)
        else:
            self.store = {}  # In-memory fallback
    
    def _cache_key(self, messages: list[dict], model: str, temperature: float) -> str:
        """Deterministic cache key from prompt contents."""
        content = json.dumps({"messages": messages, "model": model, "temp": temperature}, sort_keys=True)
        return f"llm_cache:{hashlib.sha256(content.encode()).hexdigest()}"
    
    def get(self, messages: list[dict], model: str, temperature: float) -> dict | None:
        key = self._cache_key(messages, model, temperature)
        cached = self.store.get(key) if isinstance(self.store, dict) else self.store.get(key)
        if cached:
            return json.loads(cached) if isinstance(cached, str) else cached
        return None
    
    def set(self, messages: list[dict], model: str, temperature: float, response: dict):
        key = self._cache_key(messages, model, temperature)
        value = json.dumps(response)
        if hasattr(self.store, 'setex'):
            self.store.setex(key, int(self.ttl.total_seconds()), value)
        else:
            self.store[key] = value
    
    def cached_completion(self, messages: list[dict], model: str = "gpt-4o-mini", 
                          temperature: float = 0.0, **kwargs) -> dict:
        """Complete with cache — only hits LLM on cache miss."""
        # Only cache deterministic calls
        if temperature > 0:
            return self._call_llm(messages, model, temperature, **kwargs)
        
        cached = self.get(messages, model, temperature)
        if cached:
            cached["_cached"] = True
            return cached
        
        result = self._call_llm(messages, model, temperature, **kwargs)
        self.set(messages, model, temperature, result)
        return result
    
    def _call_llm(self, messages, model, temperature, **kwargs):
        response = client.chat.completions.create(
            model=model, messages=messages, temperature=temperature, **kwargs
        )
        return {
            "content": response.choices[0].message.content,
            "tokens": {"input": response.usage.prompt_tokens, "output": response.usage.completion_tokens},
            "_cached": False,
        }

cache = PromptCache(backend="redis", ttl_hours=48)
```

---

## Prompt Injection Defense

### Attack Types and Defenses

```python
from pydantic import BaseModel, Field, field_validator
import re

class InputGuardrail:
    """Detect and block prompt injection attempts."""
    
    INJECTION_PATTERNS = [
        r"ignore\s+(previous|above|all)\s+(instructions?|prompts?|rules?)",
        r"(system|new)\s+prompt:",
        r"you\s+are\s+now\s+a",
        r"forget\s+(everything|all|your)",
        r"disregard\s+(previous|all|your)",
        r"override\s+(system|instructions?|rules?)",
        r"<\|im_start\|>",  # ChatML injection
        r"\[INST\]",  # Llama format injection
        r"###\s*(System|Human|Assistant)",  # Format injection
    ]
    
    def __init__(self):
        self.compiled_patterns = [re.compile(p, re.IGNORECASE) for p in self.INJECTION_PATTERNS]
    
    def check(self, user_input: str) -> dict:
        """Check input for injection attempts."""
        detections = []
        for pattern in self.compiled_patterns:
            matches = pattern.findall(user_input)
            if matches:
                detections.append({
                    "pattern": pattern.pattern,
                    "matches": matches,
                })
        
        return {
            "is_safe": len(detections) == 0,
            "risk_score": min(len(detections) / 3, 1.0),  # 0-1 scale
            "detections": detections,
        }

class OutputGuardrail:
    """Validate LLM output doesn't leak system prompts or contain harmful content."""
    
    SYSTEM_LEAK_PATTERNS = [
        r"my\s+(instructions?|system\s+prompt|rules?)\s+(are|say|tell)",
        r"I\s+was\s+(told|instructed|programmed)\s+to",
        r"my\s+system\s+(message|prompt)\s+(is|says|contains)",
    ]
    
    def __init__(self, blocked_strings: list[str] = None):
        self.blocked = blocked_strings or []
        self.leak_patterns = [re.compile(p, re.IGNORECASE) for p in self.SYSTEM_LEAK_PATTERNS]
    
    def check(self, output: str) -> dict:
        issues = []
        
        # Check for system prompt leakage
        for pattern in self.leak_patterns:
            if pattern.search(output):
                issues.append("potential_system_leak")
        
        # Check for blocked strings (e.g., API keys, internal URLs)
        for blocked in self.blocked:
            if blocked.lower() in output.lower():
                issues.append(f"blocked_content: {blocked[:20]}...")
        
        return {"is_safe": len(issues) == 0, "issues": issues}

# Sandwich defense: wrap user input between instructions
def sandwich_defense(user_input: str, schema: str) -> list[dict]:
    """Place user input between reinforcing instructions."""
    return [
        {"role": "system", "content": f"""You are a SQL generator. ONLY generate SELECT queries.
Schema: {schema}
CRITICAL: Respond ONLY with a SQL SELECT query. No explanations, no other content."""},
        {"role": "user", "content": f"""Generate SQL for this question (respond with ONLY SQL):
---
{user_input}
---
Remember: respond with ONLY a valid SELECT query based on the schema above."""},
    ]

# Full defense pipeline
def safe_llm_call(user_input: str, schema: str) -> dict:
    """Complete defense pipeline: input check → sanitize → LLM → output check."""
    input_guard = InputGuardrail()
    output_guard = OutputGuardrail(blocked_strings=["sk-proj-", "internal.company.com"])
    
    # 1. Check input
    input_check = input_guard.check(user_input)
    if not input_check["is_safe"]:
        return {"error": "Input rejected", "reason": "potential_injection"}
    
    # 2. Call LLM with sandwich defense
    messages = sandwich_defense(user_input, schema)
    response = client.chat.completions.create(
        model="gpt-4o-mini", messages=messages, temperature=0.0
    )
    output = response.choices[0].message.content
    
    # 3. Check output
    output_check = output_guard.check(output)
    if not output_check["is_safe"]:
        return {"error": "Output blocked", "reason": output_check["issues"]}
    
    # 4. Validate it's actually SQL
    if not output.strip().upper().startswith("SELECT"):
        return {"error": "Output is not a SELECT query"}
    
    return {"sql": output, "safe": True}
```

---

## Multi-Model Routing

Route requests to different models based on complexity to optimize cost.

```python
from dataclasses import dataclass
from enum import Enum
import tiktoken

class ModelTier(Enum):
    CHEAP = "gpt-4o-mini"       # $0.15/$0.60 per 1M tokens
    STANDARD = "gpt-4o"         # $2.50/$10.00 per 1M tokens
    PREMIUM = "claude-sonnet-4-20250514"  # For reasoning-heavy tasks

@dataclass
class RoutingRule:
    condition: str  # Description for logging
    check: callable
    model_tier: ModelTier

class ModelRouter:
    """Route requests to the optimal model based on complexity and cost."""
    
    def __init__(self):
        self.encoder = tiktoken.encoding_for_model("gpt-4o")
        self.rules: list[RoutingRule] = [
            RoutingRule(
                "Simple classification/extraction",
                lambda ctx: ctx.get("task_type") in ("classify", "extract", "format"),
                ModelTier.CHEAP
            ),
            RoutingRule(
                "Long context (>4K input tokens)",
                lambda ctx: ctx.get("input_tokens", 0) > 4000,
                ModelTier.STANDARD
            ),
            RoutingRule(
                "Multi-step reasoning required",
                lambda ctx: ctx.get("task_type") in ("debug", "analyze", "optimize"),
                ModelTier.PREMIUM
            ),
            RoutingRule(
                "SQL generation with complex joins",
                lambda ctx: ctx.get("complexity_score", 0) > 0.7,
                ModelTier.STANDARD
            ),
        ]
    
    def estimate_complexity(self, messages: list[dict]) -> dict:
        """Estimate task complexity from the prompt content."""
        full_text = " ".join(m["content"] for m in messages)
        input_tokens = len(self.encoder.encode(full_text))
        
        # Heuristic complexity scoring
        complexity_indicators = [
            "join", "subquery", "window function", "recursive",
            "analyze", "debug", "optimize", "compare", "multiple"
        ]
        complexity_score = sum(
            1 for indicator in complexity_indicators 
            if indicator in full_text.lower()
        ) / len(complexity_indicators)
        
        return {
            "input_tokens": input_tokens,
            "complexity_score": complexity_score,
            "task_type": self._classify_task(full_text),
        }
    
    def _classify_task(self, text: str) -> str:
        """Quick heuristic task classification (no LLM call)."""
        text_lower = text.lower()
        if any(w in text_lower for w in ["classify", "categorize", "label"]):
            return "classify"
        if any(w in text_lower for w in ["extract", "parse", "find"]):
            return "extract"
        if any(w in text_lower for w in ["debug", "error", "fix", "why"]):
            return "debug"
        if any(w in text_lower for w in ["analyze", "compare", "evaluate"]):
            return "analyze"
        return "generate"
    
    def route(self, messages: list[dict]) -> str:
        """Determine the best model for this request."""
        context = self.estimate_complexity(messages)
        
        for rule in self.rules:
            if rule.check(context):
                return rule.model_tier.value
        
        return ModelTier.CHEAP.value  # Default to cheapest
    
    def call(self, messages: list[dict], **kwargs) -> dict:
        """Route and call the appropriate model."""
        model = self.route(messages)
        
        response = client.chat.completions.create(
            model=model, messages=messages, **kwargs
        )
        
        return {
            "content": response.choices[0].message.content,
            "model_used": model,
            "tokens": {
                "input": response.usage.prompt_tokens,
                "output": response.usage.completion_tokens,
            }
        }

router = ModelRouter()
```

---

## Prompt Versioning and Regression Testing

```python
import hashlib
import json
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class PromptVersion:
    template_name: str
    version: str
    content_hash: str
    system_prompt: str
    user_template: str
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    test_results: dict = field(default_factory=dict)

class PromptRegistry:
    """Version control for prompts with regression testing."""
    
    def __init__(self, storage_path: str = "./prompt_versions"):
        self.path = Path(storage_path)
        self.path.mkdir(parents=True, exist_ok=True)
    
    def register(self, name: str, version: str, system_prompt: str, user_template: str) -> PromptVersion:
        """Register a new prompt version."""
        content = f"{system_prompt}|{user_template}"
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
        
        pv = PromptVersion(
            template_name=name,
            version=version,
            content_hash=content_hash,
            system_prompt=system_prompt,
            user_template=user_template,
        )
        
        filepath = self.path / f"{name}_v{version}.json"
        filepath.write_text(json.dumps(pv.__dict__, indent=2))
        return pv
    
    def regression_test(self, name: str, new_version: str, 
                        test_cases: list[dict], metric_fn: callable) -> dict:
        """Run regression tests comparing new version against current production."""
        # Load current production version
        prod_files = sorted(self.path.glob(f"{name}_v*.json"))
        if not prod_files:
            return {"status": "no_baseline", "message": "No previous version to compare"}
        
        with open(prod_files[-1]) as f:
            prod_version = json.load(f)
        
        # Load new version
        new_file = self.path / f"{name}_v{new_version}.json"
        with open(new_file) as f:
            new_prompt = json.load(f)
        
        # Run test cases against both versions
        prod_scores = []
        new_scores = []
        regressions = []
        
        for case in test_cases:
            # Test production version
            prod_messages = [
                {"role": "system", "content": prod_version["system_prompt"]},
                {"role": "user", "content": case["input"]}
            ]
            prod_result = client.chat.completions.create(
                model="gpt-4o-mini", messages=prod_messages, temperature=0.0
            )
            prod_score = metric_fn(case["expected"], prod_result.choices[0].message.content)
            prod_scores.append(prod_score)
            
            # Test new version
            new_messages = [
                {"role": "system", "content": new_prompt["system_prompt"]},
                {"role": "user", "content": case["input"]}
            ]
            new_result = client.chat.completions.create(
                model="gpt-4o-mini", messages=new_messages, temperature=0.0
            )
            new_score = metric_fn(case["expected"], new_result.choices[0].message.content)
            new_scores.append(new_score)
            
            if new_score < prod_score:
                regressions.append({
                    "case": case["input"][:100],
                    "prod_score": prod_score,
                    "new_score": new_score,
                })
        
        avg_prod = sum(prod_scores) / len(prod_scores)
        avg_new = sum(new_scores) / len(new_scores)
        
        return {
            "production_avg_score": avg_prod,
            "new_avg_score": avg_new,
            "improvement": avg_new - avg_prod,
            "regressions": regressions,
            "passed": avg_new >= avg_prod and len(regressions) <= len(test_cases) * 0.1,
        }
```

---

## Cost Optimization Strategies

```python
@dataclass
class CostTracker:
    """Track and optimize LLM costs across a pipeline."""
    
    pricing: dict = field(default_factory=lambda: {
        "gpt-4o": {"input": 2.50, "output": 10.00},          # per 1M tokens
        "gpt-4o-mini": {"input": 0.15, "output": 0.60},      # per 1M tokens
    })
    daily_budget: float = 100.0  # USD
    _spend: float = 0.0
    _calls: int = 0
    
    def record(self, model: str, input_tokens: int, output_tokens: int):
        rates = self.pricing.get(model, self.pricing["gpt-4o-mini"])
        cost = (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1_000_000
        self._spend += cost
        self._calls += 1
        
        if self._spend > self.daily_budget * 0.8:
            print(f"WARNING: 80% of daily budget consumed (${self._spend:.2f}/${self.daily_budget})")
    
    @property
    def summary(self) -> dict:
        return {
            "total_spend": f"${self._spend:.4f}",
            "total_calls": self._calls,
            "avg_cost_per_call": f"${self._spend / max(self._calls, 1):.6f}",
            "budget_remaining": f"${self.daily_budget - self._spend:.2f}",
        }

# Cost optimization techniques
OPTIMIZATION_STRATEGIES = """
1. Batch processing: Send multiple records per call (reduces per-call overhead)
2. Model routing: Use gpt-4o-mini for 80% of tasks, gpt-4o for complex ones
3. Prompt caching: Cache deterministic calls (temperature=0)
4. Prompt compression: Remove verbose instructions, use concise schemas
5. Output limiting: Set max_tokens to expected output size
6. Pre-filtering: Use regex/rules to skip records that don't need LLM
7. Progressive refinement: Start with cheap model, escalate failures to expensive one
"""
```

---

## Interview Tips

> **Tip 1:** DSPy shows you understand that prompt engineering can be automated. Mention "learned prompts" vs "hand-crafted prompts" to demonstrate senior thinking.

> **Tip 2:** For injection defense, always mention the layered approach: input validation → sandwich defense → output validation. No single defense is sufficient.

> **Tip 3:** Cost optimization is a senior concern. Know the math: 10M calls × 500 input tokens × $2.50/1M = $12,500/month on gpt-4o vs $750/month on gpt-4o-mini.

> **Tip 4:** Prompt versioning + regression testing shows production maturity. Compare to how you'd version and test any other code change.
