---
title: "Prompt Engineering - Intermediate"
topic: rag-llm
subtopic: prompt-engineering
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [prompt-engineering, react, tree-of-thought, prompt-chaining, guardrails, instructor, pydantic]
---

# Prompt Engineering — Intermediate

## Beyond Basic Prompts

At this level, you're building **multi-step LLM workflows** that handle edge cases, validate outputs, and chain multiple calls together. This is where prompt engineering becomes software engineering.

---

## Advanced Prompting Techniques

### ReAct (Reasoning + Acting)

ReAct combines reasoning traces with action steps. The model thinks about what to do, takes an action, observes the result, then reasons again.

```python
from openai import OpenAI
import json

client = OpenAI()

REACT_SYSTEM_PROMPT = """You are a data pipeline debugger. Use the ReAct framework:

Thought: Analyze what you know and what you need to find out
Action: Choose one of the available actions
Observation: Process the result
... repeat until you can provide a final answer

Available actions:
- query_metrics(metric_name, time_range) - Get pipeline metrics
- check_logs(service, severity, time_range) - Check service logs  
- get_schema(table_name) - Get table schema
- run_query(sql) - Run a diagnostic SQL query

When you have enough information, respond with:
Final Answer: {your conclusion and recommended fix}

Respond in this exact format for each step."""

def react_agent(question: str, tool_executor) -> str:
    """Run a ReAct loop for pipeline debugging."""
    messages = [
        {"role": "system", "content": REACT_SYSTEM_PROMPT},
        {"role": "user", "content": question}
    ]
    
    max_iterations = 5
    for i in range(max_iterations):
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.0,
        )
        
        reply = response.choices[0].message.content
        messages.append({"role": "assistant", "content": reply})
        
        if "Final Answer:" in reply:
            return reply.split("Final Answer:")[-1].strip()
        
        # Parse and execute action
        if "Action:" in reply:
            action_line = [l for l in reply.split("\n") if l.startswith("Action:")][0]
            result = tool_executor(action_line)
            messages.append({"role": "user", "content": f"Observation: {result}"})
    
    return "Max iterations reached without conclusion."
```

### Tree of Thought (ToT)

Generate multiple reasoning paths and evaluate which is best. Useful for complex data modeling decisions.

```python
def tree_of_thought_analysis(problem: str, num_paths: int = 3) -> dict:
    """Generate multiple solution paths and evaluate them."""
    
    # Step 1: Generate diverse approaches
    generation_prompt = f"""Given this data engineering problem:
{problem}

Generate {num_paths} fundamentally different approaches to solve it.
For each approach, provide:
- Name: short label
- Strategy: 2-3 sentence description
- Tradeoffs: one pro and one con

Respond as JSON array."""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": generation_prompt}],
        response_format={"type": "json_object"},
        temperature=0.7,  # Higher temp for diverse ideas
    )
    approaches = json.loads(response.choices[0].message.content)
    
    # Step 2: Evaluate each approach
    eval_prompt = f"""Problem: {problem}

Evaluate these approaches on a 1-10 scale for:
- Scalability (handles 10x growth)
- Maintainability (easy to debug/modify)
- Cost efficiency (compute + storage)
- Implementation speed (time to production)

Approaches: {json.dumps(approaches)}

Respond with JSON: {{"evaluations": [...], "recommendation": "..."}}"""

    eval_response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": eval_prompt}],
        response_format={"type": "json_object"},
        temperature=0.0,  # Deterministic for evaluation
    )
    
    return json.loads(eval_response.choices[0].message.content)
```

### Self-Consistency

Run the same prompt multiple times with higher temperature and take the majority answer. Reduces hallucination for critical decisions.

```python
def self_consistent_classify(text: str, categories: list[str], n_samples: int = 5) -> str:
    """Run classification multiple times and take majority vote."""
    from collections import Counter
    
    messages = [
        {"role": "system", "content": f"Classify the text into one of: {categories}. Respond with just the category name."},
        {"role": "user", "content": text}
    ]
    
    results = []
    for _ in range(n_samples):
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,  # Allow variation between samples
            max_tokens=20,
        )
        results.append(response.choices[0].message.content.strip())
    
    # Majority vote
    counter = Counter(results)
    winner, count = counter.most_common(1)[0]
    confidence = count / n_samples
    
    return {"classification": winner, "confidence": confidence, "all_votes": dict(counter)}
```

---

## Prompt Templates with Variables

Production prompts need to be reusable. Use template systems to inject variables dynamically.

```python
from string import Template
from dataclasses import dataclass
from typing import Any

@dataclass
class PromptTemplate:
    """Reusable prompt template with variable injection."""
    name: str
    version: str
    system_template: str
    user_template: str
    required_vars: list[str]
    
    def render(self, **kwargs) -> list[dict]:
        """Render template with variables, validating all required vars are provided."""
        missing = set(self.required_vars) - set(kwargs.keys())
        if missing:
            raise ValueError(f"Missing required variables: {missing}")
        
        return [
            {"role": "system", "content": Template(self.system_template).safe_substitute(**kwargs)},
            {"role": "user", "content": Template(self.user_template).safe_substitute(**kwargs)},
        ]

# Define reusable templates
SQL_GENERATION_TEMPLATE = PromptTemplate(
    name="text_to_sql",
    version="2.1",
    system_template="""You are a $dialect SQL expert. Generate queries for the following schema:
$schema

Rules:
- Use only tables and columns defined in the schema
- Add appropriate WHERE clauses for performance
- Use CTEs for complex queries
- Respond with ONLY the SQL query, no explanation""",
    user_template="Convert to SQL: $question",
    required_vars=["dialect", "schema", "question"]
)

DATA_QUALITY_TEMPLATE = PromptTemplate(
    name="data_quality_check",
    version="1.3",
    system_template="""You are a data quality analyst checking $table_name.
Schema: $schema
Business rules: $rules
Respond only with valid JSON.""",
    user_template="Check these records for issues:\n$records",
    required_vars=["table_name", "schema", "rules", "records"]
)

# Usage
messages = SQL_GENERATION_TEMPLATE.render(
    dialect="PostgreSQL",
    schema="orders(id, customer_id, amount, status, created_at)\ncustomers(id, name, tier)",
    question="Get monthly revenue by customer tier for 2024"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=messages,
    temperature=0.0,
)
```

---

## Guardrails and Output Validation

Never trust raw LLM output in a pipeline. Always validate.

### Using Instructor (Pydantic + OpenAI)

```python
import instructor
from pydantic import BaseModel, Field, field_validator
from typing import Literal
from openai import OpenAI

# Patch OpenAI client with instructor
client = instructor.from_openai(OpenAI())

class SQLQuery(BaseModel):
    """Validated SQL output from LLM."""
    query: str = Field(description="The generated SQL query")
    tables_used: list[str] = Field(description="Tables referenced in the query")
    estimated_complexity: Literal["simple", "moderate", "complex"]
    
    @field_validator("query")
    @classmethod
    def validate_sql(cls, v: str) -> str:
        # Basic SQL injection prevention
        dangerous_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE"]
        upper_v = v.upper()
        for keyword in dangerous_keywords:
            if keyword in upper_v and "SELECT" not in upper_v.split(keyword)[0]:
                raise ValueError(f"Dangerous SQL keyword detected: {keyword}")
        if not v.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")
        return v

# Instructor automatically retries with validation errors as feedback
result = client.chat.completions.create(
    model="gpt-4o",
    response_model=SQLQuery,
    max_retries=3,  # Auto-retry on validation failure
    messages=[
        {"role": "system", "content": "Generate read-only SQL for the given schema."},
        {"role": "user", "content": "Schema: orders(id, amount, date)\nQuestion: total revenue by month"}
    ],
)

print(result.query)           # Validated SQL
print(result.tables_used)     # ["orders"]
print(result.estimated_complexity)  # "simple"
```

### Custom Guardrails

```python
from pydantic import BaseModel, Field, model_validator
import re

class PipelineRecommendation(BaseModel):
    """LLM recommendation with built-in guardrails."""
    action: Literal["scale_up", "scale_down", "add_partition", "optimize_query", "no_action"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=20, max_length=500)
    estimated_impact: str
    requires_downtime: bool
    
    @model_validator(mode="after")
    def validate_logic(self):
        # High-impact actions need high confidence
        if self.action in ("scale_down",) and self.confidence < 0.8:
            raise ValueError("Scale-down actions require confidence >= 0.8")
        # Downtime actions need explicit reasoning about timing
        if self.requires_downtime and "window" not in self.reasoning.lower():
            raise ValueError("Downtime actions must mention maintenance window in reasoning")
        return self
```

---

## Prompt Chaining for Complex Tasks

Break complex problems into sequential LLM calls where each output feeds into the next.

```python
from dataclasses import dataclass
from typing import Callable

@dataclass
class ChainStep:
    name: str
    prompt_template: PromptTemplate
    post_processor: Callable | None = None

class PromptChain:
    """Execute a sequence of LLM calls, passing context between steps."""
    
    def __init__(self, steps: list[ChainStep]):
        self.steps = steps
        self.results = {}
    
    def execute(self, initial_context: dict) -> dict:
        context = initial_context.copy()
        
        for step in self.steps:
            messages = step.prompt_template.render(**context)
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            
            result = json.loads(response.choices[0].message.content)
            
            if step.post_processor:
                result = step.post_processor(result)
            
            self.results[step.name] = result
            context.update(result)  # Pass results to next step
        
        return self.results

# Example: Schema analysis → SQL generation → Optimization
schema_analysis_template = PromptTemplate(
    name="analyze_schema",
    version="1.0",
    system_template="Analyze this database schema and identify: tables, relationships, indexes.",
    user_template="Schema DDL:\n$schema_ddl",
    required_vars=["schema_ddl"]
)

sql_gen_template = PromptTemplate(
    name="generate_sql",
    version="1.0",
    system_template="Generate SQL using this schema analysis: $schema_analysis",
    user_template="Question: $question",
    required_vars=["schema_analysis", "question"]
)

optimize_template = PromptTemplate(
    name="optimize",
    version="1.0",
    system_template="Optimize this SQL query. Schema info: $schema_analysis",
    user_template="Original query:\n$query\n\nSuggest optimizations as JSON.",
    required_vars=["schema_analysis", "query"]
)

chain = PromptChain([
    ChainStep("analysis", schema_analysis_template),
    ChainStep("generation", sql_gen_template),
    ChainStep("optimization", optimize_template),
])
```

---

## Few-Shot Example Selection Strategies

Not all examples are equally useful. Choose them strategically.

```python
import numpy as np
from typing import Any

class FewShotSelector:
    """Select the most relevant examples for a given input."""
    
    def __init__(self, examples: list[dict], embedding_model: str = "text-embedding-3-small"):
        self.examples = examples
        self.embedding_model = embedding_model
        self._embeddings = None
    
    def _get_embedding(self, text: str) -> list[float]:
        response = client.embeddings.create(model=self.embedding_model, input=text)
        return response.data[0].embedding
    
    def _compute_embeddings(self):
        """Pre-compute embeddings for all examples."""
        if self._embeddings is None:
            texts = [ex["input"] for ex in self.examples]
            response = client.embeddings.create(model=self.embedding_model, input=texts)
            self._embeddings = [d.embedding for d in response.data]
    
    def select_similar(self, query: str, k: int = 3) -> list[dict]:
        """Select k most similar examples to the query (semantic similarity)."""
        self._compute_embeddings()
        query_emb = self._get_embedding(query)
        
        # Cosine similarity
        similarities = [
            np.dot(query_emb, ex_emb) / (np.linalg.norm(query_emb) * np.linalg.norm(ex_emb))
            for ex_emb in self._embeddings
        ]
        
        top_k_indices = np.argsort(similarities)[-k:][::-1]
        return [self.examples[i] for i in top_k_indices]
    
    def select_diverse(self, query: str, k: int = 3) -> list[dict]:
        """Select examples that are similar to query but diverse from each other."""
        similar = self.select_similar(query, k=k*2)  # Get more candidates
        selected = [similar[0]]  # Start with most similar
        
        for candidate in similar[1:]:
            if len(selected) >= k:
                break
            # Only add if different enough from already selected
            is_diverse = all(
                candidate["input"][:50] != s["input"][:50] 
                for s in selected
            )
            if is_diverse:
                selected.append(candidate)
        
        return selected

# Usage
examples = [
    {"input": "total sales by region", "output": "SELECT region, SUM(amount) FROM orders GROUP BY region"},
    {"input": "customers who haven't ordered in 30 days", "output": "SELECT * FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE date > CURRENT_DATE - 30)"},
    {"input": "average order value by month", "output": "SELECT DATE_TRUNC('month', date), AVG(amount) FROM orders GROUP BY 1"},
    # ... more examples
]

selector = FewShotSelector(examples)
relevant_examples = selector.select_similar("revenue by product category last quarter", k=3)
```

---

## Handling Edge Cases

### Hallucination Detection

```python
def detect_hallucination(query: str, response: str, context: str) -> dict:
    """Check if the LLM response contains information not in the provided context."""
    
    check_prompt = f"""Compare the response to the provided context.
Identify any claims in the response that are NOT supported by the context.

Context:
{context}

Response to check:
{response}

For each unsupported claim, provide:
- claim: the specific text
- severity: "fabricated" (completely made up) or "extrapolated" (reasonable inference but not stated)

Respond as JSON: {{"has_hallucinations": bool, "unsupported_claims": [...]}}"""

    result = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": check_prompt}],
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    
    return json.loads(result.choices[0].message.content)
```

### Handling Refusals and Off-Topic Responses

```python
def robust_llm_call(messages: list[dict], max_retries: int = 3, 
                     expected_format: str = "json") -> dict | None:
    """LLM call with retry logic for common failure modes."""
    
    refusal_patterns = [
        "I cannot", "I'm unable to", "I don't have access",
        "As an AI", "I apologize", "I'm not able to"
    ]
    
    for attempt in range(max_retries):
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.0,
            response_format={"type": "json_object"} if expected_format == "json" else None,
        )
        
        content = response.choices[0].message.content
        
        # Check for refusal
        if any(pattern.lower() in content.lower() for pattern in refusal_patterns):
            # Rephrase and retry
            messages.append({"role": "assistant", "content": content})
            messages.append({"role": "user", "content": 
                "Please complete the task as instructed. This is for automated data processing, not harmful use."})
            continue
        
        # Validate format
        if expected_format == "json":
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                messages.append({"role": "assistant", "content": content})
                messages.append({"role": "user", "content": 
                    "Your response was not valid JSON. Please respond with ONLY valid JSON."})
                continue
        
        return {"content": content}
    
    return None  # All retries exhausted
```

---

## A/B Testing Prompts

```python
import hashlib
import time
from dataclasses import dataclass, field

@dataclass
class PromptVariant:
    name: str
    template: PromptTemplate
    metrics: dict = field(default_factory=lambda: {
        "calls": 0, "successes": 0, "avg_latency_ms": 0.0, "total_tokens": 0
    })

class PromptABTest:
    """A/B test different prompt variants and track performance."""
    
    def __init__(self, variants: list[PromptVariant], split_key: str = "record_id"):
        self.variants = variants
        self.split_key = split_key
    
    def _assign_variant(self, record: dict) -> PromptVariant:
        """Deterministic assignment based on record key (consistent hashing)."""
        key = str(record.get(self.split_key, id(record)))
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        idx = hash_val % len(self.variants)
        return self.variants[idx]
    
    def run(self, record: dict, context: dict) -> tuple[dict, str]:
        """Run the assigned variant and track metrics."""
        variant = self._assign_variant(record)
        
        start = time.time()
        messages = variant.template.render(**context)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        
        latency = (time.time() - start) * 1000
        tokens = response.usage.total_tokens
        content = response.choices[0].message.content
        
        # Update metrics
        variant.metrics["calls"] += 1
        variant.metrics["total_tokens"] += tokens
        variant.metrics["avg_latency_ms"] = (
            (variant.metrics["avg_latency_ms"] * (variant.metrics["calls"] - 1) + latency)
            / variant.metrics["calls"]
        )
        
        try:
            result = json.loads(content)
            variant.metrics["successes"] += 1
            return result, variant.name
        except json.JSONDecodeError:
            return None, variant.name
    
    def report(self) -> dict:
        """Generate A/B test performance report."""
        return {
            v.name: {
                **v.metrics,
                "success_rate": v.metrics["successes"] / max(v.metrics["calls"], 1),
                "avg_tokens_per_call": v.metrics["total_tokens"] / max(v.metrics["calls"], 1),
            }
            for v in self.variants
        }
```

---

## Interview Tips

> **Tip 1:** When asked about production prompts, always mention: templates (reusability), validation (Pydantic/instructor), retry logic (handle failures), and observability (log prompts + responses for debugging).

> **Tip 2:** ReAct is the pattern behind LangChain agents. Understanding it means you can build agents without the framework overhead. Interviewers love this.

> **Tip 3:** For "how would you handle hallucinations?" — answer with a layered approach: constrained output (JSON mode), schema validation (Pydantic), and semantic checks (compare response to source context).

> **Tip 4:** A/B testing prompts shows production maturity. Mention deterministic assignment (consistent hashing) so the same record always hits the same variant.
