---
title: "Mosaic AI - Senior Deep Dive"
topic: databricks
subtopic: mosaic-ai
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, mosaic-ai, llm-ops, agent-framework, evaluation, guardrails, production-ai]
---

# Mosaic AI — Senior Deep Dive

## Production LLM System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Mosaic AI Platform                         │
│                                                              │
│  Data Layer (Delta Lakehouse)                                │
│    Documents → Vector Search Index (auto-embedded)           │
│                     ↓                                        │
│  AI Layer                                                    │
│    AI Gateway ←→ Foundation Model APIs (DBRX, Llama)        │
│    (rate limits, logging, routing)                           │
│                     ↓                                        │
│  Application Layer                                           │
│    Agent Framework (multi-step reasoning + tool calls)       │
│    Model Serving (custom fine-tuned models)                  │
│                     ↓                                        │
│  Observability Layer                                         │
│    MLflow Tracing → Lakehouse Monitoring → Alerts            │
└──────────────────────────────────────────────────────────────┘
```

---

## LLM Evaluation Framework

Evaluating LLM outputs is non-trivial — traditional ML metrics don't apply:

```python
import mlflow
import pandas as pd

# Define evaluation dataset
eval_data = pd.DataFrame({
    "inputs": [
        "What is the return policy for electronics?",
        "How do I track my order?",
        "Can I return a used item?"
    ],
    "ground_truth": [
        "Electronics can be returned within 30 days with receipt.",
        "Use the tracking link in your confirmation email.",
        "Used items are not eligible for return unless defective."
    ]
})

# Run evaluation with MLflow
with mlflow.start_run():
    results = mlflow.evaluate(
        model=rag_pipeline_function,     # your RAG function
        data=eval_data,
        targets="ground_truth",
        model_type="question-answering",
        evaluators="default",
        evaluator_config={
            "col_mapping": {"inputs": "inputs"},
            "judged_by": "databricks-dbrx-instruct",  # LLM-as-judge
        }
    )

    # Metrics logged automatically:
    # - answer_similarity (0-5 scale via LLM judge)
    # - faithfulness (does answer stick to context?)
    # - relevance (is context relevant to question?)
    print(results.metrics)
```

**LLM-as-judge pattern:** Use a stronger model (DBRX, GPT-4) to evaluate outputs from a weaker model. Score dimensions: correctness, helpfulness, harmlessness, groundedness.

---

## Guardrails and Safety

LLMs in production need safety layers:

```python
from databricks.sdk import WorkspaceClient

# Configure safety filters on a serving endpoint
w = WorkspaceClient()

# Using Lakehouse Monitoring for content safety
w.serving_endpoints.patch(
    name="customer-support-bot",
    config={
        "guardrails": {
            "input": {
                "pii": {"behavior": "BLOCK"},       # block if PII detected
                "safety": {"behavior": "BLOCK"},    # block harmful inputs
                "valid_topics": {
                    "behavior": "BLOCK",
                    "topics": ["order support", "product questions", "returns"]
                    # block off-topic: "help me write malware"
                }
            },
            "output": {
                "pii": {"behavior": "REDACT"},      # redact PII in responses
                "safety": {"behavior": "BLOCK"}
            }
        }
    }
)
```

**Custom guardrail with prompt shield:**
```python
def check_prompt_injection(user_input: str) -> bool:
    """Detect prompt injection attempts."""
    injection_patterns = [
        "ignore previous instructions",
        "you are now",
        "disregard your system prompt",
        "pretend you are"
    ]
    lower_input = user_input.lower()
    return any(p in lower_input for p in injection_patterns)

def safe_rag_pipeline(user_question: str) -> str:
    if check_prompt_injection(user_question):
        return "I can only answer questions about our products and policies."

    # Sanitize: truncate to prevent token flooding
    safe_question = user_question[:1000]

    # Proceed with RAG
    return rag_answer(safe_question)
```

---

## Multi-Agent Orchestration

Complex tasks require multiple specialized agents:

```python
from databricks.sdk import WorkspaceClient
import mlflow

class DataAnalystAgent:
    """Agent specialized in SQL and data analysis."""

    def __init__(self):
        self.client = mlflow.deployments.get_deploy_client("databricks")

    @mlflow.trace(span_type="AGENT")
    def analyze(self, question: str) -> dict:
        system_prompt = (
            "You are a data analyst. Write SQL to answer business questions. "
            "Available tables: prod.sales.orders, prod.customers.profiles. "
            "Return valid SQL only, no explanation."
        )
        response = self.client.predict(
            endpoint="databricks-dbrx-instruct",
            inputs={"messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ]}
        )
        sql = response["choices"][0]["message"]["content"]
        result = spark.sql(sql).limit(100).toPandas()
        return {"sql": sql, "result": result.to_dict()}


class ReportWriterAgent:
    """Agent specialized in writing business reports."""

    @mlflow.trace(span_type="AGENT")
    def write_report(self, data: dict, audience: str = "executives") -> str:
        context = f"Data: {data['result']}\nSQL used: {data['sql']}"
        response = self.client.predict(
            endpoint="databricks-dbrx-instruct",
            inputs={"messages": [
                {"role": "system", "content": f"Write a concise business report for {audience}."},
                {"role": "user", "content": context}
            ]}
        )
        return response["choices"][0]["message"]["content"]


# Orchestrator
def run_report_pipeline(business_question: str) -> str:
    with mlflow.start_span(name="report-pipeline") as span:
        span.set_inputs({"question": business_question})

        analyst = DataAnalystAgent()
        writer = ReportWriterAgent()

        data = analyst.analyze(business_question)
        report = writer.write_report(data)

        span.set_outputs({"report": report})
        return report
```

---

## Cost Optimization for LLM Workloads

```python
# Pattern 1: Semantic cache — skip LLM call if similar question was asked recently
from databricks.vector_search.client import VectorSearchClient

def semantic_cache_lookup(question: str, similarity_threshold: float = 0.95) -> str | None:
    """Return cached answer if semantically similar question was asked."""
    cache_index = vsc.get_index("prod.ai_cache.qa_cache")
    results = cache_index.similarity_search(
        query_text=question,
        columns=["question", "answer", "score"],
        num_results=1,
        filters={"score": {"$gte": similarity_threshold}}
    )

    if results["result"]["data_array"]:
        return results["result"]["data_array"][0]["answer"]
    return None

def cached_rag_answer(question: str) -> str:
    cached = semantic_cache_lookup(question)
    if cached:
        return cached   # No LLM call — instant + free

    answer = rag_answer(question)   # LLM call
    # Store in cache for future similar questions
    cache_df = spark.createDataFrame([{"question": question, "answer": answer}])
    fs.write_table("prod.ai_cache.qa_cache", cache_df, mode="merge")
    return answer

# Pattern 2: Model routing — use cheap model for simple queries
def route_to_model(question: str) -> str:
    complexity_check = classify_complexity(question)  # small model classifier
    if complexity_check == "simple":
        return "databricks-mistral-7b-instruct"   # cheap, fast
    else:
        return "databricks-dbrx-instruct"          # powerful, expensive
```

---

## Interview Tips

> **Tip 1:** "How do you evaluate a RAG pipeline in production?" — "Two levels: offline (before deployment) and online (in production). Offline: use MLflow evaluate with an LLM-as-judge scoring faithfulness (does the answer use the retrieved context?), relevance (is the context relevant?), and correctness vs ground truth. Online: A/B test RAG vs no-RAG, measure task completion rate, user thumbs up/down, and downstream business metrics (e.g., support ticket deflection rate)."

> **Tip 2:** "What guardrails do you need for a customer-facing LLM?" — "Input guardrails: PII detection (don't let users exfiltrate data), prompt injection detection (prevent jailbreaks), topic validation (block off-topic requests). Output guardrails: PII redaction (don't return customer PII in answers), safety filtering (no harmful content), factual grounding check (answer must be supported by retrieved context). Databricks has built-in guardrails config on serving endpoints."

> **Tip 3:** "How would you reduce LLM cost at scale?" — "Three levers: (1) Semantic caching — use Vector Search to check if a semantically similar question was answered recently; return cached answer. (2) Model routing — classify query complexity and route simple queries to a cheap small model (Mistral-7B), complex ones to DBRX or Llama-70B. (3) Incremental processing — only call LLM on new records, cache results in a Delta table keyed by input hash."
