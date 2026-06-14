---
title: "Mosaic AI - Intermediate"
topic: databricks
subtopic: mosaic-ai
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, mosaic-ai, rag, llm-fine-tuning, ai-gateway, chain, agent]
---

# Mosaic AI — Intermediate Concepts

## Building RAG Pipelines with Vector Search

Retrieval-Augmented Generation (RAG) in Databricks uses Vector Search + Foundation Model APIs:

```python
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

vsc = VectorSearchClient()
deploy_client = mlflow.deployments.get_deploy_client("databricks")

def rag_answer(question: str, top_k: int = 3) -> str:
    # Step 1: Retrieve relevant chunks
    results = vsc.get_index("prod.docs.policy_index").similarity_search(
        query_text=question,
        columns=["title", "content", "source_url"],
        num_results=top_k
    )

    # Step 2: Build context from retrieved chunks
    context_chunks = [r["content"] for r in results["result"]["data_array"]]
    context = "\n\n---\n\n".join(context_chunks)

    # Step 3: Generate answer
    response = deploy_client.predict(
        endpoint="databricks-dbrx-instruct",
        inputs={
            "messages": [
                {"role": "system", "content":
                    "Answer the question using ONLY the provided context. "
                    "If the answer is not in the context, say 'I don't know'."},
                {"role": "user", "content":
                    f"Context:\n{context}\n\nQuestion: {question}"}
            ],
            "max_tokens": 500,
            "temperature": 0.0
        }
    )
    return response["choices"][0]["message"]["content"]

# Usage
answer = rag_answer("What is the return policy for electronics?")
```

---

## Mosaic AI Agent Framework

Build multi-step agents that can call tools, query databases, and use RAG:

```python
import mlflow
from databricks.sdk import WorkspaceClient

# Define tools the agent can call
@mlflow.trace(span_type="TOOL")
def query_sales_data(sql_query: str) -> str:
    """Execute a SQL query and return results as string."""
    result = spark.sql(sql_query).limit(100).toPandas()
    return result.to_string()

@mlflow.trace(span_type="TOOL")
def search_documentation(question: str) -> str:
    """Search internal documentation using RAG."""
    return rag_answer(question)

# Define the agent
tools = [
    {
        "type": "function",
        "function": {
            "name": "query_sales_data",
            "description": "Query the sales database with SQL",
            "parameters": {
                "type": "object",
                "properties": {"sql_query": {"type": "string"}},
                "required": ["sql_query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_documentation",
            "description": "Search internal docs and policies",
            "parameters": {
                "type": "object",
                "properties": {"question": {"type": "string"}},
                "required": ["question"]
            }
        }
    }
]

# Agent loop
def run_agent(user_message: str, max_iterations: int = 5) -> str:
    messages = [
        {"role": "system", "content": "You are a data analyst assistant. Use tools to answer questions."},
        {"role": "user", "content": user_message}
    ]

    for _ in range(max_iterations):
        response = deploy_client.predict(
            endpoint="databricks-meta-llama-3-1-70b-instruct",
            inputs={"messages": messages, "tools": tools, "max_tokens": 1000}
        )

        msg = response["choices"][0]["message"]
        messages.append(msg)

        if msg.get("tool_calls"):
            for tool_call in msg["tool_calls"]:
                fn_name = tool_call["function"]["name"]
                fn_args = json.loads(tool_call["function"]["arguments"])

                if fn_name == "query_sales_data":
                    result = query_sales_data(**fn_args)
                elif fn_name == "search_documentation":
                    result = search_documentation(**fn_args)

                messages.append({
                    "role": "tool",
                    "content": result,
                    "tool_call_id": tool_call["id"]
                })
        else:
            return msg["content"]

    return "Max iterations reached"
```

---

## LLM Fine-Tuning on Databricks

Fine-tune foundation models on your proprietary data using Mosaic AI:

```python
# Prepare training data in chat format
training_data = spark.createDataFrame([
    {
        "messages": [
            {"role": "user", "content": "Classify this ticket: My payment failed"},
            {"role": "assistant", "content": "billing"}
        ]
    },
    {
        "messages": [
            {"role": "user", "content": "Classify this ticket: App crashes on login"},
            {"role": "assistant", "content": "technical"}
        ]
    }
])

training_data.write.format("delta").saveAsTable("ml.finetune.training_data")

# Create fine-tuning run via SDK
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

run = w.fine_tuning.runs.create(
    custom_weights_path="dbfs:/models/fine-tuned/support-classifier",
    training_data_path="ml.finetune.training_data",
    eval_data_path="ml.finetune.eval_data",
    model="meta-llama/Meta-Llama-3-8B-Instruct",
    train_data_percentage=1.0,
    num_epochs=3,
    registered_model_name="support-ticket-classifier"
)

print(f"Fine-tuning job: {run.id}")
```

---

## AI Gateway Configuration

Centralize and govern all LLM calls:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Create an AI Gateway route
w.serving_endpoints.create(
    name="company-llm-gateway",
    config={
        "served_entities": [{
            "external_model": {
                "name": "gpt-4o",
                "provider": "openai",
                "openai_config": {
                    "openai_api_key": "{{secrets/openai/api-key}}"
                },
                "task": "llm/v1/chat"
            }
        }],
        "rate_limits": [
            {"calls": 1000, "renewal_period": "minute", "key": "endpoint"}
        ],
        "auto_capture_config": {
            "catalog_name": "prod",
            "schema_name": "ai_logs",
            "table_name_prefix": "llm_gateway"
        }
    }
)
# All calls now go through gateway — logged, rate-limited, auditable
```

---

## MLflow Tracing for LLM Observability

```python
import mlflow

mlflow.set_experiment("/ai-observability/rag-pipeline")

with mlflow.start_span(name="rag-pipeline") as span:
    span.set_inputs({"question": question})

    with mlflow.start_span(name="vector-search"):
        chunks = vsc.get_index("policy_index").similarity_search(
            query_text=question, num_results=3
        )

    with mlflow.start_span(name="llm-completion"):
        answer = deploy_client.predict(
            endpoint="databricks-dbrx-instruct",
            inputs={"messages": [...], "max_tokens": 500}
        )

    span.set_outputs({"answer": answer["choices"][0]["message"]["content"]})

# Traces visible in MLflow UI — latency breakdown, token counts, errors
```

---

## Interview Tips

> **Tip 1:** "How would you build a RAG chatbot on Databricks?" — "Three steps: (1) Create a Vector Search index on the document Delta table — Databricks handles chunking, embedding, and indexing automatically. (2) At query time, call `similarity_search` to get top-k relevant chunks. (3) Pass chunks as context to a Foundation Model API call (DBRX or Llama). Log everything with MLflow Tracing for observability."

> **Tip 2:** "When would you fine-tune vs use prompt engineering?" — "Prompt engineering is free and immediate — start there. Fine-tune when: (1) prompt engineering can't achieve the accuracy you need on a specialized domain, (2) you have 100+ labeled examples, and (3) you need lower latency or cost (fine-tuned smaller models often beat larger base models). Fine-tuning requires labeled data, training compute, and ongoing maintenance."

> **Tip 3:** "What is the AI Gateway and how does it help with cost control?" — "AI Gateway is a central proxy for LLM calls. It adds rate limits (calls/minute per user or endpoint), centralized logging (every call logged to a Delta table with token counts), and provider abstraction. For cost: alert when daily token usage exceeds budget using the auto-captured log table."
