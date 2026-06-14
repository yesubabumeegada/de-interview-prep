---
title: "Mosaic AI - Fundamentals"
topic: databricks
subtopic: mosaic-ai
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, mosaic-ai, llm, model-serving, vector-search, foundation-models]
---

# Mosaic AI — Fundamentals

## 🎯 Analogy

Mosaic AI is Databricks' AI platform — their answer to what Snowflake Cortex is for Snowflake users. It's a suite of tools for serving, fine-tuning, and building AI applications, all within the Databricks Lakehouse. Think of it as a complete AI factory: raw data goes in, production AI applications come out.

---

## What Is Mosaic AI?

Mosaic AI (formerly Databricks AI / MosaicML after acquisition) is Databricks' unified AI platform:

| Component | Purpose |
|-----------|---------|
| **Foundation Model APIs** | Call hosted LLMs (DBRX, Llama, Mistral) via SQL or Python |
| **Model Serving** | Deploy any model (MLflow, custom, or external) as a REST endpoint |
| **Vector Search** | Managed vector database for semantic search and RAG |
| **AI Playground** | Interactive LLM testing and prompt engineering UI |
| **AI Gateway** | Central proxy for all LLM calls with rate limiting, logging, cost tracking |
| **Mosaic AI Agent Framework** | Build multi-step AI agents with tool calling |

---

## Foundation Model APIs

Call LLMs directly from Databricks without managing infrastructure:

```python
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

# Chat completion
response = client.predict(
    endpoint="databricks-dbrx-instruct",   # hosted by Databricks
    inputs={
        "messages": [
            {"role": "system", "content": "You are a helpful data assistant."},
            {"role": "user", "content": "Explain what a Delta table is in 2 sentences."}
        ],
        "max_tokens": 200,
        "temperature": 0.1
    }
)
print(response["choices"][0]["message"]["content"])
```

**Available hosted models:**
- `databricks-dbrx-instruct` — Databricks' open model (132B MoE)
- `databricks-meta-llama-3-1-70b-instruct` — Meta Llama 3.1 70B
- `databricks-mistral-7b-instruct` — Mistral 7B
- `databricks-bge-large-en` — Embeddings model

```sql
-- Also callable from SQL
SELECT ai_query(
    'databricks-dbrx-instruct',
    CONCAT('Classify this ticket as billing/technical/other: ', ticket_body)
) AS category
FROM support_tickets
LIMIT 100;
```

---

## Model Serving Endpoints

Deploy any MLflow model as a production REST API:

```python
import requests

# Create a serving endpoint
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.serving_endpoints.create(
    name="fraud-detection-v2",
    config={
        "served_models": [{
            "model_name": "fraud-detection-classifier",
            "model_version": "5",
            "workload_size": "Small",    # Small/Medium/Large
            "scale_to_zero_enabled": True  # no cost when idle
        }]
    }
)

# Call the endpoint
response = requests.post(
    url="https://<workspace>.azuredatabricks.net/serving-endpoints/fraud-detection-v2/invocations",
    headers={"Authorization": f"Bearer {token}"},
    json={"dataframe_records": [{"feature1": 0.5, "feature2": 1.2}]}
)
print(response.json())
```

---

## Vector Search

Managed vector database for semantic search — foundation for RAG pipelines:

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create a vector search index on a Delta table
vsc.create_delta_sync_index(
    endpoint_name="vs-endpoint",
    index_name="prod.docs.policy_index",
    source_table_name="prod.docs.policy_documents",  # Delta table
    pipeline_type="TRIGGERED",       # TRIGGERED (batch) or CONTINUOUS (streaming)
    primary_key="doc_id",
    embedding_source_column="content",   # text column to embed
    embedding_model_endpoint_name="databricks-bge-large-en"  # embedding model
)

# Search
results = vsc.get_index("prod.docs.policy_index").similarity_search(
    query_text="What is the refund policy for electronics?",
    columns=["doc_id", "title", "content"],
    num_results=5
)
```

---

## AI Playground

A UI for experimenting with LLMs before building pipelines:
- Test different models (DBRX, Llama, Mistral) side by side
- Adjust temperature, max tokens, system prompts
- Export prompt + model selection directly to a notebook
- No API setup required

---

## Key Concepts

| Term | Meaning |
|------|---------|
| **Foundation Model API** | Pre-hosted LLM accessible via API — no deployment needed |
| **Serving endpoint** | REST API endpoint serving a specific model version |
| **Vector Search index** | Managed vector DB synced from a Delta table |
| **AI Gateway** | Central LLM proxy for governance and cost control |
| **DBRX** | Databricks' own open LLM (132B Mixture-of-Experts) |

---

## Interview Tips

> **Tip 1:** "What's the difference between Foundation Model APIs and Model Serving?" — "Foundation Model APIs serve Databricks-hosted models (DBRX, Llama, Mistral) — you just call them, no deployment needed. Model Serving deploys YOUR models (trained in MLflow, custom code) as REST endpoints that you manage and pay for compute."

> **Tip 2:** "How does Mosaic AI Vector Search differ from a manual embeddings approach?" — "Vector Search is fully managed: it syncs embeddings automatically from a Delta table (you don't write the embedding pipeline), manages the vector index, and handles incremental updates. Manual approach: you call an embedding model, store vectors in a VECTOR column, and query with VECTOR_COSINE_SIMILARITY — more control but much more setup."

> **Tip 3:** "What is the AI Gateway and why would you use it?" — "AI Gateway is a central proxy for all LLM calls in your organization. Benefits: (1) Rate limiting — prevent runaway usage costs. (2) Centralized logging — every LLM call is auditable. (3) Provider abstraction — switch from OpenAI to DBRX without changing app code. (4) Cost attribution — track token usage by team or use case."
