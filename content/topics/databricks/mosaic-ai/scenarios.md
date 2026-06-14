---
title: "Mosaic AI - Scenario Questions"
topic: databricks
subtopic: mosaic-ai
content_type: scenario_question
tags: [databricks, mosaic-ai, scenarios, interview, llm, rag, vector-search]
---

# Scenario Questions — Mosaic AI

<article data-difficulty="junior">

## 🟢 Junior: Classify Support Tickets with Foundation Model APIs

**Scenario:** Your support team receives 2,000 tickets/day across 5 categories: billing, technical, account, feature_request, other. Currently, 3 support agents manually read and route each ticket, taking 30 minutes per agent per day. Use Mosaic AI to automate classification.

<details>
<summary>✅ Solution</summary>

```python
import mlflow.deployments
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

client = mlflow.deployments.get_deploy_client("databricks")

# Define classification function
def classify_ticket(ticket_body: str) -> str:
    """Classify a support ticket into one of 5 categories."""
    response = client.predict(
        endpoint="databricks-mistral-7b-instruct",  # cheap model for simple classification
        inputs={
            "messages": [
                {"role": "system", "content":
                    "Classify the support ticket into EXACTLY ONE category. "
                    "Return ONLY the category word: billing, technical, account, feature_request, or other."},
                {"role": "user", "content": f"Ticket: {ticket_body[:1000]}"}
            ],
            "max_tokens": 10,
            "temperature": 0.0
        }
    )
    category = response["choices"][0]["message"]["content"].strip().lower()
    # Validate output
    valid_categories = {"billing", "technical", "account", "feature_request", "other"}
    return category if category in valid_categories else "other"

# Register as Spark UDF for batch processing
classify_udf = F.udf(classify_ticket, StringType())

# Classify all unrouted tickets from today
tickets = spark.table("prod.support.tickets") \
    .filter("routed_at IS NULL") \
    .filter("created_at >= current_date()")

classified = tickets.withColumn("category", classify_udf(F.col("body")))

# Route to teams
classified.withColumn("assigned_team",
    F.when(F.col("category") == "billing", "finance-support")
     .when(F.col("category") == "technical", "engineering-support")
     .when(F.col("category") == "account", "account-management")
     .otherwise("general-support")
).write.mode("append").saveAsTable("prod.support.routed_tickets")
```

**Key decisions:**
- Use Mistral-7B (cheap, fast) not DBRX — simple classification doesn't need a large model
- `max_tokens=10` — forces a single-word response, reduces cost significantly
- `temperature=0.0` — deterministic output for classification tasks
- Validate output against allowed categories to handle rare hallucinations

**Expected savings:** 2,000 tickets × 1 min manual routing = 33 agent-hours/day saved. Token cost: ~2,000 × 200 tokens = 400K tokens ≈ $0.04/day (Mistral rate).

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Build a RAG System for Product Documentation

**Scenario:** Your company has 5,000 product documentation pages stored in a Delta table (`prod.docs.pages`: `page_id`, `title`, `content`, `product_area`). Customers ask complex questions that require searching across multiple pages. Build a RAG system that can answer questions accurately and cite sources.

<details>
<summary>✅ Solution</summary>

```python
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments
import pandas as pd

vsc = VectorSearchClient()
deploy_client = mlflow.deployments.get_deploy_client("databricks")

# Step 1: Create Vector Search index (one-time setup)
# The index auto-embeds content and stays in sync with the Delta table
vsc.create_delta_sync_index(
    endpoint_name="docs-vs-endpoint",
    index_name="prod.docs.page_index",
    source_table_name="prod.docs.pages",
    pipeline_type="TRIGGERED",          # refresh on demand or schedule
    primary_key="page_id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

# Step 2: RAG function
def answer_product_question(
    question: str,
    product_area: str | None = None,
    num_chunks: int = 4
) -> dict:
    # Retrieve relevant pages
    search_kwargs = {
        "query_text": question,
        "columns": ["page_id", "title", "content", "product_area"],
        "num_results": num_chunks
    }
    if product_area:
        search_kwargs["filters"] = {"product_area": {"$eq": product_area}}

    results = vsc.get_index("prod.docs.page_index").similarity_search(**search_kwargs)
    chunks = results["result"]["data_array"]

    if not chunks:
        return {"answer": "No relevant documentation found.", "sources": []}

    # Build context string
    context = "\n\n---\n\n".join([
        f"[{c['title']}]\n{c['content'][:800]}"
        for c in chunks
    ])

    # Generate answer
    response = deploy_client.predict(
        endpoint="databricks-dbrx-instruct",
        inputs={
            "messages": [
                {"role": "system", "content":
                    "You are a product documentation assistant. "
                    "Answer using ONLY the provided documentation. "
                    "If the answer is not in the docs, say so explicitly. "
                    "Be concise and specific."},
                {"role": "user", "content":
                    f"Documentation:\n{context}\n\nQuestion: {question}"}
            ],
            "max_tokens": 600,
            "temperature": 0.0
        }
    )

    return {
        "answer": response["choices"][0]["message"]["content"],
        "sources": [{"title": c["title"], "page_id": c["page_id"]} for c in chunks]
    }

# Example usage
result = answer_product_question(
    question="How do I configure SSO with SAML?",
    product_area="authentication"
)
print(result["answer"])
print("Sources:", result["sources"])

# Deploy as Model Serving endpoint for production use
# Wrap in pyfunc and register in MLflow, then serve via w.serving_endpoints.create(...)
```

**Quality improvements:**
- `product_area` filter reduces noise — only retrieve docs from the right area
- Context limited to 800 chars/chunk — prevents context overflow
- `temperature=0.0` — factual questions need deterministic answers
- Source citations let users verify answers — critical for trust

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Production AI Gateway Architecture

**Scenario:** Your company has 15 teams using LLMs via 4 different providers (OpenAI, Anthropic, Azure OpenAI, Databricks). Problems: (1) no visibility into who is spending what, (2) two teams hit provider rate limits during peak hours, (3) a team accidentally logged customer PII in prompts, (4) provider APIs change without warning. Design a centralized AI gateway solution on Databricks.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
15 teams → Databricks AI Gateway → Provider routing
                    ↓
          - Rate limits per team
          - PII scrubbing (input + output)
          - Full audit logging (Delta table)
          - Fallback routing (OpenAI → Azure backup)
          - Cost attribution by team/use-case
```

**1. Gateway configuration:**

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Create routes for each provider (abstracted by alias)
routes = [
    {
        "name": "company-gpt4",     # alias teams call
        "served_entities": [{
            "external_model": {
                "name": "gpt-4o",
                "provider": "openai",
                "openai_config": {"openai_api_key": "{{secrets/llm/openai-key}}"},
                "task": "llm/v1/chat"
            }
        }],
        "rate_limits": [
            # Per-team rate limits using caller identity
            {"calls": 500, "renewal_period": "minute", "key": "user"},
            {"calls": 10000, "renewal_period": "hour", "key": "endpoint"}
        ],
        "auto_capture_config": {
            "catalog_name": "prod",
            "schema_name": "ai_gateway_logs",
            "table_name_prefix": "gpt4_requests",
            "enabled": True     # logs all requests and responses
        }
    },
    {
        "name": "company-llm",      # Databricks-hosted, no egress
        "served_entities": [{
            "external_model": {
                "name": "databricks-dbrx-instruct",
                "provider": "databricks",
                "task": "llm/v1/chat"
            }
        }],
        "rate_limits": [
            {"calls": 2000, "renewal_period": "minute", "key": "endpoint"}
        ]
    }
]
```

**2. PII scrubbing middleware (custom wrapper):**

```python
import re

PII_PATTERNS = {
    "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "ssn": r'\b\d{3}-\d{2}-\d{4}\b',
    "credit_card": r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
    "phone": r'\b(\+1)?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b'
}

def scrub_pii(text: str) -> tuple[str, list[str]]:
    """Remove PII and return scrubbed text + list of detected PII types."""
    detected = []
    for pii_type, pattern in PII_PATTERNS.items():
        if re.search(pattern, text):
            detected.append(pii_type)
            text = re.sub(pattern, f"[{pii_type.upper()}_REDACTED]", text)
    return text, detected

def safe_llm_call(messages: list, team: str, use_case: str) -> dict:
    """Wrapper that scrubs PII, calls gateway, logs metadata."""
    # Scrub input
    scrubbed_messages = []
    detected_pii_types = []
    for msg in messages:
        scrubbed_content, pii_types = scrub_pii(msg["content"])
        scrubbed_messages.append({**msg, "content": scrubbed_content})
        detected_pii_types.extend(pii_types)

    if detected_pii_types:
        log_pii_incident(team, use_case, detected_pii_types)   # alert security team

    # Call gateway
    response = deploy_client.predict(
        endpoint="company-llm",   # team uses alias, not direct provider
        inputs={"messages": scrubbed_messages, "max_tokens": 1000}
    )

    # Scrub output too
    answer = response["choices"][0]["message"]["content"]
    scrubbed_answer, _ = scrub_pii(answer)

    return {"answer": scrubbed_answer, "pii_detected": bool(detected_pii_types)}
```

**3. Cost attribution dashboard:**

```sql
-- Weekly cost by team from gateway logs
SELECT
    user_identity AS team,
    endpoint_name AS model,
    COUNT(*) AS requests,
    SUM(usage.total_tokens) AS total_tokens,
    ROUND(SUM(usage.total_tokens) / 1e6 *
        CASE endpoint_name
            WHEN 'company-gpt4' THEN 5.0    -- GPT-4o rate
            WHEN 'company-llm' THEN 0.95    -- DBRX rate
        END, 2) AS est_cost_usd
FROM prod.ai_gateway_logs.gpt4_requests
WHERE timestamp >= DATEADD(week, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY est_cost_usd DESC;
```

**4. Fallback routing (high availability):**
```python
def resilient_llm_call(messages: list, primary: str = "company-gpt4",
                        fallback: str = "company-llm") -> dict:
    try:
        return deploy_client.predict(endpoint=primary, inputs={"messages": messages})
    except Exception as e:
        log_provider_failure(primary, str(e))
        return deploy_client.predict(endpoint=fallback, inputs={"messages": messages})
```

**Outcomes from this design:**
- PII incidents: 0 post-implementation (was 3/month)
- Cost visibility: per-team weekly spend report automated
- Rate limit issues resolved: per-team limits prevent one team starving others
- Provider migration: teams call `company-gpt4` alias — switching the underlying provider is a config change, not a code change in 15 apps

</details>
</article>
