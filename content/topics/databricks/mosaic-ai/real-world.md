---
title: "Mosaic AI - Real-World Examples"
topic: databricks
subtopic: mosaic-ai
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, mosaic-ai, production, rag, agent, llm, fine-tuning]
---

# Mosaic AI — Real-World Production Examples

## Production Pattern: Internal Knowledge Base Chatbot

A tech company built an employee-facing HR and IT policy chatbot serving 5,000 employees:

```python
# Architecture: HR/IT documents in Delta → Vector Search → DBRX → Slack

# 1. Document ingestion pipeline (runs daily)
from databricks.vector_search.client import VectorSearchClient

# Source documents: HR policies, IT guides, onboarding docs (stored in Delta)
vsc = VectorSearchClient()

# Create sync index — auto-embeds when source table is updated
vsc.create_delta_sync_index(
    endpoint_name="hr-vs-endpoint",
    index_name="prod.hr_docs.policy_index",
    source_table_name="prod.hr_docs.policies",   # Delta table with title, content, department
    pipeline_type="TRIGGERED",
    primary_key="doc_id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

# 2. Chatbot endpoint (deployed as Model Serving)
import mlflow.pyfunc

class HRChatbot(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        from databricks.vector_search.client import VectorSearchClient
        import mlflow.deployments
        self.vsc = VectorSearchClient()
        self.llm = mlflow.deployments.get_deploy_client("databricks")

    def predict(self, context, model_input):
        question = model_input["question"].iloc[0]
        department_filter = model_input.get("department", pd.Series([None])).iloc[0]

        # Retrieve relevant chunks
        search_kwargs = {
            "query_text": question,
            "columns": ["title", "content", "department"],
            "num_results": 4
        }
        if department_filter:
            search_kwargs["filters"] = {"department": {"$eq": department_filter}}

        results = self.vsc.get_index("prod.hr_docs.policy_index").similarity_search(**search_kwargs)
        chunks = [r["content"] for r in results["result"]["data_array"]]
        context_text = "\n\n---\n\n".join(chunks)

        # Generate answer
        response = self.llm.predict(
            endpoint="databricks-dbrx-instruct",
            inputs={
                "messages": [
                    {"role": "system", "content":
                        "You are an HR assistant. Answer using ONLY the provided policy documents. "
                        "Be specific and cite the relevant policy. "
                        "If not found, say 'Please contact HR directly at hr@company.com'."},
                    {"role": "user", "content": f"Context:\n{context_text}\n\nQuestion: {question}"}
                ],
                "max_tokens": 600,
                "temperature": 0.0
            }
        )
        return pd.DataFrame([{
            "answer": response["choices"][0]["message"]["content"],
            "sources": [r["title"] for r in results["result"]["data_array"]]
        }])
```

**Results:** 42% reduction in HR ticket volume. Average query resolution in 8 seconds (was 24 hours waiting for HR response).

---

## Production Pattern: Code Review Agent

An engineering team built a CI/CD integrated code review agent:

```python
# Agent that reviews PRs and flags data quality issues
def code_review_agent(diff_text: str, file_name: str) -> dict:
    client = mlflow.deployments.get_deploy_client("databricks")

    # Step 1: Classify what type of change this is
    classification = client.predict(
        endpoint="databricks-mistral-7b-instruct",    # cheap model for routing
        inputs={
            "messages": [
                {"role": "user", "content":
                    f"Classify this code change: SQL/Python/Config/Test/Docs. File: {file_name}\n"
                    f"Diff (first 500 chars): {diff_text[:500]}\n"
                    f"Return ONLY the category word."}
            ],
            "max_tokens": 10
        }
    )
    change_type = classification["choices"][0]["message"]["content"].strip()

    # Step 2: Domain-specific review with appropriate prompt
    if change_type == "SQL":
        review_prompt = (
            "Review this SQL change for: (1) Missing WHERE clauses on large tables, "
            "(2) Cartesian joins, (3) SELECT * usage, (4) Missing NULL handling. "
            "Return JSON: {\"issues\": [{\"severity\": \"high|medium|low\", \"message\": \"...\"}]}"
        )
    elif change_type == "Python":
        review_prompt = (
            "Review this Python/PySpark code for: (1) N+1 query patterns, "
            "(2) collect() on large DataFrames, (3) UDFs that should be native Spark functions. "
            "Return JSON: {\"issues\": [{\"severity\": \"high|medium|low\", \"message\": \"...\"}]}"
        )
    else:
        review_prompt = "Briefly review this change for obvious issues. Return JSON: {\"issues\": []}"

    review = client.predict(
        endpoint="databricks-dbrx-instruct",
        inputs={
            "messages": [
                {"role": "system", "content": review_prompt},
                {"role": "user", "content": f"Code diff:\n{diff_text[:3000]}"}
            ],
            "max_tokens": 500
        }
    )

    import json
    return json.loads(review["choices"][0]["message"]["content"])
```

---

## Production Pattern: Fine-Tuned Support Classifier

A SaaS company fine-tuned Mistral-7B on 50,000 labeled support tickets — reducing misrouting from 18% to 3%:

**Before (zero-shot with GPT-4):**
- Cost: $0.012 per ticket × 10,000 tickets/day = $120/day = $43,800/year
- Accuracy: 82% (18% misrouting)
- Latency: 2-3 seconds

**After (fine-tuned Mistral-7B on Databricks):**
- Training: 50K examples, 3 epochs, 2 hours on A10G cluster
- Cost: $0.0002 per ticket = $0.60/day inference + ~$200 monthly serving
- Accuracy: 97% (3% misrouting)
- Latency: 180ms

```python
# Fine-tuning job
run = w.fine_tuning.runs.create(
    custom_weights_path="dbfs:/models/support-classifier",
    training_data_path="ml.finetune.support_tickets_train",
    eval_data_path="ml.finetune.support_tickets_eval",
    model="mistralai/Mistral-7B-Instruct-v0.2",
    num_epochs=3,
    registered_model_name="support-ticket-classifier-ft"
)

# Serving endpoint for fine-tuned model
w.serving_endpoints.create(
    name="support-classifier",
    config={
        "served_models": [{
            "model_name": "support-ticket-classifier-ft",
            "model_version": "1",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

---

## Production Pattern: Monitoring LLM Quality Over Time

```python
# Log every LLM call to a Delta table for quality tracking
from databricks.sdk import WorkspaceClient

# AI Gateway auto-captures all calls (configured on endpoint)
# Analyze quality trends weekly:
quality_trends = spark.sql("""
    SELECT
        DATE_TRUNC('week', timestamp) AS week,
        COUNT(*) AS total_calls,
        AVG(CASE WHEN user_feedback = 'positive' THEN 1 ELSE 0 END) AS positive_feedback_rate,
        AVG(response_latency_ms) AS avg_latency_ms,
        SUM(total_tokens) AS total_tokens,
        ROUND(SUM(total_tokens) / 1e6 * 0.95, 2) AS est_cost_usd  -- DBRX rate
    FROM prod.ai_logs.llm_gateway_requests
    WHERE endpoint_name = 'customer-support-bot'
      AND timestamp >= DATEADD(month, -3, CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 1
""")

display(quality_trends)

# Alert on quality regression
latest_week = quality_trends.orderBy("week", ascending=False).first()
if latest_week["positive_feedback_rate"] < 0.80:
    send_alert(f"LLM quality alert: feedback rate={latest_week['positive_feedback_rate']:.1%}")
```
