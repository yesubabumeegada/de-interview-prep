---
title: "Agentic Patterns - Real World"
topic: rag-llm
subtopic: agentic-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, agents, production, data-engineering, observability]
---

# Agentic Patterns — Real World

## Case Study 1: Autonomous Data Quality Agent

**Problem:** A fintech DE team spends 3 hours/day triaging dbt test failures. They built an agent to auto-triage and suggest fixes.

### Architecture

```
Airflow DAG (dbt run) → failure webhook → Agent Lambda
                                              │
                     ┌────────────────────────┼──────────────────────┐
                     ▼                        ▼                      ▼
              get_dbt_test_logs       query_lineage_graph    search_runbook_docs
                     │                        │                      │
                     └────────────────────────┴──────────────────────┘
                                              │
                                    generate_fix_suggestion
                                              │
                                    [HUMAN CHECKPOINT: Slack approval]
                                              │
                                    create_jira_ticket OR auto_apply_fix
```

### Implementation

```python
import boto3
import json
from openai import OpenAI

client = OpenAI()

def get_dbt_test_logs(run_id: str) -> dict:
    """Fetch dbt Cloud run logs via API."""
    import requests
    resp = requests.get(
        f"https://cloud.getdbt.com/api/v2/accounts/12345/runs/{run_id}/artifacts/run_results.json",
        headers={"Authorization": "Token dbt_..."}
    )
    return resp.json()

def query_lineage_graph(model_name: str) -> dict:
    """Get upstream/downstream dependencies from dbt manifest."""
    # In production: parse manifest.json or query dbt Semantic Layer
    return {
        "upstream": ["raw.orders", "raw.customers"],
        "downstream": ["marts.revenue", "marts.churn_analysis"],
        "owner": "data-team@company.com"
    }

def search_runbook_docs(query: str) -> str:
    """Search internal runbooks via vector search."""
    # Calls your internal RAG pipeline
    return "Previous fix for null revenue: check upstream Stripe webhook failures"

dq_tools = [
    {"type": "function", "function": {"name": "get_dbt_test_logs", "description": "Get dbt test failure details", "parameters": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}}},
    {"type": "function", "function": {"name": "query_lineage_graph", "description": "Get model dependencies", "parameters": {"type": "object", "properties": {"model_name": {"type": "string"}}, "required": ["model_name"]}}},
    {"type": "function", "function": {"name": "search_runbook_docs", "description": "Search runbooks for past fixes", "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}}
]

tool_map = {
    "get_dbt_test_logs": get_dbt_test_logs,
    "query_lineage_graph": query_lineage_graph,
    "search_runbook_docs": search_runbook_docs
}

def triage_dbt_failure(run_id: str, test_name: str) -> str:
    messages = [
        {"role": "system", "content": """You are a data engineering expert. Triage dbt test failures:
1. Get the test logs
2. Identify the affected model and check its lineage
3. Search runbooks for similar past failures
4. Provide: root cause, severity (P1/P2/P3), and recommended fix"""},
        {"role": "user", "content": f"Triage dbt test failure: run_id={run_id}, test={test_name}"}
    ]

    for _ in range(8):
        resp = client.chat.completions.create(model="gpt-4o", messages=messages, tools=dq_tools)
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return msg.content
        messages.append(msg)
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            result = tool_map[tc.function.name](**args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(result)})

    return "Agent could not diagnose within iteration limit"

# Triggered by Airflow webhook
def lambda_handler(event, context):
    payload = json.loads(event["body"])
    analysis = triage_dbt_failure(payload["run_id"], payload["test_name"])

    # Post to Slack for human review (HITL checkpoint)
    boto3.client("sns").publish(
        TopicArn="arn:aws:sns:us-east-1:123:dbt-alerts",
        Message=json.dumps({"analysis": analysis, "run_id": payload["run_id"]}),
        Subject="DQ Agent: dbt failure triaged"
    )
    return {"statusCode": 200}
```

---

## Case Study 2: NL-to-SQL Agent with Validation

**Problem:** A self-serve analytics tool where business users ask questions in English and the agent generates + validates SQL before execution.

```python
from typing import Optional
import re

ALLOWED_OPERATIONS = {"SELECT", "WITH"}  # no mutations

def validate_sql(sql: str) -> tuple[bool, str]:
    """Safety validation before execution."""
    sql_upper = sql.upper().strip()
    # Only allow read operations
    first_word = sql_upper.split()[0]
    if first_word not in ALLOWED_OPERATIONS:
        return False, f"Rejected: only SELECT/WITH allowed, got {first_word}"
    # Block dangerous patterns
    dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "GRANT"]
    for kw in dangerous:
        if kw in sql_upper:
            return False, f"Rejected: dangerous keyword {kw} found"
    return True, "OK"

def nl_to_sql_agent(question: str, schema_context: str) -> dict:
    """Convert natural language question to validated SQL."""
    messages = [
        {"role": "system", "content": f"""You are a Snowflake SQL expert.
Database schema:
{schema_context}

Rules:
- Only write SELECT queries
- Use explicit table aliases
- Add LIMIT 10000 if no LIMIT specified
- Always include query explanation"""},
        {"role": "user", "content": question}
    ]

    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0
    )

    # Expected: {"sql": "...", "explanation": "...", "tables_used": [...]}
    result = json.loads(resp.choices[0].message.content)

    # Validate before returning to caller
    valid, reason = validate_sql(result.get("sql", ""))
    if not valid:
        # Re-prompt with the error
        messages.append({"role": "assistant", "content": resp.choices[0].message.content})
        messages.append({"role": "user", "content": f"That SQL was rejected: {reason}. Please fix it."})
        resp2 = client.chat.completions.create(model="gpt-4o", messages=messages, response_format={"type": "json_object"})
        result = json.loads(resp2.choices[0].message.content)

    result["validated"] = True
    return result
```

---

## Observability for Agents

Production agents need full tracing. Use LangSmith or OpenTelemetry:

```python
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import functools, time

tracer = trace.get_tracer("de-agent")

def traced_tool(tool_name: str):
    """Decorator to add OTel spans to every tool call."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(f"tool.{tool_name}") as span:
                span.set_attribute("tool.name", tool_name)
                span.set_attribute("tool.args", str(kwargs))
                t0 = time.time()
                try:
                    result = fn(*args, **kwargs)
                    span.set_attribute("tool.result_length", len(str(result)))
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
                finally:
                    span.set_attribute("tool.duration_ms", (time.time() - t0) * 1000)
        return wrapper
    return decorator

@traced_tool("run_sql")
def run_sql_query(query: str, database: str) -> dict:
    # ... execute query
    pass
```

### Key Metrics to Track

```python
# Track per-agent-run in your metrics store (Prometheus/Datadog)
agent_metrics = {
    "agent_run_duration_seconds": "histogram",
    "agent_tool_calls_total": "counter",        # by tool_name, status
    "agent_iterations_total": "histogram",       # steps to completion
    "agent_llm_tokens_used": "counter",          # by model, type (input/output)
    "agent_task_success_rate": "gauge",          # rolling 24h
    "agent_cost_usd": "counter",                 # per run
}

# Alert thresholds
alerts = {
    "p95_duration > 30s": "page on-call",
    "success_rate < 0.8 for 5min": "page on-call",
    "cost_per_hour > $50": "alert data platform team"
}
```

---

## Lessons Learned (Real Production)

1. **Start with `max_iterations=5`** — most tasks complete in 3-4 steps. Higher limits just increase cost and debugging surface.

2. **Always log the full message thread** — when an agent fails in production, you need the complete thought/action/observation trace to debug.

3. **Small model for routing, large model for reasoning** — use `gpt-4o-mini` or `claude-haiku-3-5` for tool selection and routing; reserve `gpt-4o` or `claude-opus` for complex reasoning steps.

4. **Idempotent tools** — if a tool is called twice with the same args, it should return the same result. This is critical for retry logic.

5. **Human approval for writes** — any tool that modifies data (INSERT, UPDATE, external API write) must go through a HITL checkpoint in production, even if it slows the pipeline.

6. **Test with adversarial inputs** — test what happens when a tool returns an error, returns unexpected schema, or returns an empty result. Most agents break on empty results.
