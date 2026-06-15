---
title: "Agentic Patterns - Scenario Questions"
topic: rag-llm
subtopic: agentic-patterns
content_type: scenario_question
tags: [rag, llm, agents, tool-use, scenarios, interview]
---

# Scenario Questions — Agentic Patterns

<article data-difficulty="junior">

## 🟢 Junior: Build a Simple Tool-Calling Agent

**Scenario:** You're building a data assistant for your team. Users ask questions like "How many orders were placed yesterday?" and you want an LLM to generate and run the SQL automatically. Build a minimal agent that: (1) receives a natural language question, (2) generates SQL, (3) executes it, (4) returns a plain-English answer. Use OpenAI tool calling.

<details>
<summary>💡 Hint</summary>
Define one tool `execute_sql(query: str)`. Run the agent loop: call the LLM, check if it returned a `tool_call`, execute the function, append the result as a `tool` message, call the LLM again to get the final answer.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
import json

client = OpenAI()

# Simulated DB — in production, use snowflake-connector-python or bigquery
def execute_sql(query: str) -> str:
    """Execute SQL and return results as JSON string."""
    fake_db = {
        "SELECT COUNT(*) as order_count FROM orders WHERE date = CURRENT_DATE - 1":
            '[{"order_count": 1423}]',
        "SELECT SUM(revenue) as total FROM orders WHERE date = CURRENT_DATE - 1":
            '[{"total": 87650.50}]'
    }
    # Real impl: snowflake.connector.connect(...).cursor().execute(query)
    return fake_db.get(query.strip(), '[{"error": "query not in mock DB"}]')

tools = [{
    "type": "function",
    "function": {
        "name": "execute_sql",
        "description": "Execute a read-only SQL query against the orders database and return results as JSON",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A valid SQL SELECT statement"
                }
            },
            "required": ["query"]
        }
    }
}]

def data_assistant(user_question: str) -> str:
    messages = [
        {
            "role": "system",
            "content": """You are a data assistant with access to an orders database.
Table: orders (id, date, revenue, customer_id, status)
Use execute_sql to answer data questions. Always write valid SQL.
After getting results, explain the answer in plain English."""
        },
        {"role": "user", "content": user_question}
    ]

    # Agent loop — max 3 iterations for safety
    for _ in range(3):
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=tools,
            tool_choice="auto"
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content  # Final answer

        # Execute tool calls
        messages.append(msg)
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            result = execute_sql(args["query"])
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result
            })

    return "Could not answer within iteration limit"

# Test
print(data_assistant("How many orders were placed yesterday?"))
# Expected: "There were 1,423 orders placed yesterday."
```

**Key interview points:**
- Always append the assistant message before tool results
- The `tool_call_id` must match between the assistant and tool messages
- Set a max iteration limit to prevent infinite loops
- Use `temperature=0` for deterministic SQL generation

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid: Multi-Agent Pipeline with Error Handling

**Scenario:** Your team wants to automate a weekly data quality report. The pipeline should: (1) fetch dbt test failures from the last 24h, (2) for each failure, look up the affected table's owner from a metadata store, (3) group failures by owner, (4) draft a Slack message per owner with failures and suggested actions. The pipeline must handle tool failures gracefully and must NOT send Slack messages if the overall failure count is 0. Design and implement this as a multi-step agent.

<details>
<summary>💡 Hint</summary>
Use a planner-executor approach. First call: plan the steps. Then execute each step with individual tool calls. Track state in a Python dict between tool calls. Validate intermediate results before proceeding (don't call Slack if no failures). Use try/except around each tool call and include error info in the agent context.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
import json
from collections import defaultdict
from typing import Optional

client = OpenAI()

# --- Tool implementations ---

def get_dbt_failures(hours: int = 24) -> dict:
    """Fetch recent dbt test failures."""
    # Real impl: call dbt Cloud API or query metadata table
    return {
        "failures": [
            {"test": "not_null_orders_revenue", "model": "orders", "severity": "error"},
            {"test": "unique_customers_id", "model": "customers", "severity": "warn"},
            {"test": "not_null_revenue_date", "model": "revenue", "severity": "error"}
        ],
        "total": 3,
        "window_hours": hours
    }

def get_table_owner(table_name: str) -> dict:
    """Look up table owner from data catalog."""
    owners = {
        "orders": {"owner": "alice@company.com", "slack_id": "U12345", "team": "commerce"},
        "customers": {"owner": "bob@company.com", "slack_id": "U67890", "team": "growth"},
        "revenue": {"owner": "alice@company.com", "slack_id": "U12345", "team": "commerce"}
    }
    return owners.get(table_name, {"owner": "unknown", "slack_id": None, "team": "unknown"})

def send_slack_message(slack_user_id: str, message: str) -> dict:
    """Send a Slack DM to a user."""
    if not slack_user_id:
        return {"success": False, "error": "No Slack ID provided"}
    # Real impl: slack_sdk.WebClient(token=...).chat_postMessage(channel=slack_user_id, text=message)
    print(f"[SLACK DM to {slack_user_id}]: {message[:100]}...")
    return {"success": True, "message_ts": "1234567890.123456"}

# --- Agent setup ---

tools = [
    {
        "type": "function",
        "function": {
            "name": "get_dbt_failures",
            "description": "Fetch dbt test failures from the last N hours",
            "parameters": {"type": "object", "properties": {"hours": {"type": "integer", "default": 24}}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_owner",
            "description": "Get the owner and Slack ID for a dbt model/table",
            "parameters": {"type": "object", "properties": {"table_name": {"type": "string"}}, "required": ["table_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "send_slack_message",
            "description": "Send a Slack DM to a user. Only call if there are actual failures to report.",
            "parameters": {
                "type": "object",
                "properties": {
                    "slack_user_id": {"type": "string"},
                    "message": {"type": "string"}
                },
                "required": ["slack_user_id", "message"]
            }
        }
    }
]

tool_map = {
    "get_dbt_failures": get_dbt_failures,
    "get_table_owner": get_table_owner,
    "send_slack_message": send_slack_message
}

def run_dq_report_agent() -> str:
    messages = [
        {
            "role": "system",
            "content": """You are a data quality reporting agent. Your job:
1. Fetch dbt test failures from the last 24 hours
2. If total failures = 0, stop — do NOT send any Slack messages
3. For each failed model, get the table owner
4. Group failures by owner
5. Send one Slack message per owner listing their failures and suggesting they investigate
6. Report a summary when done

Handle tool errors gracefully: if get_table_owner fails, skip that failure and note it in your summary."""
        },
        {"role": "user", "content": "Run the weekly data quality report"}
    ]

    for iteration in range(15):  # enough for: 1 fetch + 3 owner lookups + 2 slack messages
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=tools,
                tool_choice="auto"
            )
        except Exception as e:
            return f"LLM call failed: {e}"

        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content  # Final summary

        messages.append(msg)

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                args = json.loads(tc.function.arguments)
                result = tool_map[fn_name](**args)
                content = json.dumps(result)
            except Exception as e:
                content = json.dumps({"error": str(e), "tool": fn_name})

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": content
            })

    return "Agent reached max iterations without completing"

if __name__ == "__main__":
    summary = run_dq_report_agent()
    print("\n=== Agent Summary ===")
    print(summary)
```

**Key points for interview:**
- Agent state flows through the message thread — no explicit state dict needed
- Validate business logic constraints in the system prompt (`if total = 0, stop`)
- Wrap each tool call in try/except; inject error JSON back as tool result
- Set `max_iterations` to `N_expected_steps * 2` to catch edge cases
- Use cheaper model (`gpt-4o-mini`) for deterministic sub-tasks to save cost

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Production Agent System for Autonomous Pipeline Repair

**Scenario:** Your company runs 500+ Airflow DAGs. When a DAG fails, the on-call engineer must: (1) check the logs, (2) identify if it's transient (retry) or systemic (needs fix), (3) if systemic, identify root cause, (4) apply fix (restart upstream, patch config, or escalate to dev). The team wants to automate 70% of this triage with an agent. Design the full production system: agent architecture, tools, HITL checkpoints, observability, cost controls, and failure modes. Then implement the core agent loop.

<details>
<summary>💡 Hint</summary>
Think about: (1) which tool calls are read-only vs destructive (HITL on destructive), (2) how to classify transient vs systemic before acting, (3) how to cap cost per incident, (4) what to log for post-incident review, (5) how to handle the agent itself failing (fallback to PagerDuty escalation).
</details>

<details>
<summary>✅ Solution</summary>

**Architecture Decision:**
```
Airflow failure alert → SNS → Lambda (entry point)
                                   │
                         ┌─────────▼──────────┐
                         │   Triage Agent      │
                         │  (gpt-4o, max 8     │
                         │   iterations,       │
                         │   $0.50 budget)     │
                         └─────────┬───────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
             get_dag_logs   check_upstream   search_runbook
                    │              │               │
                    └──────────────┴───────────────┘
                                   │
                         classify: TRANSIENT or SYSTEMIC
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
             TRANSIENT:                    SYSTEMIC:
             retry_dag (auto)              [HITL checkpoint]
                                           if approved:
                                             apply_fix OR escalate_pagerduty
```

```python
import json, time, os
from openai import OpenAI
from dataclasses import dataclass, field
from typing import Optional

client = OpenAI()

@dataclass
class AgentBudget:
    max_usd: float = 0.50
    max_iterations: int = 8
    spent_usd: float = 0.0
    iterations: int = 0

    # GPT-4o pricing (as of 2024): $2.50/1M input, $10/1M output
    INPUT_COST_PER_TOKEN = 2.50 / 1_000_000
    OUTPUT_COST_PER_TOKEN = 10.0 / 1_000_000

    def record_usage(self, input_tokens: int, output_tokens: int):
        self.spent_usd += input_tokens * self.INPUT_COST_PER_TOKEN
        self.spent_usd += output_tokens * self.OUTPUT_COST_PER_TOKEN
        self.iterations += 1

    def budget_exceeded(self) -> bool:
        return self.spent_usd >= self.max_usd or self.iterations >= self.max_iterations

# --- Tools ---

def get_dag_logs(dag_id: str, run_id: str, tail_lines: int = 100) -> dict:
    """Fetch Airflow task logs. Read-only. Safe to auto-execute."""
    # Real: airflow.api.client.AirflowClient or direct log file access
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "logs": "TaskInstance: extract_raw FAILED\nTraceback: ConnectionError: could not connect to postgres:5432\nPrevious attempts: 3 retries exhausted",
        "task_state": "failed",
        "last_success_run": "2024-01-14T06:00:00Z"
    }

def check_upstream_health(service: str) -> dict:
    """Check health of an upstream dependency. Read-only."""
    health_map = {
        "postgres": {"status": "degraded", "connections": 2, "max_connections": 100},
        "snowflake": {"status": "healthy", "latency_ms": 45},
        "s3": {"status": "healthy"}
    }
    return health_map.get(service, {"status": "unknown"})

def search_runbook(query: str) -> dict:
    """Vector search over internal runbooks. Read-only."""
    return {
        "results": [
            {
                "title": "Postgres connection exhaustion playbook",
                "steps": "1. Check pg_stat_activity 2. Kill idle connections 3. Increase max_connections if needed",
                "last_updated": "2024-01-10",
                "resolution_time_avg_min": 15
            }
        ]
    }

def retry_dag(dag_id: str, run_id: str) -> dict:
    """Trigger a DAG retry. LOW risk — auto-approvable."""
    # Real: airflow REST API POST /api/v1/dags/{dag_id}/dagRuns/{dag_id_run_id}/clear
    print(f"[AUTO-ACTION] Retrying DAG {dag_id} run {run_id}")
    return {"success": True, "new_run_id": f"{run_id}_retry_1"}

def request_human_approval(action: str, details: str) -> dict:
    """
    HITL checkpoint for destructive/risky actions.
    In production: post to Slack with approve/reject buttons,
    poll for response with timeout.
    """
    print(f"\n🔴 HUMAN APPROVAL REQUIRED")
    print(f"Proposed action: {action}")
    print(f"Details: {details}")
    # Simulate: in prod this blocks until Slack callback or times out
    response = input("Approve? (yes/no): ").strip().lower()
    return {"approved": response == "yes", "approver": "on-call-engineer", "timestamp": time.time()}

def escalate_to_pagerduty(dag_id: str, summary: str, severity: str = "high") -> dict:
    """Page the on-call engineer. Used when agent cannot resolve."""
    print(f"[PAGERDUTY] Escalating {dag_id}: {summary}")
    return {"incident_id": "INC-9876", "severity": severity}

# --- Tool registry with risk levels ---

TOOLS_CONFIG = {
    "get_dag_logs": {"risk": "low", "fn": get_dag_logs},
    "check_upstream_health": {"risk": "low", "fn": check_upstream_health},
    "search_runbook": {"risk": "low", "fn": search_runbook},
    "retry_dag": {"risk": "low", "fn": retry_dag},  # idempotent, low blast radius
    "request_human_approval": {"risk": "high", "fn": request_human_approval},
    "escalate_to_pagerduty": {"risk": "high", "fn": escalate_to_pagerduty}
}

TOOL_DEFINITIONS = [
    {"type": "function", "function": {"name": "get_dag_logs", "description": "Get Airflow task failure logs", "parameters": {"type": "object", "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}, "tail_lines": {"type": "integer", "default": 100}}, "required": ["dag_id", "run_id"]}}},
    {"type": "function", "function": {"name": "check_upstream_health", "description": "Check health of a dependency (postgres, snowflake, s3, kafka)", "parameters": {"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]}}},
    {"type": "function", "function": {"name": "search_runbook", "description": "Search internal runbooks for past incidents and fixes", "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "retry_dag", "description": "Trigger a retry for a failed DAG run. Only use for transient failures.", "parameters": {"type": "object", "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}}, "required": ["dag_id", "run_id"]}}},
    {"type": "function", "function": {"name": "request_human_approval", "description": "Request human approval before taking a risky action. Always call before escalation.", "parameters": {"type": "object", "properties": {"action": {"type": "string"}, "details": {"type": "string"}}, "required": ["action", "details"]}}},
    {"type": "function", "function": {"name": "escalate_to_pagerduty", "description": "Page the on-call engineer when the issue cannot be auto-resolved", "parameters": {"type": "object", "properties": {"dag_id": {"type": "string"}, "summary": {"type": "string"}, "severity": {"type": "string", "enum": ["low", "high", "critical"]}}, "required": ["dag_id", "summary"]}}}
]

def triage_agent(dag_id: str, run_id: str, alert_context: dict) -> dict:
    """Main triage agent entry point."""
    budget = AgentBudget()
    audit_log = []  # Full audit trail for post-incident review

    messages = [
        {
            "role": "system",
            "content": f"""You are an Airflow incident triage agent. For this failed DAG:

DAG: {dag_id}
Run ID: {run_id}
Alert: {json.dumps(alert_context)}

Your process:
1. Get the failure logs
2. Check health of any upstream services mentioned in the logs
3. Search runbooks for similar incidents
4. Classify: TRANSIENT (network blip, timeout) or SYSTEMIC (config error, data issue, resource exhaustion)
5. If TRANSIENT: retry the DAG automatically
6. If SYSTEMIC: request human approval, then escalate_to_pagerduty with full context
7. If budget exceeded before resolution: escalate immediately

Budget constraint: You have ${budget.max_usd} budget. Use read-only tools freely. Minimize LLM calls."""
        },
        {"role": "user", "content": f"Triage the failed DAG: {dag_id}"}
    ]

    try:
        while not budget.budget_exceeded():
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=TOOL_DEFINITIONS,
                tool_choice="auto",
                max_tokens=1000
            )

            budget.record_usage(
                response.usage.prompt_tokens,
                response.usage.completion_tokens
            )

            msg = response.choices[0].message
            audit_log.append({"type": "llm_response", "content": str(msg), "budget": budget.spent_usd})

            if not msg.tool_calls:
                # Final answer
                return {
                    "status": "resolved",
                    "summary": msg.content,
                    "cost_usd": budget.spent_usd,
                    "iterations": budget.iterations,
                    "audit_log": audit_log
                }

            messages.append(msg)

            for tc in msg.tool_calls:
                fn_name = tc.function.name
                args = json.loads(tc.function.arguments)
                audit_log.append({"type": "tool_call", "tool": fn_name, "args": args})

                try:
                    result = TOOLS_CONFIG[fn_name]["fn"](**args)
                    content = json.dumps(result)
                except Exception as e:
                    content = json.dumps({"error": str(e)})
                    audit_log.append({"type": "tool_error", "tool": fn_name, "error": str(e)})

                messages.append({"role": "tool", "tool_call_id": tc.id, "content": content})

        # Budget exceeded without resolution
        escalate_to_pagerduty(dag_id, f"Agent budget exceeded. Spent ${budget.spent_usd:.3f}. Manual review required.", "high")
        return {"status": "escalated", "reason": "budget_exceeded", "cost_usd": budget.spent_usd, "audit_log": audit_log}

    except Exception as e:
        # Agent itself failed — always fall back to human
        escalate_to_pagerduty(dag_id, f"Triage agent crashed: {e}", "critical")
        return {"status": "error", "error": str(e)}

# Usage
result = triage_agent(
    dag_id="etl_orders_daily",
    run_id="scheduled__2024-01-15T06:00:00+00:00",
    alert_context={"alert_type": "task_failed", "task_id": "extract_raw", "retries": 3}
)
print(json.dumps(result, indent=2, default=str))
```

**Senior-level design decisions to discuss:**
1. **Budget cap per incident** — prevents runaway costs from looping agents; fall back to PagerDuty
2. **Risk-tiered tool execution** — low-risk tools auto-execute; high-risk always go through HITL
3. **Full audit log** — every LLM response and tool call logged for post-incident review and compliance
4. **Graceful agent failure** — if the agent itself crashes, always escalate to human; never silently fail
5. **Idempotent tool design** — `retry_dag` can be called twice safely; critical for retry logic
6. **Model selection** — `gpt-4o` for reasoning; use `gpt-4o-mini` in the routing layer if cost is a concern
7. **Separation of concerns** — tool definitions (JSON schema) vs tool implementations (Python) kept separate for testability

</details>
</article>
