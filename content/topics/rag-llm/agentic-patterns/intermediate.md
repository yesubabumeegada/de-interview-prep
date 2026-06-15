---
title: "Agentic Patterns - Intermediate"
topic: rag-llm
subtopic: agentic-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, llm, agents, langchain, llamaindex, multi-agent, planning]
---

# Agentic Patterns — Intermediate

## LangChain Agents

LangChain wraps the ReAct loop with higher-level abstractions. The modern API uses `create_react_agent` + `AgentExecutor`.

```python
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import tool
from langchain import hub

llm = ChatOpenAI(model="gpt-4o", temperature=0)

@tool
def run_sql(query: str) -> str:
    """Execute a SQL query against the data warehouse. Use for data questions."""
    # Real impl: connect to Snowflake/BigQuery
    return '{"rows": [{"total": 4500000}]}'

@tool
def get_table_schema(table_name: str) -> str:
    """Return the column names and types for a given table."""
    schemas = {
        "sales": "id INT, date DATE, revenue DECIMAL, region VARCHAR",
        "customers": "id INT, name VARCHAR, segment VARCHAR, ltv DECIMAL"
    }
    return schemas.get(table_name, "Table not found")

tools = [run_sql, get_table_schema]

# Pull standard ReAct prompt from hub
prompt = hub.pull("hwchase17/react")

agent = create_react_agent(llm, tools, prompt)
executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=10,          # prevent infinite loops
    handle_parsing_errors=True  # graceful on malformed output
)

result = executor.invoke({"input": "What is the total revenue per region for Q1 2024?"})
print(result["output"])
```

### LangGraph for Stateful Agents

LangGraph gives you explicit state machines — better for complex agents:

```python
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_openai import ChatOpenAI
from typing import TypedDict, Annotated
import operator

class AgentState(TypedDict):
    messages: Annotated[list, operator.add]

llm = ChatOpenAI(model="gpt-4o").bind_tools(tools)

def call_model(state: AgentState):
    response = llm.invoke(state["messages"])
    return {"messages": [response]}

def should_continue(state: AgentState):
    last = state["messages"][-1]
    return "tools" if last.tool_calls else END

graph = StateGraph(AgentState)
graph.add_node("agent", call_model)
graph.add_node("tools", ToolNode(tools))
graph.set_entry_point("agent")
graph.add_conditional_edges("agent", should_continue)
graph.add_edge("tools", "agent")

app = graph.compile()
result = app.invoke({"messages": [("user", "Total Q1 revenue by region?")]})
```

---

## Multi-Agent Systems

### Orchestrator + Subagent Pattern

```python
from openai import OpenAI
import json

client = OpenAI()

# Subagent: specialized SQL agent
def sql_subagent(task: str) -> str:
    """Dedicated SQL execution agent."""
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # cheaper model for subagents
        messages=[
            {"role": "system", "content": "You are a SQL expert. Write and execute SQL queries only."},
            {"role": "user", "content": task}
        ],
        tools=[sql_tool_definition],
        tool_choice="required"
    )
    # ... execute tools and return result
    return "Query result: ..."

# Subagent: data quality checker
def dq_subagent(data: str) -> str:
    """Validates data quality and flags issues."""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Check this data for quality issues: nulls, outliers, duplicates."},
            {"role": "user", "content": data}
        ]
    )
    return response.choices[0].message.content

# Orchestrator: plans and delegates
orchestrator_tools = [
    {
        "type": "function",
        "function": {
            "name": "delegate_to_sql_agent",
            "description": "Send a data retrieval task to the SQL specialist agent",
            "parameters": {"type": "object", "properties": {"task": {"type": "string"}}, "required": ["task"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delegate_to_dq_agent",
            "description": "Send data to the quality checking agent",
            "parameters": {"type": "object", "properties": {"data": {"type": "string"}}, "required": ["data"]}
        }
    }
]

def orchestrator(user_goal: str) -> str:
    messages = [
        {"role": "system", "content": "You are a data engineering orchestrator. Break complex tasks into steps and delegate to specialist agents."},
        {"role": "user", "content": user_goal}
    ]
    tool_map = {"delegate_to_sql_agent": sql_subagent, "delegate_to_dq_agent": dq_subagent}

    for _ in range(5):  # max iterations
        resp = client.chat.completions.create(model="gpt-4o", messages=messages, tools=orchestrator_tools)
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return msg.content
        messages.append(msg)
        for tc in msg.tool_calls:
            fn = tool_map[tc.function.name]
            args = json.loads(tc.function.arguments)
            result = fn(**args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
    return "Max iterations reached"
```

---

## Planning Patterns

### Chain-of-Thought (CoT)

Force explicit reasoning before tool calls:

```python
system_prompt = """Before calling any tool, write your reasoning:
PLAN: [step-by-step plan]
THEN execute one step at a time."""
```

### Tree-of-Thought (ToT)

Generate multiple reasoning paths, evaluate each, pick the best:

```python
def tree_of_thought(problem: str, branches: int = 3) -> str:
    # Generate multiple approaches
    proposals = []
    for i in range(branches):
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": f"Generate approach #{i+1} for solving this problem. Be creative."},
                {"role": "user", "content": problem}
            ]
        )
        proposals.append(resp.choices[0].message.content)

    # Evaluate and pick best
    eval_resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "Evaluate these approaches and pick the best one. Return only the best approach."},
            {"role": "user", "content": "\n\n".join(f"Approach {i+1}:\n{p}" for i, p in enumerate(proposals))}
        ]
    )
    return eval_resp.choices[0].message.content
```

---

## Tool Routing

Route queries to the right tool based on intent:

```python
from enum import Enum

class ToolRoute(str, Enum):
    SQL = "sql"
    VECTOR_SEARCH = "vector_search"
    CALCULATOR = "calculator"
    HUMAN = "human_escalation"

def route_query(query: str) -> ToolRoute:
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # cheap routing model
        messages=[
            {"role": "system", "content": """Classify this query into one routing category:
- sql: needs structured data from a database
- vector_search: needs semantic document search
- calculator: needs arithmetic/math computation
- human_escalation: too complex or sensitive for automation

Return ONLY the category name."""},
            {"role": "user", "content": query}
        ],
        temperature=0
    )
    return ToolRoute(response.choices[0].message.content.strip())
```

---

## Agent Evaluation Metrics

| Metric | What it measures | How to compute |
|--------|-----------------|----------------|
| **Task completion rate** | % of tasks fully solved | Pass/fail on ground-truth test set |
| **Steps to completion** | Efficiency | Avg tool calls per task |
| **Tool call accuracy** | Correct tool + arguments | Compare to expected tool sequence |
| **Hallucination rate** | Invented facts/tool calls | Human eval or LLM judge |
| **Latency p95** | Worst-case response time | Percentile of wall-clock times |

```python
# LLM-as-judge for agent evaluation
def evaluate_agent_response(task: str, agent_answer: str, ground_truth: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are an evaluator. Score the agent answer 0-10 for correctness vs ground truth. Return JSON: {score: int, reason: str}"},
            {"role": "user", "content": f"Task: {task}\nAgent answer: {agent_answer}\nGround truth: {ground_truth}"}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)
```
