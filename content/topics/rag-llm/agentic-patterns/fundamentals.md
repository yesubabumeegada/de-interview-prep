---
title: "Agentic Patterns - Fundamentals"
topic: rag-llm
subtopic: agentic-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [rag, llm, agents, tool-use, react, function-calling]
---

# Agentic Patterns — Fundamentals

## Why This Matters for Data Engineers

LLM agents are moving from demos to production. As a DE, you'll build pipelines where an LLM decides what SQL to run, which API to call, or when to escalate to a human. Understanding the core patterns—especially tool calling and the ReAct loop—is now a baseline skill for senior DE roles.

---

## What Is an LLM Agent?

An **agent** is an LLM that can take actions: it reasons about a goal, selects a tool, executes it, observes the result, and loops until done. The key difference from a plain chat model is the **action loop**.

```
Goal → Reason → Select Tool → Execute → Observe → Reason → ... → Answer
```

Three core components:
1. **LLM (the brain)** — decides what to do next
2. **Tools** — functions the LLM can invoke (SQL query, web search, calculator)
3. **Memory** — state the agent carries across steps

---

## The ReAct Pattern (Reason + Act)

ReAct is the foundational agentic loop, introduced in the 2022 paper *"ReAct: Synergizing Reasoning and Acting in Language Models"*.

Each step has:
- **Thought** — the LLM's reasoning (not shown to user)
- **Action** — a tool call with arguments
- **Observation** — the tool's return value

```
Thought: I need to find the total sales for Q1 2024.
Action: run_sql("SELECT SUM(revenue) FROM sales WHERE quarter='Q1' AND year=2024")
Observation: [{"SUM(revenue)": 4500000}]
Thought: The total is $4.5M. I can answer the user now.
Answer: Q1 2024 total sales were $4.5M.
```

---

## Function/Tool Calling

Modern LLMs have native tool-calling support. You define tools as JSON schemas; the model returns structured tool invocations.

### OpenAI Tool Calling

```python
from openai import OpenAI
import json

client = OpenAI()

tools = [
    {
        "type": "function",
        "function": {
            "name": "run_sql_query",
            "description": "Execute a read-only SQL query and return results as JSON",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL SELECT query to execute"
                    },
                    "database": {
                        "type": "string",
                        "enum": ["warehouse", "staging", "analytics"],
                        "description": "Target database"
                    }
                },
                "required": ["query", "database"]
            }
        }
    }
]

def run_sql_query(query: str, database: str) -> dict:
    # In production: connect to your DWH (Snowflake, BigQuery, etc.)
    return {"rows": [{"revenue": 4500000}], "row_count": 1}

def agent_loop(user_message: str) -> str:
    messages = [{"role": "user", "content": user_message}]

    while True:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=tools,
            tool_choice="auto"
        )
        msg = response.choices[0].message

        # No tool call — we have a final answer
        if not msg.tool_calls:
            return msg.content

        # Execute each tool call
        messages.append(msg)  # append assistant message with tool_calls
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            result = run_sql_query(**args)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result)
            })

answer = agent_loop("What were total sales in Q1 2024?")
print(answer)
```

### Anthropic tool_use

```python
import anthropic
import json

client = anthropic.Anthropic()

tools = [
    {
        "name": "run_sql_query",
        "description": "Execute a read-only SQL query",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "database": {"type": "string"}
            },
            "required": ["query", "database"]
        }
    }
]

messages = [{"role": "user", "content": "What were total sales in Q1 2024?"}]

while True:
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        tools=tools,
        messages=messages
    )

    if response.stop_reason == "end_turn":
        # Extract text from response
        print(next(b.text for b in response.content if b.type == "text"))
        break

    # Process tool use blocks
    messages.append({"role": "assistant", "content": response.content})
    tool_results = []
    for block in response.content:
        if block.type == "tool_use":
            result = run_sql_query(**block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(result)
            })
    messages.append({"role": "user", "content": tool_results})
```

---

## Agent Memory Types

| Type | Where stored | Lifetime | Example |
|------|-------------|----------|---------|
| **In-context** | The prompt itself | Single session | Conversation history |
| **External (vector)** | Vector DB (Pinecone, pgvector) | Persistent | Past query results |
| **Episodic** | KV store (Redis, DynamoDB) | Configurable | User preferences |
| **Semantic** | Structured DB | Persistent | Entity facts |

---

## Common Failure Modes

- **Infinite loops** — agent keeps calling tools without converging; always set `max_iterations`
- **Hallucinated tool calls** — model invents tool names or arguments that don't exist; validate against schema
- **Stale observations** — agent ignores new tool results; ensure observations are always appended
- **Context overflow** — long tool outputs fill the context window; truncate or summarize observations

---

## Key Terms

| Term | Definition |
|------|-----------|
| **ReAct** | Reason + Act loop; foundational agentic pattern |
| **Tool/Function calling** | LLM returns structured JSON to invoke external functions |
| **Orchestrator** | Top-level agent that plans and delegates |
| **Subagent** | Specialized agent that executes a specific task |
| **Human-in-the-loop** | Checkpoint where a human must approve before the agent continues |
