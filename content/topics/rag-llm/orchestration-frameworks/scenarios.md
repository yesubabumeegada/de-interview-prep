---
title: "Orchestration Frameworks - Scenario Questions"
topic: rag-llm
subtopic: orchestration-frameworks
content_type: scenario_question
tags: [rag, llm, orchestration, langchain, langgraph, interview, scenarios]
---

# Scenario Questions — Orchestration Frameworks

<article data-difficulty="junior">

## 🟢 Junior: Basic Chain Setup

**Scenario:** Build a simple RAG chain using LangChain that: embeds a user question, retrieves from a Qdrant vector store, and generates an answer with GPT-4o-mini. Show the complete working code.

<details>
<summary>💡 Hint</summary>
Use LCEL pipe syntax: retriever | format_docs | prompt | llm | parser. The retriever returns documents, you format them into a string, inject into prompt, pass to LLM.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import Qdrant
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# Initialize components
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vectorstore = Qdrant.from_existing_collection(
    embeddings=embeddings, collection_name="docs", url="http://localhost:6333"
)
retriever = vectorstore.as_retriever(search_kwargs={"k": 5})
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# Prompt template
prompt = ChatPromptTemplate.from_template("""Answer based only on the context below.
If the context doesn't contain the answer, say "I don't have that information."

Context: {context}

Question: {question}

Answer:""")

# Helper to format retrieved docs
def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

# LCEL chain
rag_chain = (
    {"context": retriever | format_docs, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)

# Use it
answer = rag_chain.invoke("How does Spark handle data skew?")
print(answer)
```

**Key Points:**
- LCEL pipe (`|`) chains components sequentially
- `RunnablePassthrough()` passes the input unchanged (question goes through as-is)
- `retriever | format_docs` retrieves docs then formats them into a string
- The dict `{"context": ..., "question": ...}` creates the variables needed by the prompt
- This is the standard minimal RAG pattern in LangChain

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Choosing a Framework

**Scenario:** You're starting a new project: a Q&A bot over 10K internal wiki pages. Your team of 3 has moderate Python experience but no LLM framework experience. The PM wants a working prototype in 2 weeks. Which framework (or no framework) do you recommend?

<details>
<summary>💡 Hint</summary>
Consider: team experience, timeline, complexity of the use case, and long-term maintenance. A simple RAG bot over static docs doesn't need a complex framework.
</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: LlamaIndex for the prototype, custom code for production**

```python
# Week 1: LlamaIndex prototype (fastest time to working demo)
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, Settings
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding

Settings.llm = OpenAI(model="gpt-4o-mini")
Settings.embed_model = OpenAIEmbedding(model="text-embedding-3-small")

# Load all wiki pages, chunk, embed, index — 3 lines!
docs = SimpleDirectoryReader("./wiki_pages").load_data()
index = VectorStoreIndex.from_documents(docs)
query_engine = index.as_query_engine(similarity_top_k=5)

# Query
response = query_engine.query("What's our deployment process?")
print(response)

# Week 2: Add persistence, API endpoint, basic UI
# Store index to Qdrant for persistence
# Wrap in FastAPI for team access
# Add basic Streamlit/Gradio UI
```

| Option | Time to Prototype | Learning Curve | Production Ready |
|--------|------------------|---------------|-----------------|
| LlamaIndex | 1-2 days | Low | Good for RAG |
| LangChain | 2-3 days | Medium | Good, more flexible |
| Custom code | 3-5 days | Low (just APIs) | Best control |
| No framework | - | - | Recommended after prototype validates the use case |

**Key Points:**
- For 10K docs with straightforward Q&A: LlamaIndex gets you running fastest
- LangChain would be better if you plan to add agents/tools later
- After prototype proves value: refactor to custom code for production (remove framework overhead)
- 2-week timeline: framework is justified (saves setup time)
- 2-month timeline: custom code may be better (less to learn/maintain)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Tool Usage

**Scenario:** Your RAG bot needs to answer questions that require calculations. User asks: "If our S3 bucket has 2.5TB and we pay $0.023/GB/month, what's our monthly cost?" The knowledge base doesn't contain this specific calculation. How do you add a calculator tool?

<details>
<summary>💡 Hint</summary>
Define a tool function with the `@tool` decorator. Give it a clear description so the LLM knows when to use it. Create an agent that can choose between search and calculation.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain.tools import tool
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

@tool
def calculator(expression: str) -> str:
    """Calculate a mathematical expression. Use for cost calculations, 
    unit conversions, or any math. Example: '2500 * 0.023' or '1024 * 1024'"""
    try:
        # Safe eval (no builtins)
        result = eval(expression, {"__builtins__": {}}, {})
        return f"{expression} = {result}"
    except Exception as e:
        return f"Error: {e}"

@tool
def search_docs(query: str) -> str:
    """Search the knowledge base for information about AWS services, 
    pricing, best practices, and technical documentation."""
    docs = retriever.invoke(query)
    return "\n".join([d.page_content[:300] for d in docs[:3]])

# Agent prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful data engineering assistant. Use tools to answer questions. "
     "Use calculator for math, search_docs for technical information."),
    ("user", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

# Create agent
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
agent = create_openai_tools_agent(llm, [calculator, search_docs], prompt)
executor = AgentExecutor(agent=agent, tools=[calculator, search_docs], verbose=True)

# Query
result = executor.invoke({"input": "If our S3 bucket has 2.5TB and we pay $0.023/GB/month, what's our monthly cost?"})
# Agent thinks: "I need to calculate 2500 * 0.023"
# Calls: calculator("2500 * 0.023")
# Gets: "2500 * 0.023 = 57.5"
# Answers: "Your monthly S3 storage cost is $57.50"
```

**Key Points:**
- Tool docstring is critical — the LLM uses it to decide WHEN to call the tool
- Clear, specific descriptions prevent wrong tool usage
- Agent decides autonomously: "Do I need calculation or search for this question?"
- `verbose=True` shows the agent's reasoning steps (helpful for debugging)
- Keep tools simple and focused — one responsibility per tool

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Memory Configuration

**Scenario:** Users complain that your chatbot "forgets" what they said 2 messages ago. They ask "What is Spark AQE?" then "How does it improve performance?" and the bot doesn't know what "it" refers to. Add conversation memory.

<details>
<summary>💡 Hint</summary>
Use ConversationBufferMemory to store chat history. Pass it to the chain so each response includes prior context.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain.memory import ConversationBufferWindowMemory
from langchain.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI

# Memory: stores last 5 conversation turns
memory = ConversationBufferWindowMemory(
    k=5,                           # Keep last 5 exchanges
    memory_key="chat_history",
    return_messages=True,
    output_key="answer",
)

# Conversational RAG chain
chain = ConversationalRetrievalChain.from_llm(
    llm=ChatOpenAI(model="gpt-4o-mini", temperature=0),
    retriever=vectorstore.as_retriever(search_kwargs={"k": 5}),
    memory=memory,
    return_source_documents=True,
    verbose=True,
)

# Turn 1
result = chain.invoke({"question": "What is Spark AQE?"})
print(result["answer"])
# "Spark AQE (Adaptive Query Execution) is a feature in Spark 3.0+ that..."

# Turn 2 — "it" now resolves correctly thanks to memory!
result = chain.invoke({"question": "How does it improve performance?"})
print(result["answer"])
# "Spark AQE improves performance by: 1) coalescing small partitions..."
# The chain internally rewrites "it" → "Spark AQE" using chat history
```

**Key Points:**
- `ConversationBufferWindowMemory(k=5)` keeps last 5 turns (bounded memory)
- The chain automatically uses history to contextualize follow-up questions
- Without memory: "How does it improve?" → searches for generic "improve" → wrong results
- With memory: resolves "it" = "Spark AQE" → searches specifically for AQE performance
- For production: store memory in Redis/PostgreSQL (not in-process)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Error Handling

**Scenario:** Your LangChain RAG chain crashes when the vector database is temporarily unavailable (network timeout). Users see a 500 error. Add graceful error handling.

<details>
<summary>💡 Hint</summary>
Wrap the chain invocation in try/except. Provide a fallback response when retrieval fails. Consider using LangChain's built-in fallback mechanism.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain_core.runnables import RunnableWithFallbacks
import logging

logger = logging.getLogger(__name__)

# Method 1: Simple try/except wrapper
def safe_query(question: str) -> dict:
    """Query RAG with graceful error handling."""
    try:
        answer = rag_chain.invoke(question)
        return {"answer": answer, "status": "success"}
    except Exception as e:
        logger.error(f"RAG chain failed: {e}")
        
        if "timeout" in str(e).lower() or "connection" in str(e).lower():
            return {
                "answer": "I'm temporarily unable to search our documentation. Please try again in a moment.",
                "status": "retrieval_failed",
                "error": str(e),
            }
        else:
            return {
                "answer": "I encountered an issue processing your question. Please rephrase and try again.",
                "status": "error",
                "error": str(e),
            }

# Method 2: LangChain fallback chains
from langchain_openai import ChatOpenAI

# Primary: full RAG with retrieval
primary_chain = rag_chain

# Fallback: just ask the LLM without retrieval (degraded but functional)
fallback_chain = (
    ChatPromptTemplate.from_template("Answer this data engineering question: {question}")
    | ChatOpenAI(model="gpt-4o-mini")
    | StrOutputParser()
)

# Automatic fallback on error
robust_chain = primary_chain.with_fallbacks([fallback_chain])

# If vector DB is down → automatically falls back to LLM-only response
answer = robust_chain.invoke("How does Spark handle skew?")
```

**Key Points:**
- Never let raw exceptions reach the user — always return a helpful message
- Differentiate error types: timeout (retry later) vs bad input (rephrase)
- LangChain's `with_fallbacks()` provides automatic chain-level fallback
- Degraded response > no response (LLM-only answer is better than a 500 error)
- Log all errors for debugging (include the question that failed)
- In production: add alerting when fallback rate exceeds threshold

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: LCEL Pipeline Design

**Scenario:** Build a RAG pipeline that: (1) classifies the query type, (2) routes to different retrieval strategies based on type, (3) re-ranks results, (4) generates with streaming. Use LCEL for the full pipeline.

<details>
<summary>💡 Hint</summary>
Use RunnableBranch or conditional routing in LCEL. Chain: classify → branch(factual→keyword_search, conceptual→semantic_search) → rerank → generate_stream.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain_core.runnables import RunnableBranch, RunnableLambda, RunnableParallel
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# Step 1: Query classifier
classify_prompt = ChatPromptTemplate.from_template(
    "Classify this query as 'factual' or 'conceptual' (one word only): {question}"
)
classifier = classify_prompt | llm | StrOutputParser()

# Step 2: Different retrieval strategies
def factual_retrieval(question: str) -> list:
    """Keyword-heavy search for factual queries (config values, error codes)."""
    return hybrid_retriever.invoke(question, search_kwargs={"alpha": 0.3})  # More BM25

def conceptual_retrieval(question: str) -> list:
    """Broad semantic search for conceptual queries."""
    return semantic_retriever.invoke(question, search_kwargs={"k": 8})

# Step 3: Re-ranking
def rerank(docs_and_question: dict) -> str:
    """Cross-encoder re-rank top results."""
    question = docs_and_question["question"]
    docs = docs_and_question["docs"]
    pairs = [(question, doc.page_content) for doc in docs]
    scores = cross_encoder.predict(pairs)
    ranked = sorted(zip(docs, scores), key=lambda x: x[1], reverse=True)
    return "\n\n".join([doc.page_content for doc, _ in ranked[:5]])

# Step 4: Generation prompt
gen_prompt = ChatPromptTemplate.from_template(
    "Context:\n{context}\n\nQuestion: {question}\n\nAnswer (cite sources):"
)

# Full pipeline
def full_pipeline(question: str):
    # Classify
    query_type = classifier.invoke({"question": question}).strip().lower()
    
    # Route retrieval
    if "factual" in query_type:
        docs = factual_retrieval(question)
    else:
        docs = conceptual_retrieval(question)
    
    # Rerank
    context = rerank({"question": question, "docs": docs})
    
    # Generate (streaming)
    gen_chain = gen_prompt | llm | StrOutputParser()
    return gen_chain.stream({"context": context, "question": question})

# Usage with streaming
for chunk in full_pipeline("What is the default value of spark.sql.shuffle.partitions?"):
    print(chunk, end="", flush=True)
```

**Key Points:**
- Classification adds ~200ms but dramatically improves retrieval quality
- Factual queries (config values, error codes) → more weight on keyword/BM25 search
- Conceptual queries (explanations, how-to) → broader semantic search
- Re-ranking adds ~80ms but ensures best docs are first (LLM pays most attention to early context)
- Streaming: user sees first tokens immediately despite multi-step pipeline
- Total latency: ~200ms (classify) + 10ms (retrieve) + 80ms (rerank) + 200ms (first token) = ~500ms

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Agent with Multiple Tools

**Scenario:** Build a data engineering assistant agent that can: search documentation, query a SQL database, check job status via API, and perform calculations. The agent should decide which tools to use based on the question.

<details>
<summary>💡 Hint</summary>
Define each tool with clear descriptions. The LLM uses descriptions to select tools. Set max_iterations to prevent infinite loops. Use verbose mode for debugging.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain.tools import tool
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_openai import ChatOpenAI
import requests

@tool
def search_documentation(query: str) -> str:
    """Search internal documentation for technical information about 
    data engineering tools, best practices, and architecture decisions."""
    docs = retriever.invoke(query)
    return "\n---\n".join([d.page_content[:400] for d in docs[:3]])

@tool
def query_database(sql: str) -> str:
    """Execute a READ-ONLY SQL query against the analytics database.
    Use for questions about actual data, metrics, counts, or aggregations.
    Only SELECT statements are allowed. Tables: fact_orders, dim_customers, dim_products."""
    if not sql.strip().upper().startswith("SELECT"):
        return "ERROR: Only SELECT queries allowed for safety."
    try:
        result = db.execute(sql)
        rows = result.fetchmany(10)
        return f"Results ({len(rows)} rows):\n{rows}"
    except Exception as e:
        return f"SQL Error: {e}. Check table/column names."

@tool
def check_job_status(job_name: str) -> str:
    """Check the current status of a data pipeline job.
    Returns: status (running/succeeded/failed), last run time, duration."""
    try:
        resp = requests.get(f"http://airflow-api:8080/api/v1/dags/{job_name}/dagRuns?limit=1",
                           headers={"Authorization": "Bearer ..."}, timeout=5)
        data = resp.json()["dag_runs"][0]
        return f"Job: {job_name} | Status: {data['state']} | Started: {data['start_date']} | Duration: {data.get('duration', 'N/A')}s"
    except Exception as e:
        return f"Could not fetch job status: {e}"

@tool
def calculator(expression: str) -> str:
    """Evaluate a math expression. Use for cost calculations, capacity planning,
    or unit conversions. Example: '1024 * 1024 * 4 / 1000000000' for GB conversion."""
    try:
        return str(eval(expression, {"__builtins__": {}}, {}))
    except Exception as e:
        return f"Calculation error: {e}"

# Create agent
prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a senior data engineering assistant. 
Use the available tools to answer questions accurately:
- search_documentation: for technical concepts and best practices
- query_database: for actual data/metrics (write valid SQL)
- check_job_status: for pipeline status checks
- calculator: for math operations

Think step by step. If one tool doesn't give you enough info, try another."""),
    ("user", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

agent = create_openai_tools_agent(
    llm=ChatOpenAI(model="gpt-4o", temperature=0),
    tools=[search_documentation, query_database, check_job_status, calculator],
    prompt=prompt,
)

executor = AgentExecutor(
    agent=agent,
    tools=[search_documentation, query_database, check_job_status, calculator],
    verbose=True,
    max_iterations=5,           # Prevent infinite tool loops
    handle_parsing_errors=True, # Gracefully handle malformed tool calls
)

# Example: "How many orders did we process yesterday and is that normal?"
# Agent: 1) query_database("SELECT COUNT(*) FROM fact_orders WHERE order_date = CURRENT_DATE - 1")
#         2) search_documentation("normal daily order volume benchmark")
#         3) Final: "Yesterday we processed 45,230 orders. Our docs indicate normal range is 40K-50K."
```

**Key Points:**
- Tool descriptions are the agent's "instruction manual" — make them precise
- max_iterations=5 prevents runaway loops (agent keeps trying if it fails)
- Use GPT-4o for agents (better at tool selection than mini)
- SQL tool: always validate input (SELECT only) to prevent accidents
- Verbose mode: essential for debugging agent behavior during development
- In production: replace verbose with structured logging/tracing

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Streaming Responses

**Scenario:** Your RAG API takes 3-4 seconds for a full response. Users perceive this as slow. Implement streaming so they see the first word within 500ms while the rest generates.

<details>
<summary>💡 Hint</summary>
Use LangChain's `.stream()` method on the chain. For API delivery, use Server-Sent Events (SSE) via FastAPI's StreamingResponse.
</details>

<details>
<summary>✅ Solution</summary>

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import asyncio

app = FastAPI()

# Streaming-capable chain
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, streaming=True)
prompt = ChatPromptTemplate.from_template(
    "Context: {context}\n\nQuestion: {question}\n\nAnswer:"
)
gen_chain = prompt | llm | StrOutputParser()

@app.post("/query/stream")
async def stream_query(question: str):
    """Stream RAG response token-by-token via SSE."""
    
    # Retrieval (non-streaming, fast)
    docs = retriever.invoke(question)
    context = "\n".join([d.page_content for d in docs[:5]])
    
    # Generation (streaming)
    async def event_stream():
        async for chunk in gen_chain.astream({"context": context, "question": question}):
            yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")

# Client-side consumption (JavaScript):
# const evtSource = new EventSource('/query/stream?question=...');
# evtSource.onmessage = (e) => { 
#   if (e.data === '[DONE]') evtSource.close();
#   else document.getElementById('answer').textContent += e.data;
# };

# Timeline:
# 0ms: Request received
# 50ms: Query embedded (local model)
# 60ms: Vector search complete
# 80ms: Re-ranking complete
# ~300ms: First token from LLM arrives → sent to user immediately
# 300-3000ms: Remaining tokens stream in one by one
# User perception: "fast!" (saw first word in 300ms, not 3000ms)
```

**Key Points:**
- Streaming doesn't reduce total time — it reduces PERCEIVED latency
- First token in 300ms vs full response in 3s = dramatically better UX
- Retrieval must complete before generation starts (can't stream retrieval)
- Use `astream()` (async) for production — doesn't block the event loop
- SSE (Server-Sent Events) is simpler than WebSockets for one-way streaming
- Client handles `[DONE]` signal to know when response is complete

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Callback Implementation

**Scenario:** You need to track: total tokens used, latency per component (retrieval, generation), and which tools the agent called. Implement a custom callback handler for production monitoring.

<details>
<summary>💡 Hint</summary>
Implement `BaseCallbackHandler` with methods for each lifecycle event: on_llm_start, on_llm_end, on_tool_start, on_retriever_start, etc. Emit metrics to Prometheus.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langchain_core.callbacks import BaseCallbackHandler
from prometheus_client import Histogram, Counter
import time

# Prometheus metrics
LLM_LATENCY = Histogram("langchain_llm_latency_seconds", "LLM call duration", ["model"])
LLM_TOKENS = Counter("langchain_tokens_total", "Total tokens used", ["type"])
TOOL_CALLS = Counter("langchain_tool_calls_total", "Tool invocations", ["tool_name"])
RETRIEVER_LATENCY = Histogram("langchain_retriever_latency_seconds", "Retriever duration")

class ProductionCallbackHandler(BaseCallbackHandler):
    """Emit metrics for every LangChain operation."""
    
    def __init__(self):
        self.timers = {}
    
    def on_llm_start(self, serialized, prompts, **kwargs):
        self.timers["llm"] = time.time()
    
    def on_llm_end(self, response, **kwargs):
        duration = time.time() - self.timers.get("llm", time.time())
        model = response.llm_output.get("model_name", "unknown") if response.llm_output else "unknown"
        LLM_LATENCY.labels(model=model).observe(duration)
        
        # Token counting
        if response.llm_output and "token_usage" in response.llm_output:
            usage = response.llm_output["token_usage"]
            LLM_TOKENS.labels(type="input").inc(usage.get("prompt_tokens", 0))
            LLM_TOKENS.labels(type="output").inc(usage.get("completion_tokens", 0))
    
    def on_tool_start(self, serialized, input_str, **kwargs):
        tool_name = serialized.get("name", "unknown")
        TOOL_CALLS.labels(tool_name=tool_name).inc()
        self.timers[f"tool_{tool_name}"] = time.time()
    
    def on_retriever_start(self, serialized, query, **kwargs):
        self.timers["retriever"] = time.time()
    
    def on_retriever_end(self, documents, **kwargs):
        duration = time.time() - self.timers.get("retriever", time.time())
        RETRIEVER_LATENCY.observe(duration)
    
    def on_chain_error(self, error, **kwargs):
        # Log errors for debugging
        import logging
        logging.error(f"Chain error: {error}")

# Usage: attach to any chain or agent
callbacks = [ProductionCallbackHandler()]
result = rag_chain.invoke("...", config={"callbacks": callbacks})

# Grafana dashboard shows:
# - LLM latency p50/p99 by model
# - Token usage (daily cost tracking)
# - Tool call frequency (which tools are used most)
# - Retriever latency (vector DB health)
# - Error rates
```

**Key Points:**
- Callbacks fire automatically on every LangChain operation (no code changes to chains)
- Track latency per component to identify bottlenecks (usually LLM generation)
- Token counting enables cost tracking ($X/day)
- Tool call counts show agent behavior patterns (is it using the right tools?)
- Error callbacks catch and log failures without breaking the chain
- In production: emit to Prometheus + visualize in Grafana

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: LangGraph State Machine

**Scenario:** Build a self-correcting RAG system using LangGraph: retrieve → generate → verify (is the answer grounded?) → if not grounded: re-retrieve with transformed query → regenerate. Maximum 3 attempts.

<details>
<summary>💡 Hint</summary>
Model as a state graph with conditional edges. State tracks: question, context, answer, quality_score, attempt_count. Conditional edge after "verify": if grounded → END, if not → back to retrieve (with transformed query).
</details>

<details>
<summary>✅ Solution</summary>

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict
import json

class CorrectionState(TypedDict):
    original_question: str
    current_query: str        # May be rewritten
    context: list[str]
    answer: str
    is_grounded: bool
    attempts: int
    feedback: str             # Why it wasn't grounded

def retrieve(state: CorrectionState) -> CorrectionState:
    """Retrieve documents using current query."""
    results = vector_db.search(embed(state["current_query"]), top_k=5)
    state["context"] = [r.text for r in results]
    return state

def generate(state: CorrectionState) -> CorrectionState:
    """Generate answer from context."""
    context_str = "\n".join(state["context"])
    response = llm.invoke(
        f"Answer based ONLY on context. Context:\n{context_str}\n\nQuestion: {state['original_question']}\nAnswer:"
    )
    state["answer"] = response.content
    return state

def verify_groundedness(state: CorrectionState) -> CorrectionState:
    """Check if every claim in the answer is supported by context."""
    response = llm.invoke(f"""Check if this answer is fully supported by the context.

Context: {chr(10).join(state['context'][:3])}
Answer: {state['answer']}

Respond JSON: {{"grounded": true/false, "feedback": "what's missing or unsupported"}}""")
    
    result = json.loads(response.content)
    state["is_grounded"] = result["grounded"]
    state["feedback"] = result.get("feedback", "")
    state["attempts"] = state.get("attempts", 0) + 1
    return state

def transform_query(state: CorrectionState) -> CorrectionState:
    """Rewrite query based on verification feedback."""
    response = llm.invoke(
        f"Original question: {state['original_question']}\n"
        f"Previous search didn't find: {state['feedback']}\n"
        f"Rewrite the search query to find the missing information:"
    )
    state["current_query"] = response.content
    return state

def should_retry(state: CorrectionState) -> str:
    """Decide: accept answer or retry."""
    if state["is_grounded"]:
        return "accept"
    elif state["attempts"] >= 3:
        return "accept"  # Give up after 3 attempts, return best effort
    else:
        return "retry"

# Build graph
graph = StateGraph(CorrectionState)

graph.add_node("retrieve", retrieve)
graph.add_node("generate", generate)
graph.add_node("verify", verify_groundedness)
graph.add_node("transform_query", transform_query)

graph.set_entry_point("retrieve")
graph.add_edge("retrieve", "generate")
graph.add_edge("generate", "verify")
graph.add_conditional_edges("verify", should_retry, {
    "accept": END,
    "retry": "transform_query",
})
graph.add_edge("transform_query", "retrieve")  # Loop back

app = graph.compile()

# Invoke
result = app.invoke({
    "original_question": "What's the memory overhead for Spark broadcast joins?",
    "current_query": "What's the memory overhead for Spark broadcast joins?",
    "context": [],
    "answer": "",
    "is_grounded": False,
    "attempts": 0,
    "feedback": "",
})

# Flow: retrieve → generate → verify (not grounded) → transform → retrieve → generate → verify (grounded) → END
```

**Key Points:**
- LangGraph's conditional edges enable loops with termination conditions
- State accumulates across iterations (feedback informs query transformation)
- max attempts (3) prevents infinite loops
- Query transformation uses the "why it wasn't grounded" feedback to improve retrieval
- This pattern catches hallucinations before they reach the user
- Adds 1-3 seconds for correction iterations but dramatically improves accuracy

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Agent Orchestration

**Scenario:** Build a research assistant with 3 specialized agents: (1) Documentation Researcher (searches technical docs), (2) Data Analyst (writes and executes SQL), (3) Synthesizer (combines findings into a coherent answer). They should collaborate on complex questions.

<details>
<summary>💡 Hint</summary>
Use LangGraph with a supervisor pattern or sequential handoff. Each agent has its own tools and expertise. The supervisor decides which agent to invoke next based on what information is still needed.
</details>

<details>
<summary>✅ Solution</summary>

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict

class ResearchState(TypedDict):
    question: str
    doc_findings: list[str]
    data_findings: list[str]
    final_answer: str
    agents_used: list[str]

def supervisor(state: ResearchState) -> str:
    """Decide which agent to run next."""
    question = state["question"].lower()
    
    # If we have no doc findings yet, start with researcher
    if not state["doc_findings"]:
        return "researcher"
    
    # If question needs data and we haven't queried yet
    if not state["data_findings"] and any(w in question for w in ["how many", "count", "average", "total", "cost"]):
        return "analyst"
    
    # We have enough info, synthesize
    return "synthesizer"

def researcher_agent(state: ResearchState) -> ResearchState:
    """Searches documentation for relevant technical information."""
    docs = retriever.invoke(state["question"])
    findings = [d.page_content[:500] for d in docs[:3]]
    state["doc_findings"] = findings
    state["agents_used"].append("researcher")
    return state

def analyst_agent(state: ResearchState) -> ResearchState:
    """Generates and executes SQL to answer data questions."""
    # Generate SQL based on the question + schema context
    sql = llm.invoke(
        f"Generate SQL for: {state['question']}\nSchema: {SCHEMA}\nSQL only:"
    ).content
    
    try:
        result = db.execute(sql)
        rows = result.fetchmany(5)
        state["data_findings"] = [f"Query: {sql}\nResult: {rows}"]
    except Exception as e:
        state["data_findings"] = [f"SQL failed: {e}"]
    
    state["agents_used"].append("analyst")
    return state

def synthesizer_agent(state: ResearchState) -> ResearchState:
    """Combines all findings into a coherent answer."""
    all_context = "\n\n".join(
        [f"[Documentation]: {f}" for f in state["doc_findings"]] +
        [f"[Data Analysis]: {f}" for f in state["data_findings"]]
    )
    
    response = llm.invoke(
        f"Synthesize a comprehensive answer using all available information.\n\n"
        f"Question: {state['question']}\n\n"
        f"Available Information:\n{all_context}\n\n"
        f"Comprehensive Answer:"
    )
    state["final_answer"] = response.content
    state["agents_used"].append("synthesizer")
    return state

# Build multi-agent graph
graph = StateGraph(ResearchState)
graph.add_node("researcher", researcher_agent)
graph.add_node("analyst", analyst_agent)
graph.add_node("synthesizer", synthesizer_agent)

graph.set_entry_point("researcher")
graph.add_conditional_edges("researcher", supervisor, {
    "analyst": "analyst",
    "synthesizer": "synthesizer",
})
graph.add_edge("analyst", "synthesizer")
graph.add_edge("synthesizer", END)

multi_agent = graph.compile()

# Usage
result = multi_agent.invoke({
    "question": "How many orders did we process last quarter and does that match our capacity planning docs?",
    "doc_findings": [],
    "data_findings": [],
    "final_answer": "",
    "agents_used": [],
})

# Flow: researcher (finds capacity docs) → analyst (queries order counts) → synthesizer (combines both)
print(f"Agents used: {result['agents_used']}")  # ['researcher', 'analyst', 'synthesizer']
print(result["final_answer"])
```

**Key Points:**
- Each agent has specialized tools and expertise (separation of concerns)
- Supervisor decides routing based on what info is still needed
- Sequential handoff: each agent builds on previous findings
- For more complex cases: add cycles (analyst asks researcher for clarification)
- Production: add timeout per agent, max total time, and graceful degradation
- Tracing: track which agents were used and what each contributed

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Scaling

**Scenario:** Your LangChain RAG service handles 50 QPS but you need 500 QPS. The bottleneck is: 150ms embedding (API call), 10ms vector search, 80ms reranking (local), 1.5s LLM generation. Design the scaling architecture.

<details>
<summary>💡 Hint</summary>
Bottleneck analysis: LLM generation is 75% of time and has API rate limits. Solutions: local embedding (remove API call), response caching, request batching, async processing, and horizontal scaling of API servers.
</details>

<details>
<summary>✅ Solution</summary>

```python
import asyncio
from functools import lru_cache

class ScaledRAGService:
    """RAG service optimized for 500 QPS."""
    
    def __init__(self):
        # Optimization 1: Local embedding (150ms API → 10ms local)
        self.embedder = SentenceTransformer("all-MiniLM-L6-v2", device="cuda")
        
        # Optimization 2: Response cache (30% hit rate → 150 fewer QPS to handle)
        self.cache = RedisCache(url="redis://cache-cluster:6379", ttl=3600)
        
        # Optimization 3: Connection pool for vector DB
        self.vector_pool = ConnectionPool(max_connections=100)
        
        # Optimization 4: LLM semaphore (respect rate limits)
        self.llm_sem = asyncio.Semaphore(100)  # Max 100 concurrent LLM calls
        
        # Optimization 5: Pre-warmed reranker
        self.reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", device="cuda")
    
    async def answer(self, question: str) -> dict:
        # Check cache first (0ms if hit)
        cached = await self.cache.get(question)
        if cached:
            return cached
        
        # Embed locally (10ms vs 150ms API)
        query_vec = self.embedder.encode(question)
        
        # Retrieve (10ms, pooled connection)
        async with self.vector_pool.get() as conn:
            results = await conn.search(query_vec, limit=20)
        
        # Rerank (80ms, local GPU)
        pairs = [(question, r.text) for r in results]
        scores = self.reranker.predict(pairs)
        top_5 = sorted(zip(results, scores), key=lambda x: x[1], reverse=True)[:5]
        context = "\n".join([r.text for r, _ in top_5])
        
        # Generate (1.5s, rate-limited)
        async with self.llm_sem:
            answer = await openai_client.chat.completions.create(
                model="gpt-4o-mini", messages=[...], stream=False
            )
        
        result = {"answer": answer.choices[0].message.content}
        await self.cache.set(question, result)
        return result

# CAPACITY MATH:
# Before optimization: 150ms + 10ms + 80ms + 1500ms = 1740ms per request
# With 50 concurrent: 50 / 1.74s = 29 QPS per server (need 18 servers for 500 QPS!)

# After optimization:
# - Cache hit (30%): 500 * 0.3 = 150 QPS served instantly
# - Remaining 350 QPS need full pipeline
# - Latency: 10ms + 10ms + 80ms + 1500ms = 1600ms
# - 100 concurrent LLM calls: 100 / 1.6s = 62 QPS per server
# - Need 6 servers for 350 QPS (6 * 62 = 372 QPS capacity)

# ARCHITECTURE:
# - 6 API servers (c6g.2xlarge) behind ALB
# - 2 GPU instances (g5.xlarge) for embedding + reranking
# - 3-node Redis cluster for caching
# - 3-node Qdrant cluster for vector search
# - Monthly cost: ~$5K (vs $30K+ for 18 servers without optimization)
```

**Key Points:**
- Local embedding: removes 150ms API latency (biggest non-LLM win)
- Caching: 30% hit rate means 30% fewer expensive LLM calls
- LLM is always the bottleneck — semaphore prevents rate limit errors
- Async throughout: maximizes concurrency within each server
- 6 servers handles 500 QPS with optimization vs 18 without
- For streaming: perceived latency drops to 300ms (first token) even though total is 1.6s

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Framework vs Custom Decision

**Scenario:** Your team has been using LangChain for 6 months. Pain points: (1) debugging is hard (stack traces are 50 lines deep in framework code), (2) version updates break things monthly, (3) performance overhead is 15%, (4) half the team doesn't understand the abstractions. Should you stay or migrate to custom code?

<details>
<summary>💡 Hint</summary>
Consider: ROI of the framework (what value does it still provide?), migration cost (how much effort to replace?), ongoing maintenance cost of framework vs custom code, and team velocity.
</details>

<details>
<summary>✅ Solution</summary>

```python
# DECISION FRAMEWORK:

KEEP_FRAMEWORK_IF = [
    "You use many LangChain features (agents, tools, memory, callbacks)",
    "Your RAG pipeline is complex (multi-step, branching, self-correcting)",
    "LangSmith tracing provides value you'd have to rebuild",
    "Team is growing and framework provides guardrails for new members",
    "You're still iterating rapidly on the RAG architecture",
]

MIGRATE_TO_CUSTOM_IF = [
    "You only use basic RAG (retrieve → generate) — framework is overkill",
    "Debugging takes longer than building (abstractions hide problems)",
    "Performance overhead matters (15% latency at 500+ QPS is significant)",
    "Version updates cause breakage (technical debt accumulates)",
    "Most team members don't understand the framework (bus factor risk)",
]

# YOUR CASE: 4/5 migration signals present → MIGRATE

# MIGRATION PLAN (4 weeks):
# Week 1: Extract core RAG logic into simple classes
class CustomRAG:
    """100 lines that replace 5 LangChain modules."""
    
    async def answer(self, question: str) -> dict:
        # Direct API calls — no framework overhead, full debuggability
        embedding = await self.embed(question)              # 10ms
        results = await self.vector_db.search(embedding)    # 10ms  
        reranked = self.rerank(question, results)           # 80ms
        answer = await self.generate(question, reranked)    # 1.5s
        return {"answer": answer, "sources": results}

# Week 2: Replace LangSmith with custom tracing
import structlog
logger = structlog.get_logger()

async def answer_with_tracing(self, question: str) -> dict:
    with logger.bind(query_id=uuid4(), question=question):
        logger.info("rag_start")
        t0 = time.time()
        
        embedding = await self.embed(question)
        logger.info("embed_done", latency_ms=(time.time()-t0)*1000)
        
        results = await self.vector_db.search(embedding)
        logger.info("retrieve_done", latency_ms=(time.time()-t0)*1000, num_results=len(results))
        
        # ... etc
        logger.info("rag_complete", total_ms=(time.time()-t0)*1000)
    return result

# Week 3: Port agent logic to OpenAI function calling directly
# Week 4: Testing, performance validation, deploy

# RESULTS AFTER MIGRATION:
# - Debugging: stack traces are YOUR code (5 lines vs 50)
# - Performance: 15% latency improvement (no framework overhead)
# - Maintenance: no more breaking version updates
# - Understanding: entire team can read and modify the code
# - Lost: LangSmith UI (replaced with Grafana + structured logs)
# - Lost: Easy prototyping (now requires more code for experiments)
```

**Key Points:**
- Framework ROI decreases as your architecture stabilizes (most value is during exploration)
- Migration cost is usually 2-4 weeks for a team of 3 (less than expected)
- Custom code is more debuggable, faster, and version-stable
- You lose: community integrations, LangSmith, and rapid prototyping convenience
- Compromise: keep LangChain for R&D/prototyping, custom code for production
- Decision should be data-driven: measure time spent debugging framework vs building features

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Observability Setup

**Scenario:** Your production RAG service handles 10K queries/day but you can't answer: "Why was this specific query answered incorrectly?" Set up end-to-end observability that lets you trace any query from input to output, seeing every intermediate step.

<details>
<summary>💡 Hint</summary>
Distributed tracing: assign a trace_id per request, log every step (embed, retrieve, rerank, generate) with inputs/outputs/latency, store in a queryable system. Enable replaying any historical query.
</details>

<details>
<summary>✅ Solution</summary>

```python
import uuid
import time
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime

@dataclass
class TraceSpan:
    name: str
    start_time: float
    end_time: float = 0
    input_data: dict = field(default_factory=dict)
    output_data: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    error: str = None

@dataclass
class RAGTrace:
    trace_id: str
    question: str
    timestamp: datetime
    spans: list[TraceSpan] = field(default_factory=list)
    final_answer: str = ""
    total_latency_ms: float = 0
    
    def add_span(self, span: TraceSpan):
        self.spans.append(span)

class TracedRAGService:
    """RAG with full observability — every step is recorded."""
    
    async def answer(self, question: str) -> dict:
        trace = RAGTrace(
            trace_id=str(uuid.uuid4()),
            question=question,
            timestamp=datetime.now(),
        )
        t_start = time.time()
        
        # Span 1: Embedding
        span = TraceSpan(name="embed", start_time=time.time(), input_data={"question": question[:200]})
        query_vec = await self.embed(question)
        span.end_time = time.time()
        span.output_data = {"dimensions": len(query_vec)}
        trace.add_span(span)
        
        # Span 2: Retrieval
        span = TraceSpan(name="retrieve", start_time=time.time())
        results = await self.vector_db.search(query_vec, top_k=20)
        span.end_time = time.time()
        span.output_data = {
            "num_results": len(results),
            "top_score": results[0].score if results else 0,
            "doc_ids": [r.id for r in results[:5]],
        }
        trace.add_span(span)
        
        # Span 3: Reranking
        span = TraceSpan(name="rerank", start_time=time.time())
        reranked = self.rerank(question, results)
        span.end_time = time.time()
        span.output_data = {"top_5_ids": [r.id for r in reranked[:5]]}
        trace.add_span(span)
        
        # Span 4: Generation
        span = TraceSpan(name="generate", start_time=time.time())
        span.input_data = {"context_length": sum(len(r.text) for r in reranked[:5])}
        answer = await self.generate(question, reranked[:5])
        span.end_time = time.time()
        span.output_data = {"answer_length": len(answer), "answer_preview": answer[:200]}
        trace.add_span(span)
        
        # Complete trace
        trace.final_answer = answer
        trace.total_latency_ms = (time.time() - t_start) * 1000
        
        # Store trace (async, don't block response)
        asyncio.create_task(self.store_trace(trace))
        
        return {"answer": answer, "trace_id": trace.trace_id}
    
    async def store_trace(self, trace: RAGTrace):
        """Store in Elasticsearch/PostgreSQL for querying."""
        await self.trace_store.insert(asdict(trace))
    
    async def replay_query(self, trace_id: str) -> dict:
        """Replay a historical query to debug issues."""
        trace = await self.trace_store.get(trace_id)
        # Shows exactly what happened: which docs were retrieved, scores, final context
        return trace

# DEBUGGING WORKFLOW:
# 1. User reports: "Query X gave wrong answer"
# 2. Look up trace_id from logs
# 3. trace = await service.replay_query(trace_id)
# 4. Inspect: Was retrieval good? (check top_score, doc_ids)
#    → If retrieval failed: chunking/embedding issue
#    → If retrieval was good but answer wrong: generation/prompt issue
# 5. Fix root cause, add to evaluation test set to prevent regression
```

**Key Points:**
- Every query gets a unique trace_id (link user reports to exact system behavior)
- Each step records: input, output, latency, and any errors
- Store traces in a queryable system (Elasticsearch, PostgreSQL, or LangSmith)
- Async storage doesn't add latency to the response path
- Replay capability: re-run any historical query to understand what went wrong
- Metrics derived from traces: latency percentiles, retrieval quality, error rates
- Retention: keep 7-30 days of traces (enough for debugging, not excessive storage)

</details>

</article>
