---
title: "Orchestration Frameworks - Real-World Production Examples"
topic: rag-llm
subtopic: orchestration-frameworks
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, production, fastapi, langserve, deployment, scaling]
---

# Orchestration Frameworks — Real-World Production Examples

## Pattern 1: Production RAG API with FastAPI

```python
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio

# Models
class QueryRequest(BaseModel):
    question: str
    session_id: str = None
    top_k: int = 5

class QueryResponse(BaseModel):
    answer: str
    sources: list[dict]
    latency_ms: float

# Application lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize expensive resources
    app.state.rag = ProductionRAG()
    app.state.cache = await RedisCache.connect("redis://cache:6379")
    yield
    # Shutdown: cleanup
    await app.state.cache.close()

app = FastAPI(lifespan=lifespan)

@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Standard RAG query endpoint."""
    import time
    start = time.time()
    
    result = await app.state.rag.answer(
        question=request.question,
        session_id=request.session_id,
        top_k=request.top_k,
    )
    
    return QueryResponse(
        answer=result["answer"],
        sources=result["sources"],
        latency_ms=(time.time() - start) * 1000,
    )

@app.post("/query/stream")
async def query_stream(request: QueryRequest):
    """Streaming RAG endpoint (token-by-token)."""
    async def generate():
        async for chunk in app.state.rag.answer_stream(request.question):
            yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")

@app.post("/feedback")
async def record_feedback(query_id: str, rating: str):
    """Record user feedback (thumbs up/down)."""
    await app.state.rag.record_feedback(query_id, rating)
    return {"status": "recorded"}
```

---

## Pattern 2: Data Catalog Chatbot

A multi-source RAG chatbot that answers questions about your data infrastructure:

```python
class DataCatalogBot:
    """Chatbot that answers questions about tables, columns, lineage, and quality."""
    
    def __init__(self):
        self.sources = {
            "schema_docs": VectorRetriever("schema_descriptions"),
            "dbt_docs": VectorRetriever("dbt_model_docs"),
            "lineage": GraphRetriever("data_lineage_graph"),
            "quality": SQLRetriever("data_quality_db"),
        }
    
    async def answer(self, question: str) -> dict:
        # Step 1: Classify intent
        intent = await self.classify(question)
        
        # Step 2: Route to appropriate sources
        context = []
        if intent in ["schema", "column_meaning"]:
            docs = await self.sources["schema_docs"].search(question)
            context.extend(docs)
        
        if intent in ["lineage", "upstream", "downstream"]:
            lineage = await self.sources["lineage"].trace(question)
            context.append(f"Data Lineage:\n{lineage}")
        
        if intent in ["quality", "freshness", "row_count"]:
            metrics = await self.sources["quality"].query(question)
            context.append(f"Quality Metrics:\n{metrics}")
        
        # Always include relevant dbt docs
        dbt_docs = await self.sources["dbt_docs"].search(question, top_k=2)
        context.extend(dbt_docs)
        
        # Step 3: Generate answer
        answer = await self.generate(question, context)
        
        return {"answer": answer, "intent": intent, "sources_used": list(self.sources.keys())}
    
    async def classify(self, question: str) -> str:
        response = await llm.ainvoke(
            f"Classify: schema, column_meaning, lineage, quality, freshness, or general.\nQ: {question}\nCategory:"
        )
        return response.content.strip().lower()

# Example interactions:
# "What does the customer_ltv column in dim_customers mean?"
#   → Routes to schema_docs, returns column description + business logic
#
# "Where does fact_orders get its data from?"
#   → Routes to lineage graph, traces upstream sources
#
# "When was dim_products last updated?"
#   → Routes to quality DB, returns freshness metrics
```

---

## Pattern 3: Multi-Turn Conversation with State

```python
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite import SqliteSaver
from typing import TypedDict, Annotated

class ConversationState(TypedDict):
    messages: list[dict]           # Chat history
    current_question: str          # Latest question (standalone)
    context: list[str]             # Retrieved docs
    answer: str                    # Generated answer

def contextualize_question(state: ConversationState) -> ConversationState:
    """Rewrite question using chat history to resolve references."""
    if len(state["messages"]) <= 1:
        state["current_question"] = state["messages"][-1]["content"]
    else:
        # Use LLM to make question standalone
        history = "\n".join([f"{m['role']}: {m['content']}" for m in state["messages"][:-1]])
        latest = state["messages"][-1]["content"]
        
        standalone = llm.invoke(
            f"Rewrite this question to be self-contained.\nHistory:\n{history}\nQuestion: {latest}"
        )
        state["current_question"] = standalone.content
    return state

def retrieve(state: ConversationState) -> ConversationState:
    results = vector_db.search(embed(state["current_question"]), top_k=5)
    state["context"] = [r.text for r in results]
    return state

def generate(state: ConversationState) -> ConversationState:
    context_str = "\n".join(state["context"])
    answer = llm.invoke(
        f"Context: {context_str}\n\nQuestion: {state['current_question']}\n\nAnswer:"
    )
    state["answer"] = answer.content
    # Add to history
    state["messages"].append({"role": "assistant", "content": answer.content})
    return state

# Build stateful graph with persistence
graph = StateGraph(ConversationState)
graph.add_node("contextualize", contextualize_question)
graph.add_node("retrieve", retrieve)
graph.add_node("generate", generate)
graph.set_entry_point("contextualize")
graph.add_edge("contextualize", "retrieve")
graph.add_edge("retrieve", "generate")
graph.add_edge("generate", END)

# Persist state across requests (SQLite for dev, PostgreSQL for production)
checkpointer = SqliteSaver.from_conn_string("conversations.db")
app = graph.compile(checkpointer=checkpointer)

# Each session has persistent state
config = {"configurable": {"thread_id": "user-session-123"}}

# Turn 1
result = app.invoke(
    {"messages": [{"role": "user", "content": "What is Spark AQE?"}]},
    config=config,
)

# Turn 2 (references Turn 1)
result = app.invoke(
    {"messages": result["messages"] + [{"role": "user", "content": "How does it handle skew?"}]},
    config=config,
)
# "it" → "Spark AQE" (resolved via contextualization)
```

---

## Pattern 4: Scaling to 1000+ Concurrent Users

```python
import asyncio
from functools import lru_cache

class ScalableRAGService:
    """Production RAG service handling 1000+ concurrent users."""
    
    def __init__(self):
        # Connection pooling
        self.vector_pool = AsyncConnectionPool(max_size=50)
        self.redis = aioredis.from_url("redis://cache-cluster:6379", max_connections=100)
        
        # Rate limiting per user
        self.rate_limiter = SlidingWindowRateLimiter(max_requests=30, window_seconds=60)
        
        # Semaphore for LLM calls (respect API rate limits)
        self.llm_semaphore = asyncio.Semaphore(50)
    
    async def answer(self, question: str, user_id: str) -> dict:
        # Rate limit check
        if not await self.rate_limiter.allow(user_id):
            raise HTTPException(429, "Rate limit exceeded. Try again in 60 seconds.")
        
        # Cache check
        cached = await self.redis.get(f"answer:{hash(question)}")
        if cached:
            return json.loads(cached)
        
        # Embed (local model — no API bottleneck)
        query_vec = self.local_embedder.encode(question)
        
        # Retrieve (pooled connection)
        async with self.vector_pool.acquire() as conn:
            results = await conn.search(query_vec, top_k=5)
        
        # Generate (semaphore-controlled)
        async with self.llm_semaphore:
            answer = await self.llm.chat.completions.create(
                model="gpt-4o-mini",
                messages=[...],
                stream=False,
            )
        
        result = {"answer": answer.choices[0].message.content, "sources": [...]}
        
        # Cache (1 hour TTL)
        await self.redis.setex(f"answer:{hash(question)}", 3600, json.dumps(result))
        
        return result

# Deployment: 4 API servers behind ALB
# Each server: 50 concurrent LLM calls × 4 servers = 200 concurrent LLM calls
# With 30% cache hit rate: effective capacity = 200 / 0.7 = ~285 concurrent generating
# At avg 1.5s per generation: 285 / 1.5 = 190 QPS sustained
# With streaming (first token 200ms): perceived latency much lower
```

---

## Pattern 5: Monitoring with LangSmith

```python
import os

# Enable LangSmith tracing (automatic for all LangChain components)
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "ls__..."
os.environ["LANGCHAIN_PROJECT"] = "rag-production"

# Custom metadata for each trace
from langsmith import traceable

@traceable(
    name="production_rag_query",
    metadata={"version": "v2.3", "environment": "production"}
)
async def handle_query(question: str, user_id: str) -> dict:
    """Every call is traced with full input/output/latency in LangSmith."""
    result = await rag_service.answer(question, user_id)
    return result

# LangSmith provides:
# - Full trace of every chain execution (inputs, outputs, latency per step)
# - Token usage tracking (cost per query)
# - Error debugging (see exact input that caused failure)
# - Feedback integration (link user thumbs-up to specific traces)
# - Evaluation datasets (sample traces → create test sets)
# - Regression detection (compare trace patterns over time)

# Monthly cost: ~$39/mo for 50K traces (LangSmith Plus)
# ROI: saves hours of debugging, catches issues proactively
```

---

## Interview Tips

> **Tip 1:** "How do you deploy a RAG system to production?" — FastAPI service with streaming endpoint, Redis for caching, connection pooling for vector DB, semaphore for LLM rate limits, health check endpoint, Prometheus metrics. Deploy behind a load balancer with auto-scaling based on CPU/memory. Use Docker + Kubernetes for orchestration.

> **Tip 2:** "How do you handle multi-turn conversations?" — Persist chat history (database or LangGraph checkpointer). Before each retrieval, rewrite the latest question using history to resolve pronouns ("it", "that", "the same thing"). Retrieve with the standalone question, generate with the original phrasing. Limit history to last 5-10 turns to control context size.

> **Tip 3:** "How do you scale RAG to handle high traffic?" — Local embedding model (removes API bottleneck), connection pooling (vector DB), response caching (30-50% hit rate), streaming (perceived latency), async processing (maximize concurrency), and horizontal scaling (multiple API servers). Target: 200+ QPS on 4 servers for ~$3K/month infra.
