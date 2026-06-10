---
title: "Orchestration Frameworks - Senior Deep Dive"
topic: rag-llm
subtopic: orchestration-frameworks
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [rag, llm, langgraph, multi-agent, production, state-machines, observability]
---

# Orchestration Frameworks — Senior-Level Deep Dive

## LangGraph for Stateful Workflows

LangGraph extends LangChain with graph-based state machines — essential for complex multi-step RAG:

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated
import operator

# Define state
class RAGState(TypedDict):
    question: str
    context: list[str]
    answer: str
    quality_score: float
    attempts: int
    route: str

# Define nodes (functions that transform state)
def classify_query(state: RAGState) -> RAGState:
    """Route query to appropriate handling path."""
    question = state["question"]
    # Simple heuristic or LLM classification
    if any(w in question.lower() for w in ["how many", "count", "total"]):
        state["route"] = "sql"
    elif any(w in question.lower() for w in ["compare", "vs", "difference"]):
        state["route"] = "multi_source"
    else:
        state["route"] = "standard"
    return state

def retrieve(state: RAGState) -> RAGState:
    """Standard vector retrieval."""
    results = vector_db.search(embed(state["question"]), top_k=5)
    state["context"] = [r.text for r in results]
    return state

def generate(state: RAGState) -> RAGState:
    """Generate answer from context."""
    state["answer"] = llm_generate(state["question"], state["context"])
    return state

def check_quality(state: RAGState) -> RAGState:
    """Self-check: is the answer grounded?"""
    score = evaluate_faithfulness(state["answer"], state["context"])
    state["quality_score"] = score
    state["attempts"] = state.get("attempts", 0) + 1
    return state

def should_retry(state: RAGState) -> str:
    """Conditional edge: retry if quality is low and attempts < 3."""
    if state["quality_score"] >= 0.8 or state["attempts"] >= 3:
        return "done"
    return "retry"

# Build graph
graph = StateGraph(RAGState)

# Add nodes
graph.add_node("classify", classify_query)
graph.add_node("retrieve", retrieve)
graph.add_node("generate", generate)
graph.add_node("check_quality", check_quality)

# Add edges
graph.set_entry_point("classify")
graph.add_edge("classify", "retrieve")
graph.add_edge("retrieve", "generate")
graph.add_edge("generate", "check_quality")

# Conditional edge: retry or finish
graph.add_conditional_edges("check_quality", should_retry, {"retry": "retrieve", "done": END})

# Compile and run
app = graph.compile()
result = app.invoke({"question": "How does Spark AQE handle skew?", "attempts": 0})
```

---

## Multi-Agent Systems

Multiple specialized agents collaborating on complex tasks:

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict

class ResearchState(TypedDict):
    question: str
    research_notes: list[str]
    sql_results: list[str]
    final_answer: str
    current_agent: str

def researcher_agent(state: ResearchState) -> ResearchState:
    """Agent specialized in searching documentation."""
    notes = search_docs(state["question"])
    state["research_notes"].extend(notes)
    return state

def analyst_agent(state: ResearchState) -> ResearchState:
    """Agent specialized in data analysis (SQL queries)."""
    sql = generate_sql(state["question"])
    results = execute_sql(sql)
    state["sql_results"].append(f"Query: {sql}\nResult: {results}")
    return state

def synthesizer_agent(state: ResearchState) -> ResearchState:
    """Agent that combines all findings into a final answer."""
    all_context = state["research_notes"] + state["sql_results"]
    state["final_answer"] = synthesize(state["question"], all_context)
    return state

def router(state: ResearchState) -> str:
    """Decide which agent to invoke next."""
    if not state["research_notes"]:
        return "researcher"
    elif not state["sql_results"] and needs_data(state["question"]):
        return "analyst"
    else:
        return "synthesizer"

# Build multi-agent graph
graph = StateGraph(ResearchState)
graph.add_node("researcher", researcher_agent)
graph.add_node("analyst", analyst_agent)
graph.add_node("synthesizer", synthesizer_agent)

graph.set_entry_point("researcher")
graph.add_conditional_edges("researcher", router, {
    "analyst": "analyst",
    "synthesizer": "synthesizer",
})
graph.add_edge("analyst", "synthesizer")
graph.add_edge("synthesizer", END)

multi_agent = graph.compile()
```

---

## Production Patterns

### Circuit Breaker

```python
import time
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"        # Normal operation
    OPEN = "open"            # Failing, skip calls
    HALF_OPEN = "half_open"  # Testing if recovered

class CircuitBreaker:
    """Prevent cascading failures when LLM/vector DB is down."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.last_failure_time = 0
    
    def call(self, func, *args, **kwargs):
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception("Circuit breaker OPEN — service unavailable")
        
        try:
            result = func(*args, **kwargs)
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
            raise

# Usage
llm_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
vector_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

def safe_rag(question: str) -> str:
    try:
        context = vector_breaker.call(retrieve, question)
        answer = llm_breaker.call(generate, question, context)
        return answer
    except Exception:
        return "Service temporarily degraded. Please try again in a moment."
```

### Response Caching

```python
import hashlib
import redis

class SemanticCache:
    """Cache RAG responses for similar (not just identical) queries."""
    
    def __init__(self, redis_client, similarity_threshold: float = 0.95):
        self.redis = redis_client
        self.threshold = similarity_threshold
    
    def get(self, question: str) -> str | None:
        # Exact match cache
        key = hashlib.md5(question.lower().strip().encode()).hexdigest()
        cached = self.redis.get(f"rag_cache:{key}")
        if cached:
            return cached.decode()
        return None
    
    def set(self, question: str, answer: str, ttl: int = 3600):
        key = hashlib.md5(question.lower().strip().encode()).hexdigest()
        self.redis.setex(f"rag_cache:{key}", ttl, answer)
```

---

## Observability (Distributed Tracing)

```python
# LangSmith integration for full observability
import os
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "your-key"
os.environ["LANGCHAIN_PROJECT"] = "production-rag"

# Every chain invocation is automatically traced:
# - Input/output at each step
# - Latency per component
# - Token usage
# - Error traces
# - Feedback scores (link user thumbs up/down to specific traces)

# Custom spans for non-LangChain components:
from langsmith import traceable

@traceable(name="vector_search")
def search_vectors(query: str, top_k: int = 5):
    """This function call is automatically traced in LangSmith."""
    return vector_db.search(embed(query), top_k=top_k)

@traceable(name="rerank")
def rerank_results(query: str, candidates: list):
    return cross_encoder.predict([(query, c.text) for c in candidates])
```

---

## When to Drop the Framework

```python
# Framework overhead: 5-15% added latency from abstraction layers
# For high-performance production systems, consider dropping the framework:

# Signs you should go custom:
# 1. You're fighting the framework more than using it
# 2. Debugging requires understanding framework internals
# 3. Performance requirements exceed what abstractions allow
# 4. Your use case doesn't match the framework's assumptions

# Lean production RAG (no framework, ~100 lines):
class ProductionRAG:
    """Framework-free RAG for maximum performance and debuggability."""
    
    def __init__(self, config):
        self.embedder = LocalEmbedder(config.model)
        self.vector_db = QdrantClient(config.qdrant_url)
        self.llm = AsyncOpenAI()
        self.cache = RedisCache(config.redis_url)
        self.reranker = CrossEncoder(config.reranker_model)
    
    async def answer(self, question: str) -> dict:
        # Check cache
        cached = await self.cache.get(question)
        if cached:
            return cached
        
        # Embed (local, 10ms)
        query_vec = self.embedder.encode(question)
        
        # Retrieve (5ms)
        candidates = self.vector_db.search("docs", query_vec, limit=20)
        
        # Rerank (50ms)
        pairs = [(question, c.payload["text"]) for c in candidates]
        scores = self.reranker.predict(pairs)
        top_5 = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)[:5]
        
        # Generate (streaming, 200ms to first token)
        context = "\n".join([c.payload["text"] for c, _ in top_5])
        answer = await self.llm.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Answer based on context. Cite sources."},
                {"role": "user", "content": f"Context:\n{context}\n\nQ: {question}"}
            ],
            temperature=0,
        )
        
        result = {"answer": answer.choices[0].message.content, "sources": [c.id for c, _ in top_5]}
        await self.cache.set(question, result, ttl=3600)
        return result
```

---

## Interview Tips

> **Tip 1:** "LangGraph vs plain LangChain?" — LangChain chains are linear (A→B→C). LangGraph adds branching, loops, and conditional edges — essential for self-correcting RAG (retry if quality is low), agentic workflows (decide next action based on current state), and multi-agent collaboration. Use LangGraph when your workflow has conditional logic or cycles.

> **Tip 2:** "How do you handle failures in production LLM apps?" — Circuit breaker (stop calling failing service), retry with backoff (transient errors), fallback models (primary fails → use cheaper model), graceful degradation (vector DB down → answer from LLM knowledge), and response caching (serve cached answer if everything is down).

> **Tip 3:** "When do you drop the framework?" — When the framework adds more complexity than it removes. Signs: you're patching around framework limitations, debugging requires reading framework source code, or latency overhead is unacceptable. For simple RAG in production, 100 lines of custom code may be cleaner and faster than a framework.

## ⚡ Cheat Sheet

**RAG pipeline architecture**
```
Document → Chunk → Embed → Store in Vector DB
Query → Embed query → ANN search → Retrieve top-k chunks → Augment prompt → LLM → Answer
```

**Chunking strategies**
```python
# Fixed-size with overlap
text_splitter = RecursiveCharacterTextSplitter(chunk_size=512, chunk_overlap=50)
chunks = text_splitter.split_text(document)

# Semantic chunking (split on topic boundaries)
from langchain.text_splitter import SemanticChunker
chunker = SemanticChunker(embedding_model)

# Hierarchical: large chunks for context, small for retrieval
# Parent-child: store parent chunk, retrieve child, return parent to LLM
```

**Embedding models**
| Model | Dims | Use case |
|---|---|---|
| text-embedding-3-small | 1536 | General purpose, OpenAI |
| text-embedding-3-large | 3072 | Higher accuracy, OpenAI |
| all-MiniLM-L6-v2 | 384 | Fast, local, free |
| BAAI/bge-large-en | 1024 | Strong retrieval, local |
| Cohere embed-v3 | 1024 | Multilingual |

**Vector databases**
| DB | Type | Strengths |
|---|---|---|
| Pinecone | Managed | Easy ops, fully managed |
| Weaviate | OSS/managed | Hybrid search (vector + BM25) |
| Qdrant | OSS/managed | Fast, Rust-based, payload filtering |
| pgvector | PostgreSQL extension | Existing Postgres infrastructure |
| Chroma | OSS | Local dev, lightweight |
| FAISS | Library | Fastest local, no persistence |

**Retrieval optimization**
```python
# Hybrid search (vector + keyword)
results = vector_db.hybrid_search(
    query=query, vector=embed(query), alpha=0.7  # 0=pure BM25, 1=pure vector
)
# Re-ranking with cross-encoder
from sentence_transformers import CrossEncoder
ranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
scores = ranker.predict([(query, doc.text) for doc in results])
reranked = sorted(zip(results, scores), key=lambda x: x[1], reverse=True)
```

**Evaluation metrics**
```
Faithfulness:    LLM answer only uses facts from retrieved context (anti-hallucination)
Answer Relevance: answer addresses the question
Context Precision: retrieved chunks actually contain the answer
Context Recall:   all relevant chunks were retrieved
RAGAS framework: automated evaluation of all four metrics
```

**Prompt engineering patterns**
```python
# System prompt with RAG context
system_prompt = """You are a data engineering assistant.
Answer only based on the provided context. If the answer is not in the context, say 'I don't know.'
Context:
{context}"""

# Few-shot prompting
few_shot_examples = [
    {"question": "What is Delta Lake?", "answer": "Delta Lake is an open-source..."},
]

# Chain-of-thought (CoT): "Let's think step by step"
# React pattern: Reason + Act (tool use) + Observe → loop until answer
```

**Fine-tuning vs RAG**
```
RAG:         best for dynamic/proprietary knowledge; no training needed; updatable
Fine-tuning: best for domain tone/style; specialized tasks; fixed knowledge cutoff
Combine:     fine-tune for domain adaptation + RAG for factual grounding
```

**Key interview points**
- Chunk size tradeoff: small chunks = precise retrieval; large chunks = more context
- Cosine similarity vs dot product: cosine for variable-length texts; dot for normalized
- Metadata filtering: filter by document_type, date, or source before ANN search
- Guardrails: LLM output validation (Guardrails AI, Nemo Guardrails, Instructor)
