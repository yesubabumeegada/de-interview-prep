---
title: "Retrieval Pipelines - Scenario Questions"
topic: rag-llm
subtopic: retrieval-pipelines
content_type: scenario_question
tags: [rag, llm, retrieval, interview, scenarios]
---

# Scenario Questions — Retrieval Pipelines

<article data-difficulty="junior">

## 🟢 Junior: Basic RAG Setup

**Scenario:** You need to build a simple Q&A bot over 1,000 internal wiki pages. Design the minimal RAG pipeline: what components do you need, and write the code for the core query flow.

<details>
<summary>💡 Hint</summary>
Minimal RAG: embed docs → store in vector DB → on query: embed question → search → stuff context into prompt → generate answer.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
from pinecone import Pinecone

client = OpenAI()
pc = Pinecone(api_key="your-key")
index = pc.Index("wiki")

def answer_question(question: str) -> str:
    """Minimal RAG: embed → search → generate."""
    
    # Step 1: Embed the question
    emb = client.embeddings.create(model="text-embedding-3-small", input=[question])
    query_vec = emb.data[0].embedding
    
    # Step 2: Search for relevant wiki pages
    results = index.query(vector=query_vec, top_k=3, include_metadata=True)
    context = "\n\n".join([m.metadata["text"] for m in results.matches])
    
    # Step 3: Generate answer grounded in context
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Answer based only on the provided context. If unsure, say so."},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
        ],
        temperature=0,
    )
    return response.choices[0].message.content
```

**Key Points:**
- Components needed: embedding model, vector database, LLM, prompt template
- For 1,000 pages: any vector DB works (pgvector, Pinecone, ChromaDB)
- Chunk each wiki page into ~500 char pieces before embedding
- top_k=3 is a reasonable starting point for focused Q&A
- Temperature=0 for factual answers reduces hallucination

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Top-K Selection

**Scenario:** Your RAG bot returns irrelevant information mixed with relevant content. Users say "the answer includes stuff about Kafka when I asked about Spark." You're using top_k=10. What's wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>
top_k=10 retrieves 10 chunks — some may be off-topic. Fewer, more relevant chunks give better answers. Also consider adding a similarity threshold.
</details>

<details>
<summary>✅ Solution</summary>

```python
def improved_retrieval(question: str, top_k: int = 5, min_score: float = 0.5):
    """Retrieve with reduced top_k and minimum score threshold."""
    query_vec = embed(question)
    results = index.query(vector=query_vec, top_k=top_k, include_metadata=True)
    
    # Filter out low-relevance results
    relevant = [m for m in results.matches if m.score >= min_score]
    
    if not relevant:
        return "I couldn't find relevant information to answer this question."
    
    context = "\n\n".join([m.metadata["text"] for m in relevant])
    return generate_answer(question, context)

# Before: top_k=10, no threshold → 3 relevant + 7 irrelevant chunks → noisy context
# After: top_k=5, min_score=0.5 → 3-4 relevant chunks only → focused answer
```

**Key Points:**
- Reduce top_k from 10 to 3-5 for focused Q&A (less noise)
- Add minimum similarity threshold (0.4-0.6 depending on model)
- More context is NOT always better — irrelevant context confuses the LLM
- If answers are incomplete with top_k=3, increase gradually while monitoring quality
- Alternative: retrieve top_k=10 but re-rank to top-3 before passing to LLM

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Prompt Template Design

**Scenario:** Your RAG bot sometimes ignores the retrieved context and answers from its own knowledge, producing incorrect answers. How do you design a prompt that forces it to only use the provided context?

<details>
<summary>💡 Hint</summary>
Use strong system instructions that explicitly tell the LLM to answer ONLY from context, cite sources, and say "I don't know" when the context doesn't cover the question.
</details>

<details>
<summary>✅ Solution</summary>

```python
SYSTEM_PROMPT = """You are a technical assistant. You MUST follow these rules:
1. Answer ONLY based on the provided context documents
2. If the context does not contain the answer, respond: "I don't have information about this in our documentation."
3. NEVER make up information or use knowledge not in the context
4. Cite which context section you're referencing using [Source N]
5. If the question is ambiguous, ask for clarification"""

def generate_grounded_answer(question: str, context_chunks: list[dict]) -> str:
    # Number the sources for citation
    numbered_context = ""
    for i, chunk in enumerate(context_chunks, 1):
        numbered_context += f"[Source {i}]: {chunk['text']}\n\n"
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Context:\n{numbered_context}\nQuestion: {question}"}
        ],
        temperature=0,  # Deterministic = less creative = less hallucination
    )
    return response.choices[0].message.content
```

**Key Points:**
- Explicit "ONLY from context" instruction reduces hallucination significantly
- temperature=0 makes outputs more deterministic and factual
- Numbered sources enable citation tracking (users can verify)
- "Say I don't know" is critical — prevents making up answers
- Test edge cases: questions the context doesn't cover should trigger the refusal
- Alternative: use structured output (JSON) to separate answer from confidence score

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Handling No Results

**Scenario:** A user asks "What's our policy on remote work in Japan?" but your knowledge base only covers US policies. The vector search returns results about US remote work (low similarity scores of 0.3-0.4). The bot incorrectly answers with US policy as if it applies to Japan. How do you handle this?

<details>
<summary>💡 Hint</summary>
Set a minimum relevance threshold. If all results score below it, return a "no information available" response instead of hallucinating with loosely related content.
</details>

<details>
<summary>✅ Solution</summary>

```python
def safe_rag_answer(question: str, min_relevance: float = 0.5) -> dict:
    """RAG with graceful handling of low-relevance results."""
    query_vec = embed(question)
    results = index.query(vector=query_vec, top_k=5, include_metadata=True)
    
    # Check if any results are sufficiently relevant
    relevant_results = [r for r in results.matches if r.score >= min_relevance]
    
    if not relevant_results:
        # No relevant documents found
        best_score = results.matches[0].score if results.matches else 0
        return {
            "answer": "I don't have information about this topic in our knowledge base. "
                     "This question may need to be directed to the appropriate team.",
            "confidence": "low",
            "best_match_score": best_score,
            "suggestion": "Try asking about US remote work policies, or contact HR for Japan-specific policies."
        }
    
    # Proceed with normal RAG
    context = "\n".join([r.metadata["text"] for r in relevant_results])
    answer = generate_answer(question, context)
    
    return {
        "answer": answer,
        "confidence": "high" if relevant_results[0].score > 0.7 else "medium",
        "sources": [r.metadata.get("source", "") for r in relevant_results],
    }
```

**Key Points:**
- Never pass low-relevance results to the LLM as if they're answers
- Threshold of 0.5 is a reasonable starting point (calibrate on your data)
- Provide helpful alternatives: what IS available, who to ask instead
- Log "no results" queries — they indicate knowledge gaps to fill
- Consider a fallback: "I found information about US policies. Would that be helpful?"

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Source Attribution

**Scenario:** Your legal team requires that every answer includes a reference to the source document and page number. Users need to verify AI-generated answers against original documents. How do you implement source attribution?

<details>
<summary>💡 Hint</summary>
Store source metadata (document name, page, section) with each chunk in the vector DB. Include source references in the prompt and instruct the LLM to cite them.
</details>

<details>
<summary>✅ Solution</summary>

```python
def rag_with_sources(question: str) -> dict:
    """RAG with mandatory source citations."""
    query_vec = embed(question)
    results = index.query(vector=query_vec, top_k=5, include_metadata=True)
    
    # Format context with explicit source labels
    sources = []
    context_parts = []
    for i, r in enumerate(results.matches, 1):
        source_info = {
            "id": i,
            "document": r.metadata.get("source_file", "Unknown"),
            "page": r.metadata.get("page_number", "N/A"),
            "section": r.metadata.get("section", ""),
        }
        sources.append(source_info)
        context_parts.append(f"[Source {i} - {source_info['document']}, p.{source_info['page']}]\n{r.metadata['text']}")
    
    context = "\n\n".join(context_parts)
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Answer the question using ONLY the provided sources. "
             "ALWAYS cite sources using [Source N] notation after each claim."},
            {"role": "user", "content": f"Sources:\n{context}\n\nQuestion: {question}"}
        ],
        temperature=0,
    )
    
    return {
        "answer": response.choices[0].message.content,
        "sources": sources,
        # Example output: "According to [Source 1], the PTO policy allows..."
    }
```

**Key Points:**
- Store metadata during indexing: source file, page number, section header, timestamp
- Number sources in the prompt so the LLM can reference them by number
- Instruct the LLM to cite after every claim (not just at the end)
- Return source metadata separately so the UI can render clickable links
- For legal compliance: log which sources contributed to each answer (audit trail)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Hybrid Search Implementation

**Scenario:** Users searching for "spark.sql.shuffle.partitions = 200" get irrelevant results because the dense embedding doesn't understand config parameter names literally. How do you add keyword search (BM25) alongside semantic search?

<details>
<summary>💡 Hint</summary>
Implement hybrid search: run BM25 (exact keyword match) AND dense vector search in parallel, then merge results using Reciprocal Rank Fusion (RRF).
</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np
from rank_bm25 import BM25Okapi

class HybridRAGPipeline:
    def __init__(self, documents: list[dict], vector_db):
        self.vector_db = vector_db
        self.documents = documents
        
        # Build BM25 index
        tokenized = [doc["text"].lower().split() for doc in documents]
        self.bm25 = BM25Okapi(tokenized)
        self.doc_ids = [doc["id"] for doc in documents]
    
    def search(self, query: str, top_k: int = 5, alpha: float = 0.5) -> list[dict]:
        """alpha: weight for dense search (1-alpha for sparse)."""
        
        # Dense search
        query_vec = embed(query)
        dense_results = self.vector_db.search(query_vec, top_k=30)
        dense_ids = [r.id for r in dense_results]
        
        # Sparse search (BM25)
        query_tokens = query.lower().split()
        bm25_scores = self.bm25.get_scores(query_tokens)
        top_sparse = np.argsort(bm25_scores)[::-1][:30]
        sparse_ids = [self.doc_ids[i] for i in top_sparse if bm25_scores[i] > 0]
        
        # RRF fusion
        k = 60
        scores = {}
        for rank, doc_id in enumerate(dense_ids, 1):
            scores[doc_id] = scores.get(doc_id, 0) + alpha / (k + rank)
        for rank, doc_id in enumerate(sparse_ids, 1):
            scores[doc_id] = scores.get(doc_id, 0) + (1 - alpha) / (k + rank)
        
        # Sort by fused score
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [{"id": doc_id, "score": score} for doc_id, score in ranked[:top_k]]

# For config parameters like "spark.sql.shuffle.partitions":
# Dense search: might return general "Spark configuration" docs (semantic match)
# BM25 search: finds docs containing exact string "spark.sql.shuffle.partitions" (keyword match)
# Hybrid: combines both → best results
```

**Key Points:**
- alpha=0.5 gives equal weight; increase for conceptual queries, decrease for keyword-heavy queries
- BM25 catches exact matches that dense embeddings miss (config params, error codes, version numbers)
- RRF is parameter-free (just k=60 constant) and works well without tuning
- Docs appearing in both ranked lists get boosted (strongest signal)
- Production: use Elasticsearch for BM25 at scale (not in-memory BM25Okapi)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Re-Ranking Pipeline

**Scenario:** Your RAG returns 5 documents but the most relevant one is often at position 3-4 instead of position 1. The LLM focuses on the first documents, producing suboptimal answers. Implement cross-encoder re-ranking to put the best document first.

<details>
<summary>💡 Hint</summary>
Retrieve top-20 with bi-encoder (fast), then re-rank those 20 with a cross-encoder (precise), and pass only the top-5 reranked results to the LLM.
</details>

<details>
<summary>✅ Solution</summary>

```python
from sentence_transformers import CrossEncoder

class ReRankingRAG:
    def __init__(self):
        self.reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    
    def answer(self, question: str) -> str:
        # Step 1: Retrieve top-20 (fast bi-encoder search)
        query_vec = embed(question)
        candidates = vector_db.search(query_vec, top_k=20)
        
        # Step 2: Re-rank with cross-encoder (precise)
        pairs = [(question, c.metadata["text"]) for c in candidates]
        scores = self.reranker.predict(pairs)
        
        # Sort by cross-encoder score (higher = more relevant)
        reranked = sorted(
            zip(candidates, scores), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # Step 3: Pass only top-5 reranked results to LLM
        top_5 = [c.metadata["text"] for c, _ in reranked[:5]]
        context = "\n\n".join(top_5)
        
        return generate_answer(question, context)

# Before re-ranking:  Position of best doc: often 3-4 out of 5
# After re-ranking:   Position of best doc: consistently 1-2 out of 5
# Impact: LLM gets the best context first → better answers

# Latency cost: ~50-100ms for re-ranking 20 documents (acceptable)
# Quality gain: 10-20% improvement in answer relevance
```

**Key Points:**
- Over-fetch (20) from fast bi-encoder, then re-rank to precise top-5
- Cross-encoder sees query+document together → much more accurate relevance scoring
- The LLM pays more attention to early context → ordering matters
- Latency: ~80ms for 20 pairs with MiniLM cross-encoder (local, no API call)
- Alternative: Cohere Rerank API ($1/1000 queries) — no local model needed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Decomposition

**Scenario:** Users ask complex questions like "Compare the cost and performance of Kinesis vs MSK for a 1TB/day streaming workload" but your docs cover Kinesis and MSK separately. A single retrieval finds Kinesis OR MSK docs, rarely both. How do you retrieve comprehensive information?

<details>
<summary>💡 Hint</summary>
Decompose the complex question into sub-questions ("Kinesis cost for 1TB/day?", "MSK cost for 1TB/day?", "Kinesis vs MSK performance?"), retrieve for each, then merge all context.
</details>

<details>
<summary>✅ Solution</summary>

```python
async def decompose_and_answer(question: str) -> str:
    """Decompose complex question, retrieve for each part, synthesize answer."""
    
    # Step 1: Decompose into sub-questions
    decomp_response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{
            "role": "user",
            "content": f"""Break this question into 2-4 simpler sub-questions that together 
fully address the original. Return one per line.

Question: {question}"""
        }],
        temperature=0,
    )
    sub_questions = [q.strip().lstrip("0123456789.- ") 
                     for q in decomp_response.choices[0].message.content.split("\n") 
                     if q.strip()]
    
    # Step 2: Retrieve for each sub-question (parallel)
    all_context = []
    seen_ids = set()
    
    for sub_q in sub_questions:
        results = vector_db.search(embed(sub_q), top_k=3)
        for r in results:
            if r.id not in seen_ids:
                seen_ids.add(r.id)
                all_context.append({"sub_question": sub_q, "text": r.metadata["text"]})
    
    # Step 3: Generate comprehensive answer using all gathered context
    context_str = "\n\n".join([
        f"[For: {c['sub_question']}]\n{c['text']}" for c in all_context
    ])
    
    response = await client.chat.completions.create(
        model="gpt-4o",  # Use stronger model for synthesis
        messages=[
            {"role": "system", "content": "Synthesize a comprehensive answer using all provided context."},
            {"role": "user", "content": f"Context:\n{context_str}\n\nOriginal question: {question}"}
        ],
        temperature=0,
    )
    return response.choices[0].message.content

# Decomposition example:
# "Compare cost and performance of Kinesis vs MSK for 1TB/day" becomes:
# 1. "What is the cost of Kinesis Data Streams for 1TB/day throughput?"
# 2. "What is the cost of Amazon MSK for 1TB/day throughput?"
# 3. "How does Kinesis performance compare to MSK for streaming workloads?"
# Each sub-question retrieves relevant docs → comprehensive comparison
```

**Key Points:**
- Single query retrieves one topic; decomposition retrieves ALL relevant topics
- Deduplicate by ID across sub-questions (same doc may match multiple)
- Use stronger model (GPT-4o) for synthesis of multi-source context
- Adds ~500ms latency (LLM decomposition call) but dramatically improves completeness
- Works great for: comparisons, multi-step questions, "how does X affect Y and Z?"

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Conversation History Integration

**Scenario:** In a chat interface, the user asks "Tell me about Spark AQE" and then follows up with "How does it handle skew?" — but "it" refers to AQE. Your RAG system searches for "How does it handle skew?" and retrieves generic skew handling docs, not AQE-specific ones. Fix this.

<details>
<summary>💡 Hint</summary>
Before retrieval, rewrite the user's message to be self-contained by resolving pronouns and references using chat history. "How does it handle skew?" → "How does Spark AQE handle data skew?"
</details>

<details>
<summary>✅ Solution</summary>

```python
class ConversationalRAG:
    def __init__(self):
        self.sessions = {}  # session_id → message history
    
    async def answer(self, question: str, session_id: str) -> str:
        history = self.sessions.get(session_id, [])
        
        # Step 1: Contextualize the question (resolve references)
        if history:
            standalone = await self.make_standalone(question, history)
        else:
            standalone = question
        
        # Step 2: Normal RAG with the standalone question
        results = vector_db.search(embed(standalone), top_k=5)
        context = "\n".join([r.metadata["text"] for r in results])
        answer = await self.generate(standalone, context)
        
        # Step 3: Update history
        history.append({"role": "user", "content": question})
        history.append({"role": "assistant", "content": answer})
        self.sessions[session_id] = history[-10:]  # Keep last 10 messages
        
        return answer
    
    async def make_standalone(self, question: str, history: list[dict]) -> str:
        """Rewrite question to be self-contained."""
        hist_text = "\n".join([f"{m['role']}: {m['content']}" for m in history[-4:]])
        
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": f"""Rewrite this follow-up question to be self-contained.
Replace pronouns/references with their actual subjects from the conversation.

Conversation:
{hist_text}

Follow-up: {question}

Self-contained question:"""
            }],
            temperature=0,
        )
        return response.choices[0].message.content.strip()

# Before contextualization:
# Query: "How does it handle skew?" → retrieves generic skew docs
# After contextualization:
# Query: "How does Spark AQE handle data skew?" → retrieves AQE skew handling docs
```

**Key Points:**
- "Make standalone" resolves pronouns: "it" → "Spark AQE", "that" → "the previous config"
- Only last 4-6 messages needed for context (keep session memory bounded)
- Adds ~200ms for the contextualization LLM call (acceptable for chat UX)
- Without this: every follow-up question retrieves wrong documents
- The retrieval uses the standalone question; the final answer uses the original phrasing

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Agentic RAG Design

**Scenario:** Build a research assistant that can answer complex questions requiring multiple retrieval steps, tool usage (SQL queries, API calls), and iterative reasoning. Example: "What was our average order value last quarter, and how does it compare to what our documentation recommends?"

<details>
<summary>💡 Hint</summary>
Use a ReAct-style agent with tools: vector_search (docs), sql_query (database), and a reasoning loop that decides what to retrieve next based on what it's learned so far.
</details>

<details>
<summary>✅ Solution</summary>

```python
import json

class ResearchAgent:
    """Agentic RAG: iteratively retrieves from multiple sources to answer complex questions."""
    
    def __init__(self):
        self.tools = [
            {"type": "function", "function": {
                "name": "search_docs",
                "description": "Search documentation and knowledge base",
                "parameters": {"type": "object", "properties": {
                    "query": {"type": "string"}
                }, "required": ["query"]}
            }},
            {"type": "function", "function": {
                "name": "query_database",
                "description": "Run a SQL query against the analytics database",
                "parameters": {"type": "object", "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"}
                }, "required": ["sql"]}
            }},
            {"type": "function", "function": {
                "name": "calculate",
                "description": "Perform a calculation",
                "parameters": {"type": "object", "properties": {
                    "expression": {"type": "string"}
                }, "required": ["expression"]}
            }},
        ]
    
    async def research(self, question: str, max_steps: int = 8) -> dict:
        messages = [
            {"role": "system", "content": """You are a research assistant with access to tools.
Think step by step:
1. Determine what information you need
2. Use tools to gather that information
3. Continue until you have enough to answer comprehensively
4. Provide a final answer with sources and data"""},
            {"role": "user", "content": question}
        ]
        
        steps = []
        
        for step in range(max_steps):
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=self.tools,
                tool_choice="auto",
            )
            
            msg = response.choices[0].message
            
            if not msg.tool_calls:
                # Agent is done — return final answer
                return {"answer": msg.content, "steps": steps}
            
            # Execute tool calls
            messages.append(msg)
            for call in msg.tool_calls:
                result = await self.execute_tool(call.function.name, json.loads(call.function.arguments))
                steps.append({"tool": call.function.name, "args": call.function.arguments, "result": str(result)[:500]})
                messages.append({"role": "tool", "tool_call_id": call.id, "content": str(result)})
        
        return {"answer": "Could not complete research within step limit.", "steps": steps}
    
    async def execute_tool(self, name: str, args: dict):
        if name == "search_docs":
            results = vector_db.search(embed(args["query"]), top_k=3)
            return [r.metadata["text"][:300] for r in results]
        elif name == "query_database":
            return execute_safe_sql(args["sql"])  # With query validation
        elif name == "calculate":
            return eval(args["expression"])  # Sandboxed

# Example execution for "What was our AOV last quarter vs documentation recommendation?":
# Step 1: query_database("SELECT AVG(order_total) FROM orders WHERE order_date >= '2024-01-01'")
#   → $87.50
# Step 2: search_docs("recommended average order value benchmark")
#   → "Our documentation recommends targeting $75-95 AOV for this market segment"
# Step 3: calculate("87.50 / 85 * 100 - 100")
#   → 2.94% above midpoint
# Final: "Your AOV last quarter was $87.50, which is within the recommended $75-95 range..."
```

**Key Points:**
- Agent decides WHAT to retrieve and WHEN — not a fixed pipeline
- Multiple data sources (docs + SQL + APIs) in one coherent answer
- max_steps prevents infinite loops
- GPT-4o for complex reasoning; GPT-4o-mini for simpler tool calls
- Log all steps for debugging and auditability
- Production safety: SQL query validation, tool timeouts, cost guards

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Self-Correcting Retrieval

**Scenario:** Your RAG system sometimes retrieves plausible-looking but incorrect documents (e.g., docs about Spark 2.x when the question is about Spark 3.5 features). The LLM doesn't know the context is wrong and generates confidently incorrect answers. Design a self-correcting system.

<details>
<summary>💡 Hint</summary>
Add a verification step: after initial retrieval, have an LLM judge evaluate whether each retrieved document actually answers the question. If not, rewrite the query and try again.
</details>

<details>
<summary>✅ Solution</summary>

```python
class SelfCorrectingRAG:
    """Verify retrieval quality and retry with different strategy if insufficient."""
    
    async def answer(self, question: str) -> dict:
        # Attempt 1: Standard retrieval
        results = await self.retrieve(question, top_k=5)
        
        # Verify: are these results actually relevant?
        verification = await self.verify_relevance(question, results)
        
        if verification["sufficient"]:
            answer = await self.generate(question, [r["text"] for r in verification["relevant_docs"]])
            return {"answer": answer, "confidence": "high", "attempts": 1}
        
        # Attempt 2: Query transformation + expanded search
        transformed = await self.transform_query(question, verification["feedback"])
        results_2 = await self.retrieve(transformed, top_k=10)
        
        verification_2 = await self.verify_relevance(question, results_2)
        
        if verification_2["sufficient"]:
            answer = await self.generate(question, [r["text"] for r in verification_2["relevant_docs"]])
            return {"answer": answer, "confidence": "medium", "attempts": 2}
        
        # Attempt 3: Fallback — acknowledge limitations
        return {
            "answer": "I found some related information but cannot confidently answer this question. "
                     "The available documentation may not cover this specific topic.",
            "confidence": "low",
            "attempts": 3,
            "partial_context": [r["text"][:100] for r in results_2[:3]],
        }
    
    async def verify_relevance(self, question: str, results: list[dict]) -> dict:
        """LLM judges whether retrieved docs answer the question."""
        docs_text = "\n---\n".join([f"Doc {i+1}: {r['text'][:300]}" for i, r in enumerate(results)])
        
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": f"""Evaluate if these documents can answer the question.

Question: {question}

Documents:
{docs_text}

For each document, state if it's RELEVANT or IRRELEVANT to the question.
Then state if there's SUFFICIENT information to fully answer the question.
If insufficient, explain what's MISSING.

Respond in JSON: {{
  "doc_relevance": [true/false for each doc],
  "sufficient": true/false,
  "feedback": "what's missing or wrong"
}}"""
            }],
            response_format={"type": "json_object"},
            temperature=0,
        )
        
        eval_result = json.loads(response.choices[0].message.content)
        relevant_docs = [r for r, is_rel in zip(results, eval_result["doc_relevance"]) if is_rel]
        
        return {
            "sufficient": eval_result["sufficient"],
            "relevant_docs": relevant_docs,
            "feedback": eval_result.get("feedback", ""),
        }
    
    async def transform_query(self, original: str, feedback: str) -> str:
        """Rewrite query based on verification feedback."""
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": f"""The search for "{original}" didn't find good results.
Feedback: {feedback}

Rewrite the query to better find the needed information. Be more specific."""
            }],
            temperature=0.3,
        )
        return response.choices[0].message.content
```

**Key Points:**
- Verification catches cases where embeddings are misleadingly similar (Spark 2.x vs 3.5)
- Query transformation uses the feedback ("needs Spark 3.5 specific info") to improve retry
- Maximum 3 attempts balances quality vs latency
- Confidence levels help the UI decide how to present the answer
- Cost: 2-3 additional LLM calls per query (but only when initial retrieval is poor)
- Production: track % of queries needing retries — high rate means chunking/embedding issues

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Scaling to 1000+ QPS

**Scenario:** Your RAG service currently handles 50 queries/second. The product is launching to 100K users and needs to support 1000+ QPS with p99 < 2 seconds. Design the scaling architecture.

<details>
<summary>💡 Hint</summary>
Key bottlenecks: LLM generation (slow), embedding API (network), vector search (fast). Use: caching layers, local embedding model, async streaming, connection pooling, and horizontal scaling of API servers.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE for 1000 QPS, p99 < 2s:

# Bottleneck analysis:
# - Query embedding: 50ms (local GPU) or 200ms (API)     → USE LOCAL
# - Vector search: 5-10ms                                  → Already fast
# - Re-ranking: 50ms (local cross-encoder)                → Acceptable
# - LLM generation: 500-2000ms                            → MAIN BOTTLENECK

SCALING_ARCHITECTURE = {
    "api_servers": {
        "count": 8,
        "instance": "c6g.2xlarge",  # 8 vCPU, 16 GB
        "framework": "FastAPI with uvicorn (async)",
        "purpose": "Handle concurrent requests, orchestrate pipeline",
    },
    "embedding_gpus": {
        "count": 2,
        "instance": "g5.xlarge (A10G)",
        "model": "all-MiniLM-L6-v2 (local, 10ms/query)",
        "purpose": "Query embedding without API dependency",
    },
    "vector_db": {
        "type": "Qdrant cluster (3 nodes)",
        "capacity": "Handles 10K QPS easily",
    },
    "llm_pool": {
        "provider": "OpenAI GPT-4o-mini",
        "concurrent_limit": 200,  # Rate limit
        "strategy": "Streaming responses + response caching",
    },
    "cache_layers": {
        "l1_response": "Redis: full answer cache (30% hit rate at 1hr TTL)",
        "l2_embedding": "Redis: query embedding cache (60% hit rate)",
        "l3_context": "Redis: retrieval results cache (50% hit rate)",
    },
}

# With 30% response cache hit rate:
# 1000 QPS total → 700 QPS need full pipeline
# 700 QPS × 200 concurrent LLM slots = ~3.5 QPS per slot
# Each LLM call takes ~800ms → each slot handles ~1.25 QPS
# 200 slots × 1.25 = 250 QPS sustained... NOT ENOUGH!

# FIX: Semantic cache (similar queries return cached answers)
# + Reduce LLM calls by grouping similar concurrent queries
# + Use smaller model (gpt-4o-mini) for simple queries (faster)
# + Pre-generate answers for common questions

# REALISTIC THROUGHPUT:
# 1000 QPS total
# - 300 cache hits (30%) → instant response
# - 200 semantic cache hits (20%) → near-instant
# - 500 need full pipeline → 200 LLM concurrent slots handles this
# Each slot: 800ms per request → 200/0.8 = 250 QPS capacity ✓

# Monthly cost: ~$15K (API servers + GPUs + LLM usage + vector DB + cache)
```

**Key Points:**
- LLM is the bottleneck — cache aggressively to reduce LLM calls
- Local embedding model eliminates API round-trip (200ms → 10ms)
- Semantic caching: similar (not identical) queries return cached answers
- Stream responses: user sees first token in 200ms even if full answer takes 1.5s
- Horizontal scale API servers; vertical scale LLM concurrency (rate limits)
- Monitor: cache hit rates, LLM queue depth, p99 latency by query type

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Modal RAG

**Scenario:** Your documentation includes architecture diagrams, flowcharts, and screenshots alongside text. Users ask questions like "What does the data flow look like in our ETL pipeline?" and expect answers referencing visual content. Build a multi-modal RAG system.

<details>
<summary>💡 Hint</summary>
Use a vision model to describe images during indexing → embed descriptions as text. At query time, retrieve both text and image-description chunks. Optionally include the original image in the response.
</details>

<details>
<summary>✅ Solution</summary>

```python
import base64

class MultiModalRAG:
    """RAG that handles text, images, and diagrams."""
    
    async def index_document(self, doc_path: str):
        """Extract and index all modalities from a document."""
        content = parse_document(doc_path)  # Returns text blocks + image blocks
        
        chunks = []
        for block in content:
            if block["type"] == "text":
                chunks.append({"text": block["content"], "type": "text"})
            
            elif block["type"] == "image":
                # Describe image with vision model
                description = await self.describe_image(block["image_bytes"])
                chunks.append({
                    "text": f"[Diagram]: {description}",
                    "type": "image",
                    "image_url": block.get("url"),  # For displaying in response
                })
        
        # Embed all chunks (text descriptions of everything)
        embeddings = embed([c["text"] for c in chunks])
        
        # Store in vector DB with type metadata
        for chunk, emb in zip(chunks, embeddings):
            vector_db.upsert({
                "id": generate_id(),
                "vector": emb,
                "metadata": {
                    "text": chunk["text"],
                    "type": chunk["type"],
                    "image_url": chunk.get("image_url"),
                    "source": doc_path,
                }
            })
    
    async def answer(self, question: str) -> dict:
        """Answer using both text and visual content."""
        results = vector_db.search(embed(question), top_k=5)
        
        # Separate text and image results
        text_context = [r.metadata["text"] for r in results if r.metadata["type"] == "text"]
        image_context = [r.metadata for r in results if r.metadata["type"] == "image"]
        
        # Generate answer referencing both
        context = "\n\n".join(text_context)
        if image_context:
            context += "\n\n[Visual content found]:\n" + "\n".join(
                [f"- {img['text']}" for img in image_context]
            )
        
        answer = await generate(question, context)
        
        return {
            "answer": answer,
            "images": [img["image_url"] for img in image_context if img.get("image_url")],
            "text_sources": text_context[:2],
        }
    
    async def describe_image(self, image_bytes: bytes) -> str:
        b64 = base64.b64encode(image_bytes).decode()
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this technical diagram in detail. Include all components, connections, data flows, and labels visible in the image."},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}}
                ]
            }],
            max_tokens=500,
        )
        return response.choices[0].message.content
```

**Key Points:**
- Images become text descriptions during indexing → unified text-based retrieval
- Original image URLs preserved for display in the response UI
- Vision model describes diagrams in detail (components, flows, labels)
- Cost: ~$0.01 per image description (one-time during indexing)
- For architecture questions: image chunks often rank higher than text chunks
- Future: multi-modal embedding models (CLIP) can embed images directly without description

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are the main stages of a RAG retrieval pipeline?**
A: A typical pipeline includes query processing (rewriting, expansion), first-stage retrieval (dense or sparse search over a vector store or BM25 index), reranking (scoring candidate passages more precisely), and context assembly (ordering and truncating passages for the prompt). Each stage refines the result set.

**Q: What is the difference between dense retrieval and sparse retrieval?**
A: Sparse retrieval (e.g., BM25) relies on exact term matching and TF-IDF-style scoring—fast and interpretable but misses synonyms. Dense retrieval uses learned embeddings to capture semantic similarity—handles paraphrases well but is slower and requires a vector index. Hybrid retrieval combines both to get the benefits of each.

**Q: What is a reranker and where does it fit in the pipeline?**
A: A reranker is a cross-encoder model that scores query-passage relevance more accurately than the initial embedding similarity. It runs after first-stage retrieval on a small candidate set (e.g., top 50), reordering them to surface the most relevant passages before they're passed to the LLM. Rerankers trade latency for precision.

**Q: What is Reciprocal Rank Fusion (RRF) and when is it used?**
A: RRF is a score-free fusion method that combines ranked lists from multiple retrievers (e.g., BM25 + dense) by summing reciprocal ranks. It's simple, parameter-free, and empirically robust—often outperforming score-based fusion because it avoids scale incompatibility between different retrieval systems.

**Q: What is query rewriting and why is it used?**
A: Query rewriting uses an LLM to reformulate the user's question into a form more likely to match the document index—e.g., expanding acronyms, adding context from prior conversation turns, or generating multiple alternative phrasings (HyDE, multi-query). It improves recall when the user's phrasing doesn't match document vocabulary.

**Q: What is HyDE (Hypothetical Document Embeddings)?**
A: HyDE generates a hypothetical answer to the query using an LLM, then embeds that answer and uses it as the search query instead of the raw question. Because the hypothetical answer resembles the style of actual documents, it often retrieves more relevant passages than embedding the question directly.

**Q: How do you handle multi-hop questions in retrieval?**
A: Multi-hop questions require information from multiple passages that must be combined. Strategies include iterative retrieval (retrieve, extract a sub-answer, use it to form a new query), graph-based retrieval (follow entity links), or decomposing the question into sub-questions and retrieving for each independently before synthesizing.

**Q: How do you scale a retrieval pipeline to billions of documents?**
A: Use approximate nearest neighbor (ANN) indexes (e.g., HNSW, IVF-PQ in Faiss/Pinecone/Weaviate) rather than exact search. Shard the index across nodes. Apply quantization to reduce vector storage. Use a cascaded retrieval architecture: fast coarse retrieval narrows to a manageable candidate set before precise reranking.

---

## 💼 Interview Tips

- Frame your retrieval pipeline as a funnel: each stage trades coverage for precision. Showing you think in terms of recall@k at each stage demonstrates systems-level thinking.
- Hybrid retrieval (BM25 + dense + RRF) is almost always better than either alone in production—mentioning it unprompted signals practical experience over textbook knowledge.
- Senior interviewers will probe latency budgets. Know the rough latency of each pipeline stage and where you'd optimize first (often reranking is the bottleneck).
- Discuss failure modes: what happens when the index is stale, when queries are adversarial, or when retrieval consistently misses for a certain query type? Having answers to these shows production readiness.
- Bring up observability—logging retrieval scores, monitoring recall metrics, and alerting on drift lets you catch degradation before users do. This is a differentiator for senior candidates.
