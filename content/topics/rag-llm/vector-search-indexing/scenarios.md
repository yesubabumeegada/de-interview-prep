---
title: "Vector Search & Indexing - Scenario Questions"
topic: rag-llm
subtopic: vector-search-indexing
content_type: scenario_question
tags: [rag, llm, vector-search, scenarios, interview, hnsw, hybrid-search]
---

# Scenario Questions — Vector Search & Indexing

<article data-difficulty="junior">

## 🟢 Junior: Choose a Vector Database for a New RAG Project

**Scenario:** You're starting a new internal knowledge base RAG project. You have ~50,000 documents, a small team, and you're already running PostgreSQL on AWS RDS. A colleague suggests using Pinecone. Another says to use ChromaDB locally for development. What do you recommend and why?

<details>
<summary>✅ Solution</summary>

For 50,000 documents with an existing PostgreSQL setup, **pgvector is the pragmatic choice** for production, with ChromaDB for local development.

**Why not Pinecone at this scale:**
- Pinecone is a managed SaaS — adds $70-200/month for a scale you don't need
- 50K documents is tiny; pgvector handles millions on a modest RDS instance
- Adds another system to manage, authenticate, and monitor

**Recommended setup:**

```sql
-- Enable pgvector on existing PostgreSQL
CREATE EXTENSION IF NOT EXISTS vector;

-- Create embeddings table
CREATE TABLE document_embeddings (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id      TEXT NOT NULL REFERENCES documents(id),
    content     TEXT NOT NULL,
    embedding   vector(1536),           -- OpenAI text-embedding-3-small
    metadata    JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- HNSW index (pgvector 0.5+) — fast approximate search
CREATE INDEX ON document_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
```

```python
# Similarity search
def search(query: str, top_k: int = 5) -> list[dict]:
    query_embedding = embed(query)  # [1536 floats]

    results = db.execute("""
        SELECT doc_id, content, metadata,
               1 - (embedding <=> %s::vector) AS similarity
        FROM document_embeddings
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """, (query_embedding, query_embedding, top_k))

    return results.fetchall()
```

**For local development — ChromaDB:**
```python
import chromadb

client = chromadb.Client()  # in-memory, no setup
collection = client.create_collection("docs")

collection.add(
    documents=["doc text..."],
    embeddings=[[0.1, 0.2, ...]],
    ids=["doc1"]
)

results = collection.query(query_embeddings=[query_emb], n_results=5)
```

**Migration path:** When you outgrow pgvector (> 5M vectors or need advanced filtering), migrate to Qdrant or Weaviate. The embedding and search logic stays the same — only the client changes.

**Decision rule:** Use pgvector if you're already on Postgres and scale is < 5M vectors. It's one less system, lower ops burden, and supports SQL joins with your existing data.

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Debug Poor RAG Retrieval Quality

**Scenario:** Your company deployed a RAG system over 200K support documents. Users are complaining that answers are wrong or generic. You investigate and find: the embedding model is `text-embedding-ada-002`, chunk size is 2,000 tokens with no overlap, and you're returning top-3 chunks. The vector search recall@5 on your evaluation set is 58%. How do you diagnose and fix this?

<details>
<summary>✅ Solution</summary>

**Diagnosis framework — work backwards from recall@58%:**

58% recall@5 means 42% of the time, the relevant document isn't in the top-5 results. Common causes:

**1. Chunk size too large (most likely culprit)**
```python
# 2,000 tokens ≈ 1,500 words — too large for embedding models
# The embedding averages over 1,500 words; specific answer gets diluted

# Fix: smaller chunks with overlap
def chunk_document(text: str, chunk_size: int = 400, overlap: int = 64):
    tokens = tokenizer.encode(text)
    chunks = []
    for i in range(0, len(tokens), chunk_size - overlap):
        chunk_tokens = tokens[i:i + chunk_size]
        chunks.append(tokenizer.decode(chunk_tokens))
    return chunks

# Test: compare recall at chunk_size = 256, 384, 512, 768, 1024
# Most support docs peak at 384-512 tokens
```

**2. Missing overlap causes answer to fall between chunks**
```
Without overlap:  |-- chunk 1 --|-- chunk 2 --|-- chunk 3 --|
                               ^ answer spans this boundary → in neither chunk

With overlap:     |-- chunk 1 --|
                           |-- chunk 2 --|
                                    |-- chunk 3 --|
                              ^ answer is fully in chunk 2
```

**3. Upgrade embedding model**
```python
# Benchmark on your evaluation set (200 questions with known answers)
models = [
    "text-embedding-ada-002",           # baseline
    "text-embedding-3-small",           # 4x cheaper, usually better
    "text-embedding-3-large",           # best quality
]
# Run recall@5 for each and pick the Pareto-optimal choice
```

**4. Add hybrid search for keyword-sensitive queries**
```python
# Support queries often contain exact error codes, product names
# "Error 0x80070422 on Windows Update" → BM25 beats dense search here

from rank_bm25 import BM25Okapi

# Build BM25 index over chunks
bm25 = BM25Okapi([chunk.split() for chunk in all_chunks])

def hybrid_search(query: str, top_k: int = 10):
    dense_hits = vector_search(query, top_k * 3)
    sparse_hits = bm25.get_top_n(query.split(), all_chunks, n=top_k * 3)
    return rrf_merge(dense_hits, sparse_hits)[:top_k]
```

**Evaluation loop:**
```python
def evaluate_recall(questions: list[dict], search_fn, k: int = 5) -> float:
    hits = 0
    for q in questions:
        results = search_fn(q["query"], top_k=k)
        if any(q["relevant_doc_id"] in r["doc_id"] for r in results):
            hits += 1
    return hits / len(questions)

# Run after each change
baseline = evaluate_recall(eval_set, vector_search_only)          # 0.58
after_rechunk = evaluate_recall(eval_set, rechunked_search)       # 0.73
after_hybrid = evaluate_recall(eval_set, hybrid_search)           # 0.81
after_rerank = evaluate_recall(eval_set, hybrid_with_rerank)      # 0.87
```

**Expected improvement order:** chunk size fix > embedding upgrade > hybrid search > reranking. Fix in that order — each is incremental effort.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Vector Search Platform for 500M Documents

**Scenario:** You're the lead DE at a legal tech company. You need to build a vector search platform over 500M legal documents (contracts, case law, regulations). Requirements: p99 query latency < 200ms, recall@10 > 95%, support for metadata filtering (jurisdiction, document type, date range), multi-tenant (50 law firms, isolated data), and daily ingestion of 100K new documents. Design the full architecture.

<details>
<summary>✅ Solution</summary>

**Scale analysis:**
```
500M documents × 512 tokens avg × 1536 dim float32:
  Embeddings raw: 500M × 1536 × 4 bytes = 3TB
  HNSW (M=16):   3TB + graph overhead (~50%) = 4.5TB RAM → not feasible
  IVF-PQ:        500M × 96 bytes (m=96, bits=8) = 48GB RAM → feasible

Choice: Qdrant with IVF-PQ or Weaviate with on-disk HNSW (mmap)
```

**Architecture:**

```
Ingestion Pipeline:
  S3 (raw docs)
  → Document Parser (Textract for PDFs, custom for contracts)
  → Chunker (512 tokens, 64 overlap, section-aware)
  → Kafka topic: doc-chunks-to-embed
  → Embedding Workers (10 × GPU instances, text-embedding-3-large batch API)
  → Kafka topic: chunks-with-embeddings
  → Qdrant Writer (upsert with payload: firm_id, jurisdiction, doc_type, date)

Query Pipeline:
  API Gateway (per-firm JWT → extract firm_id)
  → Query Embedder (text-embedding-3-small for latency)
  → Qdrant search with firm_id filter (pre-filter, mandatory)
  → Optional: hybrid BM25 merge (for case law citation numbers)
  → Cross-encoder rerank (top-100 → top-10)
  → Response
```

**Multi-tenancy: collection-per-firm vs payload filtering**

```
50 firms, data isolation requirement:
Option A: 50 separate Qdrant collections
  Pro: complete isolation, simple auth, independent scaling
  Con: 50x management overhead, can't share infrastructure efficiently

Option B: Single collection with firm_id payload filter (mandatory)
  Pro: simpler ops, shared infra
  Con: noisy neighbor risk, slightly slower (filter overhead)
  Con: accidental cross-firm leak if filter is dropped (security risk)

Decision: Collection-per-firm for strict isolation
  (legal data — a cross-firm data leak is career-ending)
  Use Qdrant namespaces or per-firm API keys mapped to collections
```

**Qdrant configuration for 500M docs:**
```python
client.create_collection(
    collection_name=f"firm_{firm_id}_docs",
    vectors_config=VectorParams(
        size=3072,  # text-embedding-3-large
        distance=Distance.COSINE,
        on_disk=True,           # mmap vectors from disk → handles > RAM
        hnsw_config=HnswConfigDiff(
            m=16,
            ef_construct=128,
            on_disk=True        # HNSW graph also on disk (NVMe SSD required)
        )
    ),
    optimizers_config=OptimizersConfigDiff(
        indexing_threshold=50_000,  # switch to HNSW after 50K points
        memmap_threshold=100_000    # mmap after 100K points
    )
)
```

**Daily ingestion of 100K documents:**
```python
# Airflow DAG — daily ingestion
@dag(schedule="0 2 * * *")
def legal_doc_ingestion():

    # Step 1: Fetch new docs from firm SFTP/S3 (fan-out per firm)
    @task
    def fetch_new_docs(firm_id: str) -> list[str]:
        return s3.list_objects_since(yesterday, prefix=f"firms/{firm_id}/")

    # Step 2: Parse + chunk (Spark job for 100K docs)
    @task
    def parse_and_chunk(doc_paths: list[str]) -> list[dict]:
        return spark.read.text(doc_paths).map(chunk_document).collect()

    # Step 3: Embed via OpenAI Batch API (50% cost reduction vs sync)
    @task
    def embed_chunks(chunks: list[dict]) -> list[dict]:
        batch_job = openai.batches.create(
            input_file_id=upload_jsonl(chunks),
            endpoint="/v1/embeddings",
            completion_window="24h"   # daily pipeline has time
        )
        return wait_for_batch(batch_job.id)  # polls until complete

    # Step 4: Upsert to Qdrant
    @task
    def upsert_to_qdrant(chunks_with_embeddings: list[dict], firm_id: str):
        client.upsert(
            collection_name=f"firm_{firm_id}_docs",
            points=[PointStruct(id=c["id"], vector=c["embedding"], payload=c["metadata"])
                    for c in chunks_with_embeddings]
        )
```

**Latency budget for p99 < 200ms:**
```
Query embedding (text-embedding-3-small):  ~60ms
Qdrant ANN search (top-100, on-disk):      ~80ms
Cross-encoder rerank (100 → 10):           ~40ms
Network + serialization:                   ~15ms
Total:                                     ~195ms ✓

If latency is tight:
  - Cache query embeddings (Redis, TTL=1h) → save 60ms on repeated queries
  - Skip rerank for simple queries (route by query complexity)
  - Use text-embedding-3-small for query (3072→1536 dim, 30ms faster)
```

**Trade-offs to discuss in interview:**
1. **On-disk HNSW** adds ~3x latency vs in-memory — acceptable with NVMe, not with HDD
2. **text-embedding-3-large** (3072 dim) has better recall for legal text but doubles memory and latency vs small (1536 dim) — benchmark on legal domain before committing
3. **Cross-encoder rerank** costs 40ms but improves NDCG@10 by ~15% — worth it for legal where precision is critical
4. **Collection-per-firm** → 50 Qdrant collections — manageable but consider Qdrant Cloud for managed hosting as you grow past 50 firms

</details>
</article>
