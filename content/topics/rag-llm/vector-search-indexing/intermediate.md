---
title: "Vector Search & Indexing - Intermediate"
topic: rag-llm
subtopic: vector-search-indexing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, vector-search, hybrid-search, bm25, reranking, filtering, pgvector]
---

# Vector Search & Indexing — Intermediate

## Product Quantization (PQ) for Memory Efficiency

HNSW + raw FP32 vectors at 1M × 1536 dimensions = **6 GB of RAM**. PQ compresses vectors ~32x while preserving ~90% of recall:

```python
import faiss
import numpy as np

d = 1536    # dimensions
n = 1000000 # 1M vectors
M_pq = 96   # number of subvectors (d must be divisible by M_pq)
nbits = 8   # bits per subquantizer (256 centroids per subvector)

vectors = np.random.rand(n, d).astype("float32")

# IVF + PQ: scalable + compressed
n_list = 4096
quantizer = faiss.IndexFlatIP(d)
index = faiss.IndexIVFPQ(quantizer, d, n_list, M_pq, nbits)

# Must train on representative sample
train_size = min(100000, n)
index.train(vectors[:train_size])
index.add(vectors)
index.nprobe = 64

# Memory comparison:
# Raw FP32 1M×1536:  1M × 1536 × 4B = 6.0 GB
# PQ-compressed:     1M × M_pq × 1B = 96 MB  → 62x compression
# With HNSW graph:   add ~2KB/vector = +2 GB
# IVF centroids:     n_list × d × 4B = ~24 MB

query = np.random.rand(1, d).astype("float32")
distances, indices = index.search(query, 10)

# Benchmark (A100 GPU, 1M vectors):
# IVFFlat nprobe=64:  ~5ms/query, recall@10 ~95%
# IVFPQ nprobe=64:    ~2ms/query, recall@10 ~90%, 62x less RAM
```

---

## Hybrid Search: Dense + Sparse

Dense (embedding) search is great for semantic similarity. Sparse (keyword) search (BM25) is great for exact term matching. Hybrid combines both.

**When to use hybrid:**
- Technical documentation with specific function names, version numbers
- Product search where brand/model exact match matters
- Code search where syntax matters

### BM25 (Sparse) + Dense (Embedding) with Reciprocal Rank Fusion

```python
from rank_bm25 import BM25Okapi
import numpy as np
from openai import OpenAI

client = OpenAI()

# Sample corpus
documents = [
    "Apache Spark uses RDDs for distributed data processing",
    "Kafka consumer groups allow parallel message consumption",
    "dbt transforms raw data into analytics-ready models",
    "HNSW algorithm builds hierarchical graph for vector search",
    "Snowflake separates compute from storage for elastic scaling"
]

# Build BM25 index (sparse)
tokenized_docs = [doc.lower().split() for doc in documents]
bm25 = BM25Okapi(tokenized_docs)

# Build dense index (embeddings)
embeddings_response = client.embeddings.create(
    model="text-embedding-3-small",
    input=documents
)
doc_embeddings = np.array([e.embedding for e in embeddings_response.data], dtype="float32")

import faiss
d = len(doc_embeddings[0])
faiss.normalize_L2(doc_embeddings)
dense_index = faiss.IndexFlatIP(d)
dense_index.add(doc_embeddings)

def reciprocal_rank_fusion(rankings: list[list[int]], k: int = 60) -> list[tuple[int, float]]:
    """
    RRF formula: score(d, q) = Σ 1/(k + rank(d))
    k=60 is standard (from original RRF paper).
    """
    scores = {}
    for ranking in rankings:
        for rank, doc_id in enumerate(ranking):
            scores[doc_id] = scores.get(doc_id, 0) + 1.0 / (k + rank + 1)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)

def hybrid_search(query: str, top_k: int = 5, alpha: float = 0.5) -> list[dict]:
    """
    Hybrid search: combine BM25 + dense retrieval via RRF.
    alpha: weight for dense (0=pure BM25, 1=pure dense) — not used in RRF directly
    but can be used for weighted score combination.
    """
    # Sparse (BM25) search
    bm25_scores = bm25.get_scores(query.lower().split())
    sparse_ranking = np.argsort(bm25_scores)[::-1].tolist()

    # Dense search
    query_emb = np.array([client.embeddings.create(model="text-embedding-3-small", input=query).data[0].embedding], dtype="float32")
    faiss.normalize_L2(query_emb)
    _, dense_indices = dense_index.search(query_emb, len(documents))
    dense_ranking = dense_indices[0].tolist()

    # Fuse with RRF
    fused = reciprocal_rank_fusion([sparse_ranking, dense_ranking])

    return [
        {"doc_id": doc_id, "text": documents[doc_id], "rrf_score": score}
        for doc_id, score in fused[:top_k]
    ]

# Test
results = hybrid_search("HNSW hierarchical graph algorithm")
for r in results:
    print(f"Score: {r['rrf_score']:.4f} | {r['text'][:60]}")
```

---

## Re-ranking

Re-ranking uses a more expensive cross-encoder model as a second pass to re-order the top-k results from the initial retrieval:

```python
# Cross-encoder reranking with sentence-transformers
from sentence_transformers import CrossEncoder
from openai import OpenAI

cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

def rerank_results(query: str, candidates: list[str], top_k: int = 3) -> list[dict]:
    """
    Re-rank candidates using cross-encoder.
    Cross-encoder processes (query, document) pairs jointly — much more accurate than
    bi-encoder similarity but cannot be pre-computed (too slow for full corpus).
    """
    pairs = [(query, doc) for doc in candidates]
    scores = cross_encoder.predict(pairs)

    ranked = sorted(
        zip(candidates, scores),
        key=lambda x: x[1],
        reverse=True
    )

    return [{"text": doc, "cross_encoder_score": float(score)} for doc, score in ranked[:top_k]]

# Cohere Rerank (API-based — no local model needed)
import cohere
co = cohere.Client("...")

def cohere_rerank(query: str, documents: list[str], top_n: int = 3) -> list[dict]:
    """Use Cohere Rerank API — state-of-art reranker, no local GPU needed."""
    results = co.rerank(
        query=query,
        documents=documents,
        top_n=top_n,
        model="rerank-english-v3.0"  # or rerank-multilingual-v3.0
    )
    return [
        {
            "text": documents[r.index],
            "relevance_score": r.relevance_score,
            "original_rank": r.index
        }
        for r in results.results
    ]

def rag_with_reranking(query: str, top_k_retrieve: int = 20, top_k_rerank: int = 5) -> str:
    """
    Two-stage retrieval:
    Stage 1: Fast ANN search, retrieve top 20 (high recall, lower precision)
    Stage 2: Cross-encoder re-rank to top 5 (high precision)
    """
    # Stage 1: fast vector search (retrieve more than needed)
    candidate_docs = vector_search(query, top_k=top_k_retrieve)

    # Stage 2: expensive but accurate reranking
    reranked = cohere_rerank(query, [d["text"] for d in candidate_docs], top_n=top_k_rerank)

    # Use reranked results for LLM context
    context = "\n\n".join(r["text"] for r in reranked)
    return context
```

**Benchmark impact of reranking:**
- Initial retrieval recall@20: ~85%
- After cross-encoder rerank to top 5: precision@5 improves from ~60% to ~85%
- Latency: +100-300ms for cross-encoder (local), +150-400ms for Cohere API

---

## Filtering: Pre-filter vs Post-filter vs In-filter

```python
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range, SearchParams

qdrant = QdrantClient(host="localhost", port=6333)

# Strategy 1: Pre-filter (filter candidates before ANN search)
# Use when: filter is highly selective (< 10% match)
# Risk: if filter is too narrow, ANN graph traversal has few valid neighbors
def pre_filter_search(query_vector: list[float], department: str, top_k: int = 5):
    return qdrant.search(
        collection_name="knowledge_base",
        query_vector=query_vector,
        query_filter=Filter(must=[
            FieldCondition(key="metadata.department", match=MatchValue(value=department))
        ]),
        limit=top_k,
        search_params=SearchParams(hnsw_ef=128, exact=False)
    )

# Strategy 2: Post-filter (ANN search → apply filter to results)
# Use when: filter is less selective (50%+ match)
# Risk: may return fewer than top_k results if many candidates filtered out
def post_filter_search(query_vector: list[float], date_after: str, top_k: int = 5):
    # Retrieve more candidates (top_k * 5) to account for post-filter removal
    candidates = qdrant.search(
        collection_name="knowledge_base",
        query_vector=query_vector,
        limit=top_k * 5
    )
    return [c for c in candidates if c.payload.get("date", "") >= date_after][:top_k]

# Strategy 3: In-filter / Native filtered search (Qdrant recommended approach)
# Qdrant uses HNSW with payload indexing — filter is applied during graph traversal
# Best of both worlds: no recall penalty, no post-filter shortage
def native_filtered_search(query_vector: list[float], department: str, min_score: float, top_k: int = 5):
    return qdrant.search(
        collection_name="knowledge_base",
        query_vector=query_vector,
        query_filter=Filter(
            must=[
                FieldCondition(key="metadata.department", match=MatchValue(value=department)),
                FieldCondition(key="metadata.quality_score", range=Range(gte=min_score))
            ]
        ),
        limit=top_k,
        with_payload=True,
        score_threshold=0.7  # minimum similarity score
    )

# Rule of thumb:
# Filter selectivity > 50%: pre-filter (more data to traverse, but still navigable)
# Filter selectivity 10-50%: in-filter (Qdrant native)
# Filter selectivity < 10%: post-filter + retrieve extra candidates
```

---

## pgvector: Vector Search in PostgreSQL

For teams already on PostgreSQL, pgvector adds vector search without new infrastructure:

```sql
-- Enable extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create table with embedding column
CREATE TABLE document_chunks (
    id          BIGSERIAL PRIMARY KEY,
    doc_id      TEXT NOT NULL,
    chunk_text  TEXT NOT NULL,
    embedding   vector(1536),   -- OpenAI text-embedding-3-small dimension
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    metadata    JSONB
);

-- HNSW index (recommended for most workloads)
CREATE INDEX ON document_chunks USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 200);

-- IVFFlat alternative (less memory, lower build time)
-- CREATE INDEX ON document_chunks USING ivfflat (embedding vector_cosine_ops)
-- WITH (lists = 100);  -- lists ≈ sqrt(n_rows)

-- Similarity search (cosine)
SELECT
    id,
    chunk_text,
    metadata,
    1 - (embedding <=> '[0.12, -0.45, ...]'::vector) AS cosine_similarity
FROM document_chunks
WHERE metadata->>'department' = 'engineering'    -- filter in SQL
ORDER BY embedding <=> '[0.12, -0.45, ...]'::vector
LIMIT 10;

-- Hybrid search: BM25 (full-text) + vector
SELECT
    id,
    chunk_text,
    ts_rank(to_tsvector('english', chunk_text), plainto_tsquery('english', $1)) AS bm25_score,
    1 - (embedding <=> $2::vector) AS dense_score
FROM document_chunks
WHERE to_tsvector('english', chunk_text) @@ plainto_tsquery('english', $1)
ORDER BY bm25_score + dense_score DESC
LIMIT 10;
```

```python
import psycopg2
import json

def pgvector_search(query_embedding: list[float], filter_dept: str, top_k: int = 5) -> list[dict]:
    conn = psycopg2.connect("postgresql://user:pass@localhost/rag_db")
    cur = conn.cursor()

    cur.execute("""
        SELECT id, chunk_text, metadata,
               1 - (embedding <=> %s::vector) AS similarity
        FROM document_chunks
        WHERE metadata->>'department' = %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """, (query_embedding, filter_dept, query_embedding, top_k))

    return [
        {"id": row[0], "text": row[1], "metadata": row[2], "similarity": row[3]}
        for row in cur.fetchall()
    ]
```

**pgvector vs dedicated vector DB:**

| Criteria | pgvector | Pinecone/Qdrant |
|----------|---------|----------------|
| Scale | <5M vectors (good perf) | 100M+ |
| Infra overhead | None (already have PG) | New service |
| Filtering | Full SQL power | Metadata filters |
| Hybrid search | Native FTS + vector | Limited |
| ACID transactions | Yes | No |
| Best for | Existing PG teams, <1M docs | Dedicated ML search |
