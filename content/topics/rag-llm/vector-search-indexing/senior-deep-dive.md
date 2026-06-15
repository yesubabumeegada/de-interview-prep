---
title: "Vector Search & Indexing - Senior Deep Dive"
topic: rag-llm
subtopic: vector-search-indexing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [rag, llm, vector-search, hnsw, ivf, hybrid-search, reranking, ann]
---

# Vector Search & Indexing — Senior Deep Dive

## HNSW Internals: Why It's the Default Choice

HNSW (Hierarchical Navigable Small World) builds a multi-layer graph where:
- **Bottom layer (layer 0)**: every node, dense connections to nearest neighbors
- **Upper layers**: logarithmically fewer nodes, long-range "highway" connections
- **Search**: enter at top layer, greedily descend to target

```
Layer 2:  A ─────────────────── Z        (few nodes, long jumps)
Layer 1:  A ──── C ──── F ──── Z        (medium density)
Layer 0:  A─B─C─D─E─F─G─H─I─J─Z        (all nodes, short hops)

Search for query Q:
  1. Enter at layer 2, find closest node to Q → greedy descent
  2. Drop to layer 1, refine
  3. Drop to layer 0, collect ef_search candidates
  4. Return top-k
```

**Key parameters:**
```python
import hnswlib

index = hnswlib.Index(space='cosine', dim=1536)
index.init_index(
    max_elements=1_000_000,
    ef_construction=200,   # candidates explored during build (higher = better quality, slower build)
    M=16                   # connections per node per layer (higher = better recall, more memory)
)

# Memory: ~(M * 2 * dim * 4 bytes) per vector
# M=16, dim=1536: ~200KB per 1000 vectors

# Search-time parameter
index.set_ef(50)           # candidates explored at query time (ef >= k)
# ef=50: good balance; ef=200: high recall but slower
```

**Recall vs latency tradeoff:**
```
M=16, ef_construction=200, ef=50:
  Recall@10: ~97%,  Latency p99: 3ms   (production default)

M=32, ef_construction=400, ef=100:
  Recall@10: ~99.5%, Latency p99: 8ms  (high-stakes retrieval)

M=8,  ef_construction=100, ef=25:
  Recall@10: ~92%,  Latency p99: 1ms   (cost-optimized)
```

---

## IVF + PQ: Memory-Efficient Large-Scale Search

For 100M+ vectors where HNSW memory is prohibitive:

```
HNSW at 1B vectors (dim=1536, M=16): ~3TB RAM → impractical
IVF-PQ at 1B vectors: ~50GB RAM → feasible
```

**IVF (Inverted File Index):**
```python
import faiss

d = 1536      # dimension
nlist = 4096  # number of Voronoi cells (clusters)
quantizer = faiss.IndexFlatL2(d)
index = faiss.IndexIVFFlat(quantizer, d, nlist)

index.train(train_vectors)   # k-means clustering to build cells
index.add(all_vectors)

index.nprobe = 64            # search 64 out of 4096 cells
# nprobe=64: ~98% recall for typical datasets
# nprobe=8: ~85% recall, 8x faster
```

**PQ (Product Quantization) for memory compression:**
```python
# PQ compresses each vector from 1536 float32 (6144 bytes) to 96 bytes
# by splitting dimensions into sub-spaces and quantizing each

m = 96        # number of sub-quantizers (must divide d evenly: 1536/96=16)
bits = 8      # bits per sub-quantizer (2^8 = 256 centroids per sub-space)

index = faiss.IndexIVFPQ(quantizer, d, nlist, m, bits)
# Memory: 1B vectors × 96 bytes = 96GB (vs 6TB raw)
# Recall hit: ~5-8% vs IVFFlat (acceptable for most applications)
```

---

## Hybrid Search: Dense + Sparse

Pure dense search misses keyword matches; pure BM25 misses semantic similarity. Hybrid combines both.

```python
from rank_bm25 import BM25Okapi
import numpy as np

def hybrid_search(query: str, top_k: int = 10, alpha: float = 0.6):
    """
    alpha: weight for dense scores (1-alpha for sparse)
    alpha=0.6: slight preference for semantic; alpha=0.3: prefer keywords
    """
    # Dense search
    query_embedding = embed(query)
    dense_results = vector_index.search(query_embedding, top_k * 3)
    # Returns [(doc_id, score), ...]

    # Sparse BM25 search
    tokenized_query = query.lower().split()
    sparse_scores = bm25.get_scores(tokenized_query)
    sparse_top = np.argsort(sparse_scores)[::-1][:top_k * 3]

    # Reciprocal Rank Fusion (RRF) — robust combination method
    k = 60  # RRF constant (typically 60)
    rrf_scores = {}

    for rank, (doc_id, _) in enumerate(dense_results):
        rrf_scores[doc_id] = rrf_scores.get(doc_id, 0) + 1 / (k + rank + 1)

    for rank, doc_id in enumerate(sparse_top):
        rrf_scores[doc_id] = rrf_scores.get(doc_id, 0) + (1 - alpha) / (k + rank + 1)

    return sorted(rrf_scores.items(), key=lambda x: -x[1])[:top_k]
```

**When hybrid beats dense-only:**
- Product catalog search: "iPhone 15 Pro 256GB titanium" — exact model number match
- Legal/medical docs: precise terminology matters (BM25 excels)
- Code search: function/variable names are literal keywords

---

## Re-ranking: Two-Stage Retrieval

```
Stage 1 (ANN): Fast approximate retrieval of top-100 candidates (milliseconds)
Stage 2 (Cross-encoder): Precise re-scoring of top-100 → return top-10 (adds ~50ms)

Combined latency: ~80ms vs 1000ms for cross-encoder on full corpus
```

```python
from sentence_transformers import CrossEncoder

cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

def rerank(query: str, candidates: list[dict], top_k: int = 5) -> list[dict]:
    pairs = [(query, doc["text"]) for doc in candidates]
    scores = cross_encoder.predict(pairs)

    ranked = sorted(
        zip(candidates, scores),
        key=lambda x: -x[1]
    )
    return [doc for doc, _ in ranked[:top_k]]

# Pipeline
stage1 = vector_index.search(query_embedding, k=100)  # fast ANN
stage2 = rerank(query, stage1, top_k=5)               # precise reranking
```

**Cohere Rerank API (managed alternative):**
```python
import cohere
co = cohere.Client(api_key)

results = co.rerank(
    query=query,
    documents=[doc["text"] for doc in candidates],
    top_n=5,
    model="rerank-english-v3.0"
)
# ~$0.002 per 1000 searches — cheap for production
```

---

## Filtering Strategies: Pre-filter vs Post-filter

```
Scenario: 10M product vectors, search for "red sneakers" filtered to brand="Nike" (1% of corpus = 100K docs)
```

**Post-filter (naive):** Retrieve top-K from full corpus → filter by brand
```
Problem: if top-200 has only 15 Nike docs, you return 15 results instead of top-5
```

**Pre-filter:** Build a bitmap of Nike docs → search only within that subset
```python
# Weaviate pre-filtering
result = client.query.get("Product", ["name", "brand"]).with_near_vector(
    {"vector": query_embedding}
).with_where({
    "path": ["brand"],
    "operator": "Equal",
    "valueText": "Nike"
}).with_limit(5).do()
# Efficient when filter selectivity > 5% (large enough subset)
# Degrades when filter is very selective (< 0.1%) — tiny search space
```

**In-filter (Qdrant):** Search the graph but skip non-matching nodes
```python
from qdrant_client.models import Filter, FieldCondition, MatchValue

results = qdrant_client.search(
    collection_name="products",
    query_vector=query_embedding,
    query_filter=Filter(must=[
        FieldCondition(key="brand", match=MatchValue(value="Nike"))
    ]),
    limit=5
)
# Best for moderate selectivity — avoids rebuilding the ANN index per filter
```

**Choosing strategy:**
```
Filter covers > 20% of corpus  → pre-filter (efficient subset search)
Filter covers 1-20%            → in-filter / payload indexing
Filter covers < 1%             → post-filter with oversampling (retrieve 10x, filter down)
```

---

## Vector DB Comparison: Production Decision Matrix

| | Pinecone | Weaviate | Qdrant | pgvector | ChromaDB |
|--|---------|---------|-------|---------|---------|
| **Hosting** | Managed SaaS | Self/managed | Self/managed | Self (Postgres) | Self/local |
| **Scale** | 1B+ | 100M+ | 100M+ | ~10M practical | ~1M |
| **Hybrid search** | Yes | Yes (BM25) | Yes | Limited | No |
| **Filtering** | Good | Excellent | Excellent | Good | Basic |
| **Cost at 10M vectors** | ~$200/mo | Free (self-host) | Free (self-host) | EC2 cost | Free |
| **Best for** | Startup, managed | Complex filtering | High perf/filtering | Already on Postgres | Dev/prototyping |

**When to choose pgvector:**
```sql
-- If you're already on PostgreSQL and scale < 5M vectors
CREATE EXTENSION vector;
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    content TEXT,
    embedding vector(1536)
);

-- HNSW index (pgvector 0.5+)
CREATE INDEX ON embeddings USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Hybrid: combine with full-text search
SELECT id, content,
    embedding <=> query_embedding AS vector_dist,
    ts_rank(to_tsvector(content), query_tsquery) AS text_rank
FROM embeddings
ORDER BY (0.6 * (1 - (embedding <=> $1)) + 0.4 * ts_rank(...)) DESC
LIMIT 10;
```

---

## Interview Tips

> **Tip 1:** "What's the difference between HNSW and IVF?" — "HNSW builds a multi-layer navigable graph — excellent recall (~97-99%) with fast queries (~3ms), but memory-intensive (~200 bytes/vector). IVF clusters vectors into Voronoi cells and searches only a subset (nprobe cells) — lower recall (~92-98%) but scales to billions of vectors with PQ compression (~50-100 bytes/vector compressed). Choose HNSW for up to ~50M vectors; IVF-PQ for hundreds of millions or billions."

> **Tip 2:** "When would you use hybrid search over pure dense search?" — "When queries contain exact terms that matter — product codes, proper nouns, technical identifiers, code snippets. Dense search handles semantic similarity but can miss 'GPT-4-turbo-preview' if those exact tokens weren't prominent in training data. Hybrid with RRF fusion gives you semantic understanding plus keyword precision. I'd always benchmark both on your specific dataset before deciding."

> **Tip 3:** "How do you handle metadata filtering efficiently?" — "Filtering strategy depends on selectivity. If the filter matches > 10% of the corpus (e.g., filtering by language='en' in a mostly-English dataset), pre-filter is efficient. For moderate selectivity (1-10%), use in-graph filtering (Qdrant payload index). For very selective filters (< 1%), post-filter with over-retrieval works. The failure mode to avoid: returning too few results because the filtered subset was exhausted — oversampling by 5-10x at stage 1 solves this."
