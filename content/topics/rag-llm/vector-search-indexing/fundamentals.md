---
title: "Vector Search & Indexing - Fundamentals"
topic: rag-llm
subtopic: vector-search-indexing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [rag, vector-search, embeddings, ann, hnsw, faiss, pinecone]
---

# Vector Search & Indexing — Fundamentals

## Why This Matters for Data Engineers

RAG pipelines depend on fast, accurate vector search. As a DE, you'll decide which vector database to use, configure index parameters, and debug retrieval quality. Understanding ANN algorithms helps you tune for the right precision-vs-speed tradeoff.

---

## What Is Vector Search?

Instead of searching by exact keyword match (like SQL `LIKE`), vector search finds the most semantically similar items by comparing mathematical representations (embeddings) in high-dimensional space.

```
Query: "revenue by region"  →  embed  →  [0.12, -0.45, 0.83, ...]  →  find nearest neighbors
                                                                              │
                                                                    ["sales by geography",
                                                                     "regional revenue breakdown",
                                                                     "income per area"]
```

---

## ANN (Approximate Nearest Neighbor) Algorithms

Exact nearest neighbor search in high-dimensional space is O(n × d) — too slow at scale. ANN algorithms trade a small amount of recall for massive speed gains.

### HNSW (Hierarchical Navigable Small World)

The most widely used algorithm in production. Key idea: build a layered graph where upper layers are sparse (fast navigation) and lower layers are dense (precise search).

```
Layer 2 (sparse): ●────────────────●
                        │
Layer 1 (medium): ●──●──●──●──────●──●
                        │
Layer 0 (dense):  ●─●─●─●─●─●─●─●─●─●─●

Search: start at top layer, greedily move to neighbors closer to query, zoom down layers
```

**HNSW parameters:**
- **`M`**: number of connections per node. Higher M → better recall, more memory, slower build. Default: 16-32. For high-recall production: 32-64.
- **`ef_construction`**: search width during index build. Higher → better index quality, slower build. Default: 200. Recommended: 400+ for production.
- **`ef_search`** (runtime): search width during query. Higher → better recall, slower query. Tune to hit your recall target.

**Memory**: HNSW stores the full graph in RAM: ~4 bytes/dimension × n_vectors + graph overhead (~2KB/vector at M=16).

### IVF (Inverted File Index)

Divides the vector space into `n_list` Voronoi cells. At search time, only probes `nprobe` nearest cells.

```
Train:  k-means → n_list cluster centroids
Index:  assign each vector to nearest centroid (inverted list)
Search: find top nprobe centroids → search their lists → merge results
```

- **`n_list`**: number of clusters. Rule of thumb: `sqrt(n_vectors)`. For 1M vectors: 1000 clusters.
- **`nprobe`**: clusters to search. More nprobe → better recall, slower query. Start with n_list/10.

---

## FAISS (Facebook AI Similarity Search)

The reference library for vector indexing. Key index types:

```python
import faiss
import numpy as np

d = 1536    # OpenAI text-embedding-3-small dimension
n = 100000  # number of vectors

# Generate sample data
vectors = np.random.rand(n, d).astype("float32")
faiss.normalize_L2(vectors)  # normalize for cosine similarity

# Option 1: Flat (exact search, brute force — use only for < 100K vectors)
index_flat = faiss.IndexFlatIP(d)  # IP = inner product (cosine after normalization)
index_flat.add(vectors)
k = 10
query = np.random.rand(1, d).astype("float32")
faiss.normalize_L2(query)
distances, indices = index_flat.search(query, k)
print(f"Top-{k} matches: {indices[0]}")

# Option 2: IVF (fast, scalable to millions)
n_list = 1000  # clusters
quantizer = faiss.IndexFlatIP(d)
index_ivf = faiss.IndexIVFFlat(quantizer, d, n_list, faiss.METRIC_INNER_PRODUCT)
index_ivf.train(vectors)   # must train first
index_ivf.add(vectors)
index_ivf.nprobe = 50      # search 50 cells (5% of total — tune for recall)
distances, indices = index_ivf.search(query, k)

# Option 3: HNSW (fast, great recall, RAM-intensive)
index_hnsw = faiss.IndexHNSWFlat(d, 32)  # M=32
index_hnsw.hnsw.efConstruction = 400
index_hnsw.add(vectors)
index_hnsw.hnsw.efSearch = 64  # runtime parameter
distances, indices = index_hnsw.search(query, k)
```

---

## Embedding Model Selection

| Model | Dimension | Cost | Retrieval Quality | Use Case |
|-------|-----------|------|------------------|---------|
| `text-embedding-3-small` | 1536 | $0.02/M tokens | Good | General RAG, cost-sensitive |
| `text-embedding-3-large` | 3072 | $0.13/M tokens | Excellent | High-stakes retrieval |
| `text-embedding-ada-002` | 1536 | $0.10/M tokens | Good | Legacy OpenAI |
| `sentence-transformers/all-mpnet-base-v2` | 768 | Free (self-hosted) | Good | On-premise |
| `BAAI/bge-large-en-v1.5` | 1024 | Free (self-hosted) | Excellent | Best open-source |
| `Cohere Embed v3` | 1024 | $0.10/M tokens | Excellent | Multilingual, reranking |

```python
from openai import OpenAI
from sentence_transformers import SentenceTransformer

# OpenAI — API, no infra
openai_client = OpenAI()
def embed_openai(texts: list[str]) -> list[list[float]]:
    resp = openai_client.embeddings.create(model="text-embedding-3-small", input=texts)
    return [d.embedding for d in resp.data]

# Sentence Transformers — free, self-hosted
st_model = SentenceTransformer("BAAI/bge-large-en-v1.5")
def embed_local(texts: list[str]) -> list[list[float]]:
    return st_model.encode(texts, normalize_embeddings=True).tolist()

# Quick benchmark: measure recall@10 on a test set
# BAAI/bge-large: MTEB score ~63.3 (state-of-art open-source)
# text-embedding-3-large: MTEB score ~64.6 (state-of-art commercial)
# text-embedding-3-small: MTEB score ~61.5
```

---

## Vector Database Quick Comparison

| | Pinecone | Weaviate | Qdrant | ChromaDB | pgvector |
|--|---------|---------|--------|---------|---------|
| **Deployment** | Managed SaaS | Self/Cloud | Self/Cloud | Self/Cloud | PostgreSQL extension |
| **ANN Algorithm** | Proprietary (HNSW-based) | HNSW | HNSW | HNSW | HNSW or IVFFlat |
| **Filtering** | Metadata filter | GraphQL + filter | Payload filter | Metadata filter | SQL WHERE |
| **Scale** | 100M+ vectors | 10M+ | 10M+ | <1M (dev) | <5M (perf) |
| **Cost** | $$$$ | $ | $ | Free | $ (compute) |
| **Best for** | Production, managed | ML search + graphs | High-perf self-hosted | Prototyping | Existing PG infra |

---

## Key Terms

| Term | Definition |
|------|-----------|
| **ANN** | Approximate Nearest Neighbor — trades small recall loss for huge speed gain |
| **HNSW** | Graph-based ANN; best general-purpose choice |
| **IVF** | Clustering-based ANN; good for batch workloads |
| **ef_construction** | Build-time search width (HNSW); higher = better index, slower build |
| **ef_search / nprobe** | Query-time search width; higher = better recall, slower query |
| **Recall@k** | % of true nearest neighbors found in top-k results |
| **QPS** | Queries per second — primary throughput metric |
