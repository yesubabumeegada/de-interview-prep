---
title: "Vector Databases - Fundamentals"
topic: rag-llm
subtopic: vector-databases
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [rag, llm, vector-database, similarity-search, ann, hnsw, pinecone, pgvector]
---

# Vector Databases — Fundamentals

## What Is a Vector Database?

A vector database is a specialized storage system designed to **index, store, and query high-dimensional vectors** efficiently. Unlike traditional databases that search by exact match (WHERE id = 42), vector databases find the most similar vectors to a query vector — enabling semantic search.

```python
# Traditional database: exact match
# SELECT * FROM products WHERE name = 'winter jacket'

# Vector database: semantic similarity
# "Find the 10 vectors most similar to this query vector"
# Returns results like "warm coat", "insulated parka", "down jacket"
```

> **Key Insight for DE:** Vector databases solve the same problem as indexes in relational DBs — making search fast. But instead of B-trees on exact values, they use algorithms like HNSW to find approximate nearest neighbors in high-dimensional space.

---

## Why Not Just Use Brute-Force Search?

Brute-force (comparing query against every stored vector) is O(n) — unusable at scale:

| Documents | Dimensions | Brute-Force Time | ANN (HNSW) Time |
|-----------|-----------|-----------------|-----------------|
| 10K | 768 | ~5 ms | ~1 ms |
| 1M | 768 | ~500 ms | ~2 ms |
| 100M | 768 | ~50 seconds | ~5 ms |
| 1B | 768 | ~8 minutes | ~10 ms |

Vector databases use **Approximate Nearest Neighbor (ANN)** algorithms to search in milliseconds regardless of dataset size, trading a small amount of accuracy (typically 95-99% recall) for massive speed gains.

---

## The Query Flow

The following diagram shows how a search query flows through a vector database:

```mermaid
flowchart LR
    A["User Query<br>(text)"] --> B["Embedding Model<br>(text to vector)"]
    B --> C["Vector Database<br>(ANN search)"]
    C --> D["Top-K Candidates<br>(by vector similarity)"]
    D --> E["Metadata Filter<br>(optional post-filter)"]
    E --> F["Final Results<br>(ranked documents)"]
```

The vector database receives a query vector and returns the K most similar stored vectors, optionally filtering by metadata (e.g., only return documents from a specific category).

---

## Popular Vector Database Options

| Database | Type | Best For | Hosting | Pricing Model |
|----------|------|----------|---------|---------------|
| **Pinecone** | Managed cloud | Serverless, easy start | Fully managed | Per-query + storage |
| **Qdrant** | Open-source / Cloud | Self-hosted or managed | Both | Free (self) or usage |
| **Weaviate** | Open-source / Cloud | Hybrid search, modules | Both | Free (self) or usage |
| **Milvus** | Open-source | Large-scale, GPU support | Self-hosted | Free |
| **pgvector** | PostgreSQL extension | Existing Postgres stack | Self-hosted | Free |
| **ChromaDB** | Open-source | Prototyping, local dev | Local / embedded | Free |
| **FAISS** | Library (not a DB) | Research, batch processing | In-process | Free |

**Decision framework:**
- **Prototype / <100K vectors:** ChromaDB or pgvector
- **Production / 1-50M vectors:** Pinecone, Qdrant, or Weaviate
- **Large-scale / >100M vectors:** Milvus or Qdrant self-hosted
- **Already using Postgres:** pgvector (simplest operational story)

---

## Index Types

The index determines how vectors are organized for fast search:

### HNSW (Hierarchical Navigable Small World)

The most popular index type. Builds a multi-layer graph where each node connects to its nearest neighbors.

```python
# Conceptually: a graph where similar vectors are connected
# Search: start at a random node, greedily hop to closer neighbors
# Like "six degrees of separation" but for vectors

# Key parameters:
# M = 16: each node connects to 16 neighbors (higher = more accurate, more memory)
# ef_construction = 200: how many candidates to consider when building (higher = better graph)
# ef_search = 50: how many candidates to consider when querying (higher = more accurate, slower)
```

**Trade-offs:** High recall (95-99%), fast queries, but uses ~4x the memory of raw vectors for the graph structure.

### IVF (Inverted File Index)

Clusters vectors into buckets. At query time, only searches the nearest buckets.

```python
# Conceptually: K-means clustering of all vectors into N buckets
# Search: find the nearest 5 buckets, then brute-force within those buckets
# nprobe = 5: how many buckets to search (higher = more accurate, slower)
```

**Trade-offs:** Lower memory than HNSW, but slightly lower recall. Good for disk-based storage.

### Flat (Brute-Force)

No index — compares against every vector. Guarantees 100% recall.

```python
# Use when: dataset is small (<50K vectors) or you need perfect accuracy
# Advantage: zero index build time, guaranteed correct results
# Disadvantage: O(n) query time
```

---

## Basic Operations

### Using Pinecone (Managed)

```python
from pinecone import Pinecone, ServerlessSpec

# Initialize
pc = Pinecone(api_key="your-key")

# Create index
pc.create_index(
    name="documents",
    dimension=1536,
    metric="cosine",
    spec=ServerlessSpec(cloud="aws", region="us-east-1")
)

index = pc.Index("documents")

# Upsert vectors with metadata
index.upsert(vectors=[
    {"id": "doc-1", "values": [0.1, 0.2, ...], "metadata": {"source": "wiki", "topic": "spark"}},
    {"id": "doc-2", "values": [0.3, 0.1, ...], "metadata": {"source": "docs", "topic": "kafka"}},
])

# Query: find 5 most similar vectors
results = index.query(
    vector=[0.15, 0.22, ...],  # Query embedding
    top_k=5,
    include_metadata=True,
    filter={"topic": {"$eq": "spark"}}  # Only return Spark-related docs
)

for match in results.matches:
    print(f"ID: {match.id}, Score: {match.score:.3f}, Topic: {match.metadata['topic']}")
```

### Using pgvector (PostgreSQL Extension)

```sql
-- Enable extension
CREATE EXTENSION vector;

-- Create table with vector column
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    content TEXT,
    source VARCHAR(50),
    embedding vector(1536)  -- 1536-dimensional vector
);

-- Create HNSW index
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);

-- Insert
INSERT INTO documents (content, source, embedding)
VALUES ('Apache Spark guide', 'docs', '[0.1, 0.2, ...]');

-- Query: find 5 nearest neighbors
SELECT id, content, source,
       1 - (embedding <=> '[0.15, 0.22, ...]'::vector) AS similarity
FROM documents
WHERE source = 'docs'  -- Metadata filter (uses standard SQL WHERE)
ORDER BY embedding <=> '[0.15, 0.22, ...]'::vector  -- Cosine distance
LIMIT 5;
```

### Using ChromaDB (Local/Prototype)

```python
import chromadb

client = chromadb.Client()

# Create collection
collection = client.create_collection(name="documents", metadata={"hnsw:space": "cosine"})

# Add documents (auto-embeds if you configure an embedding function)
collection.add(
    ids=["doc-1", "doc-2"],
    embeddings=[[0.1, 0.2, ...], [0.3, 0.1, ...]],
    metadatas=[{"source": "wiki"}, {"source": "docs"}],
    documents=["Spark partitioning guide", "Kafka architecture overview"]
)

# Query
results = collection.query(
    query_embeddings=[[0.15, 0.22, ...]],
    n_results=5,
    where={"source": "docs"}  # Metadata filter
)
```

---

## Metadata Filtering

Most vector databases support filtering results by metadata fields:

```python
# Pinecone filter syntax
filter = {
    "topic": {"$eq": "spark"},
    "date": {"$gte": "2024-01-01"},
    "source": {"$in": ["docs", "wiki"]},
}

# Qdrant filter syntax
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

filter = Filter(
    must=[
        FieldCondition(key="topic", match=MatchValue(value="spark")),
        FieldCondition(key="date", range=Range(gte="2024-01-01")),
    ]
)
```

**Important:** Filtering can happen **pre-search** (filter first, then ANN on subset) or **post-search** (ANN first, then filter results). Pre-filter is more accurate but slower for high-selectivity filters.

---

## Namespaces and Collections

Organize vectors into logical groups:

```python
# Pinecone: namespaces within an index
index.upsert(vectors=[...], namespace="engineering-docs")
index.upsert(vectors=[...], namespace="sales-docs")
results = index.query(vector=q, namespace="engineering-docs", top_k=5)

# Qdrant: separate collections
client.create_collection("engineering-docs", vectors_config=...)
client.create_collection("sales-docs", vectors_config=...)

# pgvector: use standard tables/schemas
# CREATE TABLE engineering_docs (id serial, embedding vector(1536));
# CREATE TABLE sales_docs (id serial, embedding vector(1536));
```

**When to separate:** Different embedding models (dimensions differ), different access patterns, or multi-tenant isolation requirements.

---

## Interview Tips

> **Tip 1:** "What vector database would you choose?" — Match to constraints: managed (Pinecone) for zero-ops, pgvector if already on Postgres, Qdrant/Milvus for large-scale self-hosted. Always mention the trade-off between operational simplicity and cost control.

> **Tip 2:** "HNSW vs IVF?" — HNSW gives better recall and faster queries but uses more memory (graph structure). IVF is more memory-efficient and works well with disk-based storage. For most RAG use cases with <50M vectors, HNSW is the default choice.

> **Tip 3:** "How do you handle updates in a vector database?" — Upsert (insert or update by ID). For deletions, most vector DBs support delete-by-ID or delete-by-metadata-filter. Bulk updates: batch upsert in chunks of 100-1000 vectors per request.
