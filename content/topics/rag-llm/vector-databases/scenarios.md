---
title: "Vector Databases - Scenario Questions"
topic: rag-llm
subtopic: vector-databases
content_type: scenario_question
tags: [rag, llm, vector-database, interview, scenarios]
---

# Scenario Questions — Vector Databases

<article data-difficulty="junior">

## 🟢 Junior: Choosing a Vector Database

**Scenario:** Your startup (5 engineers) is building a customer support chatbot. You have 20K help articles to search. The team already uses PostgreSQL for the main app. Which vector database do you recommend?

<details>
<summary>💡 Hint</summary>
Consider operational overhead, existing infrastructure, and dataset size. 20K vectors is tiny.
</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: pgvector (PostgreSQL extension)**

```sql
-- Enable the extension (one command)
CREATE EXTENSION vector;

-- Add vector column to existing articles table
ALTER TABLE help_articles ADD COLUMN embedding vector(1536);

-- Create HNSW index
CREATE INDEX ON help_articles USING hnsw (embedding vector_cosine_ops);

-- Query: find 5 most similar articles
SELECT id, title, 1 - (embedding <=> $1::vector) AS similarity
FROM help_articles
ORDER BY embedding <=> $1::vector
LIMIT 5;
```

**Key Points:**
- 20K vectors is tiny — even brute-force takes <5ms, HNSW makes it <1ms
- No new infrastructure to deploy, monitor, or secure
- Team already knows PostgreSQL — zero learning curve
- Metadata filtering uses standard SQL WHERE clauses
- Backups, replication, and monitoring already in place
- Upgrade path: if you outgrow pgvector (>5M vectors), migrate to Qdrant later

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Basic Similarity Search

**Scenario:** You've stored 10K document embeddings in Pinecone. A user searches for "how to handle null values in Spark DataFrames". Write the code to embed the query and retrieve the top 5 most relevant documents.

<details>
<summary>💡 Hint</summary>
Embed the query with the same model used for documents, then call the vector DB's query/search method with top_k=5.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
from pinecone import Pinecone

openai_client = OpenAI()
pc = Pinecone(api_key="your-pinecone-key")
index = pc.Index("knowledge-base")

def search_documents(query: str, top_k: int = 5) -> list[dict]:
    """Embed query and search vector database."""
    # Step 1: Embed the query (MUST use same model as documents)
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=[query]
    )
    query_vector = response.data[0].embedding
    
    # Step 2: Search vector database
    results = index.query(
        vector=query_vector,
        top_k=top_k,
        include_metadata=True
    )
    
    # Step 3: Format results
    return [
        {
            "id": match.id,
            "score": match.score,
            "title": match.metadata.get("title", ""),
            "content": match.metadata.get("content", ""),
        }
        for match in results.matches
    ]

results = search_documents("how to handle null values in Spark DataFrames")
for r in results:
    print(f"[{r['score']:.3f}] {r['title']}")
```

**Key Points:**
- Query MUST be embedded with the same model used for documents (mixing models = garbage results)
- Cosine similarity scores: >0.8 very relevant, 0.5-0.8 somewhat relevant, <0.5 likely irrelevant
- `include_metadata=True` returns stored metadata alongside vectors
- Latency: ~150ms (embedding) + ~20ms (search) = ~170ms total

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Metadata Filtering

**Scenario:** Your RAG system indexes documents from 5 different teams (engineering, sales, marketing, legal, finance). A user from the engineering team should only see engineering documents in search results. How do you implement this?

<details>
<summary>💡 Hint</summary>
Store team as metadata with each vector, then apply a metadata filter at query time to restrict results.
</details>

<details>
<summary>✅ Solution</summary>

```python
# When indexing: include team in metadata
index.upsert(vectors=[
    {
        "id": "doc-123",
        "values": embedding,
        "metadata": {
            "team": "engineering",
            "title": "Spark Optimization Guide",
            "created_at": "2024-01-15",
        }
    }
])

# When querying: filter by team
def search_for_team(query: str, team: str, top_k: int = 5):
    query_vector = embed(query)
    results = index.query(
        vector=query_vector,
        top_k=top_k,
        include_metadata=True,
        filter={"team": {"$eq": team}}  # Only return docs from this team
    )
    return results

# Engineering user sees only engineering docs
results = search_for_team("deployment best practices", team="engineering")

# Cross-team search (admin/leadership)
results = index.query(
    vector=query_vector,
    top_k=10,
    filter={"team": {"$in": ["engineering", "sales"]}}  # Multiple teams
)
```

**Key Points:**
- Metadata filters are applied during ANN search, not after (efficient)
- Common filter fields: team, department, document_type, date_range, access_level
- This is the simplest multi-tenancy pattern — one index with metadata-based access control
- For strict isolation (compliance), use separate namespaces or collections per team

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Understanding Similarity Thresholds

**Scenario:** Your search returns results with similarity scores of 0.92, 0.78, 0.45, 0.31, and 0.12. The user complains that the last two results are irrelevant. How do you implement a relevance threshold?

<details>
<summary>💡 Hint</summary>
Apply a minimum score cutoff. The threshold depends on your model and domain — calibrate it by examining results at various score levels.
</details>

<details>
<summary>✅ Solution</summary>

```python
def search_with_threshold(query: str, min_score: float = 0.5, top_k: int = 10) -> list[dict]:
    """Search with a minimum relevance threshold."""
    query_vector = embed(query)
    
    # Retrieve more candidates than needed, then filter
    results = index.query(vector=query_vector, top_k=top_k, include_metadata=True)
    
    # Apply threshold
    relevant_results = [
        match for match in results.matches
        if match.score >= min_score
    ]
    
    # Handle no results gracefully
    if not relevant_results:
        return [{
            "message": "No sufficiently relevant documents found.",
            "suggestion": "Try rephrasing your question or broadening the search.",
            "best_score": results.matches[0].score if results.matches else 0
        }]
    
    return relevant_results

# Calibration: examine results at different thresholds
# Score > 0.85: almost certainly relevant (high precision)
# Score 0.6-0.85: probably relevant (balanced)
# Score 0.4-0.6: maybe relevant (high recall, lower precision)
# Score < 0.4: likely irrelevant for most models
```

**Key Points:**
- Threshold varies by model: OpenAI embeddings typically need >0.5, MiniLM needs >0.4
- Calibrate on YOUR data: label 100 query-result pairs as relevant/irrelevant, find the score that separates them
- Better approach: use a cross-encoder re-ranker instead of hard threshold
- Always handle "no results" gracefully — don't return empty responses to users

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Creating an HNSW Index

**Scenario:** You loaded 500K vectors into pgvector but queries take 800ms. Before adding an index, queries were doing brute-force sequential scan. Create an appropriate HNSW index.

<details>
<summary>💡 Hint</summary>
pgvector supports HNSW indexes with configurable M and ef_construction parameters. After creating the index, queries should drop from 800ms to <10ms.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Before: sequential scan (brute-force), 800ms for 500K vectors
EXPLAIN ANALYZE
SELECT id, 1 - (embedding <=> query_vec) AS similarity
FROM documents ORDER BY embedding <=> query_vec LIMIT 10;
-- "Seq Scan on documents"  → 800ms

-- Create HNSW index (takes 5-10 minutes for 500K vectors)
CREATE INDEX idx_documents_embedding ON documents
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 200);

-- After: index scan, <10ms
SET hnsw.ef_search = 64;  -- Controls recall vs speed at query time

EXPLAIN ANALYZE
SELECT id, 1 - (embedding <=> query_vec) AS similarity
FROM documents ORDER BY embedding <=> query_vec LIMIT 10;
-- "Index Scan using idx_documents_embedding"  → 5ms
```

```python
# Python equivalent with psycopg2
import psycopg2

conn = psycopg2.connect("postgresql://localhost/mydb")
cur = conn.cursor()

# Create index (run once)
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_docs_embedding
    ON documents USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200)
""")
conn.commit()

# Query with index (fast)
cur.execute("SET hnsw.ef_search = 64")
cur.execute("""
    SELECT id, title, 1 - (embedding <=> %s::vector) AS similarity
    FROM documents
    ORDER BY embedding <=> %s::vector
    LIMIT 10
""", (query_embedding, query_embedding))

results = cur.fetchall()
```

**Key Points:**
- Index build is one-time cost: ~10 min for 500K vectors, ~1 hour for 5M
- `m=16`: each node connects to 16 neighbors (good default)
- `ef_construction=200`: build quality (higher = better graph, slower build)
- `ef_search=64`: query-time parameter (higher = better recall, slower query)
- Speedup: 800ms → 5ms (160x faster) with 97%+ recall

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Scaling to 10M Vectors

**Scenario:** Your pgvector instance with 10M vectors (1536 dims) is consuming 95GB RAM and queries take 50ms (up from 5ms at 500K). The server has 128GB RAM. How do you scale without migrating to a different vector database?

<details>
<summary>💡 Hint</summary>
Options: reduce dimensionality, use quantization (pgvector doesn't natively support it but you can store smaller vectors), partition the table, or add read replicas.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Problem: 10M × 1536 dims × 4 bytes = 60GB raw + HNSW overhead = ~95GB

-- Solution 1: Reduce dimensions with Matryoshka (if model supports it)
-- Re-embed with 512 dims instead of 1536
-- Memory: 10M × 512 × 4 = 20GB + HNSW = ~35GB (63% reduction)
ALTER TABLE documents ADD COLUMN embedding_compact vector(512);
-- Re-embed all documents with truncated vectors

-- Solution 2: Table partitioning by date/category
CREATE TABLE documents_2024 PARTITION OF documents
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
-- Each partition gets its own smaller HNSW index
-- Queries targeting one partition are faster

-- Solution 3: Partial indexes (only index recent/active docs)
CREATE INDEX idx_recent_docs ON documents
USING hnsw (embedding vector_cosine_ops)
WHERE created_at > '2023-01-01';
-- Old docs excluded from index → smaller, faster

-- Solution 4: Read replica for search queries
-- Main DB handles writes, replica handles all search traffic
-- Doubles effective capacity for read-heavy workloads
```

```python
# Solution 5: Half-precision vectors (requires pgvector 0.7+)
# halfvec type uses 2 bytes per dimension instead of 4
# Memory: 10M × 1536 × 2 = 30GB + HNSW = ~50GB

# Create table with halfvec
cur.execute("""
    CREATE TABLE documents_optimized (
        id BIGINT PRIMARY KEY,
        title TEXT,
        embedding halfvec(1536)  -- Half precision: 2 bytes per dim
    )
""")
cur.execute("""
    CREATE INDEX ON documents_optimized
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200)
""")
```

**Key Points:**
- halfvec (pgvector 0.7+): 50% memory savings with negligible quality loss
- Dimensionality reduction (1536→512): 67% savings but requires re-embedding
- Table partitioning: query only hits relevant partition
- Read replicas: scale read throughput without changing data layout
- When to migrate: if you need >50M vectors or <5ms p99, consider Qdrant/Milvus

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implementing Hybrid Search

**Scenario:** Users search for "error code E4012 in Spark executor" but dense vector search returns general Spark articles without the specific error code. How do you combine keyword search (finds exact "E4012") with semantic search (understands "Spark executor errors")?

<details>
<summary>💡 Hint</summary>
Use hybrid search: run both BM25 (keyword) and dense (semantic) searches, then combine results using Reciprocal Rank Fusion (RRF).
</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np
from rank_bm25 import BM25Okapi
from qdrant_client import QdrantClient

class HybridSearchEngine:
    def __init__(self, client: QdrantClient, collection: str, documents: list[dict]):
        self.client = client
        self.collection = collection
        
        # Build BM25 index for keyword search
        tokenized = [doc["text"].lower().split() for doc in documents]
        self.bm25 = BM25Okapi(tokenized)
        self.doc_ids = [doc["id"] for doc in documents]
    
    def search(self, query: str, query_vector: list[float], top_k: int = 10, alpha: float = 0.5):
        """Hybrid search combining dense + sparse with RRF."""
        
        # Dense search (semantic)
        dense_results = self.client.search(
            collection_name=self.collection,
            query_vector=query_vector,
            limit=50  # Over-fetch for fusion
        )
        dense_ranked = [(r.id, r.score) for r in dense_results]
        
        # Sparse search (BM25 keyword)
        tokenized_query = query.lower().split()
        bm25_scores = self.bm25.get_scores(tokenized_query)
        top_bm25_idx = np.argsort(bm25_scores)[::-1][:50]
        sparse_ranked = [(self.doc_ids[i], bm25_scores[i]) for i in top_bm25_idx if bm25_scores[i] > 0]
        
        # Reciprocal Rank Fusion
        combined = self._rrf(
            [doc_id for doc_id, _ in dense_ranked],
            [doc_id for doc_id, _ in sparse_ranked],
            alpha=alpha
        )
        
        return combined[:top_k]
    
    def _rrf(self, dense_ids: list, sparse_ids: list, k: int = 60, alpha: float = 0.5):
        """Combine rankings using weighted RRF."""
        scores = {}
        
        for rank, doc_id in enumerate(dense_ids, 1):
            scores[doc_id] = scores.get(doc_id, 0) + alpha * (1.0 / (k + rank))
        
        for rank, doc_id in enumerate(sparse_ids, 1):
            scores[doc_id] = scores.get(doc_id, 0) + (1 - alpha) * (1.0 / (k + rank))
        
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

# Usage:
engine = HybridSearchEngine(client, "docs", all_documents)
results = engine.search(
    query="error code E4012 in Spark executor",
    query_vector=embed("error code E4012 in Spark executor"),
    alpha=0.6  # 60% weight on semantic, 40% on keyword
)
# Now "E4012" matches via BM25, and "Spark executor errors" matches via dense
```

**Key Points:**
- Pure dense search misses exact terms (error codes, product IDs, version numbers)
- Pure keyword search misses semantic understanding (paraphrases, related concepts)
- Hybrid with RRF gets the best of both — boosts docs that appear in both ranked lists
- alpha parameter: tune based on your query types (more keyword-heavy → lower alpha)
- Alternative: store sparse vectors natively in Qdrant/Weaviate for single-query hybrid

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Tenancy Design

**Scenario:** Your SaaS platform serves 200 customers, each with 10K-500K documents. You need tenant isolation (Customer A must never see Customer B's data), different retention policies, and the ability to delete a tenant's data completely. Design the multi-tenancy approach.

<details>
<summary>💡 Hint</summary>
Consider: collection-per-tenant (strong isolation, high operational cost), namespace-per-tenant (moderate isolation), or metadata filtering (simple, weakest isolation). Balance isolation strength vs operational complexity.
</details>

<details>
<summary>✅ Solution</summary>

```python
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Filter, FieldCondition, MatchValue

class MultiTenantVectorDB:
    """Namespace-per-tenant pattern with Qdrant."""
    
    def __init__(self, client: QdrantClient):
        self.client = client
        self.collection = "saas_documents"  # Single collection
    
    def provision_tenant(self, tenant_id: str):
        """Set up a new tenant — just a metadata convention, no infra change."""
        # No action needed for namespace pattern — tenant data is isolated by payload filter
        # For audit: record tenant provisioning
        pass
    
    def index_document(self, tenant_id: str, doc_id: str, embedding: list[float], metadata: dict):
        """Index a document with tenant isolation."""
        self.client.upsert(
            collection_name=self.collection,
            points=[{
                "id": f"{tenant_id}_{doc_id}",  # Tenant-prefixed ID
                "vector": embedding,
                "payload": {
                    "tenant_id": tenant_id,  # REQUIRED for isolation
                    **metadata,
                }
            }]
        )
    
    def search(self, tenant_id: str, query_vector: list[float], top_k: int = 10):
        """Search within a single tenant's documents only."""
        return self.client.search(
            collection_name=self.collection,
            query_vector=query_vector,
            limit=top_k,
            query_filter=Filter(
                must=[FieldCondition(key="tenant_id", match=MatchValue(value=tenant_id))]
            )
        )
    
    def delete_tenant(self, tenant_id: str):
        """Complete tenant data deletion (GDPR compliance)."""
        self.client.delete(
            collection_name=self.collection,
            points_selector=Filter(
                must=[FieldCondition(key="tenant_id", match=MatchValue(value=tenant_id))]
            )
        )
    
    def get_tenant_stats(self, tenant_id: str) -> dict:
        """Get vector count and usage for a tenant."""
        result = self.client.count(
            collection_name=self.collection,
            count_filter=Filter(
                must=[FieldCondition(key="tenant_id", match=MatchValue(value=tenant_id))]
            )
        )
        return {"tenant_id": tenant_id, "vector_count": result.count}

# For 200 tenants with 10K-500K docs each:
# Total: ~20-100M vectors in one collection
# Isolation: filter-based (fast, but requires trusting the filter logic)
# Deletion: filter-based delete (complete, auditable)
```

**Key Points:**
- Metadata filtering: simplest, scales to 1000+ tenants, but isolation is app-level (bug could leak data)
- For compliance-heavy industries (healthcare, finance): collection-per-tenant despite higher cost
- Create a payload index on `tenant_id` for fast filtering
- Tenant deletion via filter: complete but may leave tombstones until compaction
- Monitor per-tenant search latency — large tenants may need their own shard

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Index Tuning for Recall vs Latency

**Scenario:** Your production vector search has recall@10 of 92% and p99 latency of 15ms. The product team wants 98% recall (users miss relevant docs). The SRE team says latency must stay under 20ms. Can you achieve both?

<details>
<summary>💡 Hint</summary>
Increase ef_search (query-time parameter) to improve recall. Each increment costs latency. Plot the recall-latency curve to find if 98% recall is achievable under 20ms.
</details>

<details>
<summary>✅ Solution</summary>

```python
import time
import numpy as np

def benchmark_recall_latency(client, collection, test_queries, ground_truth, ef_values):
    """Benchmark recall@10 vs latency at different ef_search values."""
    results = []
    
    for ef in ef_values:
        latencies = []
        recalls = []
        
        for query_vec, true_ids in zip(test_queries, ground_truth):
            start = time.time()
            hits = client.search(
                collection_name=collection,
                query_vector=query_vec,
                limit=10,
                search_params={"hnsw_ef": ef}
            )
            latencies.append((time.time() - start) * 1000)
            
            retrieved_ids = set(h.id for h in hits)
            recall = len(retrieved_ids & set(true_ids[:10])) / 10
            recalls.append(recall)
        
        results.append({
            "ef_search": ef,
            "recall@10": np.mean(recalls),
            "p50_latency_ms": np.percentile(latencies, 50),
            "p99_latency_ms": np.percentile(latencies, 99),
        })
        print(f"ef={ef}: recall={np.mean(recalls):.3f}, p99={np.percentile(latencies, 99):.1f}ms")
    
    return results

# Run benchmark
ef_values = [32, 64, 96, 128, 192, 256]
benchmarks = benchmark_recall_latency(client, "docs", test_queries, ground_truth, ef_values)

# Typical results for 10M vectors:
# ef=32:  recall=0.89, p99=5ms
# ef=64:  recall=0.94, p99=8ms    ← current setting
# ef=128: recall=0.97, p99=14ms
# ef=192: recall=0.985, p99=18ms  ← SWEET SPOT (98% recall, under 20ms)
# ef=256: recall=0.99, p99=25ms   ← exceeds latency budget

# Solution: set ef_search = 192
client.update_collection(
    collection_name="docs",
    params={"hnsw_ef": 192}  # Or set per-query for different use cases
)
```

**Key Points:**
- ef_search is the primary recall vs latency knob — test systematically
- Increasing M (graph connectivity) also helps recall but costs memory
- At 10M vectors: ef=192 typically gives 98% recall at ~18ms
- If p99 still too high: add read replicas to reduce per-node load
- Consider two-tier: fast search (low ef) for autocomplete, precise (high ef) for final answers
- Always benchmark on YOUR data — generic guidelines are starting points only

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Billion-Scale Architecture

**Scenario:** Design a vector search system for a social media platform: 2 billion posts, 768-dim embeddings, 50K queries/second, p99 < 50ms, 99.99% availability. Budget: $50K/month on AWS.

<details>
<summary>💡 Hint</summary>
At 2B vectors you need: aggressive quantization (binary or int4), distributed sharding, tiered storage (hot/warm), and caching for popular queries. Calculate memory requirements first.
</details>

<details>
<summary>✅ Solution</summary>

```python
# SIZING CALCULATION:
# 2B vectors × 768 dims × 4 bytes (float32) = 6.1 TB (raw) — impossible in RAM
# With int8: 2B × 768 × 1 = 1.5 TB — still very expensive
# With binary: 2B × 768 / 8 = 192 GB — feasible!

# ARCHITECTURE: Two-phase search with binary + full precision

# Phase 1: Binary HNSW for fast candidate retrieval
# 2B vectors × 96 bytes (binary packed) = 192 GB
# Plus HNSW graph overhead: ~400 GB total
# Distributed across 8 shards, 2 replicas each = 16 nodes
# ~50 GB per node (fits in r6g.2xlarge with 64 GB RAM)

# Phase 2: Full-precision re-scoring of top candidates
# Store float16 vectors on SSD (2B × 768 × 2 = 3 TB total)
# Load only top-200 candidates per query for rescoring

ARCHITECTURE = {
    "search_tier": {
        "purpose": "Binary HNSW for fast candidate generation",
        "nodes": 16,  # 8 shards × 2 replicas
        "instance": "r6g.2xlarge",  # 64 GB RAM, 8 vCPU
        "cost": 16 * 187,  # $2,992/mo
    },
    "rescore_tier": {
        "purpose": "Full-precision rescore of top-200 candidates",
        "nodes": 4,  # SSD-optimized
        "instance": "i3.2xlarge",  # 64 GB RAM + 1.9 TB NVMe
        "cost": 4 * 468,  # $1,872/mo
    },
    "cache_tier": {
        "purpose": "Query result cache (popular queries)",
        "nodes": 3,
        "instance": "r6g.xlarge",  # ElastiCache Redis cluster
        "cost": 3 * 146,  # $438/mo
    },
    "embedding_tier": {
        "purpose": "Query embedding (local model)",
        "nodes": 4,
        "instance": "g5.xlarge",  # A10G GPU
        "cost": 4 * 744,  # $2,976/mo
    },
    "load_balancer": {"cost": 200},
    "monitoring": {"cost": 300},
    "total_monthly": "$8,778"
}

# QUERY FLOW:
# 1. Check Redis cache → if hit, return (expect 30% hit rate for social media)
# 2. Embed query locally (GPU, 5ms)
# 3. Binary search: scatter to 8 shards, each returns top-200 (10ms)
# 4. Merge shard results: top-200 globally (1ms)
# 5. Rescore top-200 with float16 vectors from SSD (15ms)
# 6. Return top-10 to user
# Total: 5 + 10 + 1 + 15 = 31ms (well within 50ms p99)

# QPS CAPACITY:
# 50K QPS with 30% cache hit = 35K QPS hitting search nodes
# 16 search nodes / 8 shards = 2 replicas per shard
# 35K / 8 shards = 4,375 QPS per shard
# 4,375 / 2 replicas = 2,188 QPS per node (feasible for binary search)
```

**Key Points:**
- Binary quantization makes 2B vectors feasible (192 GB vs 6.1 TB)
- Two-phase (binary search → full rescore) recovers quality lost to quantization
- Sharding by hash distributes load evenly; each shard searched in parallel
- Redis cache handles repeated/trending queries (social media has high repeat rate)
- Total: $8.8K/mo (well under $50K budget — room for growth)
- 99.99% availability: 2 replicas per shard, multi-AZ, automated failover

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Real-Time Updates with Consistency

**Scenario:** Your e-commerce search must reflect inventory changes within 5 seconds (item goes out of stock → immediately removed from search results). Your vector DB has eventual consistency with ~10 second propagation delay. How do you achieve 5-second freshness?

<details>
<summary>💡 Hint</summary>
You can't speed up the vector DB's consistency. Instead, implement a "hot filter" that overlays real-time state on top of ANN results, filtering out stale items post-retrieval.
</details>

<details>
<summary>✅ Solution</summary>

```python
import redis
import time
from typing import Optional

class RealTimeVectorSearch:
    """Vector search with real-time state overlay for freshness guarantee."""
    
    def __init__(self, vector_client, redis_client: redis.Redis):
        self.vector_client = vector_client
        self.redis = redis_client  # Stores real-time state changes
    
    def update_item_state(self, item_id: str, in_stock: bool):
        """Called by inventory service when stock changes (via Kafka/SNS)."""
        if not in_stock:
            # Mark as unavailable in Redis (expires in 60s — by then vector DB is consistent)
            self.redis.setex(f"unavailable:{item_id}", 60, "1")
        else:
            self.redis.delete(f"unavailable:{item_id}")
    
    def search(self, query_vector: list[float], top_k: int = 10) -> list[dict]:
        """Search with real-time availability filtering."""
        # Over-fetch from vector DB (some results may be filtered out)
        candidates = self.vector_client.search(
            collection_name="products",
            query_vector=query_vector,
            limit=top_k * 3  # 3x over-fetch to account for filtered items
        )
        
        # Check real-time state for each candidate
        pipeline = self.redis.pipeline()
        for hit in candidates:
            pipeline.exists(f"unavailable:{hit.id}")
        availability = pipeline.execute()
        
        # Filter out unavailable items
        available_results = [
            hit for hit, is_unavailable in zip(candidates, availability)
            if not is_unavailable
        ]
        
        return available_results[:top_k]

# Event flow:
# 1. Item goes out of stock → inventory service publishes event
# 2. Consumer writes to Redis: "unavailable:{item_id}" (< 1 second)
# 3. Consumer also updates vector DB metadata (10 second propagation)
# 4. Searches check Redis overlay → item excluded immediately
# 5. After 60s: Redis key expires, vector DB is now consistent (filter removed)
```

**Key Points:**
- Redis overlay provides <1 second freshness (network hop only)
- Over-fetch compensates for filtered results (3x factor handles up to 67% unavailability)
- Redis TTL auto-expires once vector DB catches up (self-healing)
- Pattern works for any real-time state: price changes, content moderation, user blocks
- Cost: one Redis GET per result candidate (~0.1ms total for pipeline of 30)
- Alternative: if vector DB supports "exclude by ID list", pass unavailable IDs as a filter

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cost Optimization at Scale

**Scenario:** Your vector DB costs $15K/month for 30M vectors (1536 dims, float32, Pinecone). The CFO says cut to $5K without degrading search quality significantly. What's your optimization plan?

<details>
<summary>💡 Hint</summary>
Levers: dimensionality reduction, quantization, tiered storage, removing stale vectors, and potentially migrating to self-hosted.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Current state: 30M × 1536 dims × float32 = 180 GB on Pinecone
# Pinecone serverless: ~$0.50/GB/month storage + query costs
# Total: $15K/month

# OPTIMIZATION PLAN (implemented in priority order):

# 1. Matryoshka truncation: 1536 → 512 dims (if model supports it)
# Memory: 30M × 512 × 4 = 60 GB (67% reduction)
# Quality: <3% recall@10 degradation (verify with eval set)
# Savings: ~$6K/mo → $9K/mo remaining

# 2. Remove stale/unused vectors
# Analysis: 40% of vectors haven't been retrieved in 90 days
# Action: archive to S3, remove from live index
# After: 18M vectors × 512 dims = 36 GB
# Savings: ~$3K/mo → $6K/mo remaining

# 3. Migrate to self-hosted Qdrant with int8 quantization
# 18M × 512 × 1 byte (int8) = 9 GB + HNSW overhead = ~20 GB
# Infrastructure: 2× r6g.xlarge (32 GB) with replication = $292/mo
# Embedding GPU (for queries): 1× g5.xlarge = $744/mo
# Monitoring + ops: ~$200/mo
# Total: ~$1,236/mo for the vector search component

# BUT account for:
# - Engineering time for migration: ~2 weeks (one-time)
# - Ongoing ops burden: monitoring, upgrades, capacity planning
# - Let's budget $2K/mo total including ops overhead

# FINAL COST: ~$3K/mo (80% reduction, within $5K budget)

# IMPLEMENTATION TIMELINE:
timeline = {
    "Week 1": "Evaluate Matryoshka quality, run A/B test",
    "Week 2": "Truncate embeddings, validate recall",
    "Week 3": "Identify and archive stale vectors",
    "Week 4-5": "Set up Qdrant cluster, migrate data",
    "Week 6": "Dual-read validation, switch traffic",
    "Week 7": "Monitor, decommission Pinecone",
}
```

**Key Points:**
- Start with easy wins: dimension reduction and pruning stale data (no migration needed)
- Self-hosted saves 80%+ at this scale but adds operational burden
- Quantization (int8) is nearly lossless and halves memory again
- Always validate quality at each step: run eval before committing
- Keep Pinecone running during migration as fallback (1-2 weeks overlap cost is worthwhile)
- For teams without infra capacity: Qdrant Cloud or Weaviate Cloud as middle ground

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Zero-Downtime Migration

**Scenario:** You're migrating from Pinecone to Qdrant (self-hosted). The system serves 5K queries/minute to production users. Design a migration plan with zero downtime and zero data loss.

<details>
<summary>💡 Hint</summary>
Use dual-write + progressive traffic shifting. Never do a "big bang" cutover for a production system serving live traffic.
</details>

<details>
<summary>✅ Solution</summary>

```python
import random
from enum import Enum

class SearchBackend(Enum):
    PINECONE = "pinecone"
    QDRANT = "qdrant"
    BOTH = "both"  # Shadow mode: query both, return Pinecone results

class MigrationOrchestrator:
    """Manages progressive migration from Pinecone to Qdrant."""
    
    def __init__(self, pinecone_index, qdrant_client, config):
        self.pinecone = pinecone_index
        self.qdrant = qdrant_client
        self.config = config  # Feature flag service
    
    # Phase 1: Dual-write (both systems get all writes)
    def upsert(self, vectors: list[dict]):
        """Write to both systems simultaneously."""
        # Always write to Pinecone (still primary)
        self.pinecone.upsert(vectors=vectors)
        
        # Also write to Qdrant (building up the new index)
        points = [self._convert_to_qdrant(v) for v in vectors]
        self.qdrant.upsert(collection_name="docs", points=points, wait=True)
    
    # Phase 2: Shadow reads (query both, compare, serve Pinecone)
    def search_shadow(self, query_vector, top_k=10):
        """Query both systems, log differences, return Pinecone results."""
        pinecone_results = self.pinecone.query(vector=query_vector, top_k=top_k)
        qdrant_results = self.qdrant.search(
            collection_name="docs", query_vector=query_vector, limit=top_k
        )
        
        # Compare results (log for validation)
        self._log_comparison(pinecone_results, qdrant_results)
        
        return pinecone_results  # Still serving from Pinecone
    
    # Phase 3: Progressive traffic shift
    def search(self, query_vector, top_k=10):
        """Route traffic based on feature flag percentage."""
        qdrant_pct = self.config.get("qdrant_traffic_pct", 0)  # 0, 10, 25, 50, 100
        
        if random.random() * 100 < qdrant_pct:
            return self._search_qdrant(query_vector, top_k)
        else:
            return self._search_pinecone(query_vector, top_k)

# MIGRATION RUNBOOK:
# Day 1-3:   Bulk migrate existing 30M vectors to Qdrant (background job)
# Day 4:     Enable dual-write (new writes go to both)
# Day 5-7:   Shadow mode — query both, compare results, fix discrepancies
# Day 8:     Validation: >98% result overlap confirmed
# Day 9:     10% traffic to Qdrant (monitor latency, errors)
# Day 10:    25% traffic (monitor)
# Day 11:    50% traffic (monitor)
# Day 12:    100% traffic to Qdrant
# Day 13-19: Keep Pinecone running as hot standby
# Day 20:    Decommission Pinecone (stop billing)
```

**Key Points:**
- Dual-write ensures both systems have identical data from day 4 onward
- Shadow mode catches bugs without impacting users (log discrepancies)
- Progressive traffic shift: if Qdrant has issues, instantly roll back to 0%
- >98% result overlap threshold before increasing traffic (accounts for ANN non-determinism)
- Keep old system as hot standby for 1 week after full cutover
- Total migration duration: ~3 weeks for a careful, zero-risk approach

</details>

</article>
