---
title: "Vector Search & Indexing - Real-World Examples"
topic: rag-llm
subtopic: vector-search-indexing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, vector-search, production, hnsw, hybrid-search, reranking]
---

# Vector Search & Indexing — Real-World Production Examples

## Production Pattern: E-Commerce Semantic Search at Scale

A retail company migrated from Elasticsearch keyword search to hybrid vector search for 8M product listings.

**Problem:** Keyword search failed on intent gaps:
- "comfortable office chair for back pain" → returned chairs with "comfort" in description, missed ergonomic chairs
- "gift for 5 year old boy" → returned products tagged "boys 5T" (clothing), not toys

**Solution: HNSW + BM25 hybrid with Qdrant**

```python
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, HnswConfigDiff

client = QdrantClient(host="qdrant-prod", port=6333)

# Create collection with HNSW tuned for high recall
client.create_collection(
    collection_name="products",
    vectors_config=VectorParams(
        size=1536,
        distance=Distance.COSINE,
        hnsw_config=HnswConfigDiff(
            m=16,
            ef_construct=200,
            full_scan_threshold=10_000  # use HNSW for > 10K points
        )
    )
)

# Index 8M products — batch insertion
def index_products(products: list[dict], batch_size: int = 512):
    for i in range(0, len(products), batch_size):
        batch = products[i:i+batch_size]
        embeddings = embed_batch([p["text"] for p in batch])  # OpenAI batch API
        client.upsert(
            collection_name="products",
            points=[
                PointStruct(
                    id=p["id"],
                    vector=emb,
                    payload={
                        "name": p["name"],
                        "category": p["category"],
                        "brand": p["brand"],
                        "price": p["price"],
                        "in_stock": p["in_stock"]
                    }
                )
                for p, emb in zip(batch, embeddings)
            ]
        )
```

**Hybrid search with category filtering:**
```python
def search_products(
    query: str,
    category: str = None,
    max_price: float = None,
    top_k: int = 20
) -> list[dict]:

    query_embedding = embed(query)

    # Build filter
    filters = [FieldCondition(key="in_stock", match=MatchValue(value=True))]
    if category:
        filters.append(FieldCondition(key="category", match=MatchValue(value=category)))
    if max_price:
        filters.append(FieldCondition(key="price", range=Range(lte=max_price)))

    # Stage 1: Dense ANN retrieval
    dense_hits = client.search(
        collection_name="products",
        query_vector=query_embedding,
        query_filter=Filter(must=filters),
        limit=top_k * 3,   # oversample for reranking
        with_payload=True
    )

    # Stage 2: BM25 sparse retrieval on same filtered set
    sparse_hits = bm25_index.search(query, filters=filters, top_k=top_k * 3)

    # Stage 3: RRF fusion
    candidates = rrf_merge(dense_hits, sparse_hits, alpha=0.65)[:top_k]

    # Stage 4: Cross-encoder rerank for top results
    return cross_encoder_rerank(query, candidates, top_k=10)
```

**Results:**
- Search relevance (NDCG@10): 0.61 → 0.79 (+29%)
- Add-to-cart rate from search: +18%
- Query latency p99: 85ms (acceptable for e-commerce)
- Indexing pipeline: 8M products in 4 hours using OpenAI batch API ($45 one-time cost)

---

## Production Pattern: Enterprise Knowledge Base RAG

A 10,000-employee company built an internal knowledge base RAG over 500K documents (HR policies, engineering docs, legal contracts, IT runbooks).

**Challenge: Different document types need different chunking and retrieval strategies**

```python
# Document-type-aware chunking
def chunk_document(doc: dict) -> list[dict]:
    if doc["type"] == "policy":
        # Policies: chunk by section with overlap
        return chunk_by_section(doc["text"], chunk_size=512, overlap=64)

    elif doc["type"] == "runbook":
        # Runbooks: keep procedures atomic (don't split steps)
        return chunk_by_procedure(doc["text"])

    elif doc["type"] == "contract":
        # Contracts: chunk by clause (legal structure matters)
        return chunk_by_clause(doc["text"], max_tokens=256)

    else:
        # Default: fixed-size with overlap
        return chunk_fixed(doc["text"], size=384, overlap=64)
```

**Multi-index architecture for access control:**
```python
# Separate collection per security classification
# Prevents accidental cross-classification retrieval

collections = {
    "public":         # all employees
    "internal":       # employees only (not contractors)
    "confidential":   # managers and above
    "restricted":     # executives and legal only
}

def search_with_access_control(
    query: str,
    user_classification: str,
    top_k: int = 5
) -> list[dict]:
    # Only search collections user has access to
    accessible = [c for c in collections if classification_allows(user_classification, c)]

    all_results = []
    for collection in accessible:
        hits = client.search(collection_name=collection, ...)
        all_results.extend(hits)

    # Merge and re-rank across collections
    return rerank(query, all_results, top_k=top_k)
```

**Embedding model selection result:**
```
Tested on internal QA benchmark (200 question-answer pairs):

Model                    | Recall@5 | Cost/1M tokens | Latency
-------------------------|----------|----------------|--------
OpenAI text-ada-002      | 78%      | $0.10          | 80ms
OpenAI text-embedding-3-small | 83% | $0.02          | 60ms  ← chosen
OpenAI text-embedding-3-large | 87% | $0.13          | 80ms
sentence-transformers/all-mpnet | 71% | $0 (self-hosted) | 30ms

Decision: text-embedding-3-small — 4x cost reduction vs ada-002, better recall
```

---

## Incident: Index Staleness Caused 40% Cache Miss Spike

**What happened:** The vector index was rebuilt weekly (Sunday 2am). A major product catalog update shipped on Thursday — 200K new products. For 3 days, searches returned no results for new products.

**Root cause:** Indexing pipeline was scheduled weekly but product catalog updated continuously.

**Fix: Near-real-time index updates**

```python
# Kafka consumer: listen for product update events → update vector index
from confluent_kafka import Consumer

def consume_product_events():
    consumer = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "vector-indexer"})
    consumer.subscribe(["product.updated", "product.created", "product.deleted"])

    batch = []
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg:
            event = json.loads(msg.value())
            batch.append(event)

        if len(batch) >= 100 or (batch and time.time() - last_flush > 30):
            index_batch(batch)
            batch = []
            last_flush = time.time()

def index_batch(events: list[dict]):
    for event in events:
        if event["type"] in ("created", "updated"):
            embedding = embed(build_product_text(event["product"]))
            client.upsert("products", [PointStruct(id=event["product_id"], vector=embedding, payload=event["product"])])
        elif event["type"] == "deleted":
            client.delete("products", points_selector=PointIdsList(points=[event["product_id"]]))
```

**Outcome:** Index lag reduced from up to 7 days → under 60 seconds. Search result freshness became a non-issue.

---

## Cost Optimization: Tiered Embedding Strategy

**Problem:** Embedding 50M documents at $0.13/1M tokens (text-embedding-3-large) = $6,500 per full re-index.

**Solution: Tiered quality based on document importance**

```python
def select_embedding_model(doc: dict) -> str:
    # Tier 1: High-value docs → best model
    if doc["view_count"] > 10_000 or doc["type"] in ("faq", "product_page"):
        return "text-embedding-3-large"  # $0.13/1M tokens

    # Tier 2: Standard docs → balanced model
    elif doc["type"] in ("blog", "support_article"):
        return "text-embedding-3-small"  # $0.02/1M tokens

    # Tier 3: Archival/low-traffic → self-hosted
    else:
        return "sentence-transformers/all-MiniLM-L6-v2"  # $0 (GPU server)

# Cost breakdown:
# 5M high-value docs × 500 tokens avg = 2.5B tokens × $0.13 = $325
# 20M standard docs × 400 tokens avg  = 8B tokens   × $0.02 = $160
# 25M archival docs                   = self-hosted         = $50 (compute)
# Total: $535 vs $6,500 uniform large model → 92% cost reduction
```

---

## Interview Tips

> **Tip 1:** "How would you set up a production vector search pipeline?" — "Four components: (1) Embedding pipeline — batch embed documents using the model selected for quality/cost tradeoff, store embeddings with metadata. (2) Vector index — HNSW for < 50M docs (Qdrant/Weaviate), IVF-PQ for larger scale. (3) Real-time update consumer — Kafka or CDC events trigger incremental upserts so the index stays fresh. (4) Query pipeline — embed query, ANN retrieval of top-50, optional cross-encoder rerank to top-5. Add BM25 hybrid if keyword precision matters."

> **Tip 2:** "How do you handle updates to a vector index without downtime?" — "Two strategies: (1) Write-through: upsert new/updated vectors immediately as they change — most vector DBs support this natively. (2) Blue-green: build a new index in parallel, swap the alias when ready (works for major embedding model changes that require full re-indexing). The failure mode is treating the index as immutable — any catalog update leaves stale results until the next full rebuild."
