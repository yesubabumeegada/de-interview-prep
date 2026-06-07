---
title: "Vector Databases - Real-World Production Examples"
topic: rag-llm
subtopic: vector-databases
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, vector-database, production, deployment, monitoring, migration]
---

# Vector Databases — Real-World Production Examples

## Pattern 1: Production Qdrant Deployment

```python
from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams, HnswConfigDiff, OptimizersConfigDiff,
    ScalarQuantization, ScalarQuantizationConfig,
    Distance, PointStruct
)

# Production client with connection pooling
client = QdrantClient(
    url="https://qdrant-cluster.internal:6333",
    api_key="your-key",
    timeout=30,
    prefer_grpc=True,  # gRPC is faster for bulk operations
)

# Create production-grade collection
client.create_collection(
    collection_name="knowledge_base",
    vectors_config=VectorParams(
        size=1024,
        distance=Distance.COSINE,
        on_disk=True,  # Vectors on disk, quantized version in RAM
    ),
    hnsw_config=HnswConfigDiff(
        m=16,
        ef_construct=256,
        on_disk=False,  # Keep graph in RAM for fast traversal
    ),
    optimizers_config=OptimizersConfigDiff(
        indexing_threshold=20000,
        memmap_threshold=50000,
        max_segment_size=200000,  # Control segment sizes
    ),
    quantization_config=ScalarQuantization(
        scalar=ScalarQuantizationConfig(
            type="int8",
            quantile=0.99,
            always_ram=True,  # Quantized vectors always in RAM
        )
    ),
    replication_factor=2,  # HA: 2 copies of each shard
    shard_number=3,         # 3 shards for parallelism
)
```

---

## Pattern 2: Capacity Planning

```python
def plan_vector_db_capacity(
    total_documents: int,
    avg_chunks_per_doc: int,
    embedding_dims: int,
    target_qps: int,
    growth_rate_monthly: float = 0.1,  # 10% monthly growth
    retention_months: int = 12,
) -> dict:
    """Plan vector DB infrastructure for production deployment."""
    
    # Vector count
    total_vectors = total_documents * avg_chunks_per_doc
    vectors_in_12_months = total_vectors * (1 + growth_rate_monthly) ** retention_months
    
    # Memory calculation (int8 quantized + HNSW overhead)
    bytes_per_vector = embedding_dims * 1  # int8
    hnsw_bytes_per_vector = 16 * 4 * 2  # M=16 neighbors, 4 bytes per ID, ~2 layers avg
    metadata_bytes = 200  # average payload size
    total_bytes_per_vector = bytes_per_vector + hnsw_bytes_per_vector + metadata_bytes
    
    total_memory_gb = (vectors_in_12_months * total_bytes_per_vector) / (1024**3)
    
    # Node sizing (70% utilization target)
    memory_per_node_gb = 64  # r6g.2xlarge
    nodes_for_memory = int(np.ceil(total_memory_gb / (memory_per_node_gb * 0.7)))
    
    # QPS capacity (~5000 QPS per node with int8 + HNSW)
    nodes_for_qps = int(np.ceil(target_qps / 5000))
    
    # Take the larger requirement, add replication
    data_nodes = max(nodes_for_memory, nodes_for_qps)
    total_nodes = data_nodes * 2  # 2x replication for HA
    
    # Cost
    cost_per_node = 0.256 * 730  # r6g.2xlarge on-demand hourly × hours/month
    monthly_cost = total_nodes * cost_per_node
    
    return {
        "total_vectors_now": total_vectors,
        "total_vectors_12mo": int(vectors_in_12_months),
        "memory_required_gb": round(total_memory_gb, 1),
        "data_nodes": data_nodes,
        "total_nodes_with_ha": total_nodes,
        "instance_type": "r6g.2xlarge (64 GB RAM, 8 vCPU)",
        "monthly_cost": f"${monthly_cost:,.0f}",
    }

# Example: Documentation search for 500K docs
plan = plan_vector_db_capacity(
    total_documents=500_000,
    avg_chunks_per_doc=5,
    embedding_dims=1024,
    target_qps=500,
)
# Result: ~2.5M vectors, ~5 GB memory, 2 nodes (HA), ~$374/mo
```

---

## Pattern 3: CDC Pipeline into Vector Store

Keep your vector database in sync with source data via Change Data Capture:

```python
import json
from kafka import KafkaConsumer
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

class VectorDBSyncPipeline:
    """Consume CDC events and sync to vector database."""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            "documents.changes",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda m: json.loads(m.decode()),
            group_id="vector-sync",
            auto_offset_reset="earliest",
        )
        self.vector_client = QdrantClient("qdrant:6333")
        self.model = SentenceTransformer("BAAI/bge-base-en-v1.5", device="cuda")
        self.batch = []
        self.batch_size = 100
    
    def run(self):
        """Main sync loop — processes CDC events continuously."""
        for message in self.consumer:
            event = message.value
            self.batch.append(event)
            
            if len(self.batch) >= self.batch_size:
                self._process_batch()
                self.batch = []
    
    def _process_batch(self):
        """Process a batch of CDC events."""
        inserts_updates = [e for e in self.batch if e["op"] in ("c", "u")]  # create/update
        deletes = [e for e in self.batch if e["op"] == "d"]
        
        # Handle deletes
        if deletes:
            delete_ids = [e["doc_id"] for e in deletes]
            self.vector_client.delete(
                collection_name="knowledge_base",
                points_selector={"points": delete_ids}
            )
        
        # Handle inserts/updates: embed and upsert
        if inserts_updates:
            texts = [e["content"] for e in inserts_updates]
            embeddings = self.model.encode(texts, normalize_embeddings=True)
            
            points = [
                PointStruct(
                    id=event["doc_id"],
                    vector=embedding.tolist(),
                    payload={
                        "title": event.get("title", ""),
                        "source": event.get("source", ""),
                        "updated_at": event.get("updated_at", ""),
                    }
                )
                for event, embedding in zip(inserts_updates, embeddings)
            ]
            
            self.vector_client.upsert(
                collection_name="knowledge_base",
                points=points,
                wait=True,
            )

# Deploy as a Kubernetes deployment with 1 replica
# Processes ~1000 events/second with batching
```

---

## Pattern 4: Monitoring Vector DB Health

```python
from prometheus_client import Gauge, Histogram, Counter
import time

# Prometheus metrics
SEARCH_LATENCY = Histogram("vector_search_latency_ms", "Search latency", buckets=[1,2,5,10,20,50,100,200])
SEARCH_SCORE = Histogram("vector_search_top_score", "Top-1 similarity score", buckets=[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.95])
COLLECTION_SIZE = Gauge("vector_collection_size", "Number of vectors", ["collection"])
INDEX_HEALTH = Gauge("vector_index_health", "Percentage of vectors indexed", ["collection"])

class MonitoredVectorSearch:
    def __init__(self, client: QdrantClient, collection: str):
        self.client = client
        self.collection = collection
    
    def search(self, query_vector: list[float], top_k: int = 10, **kwargs):
        start = time.time()
        results = self.client.search(
            collection_name=self.collection,
            query_vector=query_vector,
            limit=top_k,
            **kwargs
        )
        latency_ms = (time.time() - start) * 1000
        
        # Record metrics
        SEARCH_LATENCY.observe(latency_ms)
        if results:
            SEARCH_SCORE.observe(results[0].score)
        
        # Alert on degradation
        if latency_ms > 100:
            log_warning(f"Slow vector search: {latency_ms:.0f}ms")
        if results and results[0].score < 0.3:
            log_warning(f"Low relevance score: {results[0].score:.3f}")
        
        return results
    
    def health_check(self) -> dict:
        """Periodic health check of the vector DB."""
        info = self.client.get_collection(self.collection)
        
        COLLECTION_SIZE.labels(collection=self.collection).set(info.points_count)
        
        indexed_pct = info.indexed_vectors_count / max(info.points_count, 1) * 100
        INDEX_HEALTH.labels(collection=self.collection).set(indexed_pct)
        
        return {
            "collection": self.collection,
            "total_vectors": info.points_count,
            "indexed_vectors": info.indexed_vectors_count,
            "index_health_pct": indexed_pct,
            "status": info.status,
            "segments": len(info.segments) if hasattr(info, 'segments') else "unknown",
        }

# Alert rules (Prometheus):
# - vector_search_latency_ms p99 > 50ms for 5 min → page on-call
# - vector_search_top_score avg < 0.4 for 30 min → investigate quality
# - vector_index_health < 95% → index rebuild needed
```

---

## Pattern 5: Migration Between Vector Databases

```python
class VectorDBMigration:
    """Migrate from Pinecone to Qdrant with zero downtime."""
    
    def __init__(self, source_index, target_client, target_collection):
        self.source = source_index  # Pinecone index
        self.target = target_client  # Qdrant client
        self.target_collection = target_collection
    
    def migrate(self, batch_size: int = 100):
        """Full migration with progress tracking."""
        # Step 1: Fetch all vectors from source (paginated)
        total_migrated = 0
        pagination_token = None
        
        while True:
            # Pinecone list + fetch pattern
            list_response = self.source.list(limit=batch_size, pagination_token=pagination_token)
            if not list_response.vectors:
                break
            
            ids = [v.id for v in list_response.vectors]
            fetch_response = self.source.fetch(ids=ids)
            
            # Step 2: Transform to target format and upsert
            points = [
                PointStruct(
                    id=vec_id,
                    vector=vec_data.values,
                    payload=vec_data.metadata or {}
                )
                for vec_id, vec_data in fetch_response.vectors.items()
            ]
            
            self.target.upsert(
                collection_name=self.target_collection,
                points=points,
                wait=True,
            )
            
            total_migrated += len(points)
            pagination_token = list_response.pagination.next
            
            if total_migrated % 10000 == 0:
                print(f"Migrated {total_migrated} vectors")
            
            if not pagination_token:
                break
        
        return total_migrated
    
    def validate(self, sample_size: int = 100):
        """Validate migration by comparing search results."""
        # Run same queries against both systems, compare results
        test_queries = self._get_sample_queries(sample_size)
        match_count = 0
        
        for query_vector in test_queries:
            source_results = set(r.id for r in self.source.query(vector=query_vector, top_k=10).matches)
            target_results = set(r.id for r in self.target.search(
                collection_name=self.target_collection, query_vector=query_vector, limit=10
            ))
            
            overlap = len(source_results & target_results)
            if overlap >= 8:  # 80%+ overlap is acceptable
                match_count += 1
        
        accuracy = match_count / sample_size
        print(f"Migration validation: {accuracy:.1%} queries match (target: >95%)")
        return accuracy > 0.95

# Migration runbook:
# 1. Create target collection with optimal config
# 2. Run migration (background, may take hours for large datasets)
# 3. Validate with sample queries (>95% result overlap)
# 4. Dual-read period: query both, compare, log differences
# 5. Switch traffic to target (feature flag)
# 6. Monitor for 1 week, then decommission source
```

---

## Pattern 6: Backup and Disaster Recovery

```python
# Qdrant: snapshot-based backup
import subprocess
from datetime import datetime

def backup_collection(client: QdrantClient, collection: str, s3_bucket: str):
    """Create and upload a collection snapshot."""
    # Create snapshot
    snapshot = client.create_snapshot(collection_name=collection)
    snapshot_path = snapshot.name
    
    # Upload to S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"backups/qdrant/{collection}/{timestamp}/{snapshot_path}"
    
    subprocess.run([
        "aws", "s3", "cp",
        f"http://qdrant:6333/collections/{collection}/snapshots/{snapshot_path}",
        f"s3://{s3_bucket}/{s3_key}"
    ], check=True)
    
    return s3_key

def restore_collection(client: QdrantClient, collection: str, snapshot_s3_url: str):
    """Restore collection from S3 snapshot."""
    client.recover_snapshot(
        collection_name=collection,
        location=snapshot_s3_url,
    )

# Schedule: daily snapshots, 7-day retention
# RPO: 24 hours (last snapshot)
# RTO: ~30 minutes (download snapshot + restore)
```

---

## Interview Tips

> **Tip 1:** "How do you handle disaster recovery for a vector DB?" — Snapshot-based backups to S3 (daily), cross-region replication for HA (if supported), and keep the embedding pipeline idempotent so you can rebuild from source data if needed. RPO depends on snapshot frequency; RTO depends on data size and restore speed.

> **Tip 2:** "How would you migrate from one vector DB to another?" — Dual-write period: new writes go to both. Batch-migrate existing data. Validate with sample queries (>95% result overlap). Use feature flags to switch read traffic. Keep old system warm for rollback. Never rush — bad migrations corrupt search quality silently.

> **Tip 3:** "What monitoring would you set up?" — Search latency (p50/p99), top-1 similarity scores (quality proxy), collection size growth, index health percentage, segment count (compaction health), and error rates. Alert if p99 > 50ms or avg top score drops below 0.4.
