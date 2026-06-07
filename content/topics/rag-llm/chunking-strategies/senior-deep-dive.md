---
title: "Chunking Strategies - Senior Deep Dive"
topic: rag-llm
subtopic: chunking-strategies
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [rag, llm, chunking, agentic, proposition, multi-modal, optimization]
---

# Chunking Strategies — Senior-Level Deep Dive

## Agentic Chunking (LLM-Decided Boundaries)

Instead of rule-based splitting, use an LLM to decide where to split based on semantic content:

```python
from openai import OpenAI

client = OpenAI()

def agentic_chunk(text: str, target_chunk_count: int = None) -> list[dict]:
    """Use an LLM to identify natural chunk boundaries."""
    
    prompt = f"""Analyze this text and identify natural semantic boundaries where the topic changes.
Return a JSON array of objects, each with:
- "start_sentence": the first few words of where this chunk starts
- "summary": a 1-sentence summary of what this chunk covers
- "topic": a short label for the chunk's topic

Text to analyze:
---
{text[:4000]}
---

Identify 3-8 semantic chunks. Return JSON only."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0
    )
    
    boundaries = json.loads(response.choices[0].message.content)
    
    # Split text at identified boundaries
    chunks = []
    for i, boundary in enumerate(boundaries["chunks"]):
        start_marker = boundary["start_sentence"]
        start_idx = text.find(start_marker)
        
        if i + 1 < len(boundaries["chunks"]):
            next_marker = boundaries["chunks"][i + 1]["start_sentence"]
            end_idx = text.find(next_marker)
        else:
            end_idx = len(text)
        
        chunks.append({
            "text": text[start_idx:end_idx].strip(),
            "summary": boundary["summary"],
            "topic": boundary["topic"],
        })
    
    return chunks
```

**Trade-offs:**
- Produces the most semantically coherent chunks
- Expensive: 1 LLM call per document ($0.001-0.01/doc)
- Slow: 1-5 seconds per document (not suitable for real-time)
- Non-deterministic: same document may produce different chunks on retry
- Best for: high-value documents where chunk quality critically impacts retrieval

---

## Proposition-Based Chunking (Factual Decomposition)

Decompose documents into atomic factual statements. Each proposition is self-contained and embeds precisely.

```python
def decompose_to_propositions(text: str) -> list[str]:
    """Break text into atomic, self-contained factual statements."""
    
    prompt = f"""Decompose the following text into atomic factual propositions.
Each proposition should:
1. Be a single, self-contained fact
2. Be understandable without context from other propositions
3. Include necessary context (who, what, when) within the proposition itself

Text:
{text}

Return each proposition on a new line, numbered."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    
    propositions = [
        line.strip().lstrip("0123456789. ")
        for line in response.choices[0].message.content.split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]
    
    return propositions

# Example input:
# "Spark 3.5 introduced Connect API, which decouples the client from
#  the cluster. This reduces driver memory requirements by 60%."

# Output propositions:
# 1. "Apache Spark version 3.5 introduced the Connect API."
# 2. "The Spark Connect API decouples the client application from the Spark cluster."
# 3. "The Spark Connect API reduces driver memory requirements by approximately 60%."

# Each proposition embeds precisely — no "dangling references" or partial context
```

**When to use:** Knowledge bases where precision matters more than retrieval speed. Each proposition retrieves independently without ambiguity.

---

## Late Chunking

Embed the **full document first** (using the model's full context), then split into chunks that retain document-level contextual embedding quality.

```python
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch

class LateChunker:
    """Embed full document, then chunk the token embeddings."""
    
    def __init__(self, model_name: str = "jinaai/jina-embeddings-v2-base-en"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
    
    def chunk_and_embed(self, text: str, chunk_size_tokens: int = 128) -> list[dict]:
        """
        1. Tokenize full document
        2. Run through model (full attention across entire doc)
        3. Split token embeddings into chunks
        4. Pool each chunk's token embeddings → chunk embedding
        """
        # Full-document encoding (all tokens see each other via attention)
        inputs = self.tokenizer(text, return_tensors="pt", max_length=8192, truncation=True)
        
        with torch.no_grad():
            outputs = self.model(**inputs)
        
        # Token-level embeddings (each token has seen the full document context)
        token_embeddings = outputs.last_hidden_state[0].numpy()  # [seq_len, hidden_dim]
        tokens = self.tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
        
        # Split into chunks of chunk_size_tokens
        chunks = []
        for i in range(0, len(token_embeddings), chunk_size_tokens):
            chunk_token_embs = token_embeddings[i:i + chunk_size_tokens]
            
            # Mean pooling of token embeddings → single chunk vector
            chunk_embedding = chunk_token_embs.mean(axis=0)
            chunk_embedding = chunk_embedding / np.linalg.norm(chunk_embedding)
            
            # Decode tokens back to text
            chunk_text = self.tokenizer.decode(inputs["input_ids"][0][i:i + chunk_size_tokens])
            
            chunks.append({
                "text": chunk_text,
                "embedding": chunk_embedding,
                "token_start": i,
                "token_end": min(i + chunk_size_tokens, len(token_embeddings)),
            })
        
        return chunks

# Advantage: each chunk's embedding benefits from full-document context
# "it" in chunk 5 gets meaning from "Spark" mentioned in chunk 1
# Standard chunking: each chunk is embedded in isolation, losing cross-chunk context
```

---

## Chunk Size Optimization via Evaluation

Don't guess chunk size — optimize it empirically against your retrieval metrics:

```python
import numpy as np
from dataclasses import dataclass

@dataclass
class ChunkSizeExperiment:
    chunk_size: int
    overlap: int
    recall_at_5: float
    recall_at_10: float
    avg_chunk_count: float
    index_cost_relative: float

def optimize_chunk_size(
    documents: list[str],
    test_queries: list[str],
    ground_truth: dict,  # query → relevant doc IDs
    embed_fn,
    chunk_sizes: list[int] = [128, 256, 512, 1024, 2048],
) -> list[ChunkSizeExperiment]:
    """Find optimal chunk size by measuring retrieval quality."""
    
    results = []
    
    for size in chunk_sizes:
        overlap = int(size * 0.1)  # 10% overlap
        
        # Chunk all documents
        all_chunks = []
        for doc_id, doc_text in enumerate(documents):
            chunks = split_fixed_size(doc_text, size, overlap)
            for i, chunk in enumerate(chunks):
                all_chunks.append({"id": f"{doc_id}_{i}", "doc_id": doc_id, "text": chunk})
        
        # Embed chunks
        chunk_embeddings = embed_fn([c["text"] for c in all_chunks])
        
        # Evaluate retrieval
        recalls_5, recalls_10 = [], []
        for query in test_queries:
            query_emb = embed_fn([query])[0]
            scores = np.dot(chunk_embeddings, query_emb)
            top_10_idx = np.argsort(scores)[::-1][:10]
            
            retrieved_docs = set(all_chunks[i]["doc_id"] for i in top_10_idx[:5])
            relevant_docs = set(ground_truth[query])
            
            recalls_5.append(len(retrieved_docs & relevant_docs) / len(relevant_docs))
            
            retrieved_docs_10 = set(all_chunks[i]["doc_id"] for i in top_10_idx)
            recalls_10.append(len(retrieved_docs_10 & relevant_docs) / len(relevant_docs))
        
        results.append(ChunkSizeExperiment(
            chunk_size=size,
            overlap=overlap,
            recall_at_5=np.mean(recalls_5),
            recall_at_10=np.mean(recalls_10),
            avg_chunk_count=len(all_chunks) / len(documents),
            index_cost_relative=len(all_chunks) / (len(documents) * 1),  # Relative to 1 chunk/doc
        ))
    
    # Print comparison
    for r in results:
        print(f"Size={r.chunk_size:4d} | R@5={r.recall_at_5:.3f} | R@10={r.recall_at_10:.3f} | Chunks/doc={r.avg_chunk_count:.1f}")
    
    return results

# Typical results:
# Size= 128 | R@5=0.72 | R@10=0.84 | Chunks/doc=15.2  ← too granular, noisy
# Size= 256 | R@5=0.81 | R@10=0.91 | Chunks/doc=7.8   ← good precision
# Size= 512 | R@5=0.78 | R@10=0.89 | Chunks/doc=4.1   ← balanced (often optimal)
# Size=1024 | R@5=0.69 | R@10=0.82 | Chunks/doc=2.2   ← losing precision
# Size=2048 | R@5=0.61 | R@10=0.74 | Chunks/doc=1.3   ← too coarse
```

---

## Production Chunking Pipeline at Scale

```python
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator
import hashlib

class ProductionChunkingPipeline:
    """Chunk millions of documents efficiently with dedup and versioning."""
    
    def __init__(self, chunker, embed_fn, vector_store, metadata_db):
        self.chunker = chunker
        self.embed_fn = embed_fn
        self.vector_store = vector_store
        self.metadata_db = metadata_db
    
    def process_document(self, doc_id: str, text: str, metadata: dict) -> int:
        """Chunk, embed, and index a single document."""
        # Check if document has changed since last chunking
        content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
        last_hash = self.metadata_db.get_chunk_hash(doc_id)
        
        if content_hash == last_hash:
            return 0  # Document unchanged, skip
        
        # Delete old chunks for this document
        self.vector_store.delete(filter={"doc_id": doc_id})
        
        # Create new chunks
        chunks = self.chunker.chunk(text)
        
        # Embed in batch
        embeddings = self.embed_fn([c["text"] for c in chunks])
        
        # Upsert to vector store
        points = [
            {
                "id": f"{doc_id}_chunk_{i}",
                "vector": emb.tolist(),
                "metadata": {
                    "doc_id": doc_id,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    **metadata,
                    **chunks[i].get("metadata", {}),
                }
            }
            for i, emb in enumerate(embeddings)
        ]
        self.vector_store.upsert(points)
        
        # Record new hash
        self.metadata_db.set_chunk_hash(doc_id, content_hash)
        
        return len(chunks)
    
    def process_batch(self, documents: list[dict], workers: int = 4) -> dict:
        """Process many documents with parallelism."""
        stats = {"processed": 0, "chunks_created": 0, "skipped": 0}
        
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(self.process_document, doc["id"], doc["text"], doc["metadata"])
                for doc in documents
            ]
            for future in futures:
                chunk_count = future.result()
                if chunk_count > 0:
                    stats["processed"] += 1
                    stats["chunks_created"] += chunk_count
                else:
                    stats["skipped"] += 1
        
        return stats
```

---

## Handling Versioned Chunks (Document Updates)

```python
class VersionedChunkManager:
    """Manage chunk versions when source documents update."""
    
    def update_document(self, doc_id: str, new_text: str):
        """Re-chunk a document and update only changed chunks."""
        old_chunks = self.get_existing_chunks(doc_id)
        new_chunks = self.chunker.chunk(new_text)
        
        # Compare old vs new chunks (by content hash)
        old_hashes = {hashlib.md5(c["text"].encode()).hexdigest(): c for c in old_chunks}
        new_hashes = {hashlib.md5(c["text"].encode()).hexdigest(): c for c in new_chunks}
        
        to_add = [c for h, c in new_hashes.items() if h not in old_hashes]
        to_remove = [c for h, c in old_hashes.items() if h not in new_hashes]
        unchanged = [c for h, c in new_hashes.items() if h in old_hashes]
        
        # Only re-embed and re-index changed chunks
        if to_remove:
            self.vector_store.delete(ids=[c["id"] for c in to_remove])
        
        if to_add:
            embeddings = self.embed_fn([c["text"] for c in to_add])
            self.vector_store.upsert(to_add, embeddings)
        
        return {
            "added": len(to_add),
            "removed": len(to_remove),
            "unchanged": len(unchanged),
            "re_embed_savings": f"{len(unchanged) / max(len(new_chunks), 1) * 100:.0f}%"
        }
```

---

## Interview Tips

> **Tip 1:** "How do you choose between chunking strategies?" — Start with recursive character splitter (simple, good baseline). Measure recall@10. If below target, try semantic chunking (better boundaries). If documents have natural structure (headers, sections), exploit that structure. Only use LLM-based chunking for high-value, small-volume corpora where the cost is justified.

> **Tip 2:** "How do you handle documents that update frequently?" — Content-hash each chunk. On document update, re-chunk and compare hashes. Only re-embed and re-index changed chunks. This avoids re-processing unchanged content and keeps costs proportional to actual changes.

> **Tip 3:** "What's late chunking and when would you use it?" — Late chunking embeds the full document with a long-context model (tokens see all context via attention), then splits the token embeddings into chunks. Each chunk benefits from full-document understanding. Use when cross-referencing within documents is important (e.g., "it" referring to something mentioned earlier).
