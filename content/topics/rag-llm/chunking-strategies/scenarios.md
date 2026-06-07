---
title: "Chunking Strategies - Scenario Questions"
topic: rag-llm
subtopic: chunking-strategies
content_type: scenario_question
tags: [rag, llm, chunking, interview, scenarios]
---

# Scenario Questions — Chunking Strategies

<article data-difficulty="junior">

## 🟢 Junior: Choosing Chunk Size

**Scenario:** You're building a Q&A bot over company documentation (500 pages). You try chunk sizes of 100, 500, and 2000 characters. With 100-char chunks, the bot gives incomplete answers. With 2000-char chunks, it returns irrelevant context. What's happening and what size should you use?

<details>
<summary>💡 Hint</summary>
Too small = each chunk lacks context to be meaningful. Too large = multiple topics per chunk dilutes the embedding, reducing retrieval precision.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Problem diagnosis:
# 100 chars: "partitioning in Spark" becomes chunks like ["partitioning", "in Spark uses"]
#   → Embeddings are too vague, retrieve wrong content
# 2000 chars: A chunk covers 3 different topics
#   → Embedding averages all topics, matches nothing precisely

# Sweet spot: 400-600 characters (roughly 100-150 tokens)
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,       # ~125 tokens — one focused concept per chunk
    chunk_overlap=50,     # 10% overlap to avoid mid-sentence cuts
    separators=["\n\n", "\n", ". ", " "],
)

chunks = splitter.split_text(document)
print(f"Average chunk: {sum(len(c) for c in chunks) / len(chunks):.0f} chars")
```

**Key Points:**
- 100 chars: too granular — chunks lack self-contained meaning
- 2000 chars: too broad — embedding becomes unfocused average of multiple topics
- 400-600 chars (~100-150 tokens): typically one concept per chunk, enough context for the embedding to capture meaning
- Always add 10% overlap to prevent cutting mid-sentence
- Validate: embed 50 test queries, check if the correct chunk appears in top-5

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Overlap Calculation

**Scenario:** Your chunks are 500 characters with 0 overlap. Users report that answers sometimes miss important context that spans two chunks. For example, a sentence like "This technique reduces latency by 50%, which is critical for real-time applications" gets split at "50%," — half in chunk 3, half in chunk 4. How do you fix this?

<details>
<summary>💡 Hint</summary>
Add overlap between consecutive chunks so that content near boundaries appears in both chunks.
</details>

<details>
<summary>✅ Solution</summary>

```python
def chunk_with_overlap(text: str, chunk_size: int = 500, overlap: int = 75) -> list[str]:
    """Split text into overlapping chunks."""
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to end at a sentence boundary
        last_period = chunk.rfind(". ")
        if last_period > chunk_size * 0.6:  # Only if we're past 60% of chunk
            end = start + last_period + 2
            chunk = text[start:end]
        
        chunks.append(chunk.strip())
        start = end - overlap  # Step back by overlap amount
    
    return chunks

# Example:
# Chunk 3: "...optimization reduces I/O by caching. This technique reduces latency by 50%, which is"
# Chunk 4: "latency by 50%, which is critical for real-time applications. The next consideration..."
# The overlapping text ensures neither chunk loses the complete thought.

# Rules of thumb:
# overlap = 10-20% of chunk_size
# 500 char chunks → 50-100 char overlap
# 1000 char chunks → 100-200 char overlap
```

**Key Points:**
- Overlap ensures sentences at chunk boundaries aren't orphaned
- 10-20% overlap is the sweet spot (less = missed context, more = wasted storage)
- Try to end chunks at sentence boundaries (`.`) rather than mid-word
- Storage overhead: 15% overlap means ~15% more chunks total
- Alternative: sentence-aware splitting (split only at `.` boundaries)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Token Counting

**Scenario:** You set chunk_size=500 (characters), but your embedding model has a 256-token limit. Some chunks exceed this and get silently truncated by the model. How do you ensure chunks respect the token limit?

<details>
<summary>💡 Hint</summary>
Characters ≠ tokens. You need to count tokens using the model's tokenizer, not character length.
</details>

<details>
<summary>✅ Solution</summary>

```python
import tiktoken

# Character-based counting is WRONG for token limits
text = "Apache Spark's Adaptive Query Execution optimizes at runtime"
print(f"Characters: {len(text)}")  # 62 characters
# But this is only ~10 tokens!

# Use the correct tokenizer for your model
enc = tiktoken.get_encoding("cl100k_base")  # OpenAI models

def chunk_by_tokens(text: str, max_tokens: int = 200, overlap_tokens: int = 20) -> list[str]:
    """Chunk by actual token count, not character count."""
    tokens = enc.encode(text)
    chunks = []
    start = 0
    
    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = enc.decode(chunk_tokens)
        chunks.append(chunk_text)
        start = end - overlap_tokens
    
    return chunks

# For sentence-transformers (different tokenizer):
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

def count_tokens_st(text: str) -> int:
    return len(tokenizer.encode(text, add_special_tokens=False))

# Safe chunk sizes by model:
# OpenAI text-embedding-3-small: max 8191 tokens (generous)
# all-MiniLM-L6-v2: max 256 tokens (strict!)
# BGE-large: max 512 tokens
# Always check YOUR model's limit and chunk accordingly
```

**Key Points:**
- 1 token ≈ 4 characters in English (rough approximation)
- 500 characters ≈ 125 tokens (usually safe for most models)
- MiniLM models have only 256-token limit — easy to exceed with character-based chunking
- Always use the model's actual tokenizer for counting, not approximations
- If a chunk exceeds the limit, the model silently truncates — you lose content without any error

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Handling Short Documents

**Scenario:** Your knowledge base has 10,000 FAQ entries, each only 50-100 words long. Should you chunk these, or embed each FAQ as a single vector?

<details>
<summary>💡 Hint</summary>
If a document is already shorter than your target chunk size, chunking it would be pointless or even harmful (splitting a 50-word FAQ into 2 meaningless fragments).
</details>

<details>
<summary>✅ Solution</summary>

```python
def smart_chunk(text: str, min_chunk_size: int = 100, target_chunk_size: int = 500) -> list[str]:
    """Only chunk if the document is long enough to benefit from it."""
    if len(text) <= target_chunk_size:
        # Document is already small enough — embed as-is
        return [text]
    else:
        # Document is large — chunk it
        return recursive_split(text, target_chunk_size, overlap=50)

# For FAQ entries (50-100 words = 200-400 chars):
# Each FAQ IS the chunk — no splitting needed

faqs = [
    {"q": "What is data partitioning?", "a": "Data partitioning divides a large dataset into smaller pieces..."},
    {"q": "When to use broadcast join?", "a": "Use broadcast join when one table is small enough to fit in memory..."},
]

# Embed question + answer together as one unit
for faq in faqs:
    text = f"Question: {faq['q']}\nAnswer: {faq['a']}"
    embedding = embed(text)  # One vector per FAQ — no chunking needed
```

**Key Points:**
- Don't chunk documents shorter than your target chunk size
- FAQ/short-form content: embed the entire entry as one vector
- Chunking very short text creates fragments that lack meaning
- For mixed-length corpora: apply chunking conditionally based on document length
- Rule: if document < 1.5× chunk_size, don't split it

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Fixed-Size vs Separator-Based

**Scenario:** Your team debates between splitting every 500 characters (fixed-size) versus splitting on paragraph breaks ("\n\n"). What are the trade-offs, and when would you choose each?

<details>
<summary>💡 Hint</summary>
Fixed-size gives uniform chunks but may cut mid-sentence. Separator-based respects document structure but produces variable-size chunks.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Fixed-size: predictable, uniform, but may cut awkwardly
def fixed_size_chunk(text: str, size: int = 500) -> list[str]:
    return [text[i:i+size] for i in range(0, len(text), size)]
# "...the query optimizer will" | "choose a hash join when..."  ← bad split!

# Separator-based: respects structure, but variable sizes
def paragraph_chunk(text: str) -> list[str]:
    return [p.strip() for p in text.split("\n\n") if p.strip()]
# Some paragraphs: 50 chars (too small), others: 3000 chars (too large)

# BEST: Recursive (hybrid) — try separators, fall back to size
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " "]  # Try paragraph, then line, then sentence
)
# Tries to split at paragraphs first; if still too big, splits at sentences
```

| Approach | Uniform Size | Respects Structure | Best For |
|----------|-------------|-------------------|----------|
| Fixed-size | Yes | No | Homogeneous text (logs, transcripts) |
| Separator-based | No | Yes | Well-structured docs (markdown, code) |
| Recursive (hybrid) | Mostly | Yes | General purpose (recommended default) |

**Key Points:**
- Fixed-size: simple but cuts mid-thought — add overlap to mitigate
- Separator-based: clean splits but wildly variable sizes (some too small, some too large)
- Recursive: best of both — respects structure up to a size limit, then falls back
- Start with recursive as your default; switch to specialized strategies only if needed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Semantic Chunking Implementation

**Scenario:** Your documentation has sections that discuss multiple topics within a single paragraph (no clear structural separators). Fixed-size chunking produces chunks that blend two unrelated concepts, hurting retrieval precision. Implement semantic chunking that splits where the topic changes.

<details>
<summary>💡 Hint</summary>
Embed consecutive sentences. When the cosine similarity between adjacent sentences drops below a threshold, that's a semantic boundary — split there.
</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np
import re
from sentence_transformers import SentenceTransformer

class SemanticChunker:
    def __init__(self, model_name="all-MiniLM-L6-v2", percentile_threshold: int = 25):
        self.model = SentenceTransformer(model_name)
        self.percentile = percentile_threshold
    
    def chunk(self, text: str, min_chunk_size: int = 100) -> list[str]:
        sentences = re.split(r'(?<=[.!?])\s+', text)
        if len(sentences) <= 3:
            return [text]
        
        # Embed all sentences
        embeddings = self.model.encode(sentences, normalize_embeddings=True)
        
        # Compute similarity between consecutive sentences
        similarities = [
            float(np.dot(embeddings[i], embeddings[i+1]))
            for i in range(len(embeddings) - 1)
        ]
        
        # Dynamic threshold: split at the lowest N% of similarities
        threshold = np.percentile(similarities, self.percentile)
        
        # Build chunks at break points
        chunks = []
        current = [sentences[0]]
        
        for i, sim in enumerate(similarities):
            if sim < threshold and len(" ".join(current)) >= min_chunk_size:
                chunks.append(" ".join(current))
                current = [sentences[i + 1]]
            else:
                current.append(sentences[i + 1])
        
        if current:
            chunks.append(" ".join(current))
        
        return chunks

# Usage
chunker = SemanticChunker(percentile_threshold=30)
chunks = chunker.chunk(long_document)

# Result: each chunk contains sentences about the same topic
# Split points occur where the subject matter changes
```

**Key Points:**
- Percentile-based threshold adapts to document — works regardless of overall topic density
- `percentile=25` means split at the 25% lowest similarity points (more chunks)
- `percentile=10` means split only at the most dramatic topic shifts (fewer, larger chunks)
- min_chunk_size prevents splitting into tiny fragments
- Cost: one embedding pass over all sentences (~50ms for a 5-page doc)
- Fallback: if document has only 1-3 sentences, return it as a single chunk

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Parent-Child Retrieval Strategy

**Scenario:** Your RAG system retrieves small, precise chunks (200 tokens) but the LLM generates incomplete answers because each chunk lacks surrounding context. How do you implement parent-child chunking where small chunks are retrieved but larger parent context is returned to the LLM?

<details>
<summary>💡 Hint</summary>
Create two levels: small "child" chunks (embedded, used for retrieval) linked to larger "parent" chunks (not embedded, returned as context). When a child matches, look up and return its parent.
</details>

<details>
<summary>✅ Solution</summary>

```python
from dataclasses import dataclass
from typing import Optional
import uuid

@dataclass
class ChunkPair:
    child_id: str
    child_text: str
    parent_id: str
    parent_text: str

def create_parent_child_chunks(
    text: str, 
    parent_size: int = 1500, 
    child_size: int = 300, 
    child_overlap: int = 50
) -> list[ChunkPair]:
    """Create linked parent-child chunk pairs."""
    pairs = []
    
    # Step 1: Create parent chunks (large, context-rich)
    parents = split_at_boundaries(text, parent_size)
    
    for parent_text in parents:
        parent_id = str(uuid.uuid4())[:8]
        
        # Step 2: Split each parent into child chunks (small, precise)
        children = split_fixed(parent_text, child_size, child_overlap)
        
        for child_text in children:
            pairs.append(ChunkPair(
                child_id=f"{parent_id}_c{len(pairs)}",
                child_text=child_text,
                parent_id=parent_id,
                parent_text=parent_text,
            ))
    
    return pairs

# Indexing: only embed and store CHILD chunks
def index_parent_child(pairs: list[ChunkPair], embed_fn, vector_store, doc_store):
    for pair in pairs:
        # Embed child (small, focused — good for retrieval)
        child_embedding = embed_fn(pair.child_text)
        
        # Store child in vector DB (for retrieval)
        vector_store.upsert({
            "id": pair.child_id,
            "vector": child_embedding,
            "metadata": {"parent_id": pair.parent_id}
        })
        
        # Store parent in document store (for context, NOT in vector DB)
        doc_store.set(pair.parent_id, pair.parent_text)

# Retrieval: search children, return parents
def search_parent_child(query: str, embed_fn, vector_store, doc_store, top_k=5):
    query_vec = embed_fn(query)
    
    # Retrieve matching children
    child_results = vector_store.search(query_vec, top_k=top_k * 2)
    
    # Deduplicate by parent (multiple children from same parent)
    seen_parents = set()
    parent_texts = []
    
    for child in child_results:
        parent_id = child.metadata["parent_id"]
        if parent_id not in seen_parents:
            seen_parents.add(parent_id)
            parent_texts.append(doc_store.get(parent_id))
        if len(parent_texts) >= top_k:
            break
    
    return parent_texts  # Rich context for the LLM
```

**Key Points:**
- Children are precise (match specific questions well), parents provide context (complete answers)
- Deduplicate by parent: if 3 children from the same parent match, return parent only once
- Parent size ~1500 chars gives the LLM enough context without overwhelming the prompt
- Only children are in the vector DB (storage efficient — parents in cheap doc store)
- This is the #1 technique for improving answer completeness without sacrificing retrieval precision

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Chunking Code Files

**Scenario:** Your developer docs include code examples (Python, SQL, YAML). Standard text chunking splits code mid-function, producing broken snippets that are useless for retrieval. Design a code-aware chunking strategy.

<details>
<summary>💡 Hint</summary>
Split at logical code boundaries: function/class definitions, complete SQL statements, YAML document separators. Never split inside a function body.
</details>

<details>
<summary>✅ Solution</summary>

```python
import re
from typing import list

def chunk_python_code(code: str, max_size: int = 1500) -> list[dict]:
    """Chunk Python code at function/class boundaries."""
    chunks = []
    
    # Split at top-level definitions
    pattern = r'\n(?=(?:def |class |async def |@))'
    blocks = re.split(pattern, code)
    
    current_chunk = ""
    for block in blocks:
        if len(current_chunk) + len(block) > max_size and current_chunk:
            chunks.append({"text": current_chunk.strip(), "type": "python"})
            current_chunk = block
        else:
            current_chunk += "\n" + block
    
    if current_chunk.strip():
        chunks.append({"text": current_chunk.strip(), "type": "python"})
    
    return chunks

def chunk_sql(sql_text: str) -> list[dict]:
    """Chunk SQL at statement boundaries (;)."""
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    
    chunks = []
    current = ""
    for stmt in statements:
        if len(current) + len(stmt) > 1000 and current:
            chunks.append({"text": current.strip(), "type": "sql"})
            current = stmt + ";"
        else:
            current += "\n" + stmt + ";"
    
    if current.strip():
        chunks.append({"text": current.strip(), "type": "sql"})
    
    return chunks

def chunk_mixed_doc(text: str) -> list[dict]:
    """Handle documents with prose + code blocks."""
    chunks = []
    
    # Split on code fences
    parts = re.split(r'(```[\s\S]*?```)', text)
    
    for part in parts:
        if part.startswith("```"):
            # Code block — keep as one chunk (don't split)
            lang = part.split("\n")[0].replace("```", "").strip()
            code = "\n".join(part.split("\n")[1:-1])
            chunks.append({"text": code, "type": f"code_{lang}", "is_code": True})
        else:
            # Prose — chunk normally
            prose_chunks = recursive_split(part, size=500, overlap=50)
            for pc in prose_chunks:
                if pc.strip():
                    chunks.append({"text": pc, "type": "prose", "is_code": False})
    
    return chunks
```

**Key Points:**
- Never split inside a function, class, or SQL statement — broken code embeds poorly
- Keep complete code blocks as single chunks (even if slightly larger than target size)
- For mixed docs: separate code from prose, chunk each differently
- Add metadata `is_code=True` to enable code-specific retrieval features
- Code chunks can be larger (1000-2000 chars) since code is more information-dense

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Metadata Preservation

**Scenario:** Users ask "What does page 15 of the Q4 earnings report say about revenue?" but your chunks have no page or section information. Retrieval finds the right content but can't tell the user where it came from. How do you preserve source metadata?

<details>
<summary>💡 Hint</summary>
Track document source, page number, section header, and chunk position during chunking. Store as vector metadata for attribution.
</details>

<details>
<summary>✅ Solution</summary>

```python
import fitz  # PyMuPDF for PDF extraction
from dataclasses import dataclass, field

@dataclass 
class SourcedChunk:
    text: str
    source_file: str
    page_number: int
    section_header: str
    chunk_index: int
    char_start: int
    char_end: int

def chunk_pdf_with_metadata(pdf_path: str, chunk_size: int = 500) -> list[SourcedChunk]:
    """Extract and chunk PDF, preserving page numbers and section headers."""
    doc = fitz.open(pdf_path)
    chunks = []
    current_section = "Introduction"
    global_char_pos = 0
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]
        
        page_text = ""
        for block in blocks:
            if "lines" in block:
                for line in block["lines"]:
                    text = "".join(span["text"] for span in line["spans"])
                    font_size = line["spans"][0]["size"] if line["spans"] else 12
                    
                    # Detect section headers by font size
                    if font_size > 14:
                        current_section = text.strip()
                    
                    page_text += text + "\n"
        
        # Chunk this page's text
        page_chunks = split_at_sentences(page_text, chunk_size)
        
        for i, chunk_text in enumerate(page_chunks):
            chunks.append(SourcedChunk(
                text=chunk_text,
                source_file=pdf_path,
                page_number=page_num + 1,
                section_header=current_section,
                chunk_index=len(chunks),
                char_start=global_char_pos,
                char_end=global_char_pos + len(chunk_text),
            ))
            global_char_pos += len(chunk_text)
    
    return chunks

# When indexing, store metadata alongside vectors:
def index_sourced_chunks(chunks: list[SourcedChunk], embed_fn, vector_store):
    texts = [c.text for c in chunks]
    embeddings = embed_fn(texts)
    
    vector_store.upsert([
        {
            "id": f"chunk_{c.chunk_index}",
            "vector": emb,
            "metadata": {
                "source": c.source_file,
                "page": c.page_number,
                "section": c.section_header,
                "char_range": f"{c.char_start}-{c.char_end}",
            }
        }
        for c, emb in zip(chunks, embeddings)
    ])

# At retrieval time, provide citation:
# "Based on Q4 Earnings Report, Page 15, section 'Revenue Analysis':"
```

**Key Points:**
- Page numbers enable "see page X" citations in the response
- Section headers provide hierarchical context ("this is about Revenue under Financials")
- Character offsets allow highlighting the exact passage in the original document
- Store metadata in the vector DB — it's returned with search results at zero extra cost
- For compliance/audit: metadata proves which source document generated each answer

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Recursive Splitting Strategy

**Scenario:** Your document has: long paragraphs (3000 chars), medium paragraphs (800 chars), and short bullet lists (100 chars). A single splitting strategy either over-splits the short ones or under-splits the long ones. Implement recursive splitting.

<details>
<summary>💡 Hint</summary>
Try the largest separators first (paragraphs). If a resulting chunk is still too big, try smaller separators (sentences). Keep going until all chunks are under the size limit.
</details>

<details>
<summary>✅ Solution</summary>

```python
def recursive_split(
    text: str,
    max_size: int = 500,
    overlap: int = 50,
    separators: list[str] = None
) -> list[str]:
    """Recursively split using progressively smaller separators."""
    if separators is None:
        separators = ["\n\n", "\n", ". ", ", ", " "]
    
    # Base case: text fits in one chunk
    if len(text) <= max_size:
        return [text] if text.strip() else []
    
    # Try each separator in order
    for sep in separators:
        if sep in text:
            parts = text.split(sep)
            
            # Merge parts into chunks up to max_size
            chunks = []
            current = ""
            
            for part in parts:
                candidate = current + sep + part if current else part
                
                if len(candidate) <= max_size:
                    current = candidate
                else:
                    if current:
                        chunks.append(current)
                    # If single part exceeds max_size, try next separator
                    if len(part) > max_size:
                        remaining_seps = separators[separators.index(sep) + 1:]
                        sub_chunks = recursive_split(part, max_size, overlap, remaining_seps)
                        chunks.extend(sub_chunks)
                        current = ""
                    else:
                        current = part
            
            if current:
                chunks.append(current)
            
            # Add overlap between chunks
            if overlap > 0 and len(chunks) > 1:
                chunks = add_overlap(chunks, overlap)
            
            return [c for c in chunks if c.strip()]
    
    # Fallback: hard split by character count
    return [text[i:i+max_size] for i in range(0, len(text), max_size - overlap)]

def add_overlap(chunks: list[str], overlap: int) -> list[str]:
    """Add overlap from end of previous chunk to start of next."""
    result = [chunks[0]]
    for i in range(1, len(chunks)):
        prefix = chunks[i-1][-overlap:] if len(chunks[i-1]) > overlap else ""
        result.append(prefix + chunks[i])
    return result
```

**Key Points:**
- Tries paragraph breaks first → respects document structure
- Falls back to sentence, then clause, then word boundaries
- Long paragraphs get sub-split at sentence level (not left oversized)
- Short bullet lists stay together (already under max_size)
- This is exactly how LangChain's RecursiveCharacterTextSplitter works internally

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Chunking Pipeline for 5M Documents

**Scenario:** You need to chunk and index 5 million documents (mix of PDF, HTML, Markdown) from an S3 data lake. The pipeline must handle format detection, parsing, adaptive chunking, and incremental updates (50K docs change daily). Design the system.

<details>
<summary>💡 Hint</summary>
Use content-hash for dedup (skip unchanged docs), format-specific parsers, parallel processing, and a state table tracking what's been chunked. Design for idempotency so reruns are safe.
</details>

<details>
<summary>✅ Solution</summary>

```python
import hashlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

class ProductionChunkPipeline:
    """Scalable chunking pipeline with incremental processing."""
    
    def __init__(self, config):
        self.parsers = {
            ".pdf": PdfParser(),
            ".html": HtmlParser(),
            ".md": MarkdownParser(),
            ".txt": PlainTextParser(),
        }
        self.chunker = AdaptiveChunker(target_size=500)
        self.embedder = BatchEmbedder(model="BAAI/bge-base-en-v1.5", batch_size=256)
        self.vector_store = QdrantClient(config.qdrant_url)
        self.state_db = PostgresStateDB(config.pg_url)
    
    def run_full(self, s3_prefix: str, workers: int = 8):
        """Full initial indexing of 5M documents."""
        docs = list_s3_objects(s3_prefix)
        print(f"Found {len(docs)} documents")
        
        # Process in parallel batches
        batch_size = 1000
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            self._process_batch(batch, workers)
            print(f"Progress: {i + len(batch)}/{len(docs)}")
    
    def run_incremental(self):
        """Daily incremental: only process new/changed documents."""
        # Get documents modified since last run
        last_run = self.state_db.get_last_run_timestamp()
        changed_docs = list_s3_objects_modified_after(last_run)
        
        print(f"Incremental: {len(changed_docs)} changed documents")
        self._process_batch(changed_docs, workers=4)
        self.state_db.set_last_run_timestamp()
    
    def _process_batch(self, docs: list[dict], workers: int):
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(self._process_single, doc) for doc in docs]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Error: {e}")  # Log but don't stop pipeline
    
    def _process_single(self, doc: dict) -> int:
        """Process one document: parse → chunk → embed → index."""
        doc_id = doc["key"]
        
        # Check if content changed (skip if unchanged)
        content_hash = doc.get("etag", "")
        stored_hash = self.state_db.get_hash(doc_id)
        if content_hash == stored_hash:
            return 0  # Skip unchanged document
        
        # Download and parse
        content = download_from_s3(doc["bucket"], doc["key"])
        ext = Path(doc["key"]).suffix.lower()
        parser = self.parsers.get(ext)
        if not parser:
            return 0
        
        parsed = parser.parse(content)
        
        # Delete old chunks for this document
        self.vector_store.delete(filter={"doc_id": doc_id})
        
        # Chunk adaptively based on content type
        chunks = self.chunker.chunk(parsed["text"], doc_type=ext)
        
        # Embed in batch
        texts = [c["text"] for c in chunks]
        embeddings = self.embedder.embed(texts)
        
        # Index
        points = [
            {"id": f"{doc_id}_c{i}", "vector": emb, "metadata": {
                "doc_id": doc_id, "source": doc["key"],
                "chunk_idx": i, "total_chunks": len(chunks),
                **c.get("metadata", {})
            }}
            for i, (c, emb) in enumerate(zip(chunks, embeddings))
        ]
        self.vector_store.upsert(points)
        
        # Update state
        self.state_db.set_hash(doc_id, content_hash)
        return len(chunks)

# Deployment:
# - Full run: once (8 workers, ~6 hours for 5M docs on 4x A10G)
# - Incremental: daily cron (50K docs × ~10 chunks/doc = 500K embeddings, ~30 min)
# - Monitoring: track docs processed, chunks created, errors, embedding latency
```

**Key Points:**
- Content-hash dedup avoids re-processing unchanged documents (saves 90%+ on daily runs)
- Format-specific parsers ensure clean text extraction (PDFs need special handling)
- Delete-then-reinsert pattern ensures stale chunks are cleaned up on document updates
- Parallel processing with error isolation (one bad doc doesn't kill the batch)
- State DB tracks what's been processed for reliable incremental updates
- Idempotent: safe to re-run any batch without duplicating data

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Optimizing Chunk Strategy via Evaluation Loop

**Scenario:** Your RAG system has 72% recall@10. The team suspects chunking is the bottleneck (not the embedding model or retrieval algorithm). Design an evaluation-driven approach to find the optimal chunking configuration.

<details>
<summary>💡 Hint</summary>
Create a test set of (query, relevant_document) pairs. Try multiple chunking configs (size, overlap, strategy). Measure recall@10 for each. The config with highest recall wins.
</details>

<details>
<summary>✅ Solution</summary>

```python
import itertools
import numpy as np
from dataclasses import dataclass

@dataclass
class ChunkConfig:
    strategy: str  # "fixed", "recursive", "semantic"
    chunk_size: int
    overlap_pct: float
    
    @property
    def overlap(self):
        return int(self.chunk_size * self.overlap_pct)

def optimize_chunking(
    documents: list[dict],       # [{id, text}]
    eval_set: list[dict],        # [{query, relevant_doc_ids}]
    embed_fn,
    configs: list[ChunkConfig] = None,
) -> dict:
    """Find optimal chunking config by measuring retrieval performance."""
    
    if configs is None:
        # Grid search over common configurations
        configs = [
            ChunkConfig(s, size, overlap)
            for s in ["recursive", "semantic"]
            for size in [256, 400, 600, 800, 1000]
            for overlap in [0.0, 0.1, 0.2]
        ]
    
    results = []
    
    for config in configs:
        # Chunk all documents with this config
        chunker = create_chunker(config)
        all_chunks = []
        chunk_to_doc = {}
        
        for doc in documents:
            chunks = chunker.chunk(doc["text"])
            for i, chunk in enumerate(chunks):
                chunk_id = f"{doc['id']}_c{i}"
                all_chunks.append(chunk["text"])
                chunk_to_doc[chunk_id] = doc["id"]
        
        # Embed all chunks
        chunk_embeddings = np.array(embed_fn(all_chunks))
        
        # Evaluate recall on test set
        recalls = []
        for test_case in eval_set:
            query_emb = np.array(embed_fn([test_case["query"]])[0])
            scores = np.dot(chunk_embeddings, query_emb)
            top_10_idx = np.argsort(scores)[::-1][:10]
            
            retrieved_docs = set(
                chunk_to_doc[f"{documents[i // 100]['id']}_c{i % 100}"]  
                for i in top_10_idx
            )
            relevant = set(test_case["relevant_doc_ids"])
            recall = len(retrieved_docs & relevant) / max(len(relevant), 1)
            recalls.append(recall)
        
        avg_recall = np.mean(recalls)
        results.append({
            "config": config,
            "recall_at_10": avg_recall,
            "total_chunks": len(all_chunks),
            "avg_chunk_size": np.mean([len(c) for c in all_chunks]),
        })
        
        print(f"{config.strategy} size={config.chunk_size} overlap={config.overlap_pct:.0%}: "
              f"recall@10={avg_recall:.3f} ({len(all_chunks)} chunks)")
    
    # Find best config
    best = max(results, key=lambda r: r["recall_at_10"])
    
    return {
        "best_config": best["config"],
        "best_recall": best["recall_at_10"],
        "improvement_vs_worst": best["recall_at_10"] - min(r["recall_at_10"] for r in results),
        "all_results": sorted(results, key=lambda r: -r["recall_at_10"]),
    }

# Typical findings:
# recursive, size=500, overlap=10%: recall@10 = 0.86 (best)
# semantic, size=400, overlap=0%:   recall@10 = 0.84
# recursive, size=1000, overlap=0%: recall@10 = 0.74 (too large)
# recursive, size=200, overlap=20%: recall@10 = 0.79 (too granular)
```

**Key Points:**
- You need an evaluation set: 50-200 (query, relevant_doc_ids) pairs
- Test multiple configs systematically — don't guess
- Chunk size has the most impact (typically 2x more important than strategy choice)
- Overlap provides diminishing returns beyond 15%
- Semantic chunking helps most for multi-topic documents; for single-topic docs, recursive is fine
- Re-run evaluation whenever you change embedding models (optimal chunk size may shift)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Modal Chunking

**Scenario:** Your documents contain text, code blocks, diagrams (as images), and tables. Standard text chunking ignores the non-text elements, causing the RAG system to miss answers that are only in diagrams or tables. Design a multi-modal chunking strategy.

<details>
<summary>💡 Hint</summary>
Extract each modality separately: text → standard chunks, tables → natural language descriptions, images/diagrams → vision model descriptions. Combine with metadata linking to original position.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
from dataclasses import dataclass
from enum import Enum
import fitz  # PyMuPDF

class ChunkType(Enum):
    TEXT = "text"
    CODE = "code"
    TABLE = "table"
    IMAGE = "image"

@dataclass
class MultiModalChunk:
    text: str  # Always text (either original or description)
    chunk_type: ChunkType
    original_content: bytes | str | None  # Original image/table/code
    page: int
    position: int  # Order in document

class MultiModalChunker:
    """Handle documents with text, code, tables, and images."""
    
    def __init__(self):
        self.client = OpenAI()
    
    def chunk_document(self, pdf_path: str) -> list[MultiModalChunk]:
        """Extract and chunk all content types from a PDF."""
        doc = fitz.open(pdf_path)
        chunks = []
        
        for page_num, page in enumerate(doc):
            # Extract text blocks
            text_blocks = page.get_text("blocks")
            for block in text_blocks:
                if block[6] == 0:  # Text block
                    text = block[4].strip()
                    if len(text) > 50:
                        chunks.append(MultiModalChunk(
                            text=text, chunk_type=ChunkType.TEXT,
                            original_content=text, page=page_num + 1,
                            position=len(chunks)
                        ))
            
            # Extract images and describe them
            images = page.get_images()
            for img_info in images:
                xref = img_info[0]
                img_bytes = doc.extract_image(xref)["image"]
                description = self._describe_image(img_bytes)
                
                chunks.append(MultiModalChunk(
                    text=f"[Diagram on page {page_num+1}]: {description}",
                    chunk_type=ChunkType.IMAGE,
                    original_content=img_bytes,
                    page=page_num + 1,
                    position=len(chunks)
                ))
            
            # Extract tables
            tables = self._extract_tables(page)
            for table in tables:
                description = self._describe_table(table)
                chunks.append(MultiModalChunk(
                    text=description,
                    chunk_type=ChunkType.TABLE,
                    original_content=str(table),
                    page=page_num + 1,
                    position=len(chunks)
                ))
        
        return chunks
    
    def _describe_image(self, image_bytes: bytes) -> str:
        """Use vision model to describe a diagram/image."""
        import base64
        b64 = base64.b64encode(image_bytes).decode()
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this technical diagram in detail. Include all labels, relationships, and data shown."},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}}
                ]
            }],
            max_tokens=500
        )
        return response.choices[0].message.content
    
    def _describe_table(self, table_data: list[list[str]]) -> str:
        """Convert table to natural language description for embedding."""
        headers = table_data[0] if table_data else []
        rows = table_data[1:] if len(table_data) > 1 else []
        
        description = f"Table with columns: {', '.join(headers)}. "
        description += f"Contains {len(rows)} rows. "
        
        # Include first few rows as examples
        for row in rows[:3]:
            row_desc = ". ".join(f"{h}: {v}" for h, v in zip(headers, row) if v)
            description += row_desc + ". "
        
        return description
    
    def _extract_tables(self, page) -> list[list[list[str]]]:
        """Extract tables from a PDF page."""
        # Simplified — in production use tabula-py or camelot
        tables = page.find_tables()
        return [table.extract() for table in tables] if tables else []
```

**Key Points:**
- Images/diagrams: describe with vision model → embed the description (text vector)
- Tables: convert to natural language sentences → embeds better than raw CSV format
- Code blocks: keep as separate chunks with language metadata
- All modalities become text before embedding — unified search across all content types
- Cost: vision model calls add ~$0.01 per image (budget for document processing)
- Store original content (image bytes, table data) for display in UI even though only text is embedded

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Real-Time Re-Chunking on Document Updates

**Scenario:** Your knowledge base has 500K documents that update frequently (wiki-style, 10K edits per day). Each edit changes only a few paragraphs. Re-chunking the entire document on every edit wastes compute and causes temporary retrieval gaps (old chunks deleted before new ones indexed). Design an efficient incremental re-chunking system.

<details>
<summary>💡 Hint</summary>
Diff the old and new document at chunk level using content hashes. Only re-embed changed chunks. Use atomic swap (insert new before deleting old) to prevent gaps.
</details>

<details>
<summary>✅ Solution</summary>

```python
import hashlib
from typing import set

class IncrementalReChunker:
    """Efficiently update chunks when documents change, with zero-gap guarantee."""
    
    def __init__(self, chunker, embedder, vector_store, state_store):
        self.chunker = chunker
        self.embedder = embedder
        self.vector_store = vector_store
        self.state_store = state_store  # Stores: doc_id → {chunk_hash: chunk_id}
    
    def on_document_edit(self, doc_id: str, new_text: str) -> dict:
        """Handle a document edit with minimal reprocessing."""
        
        # Step 1: Generate new chunks
        new_chunks = self.chunker.chunk(new_text)
        new_chunk_hashes = {
            hashlib.md5(c["text"].encode()).hexdigest(): c
            for c in new_chunks
        }
        
        # Step 2: Get old chunk state
        old_state = self.state_store.get(doc_id) or {}  # {hash: chunk_id}
        old_hashes = set(old_state.keys())
        new_hashes = set(new_chunk_hashes.keys())
        
        # Step 3: Compute diff
        to_add = new_hashes - old_hashes      # New/changed chunks
        to_remove = old_hashes - new_hashes    # Deleted chunks
        unchanged = old_hashes & new_hashes     # Same content, no action needed
        
        stats = {"added": len(to_add), "removed": len(to_remove), "unchanged": len(unchanged)}
        
        # Step 4: ATOMIC UPDATE (add new BEFORE removing old → no retrieval gap)
        
        # 4a: Embed and insert new chunks
        new_state = {}
        if to_add:
            texts_to_embed = [new_chunk_hashes[h]["text"] for h in to_add]
            embeddings = self.embedder.embed(texts_to_embed)
            
            for h, emb in zip(to_add, embeddings):
                chunk_id = f"{doc_id}_{h[:8]}"
                self.vector_store.upsert([{
                    "id": chunk_id,
                    "vector": emb,
                    "metadata": {"doc_id": doc_id, "version": "new"}
                }])
                new_state[h] = chunk_id
        
        # 4b: Now safe to remove old chunks (new ones are already searchable)
        if to_remove:
            ids_to_delete = [old_state[h] for h in to_remove]
            self.vector_store.delete(ids=ids_to_delete)
        
        # 4c: Keep unchanged chunk IDs
        for h in unchanged:
            new_state[h] = old_state[h]
        
        # Step 5: Update state
        self.state_store.set(doc_id, new_state)
        
        # Compute savings
        total_chunks = len(new_chunks)
        stats["embed_savings_pct"] = len(unchanged) / max(total_chunks, 1) * 100
        stats["cost_saved"] = f"${len(unchanged) * 0.000004:.4f}"  # At OpenAI embedding prices
        
        return stats

# For 10K daily edits where avg 80% of chunks are unchanged:
# Without incremental: 10K docs × 10 chunks × $0.000004 = $0.40/day embeddings
# With incremental: 10K docs × 2 changed chunks × $0.000004 = $0.08/day (80% savings)
# More importantly: zero retrieval gaps during updates
```

**Key Points:**
- Insert-before-delete prevents the gap where a document has no chunks in the index
- Content hashing identifies unchanged chunks without comparing full text
- 80%+ of chunks typically survive small edits → 80% embedding cost savings
- State store (Redis or Postgres) tracks chunk_hash → chunk_id mapping per document
- Idempotent: re-running the same edit produces the same result (safe retries)
- For bulk updates (re-chunking strategy change), fall back to full reprocess with the same insert-before-delete pattern

</details>

</article>
