---
title: "Orchestration Frameworks - Intermediate"
topic: rag-llm
subtopic: orchestration-frameworks
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, llm, langchain, lcel, agents, tools, callbacks, error-handling]
---

# Orchestration Frameworks — Intermediate

## LangChain Expression Language (LCEL)

LCEL is LangChain's modern interface — composable, streamable, and async-native:

```python
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableParallel
from langchain_community.vectorstores import Qdrant

# Components
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vectorstore = Qdrant.from_existing_collection(embeddings=embeddings, collection_name="docs", url="http://qdrant:6333")
retriever = vectorstore.as_retriever(search_kwargs={"k": 5})
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# Prompt template
prompt = ChatPromptTemplate.from_template("""Answer based on the context below.
Context: {context}
Question: {question}
Answer:""")

# LCEL chain: pipe operator (|) connects components
rag_chain = (
    RunnableParallel(
        context=retriever | (lambda docs: "\n".join(d.page_content for d in docs)),
        question=RunnablePassthrough()
    )
    | prompt
    | llm
    | StrOutputParser()
)

# Invoke
answer = rag_chain.invoke("How does Spark handle data skew?")

# Stream (token by token)
for chunk in rag_chain.stream("How does Spark handle data skew?"):
    print(chunk, end="", flush=True)

# Async
answer = await rag_chain.ainvoke("How does Spark handle data skew?")

# Batch (multiple queries)
answers = rag_chain.batch(["Question 1?", "Question 2?", "Question 3?"])
```

### Multi-Step LCEL Pipeline

```python
from langchain_core.runnables import RunnableLambda

# Step 1: Query classification
classify_prompt = ChatPromptTemplate.from_template(
    "Classify this query as 'factual', 'conceptual', or 'troubleshooting': {question}"
)
classify_chain = classify_prompt | llm | StrOutputParser()

# Step 2: Route to appropriate retrieval strategy
def route_retrieval(classification: str, question: str):
    if "factual" in classification.lower():
        return retriever.invoke(question)  # Standard vector search
    elif "troubleshooting" in classification.lower():
        return hybrid_retriever.invoke(question)  # Hybrid for error codes
    else:
        return retriever.invoke(question, search_kwargs={"k": 8})  # More context

# Step 3: Full pipeline
full_chain = (
    RunnableParallel(
        classification=classify_chain,
        question=RunnablePassthrough()
    )
    | RunnableLambda(lambda x: {
        "context": route_retrieval(x["classification"], x["question"]),
        "question": x["question"]
    })
    | prompt
    | llm
    | StrOutputParser()
)
```

---

## Agent Architecture with Tools

Agents decide which tools to use and in what order:

```python
from langchain.tools import tool
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# Define tools
@tool
def search_docs(query: str) -> str:
    """Search the documentation knowledge base for relevant information."""
    docs = retriever.invoke(query)
    return "\n\n".join([d.page_content[:500] for d in docs[:3]])

@tool
def run_sql(query: str) -> str:
    """Execute a read-only SQL query against the analytics database.
    Only SELECT queries are allowed."""
    if not query.strip().upper().startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."
    try:
        result = db.execute(query)
        return str(result.fetchmany(10))
    except Exception as e:
        return f"SQL Error: {e}"

@tool
def calculate(expression: str) -> str:
    """Evaluate a mathematical expression. Example: '1024 * 1024 * 4'"""
    try:
        return str(eval(expression, {"__builtins__": {}}, {}))
    except Exception as e:
        return f"Calculation error: {e}"

# Create agent
agent_prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a data engineering assistant with access to tools.
Use search_docs for technical questions about data engineering concepts.
Use run_sql for questions about actual data/metrics.
Use calculate for math operations.
Always explain your reasoning."""),
    MessagesPlaceholder("chat_history", optional=True),
    ("user", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

agent = create_openai_tools_agent(
    llm=ChatOpenAI(model="gpt-4o", temperature=0),
    tools=[search_docs, run_sql, calculate],
    prompt=agent_prompt,
)

executor = AgentExecutor(
    agent=agent,
    tools=[search_docs, run_sql, calculate],
    verbose=True,           # Print reasoning steps
    max_iterations=5,       # Prevent infinite loops
    handle_parsing_errors=True,
)

# The agent decides which tools to use
result = executor.invoke({
    "input": "What was our total revenue last month, and is that above the target mentioned in our docs?"
})
# Agent: 1. run_sql("SELECT SUM(revenue) FROM orders WHERE ...") → $2.3M
#         2. search_docs("revenue target quarterly") → "Target: $2M/month"
#         3. Final answer: "Revenue was $2.3M, 15% above the $2M target"
```

---

## Callbacks and Tracing

Monitor what your chains and agents do:

```python
from langchain_core.callbacks import BaseCallbackHandler
import time

class PerformanceCallback(BaseCallbackHandler):
    """Track latency and token usage for every LLM call."""
    
    def __init__(self):
        self.calls = []
        self.current_start = None
    
    def on_llm_start(self, serialized, prompts, **kwargs):
        self.current_start = time.time()
    
    def on_llm_end(self, response, **kwargs):
        duration = time.time() - self.current_start
        tokens = response.llm_output.get("token_usage", {})
        self.calls.append({
            "duration_ms": duration * 1000,
            "input_tokens": tokens.get("prompt_tokens", 0),
            "output_tokens": tokens.get("completion_tokens", 0),
            "model": response.llm_output.get("model_name", "unknown"),
        })
    
    def on_tool_start(self, serialized, input_str, **kwargs):
        print(f"  Tool: {serialized.get('name', 'unknown')} | Input: {input_str[:100]}")
    
    def summary(self) -> dict:
        return {
            "total_llm_calls": len(self.calls),
            "total_latency_ms": sum(c["duration_ms"] for c in self.calls),
            "total_tokens": sum(c["input_tokens"] + c["output_tokens"] for c in self.calls),
        }

# Use callback
cb = PerformanceCallback()
result = executor.invoke({"input": "..."}, config={"callbacks": [cb]})
print(cb.summary())

# LangSmith (production tracing):
# Set LANGCHAIN_TRACING_V2=true and LANGCHAIN_API_KEY
# All chains automatically traced with full visibility
```

---

## Error Handling and Retries

Production chains need robust error handling:

```python
from langchain_core.runnables import RunnableWithFallbacks
from tenacity import retry, stop_after_attempt, wait_exponential

# Fallback chain: if primary model fails, use backup
primary = ChatOpenAI(model="gpt-4o", temperature=0)
fallback = ChatOpenAI(model="gpt-4o-mini", temperature=0)

robust_llm = primary.with_fallbacks([fallback])  # Auto-fallback on error

# Retry with exponential backoff
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def reliable_rag(question: str) -> str:
    """RAG with retry logic for transient failures."""
    try:
        return rag_chain.invoke(question)
    except Exception as e:
        if "rate_limit" in str(e).lower():
            raise  # Retry on rate limit
        elif "timeout" in str(e).lower():
            raise  # Retry on timeout
        else:
            return "I'm unable to answer this question right now. Please try again."

# Graceful degradation
class RobustRAGChain:
    def invoke(self, question: str) -> dict:
        try:
            # Try full pipeline (retrieval + generation)
            return {"answer": rag_chain.invoke(question), "quality": "full"}
        except Exception as retrieval_error:
            try:
                # Fallback: just ask the LLM without retrieval
                return {"answer": llm.invoke(question).content, "quality": "degraded"}
            except Exception:
                return {"answer": "Service temporarily unavailable.", "quality": "error"}
```

---

## LlamaIndex Query Pipelines

LlamaIndex's newer pipeline-based approach:

```python
from llama_index.core.query_pipeline import QueryPipeline, InputComponent
from llama_index.core.response_synthesizers import TreeSummarize
from llama_index.postprocessor.cohere_rerank import CohereRerank

# Build a query pipeline with re-ranking
pipeline = QueryPipeline(verbose=True)

# Add components
pipeline.add_modules({
    "input": InputComponent(),
    "retriever": index.as_retriever(similarity_top_k=10),
    "reranker": CohereRerank(top_n=5),
    "synthesizer": TreeSummarize(llm=OpenAI(model="gpt-4o-mini")),
})

# Connect them
pipeline.add_link("input", "retriever")
pipeline.add_link("retriever", "reranker")
pipeline.add_link("reranker", "synthesizer")

# Run
response = pipeline.run(input="How does Spark AQE work?")
```

---

## Interview Tips

> **Tip 1:** "Explain LCEL pipe syntax" — LCEL uses `|` to chain components (like Unix pipes). Data flows left to right: `prompt | llm | parser`. Each component's output becomes the next component's input. It supports streaming, async, and batching natively — making it production-friendly.

> **Tip 2:** "How do agents decide which tool to use?" — The LLM receives tool descriptions (name + docstring + parameters) and uses function calling to select which tool to invoke. The agent loop: LLM decides → execute tool → feed result back → LLM decides again → until it has enough info to answer.

> **Tip 3:** "How do you handle LLM failures in production?" — Layered strategy: retry with backoff (transient errors), fallback to cheaper model (if primary fails), graceful degradation (answer without retrieval if vector DB is down), and circuit breaker (stop calling if error rate > threshold). Never let a user see a raw exception.
