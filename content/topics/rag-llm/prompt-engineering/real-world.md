---
title: "Prompt Engineering - Real-World Applications"
topic: rag-llm
subtopic: prompt-engineering
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [prompt-engineering, text-to-sql, data-quality, documentation, anomaly-detection, production-pipeline]
---

# Prompt Engineering — Real-World Applications

## Building LLM-Powered Data Infrastructure

These aren't toy examples — they're production patterns for integrating LLMs into data engineering workflows. Each section is a complete, deployable system.

---

## 1. Prompt Management System for a DE Team

A centralized system for storing, versioning, and deploying prompts across your data platform.

```python
import json
import hashlib
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any
from enum import Enum

class PromptStatus(Enum):
    DRAFT = "draft"
    TESTING = "testing"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"

@dataclass
class ManagedPrompt:
    """A versioned, managed prompt in the team registry."""
    name: str
    version: str
    status: PromptStatus
    owner: str
    system_prompt: str
    user_template: str
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    max_tokens: int = 1000
    tags: list[str] = field(default_factory=list)
    test_cases: list[dict] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    @property
    def content_hash(self) -> str:
        content = f"{self.system_prompt}|{self.user_template}|{self.model}|{self.temperature}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]

class PromptManagementSystem:
    """Team-wide prompt registry with lifecycle management."""
    
    def __init__(self, storage_dir: str = "./prompts"):
        self.storage = Path(storage_dir)
        self.storage.mkdir(parents=True, exist_ok=True)
        self._index: dict[str, list[ManagedPrompt]] = {}
        self._load_index()
    
    def _load_index(self):
        """Load all prompts from storage."""
        for file in self.storage.glob("*.json"):
            with open(file) as f:
                data = json.load(f)
                prompt = ManagedPrompt(**{k: v for k, v in data.items() 
                                         if k in ManagedPrompt.__dataclass_fields__})
                prompt.status = PromptStatus(data["status"])
                self._index.setdefault(prompt.name, []).append(prompt)
    
    def register(self, prompt: ManagedPrompt) -> str:
        """Register a new prompt version."""
        filepath = self.storage / f"{prompt.name}_v{prompt.version}.json"
        data = {**prompt.__dict__, "status": prompt.status.value}
        filepath.write_text(json.dumps(data, indent=2))
        self._index.setdefault(prompt.name, []).append(prompt)
        return prompt.content_hash
    
    def get_production(self, name: str) -> ManagedPrompt | None:
        """Get the current production version of a prompt."""
        versions = self._index.get(name, [])
        prod = [p for p in versions if p.status == PromptStatus.PRODUCTION]
        return prod[-1] if prod else None
    
    def promote(self, name: str, version: str, to_status: PromptStatus):
        """Promote a prompt through the lifecycle."""
        versions = self._index.get(name, [])
        target = next((p for p in versions if p.version == version), None)
        if not target:
            raise ValueError(f"Version {version} not found for {name}")
        
        # If promoting to production, demote current production
        if to_status == PromptStatus.PRODUCTION:
            for p in versions:
                if p.status == PromptStatus.PRODUCTION:
                    p.status = PromptStatus.DEPRECATED
        
        target.status = to_status
        # Save updated status
        filepath = self.storage / f"{name}_v{version}.json"
        data = {**target.__dict__, "status": target.status.value}
        filepath.write_text(json.dumps(data, indent=2))
    
    def render(self, name: str, **variables) -> list[dict]:
        """Render the production prompt with variables."""
        prompt = self.get_production(name)
        if not prompt:
            raise ValueError(f"No production version for '{name}'")
        
        from string import Template
        return [
            {"role": "system", "content": Template(prompt.system_prompt).safe_substitute(**variables)},
            {"role": "user", "content": Template(prompt.user_template).safe_substitute(**variables)},
        ]

# Usage
pms = PromptManagementSystem("./team_prompts")

# Register a new prompt
sql_prompt = ManagedPrompt(
    name="text_to_sql",
    version="2.1",
    status=PromptStatus.TESTING,
    owner="data-platform-team",
    system_prompt="You are a $dialect SQL expert. Schema:\n$schema\nGenerate only SELECT queries.",
    user_template="Convert to SQL: $question",
    model="gpt-4o-mini",
    tags=["sql", "generation", "self-serve"],
    test_cases=[
        {"input": {"question": "total orders"}, "expected_contains": "SELECT COUNT"},
        {"input": {"question": "revenue by month"}, "expected_contains": "GROUP BY"},
    ]
)
pms.register(sql_prompt)
pms.promote("text_to_sql", "2.1", PromptStatus.PRODUCTION)
```

---

## 2. LLM-Powered Data Quality Validation

Use LLMs to validate data against complex business rules that are hard to express in code.

```python
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import Literal
import instructor
import json

client = instructor.from_openai(OpenAI())

class BusinessRuleViolation(BaseModel):
    record_id: str
    rule_violated: str
    field_name: str
    current_value: str
    severity: Literal["critical", "warning", "info"]
    explanation: str
    suggested_fix: str | None = None

class DataQualityReport(BaseModel):
    total_records: int
    violations: list[BusinessRuleViolation]
    pass_rate: float = Field(ge=0.0, le=1.0)
    summary: str

class LLMDataValidator:
    """Validate data against business rules using LLM understanding."""
    
    def __init__(self, business_rules: str, schema: str):
        self.business_rules = business_rules
        self.schema = schema
        self.system_prompt = f"""You are a data quality analyst. Validate records against these rules:

SCHEMA:
{schema}

BUSINESS RULES:
{business_rules}

IMPORTANT:
- Only flag genuine violations of the stated rules
- Do NOT flag valid edge cases as violations
- Be specific about which rule is violated
- Provide actionable fix suggestions"""
    
    def validate_batch(self, records: list[dict], batch_id: str = "") -> DataQualityReport:
        """Validate a batch of records."""
        return client.chat.completions.create(
            model="gpt-4o-mini",
            response_model=DataQualityReport,
            max_retries=2,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": f"Validate these records (batch: {batch_id}):\n{json.dumps(records, indent=2)}"}
            ],
        )
    
    def validate_stream(self, records_iter, batch_size: int = 20) -> list[DataQualityReport]:
        """Validate records in batches from a stream."""
        reports = []
        batch = []
        batch_num = 0
        
        for record in records_iter:
            batch.append(record)
            if len(batch) >= batch_size:
                report = self.validate_batch(batch, f"batch_{batch_num}")
                reports.append(report)
                batch = []
                batch_num += 1
        
        if batch:  # Remaining records
            report = self.validate_batch(batch, f"batch_{batch_num}")
            reports.append(report)
        
        return reports

# Production usage
validator = LLMDataValidator(
    business_rules="""
    1. order_amount must be positive and less than $1,000,000
    2. shipping_date must be after order_date
    3. If status is 'delivered', delivery_date must not be null
    4. customer_tier 'enterprise' cannot have orders under $100
    5. Discount percentage cannot exceed 50% unless approved_by is not null
    6. International orders (country != 'US') must have customs_declaration = true
    """,
    schema="""orders(
        order_id INT, customer_id INT, order_amount DECIMAL,
        order_date DATE, shipping_date DATE, delivery_date DATE,
        status VARCHAR, customer_tier VARCHAR, discount_pct DECIMAL,
        approved_by VARCHAR, country VARCHAR, customs_declaration BOOL
    )"""
)

# Validate suspicious records flagged by basic checks
suspicious_records = [
    {"order_id": 5001, "customer_id": 42, "order_amount": 50.00,
     "customer_tier": "enterprise", "status": "delivered", "delivery_date": None,
     "country": "UK", "customs_declaration": False},
]
report = validator.validate_batch(suspicious_records)
print(f"Pass rate: {report.pass_rate:.0%}")
for v in report.violations:
    print(f"  [{v.severity}] {v.record_id}: {v.rule_violated} - {v.explanation}")
```

---

## 3. Text-to-SQL with Schema Context

Production-grade natural language to SQL system with validation and execution.

```python
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator
import instructor
import sqlparse
from typing import Literal

client = instructor.from_openai(OpenAI())

class GeneratedSQL(BaseModel):
    """Validated SQL output with metadata."""
    query: str
    tables_used: list[str]
    estimated_rows: str = Field(description="Estimate: 'few', 'hundreds', 'thousands', 'millions'")
    requires_index: bool
    explanation: str
    
    @field_validator("query")
    @classmethod
    def validate_sql_safety(cls, v: str) -> str:
        upper = v.upper().strip()
        if not upper.startswith(("SELECT", "WITH")):
            raise ValueError("Only SELECT/WITH queries allowed")
        forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "EXEC"]
        for keyword in forbidden:
            # Check it's not in a string literal or comment
            if keyword in upper.split("--")[0].split("/*")[0]:
                # More precise check: is it a SQL keyword or part of a name?
                import re
                if re.search(rf'\b{keyword}\b', upper):
                    raise ValueError(f"Forbidden keyword: {keyword}")
        return v

class TextToSQLEngine:
    """Production text-to-SQL with schema awareness and validation."""
    
    def __init__(self, schema_ddl: str, dialect: str = "PostgreSQL"):
        self.schema_ddl = schema_ddl
        self.dialect = dialect
        self.system_prompt = f"""You are an expert {dialect} query generator.

DATABASE SCHEMA:
{schema_ddl}

RULES:
1. Generate ONLY SELECT or WITH (CTE) queries
2. Use ONLY tables and columns defined in the schema
3. Add appropriate WHERE clauses for performance
4. Use CTEs for complex multi-step queries
5. Include comments for complex logic
6. Prefer explicit JOINs over implicit (comma-separated)
7. Always alias columns in the output for clarity"""
    
    def generate(self, question: str, context: str = "") -> GeneratedSQL:
        """Generate SQL from natural language."""
        user_msg = f"Question: {question}"
        if context:
            user_msg += f"\n\nAdditional context: {context}"
        
        return client.chat.completions.create(
            model="gpt-4o",
            response_model=GeneratedSQL,
            max_retries=3,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_msg}
            ],
        )
    
    def generate_with_examples(self, question: str, examples: list[dict]) -> GeneratedSQL:
        """Generate SQL with few-shot examples for consistency."""
        messages = [{"role": "system", "content": self.system_prompt}]
        
        for ex in examples:
            messages.append({"role": "user", "content": f"Question: {ex['question']}"})
            messages.append({"role": "assistant", "content": json.dumps({
                "query": ex["sql"],
                "tables_used": ex.get("tables", []),
                "estimated_rows": ex.get("rows", "hundreds"),
                "requires_index": ex.get("needs_index", False),
                "explanation": ex.get("explanation", ""),
            })})
        
        messages.append({"role": "user", "content": f"Question: {question}"})
        
        return client.chat.completions.create(
            model="gpt-4o",
            response_model=GeneratedSQL,
            max_retries=3,
            messages=messages,
        )
    
    def validate_and_format(self, generated: GeneratedSQL) -> str:
        """Format and validate the generated SQL."""
        formatted = sqlparse.format(
            generated.query,
            reindent=True,
            keyword_case="upper",
            identifier_case="lower"
        )
        return formatted

# Usage
engine = TextToSQLEngine(
    schema_ddl="""
    CREATE TABLE customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        tier VARCHAR(20) CHECK (tier IN ('free', 'pro', 'enterprise')),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        amount DECIMAL(10,2),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE order_items (
        item_id SERIAL PRIMARY KEY,
        order_id INT REFERENCES orders(order_id),
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10,2)
    );
    """,
    dialect="PostgreSQL"
)

result = engine.generate(
    "What are the top 10 enterprise customers by total revenue this quarter?"
)
print(engine.validate_and_format(result))
print(f"\nTables: {result.tables_used}")
print(f"Estimated rows: {result.estimated_rows}")
```

---

## 4. Automated Data Documentation Generation

Generate and maintain documentation for tables, columns, and pipelines.

```python
from openai import OpenAI
from pydantic import BaseModel, Field
import instructor
import json

client = instructor.from_openai(OpenAI())

class ColumnDoc(BaseModel):
    name: str
    description: str = Field(min_length=10)
    data_type: str
    business_meaning: str
    example_values: list[str]
    is_pii: bool
    quality_notes: str | None = None

class TableDoc(BaseModel):
    table_name: str
    description: str = Field(min_length=20)
    business_domain: str
    update_frequency: str
    primary_key: list[str]
    row_count_estimate: str
    columns: list[ColumnDoc]
    common_queries: list[str]
    related_tables: list[str]
    data_quality_notes: str

class DocumentationGenerator:
    """Auto-generate data documentation from schema + sample data."""
    
    def __init__(self):
        self.system_prompt = """You are a data documentation specialist. Generate clear, 
accurate documentation for data engineers and analysts. 

Focus on:
- Business context (what does this data represent?)
- Practical usage (how is this typically queried?)
- Data quality considerations (what to watch out for?)
- Relationships to other tables

Be specific and practical. Avoid generic descriptions."""
    
    def document_table(self, table_name: str, ddl: str, 
                       sample_data: list[dict], context: str = "") -> TableDoc:
        """Generate documentation for a table."""
        prompt = f"""Generate documentation for this table:

Table: {table_name}
DDL: {ddl}

Sample data (first 10 rows):
{json.dumps(sample_data[:10], indent=2, default=str)}

{f'Additional context: {context}' if context else ''}

Analyze the schema and sample data to infer business meaning, identify PII columns, 
and suggest common query patterns."""
        
        return client.chat.completions.create(
            model="gpt-4o",
            response_model=TableDoc,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt}
            ],
        )
    
    def generate_lineage_description(self, pipeline_code: str, 
                                      source_tables: list[str], 
                                      target_table: str) -> str:
        """Generate human-readable data lineage documentation."""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate concise data lineage documentation."},
                {"role": "user", "content": f"""Describe the data flow:
Sources: {source_tables}
Target: {target_table}
Transform logic:
```python
{pipeline_code}
```

Write a clear, 2-3 paragraph description of:
1. What data flows from where
2. What transformations are applied
3. What the target table represents after transformation"""}
            ],
            temperature=0.3,
        )
        return response.choices[0].message.content
    
    def to_markdown(self, doc: TableDoc) -> str:
        """Convert documentation to markdown format."""
        md = f"# {doc.table_name}\n\n"
        md += f"{doc.description}\n\n"
        md += f"**Domain:** {doc.business_domain}  \n"
        md += f"**Update Frequency:** {doc.update_frequency}  \n"
        md += f"**Primary Key:** {', '.join(doc.primary_key)}  \n"
        md += f"**Estimated Rows:** {doc.row_count_estimate}\n\n"
        md += "## Columns\n\n"
        md += "| Column | Type | Description | PII |\n|--------|------|-------------|-----|\n"
        for col in doc.columns:
            pii = "🔴 Yes" if col.is_pii else "No"
            md += f"| {col.name} | {col.data_type} | {col.business_meaning} | {pii} |\n"
        md += f"\n## Common Queries\n\n"
        for q in doc.common_queries:
            md += f"- {q}\n"
        md += f"\n## Related Tables\n\n"
        for t in doc.related_tables:
            md += f"- `{t}`\n"
        md += f"\n## Data Quality Notes\n\n{doc.data_quality_notes}\n"
        return md

# Usage
doc_gen = DocumentationGenerator()
table_doc = doc_gen.document_table(
    table_name="fact_orders",
    ddl="CREATE TABLE fact_orders (order_id INT, customer_id INT, amount DECIMAL, ...)",
    sample_data=[{"order_id": 1, "customer_id": 42, "amount": 99.99}],
    context="This is our main orders fact table, updated hourly from Shopify webhook events."
)
print(doc_gen.to_markdown(table_doc))
```

---

## 5. Anomaly Explanation System

When your monitoring detects anomalies, use LLMs to generate explanations.

```python
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import Literal
import instructor

client = instructor.from_openai(OpenAI())

class AnomalyExplanation(BaseModel):
    anomaly_id: str
    likely_cause: str
    confidence: Literal["high", "medium", "low"]
    evidence: list[str]
    recommended_action: str
    requires_immediate_attention: bool
    related_systems: list[str]

class AnomalyExplainer:
    """Generate human-readable explanations for detected data anomalies."""
    
    def __init__(self, system_context: str):
        self.system_prompt = f"""You are a data pipeline operations expert. 
When presented with an anomaly, analyze the evidence and provide a likely explanation.

SYSTEM CONTEXT:
{system_context}

Be specific and actionable. Rank explanations by likelihood.
Consider: upstream system changes, deployment timing, seasonal patterns, 
data drift, schema changes, and infrastructure issues."""
    
    def explain(self, anomaly: dict, recent_events: list[dict] = None) -> AnomalyExplanation:
        """Generate explanation for a detected anomaly."""
        prompt = f"""Explain this anomaly:

ANOMALY:
- Metric: {anomaly['metric']}
- Expected: {anomaly['expected']}
- Actual: {anomaly['actual']}
- Deviation: {anomaly.get('deviation', 'N/A')}
- Time: {anomaly['timestamp']}
- Affected table: {anomaly.get('table', 'N/A')}

RECENT SYSTEM EVENTS:
{json.dumps(recent_events or [], indent=2)}"""
        
        return client.chat.completions.create(
            model="gpt-4o",
            response_model=AnomalyExplanation,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt}
            ],
        )

# Usage
explainer = AnomalyExplainer(
    system_context="""
    - E-commerce platform with hourly ETL
    - Sources: Shopify (orders), Stripe (payments), Segment (events)
    - Pipeline: Airflow on AWS, data warehouse is Snowflake
    - Typical daily volume: 50K orders, 200K events
    """
)

explanation = explainer.explain(
    anomaly={
        "metric": "daily_order_count",
        "expected": 50000,
        "actual": 12000,
        "deviation": "-76%",
        "timestamp": "2024-03-15T08:00:00Z",
        "table": "fact_orders"
    },
    recent_events=[
        {"time": "2024-03-14T22:00:00Z", "event": "Shopify API rate limit increased"},
        {"time": "2024-03-15T02:00:00Z", "event": "Deploy: webhook handler v2.3"},
        {"time": "2024-03-15T03:00:00Z", "event": "Airflow task 'extract_orders' duration 3x normal"},
    ]
)
```

---

## 6. Production Prompt Pipeline (End-to-End)

Complete pipeline: template store → variable injection → LLM call → validation → output.

```python
from openai import OpenAI
from pydantic import BaseModel
from typing import Any, Callable
from dataclasses import dataclass, field
import instructor
import json
import time
import logging

logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    max_retries: int = 3
    timeout_seconds: float = 30.0
    cache_enabled: bool = True
    fallback_model: str = "gpt-4o"

@dataclass 
class PipelineResult:
    success: bool
    data: Any = None
    error: str | None = None
    model_used: str = ""
    latency_ms: float = 0.0
    tokens_used: int = 0
    cached: bool = False
    retries: int = 0

class PromptPipeline:
    """Production-grade prompt execution pipeline."""
    
    def __init__(self, config: PipelineConfig, prompt_store: PromptManagementSystem):
        self.config = config
        self.store = prompt_store
        self.client = instructor.from_openai(OpenAI())
        self.cache = {}  # Replace with Redis in production
        self._metrics = {"calls": 0, "failures": 0, "cache_hits": 0}
    
    def execute(self, prompt_name: str, variables: dict, 
                response_model: type[BaseModel],
                pre_processors: list[Callable] = None,
                post_processors: list[Callable] = None) -> PipelineResult:
        """Execute the full prompt pipeline."""
        start = time.time()
        
        try:
            # 1. Pre-processing (sanitize input, add context)
            if pre_processors:
                for processor in pre_processors:
                    variables = processor(variables)
            
            # 2. Render prompt from template store
            messages = self.store.render(prompt_name, **variables)
            
            # 3. Check cache
            if self.config.cache_enabled:
                cache_key = self._cache_key(messages)
                if cache_key in self.cache:
                    self._metrics["cache_hits"] += 1
                    return PipelineResult(
                        success=True, data=self.cache[cache_key],
                        cached=True, latency_ms=(time.time() - start) * 1000
                    )
            
            # 4. LLM call with retries
            result = None
            retries = 0
            model = self.config.model
            
            for attempt in range(self.config.max_retries):
                try:
                    result = self.client.chat.completions.create(
                        model=model,
                        response_model=response_model,
                        max_retries=1,  # Instructor internal retry
                        messages=messages,
                    )
                    break
                except Exception as e:
                    retries += 1
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == self.config.max_retries - 2:
                        model = self.config.fallback_model  # Escalate to better model
            
            if result is None:
                self._metrics["failures"] += 1
                return PipelineResult(success=False, error="All retries exhausted")
            
            # 5. Post-processing (additional validation, enrichment)
            if post_processors:
                for processor in post_processors:
                    result = processor(result)
            
            # 6. Cache result
            if self.config.cache_enabled:
                self.cache[cache_key] = result
            
            latency = (time.time() - start) * 1000
            self._metrics["calls"] += 1
            
            return PipelineResult(
                success=True, data=result,
                model_used=model, latency_ms=latency,
                retries=retries
            )
        
        except Exception as e:
            self._metrics["failures"] += 1
            return PipelineResult(
                success=False, error=str(e),
                latency_ms=(time.time() - start) * 1000
            )
    
    def _cache_key(self, messages: list[dict]) -> str:
        import hashlib
        content = json.dumps(messages, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()
    
    @property
    def metrics(self) -> dict:
        total = self._metrics["calls"] + self._metrics["failures"]
        return {
            **self._metrics,
            "success_rate": self._metrics["calls"] / max(total, 1),
            "cache_hit_rate": self._metrics["cache_hits"] / max(total, 1),
        }

# Complete production example
pipeline = PromptPipeline(
    config=PipelineConfig(model="gpt-4o-mini", cache_enabled=True),
    prompt_store=pms  # From earlier PromptManagementSystem
)

class SQLResult(BaseModel):
    query: str
    confidence: float

def sanitize_input(variables: dict) -> dict:
    """Remove potential injection from user input."""
    if "question" in variables:
        variables["question"] = variables["question"][:500]  # Limit length
    return variables

result = pipeline.execute(
    prompt_name="text_to_sql",
    variables={"dialect": "PostgreSQL", "schema": "...", "question": "total revenue"},
    response_model=SQLResult,
    pre_processors=[sanitize_input],
)

if result.success:
    print(f"SQL: {result.data.query}")
    print(f"Model: {result.model_used}, Latency: {result.latency_ms:.0f}ms")
```

---

## Interview Tips

> **Tip 1:** When asked "how would you use LLMs in a data pipeline?", lead with the validation use case. It's the most practical and defensible — LLMs checking complex business rules that are hard to codify.

> **Tip 2:** Text-to-SQL is a hot interview topic. Always mention: schema context in the prompt, output validation (sqlparse), read-only enforcement, and few-shot examples from query logs.

> **Tip 3:** The documentation generator shows you think about developer experience, not just data movement. Self-documenting data infrastructure is a senior-level concern.

> **Tip 4:** The full pipeline pattern (template → inject → call → validate → output) shows production thinking. Mention caching, retries, fallback models, and observability.
