---
title: "Metadata Management — Intermediate"
topic: data-governance
subtopic: metadata-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [metadata, business-glossary, schema-registry, metadata-api, operational-metadata]
---

# Metadata Management — Intermediate

## Business Glossary

A business glossary standardizes the definition of key business terms to prevent metric inconsistencies:

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class GlossaryTerm:
    term_id: str
    name: str
    definition: str
    domain: str
    owner: str
    synonyms: List[str] = field(default_factory=list)
    related_terms: List[str] = field(default_factory=list)
    linked_columns: List[str] = field(default_factory=list)  # table.column URNs
    approved: bool = False
    approved_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 1

# Example glossary entries
GLOSSARY = [
    GlossaryTerm(
        term_id="GT-001",
        name="Revenue",
        definition=(
            "Total monetary value of completed orders in a given period. "
            "Excludes cancelled orders, refunds, and internal test orders. "
            "Includes taxes and shipping fees."
        ),
        domain="finance",
        owner="cfo@company.com",
        synonyms=["GMV", "Total Revenue", "Gross Revenue"],
        related_terms=["Net Revenue", "Bookings", "ARR"],
        linked_columns=["gold.revenue_daily.total_revenue_usd", "gold.orders.amount"],
        approved=True,
        approved_by="cfo@company.com",
    ),
    GlossaryTerm(
        term_id="GT-002",
        name="Active Customer",
        definition=(
            "A customer who has placed at least one order in the past 90 days. "
            "Orders must be in status='completed'. Excludes test accounts."
        ),
        domain="sales",
        owner="vp-sales@company.com",
        synonyms=["Active User", "Paying Customer"],
        related_terms=["Churned Customer", "New Customer"],
        linked_columns=["gold.customers.is_active", "gold.customer_activity.last_order_date"],
        approved=True,
    ),
]

def find_term(name: str) -> Optional[GlossaryTerm]:
    """Search glossary by name or synonym."""
    name_lower = name.lower()
    for term in GLOSSARY:
        if term.name.lower() == name_lower:
            return term
        if any(s.lower() == name_lower for s in term.synonyms):
            return term
    return None
```

---

## Metadata API Layer

Build an API to expose metadata to all internal tools:

```python
from fastapi import FastAPI, HTTPException
from typing import Optional
import sqlalchemy as sa

app = FastAPI(title="Internal Metadata API", version="1.0.0")

@app.get("/tables/{table_fqn}")
def get_table_metadata(table_fqn: str):
    """Get full metadata for a table (fqn: schema.table)."""
    schema, table = table_fqn.split(".", 1) if "." in table_fqn else (None, table_fqn)
    
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT *
            FROM data_catalog.assets
            WHERE table_name = :table AND (schema_name = :schema OR :schema IS NULL)
        """), {"table": table, "schema": schema}).fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail=f"Table '{table_fqn}' not found in catalog")
    
    return dict(row._mapping)

@app.get("/tables/{table_fqn}/columns")
def get_column_metadata(table_fqn: str, pii_only: bool = False):
    """Get column-level metadata, optionally filtered to PII columns only."""
    with engine.connect() as conn:
        query = """
            SELECT column_name, data_type, description, is_pii, pii_type, sensitivity, tags
            FROM data_catalog.columns
            WHERE table_name = :table
        """
        if pii_only:
            query += " AND is_pii = TRUE"
        
        rows = conn.execute(sa.text(query), {"table": table_fqn}).fetchall()
    
    return [dict(r._mapping) for r in rows]

@app.get("/search")
def search_metadata(q: str, domain: Optional[str] = None, sensitivity: Optional[str] = None, limit: int = 20):
    """Full-text search across all catalog assets."""
    with engine.connect() as conn:
        base_query = """
            SELECT table_name, schema_name, description, owner, sensitivity, domain,
                   ts_rank(search_vector, plainto_tsquery(:q)) AS relevance
            FROM data_catalog.assets
            WHERE search_vector @@ plainto_tsquery(:q)
        """
        params = {"q": q, "limit": limit}
        
        if domain:
            base_query += " AND domain = :domain"
            params["domain"] = domain
        if sensitivity:
            base_query += " AND sensitivity = :sensitivity"
            params["sensitivity"] = sensitivity
        
        base_query += " ORDER BY relevance DESC LIMIT :limit"
        rows = conn.execute(sa.text(base_query), params).fetchall()
    
    return [dict(r._mapping) for r in rows]

@app.get("/glossary/{term_name}")
def get_glossary_term(term_name: str):
    """Look up a business glossary term."""
    term = find_term(term_name)
    if not term:
        raise HTTPException(status_code=404, detail=f"Term '{term_name}' not found in glossary")
    return {
        "term_id": term.term_id,
        "name": term.name,
        "definition": term.definition,
        "synonyms": term.synonyms,
        "linked_columns": term.linked_columns,
    }
```

---

## Operational Metadata Collection

Capture pipeline runtime metadata automatically:

```python
from contextlib import contextmanager
from datetime import datetime
import sqlalchemy as sa
import traceback

class PipelineMetadataCollector:
    """
    Context manager that captures operational metadata for every pipeline run.
    Tracks: start/end time, row counts, errors, input/output tables.
    """
    
    def __init__(self, engine, job_name: str, input_tables: list, output_tables: list):
        self.engine = engine
        self.job_name = job_name
        self.input_tables = input_tables
        self.output_tables = output_tables
        self.run_id = None
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        
        with self.engine.begin() as conn:
            result = conn.execute(sa.text("""
                INSERT INTO pipeline_runs
                (job_name, input_tables, output_tables, status, started_at)
                VALUES (:job, :inputs, :outputs, 'running', :start)
                RETURNING run_id
            """), {
                "job": self.job_name,
                "inputs": self.input_tables,
                "outputs": self.output_tables,
                "start": self.start_time,
            })
            self.run_id = result.scalar()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "failed" if exc_type else "success"
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE pipeline_runs
                SET status = :status, completed_at = NOW(), duration_seconds = :duration,
                    error_message = :error
                WHERE run_id = :run_id
            """), {
                "status": status,
                "duration": duration,
                "error": str(exc_val) if exc_val else None,
                "run_id": self.run_id,
            })
        
        # Update catalog freshness metadata
        if status == "success":
            for table in self.output_tables:
                self._update_freshness_metadata(table)
    
    def record_row_counts(self, counts: dict):
        """Record per-table row counts for this run."""
        with self.engine.begin() as conn:
            for table, count in counts.items():
                conn.execute(sa.text("""
                    UPDATE pipeline_runs
                    SET row_counts = COALESCE(row_counts, '{}')::jsonb || jsonb_build_object(:table, :count)
                    WHERE run_id = :run_id
                """), {"table": table, "count": count, "run_id": self.run_id})
    
    def _update_freshness_metadata(self, table: str):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE data_catalog.assets
                SET last_updated_at = NOW(), last_pipeline_run_id = :run_id, pipeline_status = 'success'
                WHERE table_name = :table
            """), {"run_id": self.run_id, "table": table})

# Usage in Airflow task
def transform_orders(**context):
    with PipelineMetadataCollector(
        engine,
        job_name="transform_orders",
        input_tables=["bronze.orders_raw"],
        output_tables=["silver.orders_cleaned"],
    ) as meta:
        df = read_bronze_orders()
        df_clean = clean_and_validate(df)
        write_silver_orders(df_clean)
        meta.record_row_counts({"silver.orders_cleaned": df_clean.count()})
```

---

## Interview Tips

> **Tip 1:** "What is a business glossary and why is it important?" — A curated dictionary of business terms with agreed definitions. Prevents the "one definition of revenue" problem: Finance says revenue excludes refunds, Sales includes them. The glossary links a term to the exact column/calculation that implements it. Governed by business owners, not engineers.

> **Tip 2:** "What is operational metadata and how do you capture it?" — Metadata generated by running pipelines: last run time, row counts, DQ pass rate, SLA met/missed. Captured automatically by wrapping pipelines in context managers or decorators. Stored in a metadata store and surfaced in the catalog to show data freshness and reliability.

> **Tip 3:** "How would you expose metadata programmatically to internal tools?" — Build an internal metadata API that wraps your catalog (DataHub, Snowflake information_schema, custom store). REST or GraphQL. Enables: IDE plugins that show column descriptions, BI tools that auto-populate field descriptions, custom Slack bots that answer "what is this column?"
