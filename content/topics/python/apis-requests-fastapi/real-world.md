---
title: "Python APIs (requests/FastAPI) - Real-World Production Examples"
topic: python
subtopic: apis-requests-fastapi
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, apis, production, pagination, webhooks, data-quality-api, metadata-service]
---

# Python APIs — Real-World Production Examples

## Pattern 1: Paginated API Data Extraction

Production-grade extractor handling all pagination edge cases:

```python
"""
Complete paginated API extractor with:
- Cursor and offset pagination support
- Automatic rate limit detection and compliance
- Checkpoint/resume for long extractions
- Progress metrics and alerting
"""
import time
import json
import requests
from pathlib import Path
from typing import Iterator, Dict, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

@dataclass
class ExtractionCheckpoint:
    """Enables resuming interrupted extractions."""
    cursor: Optional[str] = None
    offset: int = 0
    total_extracted: int = 0
    last_timestamp: Optional[str] = None
    
    def save(self, filepath: str):
        Path(filepath).write_text(json.dumps(self.__dict__))
    
    @classmethod
    def load(cls, filepath: str) -> "ExtractionCheckpoint":
        if Path(filepath).exists():
            data = json.loads(Path(filepath).read_text())
            return cls(**data)
        return cls()

def extract_paginated_api(
    base_url: str,
    headers: Dict[str, str],
    params: Dict = None,
    page_size: int = 200,
    checkpoint_path: str = None,
    rate_limit_rps: float = 5.0,
) -> Iterator[Dict]:
    """
    Production paginated extraction with resume support.
    Yields individual records from all pages.
    """
    session = requests.Session()
    session.headers.update(headers)
    
    checkpoint = ExtractionCheckpoint.load(checkpoint_path) if checkpoint_path else ExtractionCheckpoint()
    min_interval = 1.0 / rate_limit_rps
    
    logger.info(f"Starting extraction from {base_url} (resuming at offset={checkpoint.offset})")
    
    while True:
        request_params = {**(params or {}), "limit": page_size}
        
        if checkpoint.cursor:
            request_params["cursor"] = checkpoint.cursor
        else:
            request_params["offset"] = checkpoint.offset
        
        # Fetch page with retry
        response = _fetch_page(session, base_url, request_params)
        
        if response is None:
            break
        
        data = response.json()
        records = data.get("results", data.get("data", []))
        
        if not records:
            break
        
        for record in records:
            yield record
            checkpoint.total_extracted += 1
        
        # Update checkpoint
        checkpoint.cursor = data.get("next_cursor", data.get("cursor"))
        checkpoint.offset += len(records)
        checkpoint.last_timestamp = records[-1].get("updated_at")
        
        if checkpoint_path and checkpoint.total_extracted % 10000 == 0:
            checkpoint.save(checkpoint_path)
        
        # Check if last page
        if not checkpoint.cursor and len(records) < page_size:
            break
        
        time.sleep(min_interval)
    
    # Final checkpoint save
    if checkpoint_path:
        checkpoint.save(checkpoint_path)
    
    logger.info(f"Extraction complete: {checkpoint.total_extracted} total records")

def _fetch_page(session, url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                wait = float(response.headers.get("Retry-After", 2 ** (attempt + 1)))
                logger.warning(f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            
            if response.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None

# Usage
for record in extract_paginated_api(
    base_url="https://api.salesforce.com/v2/contacts",
    headers={"Authorization": f"Bearer {token}"},
    params={"modified_since": "2024-01-01"},
    checkpoint_path="/tmp/salesforce_checkpoint.json",
    rate_limit_rps=5.0,
):
    load_to_staging(record)
```

---

## Pattern 2: Webhook Receiver for Pipeline Triggers

```python
"""
Webhook receiver that triggers data pipelines based on upstream events.
Handles: signature verification, deduplication, async processing.
"""
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import datetime
import hashlib
import hmac
import logging

logger = logging.getLogger(__name__)
app = FastAPI(title="Pipeline Trigger Service")

# Deduplication cache
_processed_events: Dict[str, datetime] = {}

class WebhookEvent(BaseModel):
    event_id: str
    event_type: str
    source_system: str
    timestamp: datetime
    payload: Dict

@app.post("/webhooks/data-ready")
async def receive_data_ready(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Receive notification that source data is ready for processing."""
    body = await request.body()
    
    # Verify authenticity
    signature = request.headers.get("X-Webhook-Signature")
    if not _verify_signature(body, signature):
        raise HTTPException(401, "Invalid webhook signature")
    
    event = WebhookEvent.parse_raw(body)
    
    # Idempotency check
    if event.event_id in _processed_events:
        logger.info(f"Duplicate webhook ignored: {event.event_id}")
        return {"status": "already_processed"}
    
    _processed_events[event.event_id] = datetime.utcnow()
    
    # Route to appropriate handler
    handler = _get_handler(event.event_type)
    if handler:
        background_tasks.add_task(handler, event)
        return {"status": "accepted", "event_id": event.event_id}
    
    return {"status": "ignored", "reason": f"Unknown event type: {event.event_type}"}

def _verify_signature(payload: bytes, signature: Optional[str]) -> bool:
    if not signature:
        return False
    expected = hmac.new(WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)

def _get_handler(event_type: str):
    handlers = {
        "file.uploaded": handle_file_upload,
        "table.updated": handle_table_update,
        "pipeline.completed": handle_upstream_complete,
    }
    return handlers.get(event_type)

async def handle_file_upload(event: WebhookEvent):
    """Trigger file ingestion pipeline."""
    file_path = event.payload["s3_path"]
    file_format = event.payload.get("format", "parquet")
    
    logger.info(f"Triggering ingestion for {file_path}")
    await pipeline_orchestrator.trigger(
        dag_id="file_ingestion",
        params={"source_path": file_path, "format": file_format}
    )
```

---

## Pattern 3: Data Quality API

```python
"""
API for querying data quality metrics and triggering quality checks.
Used by: dashboards, alerting systems, CI/CD pipelines.
"""
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime
from enum import Enum

app = FastAPI(title="Data Quality API")

class QualityStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    RUNNING = "running"

class QualityCheckResult(BaseModel):
    check_name: str
    table_name: str
    status: QualityStatus
    metric_value: float
    threshold: float
    execution_time_ms: int
    executed_at: datetime
    details: Optional[str] = None

class TableQualityReport(BaseModel):
    table_name: str
    execution_date: date
    overall_status: QualityStatus
    checks: List[QualityCheckResult]
    total_checks: int
    passed_checks: int
    failed_checks: int

@app.get("/api/quality/tables/{table_name}", response_model=TableQualityReport)
async def get_table_quality(
    table_name: str,
    execution_date: date = Query(default=None),
):
    """Get quality report for a specific table."""
    target_date = execution_date or date.today()
    
    checks = await quality_store.get_checks(table_name, target_date)
    if not checks:
        raise HTTPException(404, f"No quality data for {table_name} on {target_date}")
    
    passed = sum(1 for c in checks if c.status == QualityStatus.PASSED)
    failed = sum(1 for c in checks if c.status == QualityStatus.FAILED)
    
    overall = QualityStatus.PASSED if failed == 0 else QualityStatus.FAILED
    
    return TableQualityReport(
        table_name=table_name,
        execution_date=target_date,
        overall_status=overall,
        checks=checks,
        total_checks=len(checks),
        passed_checks=passed,
        failed_checks=failed,
    )

@app.post("/api/quality/run")
async def trigger_quality_checks(
    table_name: str,
    checks: Optional[List[str]] = None,
    background_tasks: BackgroundTasks = None,
):
    """Trigger quality checks for a table (async execution)."""
    run_id = generate_run_id()
    background_tasks.add_task(
        execute_quality_suite, table_name, checks, run_id
    )
    return {"run_id": run_id, "status": "started"}
```

---

## Pattern 4: Internal Metadata Service

```python
"""
Metadata service for pipeline orchestration.
Tracks: table lineage, column descriptions, freshness SLOs.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime

app = FastAPI(title="Data Catalog Metadata Service")

class ColumnMetadata(BaseModel):
    name: str
    data_type: str
    description: Optional[str] = None
    is_nullable: bool = True
    is_pii: bool = False
    tags: List[str] = Field(default_factory=list)

class TableMetadata(BaseModel):
    table_name: str
    schema_name: str
    description: str
    owner: str
    columns: List[ColumnMetadata]
    upstream_tables: List[str] = Field(default_factory=list)
    downstream_tables: List[str] = Field(default_factory=list)
    freshness_slo_hours: Optional[int] = None
    last_updated: Optional[datetime] = None
    row_count: Optional[int] = None
    tags: List[str] = Field(default_factory=list)

@app.get("/api/catalog/tables/{schema}/{table}", response_model=TableMetadata)
async def get_table_metadata(schema: str, table: str):
    """Get metadata for a specific table."""
    metadata = await catalog_store.get_table(schema, table)
    if not metadata:
        raise HTTPException(404, f"Table {schema}.{table} not found in catalog")
    return metadata

@app.get("/api/catalog/lineage/{schema}/{table}")
async def get_lineage(schema: str, table: str, depth: int = Query(3, ge=1, le=10)):
    """Get upstream/downstream lineage graph."""
    lineage = await catalog_store.get_lineage(schema, table, depth)
    return {
        "table": f"{schema}.{table}",
        "upstream": lineage["upstream"],
        "downstream": lineage["downstream"],
        "depth": depth,
    }

@app.get("/api/catalog/freshness/stale")
async def get_stale_tables():
    """Find tables that haven't been updated within their SLO."""
    all_tables = await catalog_store.get_all_with_slo()
    stale = []
    
    for table in all_tables:
        if table.last_updated and table.freshness_slo_hours:
            hours_since_update = (datetime.utcnow() - table.last_updated).total_seconds() / 3600
            if hours_since_update > table.freshness_slo_hours:
                stale.append({
                    "table": f"{table.schema_name}.{table.table_name}",
                    "slo_hours": table.freshness_slo_hours,
                    "hours_stale": round(hours_since_update, 1),
                    "owner": table.owner,
                })
    
    return {"stale_tables": stale, "count": len(stale)}
```

---

## Interview Tips

> **Tip 1:** The checkpoint/resume pattern shows production maturity. Explain: "Long-running extractions (hours) can fail mid-way. By saving cursor state to disk every 10K records, I can resume exactly where I left off instead of re-extracting everything. This turns a 4-hour extraction failure into a 5-minute recovery."

> **Tip 2:** For webhook design, emphasize reliability guarantees: "I respond with 200 immediately and process asynchronously. Idempotency keys prevent double-processing from retry deliveries. The signature verification prevents spoofed triggers from unauthorized sources."

> **Tip 3:** Building internal APIs (metadata, quality) shows platform thinking. Frame it: "I build self-service APIs so data consumers can check quality, freshness, and lineage without pinging the data team. The freshness SLO endpoint powers automated alerts when tables go stale — proactive rather than reactive incident management."
