---
title: "LLM Guardrails - Scenario Questions"
topic: rag-llm
subtopic: llm-guardrails
content_type: scenario_question
tags: [rag, llm, guardrails, pii, safety, compliance, scenarios, interview]
---

# Scenario Questions — LLM Guardrails

<article data-difficulty="junior">

## 🟢 Junior: PII Redaction Before LLM Call

**Scenario:** Your team built an LLM pipeline that summarizes customer support tickets. A security audit found that ticket text sometimes contains customer emails, phone numbers, and credit card numbers. These are being sent directly to the OpenAI API, violating your data policy. Fix the pipeline to redact PII before sending to the LLM.

<details>
<summary>💡 Hint</summary>
Use the `presidio-analyzer` and `presidio-anonymizer` libraries. Analyze the text for PII entities, then anonymize (replace with placeholder tokens like `[EMAIL]`). The LLM can still summarize de-identified text effectively.
</details>

<details>
<summary>✅ Solution</summary>

```python
# pip install presidio-analyzer presidio-anonymizer spacy
# python -m spacy download en_core_web_lg

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from openai import OpenAI

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()
client = OpenAI()

SENSITIVE_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "CREDIT_CARD",
    "US_SSN",
    "IBAN_CODE",
    "IP_ADDRESS"
]

def redact_pii(text: str) -> tuple[str, bool]:
    """
    Redact PII from text.
    Returns (redacted_text, had_pii_flag).
    """
    results = analyzer.analyze(
        text=text,
        language="en",
        entities=SENSITIVE_ENTITIES,
        score_threshold=0.7  # confidence threshold — avoid false positives
    )

    if not results:
        return text, False

    anonymized = anonymizer.anonymize(
        text=text,
        analyzer_results=results,
        operators={
            "PERSON": OperatorConfig("replace", {"new_value": "[CUSTOMER_NAME]"}),
            "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[EMAIL]"}),
            "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[PHONE]"}),
            "CREDIT_CARD": OperatorConfig("replace", {"new_value": "[CARD_NUMBER]"}),
            "US_SSN": OperatorConfig("replace", {"new_value": "[SSN]"}),
            "DEFAULT": OperatorConfig("replace", {"new_value": "[REDACTED]"})
        }
    )
    return anonymized.text, True

def summarize_support_ticket(ticket_text: str) -> dict:
    """Safely summarize a customer support ticket."""
    # Redact BEFORE sending to LLM
    safe_text, had_pii = redact_pii(ticket_text)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "Summarize this customer support ticket in 2 sentences. Identify the issue type and urgency."
            },
            {"role": "user", "content": safe_text}
        ],
        temperature=0
    )

    return {
        "summary": response.choices[0].message.content,
        "pii_was_redacted": had_pii,
        "original_length": len(ticket_text),
        "processed_length": len(safe_text)
    }

# Test
ticket = """
Hi, I'm John Smith (john.smith@email.com). My credit card 4532-0151-1283-0366
is being charged incorrectly. Please refund $450. You can reach me at 555-867-5309.
"""
result = summarize_support_ticket(ticket)
print(result["summary"])
print(f"PII redacted: {result['pii_was_redacted']}")
# Summary works without any customer PII reaching OpenAI
```

**Key points:**
- Always redact before sending — never rely on the LLM to "ignore" PII
- Use `score_threshold=0.7` to balance sensitivity vs false positives
- Log that PII was detected (the flag) without logging the PII itself
- Use `gpt-4o-mini` for summarization tasks — much cheaper, sufficient quality

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid: Build an LLM Output Validation Pipeline with Retry

**Scenario:** Your team has an LLM pipeline that generates SQL queries from natural language. The LLM sometimes returns invalid JSON, uses wrong column names, or generates unsafe SQL (non-SELECT statements). Build a robust output validation layer that: (1) validates JSON structure, (2) validates SQL safety, (3) retries up to 2 times with error feedback if validation fails, (4) returns a typed result with validation metadata.

<details>
<summary>💡 Hint</summary>
Use Pydantic for schema validation. Catch the validation error, extract the error message, and re-prompt the LLM with "Your previous output had this error: [error]. Please fix it." Track retry count and fail gracefully after max retries.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pydantic import BaseModel, Field, validator
from openai import OpenAI
import json, re
from typing import Optional

client = OpenAI()

# Schema for the LLM's expected output
class SQLGeneration(BaseModel):
    sql: str = Field(..., description="The SQL query")
    explanation: str = Field(..., description="Plain English explanation of what the SQL does")
    tables_used: list[str] = Field(default_factory=list)
    confidence: float = Field(..., ge=0.0, le=1.0)

    @validator("sql")
    def validate_sql_safety(cls, v):
        sql_upper = v.upper().strip()
        # Only allow read operations
        allowed_starts = ("SELECT", "WITH", "EXPLAIN")
        if not any(sql_upper.startswith(kw) for kw in allowed_starts):
            raise ValueError(f"Only SELECT/WITH/EXPLAIN queries allowed. Got: {sql_upper[:30]}")

        # Block dangerous keywords anywhere in query
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "EXEC"]
        for kw in dangerous:
            # Check as whole word to avoid false positives (e.g., "UPDATE" in a column name alias)
            if re.search(r'\b' + kw + r'\b', sql_upper):
                raise ValueError(f"Dangerous keyword '{kw}' found in query")
        return v

    @validator("sql")
    def validate_has_limit(cls, v):
        if "LIMIT" not in v.upper() and "TOP" not in v.upper():
            # Auto-add LIMIT for safety
            v = v.rstrip(";") + "\nLIMIT 1000;"
        return v

SCHEMA_CONTEXT = """
Available tables:
- orders (id INT, customer_id INT, amount DECIMAL, status VARCHAR, created_at TIMESTAMP)
- customers (id INT, name VARCHAR, email VARCHAR, segment VARCHAR, region VARCHAR)
- products (id INT, name VARCHAR, category VARCHAR, price DECIMAL)
- order_items (order_id INT, product_id INT, quantity INT, unit_price DECIMAL)
"""

def generate_sql_with_validation(natural_language_query: str, max_retries: int = 2) -> dict:
    """
    Generate SQL from NL with validation and retry.
    Returns: {sql, explanation, tables_used, confidence, attempts, final_status}
    """
    system_prompt = f"""You are a SQL expert for a Snowflake data warehouse.

{SCHEMA_CONTEXT}

Rules:
- ONLY write SELECT queries
- Always include LIMIT unless query has COUNT(*) or aggregate only
- Return valid JSON with fields: sql, explanation, tables_used, confidence

confidence: 0.9 if you're certain, 0.7 if you're inferring, 0.5 if the request is ambiguous."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": natural_language_query}
    ]

    last_error = None
    for attempt in range(1, max_retries + 2):  # +2: initial + retries
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=600
        )

        raw_content = response.choices[0].message.content

        # Parse JSON
        try:
            raw_dict = json.loads(raw_content)
        except json.JSONDecodeError as e:
            last_error = f"Invalid JSON: {e}"
            # Add error to messages for retry
            messages.append({"role": "assistant", "content": raw_content})
            messages.append({"role": "user", "content": f"Your response was not valid JSON: {e}. Please return valid JSON only."})
            continue

        # Validate schema and SQL safety
        try:
            validated = SQLGeneration(**raw_dict)
            return {
                "status": "success",
                "sql": validated.sql,
                "explanation": validated.explanation,
                "tables_used": validated.tables_used,
                "confidence": validated.confidence,
                "attempts": attempt
            }
        except Exception as e:
            last_error = str(e)
            messages.append({"role": "assistant", "content": raw_content})
            messages.append({
                "role": "user",
                "content": f"Your previous response had a validation error: {last_error}. Please fix it and return corrected JSON."
            })

    # All retries exhausted
    return {
        "status": "failed",
        "sql": None,
        "explanation": None,
        "tables_used": [],
        "confidence": 0.0,
        "attempts": max_retries + 1,
        "error": last_error
    }

# Tests
print(generate_sql_with_validation("Show me top 10 customers by total order value"))
print(generate_sql_with_validation("Delete all orders from last year"))  # Should fail validation
print(generate_sql_with_validation("Revenue by product category for Q1 2024"))
```

**Key interview points:**
- Pydantic validators run in declaration order — put safety checks first
- Feed the exact validation error message back to the LLM — vague "try again" messages don't help
- Use `response_format={"type": "json_object"}` to guarantee parseable JSON from OpenAI
- Track `attempts` in the response for monitoring retry rates in production
- Auto-adding `LIMIT` is safer than rejecting — limits blast radius of runaway queries

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Complete Guardrail System for a GDPR-Regulated LLM Feature

**Scenario:** Your company is launching an LLM-powered feature for EU customers: users paste their marketing emails and the LLM suggests improvements. The data may contain names, emails, and other personal data of third parties (the email recipients). Under GDPR, you must: (1) not store personal data beyond what's needed, (2) not send EU customer data to US LLM providers without adequate safeguards, (3) have audit logs for 2 years, (4) respond to data subject access requests (DSARs) within 30 days. Design and implement the guardrail system including data flow, anonymization strategy, audit logging, and DSAR support.

<details>
<summary>💡 Hint</summary>
Key decisions: (1) Standard Contractual Clauses (SCCs) with OpenAI/Anthropic allow EU-to-US transfer — document this; (2) de-identify before sending, re-identify after for storage; (3) audit logs must be queryable by user_id for DSARs but must not store raw personal data; (4) right to erasure means your audit logs must support "tombstoning" records. Design the schema first, then implement.
</details>

<details>
<summary>✅ Solution</summary>

**Design Decisions:**

1. **Legal basis for transfer**: Use SCCs with OpenAI (they offer a DPA). Never send raw email text — always de-identify first as defense in depth.
2. **De-identification strategy**: Named Entity Recognition for PERSON, EMAIL, PHONE, LOCATION. Replace with stable tokens (SHA-256 of original in a local key store) so the same name maps to the same token consistently within a session.
3. **Audit schema**: Log event metadata, not content. Use SHA-256 hashes of input/output for integrity. Store `user_id` + `session_id` for DSAR queries, with erasure support.
4. **Data retention**: LLM inputs/outputs deleted after 30 days. Audit metadata retained 2 years (legal requirement). User mapping table erasable on DSAR.

```python
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from openai import OpenAI
from pydantic import BaseModel, Field
import hashlib, json, time, uuid, sqlite3
from typing import Optional
from dataclasses import dataclass

# --- Tokenization (stable anonymization within session) ---

class TokenVault:
    """Maps real PII to stable tokens — stored ONLY in-region, NEVER sent to LLM."""

    def __init__(self, db_path: str = "/secure/token_vault.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        # Note: we store the token -> entity_type mapping but NOT the original value
        # The token is SHA-256(original_value + user_salt) — one-way, irreversible by us

    def tokenize(self, value: str, entity_type: str, user_id: str, user_salt: str) -> str:
        """Convert PII to stable, irreversible token."""
        token = f"[{entity_type}_{hashlib.sha256((value + user_salt).encode()).hexdigest()[:8].upper()}]"
        self.conn.execute(
            "INSERT OR IGNORE INTO tokens VALUES (?, ?, ?, ?)",
            (token, entity_type, user_id, time.time())
        )
        self.conn.commit()
        return token

    def erase_user_tokens(self, user_id: str) -> int:
        """GDPR erasure: remove all tokens for a user."""
        cursor = self.conn.execute("DELETE FROM tokens WHERE user_id = ?", (user_id,))
        self.conn.commit()
        return cursor.rowcount

# --- GDPR-Compliant Audit Logger ---

class GDPRAuditLogger:
    def __init__(self, db_path: str = "/secure/gdpr_audit.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_events (
                event_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                input_hash TEXT,
                output_hash TEXT,
                pii_entity_types TEXT,  -- JSON array, no actual PII values
                model TEXT,
                latency_ms REAL,
                erased INTEGER DEFAULT 0,
                created_at REAL NOT NULL
            )
        """)
        # Index for DSAR queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON audit_events(user_id)")

    def log(self, user_id: str, session_id: str, event_type: str,
            input_text: str, output_text: str, pii_types: list[str], model: str, latency_ms: float):
        """Log event. NEVER stores raw PII — hashes only."""
        self.conn.execute("""
            INSERT INTO audit_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            str(uuid.uuid4()), user_id, session_id, event_type,
            hashlib.sha256(input_text.encode()).hexdigest(),
            hashlib.sha256(output_text.encode()).hexdigest(),
            json.dumps(pii_types), model, latency_ms, time.time()
        ))
        self.conn.commit()

    def dsar_report(self, user_id: str) -> dict:
        """GDPR Article 15: Right of Access — return all audit events for a user."""
        rows = self.conn.execute(
            "SELECT event_id, session_id, event_type, pii_entity_types, model, created_at FROM audit_events WHERE user_id = ? AND erased = 0",
            (user_id,)
        ).fetchall()
        return {
            "user_id": user_id,
            "total_interactions": len(rows),
            "events": [
                {
                    "event_id": r[0], "session_id": r[1], "event_type": r[2],
                    "pii_categories_processed": json.loads(r[3]),
                    "model_used": r[4],
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(r[5]))
                } for r in rows
            ]
        }

    def erase_user(self, user_id: str) -> int:
        """GDPR Article 17: Right to Erasure — tombstone records."""
        cursor = self.conn.execute("""
            UPDATE audit_events
            SET user_id = 'ERASED', input_hash = 'ERASED', output_hash = 'ERASED', erased = 1
            WHERE user_id = ?
        """, (user_id,))
        self.conn.commit()
        return cursor.rowcount

# --- Main Pipeline ---

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()
client = OpenAI()

EU_PII_ENTITIES = ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION", "DATE_TIME", "NRP"]

vault = TokenVault()
audit_logger = GDPRAuditLogger()

def improve_marketing_email_gdpr_safe(
    raw_email: str,
    user_id: str,
    session_id: str,
    user_salt: str  # per-user secret for tokenization
) -> dict:
    """GDPR-safe LLM pipeline for EU marketing email improvement."""
    t0 = time.time()

    # Step 1: Detect PII
    phi_results = analyzer.analyze(text=raw_email, language="en", entities=EU_PII_ENTITIES, score_threshold=0.65)
    pii_types = list(set(r.entity_type for r in phi_results))

    # Step 2: Tokenize (stable, irreversible) — defense-in-depth anonymization
    tokenized = raw_email
    for result in sorted(phi_results, key=lambda r: r.start, reverse=True):
        original_value = raw_email[result.start:result.end]
        token = vault.tokenize(original_value, result.entity_type, user_id, user_salt)
        tokenized = tokenized[:result.start] + token + tokenized[result.end:]

    # Step 3: Send tokenized text to LLM (SCCs cover the transfer)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a marketing copy expert. Improve the email below for clarity, engagement, and conversion.
Do not change any tokens in [BRACKETS] — those are placeholders.
Return JSON: {"improved_email": "...", "changes_made": ["list of improvements"], "tone": "professional|friendly|urgent"}"""
            },
            {"role": "user", "content": tokenized}
        ],
        response_format={"type": "json_object"},
        temperature=0.3
    )
    output_raw = response.choices[0].message.content
    output_data = json.loads(output_raw)

    latency = (time.time() - t0) * 1000

    # Step 4: Audit log (hashes only, no content)
    audit_logger.log(
        user_id=user_id,
        session_id=session_id,
        event_type="email_improvement",
        input_text=raw_email,       # hashed inside log()
        output_text=output_raw,     # hashed inside log()
        pii_types=pii_types,
        model="gpt-4o",
        latency_ms=latency
    )

    return {
        "improved_email": output_data.get("improved_email", ""),
        "changes_made": output_data.get("changes_made", []),
        "tone": output_data.get("tone", "unknown"),
        "pii_detected": len(pii_types) > 0,
        "pii_categories": pii_types,
        "session_id": session_id
    }

# DSAR flow
def handle_dsar(user_id: str) -> dict:
    """Handle a GDPR data subject access request."""
    return audit_logger.dsar_report(user_id)

# Erasure flow
def handle_erasure_request(user_id: str) -> dict:
    """Handle a GDPR right-to-erasure request."""
    audit_erased = audit_logger.erase_user(user_id)
    tokens_erased = vault.erase_user_tokens(user_id)
    return {
        "user_id": user_id,
        "audit_records_erased": audit_erased,
        "tokens_erased": tokens_erased,
        "erasure_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

# Test
result = improve_marketing_email_gdpr_safe(
    raw_email="Hi Sarah Johnson (sarah@example.com), join us in Berlin on March 15th for our product launch!",
    user_id="user_eu_12345",
    session_id=str(uuid.uuid4()),
    user_salt="random-per-user-secret"
)
print(result["improved_email"])
print(f"PII detected: {result['pii_categories']}")
```

**Senior design decisions to articulate:**

1. **Irreversible tokenization**: SHA-256(value + user_salt) — we cannot reverse-engineer the original PII from the token even if our system is compromised
2. **Defense in depth**: Anonymize BEFORE sending even though SCCs legally allow sending — reduces breach impact
3. **Audit logs are not backups**: Hashes enable integrity verification; content is never stored in audit logs
4. **DSAR in <1 minute**: Audit table indexed by user_id — O(log n) query, not a full scan
5. **Erasure is tombstoning, not deletion**: Deleted rows can't satisfy audit retention requirements. Overwrite PII fields with `ERASED`, keep metadata for 2-year legal retention
6. **Per-user salt**: Prevents cross-user token correlation attacks

</details>
</article>
