---
title: "End-to-End Case Studies — Senior Deep Dive"
topic: system-design
subtopic: end-to-end-case-studies
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, case-study, financial, healthcare, platform, senior]
---

# End-to-End Case Studies — Senior Deep Dive

## Case Study: Financial Services — Trade Processing Platform

**Requirements:**
- Process 2M trades/day (equity, fixed income, derivatives)
- Zero data loss (regulatory requirement)
- FINRA/SEC audit trail: all data changes traceable for 7 years
- T+1 settlement reports by 8 PM EST same day
- Reconciliation between internal systems and prime broker
- PII protection (trader IDs, account numbers masked)

**Architecture:**

```
Ingestion (zero data loss):
  Trading systems → FIX protocol → Kafka (RF=3, acks=all, min.insync.replicas=2)
  Prime broker (end of day batch) → SFTP → S3 → event trigger
  
  Kafka: infinite retention on trades.raw topic (7 years = regulatory)
  Alternative: Kafka → S3 within 24 hours (cheaper long-term than Kafka storage)

Audit Layer (immutable, 7-year retention):
  All raw events → S3 + Write-Once Object Lock (WORM compliance)
  S3 Object Lock: COMPLIANCE mode (even admins can't delete for 7 years)
  Metadata: MD5 hash of each file stored in a manifest (tamper detection)
  
  Data Vault model in Snowflake:
    Hubs: hub_trade, hub_account, hub_instrument
    Links: link_trade_execution, link_trade_account
    Satellites: sat_trade_details (with load_dts, end_dts for full history)
    All inserts, no updates → full audit trail

Processing (Spark + dbt):
  Bronze: raw events, S3, partitioned by trade_date
  Silver: normalized, validated, PII tokenized
    Tokenization: trader_id → HMAC-SHA256 token (reversible by authorized team only)
    Vault: PII mapping in HashiCorp Vault (prod access: compliance team only)
  Gold: position aggregations, P&L calculations, margin requirements

Settlement Reports (T+1 by 8 PM):
  Spark batch job at 6 PM: process all completed trades for the day
  dbt: settlement_instructions, position_summary, cash_movements
  Output: Snowflake + encrypted SFTP to clearinghouse
  SLA monitoring: alert if any table not ready by 7:30 PM

Reconciliation:
  Compare internal position vs prime broker position daily
  Tolerance: exact match (no tolerance for financial positions)
  Breaks (mismatches) → automatically create reconciliation tickets in Jira
  Root cause analysis: trace trade lifecycle via Data Vault satellites

DR (RPO=0, RTO=15 min):
  Active-passive: primary in us-east-1, standby in us-west-2
  Kafka MirrorMaker 2: replicates all topics to DR region
  Snowflake: Business Critical tier with failover (automatic replication)
  Failover procedure: Route53 health check → redirect to DR in 5 min
```

---

## Case Study: Healthcare Data Platform (HIPAA)

**Requirements:**
- 50M patient records from 200 hospital systems
- HIPAA compliance: PHI (Protected Health Information) must be protected
- Population health analytics: aggregate statistics for research
- Clinical decision support: real-time patient risk scores
- Interoperability: FHIR R4 standard

**Architecture:**

```
PHI Protection Strategy (HIPAA Safe Harbor):
  18 PHI identifiers must be removed or protected:
    Name, SSN, DOB, address, phone, email, medical record number, etc.
  
  Approach A: De-identification (for analytics)
    Remove all 18 identifiers → Safe Harbor compliant
    Can be shared with researchers without BAA
  
  Approach B: Pseudonymization (for operations)
    Replace PHI with opaque tokens (patient_token = SHA256(patient_id + salt))
    Mapping table in HSM (Hardware Security Module) — air-gapped from analytics
    Authorized clinical systems can de-tokenize for individual patient queries

Data Classification:
  PHI: all 18 HIPAA identifiers → encrypt at rest (AES-256) + in transit (TLS 1.3)
  PII: names, emails → tokenized
  Sensitive: diagnoses, medications → column-level encryption in Snowflake
  Public: aggregate statistics (no PHI, no PII)

Ingestion:
  Hospital EHR systems → HL7 FHIR R4 API → API Gateway → Kafka
  Lab results (HL7 v2 messages) → Mirth Connect → FHIR adapter → Kafka
  DICOM imaging → separate pipeline (very large files, not Kafka)

Processing:
  Bronze: FHIR JSON, partitioned by ingestion_date + hospital_id
    Encrypted at rest (S3 SSE-KMS with hospital-specific keys)
    Access log: every read audited (HIPAA requirement)
  Silver: normalized FHIR resources, PHI tokenized, anomalies flagged
  
  De-identified Gold: PHI removed, for research and population analytics
    Patient demographics: age bucket (not DOB), zip code prefix (not full zip)
    Available to: research partners, public health agencies
  
  Clinical Gold: PHI tokenized, for clinical decision support
    Risk scores computed per patient token
    Available to: authorized clinical systems only (with BAA)

FHIR R4 API (serving):
  FastAPI on Kubernetes
  Auth: OAuth2 + SMART on FHIR (standard healthcare auth protocol)
  Patient search: authorized users can lookup by patient token
  Auditing: every FHIR API call logged with user, timestamp, patient token
```

---

## How to Answer "Design X" in 45 Minutes

```
Template for complex systems (adapt to any domain):

Opening (2 min):
  "Before I start designing, let me clarify a few things..."
  Ask: scale, latency, consumers, SLAs, existing systems, compliance

High-level (5 min):
  "The architecture has three main components: ingestion, processing, and serving."
  Draw boxes: Sources → Kafka/S3 → Processing → DW/Delta → BI/API

Ingestion details (8 min):
  Source types: databases (CDC), events (Kafka), files (S3), APIs
  Volume math: events/sec × message size = MB/sec → pick Kafka partition count
  Durability: RF=3, acks=all for critical data

Processing details (10 min):
  Bronze/Silver/Gold (medallion)
  Batch vs streaming (justify choice based on latency requirement)
  Key transformations: SCD, dedup, schema enforcement
  DQ checks between layers

Serving (5 min):
  Who reads the data? (BI, ML, operational API)
  What format/performance do they need?
  Caching layer if needed (Redis for low-latency lookups)

Non-functional requirements (10 min):
  Fault tolerance: checkpoints, retries, DLQ
  Scalability: how to 10× the data
  Cost: spot instances, partitioning for query cost, data lifecycle
  Monitoring: freshness, volume, quality alerts

Tradeoffs (5 min):
  "The main tradeoff in my design is X vs Y. I chose X because..."
  "If budget were no object, I would add..."
  "The main risk is... mitigated by..."
```

---

## Interview Tips

> **Tip 1:** "How do you design for regulatory compliance in a DE system?" — Three pillars: (1) Immutability: write-once storage (S3 Object Lock WORM, Delta append-only mode) ensures data can't be tampered with. (2) Auditability: log every read and write with user, timestamp, and resource. Use Data Vault or event sourcing for full change history. (3) Access control: column-level encryption (Snowflake Dynamic Data Masking), row-level security by data classification, principle of least privilege for all service accounts.

> **Tip 2:** "How do you handle PII in a data engineering system?" — Classify data at ingestion: which fields are PII? Apply at the earliest possible stage (Bronze → Silver transition): (1) Drop if not needed, (2) Tokenize (replace with opaque ID, mapping in secure vault) for operational use, (3) Generalize (age bucket instead of exact DOB) for analytics. Never let raw PII reach analytical layers (Gold/reporting). Audit: log every access to tables with PII. Annual PII audit to find unmasked columns that crept in.

> **Tip 3:** "How do you design a data platform that can serve both ML and BI teams?" — Both teams share Bronze and Silver layers (single source of truth). Gold layer diverges: BI gold = pre-aggregated star schema tables, optimized for SQL joins and dashboards. ML gold = feature store with point-in-time correct feature vectors, partitioned by entity_id for efficient feature retrieval. Shared: data catalog (lineage, documentation), quality tests (both teams benefit from clean data), and governance (both must follow PII policies).
