---
title: "Teradata - Architecture Senior Deep Dive"
topic: teradata
subtopic: architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [teradata, architecture, internals, hash-map, vprocs, bynet, intelliflex, vantage]
---

# Teradata Architecture — Senior Deep Dive

## The Hash Map: Routing Engine

Teradata maintains a **hash map** — a lookup table mapping row hash values to AMP numbers. This map:
- Is stored in memory on every PE
- Contains 65,536 buckets (2^16) by default
- Each bucket maps to an AMP number
- When AMPs are added/removed, the hash map is **redistributed** (rehashing)

```mermaid
flowchart LR
    Hash["Row Hash: 0xA3F2B1C0"] --> Bucket["Bucket 0xA3F2 (upper 16 bits)"]
    Bucket --> Map["Hash Map Table"]
    Map --> AMP["AMP Assignment: AMP 7"]
```

**Implication:** Adding nodes to a Teradata system requires rehashing — redistributing rows across the new AMP count. This is a significant operation on large tables and is done during maintenance windows.

---

## BYNET Internals

BYNET is a **dual-bus switch fabric** providing:
- **Full bisectional bandwidth** — any AMP can communicate with any other at line speed simultaneously
- **Hardware multicast** — a single message can be delivered to all AMPs simultaneously (used for broadcast joins)
- **Dual paths** — BYNET A and BYNET B for redundancy; traffic load-balances across both

**BYNET bandwidth** is a key scaling bottleneck. Heavy redistribution queries (joins on non-PI columns) saturate BYNET, so minimizing cross-AMP data movement is a core tuning objective.

---

## vproc Architecture Deep Dive

In modern Teradata (Intelliflex, Vantage), physical hardware is decoupled from vproc counts:

- A system might have 10 physical nodes but 200 AMP vprocs (20 per node)
- vprocs are software threads pinned to CPU cores
- Adding nodes allows adding more vprocs without full rehashing (Teradata uses incremental hash redistribution in newer versions)
- **PE vprocs** are stateless — they can be restarted independently
- **AMP vprocs** are stateful — they own data on disk

**vproc failure recovery:**
1. Hot standby absorbs vproc
2. Hash map updated in memory across all PEs
3. Fallback data serves reads
4. System logs the failure in DBC.AMPUsage tables
5. Node replacement triggers a full AMP reclaim (background)

---

## Teradata Vantage Architecture

Teradata Vantage decouples compute from storage (similar to Snowflake) in its cloud form:

```mermaid
flowchart TD
    Client["Client Apps / BI Tools"] --> UES["Unified Ecosystem Layer"]
    UES --> Engine["Analytics Engine<br>(MPP AMP Layer)"]
    Engine --> ObjectStore["Object Store<br>AWS S3 / Azure Blob / GCS"]
    Engine --> LocalDisk["Local NVMe Cache"]
    UES --> QueryGrid["QueryGrid<br>(Federated Query)"]
    QueryGrid --> External["Hive / Spark / Presto<br>External Systems"]
```

**Key Vantage features:**
- **Separated storage:** data lives in object store, compute scales independently
- **QueryGrid:** federated queries across Teradata, Hive, Presto, Spark without data movement
- **Advanced Analytics:** native in-database ML (using Python, R via UDFs)
- **ClearScape Analytics:** Teradata's ML/AI function library running inside the engine

---

## Transaction Management Internals

Teradata uses **multi-statement transactions** with **BTEQ mode (TERA mode)** vs **ANSI mode**:

| Mode | Auto-commit | Error behavior | Rollback scope |
|---|---|---|---|
| **TERA mode** | Each statement auto-commits | Error rolls back only that statement | Statement-level |
| **ANSI mode** | Explicit COMMIT required | Error rolls back entire transaction | Transaction-level |

**Lock types:**
- **Access lock** — allows dirty reads; rarely used
- **Read lock** — shared lock; other reads allowed, no writes
- **Write lock** — exclusive; blocks reads and writes
- **Exclusive lock** — DDL operations; blocks everything

Teradata uses **table-level, partition-level, and row-hash-level locks** depending on the operation.

---

## Clique Design for Maximum Availability

```mermaid
flowchart TB
    subgraph Clique1["Clique 1"]
        N1["Node 1"] 
        N2["Node 2"]
        N3["Node 3"]
        SAN1["SAN Array 1"]
        N1 --- SAN1
        N2 --- SAN1
        N3 --- SAN1
    end
    subgraph Clique2["Clique 2"]
        N4["Node 4"]
        N5["Node 5"]
        N6["Node 6"]
        SAN2["SAN Array 2"]
        N4 --- SAN2
        N5 --- SAN2
        N6 --- SAN2
    end
    Fallback1["Primary rows on AMP in Clique 1"] -.Fallback.-> Fallback2["Mirror rows on AMP in Clique 2"]
```

**Clique sizing rules:**
- Keep cliques small (3–5 nodes) so a SAN failure only impacts a minority of AMPs
- Ensure fallback copies always land in a different clique
- Hotstandby nodes should be one per clique

---

## Performance Pathways: Where Bottlenecks Live

| Bottleneck | Cause | Solution |
|---|---|---|
| AMP CPU skew | Uneven PI distribution (hot AMPs) | Change PI, use NUPI + partitioning |
| BYNET saturation | Heavy redistribution joins | Add statistics, join on PI columns |
| Disk I/O | Full table scans on large tables | PPI, secondary indexes, block-level compression |
| Spool exhaustion | Product joins, large sorts | Collect stats, filter early, increase spool quota |
| Lock contention | Long-running write transactions | Optimize ETL batches, use multiload for bulk ops |

---

## Architecture Decisions: Trade-offs

### UPI vs NUPI at System Level
- UPI enforces uniqueness via global hash collision check — adds overhead on every insert
- NUPI skips uniqueness check — faster inserts, but allows duplicates
- For very high insert throughput, NUPI (with application-enforced uniqueness) is preferred

### Fallback vs RAID-only
- Fallback costs 2x storage but protects against AMP-level failure
- RAID protects disk, not AMP
- Production systems: use both
- Dev/test: fallback often disabled to save storage

### Number of AMPs per Node
- More AMPs = more parallelism, smaller data slices per AMP
- More AMPs = more overhead for small queries (each AMP spins up even for single-row lookups)
- Typical: 4–16 AMPs per physical node

---

## Interview Tips

> **Tip 1:** "How does adding a node affect a Teradata system?" — "Adding nodes requires updating the hash map and redistributing data. In older systems this required a full rehash (significant downtime/maintenance window). Newer Vantage supports incremental redistribution. The new AMPs take over a portion of hash buckets."

> **Tip 2:** "What's the difference between TERA mode and ANSI mode transactions?" — "TERA mode auto-commits each statement and rolls back only the failed statement. ANSI mode requires explicit COMMIT and rolls back the entire transaction on error. ANSI mode is safer for multi-statement ETL."

> **Tip 3:** "How does Teradata handle lock contention?" — "Teradata uses table-level, partition-level, and row-hash-level locks. Long ETL transactions hold write locks blocking readers. Solutions include smaller transaction batches, optimistic locking patterns, and using MULTILOAD which uses row-hash locks rather than table locks."

> **Tip 4:** "How is Teradata Vantage different from on-premise Teradata?" — "Vantage decouples compute from storage, placing data in object stores (S3/Azure Blob) and scaling compute independently. It adds QueryGrid for federated queries across Hive/Spark/Presto, and ClearScape for in-database ML — moving Teradata toward a unified analytics platform."
