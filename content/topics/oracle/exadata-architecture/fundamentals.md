---
title: "Exadata Architecture — Fundamentals"
topic: oracle
subtopic: exadata-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, exadata, smart-scan, storage-cells, offloading, hcc]
---

# Exadata Architecture — Fundamentals


## 🎯 Analogy

Think of Exadata like a purpose-built F1 car for Oracle: Smart Scan offloads filtering and column projection to the storage cells (not the database servers), so only the relevant data crosses the network — 10-100x less I/O for analytical queries.

---
## What Is Exadata?

Oracle Exadata is an engineered system (hardware + software + Oracle Database) optimized for extreme database performance. It combines:
- **Database servers** (compute): run Oracle Database instances
- **Storage cells** (Exadata Smart Flash + Exadata Storage Software): intelligent storage with processing power
- **InfiniBand network**: ultra-low latency interconnect between DB servers and storage cells

The key innovation: storage cells are intelligent — they can execute SQL operations (filtering, aggregation) and return only matching rows to the database servers, dramatically reducing network I/O.

---

## Core Components

```
┌────────────────────────────────────────────────────────────┐
│                  Exadata Database Machine                  │
│                                                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │ DB Server 1 │  │ DB Server 2 │  │ DB Server N │       │
│  │  (Oracle DB)│  │  (Oracle DB)│  │  (Oracle DB)│       │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘       │
│         │                │                │               │
│  ═══════════════ InfiniBand Network ═══════════════       │
│         │                │                │               │
│  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐       │
│  │ Storage     │  │ Storage     │  │ Storage     │       │
│  │ Cell 1      │  │ Cell 2      │  │ Cell N      │       │
│  │ (iDB + ASM) │  │ (iDB + ASM) │  │ (iDB + ASM) │       │
│  └─────────────┘  └─────────────┘  └─────────────┘       │
└────────────────────────────────────────────────────────────┘
```

| Component | Role |
|---|---|
| Database Servers | Run Oracle DB instances, application connections, buffer cache |
| Storage Cells | Store data on flash/disk, run Smart Scan, I/O Resource Manager |
| InfiniBand | 100 Gb/s interconnect (10× faster than typical SAN) |
| Exadata Storage Software (iDB) | Cell software that enables Smart Scan, offloading, HCC |

---

## Smart Scan — The Key Innovation

Smart Scan moves SQL filtering and column projection **down to the storage cells**, returning only matching data to the database servers:

```sql
-- Regular query (non-Exadata or Smart Scan not triggered):
-- 1. Storage transfers ALL blocks for ORDERS to database server
-- 2. Database server filters WHERE amount > 10000
-- → Full I/O for the table even if 1% of rows match

-- With Smart Scan on Exadata:
-- 1. Database sends predicate (amount > 10000) to storage cells
-- 2. Each storage cell evaluates the predicate on its portion of the data
-- 3. Storage cells return ONLY matching rows + projected columns
-- → Dramatically reduced network I/O between cells and DB servers

SELECT customer_id, SUM(amount) 
FROM orders 
WHERE amount > 10000 
  AND order_date > DATE '2023-01-01'
GROUP BY customer_id;
```

**When does Smart Scan activate?**
- Table must be stored on Exadata Smart Storage (ASM diskgroup on cell disks)
- Query uses **full table scan** or **full partition scan** (not index scan)
- Session-level parameter: `ALTER SESSION SET CELL_OFFLOAD_PROCESSING = TRUE;` (default)

---

## Offloading Features

Beyond Smart Scan, Exadata offloads more operations to storage cells:

| Feature | What Is Offloaded to Cells |
|---|---|
| Smart Scan | Predicate filtering, column projection |
| Storage Indexes | Automatically maintained min/max per 1MB region; skip regions that can't match predicate |
| Bloom Filter Offload | Hash join bloom filters pushed to storage cells |
| Columnar Flash Cache | Columnar in-memory format on Exadata Smart Flash Cache |
| I/O Resource Management | Storage-level IOPS limits per database/consumer group |

---

## Hybrid Columnar Compression (HCC)

HCC compresses data by storing values columnarly in Compression Units — achieves 10-50× compression ratio vs row format:

```sql
-- Compression levels (higher compression → slower DML)
-- QUERY HIGH: best for DW read workloads (10-15× compression)
ALTER TABLE sales MOVE 
COMPRESS FOR QUERY HIGH;

-- ARCHIVE HIGH: cold archive data (15-50× compression)
ALTER TABLE sales_archive MOVE 
COMPRESS FOR ARCHIVE HIGH;

-- OLTP: minimal row-level compression (2-4×, suitable for hot data)
ALTER TABLE orders MOVE 
COMPRESS FOR OLTP;

-- Check compression per segment
SELECT segment_name, compress_for, 
       ROUND(bytes/1024/1024/1024,2) size_gb
FROM dba_segments
WHERE owner = 'SALES_SCHEMA'
ORDER BY bytes DESC;
```

**HCC caveats:** HCC is only available on Exadata and ZFS storage. QUERY/ARCHIVE compressed rows can't be updated in-place — an update decompresses the entire Compression Unit, increasing I/O. Best for read-mostly or append-only data.

---

## Flash Cache and Smart Flash Log

```sql
-- View Exadata Smart Flash Cache utilization (from storage cells)
-- Run on all cells via dcli, or query v$cell_flash_cache_statistics from DB:
SELECT cell_name, status, hitcount, misscount, 
       ROUND(hitcount*100.0/(hitcount+misscount),1) hit_pct
FROM v$cell_flash_cache_statistics;

-- Objects cached in flash (large objects that wouldn't fit in buffer cache)
SELECT owner, object_name, flash_cache_usage_mb
FROM v$bh_smart_flash
ORDER BY flash_cache_usage_mb DESC
FETCH FIRST 20 ROWS ONLY;
```

---


## ▶️ Try It Yourself

```sql
-- Verify Smart Scan is being used (Exadata-specific wait events)
SELECT event, COUNT(*)
FROM v$session_event
WHERE event LIKE 'cell%'
GROUP BY event
ORDER BY COUNT(*) DESC;

-- Check Smart Scan offload statistics
SELECT name, value
FROM v$sysstat
WHERE name IN (
    'cell physical IO bytes eligible for predicate offload',
    'cell physical IO bytes saved by storage index',
    'cell physical IO interconnect bytes returned by smart scan'
);

-- Force Smart Scan (for testing)
ALTER SESSION SET "_serial_direct_read" = TRUE;

-- Disable Smart Scan (if troubleshooting)
ALTER SESSION SET "_serial_direct_read" = FALSE;
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is Smart Scan and why is it important?" — Smart Scan pushes SQL predicates (WHERE clause filters and SELECT column projections) down to the storage cells. Instead of sending all data blocks to the database server for filtering, only matching rows and needed columns travel over the InfiniBand network. For large scans that return a small subset of rows/columns, this reduces network I/O by 90%+, directly translating to faster query response.

> **Tip 2:** "What is a Storage Index on Exadata?" — Storage Indexes are automatically maintained min/max value summaries for every 1MB region of data on disk, for columns referenced in query predicates. When a Smart Scan runs, the cell checks: can this region possibly contain rows matching the predicate? If the predicate value is outside the min/max range for a region, that entire region is skipped — no I/O at all. They're automatic (no DBA action needed) and invisible to the optimizer.

> **Tip 3:** "What is Hybrid Columnar Compression?" — HCC reorganizes data columnarly within a Compression Unit (typically 32KB-1MB). Since column values are similar (e.g., thousands of 'COMPLETE' status values), they compress far better than row-format storage. Achieves 10-50× compression vs basic row storage. Only available on Exadata and compatible Oracle storage. Best for bulk-loaded, read-mostly data — DML on HCC-compressed rows forces decompression.
