---
title: "Power BI Composite Models - Fundamentals"
topic: power-bi
subtopic: composite-models
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, composite-models, directquery, aggregations]
---

# Power BI Composite Models — Fundamentals

## What Is a Composite Model?

A **Composite Model** in Power BI is a dataset that combines two or more storage modes in a single model. Before composite models, a dataset had to be either fully **Import** or fully **DirectQuery**. Composite models remove that restriction, letting you mix modes table by table.

**Introduced:** Power BI Desktop (2018), now stable and widely supported.

---

## Storage Modes Explained

| Storage Mode | Where Data Lives | Query Performance | Data Freshness |
|-------------|-----------------|-------------------|----------------|
| **Import**  | Cached in-memory (VertiPaq) | Fastest (sub-second) | As fresh as last refresh |
| **DirectQuery** | Queried live from source | Varies (source-dependent, often seconds) | Always real-time |
| **Dual**    | Both cached AND passed through | Import speed when possible, DQ fallback | Flexible |

---

## Why Composite Models Exist

**The problem with pure Import:** Large fact tables (billions of rows) cannot fit in memory, and refresh windows become too long.

**The problem with pure DirectQuery:** Every visual fires a SQL query to the source. Complex reports with many visuals generate dozens of queries simultaneously, overwhelming the source database and producing slow user experiences.

**The composite solution:** Import small, slow-changing dimensions (products, customers, calendar) and use DirectQuery for large, frequently updated fact tables. The optimizer uses the cached dimensions to push efficient queries to the DirectQuery source.

```
┌────────────────────────────────────────────────────────────┐
│  Composite Model                                           │
│                                                            │
│  DimProduct   (Import)  ← small, 50K rows, slow-changing  │
│  DimCustomer  (Import)  ← small, 200K rows                │
│  DimCalendar  (Import)  ← very small, 3,650 rows          │
│                                                            │
│  FactSales    (DirectQuery) ← 2 billion rows, real-time   │
└────────────────────────────────────────────────────────────┘
```

---

## Basic Setup in Power BI Desktop

1. Connect to your first data source (e.g., an Azure SQL Database in DirectQuery mode for FactSales).
2. Go to **Model view**, right-click a dimension table, select **Storage mode** → **Import**.
3. Power BI warns that switching to Import is irreversible in this session — click OK.
4. The model is now composite.

---

## The Dual Storage Mode

**Dual mode** tables are stored in both Import cache AND can be queried via DirectQuery. Power BI decides at query time which path to use:

- If the query involves only Dual + Import tables → uses the Import cache (fast).
- If the query involves any DirectQuery table → the Dual table participates via DirectQuery to avoid cross-source inconsistencies.

**Best practice:** Set shared dimension tables (especially date tables) to **Dual** so they can join efficiently with both Import and DirectQuery fact tables.

```
DimCalendar (Dual) → works with FactSales (Import) via cache
DimCalendar (Dual) → works with FactTransactions (DirectQuery) via live query
```

---

## Limitations to Know

- DirectQuery tables must have a **single data source** per table (you can't DirectQuery two different databases in the same table).
- Composite models **cannot** mix sources freely — relationships between DirectQuery tables from **different** sources require one side to be Import or Dual.
- Calculated tables and calculated columns that reference DirectQuery tables have restrictions.
- Some DAX functions behave differently or are unsupported in DirectQuery mode (e.g., `SUMMARIZECOLUMNS` with certain filters).

---

## Common Junior Interview Questions

**Q: What is the key difference between Import and DirectQuery storage modes?**  
A: Import loads and compresses data into Power BI's in-memory VertiPaq engine — queries are very fast but data is only as fresh as the last refresh. DirectQuery sends live queries to the source database on every visual interaction — data is always current but performance depends on the source system.

**Q: What is a composite model?**  
A: A Power BI dataset that uses a mix of Import and DirectQuery storage modes across different tables in the same model. It lets you cache small dimension tables for speed while querying large fact tables live.

**Q: What is Dual storage mode?**  
A: A hybrid mode where the table is stored in the Import cache AND can be queried via DirectQuery. Power BI chooses the most efficient path at query time based on the storage mode of related tables in the same query.

**Q: Can you always use DirectQuery for any data source?**  
A: No. DirectQuery is only supported for specific connectors (SQL Server, Azure SQL, Snowflake, BigQuery, etc.). File-based sources (Excel, CSV) and many API connectors do not support DirectQuery.
