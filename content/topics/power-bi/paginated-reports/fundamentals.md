---
title: "Paginated Reports — Fundamentals"
topic: power-bi
subtopic: paginated-reports
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, paginated-reports, interview, fundamentals]
---

# Paginated Reports — Fundamentals

## What Are Paginated Reports?

**Paginated reports** are a type of report in Power BI designed to be printed or exported as multi-page documents. They are called "paginated" because they are formatted to fit neatly across multiple pages — every row of a large table will appear, spanning as many pages as needed, without any data being hidden.

They come from **SQL Server Reporting Services (SSRS)** and use the **RDL (Report Definition Language)** file format.

**Key difference from standard Power BI reports:**

| Aspect | Standard Power BI Report | Paginated Report |
|---|---|---|
| Purpose | Interactive data exploration | Operational/printable documents |
| Data display | Sampled/aggregated for visualization | ALL rows shown (no limit) |
| Page layout | Dynamic, responsive | Pixel-perfect, fixed |
| Export | Limited | PDF, Excel, CSV, Word, TIFF |
| Ideal for | Dashboards, KPIs, trends | Invoices, statements, audit logs |

---

## When to Use Paginated Reports

Use paginated reports when:

- Users need to **print or export** a complete dataset
- The output must look exactly the same every time (pixel-perfect)
- The report is a **document**, not an exploration tool (invoice, statement, letter)
- The data could be **thousands of rows** that must all be shown
- You need **complex headers and footers** on every page
- The output format is **PDF, Excel, or Word**

**Examples:**
- Customer invoices
- Sales orders
- Audit trail reports
- Regulatory compliance reports
- Employee pay stubs
- Inventory reports with thousands of line items

---

## SSRS Heritage

Paginated reports are based on SQL Server Reporting Services (SSRS), which Microsoft has shipped since 2000. The key concepts are identical:

- **RDL format** — XML-based report definition file
- **Report Builder** — the desktop authoring tool
- **Tablix** — the core data region (combines table, matrix, and list concepts)
- **Parameters** — user-driven filtering
- **Expressions** — Visual Basic-style formulas for dynamic values

---

## Report Builder

**Report Builder** is the desktop tool for creating paginated reports. It is a standalone application downloaded from Microsoft.

**Key sections in Report Builder:**

```
Report Builder Interface:
┌─────────────────────────────────────────────────────────┐
│ Ribbon (Insert, Home, View, Run)                         │
├──────────┬────────────────────────────────┬─────────────┤
│ Report   │                                │ Properties  │
│ Data     │    Design Surface              │ Pane        │
│ Pane     │    (Drag & drop regions here)  │             │
│          │                                │             │
├──────────┴────────────────────────────────┴─────────────┤
│ Parameters (bottom)                                      │
└─────────────────────────────────────────────────────────┘
```

---

## Core Concept: Tablix

The **Tablix** data region is the primary way to display tabular data. It can function as:

- **Table**: Static columns, dynamic rows — like a traditional report
- **Matrix**: Dynamic rows AND columns — like a pivot table
- **List**: Free-form — each row can contain other report items

```
Table example:
┌──────────────┬─────────────┬──────────────┐
│ Product      │ Quantity    │ Revenue      │  ← Header row (static)
├──────────────┼─────────────┼──────────────┤
│ [Product]    │ [Quantity]  │ [Revenue]    │  ← Detail row (repeats per row)
├──────────────┼─────────────┼──────────────┤
│              │ [Sum(Qty)]  │ [Sum(Rev)]   │  ← Footer row (totals)
└──────────────┴─────────────┴──────────────┘

Matrix example (pivot):
           │ Q1       │ Q2       │ Q3       │ Q4       │ Total
───────────┼──────────┼──────────┼──────────┼──────────┼──────
North      │ $10,000  │ $12,000  │ $11,500  │ $14,000  │$47,500
South      │  $8,500  │  $9,200  │  $8,900  │ $10,100  │$36,700
───────────┼──────────┼──────────┼──────────┼──────────┼──────
Total      │ $18,500  │ $21,200  │ $20,400  │ $24,100  │$84,200
```

---

## Data Sources in Paginated Reports

Paginated reports can connect to:

| Source | Notes |
|---|---|
| **Azure SQL / SQL Server** | Most common, native support |
| **SQL Server Analysis Services (SSAS)** | Both Tabular and Multidimensional |
| **Power BI dataset** | Connect to a Power BI semantic model (Premium) |
| **Oracle, MySQL, PostgreSQL** | Via OLEDB/ODBC |
| **REST API** | Custom data extension |
| **Excel** | Via OLEDB |

**Data Connection types:**
- **Embedded**: Connection string stored in the report (portable but insecure for passwords)
- **Shared data source**: Connection defined once in Power BI Service, referenced by multiple reports

---

## Parameters

Parameters allow users to filter the report before it runs.

### Basic Parameter

```
Parameter: Start Date
  → Type: DateTime
  → Prompt: "Start Date:"
  → Default: =Today().AddMonths(-1)

Parameter: End Date
  → Type: DateTime
  → Prompt: "End Date:"
  → Default: =Today()
```

The dataset query uses parameters:

```sql
SELECT OrderID, CustomerName, OrderDate, Amount
FROM dbo.Orders
WHERE OrderDate BETWEEN @StartDate AND @EndDate
ORDER BY OrderDate
```

### Cascading Parameters

A cascading parameter's available values depend on a prior parameter's selection:

```
Region parameter (first)
    ↓ (selected value filters the next parameter's query)
State parameter (available states = states in selected region)
    ↓
City parameter (available cities = cities in selected state)
```

---

## Expressions

Expressions are Visual Basic-style formulas used throughout paginated reports.

```vb
' Text expression
="Invoice for: " & Fields!CustomerName.Value & " — " & Format(Now(), "MMMM dd, yyyy")

' Conditional color expression
=IIF(Fields!Amount.Value > 10000, "Green", "Red")

' Sum expression (aggregate)
=Sum(Fields!Amount.Value)

' Running total
=RunningValue(Fields!Amount.Value, Sum, Nothing)

' Page number
=Globals!PageNumber & " of " & Globals!TotalPages

' Conditional visibility (show/hide row)
=IIF(Parameters!ShowDetails.Value = "Yes", False, True)
```

---

## Export Formats

| Format | Use Case |
|---|---|
| PDF | Archival, printing, email attachment |
| Excel | Further analysis, data extraction |
| CSV | Data export for other systems |
| Word | Document editing |
| TIFF | Image archival |
| XML | Data exchange |
| HTML | Web display |

---

## Premium Capacity Requirement

Publishing paginated reports to Power BI Service requires **Power BI Premium** or **Premium Per User (PPU)**.

- Standard Power BI Pro workspaces cannot host paginated reports
- Premium Per User (PPU) license allows paginated reports in a PPU workspace
- The report author needs Report Builder to create the .rdl file

---

## Summary

- Paginated reports are for **print/export documents** with all rows shown
- Based on **SSRS/RDL format** — use **Report Builder** to create
- The core data region is a **Tablix** (table, matrix, or list)
- Support **parameters** (including cascading) for user-driven filtering
- Connect to **SQL Server, SSAS, Power BI datasets, and more**
- Export to **PDF, Excel, CSV, Word** and other formats
- Require **Power BI Premium** or **PPU** to publish to the Service
