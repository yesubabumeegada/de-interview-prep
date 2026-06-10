---
title: "Row-Level Security — Senior Deep Dive"
topic: power-bi
subtopic: row-level-security
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [power-bi, row-level-security, interview, senior, advanced]
---

# Row-Level Security — Senior Deep Dive

## RLS Evaluation Internals

### When RLS Filters Are Applied

RLS filters are applied **before** the visual's own filters in the query evaluation order:

```
Query Evaluation Order:
1. RLS filter (from role definition — cannot be overridden by user)
2. Visual/page/report filter
3. Slicer filter
4. Interaction filter (cross-filtering from other visuals)
```

This means a user can never circumvent RLS by manipulating visual filters — RLS is always the outermost constraint.

### RLS and the Formula Engine

RLS DAX filters are evaluated inside the formula engine for each query. The RLS expression runs as part of filter context construction:

```dax
-- This RLS filter runs for every visual query:
[Email] = USERPRINCIPALNAME()

-- Power BI adds this as an equivalent CALCULATE wrapper:
CALCULATE(
    <visual_measure>,
    FILTER(DimEmployee, DimEmployee[Email] = USERPRINCIPALNAME())
)
```

**Performance implication**: Complex RLS filters (nested LOOKUPVALUE, PATH traversal) add overhead to every query. Benchmark RLS impact using DAX Studio and compare query times with and without role context.

---

## Multi-Table RLS Architecture

In large models, RLS may need to filter across multiple tables.

### Challenge: RLS on Multiple Independent Fact Tables

If your model has `FactSales` and `FactBudget` that share a `DimEmployee` dimension, one RLS filter on `DimEmployee` filters both:

```
DimEmployee (RLS filter here) ──→ FactSales
                              ──→ FactBudget
```

But if `FactBudget` relates to a different dimension (`DimDepartment`) that doesn't share a path with `DimEmployee`, the RLS filter does NOT automatically propagate to `FactBudget`.

**Solution: Apply RLS on every independent entry point**

```dax
-- Role: "Salesperson"
-- Table: DimEmployee
[Email] = USERPRINCIPALNAME()

-- Also needed if FactBudget relates only to DimDepartment:
-- Table: DimDepartment
[Department] = LOOKUPVALUE(
    DimEmployee[Department],
    DimEmployee[Email],
    USERPRINCIPALNAME()
)
```

### RLS on Bridge Tables

In many-to-many models using bridge tables, apply RLS to the dimension (not the bridge):

```
DimTerritory (RLS here) ──→ BridgeTerritoryRep ──→ DimSalesPerson
                                                ──→ FactSales
```

```dax
-- Role filter on DimTerritory
[TerritoryID] IN
    SELECTCOLUMNS(
        FILTER(
            BridgeTerritoryRep,
            RELATED(DimSalesPerson[Email]) = USERPRINCIPALNAME()
        ),
        "TID", BridgeTerritoryRep[TerritoryID]
    )
```

---

## Performance Optimization for RLS

### Problem: LOOKUPVALUE in RLS is Slow

`LOOKUPVALUE` performs a row-by-row scan, which can be expensive if the security mapping table is large.

```dax
-- Slow: LOOKUPVALUE for every query
[Region] = LOOKUPVALUE(SecurityMapping[Region], SecurityMapping[Email], USERPRINCIPALNAME())
```

**Fix 1: Pre-filter the security mapping table in Power Query**

In Power Query, add a step to push down a `WHERE email IN (...)` filter. Not always possible without knowing users at refresh time.

**Fix 2: Use a relationship-based approach (faster)**

Instead of LOOKUPVALUE, create a relationship between `DimEmployee` and the fact path, and filter on the natural key:

```dax
-- Faster: simple equality check on a column with a relationship
[EmployeeEmail] = USERPRINCIPALNAME()
-- VertiPaq can use bitmap indexes to evaluate this efficiently
```

**Fix 3: Calculate group caching consideration**

Each unique user identity creates a distinct filter context, preventing VertiPaq cache sharing between users. For large user bases, this can cause cache misses and slower performance.

Mitigation: Group users into coarse roles (e.g., by region) and use static+dynamic hybrid:

```dax
-- Hybrid: coarse static role (limits cache variation) + dynamic fine-grained
-- Role "NorthTeam" has pre-filtered base:
[Region] = "North"
AND [Email] = USERPRINCIPALNAME()
-- Fewer distinct cache entries vs pure dynamic with no pre-filter
```

---

## RLS with Calculation Groups

Calculation groups interact with RLS in subtle ways. RLS is applied first, then calculation items.

**Issue**: If a calculation item changes the filter context (e.g., SAMEPERIODLASTYEAR), the RLS filter still applies to the modified context.

```dax
-- Calculation item: SPLY
CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(DimDate[Date]))
-- RLS filter is still active on DimEmployee even in prior year context
-- Users see their own data from the prior year — correct behavior
```

**Issue**: Calculation groups cannot bypass RLS — this is by design and is a security guarantee.

---

## OLS — Advanced Patterns

### Dynamic Column Visibility via OLS + Calculation Groups

Combine OLS and calculation groups to show different metrics to different roles:

```
Role: "Finance" → can see DimEmployee[Salary] (OLS: Read)
Role: "Manager" → cannot see DimEmployee[Salary] (OLS: None)
```

Measures referencing `[Salary]` will fail for Manager role users. Create role-specific measures:

```dax
-- Works for Finance role (can see Salary column)
Total Payroll = SUM(DimEmployee[Salary])

-- Safe measure for all roles (uses pre-aggregated table without raw salary)
Payroll Summary = SUM(AggPayroll[MonthlyPayroll])
-- AggPayroll doesn't expose individual salaries
```

### Column OLS in Tabular Editor

```json
// Tabular Editor JSON for OLS assignment
{
  "name": "Salary",
  "columnType": "Data",
  "metadataPermission": {
    "roleA": "None",
    "roleB": "Read"
  }
}
```

---

## RLS Audit and Monitoring

### Power BI Activity Log for RLS Violations

Monitor RLS-related activity via the Power BI Activity Log (available via REST API or Microsoft 365 audit):

```powershell
# PowerShell: Get Power BI activity events
$activities = Get-PowerBIActivityEvent -StartDateTime "2024-01-01T00:00:00Z" -EndDateTime "2024-01-31T23:59:59Z"
$rls_events = $activities | Where-Object { $_.ActivityType -eq "ViewReport" }
# Check userId and report context to validate users are accessing expected reports
```

### Validate RLS via XMLA

Use DAX Studio connected to the XMLA endpoint to simulate a user's view:

```
-- In DAX Studio, connect with EffectiveUserName parameter
Server: powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName
-- Connection string with impersonation:
"Provider=MSOLAP;Data Source=...;EffectiveUserName=alice@company.com"

-- Now any DAX query runs as if Alice is the user
EVALUATE SUMMARIZECOLUMNS(DimGeography[Region], "Count", COUNTROWS(FactSales))
-- Returns only North region rows if Alice's RLS filter is "North"
```

---

## RLS Anti-Patterns

### Anti-Pattern 1: RLS Filter on Fact Table

```dax
-- WRONG: Filter on fact table
-- Table: FactSales
[SalesPersonID] = LOOKUPVALUE(DimEmployee[EmployeeKey], DimEmployee[Email], USERPRINCIPALNAME())

-- Problems:
-- 1. Every row in FactSales must be evaluated (slow for large tables)
-- 2. Doesn't propagate to related dimension tables
-- 3. Difficult to maintain

-- RIGHT: Filter on dimension table
-- Table: DimEmployee
[Email] = USERPRINCIPALNAME()
-- Propagates via relationship to FactSales automatically
```

### Anti-Pattern 2: Exposing Security Logic in Reports

```
-- WRONG: Creating a visible "Security Debug" page showing the RLS filter values
-- This exposes security mapping logic to users

-- RIGHT: Use Power BI Service's "View As" testing feature
-- Never expose security internals in production reports
```

### Anti-Pattern 3: Service Principal Bypasses RLS

Service principals used for embedded scenarios bypass RLS by default. You must explicitly set the identity in the embed token:

```javascript
// WITHOUT this, service principal sees ALL data (bypasses RLS):
const token = await client.reports.generateTokenInGroup(workspaceId, reportId, {
    accessLevel: 'View'  // No identity — RLS bypassed!
});

// WITH identity set, RLS applies:
const token = await client.reports.generateTokenInGroup(workspaceId, reportId, {
    accessLevel: 'View',
    identities: [{
        username: endUserEmail,
        roles: ['Salesperson'],
        datasets: [datasetId]
    }]
});
```

---

## RLS Decision Framework

```
Does the security need change per user?
├── No  → Static RLS (one role per fixed value)
└── Yes → Dynamic RLS
    ├── Simple (user → single value)?
    │   └── USERPRINCIPALNAME() equality check
    ├── Complex (lookup table)?
    │   └── LOOKUPVALUE on SecurityMapping table
    ├── Hierarchical (manager sees reports)?
    │   └── PATH + PATHCONTAINS
    └── Need column/table hiding?
        └── OLS via Tabular Editor
```

---

## Summary

- RLS filters are applied **before** all visual filters — users cannot override them
- Apply RLS on **dimension tables** and let relationships propagate to facts
- For complex models, **explicitly filter all independent entry points**
- Use **XMLA with EffectiveUserName** for programmatic RLS testing
- **Service principals bypass RLS** unless you set identity in the embed token
- Complex RLS DAX filters reduce **VertiPaq cache hit rate** — benchmark with DAX Studio
- Use **Tabular Editor** for OLS column and table security configuration

## ⚡ Cheat Sheet

**Data model (Import vs DirectQuery vs Composite)**
```
Import:       data loaded into Power BI memory → fastest queries; stale by refresh schedule
DirectQuery:  queries sent live to source → always current; limited DAX; source load
Composite:    Import for large tables + DirectQuery for real-time; best of both
Dual storage: table can serve as Import or DirectQuery depending on query context
```

**DAX essentials**
```dax
-- Measure (always uses filter context)
Total Revenue = SUM(orders[amount])
Revenue YTD = CALCULATE([Total Revenue], DATESYTD(dates[date]))
Revenue LY  = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dates[date]))
MoM Growth  = DIVIDE([Total Revenue] - [Revenue LY], [Revenue LY])

-- CALCULATE: modifies filter context
Revenue US = CALCULATE([Total Revenue], orders[region] = "US")

-- Iterator functions (row context)
Avg Order = AVERAGEX(orders, orders[amount])
Weighted Score = SUMX(products, products[score] * products[weight]) / SUM(products[weight])

-- Variables (performance + readability)
Margin % = VAR revenue = [Total Revenue]
           VAR cost = [Total Cost]
           RETURN DIVIDE(revenue - cost, revenue)
```

**Row-level security (RLS)**
```dax
-- Static role (in Power BI Desktop)
-- Add table filter: [region] = "US"

-- Dynamic RLS (uses logged-in user)
-- Table filter expression:
[user_email] = USERPRINCIPALNAME()

-- Or via mapping table:
[region] IN VALUES(user_region_map[region])
-- where user_region_map is filtered by USERPRINCIPALNAME()
```

**Power Query M patterns**
```m
// Load from Snowflake
Source = Snowflake.Databases("xy12345.snowflakecomputing.com", "PROD"),
gold = Source{[Name="GOLD"]}[Data],
orders = gold{[Schema="PUBLIC",Item="ORDERS"]}[Data],
// Type columns
typed = Table.TransformColumnTypes(orders,{{"amount", type number}})

// Parameterized query (for incremental refresh)
#"Filtered Rows" = Table.SelectRows(orders, each [order_date] >= RangeStart 
                                              and [order_date] < RangeEnd)
```

**Incremental refresh setup**
```
1. Create parameters: RangeStart (Date/Time), RangeEnd (Date/Time)
2. Filter table in Power Query: order_date >= RangeStart AND < RangeEnd
3. Define incremental refresh policy: Archive 3 years, Refresh last 3 days
4. Publish → Power BI manages partitions automatically
```

**Performance optimization**
```
- Use Import mode for large historical tables (DirectQuery = slower)
- Avoid calculated columns; prefer measures (calculated at query time, not stored)
- Avoid bidirectional relationships (use CROSSFILTER sparingly)
- Star schema: fact table has numeric keys + measures only; dimensions separate
- Aggregations: pre-aggregate large tables; DQ falls through to aggregation table
- Performance Analyzer: shows DAX query time + visual render time per visual
```

**Key interview points**
- DAX filter context vs row context: measures use filter context; calculated columns use row context
- CALCULATE is the most powerful function — changes filter context
- Many-to-many relationships: use bridge table or CROSSFILTER(BOTH) with caution
- Composite models: connect Power BI to Fabric/Databricks via DirectQuery + import local dims
