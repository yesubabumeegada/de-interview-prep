---
title: "Dataflows — Scenarios"
topic: power-bi
subtopic: dataflows
content_type: scenario_question
tags: [power-bi, dataflows, scenarios, interview]
---

# Dataflows — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Why Use a Dataflow Instead of Transforming in Each Dataset?

**Scenario:** Your team has 10 Power BI reports, each connecting to the same SQL Server customer table and applying the same 15 transformation steps (column renaming, type changes, null handling, segment classification). A new intern asks: "Why don't we just copy-paste the Power Query steps into each dataset?" How do you explain the benefit of a shared dataflow?

<details>
<summary>💡 Hint</summary>

Think about what happens when the transformation logic needs to change, or when the source system changes a column name. Also consider how many times the source database is queried.

</details>

<details>
<summary>✅ Solution</summary>

**The problem with copy-paste transformation in each dataset:**

1. **Maintenance nightmare**: When business rules change (e.g., the company adds a new customer segment), you must update 10 separate datasets manually. Miss one → inconsistent data.

2. **Source overload**: Each dataset queries the SQL Server independently. With 10 datasets refreshing at 2 AM, the source receives 10 concurrent full table scans.

3. **Data inconsistency**: Teams may apply the same transformation slightly differently. "Enterprise" customers in Sales reports might not match "Enterprise" customers in Finance reports.

4. **No reuse**: Every new report must re-implement the same 15 steps from scratch.

**With a shared dataflow:**

```
SQL Server ← queried ONCE
    ↓
Dataflow: DimCustomer_Foundation
  (15 transformation steps, applied once)
  (Refreshes at 1 AM)
    ↓
Dataset A → connects to DimCustomer_Foundation
Dataset B → connects to DimCustomer_Foundation
...
Dataset J → connects to DimCustomer_Foundation
  (Each dataset reads from the already-transformed dataflow storage)
```

**Concrete benefits:**

| Benefit | Without Dataflow | With Dataflow |
|---|---|---|
| Transformation logic | Duplicated 10 times | Defined once |
| Source queries at refresh | 10 parallel scans | 1 scan |
| Segment logic change | Update 10 datasets | Update 1 dataflow |
| New report | Re-implement 15 steps | Connect to dataflow |
| Data consistency | Different teams, different logic | One canonical definition |

**When NOT to use a dataflow:**

If you have only 1-2 reports and no expectation of growth, a dataflow adds overhead (another object to manage, another refresh to schedule). Keep it simple for small-scale scenarios.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Dataflow Refresh Fails for One Entity While Others Succeed

**Scenario:** You have a dataflow with 5 entities: 3 standard entities (from SQL, SharePoint, API) and 2 computed entities (built from the standard ones). The API entity fails its refresh due to a timeout. The SQL and SharePoint entities succeed. What happens to the computed entities, and how do you design the dataflow to be resilient to this type of failure?

<details>
<summary>💡 Hint</summary>

Think about entity dependency. What does a computed entity need to succeed? Consider error handling patterns (try...otherwise) and whether optional data sources should block critical computations.

</details>

<details>
<summary>✅ Solution</summary>

**What happens by default:**

When the API entity fails:
1. The API entity has no data (empty or error state)
2. Any computed entity that **depends on the API entity** also fails or produces incorrect results
3. The computed entities dependent on SQL + SharePoint (not the API) refresh successfully
4. The overall dataflow status shows "Partial success" or "Failed" depending on configuration

**This is a problem if:** The computed entity joins ALL three standard entities, and a failed API entity cascades to block the computed entity from loading.

**Design for resilience — Error handling in the API entity:**

```powerquery
// API Entity: ProductReviews
// Use try...otherwise to return an empty table on failure
let
    SafeResult = try
    let
        Response = Web.Contents("https://reviews-api.company.com/reviews",
            [Headers = [Authorization = "Bearer " & ApiKey],
             Timeout = #duration(0, 0, 2, 0)]),  // 2-minute timeout
        Parsed = Json.Document(Response),
        AsTable = Table.FromList(Parsed, Splitter.SplitByNothing()),
        Expanded = Table.ExpandRecordColumn(AsTable, "Column1",
            {"productId", "avgRating", "reviewCount"})
    in
        Expanded
    otherwise
        // Return correct schema with 0 rows — computed entity can still join (left join returns null)
        #table(
            type table [productId = text, avgRating = number, reviewCount = Int64.Type],
            {}
        )
in
    SafeResult
```

**Computed entity: use LEFT JOIN (not INNER JOIN)**

```powerquery
// Computed entity: FactSales_Enriched
let
    Base = #"SalesData_Standard",

    // LEFT OUTER JOIN — keeps all sales even if reviews entity is empty
    WithReviews = Table.NestedJoin(
        Base, {"ProductKey"},
        #"ProductReviews", {"productId"},
        "Reviews", JoinKind.LeftOuter   // ← NOT Inner join
    ),
    Expanded = Table.ExpandTableColumn(WithReviews, "Reviews",
        {"avgRating"}, {"AvgRating"})
    // If ProductReviews is empty, AvgRating will be null — acceptable
in
    Expanded
```

**Additional resilience patterns:**

1. Add a "last successful refresh" timestamp column to the API entity
2. In the dataflow, log the entity status with `Table.AddColumn(#"ProductReviews", "DataSource", each "API", type text)` — allows monitoring which source contributed data
3. Configure Power Automate to alert on partial failures separately from full failures

**Result:** SQL and SharePoint data loads correctly; API failure produces nulls in the API-derived columns instead of crashing the entire dataflow.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing an Enterprise Dataflow Architecture for 50 Teams

**Scenario:** A Fortune 500 company is rolling out Power BI to 50 business units. Each unit has its own data, but they all share common master data: Customer, Product, Date, Employee, and Geography dimensions. How do you design a dataflow architecture that (1) centralizes master data, (2) allows business units to self-serve their own fact data, (3) prevents quality issues, and (4) scales without IT bottlenecks?

<details>
<summary>💡 Hint</summary>

Think in layers: Foundation (IT), Domain (business units), with governance mechanisms to enforce quality. Consider certification/endorsement, linked entities, workspace isolation, and monitoring.

</details>

<details>
<summary>✅ Solution</summary>

**Three-Layer Architecture:**

```
Layer 1: Foundation Dataflows (IT/Data Engineering, Premium workspace)
  ├── DimDate        → IT-owned, certified, no self-service modifications
  ├── DimCustomer    → from CRM, IT-maintained transformation rules
  ├── DimProduct     → from ERP, IT-maintained
  ├── DimEmployee    → from HR, IT-maintained, HR governance approved
  └── DimGeography   → from reference data, IT-maintained

Layer 2: Domain Dataflows (Business Units, governed self-service)
  ├── Sales Domain (Sales team, Sales workspace)
  │   ├── [Linked] DimCustomer (from Layer 1)
  │   ├── [Linked] DimProduct (from Layer 1)
  │   └── FactSales (team-owned, self-service)
  ├── Finance Domain (Finance team, Finance workspace)
  │   ├── [Linked] DimCustomer (from Layer 1)
  │   └── FactGL (team-owned, self-service)
  └── HR Domain (HR team, HR workspace)
      ├── [Linked] DimEmployee (from Layer 1, HR access only)
      └── FactHeadcount (team-owned)

Layer 3: Semantic Models (Analysts, Report workspaces)
  → Connect to Domain Dataflow entities
  → Build DAX measures, relationships, RLS
  → Reports connect to these semantic models
```

**Governance Enforcement:**

```
Rule 1: Shared dimensions MUST come from Foundation Dataflows
  → Enforce via workspace permissions: Domain teams cannot connect to source systems
    for Customer/Product/Date — only via linked entities from Foundation
  → Monitored via lineage API scan (weekly automated check)

Rule 2: Foundation Dataflows are read-only for Domain teams
  → Foundation workspace: IT has Admin/Member; Domain teams have Viewer only
  → Domain teams can link entities but cannot modify them

Rule 3: Domain Dataflows require endorsement before production use
  → "Promoted" by Domain data owner
  → "Certified" by Data Governance team (checks against data quality rules)

Rule 4: Source connections centralized
  → Only IT team can add new data source credentials
  → Domain teams request new sources via ticketing system
```

**Self-Service Enablement (prevent IT bottleneck):**

```
Domain teams CAN:
  ✅ Create their own fact entities (standard entities from their domain's systems)
  ✅ Create computed entities from their fact + linked Foundation dimensions
  ✅ Add custom columns, business-specific classifications
  ✅ Set their own refresh schedules

Domain teams CANNOT:
  ❌ Modify Foundation dimension entities
  ❌ Create direct connections to shared source systems (CRM, ERP) — only via linked entities
  ❌ Connect to on-premises sources without IT approval
```

**Monitoring and Quality:**

```python
# Weekly automated audit (Azure Function or Fabric Notebook):
def audit_dataflow_governance():
    all_dataflows = get_all_dataflows_in_org()
    violations = []

    for df in all_dataflows:
        # Check: does this dataflow connect directly to CRM (instead of using Foundation linked entity)?
        if uses_crm_connection_directly(df) and df.workspace != FOUNDATION_WORKSPACE:
            violations.append({
                "dataflow": df.name,
                "workspace": df.workspace,
                "violation": "Direct CRM connection — should use Foundation linked entity"
            })

        # Check: is this dataflow certified?
        if not df.is_certified and df.has_downstream_reports:
            violations.append({
                "dataflow": df.name,
                "violation": "Has downstream reports but not certified"
            })

    send_governance_report(violations)
```

**Scalability:**

- Foundation workspace uses **Premium capacity** for computed/linked entities
- Domain workspaces can use **Pro or PPU** — they read from Foundation (no computed/linked needed for just reading)
- As the organization grows to 100+ BUs, the Foundation layer stays the same; only Domain workspaces multiply
- Total ongoing IT effort: maintain 5 Foundation entities; all other work is self-service

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a computed entity and a linked entity in dataflows?" — A computed entity is a transformation applied to another entity within the same dataflow (like a derived table in SQL). A linked entity is a reference to an entity from a *different* dataflow, allowing you to reuse data without re-querying the source. Both require Premium or Fabric.

> **Tip 2:** "How does a Dataflow Gen2 differ from Gen1?" — Gen2 runs in Microsoft Fabric and stores data in Delta format in a Fabric Lakehouse (instead of CDM format in ADLS). It supports automatic staging, direct output to multiple destinations (Lakehouse, Warehouse, Azure SQL), and enables Direct Lake mode for semantic models, which provides near-Import performance without copying data into VertiPaq.

> **Tip 3:** "When would you use a dataflow instead of a direct source connection in a dataset?" — Use a dataflow when: (1) multiple datasets need the same cleaned/transformed data, (2) you want to shield datasets from source changes (one place to update when a column name changes), (3) you need to apply AI/ML functions in the transformation layer, or (4) you want to expose the transformed data to non-Power BI tools via ADLS Gen2. For a single dataset with no reuse, a direct connection is simpler.
