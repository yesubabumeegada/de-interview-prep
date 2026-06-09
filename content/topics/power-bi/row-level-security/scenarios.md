---
title: "Row-Level Security — Scenarios"
topic: power-bi
subtopic: row-level-security
content_type: scenario_question
tags: [power-bi, row-level-security, scenarios, interview]
---

# Row-Level Security — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Sales Rep Sees Only Their Own Data

**Scenario:** You have a report with a `FactSales` table and a `DimSalesPerson` table. Each sales rep should only see their own sales data when they open the report in Power BI Service. How do you implement this?

<details>
<summary>💡 Hint</summary>

Think about which table to put the RLS filter on — the dimension or the fact table. The filter should use the logged-in user's email to match against a column in the model.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Ensure DimSalesPerson has an Email column**

```
DimSalesPerson: SalesPersonKey, Name, Email, Region, Manager
```

**Step 2: Create RLS role in Power BI Desktop**

1. Go to Modeling → Manage Roles → Create
2. Name the role "SalesRepAccess"
3. Select DimSalesPerson table
4. Enter the DAX filter:

```dax
[Email] = USERPRINCIPALNAME()
```

**Step 3: Verify the relationship**

Confirm `FactSales[SalesPersonKey]` → `DimSalesPerson[SalesPersonKey]` is a 1:* active relationship with single cross-filter direction (DimSalesPerson → FactSales). The RLS filter on DimSalesPerson will propagate to FactSales via this relationship.

**Step 4: Test in Power BI Desktop**

Modeling → View as Roles → SalesRepAccess → Other User → enter test email

**Step 5: Publish and assign**

After publishing, go to Power BI Service → Dataset → Security → "SalesRepAccess" → add all sales rep emails (or an Azure AD security group containing them).

**Why filter DimSalesPerson, not FactSales?**

Filtering the dimension is the correct pattern because:
- The dimension has one row per sales rep (unique key) — efficient to filter
- The relationship propagates the filter to FactSales automatically
- You don't need to modify the large fact table directly

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Manager Sees Their Team's Data

**Scenario:** You have a `DimEmployee` table with a `ManagerEmail` column. Managers need to see their own data AND all data belonging to employees who report directly to them. A Director has managers reporting to them and should see all data under their hierarchy. How do you implement this?

<details>
<summary>💡 Hint</summary>

Think about PATH and PATHCONTAINS for hierarchical traversal. You'll need a column that encodes the full ancestry path for each employee so you can check if the logged-in user appears anywhere in the reporting chain.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Add a self-key and path column to DimEmployee**

In your data model, `DimEmployee` should have a `ManagerKey` (foreign key pointing to the same table):

```
DimEmployee:
- EmployeeKey (PK)
- EmployeeName
- Email
- ManagerKey (FK → EmployeeKey, nullable for CEO)
```

**Step 2: Add EmployeePath calculated column**

```dax
-- Calculated column in DimEmployee
EmployeePath =
PATH(DimEmployee[EmployeeKey], DimEmployee[ManagerKey])
-- CEO (no manager): "1"
-- VP reports to CEO: "1|2"
-- Manager reports to VP: "1|2|3"
-- Rep reports to Manager: "1|2|3|4"
```

**Step 3: Create RLS role**

```dax
-- Role: "HierarchyAccess"
-- Table: DimEmployee
-- DAX filter:

VAR CurrentUserKey =
    LOOKUPVALUE(
        DimEmployee[EmployeeKey],
        DimEmployee[Email],
        USERPRINCIPALNAME()
    )
RETURN
    PATHCONTAINS([EmployeePath], CurrentUserKey)
```

**How it works:**
- VP (key=2) logged in: `CurrentUserKey = 2`
- `PATHCONTAINS("1|2|3|4", 2)` = TRUE → rep's row visible
- `PATHCONTAINS("1|2|3", 2)` = TRUE → manager's row visible
- `PATHCONTAINS("1|2", 2)` = TRUE → VP's own row visible
- `PATHCONTAINS("1", 2)` = FALSE → CEO's row NOT visible

**Step 4: Verify with test users**

Test with a manager's email and confirm they see only their subtree. Test with a rep's email and confirm they see only their own row.

**Edge case: User not in the table**

If `LOOKUPVALUE` returns blank (user not in DimEmployee), `PATHCONTAINS(path, BLANK())` returns FALSE — the user sees no data. This is the safe default.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: RLS Performance Degradation with 10,000 Users

**Scenario:** Your Power BI report uses dynamic RLS with `USERPRINCIPALNAME()` matched against a 10,000-row SecurityMapping table. After deployment, analysts report that the report takes 15-20 seconds to load for the first open each day. The dataset refreshes at midnight. DAX Studio shows 90% FE time and 10% SE time on a simple card visual. Diagnose and fix this.

<details>
<summary>💡 Hint</summary>

The RLS filter expression runs for every query. Think about how VertiPaq cache works with dynamic user identities — cache can't be shared across users when each sees different data. Also consider whether LOOKUPVALUE is being evaluated in the FE vs SE.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

The 90% FE time on a card visual strongly suggests the RLS filter is causing Formula Engine overhead. With 10,000 users, the issues compound:

1. **No cache sharing**: Each user's first query generates a unique filter context (`[Email] = "specificuser@co.com"`). VertiPaq cannot share cached results across users — every user sees different data. After a midnight refresh, all 10,000 caches are cold.

2. **LOOKUPVALUE in RLS is FE-heavy**:
```dax
-- Current (slow) RLS filter:
[Region] = LOOKUPVALUE(SecurityMapping[Region], SecurityMapping[Email], USERPRINCIPALNAME())
-- LOOKUPVALUE must scan SecurityMapping for each query → FE work
```

3. **SecurityMapping as a separate unrelated table**: If SecurityMapping has no relationship to DimGeography, every query forces a FE cross-table lookup.

**Fix 1: Replace LOOKUPVALUE with a relationship-based filter**

Restructure the model so the user-to-data mapping is a relationship, not a lookup:

```
DimEmployee (Email, RegionKey) ──→ DimRegion (RegionKey, RegionName)
                                         ↓
                                    FactSales
```

```dax
-- New RLS filter on DimEmployee (fast — relationship + equality on indexed column):
[Email] = USERPRINCIPALNAME()
-- Power BI traverses the relationship to DimRegion and then to FactSales
-- SE can use bitmap indexes; no FE LOOKUPVALUE needed
```

**Fix 2: Reduce SecurityMapping scan with pre-filtering in Power Query**

If you must keep the lookup approach, ensure the SecurityMapping table has an integer surrogate key instead of email strings (lower cardinality = faster dictionary lookup):

```powerquery
// Add integer key to SecurityMapping in Power Query
WithKey = Table.AddIndexColumn(SecurityMapping, "UserKey", 1, 1, Int64.Type)
```

**Fix 3: Pre-warm cache with scheduled queries**

After midnight refresh, use Power Automate to trigger common queries for active users:

```
Power Automate Flow:
  Trigger: At 12:30 AM (30 min after refresh completes)
  Action: For each user in ActiveUsers list
    → Call Power BI REST API: POST /datasets/{id}/refreshes  (forces query cache warm-up)
```

**Fix 4: Coarse role pre-filtering (reduces distinct filter contexts)**

Group users into coarse roles first, then fine-grain within the role:

```dax
-- Role "NorthSales" (static pre-filter in role definition):
-- DAX filter on DimRegion: [RegionCode] = "NORTH"
-- Additional fine filter on DimEmployee: [Email] = USERPRINCIPALNAME()

-- Now cache is segmented by region (not individual users)
-- "NORTH" cache is shared among all North users until their individual filter is applied
```

**Fix 5: Premium capacity — Query Scale-Out**

For Premium capacity, enable **Query Scale-Out** to distribute read queries across multiple replicas. Combined with warm caches on each replica, cold-start latency is reduced.

**Result**: With relationship-based RLS and coarse pre-filtering, first-query time drops from 15-20 seconds to 2-3 seconds, and subsequent queries are sub-second from cache.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between RLS and OLS?" — RLS (Row-Level Security) restricts which rows a user can see, based on DAX filter expressions applied per role. OLS (Object-Level Security) restricts which entire tables or columns are visible. They serve different needs: RLS for data partitioning by user, OLS for hiding sensitive attributes entirely.

> **Tip 2:** "Why should you use USERPRINCIPALNAME() instead of USERNAME() for cloud deployments?" — `USERNAME()` returns a Windows domain format (`DOMAIN\user`) which applies to on-premises Analysis Services. In Power BI Service, `USERPRINCIPALNAME()` returns the user's Azure AD email (`user@company.com`), which is how users are identified in the cloud. Using `USERNAME()` in a cloud deployment typically returns an empty string or unexpected format.

> **Tip 3:** "Can a Power BI admin bypass RLS to see all data?" — Yes. Users with Admin, Member, or Contributor workspace roles and Edit permission on the dataset bypass RLS — they always see all data. Only users assigned as Report Viewer (no edit permissions) are subject to RLS. This is a commonly missed caveat: if you add a report developer to the workspace with Edit access, they bypass RLS even if their email is in the restricted role.
