---
title: "Row-Level Security — Intermediate"
topic: power-bi
subtopic: row-level-security
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, row-level-security, interview, intermediate]
---

# Row-Level Security — Intermediate

## Dynamic RLS with LOOKUPVALUE

When the user's identity needs to be looked up in a separate security mapping table, use `LOOKUPVALUE`.

### Pattern: User-to-Region Mapping via Security Table

**SecurityMapping table** (loaded from a controlled source like a database or SharePoint):

| Email | Region | Department |
|---|---|---|
| alice@co.com | North | Sales |
| bob@co.com | South | Sales |
| carol@co.com | null | Finance |

```dax
-- Role: "Employee Access"
-- Table: DimGeography
-- DAX filter:
[Region] =
    LOOKUPVALUE(
        SecurityMapping[Region],
        SecurityMapping[Email],
        USERPRINCIPALNAME()
    )
```

**Problem**: `LOOKUPVALUE` returns blank when the user is not in the table, which means no data is shown for unrecognized users — a safe default.

**Problem with null region** (Carol is Finance, not assigned to a region):
```dax
-- Handle null region: Finance users see all regions
VAR UserRegion =
    LOOKUPVALUE(SecurityMapping[Region], SecurityMapping[Email], USERPRINCIPALNAME())
RETURN
    IF(ISBLANK(UserRegion), TRUE(), [Region] = UserRegion)
```

---

## Hierarchical RLS (Manager Sees Subordinates)

A common HR scenario: managers should see data for all employees who report to them (directly or indirectly).

### Self-Referencing Dimension Pattern

**DimEmployee with manager hierarchy:**

| EmployeeKey | Email | ManagerKey | EmployeePath |
|---|---|---|---|
| 1 | ceo@co.com | blank | 1 |
| 2 | vp@co.com | 1 | 1\|2 |
| 3 | mgr@co.com | 2 | 1\|2\|3 |
| 4 | rep@co.com | 3 | 1\|2\|3\|4 |

The `EmployeePath` calculated column uses `PATH()`:

```dax
-- Calculated column in DimEmployee
EmployeePath =
PATH(DimEmployee[EmployeeKey], DimEmployee[ManagerKey])
-- Returns "1|2|3|4" for employee 4 (shows full ancestry)
```

### RLS Filter for Manager Hierarchy

```dax
-- Role: "Manager View"
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
- `CurrentUserKey` = the logged-in manager's EmployeeKey (e.g., 3)
- `PATHCONTAINS([EmployeePath], 3)` returns TRUE for employee paths that contain "3"
- Employee 3 (mgr) has path "1|2|3" → contains 3 → visible
- Employee 4 (rep) has path "1|2|3|4" → contains 3 → visible
- Employee 2 (vp) has path "1|2" → does NOT contain 3 → hidden

---

## RLS with DirectQuery Tables

RLS with DirectQuery sends the filter as a WHERE clause to the source database.

```dax
-- DirectQuery RLS filter on SQL Server table
-- This filter folds to SQL:
[SalesRegion] = LOOKUPVALUE(
    UserSecurity[Region],
    UserSecurity[Email],
    USERPRINCIPALNAME()
)
-- SQL Server receives: WHERE SalesRegion = 'North'
```

**Considerations for DirectQuery RLS:**
- Filters must be translatable to the source SQL dialect
- Complex M-style expressions won't fold — avoid custom M functions in RLS filters
- Test the generated SQL in DAX Studio with Server Timings
- Each user's first query may be slower until cached at the source

---

## Object-Level Security (OLS)

OLS restricts entire **tables** or **columns** from specific roles, even in DAX.

### When to Use OLS vs RLS

| Security Need | Use |
|---|---|
| Hide rows (e.g., user sees own region) | RLS |
| Hide an entire table (e.g., HRData) | OLS |
| Hide a specific column (e.g., Salary) | OLS |
| Show different measures per role | OLS (hide measures) |

### Setting Up OLS

OLS is **not available in the Power BI Desktop UI** — it requires **Tabular Editor** (free community version works).

```json
// Tabular Editor: Table > Columns > Right-click column > Object-Level Security
// Metadata Permission options:
// - Default (inherits from table)
// - None (completely hidden)
// - Read (visible)
```

**DAX behavior with OLS:**
- A hidden column returns an error if directly referenced in a DAX measure
- A DAX measure that works for admin users may fail for restricted users
- Use separate measures that don't reference hidden columns for non-privileged roles

---

## RLS and Row Counts in Visuals

Users with RLS can still see aggregated row counts in some visuals (like "Total: 1,000 rows"), which may reveal that more data exists than they're seeing. Solutions:

```dax
-- RLS-aware count (don't expose total row count)
Visible Rows = COUNTROWS(FactSales)
-- This returns the count WITHIN the user's RLS context — correct
-- No special handling needed; RLS automatically limits COUNTROWS
```

The issue is external tools or APIs (e.g., REST API with dataset statistics) may expose total row counts. Mitigate by not exposing datasets directly via API to end users.

---

## RLS Best Practices

### Centralized Security Table

Instead of embedding user-to-data mapping in dimension tables, maintain a dedicated security mapping table:

```
SecurityMapping (loaded from a controlled source)
┌───────────────────┬──────────────────┬───────────────┐
│ Email             │ FilterAttribute  │ FilterValue   │
├───────────────────┼──────────────────┼───────────────┤
│ alice@co.com      │ Region           │ North         │
│ bob@co.com        │ Region           │ South         │
│ admin@co.com      │ ALL              │ null          │
└───────────────────┴──────────────────┴───────────────┘
```

```dax
-- Universal dynamic RLS filter
VAR UserEmail = USERPRINCIPALNAME()
VAR UserScope =
    LOOKUPVALUE(SecurityMapping[FilterAttribute], SecurityMapping[Email], UserEmail)
VAR UserValue =
    LOOKUPVALUE(SecurityMapping[FilterValue], SecurityMapping[Email], UserEmail)
RETURN
    IF(UserScope = "ALL", TRUE(),
       IF(UserScope = "Region", [Region] = UserValue,
          FALSE()
       )
    )
```

### Principle of Least Privilege

- Default to **no access** (if user not in SecurityMapping → see nothing)
- Explicitly grant access rather than explicitly denying
- Use security groups in Azure AD rather than individual emails when possible

### RLS Audit

To audit which roles exist and who is assigned:

```
Power BI Service → Dataset → Security
-- Lists all roles and their members
```

For programmatic audit, use the Power BI REST API:
```
GET https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/users
```

---

## RLS in Embedded Scenarios

When embedding Power BI reports in custom applications (Power BI Embedded), RLS can be applied via the **embed token**:

```javascript
// Generate embed token with RLS roles
const embedToken = await client.reports.generateTokenInGroup(
    workspaceId,
    reportId,
    {
        accessLevel: 'View',
        datasetId: datasetId,
        identities: [{
            username: "alice@company.com",   // end user's identity
            roles: ["Salesperson"],          // role(s) to apply
            datasets: [datasetId]
        }]
    }
);
```

**Important**: In embedded scenarios, the `username` passed in the token is what `USERPRINCIPALNAME()` returns in DAX — not the Azure AD identity of the service principal. Your DAX RLS filter must match the value you pass as `username`.

---

## Common RLS Patterns Summary

| Pattern | Use Case | Key Function |
|---|---|---|
| Static role filter | Fixed value per role (region, department) | Hardcoded DAX predicate |
| Dynamic user match | User table with email column | `USERPRINCIPALNAME()` |
| Lookup-based dynamic | Separate security mapping table | `LOOKUPVALUE()` |
| Hierarchical | Manager sees subordinates | `PATH()` + `PATHCONTAINS()` |
| Embedded RLS | Custom app embeds Power BI | Token with username + roles |

---

## Summary

- Use **USERPRINCIPALNAME()** for cloud-based dynamic RLS (not USERNAME)
- **LOOKUPVALUE** connects user identity to data values in a security mapping table
- **PATH + PATHCONTAINS** enables hierarchical/manager-subordinate RLS
- **OLS** (Object-Level Security) hides tables/columns; requires Tabular Editor
- DirectQuery RLS folds filters to SQL — keep filter expressions SQL-compatible
- Use **centralized security mapping tables** for maintainable dynamic RLS
- In **embedded scenarios**, pass username and roles in the embed token
