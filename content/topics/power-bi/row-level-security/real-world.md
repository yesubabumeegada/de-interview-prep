---
title: "Row-Level Security — Real-World Patterns"
topic: power-bi
subtopic: row-level-security
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, row-level-security, interview, real-world, production]
---

# Row-Level Security — Real-World Patterns

## Pattern 1: Territory-Based Sales RLS

**Scenario**: A company has 200 sales reps, each assigned to one or more territories. A central HR system maintains the territory assignments. Territory assignments change quarterly. Reports must reflect the current assignment without republishing.

### Architecture

```
HR System → Power Query → SecurityMapping table (refreshes daily)
                              ↓
                         DimEmployee ← FactSales
                              ↓
                         DimTerritory
```

### SecurityMapping Power Query

```powerquery
// Refresh from HR API daily
let
    Source = OData.Feed("https://hr.company.com/api/territory-assignments"),
    FilterActive = Table.SelectRows(Source, each [Status] = "Active"),
    Selected = Table.SelectColumns(FilterActive, {"EmployeeEmail", "TerritoryCode", "TerritoryName", "ValidFrom"}),
    Typed = Table.TransformColumnTypes(Selected, {
        {"EmployeeEmail", type text},
        {"TerritoryCode", type text},
        {"TerritoryName", type text},
        {"ValidFrom", type date}
    })
in
    Typed
```

### DAX RLS Filter

```dax
-- Role: "Territory Rep"
-- Table: DimTerritory
-- Filter (allows multiple territory assignments per rep):

[TerritoryCode] IN
    CALCULATETABLE(
        VALUES(SecurityMapping[TerritoryCode]),
        SecurityMapping[EmployeeEmail] = USERPRINCIPALNAME()
    )
```

### Measures for Territory Context

```dax
-- Show current user's territory in a card
My Territories =
CONCATENATEX(
    FILTER(
        SecurityMapping,
        SecurityMapping[EmployeeEmail] = USERPRINCIPALNAME()
    ),
    SecurityMapping[TerritoryName],
    ", "
)

-- Sales in my territory vs company total
My Territory Share =
DIVIDE(
    SUM(FactSales[SalesAmount]),
    CALCULATE(SUM(FactSales[SalesAmount]), REMOVEFILTERS(DimTerritory))
)
```

---

## Pattern 2: Multi-Tenant SaaS Dashboard (Embedded RLS)

**Scenario**: A SaaS company builds a Power BI embedded dashboard served to multiple client companies (tenants). Each client should only see their own data. The embedding application authenticates users and passes tenant context to Power BI.

### Model Design

```
DimTenant (TenantID, TenantName, ContractTier)
    ↓
FactUsage (UsageID, TenantID, Date, FeatureUsed, Count)
FactBilling (BillingID, TenantID, Month, Amount)
```

### RLS Role for Multi-Tenant

```dax
-- Role: "Tenant"
-- Table: DimTenant
-- DAX filter — TenantID passed via embed token username field:
[TenantID] = USERPRINCIPALNAME()
-- The embed token sets username = "tenant123" (the TenantID, not an email)
```

### Embedding Code (Node.js)

```javascript
const { PowerBIClient } = require('@azure/arm-powerbiembedded');
const { ClientSecretCredential } = require('@azure/identity');

async function generateEmbedToken(tenantId, userId) {
    const credential = new ClientSecretCredential(
        process.env.AZURE_TENANT_ID,
        process.env.POWERBI_APP_ID,
        process.env.POWERBI_APP_SECRET
    );

    const client = new PowerBIClient(credential);

    const tokenRequest = {
        accessLevel: 'View',
        datasetId: process.env.DATASET_ID,
        identities: [{
            username: tenantId,        // This becomes USERPRINCIPALNAME() in DAX
            roles: ['Tenant'],         // RLS role to apply
            datasets: [process.env.DATASET_ID]
        }]
    };

    const embedToken = await client.reports.generateTokenInGroup(
        process.env.WORKSPACE_ID,
        process.env.REPORT_ID,
        tokenRequest
    );

    return {
        token: embedToken.token,
        reportUrl: `https://app.powerbi.com/reportEmbed?reportId=${process.env.REPORT_ID}`,
        expiry: embedToken.expiration
    };
}
```

### DAX Measures for Tenant Context

```dax
-- Tenant-specific KPIs
Current Tenant Name =
SELECTEDVALUE(DimTenant[TenantName], "Unknown")

-- Usage in current period (tenant's data only)
Monthly Active Users =
CALCULATE(
    DISTINCTCOUNT(FactUsage[UserID]),
    DATESMTD(DimDate[Date])
)

-- Benchmark vs anonymized peer average (careful with info exposure)
-- This measure shows tenant data vs average across all tenants visible to THIS user
-- Since RLS filters to one tenant, this effectively shows their own data
Avg Daily Usage =
AVERAGEX(
    VALUES(DimDate[Date]),
    CALCULATE(SUM(FactUsage[Count]))
)
```

---

## Pattern 3: Finance Department — Column-Level Security

**Scenario**: The HR dataset includes employee salaries. Finance and HR should see salary data; all other users (including managers) should see employee names and departments but NOT individual salaries.

### OLS Setup (Tabular Editor)

```
Table: DimEmployee
Column: AnnualSalary
  → Role "HR": Read
  → Role "Finance": Read
  → Role "Manager": None (hidden)
  → Role "Employee": None (hidden)
```

### Safe Measures by Role

```dax
-- This measure works for Finance/HR (can see AnnualSalary column)
Total Payroll =
SUM(DimEmployee[AnnualSalary])

-- This measure is safe for all roles (uses pre-aggregated budget table)
Budget Headcount Cost =
SUM(FactBudget[HeadcountCost])
-- FactBudget has aggregated cost data without individual salaries

-- Role-adaptive card title
Salary Visibility Label =
IF(
    ISEMPTY(FILTER(DimEmployee, NOT ISBLANK(DimEmployee[AnnualSalary]))),
    "Salary data: Restricted",
    "Salary data: Available"
)
```

### Separate Measures Page for Finance Role

Use a **bookmark** with a visuals toggle:
- All users see: Employee Name, Department, Headcount, Budget cost
- Finance/HR users also see: AnnualSalary, Payroll Total, Salary Band distribution

Use OLS to hide the salary visual for non-Finance roles instead of DAX branching — cleaner and more secure.

---

## Pattern 4: Self-Service Security Administration

**Scenario**: The BI team cannot update RLS assignments every time someone changes roles. Build a system where HR team members can update security assignments via a SharePoint list, without BI team involvement.

### Architecture

```
SharePoint List (HR-managed)
    ↓ Power Query refresh (daily or on-demand)
SecurityMapping table in Power BI dataset
    ↓ DAX RLS
DimEmployee → FactSales
```

### SharePoint Security List Power Query

```powerquery
let
    // HR team manages this SharePoint list
    Source = SharePoint.Tables(
        "https://company.sharepoint.com/sites/HR/",
        [ApiVersion = 15]
    ),
    SecurityList = Source{[Title="PowerBI Security Assignments"]}[Items],

    // Keep only relevant columns
    Selected = Table.SelectColumns(SecurityList, {
        "EmployeeEmail", "Region", "Department", "AccessLevel", "ValidFrom", "ValidTo"
    }),

    // Filter to currently active assignments
    Today = Date.From(DateTime.LocalNow()),
    Active = Table.SelectRows(Selected, each
        [ValidFrom] <= Today and
        ([ValidTo] = null or [ValidTo] >= Today)
    ),

    // Type enforcement
    Typed = Table.TransformColumnTypes(Active, {
        {"EmployeeEmail", type text},
        {"Region", type text},
        {"Department", type text},
        {"AccessLevel", type text},
        {"ValidFrom", type date},
        {"ValidTo", type nullable date}
    })
in
    Typed
```

### Dynamic RLS using SharePoint Security List

```dax
-- Role: "Standard User"
-- Table: DimGeography

VAR UserEmail = USERPRINCIPALNAME()
VAR UserAccess =
    FILTER(SecurityMapping, SecurityMapping[EmployeeEmail] = UserEmail)
VAR UserRegion =
    MAXX(UserAccess, SecurityMapping[Region])
VAR UserLevel =
    MAXX(UserAccess, SecurityMapping[AccessLevel])

RETURN
    SWITCH(
        TRUE(),
        UserLevel = "Global", TRUE(),           // Global access: see everything
        UserLevel = "Regional", [Region] = UserRegion,  // Regional: own region only
        UserLevel = "None", FALSE(),             // Explicitly denied
        FALSE()                                  // Default: deny if not found
    )
```

**Benefits:**
- HR team updates SharePoint list to onboard/offboard users
- Power BI dataset refreshes daily (or on-demand via Power Automate webhook)
- No BI team involvement for routine security changes
- Audit trail is in SharePoint's list history
