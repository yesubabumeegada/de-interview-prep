---
title: "Paginated Reports — Real-World Patterns"
topic: power-bi
subtopic: paginated-reports
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, paginated-reports, interview, real-world, production]
---

# Paginated Reports — Real-World Patterns

## Pattern 1: Monthly Customer Invoice Report

**Scenario**: Finance generates monthly invoices for 5,000 customers. Each invoice must include company letterhead, customer details, line items, totals, and a payment due date. Invoices are emailed as PDFs.

### Report Structure

```
Page 1 (each customer):
┌──────────────────────────────────────────────────────────┐
│ ACME Corp Logo         │  INVOICE                        │
│ 123 Main St            │  Invoice #: INV-2024-001234     │
│ New York, NY 10001     │  Date: December 31, 2024        │
│                        │  Due Date: January 31, 2025     │
├────────────────────────┴─────────────────────────────────┤
│ Bill To:                                                  │
│ [CustomerName]                                            │
│ [CustomerAddress]                                         │
│ [CustomerCity], [CustomerState] [CustomerZip]             │
├──────────────────────────────────────────────────────────┤
│ Description         │ Qty  │ Unit Price │ Amount          │
│ [ServiceDescription]│ [Qty]│ [UnitPrice]│ [LineAmount]   │
│ ...                 │ ...  │ ...        │ ...             │
├──────────────────────────────────────────────────────────┤
│                     │      │ Subtotal:  │ [Subtotal]      │
│                     │      │ Tax (8%):  │ [Tax]           │
│                     │      │ Total:     │ [Total]         │
├──────────────────────────────────────────────────────────┤
│ Payment Terms: Net 30                                     │
│ Bank: First National Bank  Routing: 021000021             │
│ Account: 1234567890                                       │
└──────────────────────────────────────────────────────────┘
```

### SQL Dataset Query

```sql
SELECT
    i.InvoiceID,
    i.InvoiceNumber,
    i.InvoiceDate,
    DATEADD(DAY, 30, i.InvoiceDate) AS DueDate,
    c.CustomerID,
    c.CompanyName AS CustomerName,
    c.BillingAddress,
    c.BillingCity,
    c.BillingState,
    c.BillingZip,
    il.LineNumber,
    il.ServiceDescription,
    il.Quantity,
    il.UnitPrice,
    il.Quantity * il.UnitPrice AS LineAmount,
    i.SubTotal,
    i.TaxAmount,
    i.TotalAmount
FROM dbo.Invoices i
    JOIN dbo.Customers c ON i.CustomerID = c.CustomerID
    JOIN dbo.InvoiceLines il ON i.InvoiceID = il.InvoiceID
WHERE i.InvoiceDate BETWEEN @StartDate AND @EndDate
    AND (@CustomerID = 0 OR i.CustomerID = @CustomerID)
ORDER BY i.CustomerID, i.InvoiceID, il.LineNumber
```

### Key Report Builder Configuration

```
Parameters:
  @StartDate: DateTime, default = first day of last month
  @EndDate: DateTime, default = last day of last month
  @CustomerID: Integer, default = 0 (all customers)

Page Break:
  Customer Group → Page Break → Before (each customer on its own page)
  Keep Together → true (prevent single invoice from splitting awkwardly)

Expressions:
  Due Date: =DateAdd("d", 30, Fields!InvoiceDate.Value)
  Tax: =Sum(Fields!LineAmount.Value) * 0.08
  Total Due: =Sum(Fields!LineAmount.Value) * 1.08
  Page counter: =Globals!PageNumber & " of " & Globals!TotalPages
```

### Report Bursting via Power Automate

```json
{
  "trigger": "Schedule: Last day of month at 11 PM",
  "actions": [
    {
      "foreach_customer": "SELECT CustomerID, Email FROM dbo.Customers WHERE ActiveBilling = 1",
      "generate_pdf": {
        "endpoint": "POST /reports/{reportId}/ExportTo",
        "body": {
          "format": "PDF",
          "paginatedReportConfiguration": {
            "parameterValues": [
              {"name": "CustomerID", "value": "{CustomerID}"},
              {"name": "StartDate", "value": "{first day of month}"},
              {"name": "EndDate", "value": "{last day of month}"}
            ]
          }
        }
      },
      "email_pdf": {
        "to": "{Email}",
        "subject": "Your Invoice for {Month} {Year}",
        "attachment": "{PDF}"
      }
    }
  ]
}
```

---

## Pattern 2: Regulatory Audit Trail Report

**Scenario**: Compliance requires a complete audit trail of all data changes for a specific account over a date range. The report must include every transaction row — potentially thousands — with exact timestamps and user IDs. Must export to PDF and CSV.

### Report Design

```
Report: Account Audit Trail
Parameters:
  @AccountID: text (required)
  @StartDate: datetime
  @EndDate: datetime
  @ExportFormat: (PDF / CSV / Excel)

Dataset:
  SELECT
      al.EventTimestamp,
      al.EventType,        -- INSERT, UPDATE, DELETE
      al.TableName,
      al.RecordID,
      al.ColumnName,
      al.OldValue,
      al.NewValue,
      al.ChangedBy,
      al.ApplicationName,
      al.IPAddress
  FROM dbo.AuditLog al
  WHERE al.AccountID = @AccountID
    AND al.EventTimestamp BETWEEN @StartDate AND @EndDate
  ORDER BY al.EventTimestamp ASC
```

### Expressions for Audit Context

```vb
' Color code event types
=SWITCH(
    Fields!EventType.Value = "INSERT", "LightGreen",
    Fields!EventType.Value = "DELETE", "LightCoral",
    Fields!EventType.Value = "UPDATE", "LightYellow",
    True, "White"
)

' Show change summary
=IIF(Fields!EventType.Value = "UPDATE",
     Fields!ColumnName.Value & ": " & Fields!OldValue.Value & " → " & Fields!NewValue.Value,
     Fields!EventType.Value & " record " & Fields!RecordID.Value)

' Report certification line
="This report was generated on " & Format(Now(), "MMMM dd, yyyy HH:mm:ss") &
 " by " & User!UserID & " and contains " & CountRows() & " audit events."
```

---

## Pattern 3: Financial Statement with Drill-Through to Paginated Detail

**Scenario**: Executive dashboard (standard Power BI report) shows summary KPIs. Clicking "View Detail" opens a paginated report for the full transaction-level detail.

### Standard Report: Drill-Through to Paginated

In Power BI Desktop standard report:
1. Right-click a visual → Add drill-through → **Paginated reports**
2. Select the paginated report in Power BI Service
3. Map parameters:
   - `AccountID` ← from context (slicer or visual filter)
   - `StartDate` ← from date slicer
   - `EndDate` ← from date slicer

When a user right-clicks a data point → "Drill through" → "Account Detail Report" → paginated report opens with parameters pre-filled.

### DAX Measure for the Paginated Report URL

```dax
-- Generates a direct URL to the paginated report pre-filtered
Paginated Detail URL =
VAR AccountID = SELECTEDVALUE(DimAccount[AccountID])
VAR StartDate = FORMAT(MIN(DimDate[Date]), "YYYY-MM-DD")
VAR EndDate = FORMAT(MAX(DimDate[Date]), "YYYY-MM-DD")
RETURN
    "https://app.powerbi.com/groups/{workspace}/rdlreports/{reportId}?rp:AccountID=" &
    AccountID & "&rp:StartDate=" & StartDate & "&rp:EndDate=" & EndDate
```

---

## Pattern 4: Cascading Parameter Report for Multi-Level Filtering

**Scenario**: Users want to filter a regional sales report by Geography → State → City → Customer. Each level should only show values relevant to the prior selection.

### Cascading Parameter Datasets

```sql
-- Dataset: Regions (no dependency)
SELECT DISTINCT RegionName FROM dbo.DimGeography ORDER BY RegionName

-- Dataset: States (depends on @Region)
SELECT DISTINCT StateName
FROM dbo.DimGeography
WHERE RegionName = @Region
ORDER BY StateName

-- Dataset: Cities (depends on @Region and @State)
SELECT DISTINCT CityName
FROM dbo.DimGeography
WHERE RegionName = @Region AND StateName = @State
ORDER BY CityName

-- Dataset: CustomerList (depends on all three)
SELECT CustomerID, CustomerName
FROM dbo.DimCustomer c
    JOIN dbo.DimGeography g ON c.GeographyKey = g.GeographyKey
WHERE g.RegionName = @Region
  AND g.StateName = @State
  AND g.CityName = @City
ORDER BY CustomerName

-- Main report dataset (depends on all parameters)
SELECT ...
FROM dbo.FactSales fs
    JOIN dbo.DimCustomer c ON fs.CustomerKey = c.CustomerKey
    JOIN dbo.DimGeography g ON c.GeographyKey = g.GeographyKey
WHERE g.RegionName = @Region
  AND g.StateName = @State
  AND g.CityName = @City
  AND (@CustomerID = 0 OR c.CustomerID = @CustomerID)
  AND fs.OrderDate BETWEEN @StartDate AND @EndDate
```

### Parameter Configuration

```
Parameters (in order):
1. @StartDate  → Date/Time, user input
2. @EndDate    → Date/Time, user input
3. @Region     → Get values from "Regions" dataset
4. @State      → Get values from "States" dataset (depends on @Region)
5. @City       → Get values from "Cities" dataset (depends on @Region, @State)
6. @CustomerID → Get values from "CustomerList" dataset (depends on all three), default = 0 (All)
```

**Important**: In Report Builder, specify parameter dependencies:
- "States" dataset query references `@Region` — Report Builder marks States as depending on Region
- Report Builder automatically refreshes the dropdown when Region is changed
