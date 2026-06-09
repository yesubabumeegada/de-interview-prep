---
title: "Paginated Reports — Scenarios"
topic: power-bi
subtopic: paginated-reports
content_type: scenario_question
tags: [power-bi, paginated-reports, scenarios, interview]
---

# Paginated Reports — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: When Should You Use Paginated Reports vs Standard Power BI Reports?

**Scenario:** A business user requests a "Sales Detail Report" that shows all 50,000 individual order line items for a given month, formatted to print on letter-size paper with company letterhead and totals at the bottom. Should you build this as a standard Power BI report or a paginated report? Explain your reasoning.

<details>
<summary>💡 Hint</summary>

Think about the differences between the two report types. Consider: how many rows need to be shown, whether the output will be printed, and whether pixel-perfect formatting is needed.

</details>

<details>
<summary>✅ Solution</summary>

**Answer: Paginated Report**

This should be a paginated report. Here is the reasoning for each requirement:

| Requirement | Standard Report | Paginated Report | Winner |
|---|---|---|---|
| 50,000 line items shown completely | Limit of ~30,000 in table visual; not designed for this | Renders ALL rows across as many pages as needed | Paginated ✅ |
| Print on letter-size paper | Dynamic layout, not print-optimized | Pixel-perfect, designed for printing | Paginated ✅ |
| Company letterhead on every page | Cannot have per-page headers | Page headers render on every page | Paginated ✅ |
| Totals at the bottom of the report | Possible but formatting is limited | Group and report-level totals are native features | Paginated ✅ |
| Interactive exploration/drilling | ✅ Built for this | Limited interactivity | Standard |

**When would you use a standard report instead?**

Use a standard Power BI report when:
- Users need to filter and explore data interactively
- Charts, KPI cards, and trend visuals are the primary output
- The data doesn't need to be printed in its entirety
- Mobile responsiveness is needed

**For this specific scenario:**

Build the paginated report with:
- Company logo in the page header
- Tablix data region showing all line items
- Group footer with subtotals per category
- Report footer with grand total
- Export to PDF for printing or emailing

The user can optionally also have a standard Power BI dashboard for exploration, with a drill-through link to this paginated report for the complete line-item detail.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Parameterized Cascading Filters Not Working Correctly

**Scenario:** You've built a paginated report with cascading parameters: Region → State → City. When you test the report, changing the Region dropdown correctly updates the State dropdown. But after selecting a State, the City dropdown still shows cities from ALL states, not just the selected state. What is wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about how cascading parameters work in Report Builder. The dataset for the City dropdown must reference BOTH @Region and @State — not just @State. Also check whether the parameter dependency is correctly configured.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause:**

The City parameter's available values dataset is only filtering by `@State`, not by both `@Region` AND `@State`. Or, the dataset is correct but Report Builder doesn't know the City dataset depends on both parameters.

**Check 1: Dataset query for Cities**

```sql
-- WRONG: Only filters by State (cities from other regions with same state name might appear)
SELECT DISTINCT CityName FROM dbo.DimGeography
WHERE StateName = @State
ORDER BY CityName

-- CORRECT: Filter by both Region AND State
SELECT DISTINCT CityName FROM dbo.DimGeography
WHERE RegionName = @Region AND StateName = @State
ORDER BY CityName
```

**Check 2: Parameter dependency in Report Builder**

In Report Builder:
1. Open Report Parameters → @City
2. Available Values → Get values from dataset → Select "CityDataset"
3. Verify that `CityDataset` references `@Region` in its query text

If `@Region` appears in the `CityDataset` query, Report Builder automatically marks City as depending on Region. If the query only has `@State`, it doesn't know to refresh City when Region changes.

**Complete Working Setup:**

```
Dataset: RegionDataset
  Query: SELECT DISTINCT RegionName FROM DimGeography ORDER BY RegionName
  No dependencies

Parameter: @Region
  Available Values → RegionDataset[RegionName]

Dataset: StateDataset
  Query: SELECT DISTINCT StateName FROM DimGeography WHERE RegionName = @Region ORDER BY StateName
  Depends on: @Region (auto-detected because @Region is in query)

Parameter: @State
  Available Values → StateDataset[StateName]
  Depends on: @Region (inferred)

Dataset: CityDataset
  Query: SELECT DISTINCT CityName FROM DimGeography WHERE RegionName = @Region AND StateName = @State ORDER BY CityName
  Depends on: @Region and @State (both in query)

Parameter: @City
  Available Values → CityDataset[CityName]
  Depends on: @Region and @State (inferred)
```

**Testing the Fix:**

1. Run the report
2. Select Region = "West"
3. State dropdown should show only Western states
4. Select State = "California"
5. City dropdown should show only California cities (not cities from Texas or Oregon)

If it still shows all cities after step 5, check whether the parameter order in the Parameters pane matches the dependency order (Region must come before State, which must come before City).

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Automating Personalized Monthly Statement Generation and Delivery

**Scenario:** A bank needs to generate monthly account statements for 200,000 customers. Each statement must include account holder name, all transactions for the month, opening/closing balance, and must be delivered as a PDF to the customer's email on the 1st of each month. The statements must also be archived in SharePoint for 7 years. Design the complete solution.

<details>
<summary>💡 Hint</summary>

Think about all components: the paginated report design, the parameterization strategy, the automation mechanism (Power Automate or Azure Function), batching for 200K customers, archival, and error handling.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture Overview:**

```
SQL Server (Transactions + Customer data)
    ↓
Paginated Report (.rdl) — parameterized by CustomerID + Month
    ↓
Azure Function (orchestrator — runs on 1st of each month)
    ↓ (for each customer)
Power BI REST API ExportTo endpoint → PDF
    ↓
Azure Blob Storage (archival) + SendGrid (email delivery)
```

**Step 1: Paginated Report Design**

```sql
-- Report dataset: AccountStatement
SELECT
    t.TransactionDate,
    t.TransactionType,    -- Debit/Credit
    t.Description,
    t.Amount,
    t.RunningBalance
FROM dbo.Transactions t
WHERE t.AccountID = @AccountID
  AND t.TransactionDate BETWEEN @StartDate AND @EndDate
ORDER BY t.TransactionDate, t.TransactionID;

-- Separate dataset for account summary
SELECT
    a.AccountID,
    a.AccountNumber,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.Email,
    c.MailingAddress,
    a.OpeningBalance,
    a.ClosingBalance,
    a.AccountType
FROM dbo.Accounts a
    JOIN dbo.Customers c ON a.CustomerID = c.CustomerID
WHERE a.AccountID = @AccountID;
```

```
Parameters:
  @AccountID: Integer
  @StartDate: DateTime, default = first day of last month
  @EndDate: DateTime, default = last day of last month

Page break: None (single continuous document per customer)
Footer: "Statement period: {StartDate} - {EndDate}" + "Page X of Y"
```

**Step 2: Azure Function Orchestrator**

```python
import asyncio
import aiohttp
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

async def generate_statements_batch(customer_ids: list, month_start: str, month_end: str):
    """Process customers in batches of 50 to avoid API rate limits"""
    BATCH_SIZE = 50
    WORKSPACE_ID = "<workspace-id>"
    REPORT_ID = "<report-id>"

    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    headers = {"Authorization": f"Bearer {token.token}"}

    async def export_customer_pdf(session, customer_id, email):
        # 1. Trigger export
        export_body = {
            "format": "PDF",
            "paginatedReportConfiguration": {
                "parameterValues": [
                    {"name": "AccountID", "value": str(customer_id)},
                    {"name": "StartDate", "value": month_start},
                    {"name": "EndDate", "value": month_end}
                ]
            }
        }
        async with session.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/reports/{REPORT_ID}/ExportTo",
            json=export_body, headers=headers
        ) as resp:
            export_id = (await resp.json())["id"]

        # 2. Poll for completion (max 10 minutes)
        for attempt in range(60):
            await asyncio.sleep(10)
            async with session.get(
                f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/reports/{REPORT_ID}/exports/{export_id}",
                headers=headers
            ) as status_resp:
                status = (await status_resp.json())["status"]
                if status == "Succeeded":
                    break
                elif status == "Failed":
                    raise Exception(f"Export failed for customer {customer_id}")

        # 3. Download PDF
        async with session.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/reports/{REPORT_ID}/exports/{export_id}/file",
            headers=headers
        ) as file_resp:
            pdf_content = await file_resp.read()

        # 4. Archive to Azure Blob Storage
        blob_client = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container = blob_client.get_container_client("statements")
        blob_name = f"{month_start[:7]}/{customer_id}/statement_{month_start[:7]}.pdf"
        container.upload_blob(blob_name, pdf_content, overwrite=True)

        # 5. Send email (via SendGrid API)
        await send_email_with_pdf(email, pdf_content, month_start[:7])

    # Process in batches
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(customer_ids), BATCH_SIZE):
            batch = customer_ids[i:i+BATCH_SIZE]
            tasks = [export_customer_pdf(session, cid, email) for cid, email in batch]
            # Process batch concurrently (50 at a time)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log any failures for retry
            failures = [(batch[j], e) for j, e in enumerate(results) if isinstance(e, Exception)]
            if failures:
                log_failures_for_retry(failures)
```

**Step 3: Retry and Monitoring**

```python
# Azure Queue Storage for failed customers
# Failed exports are queued for retry with exponential backoff

def retry_failed_customers():
    """Retry queue processor — runs hourly"""
    failed_queue = get_azure_queue("statement-failures")
    for message in failed_queue.receive_messages():
        customer_id, attempt = parse_retry_message(message)
        if attempt <= 3:
            try:
                generate_single_statement(customer_id)
                failed_queue.delete_message(message)
            except Exception:
                requeue_with_backoff(customer_id, attempt + 1, delay_minutes=attempt * 30)
        else:
            # After 3 retries, alert the operations team
            send_ops_alert(f"Statement generation failed 3x for customer {customer_id}")
            failed_queue.delete_message(message)
```

**Total estimated time for 200K customers:**
- 200,000 customers ÷ 50 concurrent = 4,000 batches
- Average export time: ~8 seconds per statement
- 50 concurrent × 8 seconds = 8 seconds per batch
- 4,000 batches × 8 seconds = ~9 hours total
- Schedule: Start at 11 PM on last day of month → complete by 8 AM on 1st

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a Tablix table and a Tablix matrix in paginated reports?" — A Tablix table has static column definitions with dynamic rows (like a SQL result table). A Tablix matrix has both dynamic rows and dynamic columns — column headers are generated from data values (like a pivot table). Both use the same underlying Tablix data region; the distinction is how row groups and column groups are configured.

> **Tip 2:** "Can paginated reports connect to a Power BI semantic model?" — Yes, in Premium/PPU workspaces. The connection type is "Microsoft Power BI dataset" and you write DAX queries instead of SQL. This allows reusing the existing business logic, measures, and RLS from the semantic model. RLS is enforced — users only see data they have permission to access.

> **Tip 3:** "What license is required for paginated reports in Power BI Service?" — Power BI Premium capacity (P1+) or Premium Per User (PPU). Standard Power BI Pro workspaces cannot host paginated reports. This is a common interview gotcha — people assume Pro is sufficient since it handles regular reports, but paginated reports are a Premium-only feature.
