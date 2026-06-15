---
title: "Data Mesh Design — Intermediate"
topic: system-design
subtopic: data-mesh-design
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, data-mesh, data-product, governance, platform]
---

# Data Mesh Design — Intermediate

## Identifying Domain Boundaries

The hardest part of data mesh is getting the domain decomposition right. Wrong boundaries cause:
- Duplicate data across domains
- Confusion about who owns what
- Cross-domain dependencies that become bottlenecks

### Using Bounded Contexts (from DDD)

Domain-Driven Design (DDD) provides the "bounded context" concept — a coherent area of the business with its own language and models.

```
Orders domain:
  Entities: Order, OrderItem, OrderStatus, Discount
  Language: "place an order", "cancel an order", "order shipped"
  Data: orders, order_items, order_statuses

Payments domain:
  Entities: Payment, Refund, PaymentMethod, Invoice
  Language: "charge", "refund", "authorize"
  Data: payments, refunds, invoices

Inventory domain:
  Entities: Product, SKU, Warehouse, Stock, Reservation
  Language: "reserve stock", "fulfill", "reorder"
  Data: products, inventory, warehouses

Customer domain:
  Entities: Customer, Address, Preference, Segment
  Language: "register", "profile", "segment"
  Data: customers, addresses, segments
```

### Warning Signs of Wrong Domain Boundaries

| Problem | Symptom | Fix |
|---|---|---|
| Too fine-grained | Domains need each other's data constantly | Merge them |
| Too coarse-grained | One "domain" has 200 tables and 30 engineers | Split by subdomain |
| Shared mutable data | Two domains write to the same table | Assign ownership to one; other reads via output port |
| No clear owner | "Who owns the customer address table?" → confusion | Assign to Customer domain |

---

## Data Product Design: The Full Specification

A data product is not just a table. It's a fully specified, discoverable, SLA-backed artifact.

### Data Product Contract (YAML)

```yaml
# data_product.yaml — committed to the domain team's git repo
apiVersion: "datamesh/v1"
kind: DataProduct

metadata:
  name: "orders-daily-summary"
  domain: "orders"
  owner:
    team: "orders-engineering"
    contact: "orders-de@company.com"
    oncall: "https://pagerduty.com/service/orders"
  version: "3.2.0"
  status: "production"  # draft | staging | production | deprecated

description: |
  Daily summary of orders by region and product category.
  Updated daily by 06:00 UTC. Used by Finance, Marketing, and Supply Chain teams.

output_ports:
  - type: delta_table
    location: "s3://data-mesh/orders/orders_daily_summary/"
    format: "delta"
    partitioned_by: ["order_date", "region"]
    schema_path: "schemas/orders_daily_summary.avsc"
    query_examples:
      - description: "Revenue by region this month"
        sql: "SELECT region, SUM(revenue) FROM orders_daily_summary WHERE order_date >= '2024-01-01' GROUP BY region"

sla:
  freshness:
    schedule: "0 6 * * *"          # Updated daily by 6 AM UTC
    max_lag_hours: 2                # Alert if not fresh by 8 AM
  availability: "99.5%"
  row_count_bounds:
    min: 50000                      # Alert if fewer rows than expected
    max: 5000000
  quality_checks:
    - "revenue IS NOT NULL"
    - "order_count > 0"
    - "region IN ('NA', 'EU', 'APAC', 'LATAM')"

access_policy:
  default: "read"                   # All authenticated users can read
  classification: "internal"
  pii_columns: []                   # No PII in this summary table
  restricted_columns: ["gross_margin"]  # Finance only

dependencies:
  upstream:
    - domain: "orders"
      product: "orders-raw-events"
    - domain: "payments"
      product: "payments-confirmed"
  downstream_consumers:
    - "finance-team: revenue-report"
    - "marketing-team: campaign-attribution"
    - "supply-chain: demand-forecast"

changelog:
  - version: "3.2.0"
    date: "2024-01-15"
    change: "Added gross_margin column"
  - version: "3.1.0"
    date: "2023-12-01"
    change: "Added LATAM region"
```

---

## Self-Serve Platform: What the Central Team Provides

The central platform team's goal: **any domain team can publish a production-quality data product without filing a ticket with the platform team**.

### Platform Capabilities

**1. Compute (zero setup)**
```python
# Domain team can spin up Spark with zero config:
from company_platform import get_spark

spark = get_spark(
    team="orders",
    workload="orders-daily-transform",
    size="medium"  # Central team pre-configures instance types, auto-scaling
)
# Platform handles: cluster provisioning, IAM roles, monitoring, auto-scaling
```

**2. Data Product Publishing**
```bash
# Platform CLI: publish a data product from the domain team's terminal
datamesh publish \
  --product orders-daily-summary \
  --source-path s3://orders/processed/daily_summary/ \
  --contract data_product.yaml

# Platform automatically:
# - Validates schema against contract
# - Runs quality checks defined in contract
# - Registers in data catalog (DataHub/Unity Catalog)
# - Sets up SLA monitoring
# - Configures access controls per policy
# - Sends Slack notification: "New version 3.2.0 of orders-daily-summary published"
```

**3. CI/CD Templates**
```yaml
# GitHub Actions template provided by platform team
# Domain team just uses it — no CI/CD expertise needed
name: Data Product CI/CD
on:
  push:
    branches: [main]

jobs:
  validate:
    uses: company/data-platform/.github/workflows/validate-product.yml@main
    with:
      contract_path: data_product.yaml
      schema_path: schemas/
    secrets: inherit

  test:
    uses: company/data-platform/.github/workflows/run-dbt-tests.yml@main
    with:
      dbt_project_path: .
      target: staging

  publish:
    needs: [validate, test]
    uses: company/data-platform/.github/workflows/publish-product.yml@main
    with:
      environment: production
    secrets: inherit
```

**4. Automatic PII Classification**
```python
# Platform scans all new data products for PII automatically
# Uses AWS Macie / GCP DLP / internal classifier

class PIIScanner:
    PII_PATTERNS = {
        "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "ssn": r"\d{3}-\d{2}-\d{4}",
        "credit_card": r"\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}",
        "phone": r"\+?1?\d{10,14}"
    }
    
    def scan_schema(self, schema: dict, sample_data: DataFrame) -> dict:
        """Returns flagged PII columns with confidence scores."""
        results = {}
        for column in schema["fields"]:
            col_name = column["name"].lower()
            # Name-based detection
            if any(pii_kw in col_name for pii_kw in ["email", "phone", "ssn", "address"]):
                results[column["name"]] = {"type": "PII", "method": "name_match", "confidence": 0.9}
            # Value-based detection (sample 1000 rows)
            else:
                sample = sample_data.select(column["name"]).limit(1000).collect()
                for pii_type, pattern in self.PII_PATTERNS.items():
                    matches = sum(1 for row in sample if re.match(pattern, str(row[0])))
                    if matches / len(sample) > 0.8:
                        results[column["name"]] = {"type": "PII", "method": "value_match",
                                                   "confidence": matches/len(sample)}
        return results
```

---

## Federated Governance in Practice

### The Governance Stack

```
Global policies (central governance council):
  Defined in: governance.yaml (committed to central repo)
  Enforced by: Unity Catalog / Apache Ranger (technical enforcement)
  
  Examples:
  - All columns named "email*", "ssn*", "dob*" → automatically masked
  - Data older than 7 years → automatically deleted
  - No data product can have "unknown" as owner
  - All data crossing EU boundaries → must be anonymized

Domain policies (set by each domain team):
  Defined in: data_product.yaml (in domain team's repo)
  Examples:
  - "Only finance-team can see gross_margin column"
  - "Marketing team can access email after signing DPA"
  - "Raw orders data requires manager approval"
```

### Unity Catalog: Technical Enforcement

```sql
-- Central platform team creates policies in Unity Catalog
-- Policies apply automatically to ALL queries across ALL clusters

-- Global PII masking policy
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('data-science-approved') THEN email
  ELSE regexp_replace(email, '(.)(.*?)(@)', '$1***$3')  -- m***@company.com
END;

-- Apply to column level (automatic for any column named email*)
ALTER TABLE orders.customers
ALTER COLUMN customer_email
SET MASK mask_email;
-- Now: any user not in data-science-approved group sees m***@company.com
-- No code change needed in any query or pipeline

-- Row-level security: EU users only see EU data
CREATE ROW FILTER eu_data_filter ON orders.orders_raw
USING (CASE WHEN is_account_group_member('eu-team') THEN region = 'EU' ELSE true END);
```

---

## Cross-Domain Data Access Patterns

### Pattern 1: Direct Table Access (Most Common)

```sql
-- Marketing team's dbt model reading from Orders domain product
-- With Unity Catalog, the domain team has granted read access to marketing
SELECT o.order_date, o.customer_id, o.revenue, c.segment
FROM orders.orders_daily_summary o          -- Orders domain data product
JOIN customers.customer_segments c           -- Customer domain data product
  ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - 30
```

### Pattern 2: Data Product API (Computed Results)

```python
# For real-time or computed data, domains expose an API
import httpx

# Marketing team calls Orders domain API for latest customer LTV
response = httpx.get(
    "https://orders-api.internal/data-products/customer-ltv",
    params={"customer_id": "cust-123"},
    headers={"Authorization": f"Bearer {service_token}"}
)
ltv_data = response.json()
```

### Pattern 3: Event Subscription (Near Real-Time)

```python
# Marketing subscribes to order events from Orders domain
# Orders domain publishes to Kafka; marketing consumes

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders.OrderPlaced",           # Domain: orders, event: OrderPlaced
    bootstrap_servers="kafka:9092",
    group_id="marketing-attribution",
    # Access controlled: only authorized consumer groups can subscribe
)

for message in consumer:
    order_event = json.loads(message.value)
    # Marketing updates their attribution model with each order event
    update_campaign_attribution(order_event)
```
