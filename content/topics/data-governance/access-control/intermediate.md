---
title: "Access Control & RBAC — Intermediate"
topic: data-governance
subtopic: access-control
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [access-control, rbac, dynamic-masking, row-level-security, snowflake]
---

# Access Control & RBAC — Intermediate

## Dynamic Data Masking

Show different views of PII data based on user role:

```sql
-- Snowflake: Create masking policy for email
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'DPO_TEAM') THEN val
    WHEN CURRENT_ROLE() IN ('ANALYST_APPROVED_PII') THEN val
    ELSE SHA2(val)  -- Hash the email for all other roles
  END;

-- Apply masking policy to column
ALTER TABLE gold.customers
  MODIFY COLUMN email
  SET MASKING POLICY email_mask;

-- Now:
-- analyst_revenue role:         SELECT email → returns 'e3b0c44...' (hashed)
-- analyst_approved_pii role:    SELECT email → returns 'john@company.com' (real)
-- data_admin role:              SELECT email → returns 'john@company.com' (real)
```

```sql
-- Masking for credit card numbers (show last 4 digits only)
CREATE OR REPLACE MASKING POLICY card_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'PAYMENTS_TEAM' THEN val
    ELSE CONCAT('****-****-****-', RIGHT(val, 4))
  END;
```

---

## Row-Level Security

Restrict which rows a user can see based on their attributes:

```sql
-- Snowflake: Row Access Policy
-- Analysts can only see their own region's data
CREATE OR REPLACE ROW ACCESS POLICY region_filter AS (region_col VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'DATA_ADMIN' THEN TRUE  -- admins see all
    WHEN CURRENT_ROLE() LIKE 'ANALYST_%' THEN
      -- Analyst role name encodes their region: ANALYST_US, ANALYST_EU
      region_col = SPLIT_PART(CURRENT_ROLE(), '_', 2)
    ELSE FALSE
  END;

-- Apply to orders table
ALTER TABLE gold.orders
  ADD ROW ACCESS POLICY region_filter ON (region);

-- Now:
-- ANALYST_US: SELECT * FROM gold.orders → sees only US rows
-- ANALYST_EU: SELECT * FROM gold.orders → sees only EU rows
-- DATA_ADMIN: SELECT * FROM gold.orders → sees all rows
```

---

## Access Request Workflow

Automate the approval process for sensitive data:

```python
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

class RequestStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"

@dataclass
class AccessRequest:
    request_id: str
    requester: str
    table_name: str
    role_requested: str
    business_justification: str
    access_duration_days: int  # 30, 90, 365, or -1 for permanent
    status: RequestStatus = RequestStatus.PENDING
    requested_at: datetime = field(default_factory=datetime.utcnow)
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

class AccessRequestSystem:
    """Self-service access request and approval workflow."""
    
    def __init__(self, engine, snowflake_client, notification_client):
        self.engine = engine
        self.sf = snowflake_client
        self.notify = notification_client
    
    def submit_request(self, request: AccessRequest) -> str:
        """Submit an access request and notify approver."""
        import sqlalchemy as sa
        
        # Determine approver based on table sensitivity
        approver = self._get_approver(request.table_name)
        
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO access_requests
                (request_id, requester, table_name, role_requested, justification, 
                 duration_days, status, requested_at, approver)
                VALUES (:id, :req, :table, :role, :just, :days, 'pending', NOW(), :approver)
            """), {
                "id": request.request_id,
                "req": request.requester,
                "table": request.table_name,
                "role": request.role_requested,
                "just": request.business_justification,
                "days": request.access_duration_days,
                "approver": approver,
            })
        
        self.notify.send(
            to=approver,
            subject=f"Access request: {request.requester} → {request.table_name}",
            body=(
                f"Access request from {request.requester}:\n"
                f"Table: {request.table_name}\n"
                f"Role: {request.role_requested}\n"
                f"Duration: {request.access_duration_days} days\n"
                f"Justification: {request.business_justification}\n\n"
                f"Approve: https://governance.company.com/requests/{request.request_id}/approve\n"
                f"Deny: https://governance.company.com/requests/{request.request_id}/deny"
            ),
        )
        return request.request_id
    
    def approve(self, request_id: str, approver: str):
        """Approve request and provision access in Snowflake."""
        import sqlalchemy as sa
        
        with self.engine.connect() as conn:
            req = conn.execute(sa.text(
                "SELECT * FROM access_requests WHERE request_id = :id"
            ), {"id": request_id}).fetchone()
        
        # Provision in Snowflake
        expires_at = datetime.utcnow() + timedelta(days=req.duration_days)
        self.sf.execute(f"GRANT ROLE {req.role_requested} TO USER {req.requester}")
        
        # Schedule revocation
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE access_requests
                SET status = 'approved', reviewed_by = :approver,
                    reviewed_at = NOW(), expires_at = :expires
                WHERE request_id = :id
            """), {"approver": approver, "expires": expires_at, "id": request_id})
        
        self.notify.send(
            to=req.requester,
            subject=f"Access granted: {req.table_name}",
            body=f"Access to {req.table_name} granted. Expires: {expires_at.date()}.",
        )
    
    def revoke_expired_access(self):
        """Cron job: revoke access for expired requests."""
        import sqlalchemy as sa
        
        with self.engine.connect() as conn:
            expired = conn.execute(sa.text("""
                SELECT request_id, requester, role_requested, table_name
                FROM access_requests
                WHERE status = 'approved' AND expires_at < NOW()
            """)).fetchall()
        
        for req in expired:
            self.sf.execute(f"REVOKE ROLE {req.role_requested} FROM USER {req.requester}")
            
            with self.engine.begin() as conn:
                conn.execute(sa.text(
                    "UPDATE access_requests SET status = 'expired' WHERE request_id = :id"
                ), {"id": req.request_id})
            
            self.notify.send(
                to=req.requester,
                subject=f"Access to {req.table_name} has expired",
                body="Your temporary access has expired. Request renewal if still needed.",
            )
    
    def _get_approver(self, table_name: str) -> str:
        """Determine approver based on table sensitivity."""
        import sqlalchemy as sa
        
        with self.engine.connect() as conn:
            tags = conn.execute(sa.text(
                "SELECT tags FROM data_catalog.assets WHERE table_name = :t"
            ), {"t": table_name}).scalar() or []
        
        if "pii" in tags:
            return "dpo@company.com"
        return "data-governance@company.com"
```

---

## Access Audit Reporting

```sql
-- Monthly access audit: who has access to PII tables
SELECT
    u.user_email,
    u.department,
    u.role_name,
    t.table_name,
    t.sensitivity_tag,
    ar.approved_by,
    ar.approved_at,
    ar.expires_at,
    CASE
        WHEN ar.expires_at IS NULL THEN 'permanent'
        WHEN ar.expires_at < NOW() THEN 'EXPIRED (still has access!)'
        ELSE CONCAT(DATE_DIFF('day', NOW(), ar.expires_at), ' days remaining')
    END AS access_status
FROM active_grants g
JOIN users u ON g.user_email = u.user_email
JOIN data_catalog.assets t ON g.table_name = t.table_name
LEFT JOIN access_requests ar ON ar.requester = g.user_email AND ar.table_name = g.table_name
WHERE 'pii' = ANY(t.tags)
ORDER BY u.department, t.table_name;
```

---

## Interview Tips

> **Tip 1:** "What is dynamic data masking?" — A database feature that returns different values based on the caller's role. PII analysts see real emails; other roles see hashed values. The masking happens at query time — the underlying data is unchanged. Available in Snowflake, BigQuery, Databricks Unity Catalog.

> **Tip 2:** "How do you handle temporary access?" — Use a self-service access request system with a business justification field, time-limited grants (30/90/365 days), approval workflow, and automated revocation when access expires. Track all grants in a registry so access can be audited and expired grants caught.

> **Tip 3:** "What is row-level security?" — A policy that filters which rows a user can see, applied transparently at query time. The analyst doesn't see an error — they just see fewer rows. Use case: regional analysts see only their region, multi-tenant systems where tenants can only see their own data.
