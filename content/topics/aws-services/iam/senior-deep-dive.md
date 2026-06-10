---
title: "AWS IAM - Senior Deep Dive"
topic: aws-services
subtopic: iam
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, iam, security, zero-trust, governance]
---

# AWS IAM — Senior-Level Deep Dive

## Service Control Policies (SCPs)

SCPs set guardrails at the Organization level — they limit what member accounts CAN do, regardless of their IAM policies:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyRegionsOutsideUS",
            "Effect": "Deny",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "StringNotEquals": {
                    "aws:RequestedRegion": ["us-east-1", "us-west-2"]
                },
                "ArnNotLike": {
                    "aws:PrincipalArn": "arn:aws:iam::*:role/OrganizationAdmin"
                }
            }
        },
        {
            "Sid": "DenyDeletingCloudTrail",
            "Effect": "Deny",
            "Action": [
                "cloudtrail:StopLogging",
                "cloudtrail:DeleteTrail"
            ],
            "Resource": "*"
        },
        {
            "Sid": "DenyLeavingOrganization",
            "Effect": "Deny",
            "Action": "organizations:LeaveOrganization",
            "Resource": "*"
        }
    ]
}
```

**SCP patterns for data platforms:**
- Restrict regions (data residency compliance)
- Prevent CloudTrail/Config deletion (audit trail)
- Require encryption on S3 buckets
- Block public S3 access
- Require specific tags on created resources

---

## Attribute-Based Access Control (ABAC)

ABAC grants access based on tags rather than explicit resource ARNs:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AccessOwnTeamResources",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": "arn:aws:s3:::data-lake-*/*",
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/team": "${aws:PrincipalTag/team}"
                }
            }
        },
        {
            "Sid": "AccessOwnGlueJobs",
            "Effect": "Allow",
            "Action": ["glue:StartJobRun", "glue:GetJob", "glue:GetJobRun"],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceTag/team": "${aws:PrincipalTag/team}"
                }
            }
        }
    ]
}
```

**ABAC vs RBAC comparison:**

| Aspect | RBAC (Role-Based) | ABAC (Attribute-Based) |
|--------|-------------------|----------------------|
| Scaling | New role per team/project | One policy, tag controls access |
| Policy count | Grows with teams | Fixed (tag-based conditions) |
| New resource access | Update policy | Tag the resource correctly |
| Granularity | Role-level | Tag-level (fine-grained) |
| Complexity | Simple to understand | Requires tag discipline |
| Best for | Small orgs (<10 teams) | Large orgs (100+ teams) |

---

## IAM Identity Center (SSO)

```python
import boto3

sso_admin = boto3.client('sso-admin')

# Create a permission set for data engineers
response = sso_admin.create_permission_set(
    InstanceArn='arn:aws:sso:::instance/ssoins-1234567890',
    Name='DataEngineer',
    Description='Access for data engineering team',
    SessionDuration='PT8H',
    Tags=[{'Key': 'team', 'Value': 'data-engineering'}]
)

permission_set_arn = response['PermissionSet']['PermissionSetArn']

# Attach inline policy
sso_admin.put_inline_policy_to_permission_set(
    InstanceArn='arn:aws:sso:::instance/ssoins-1234567890',
    PermissionSetArn=permission_set_arn,
    InlinePolicy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:Get*", "s3:List*", "glue:*", "athena:*", "logs:*"],
                "Resource": "*"
            }
        ]
    })
)

# Assign to group across multiple accounts
sso_admin.create_account_assignment(
    InstanceArn='arn:aws:sso:::instance/ssoins-1234567890',
    TargetId='111111111111',  # AWS Account ID
    TargetType='AWS_ACCOUNT',
    PermissionSetArn=permission_set_arn,
    PrincipalType='GROUP',
    PrincipalId='data-engineers-group-id'
)
```

---

## Least Privilege at Scale

```python
# Strategy 1: Start with AWS managed policy, then reduce
# Use Access Advisor to find unused permissions

iam = boto3.client('iam')

# Get service last accessed data
response = iam.generate_service_last_accessed_details(Arn='arn:aws:iam::123456789:role/GlueETLRole')
job_id = response['JobId']

# Wait and retrieve results
details = iam.get_service_last_accessed_details(JobId=job_id)
for service in details['ServicesLastAccessed']:
    if service.get('LastAuthenticated') is None:
        print(f"UNUSED: {service['ServiceName']} — remove from policy")
    else:
        days_ago = (datetime.utcnow() - service['LastAuthenticated'].replace(tzinfo=None)).days
        if days_ago > 90:
            print(f"STALE: {service['ServiceName']} last used {days_ago} days ago")

# Strategy 2: Use IAM Access Analyzer policy generation
access_analyzer = boto3.client('accessanalyzer')

# Generate policy from CloudTrail activity (last 90 days)
response = access_analyzer.start_policy_generation(
    policyGenerationDetails={
        'principalArn': 'arn:aws:iam::123456789:role/GlueETLRole'
    },
    cloudTrailDetails={
        'trails': [{'cloudTrailArn': 'arn:aws:cloudtrail:us-east-1:123456789:trail/main'}],
        'accessRole': 'arn:aws:iam::123456789:role/AccessAnalyzerRole',
        'startTime': datetime(2024, 1, 1),
        'endTime': datetime(2024, 3, 31)
    }
)
# Result: a policy with ONLY the permissions actually used in those 90 days
```

---

## Automated Policy Generation

```python
# Generate least-privilege policy from CloudTrail events
def generate_minimum_policy(role_name, days=90):
    """Analyze CloudTrail to build minimum required permissions"""
    cloudtrail = boto3.client('cloudtrail')
    
    # Look up all API calls made by this role
    events = cloudtrail.lookup_events(
        LookupAttributes=[
            {'AttributeKey': 'ResourceName', 'AttributeValue': role_name}
        ],
        StartTime=datetime.utcnow() - timedelta(days=days),
        EndTime=datetime.utcnow()
    )
    
    # Extract unique action:resource pairs
    permissions = set()
    for event in events['Events']:
        detail = json.loads(event['CloudTrailEvent'])
        action = f"{detail['eventSource'].split('.')[0]}:{detail['eventName']}"
        resources = detail.get('resources', [])
        for resource in resources:
            permissions.add((action, resource.get('ARN', '*')))
    
    # Build policy from observed usage
    statements = []
    by_service = {}
    for action, resource in permissions:
        service = action.split(':')[0]
        by_service.setdefault(service, {'actions': set(), 'resources': set()})
        by_service[service]['actions'].add(action)
        by_service[service]['resources'].add(resource)
    
    for service, data in by_service.items():
        statements.append({
            'Effect': 'Allow',
            'Action': sorted(data['actions']),
            'Resource': sorted(data['resources'])
        })
    
    return {'Version': '2012-10-17', 'Statement': statements}
```

---

## Privilege Escalation Prevention

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyPrivilegeEscalation",
            "Effect": "Deny",
            "Action": [
                "iam:CreateRole",
                "iam:AttachRolePolicy",
                "iam:PutRolePolicy",
                "iam:CreatePolicy",
                "iam:CreatePolicyVersion",
                "iam:SetDefaultPolicyVersion",
                "iam:PassRole",
                "iam:CreateUser",
                "iam:AttachUserPolicy",
                "iam:AddUserToGroup",
                "lambda:CreateFunction",
                "lambda:UpdateFunctionCode"
            ],
            "Resource": "*",
            "Condition": {
                "ArnNotLike": {
                    "aws:PrincipalArn": [
                        "arn:aws:iam::*:role/SecurityAdmin",
                        "arn:aws:iam::*:role/PlatformAdmin"
                    ]
                }
            }
        }
    ]
}
```

**Common escalation paths to block:**
- `iam:PassRole` + `lambda:CreateFunction` → create Lambda with admin role
- `iam:CreatePolicyVersion` → overwrite existing policy with admin access
- `iam:AttachRolePolicy` → attach AdministratorAccess to own role
- `sts:AssumeRole` on overly permissive trust policies

---

## Security Audit Patterns

```python
def security_audit():
    """Run IAM security checks"""
    iam = boto3.client('iam')
    findings = []
    
    # Check 1: Roles with wildcard (*) resource access
    for role in iam.list_roles()['Roles']:
        policies = iam.list_attached_role_policies(RoleName=role['RoleName'])
        for policy in policies['AttachedPolicies']:
            version = iam.get_policy(PolicyArn=policy['PolicyArn'])['Policy']['DefaultVersionId']
            doc = iam.get_policy_version(PolicyArn=policy['PolicyArn'], VersionId=version)['PolicyVersion']['Document']
            for stmt in doc.get('Statement', []):
                if stmt.get('Resource') == '*' and stmt.get('Effect') == 'Allow':
                    findings.append(f"WIDE: {role['RoleName']} has Resource:* for {stmt.get('Action')}")
    
    # Check 2: Roles not used in 90 days
    for role in iam.list_roles()['Roles']:
        last_used = role.get('RoleLastUsed', {}).get('LastUsedDate')
        if last_used and (datetime.utcnow() - last_used.replace(tzinfo=None)).days > 90:
            findings.append(f"STALE: {role['RoleName']} unused for 90+ days")
    
    # Check 3: Cross-account trust to unknown accounts
    known_accounts = {'123456789', '987654321'}
    for role in iam.list_roles()['Roles']:
        trust = role['AssumeRolePolicyDocument']
        for stmt in trust.get('Statement', []):
            principal = stmt.get('Principal', {}).get('AWS', '')
            if isinstance(principal, str) and ':' in principal:
                account_id = principal.split(':')[4]
                if account_id not in known_accounts:
                    findings.append(f"EXTERNAL: {role['RoleName']} trusts unknown account {account_id}")
    
    return findings
```

---

## Interview Tips

> **Tip 1:** "How do you implement least privilege for a data platform with 50+ roles?" — "Three-phase approach: (1) Start with AWS managed policies for initial development. (2) After 90 days, use IAM Access Analyzer policy generation to create policies from actual CloudTrail usage. (3) Apply permission boundaries as guardrails to prevent escalation. ABAC (tag-based access) scales better than creating individual policies per team — one policy covers all teams, tags determine access scope."

> **Tip 2:** "How do you prevent privilege escalation in IAM?" — "Block the dangerous action combinations: iam:PassRole + lambda:CreateFunction, iam:CreatePolicyVersion, iam:AttachRolePolicy. Implement via SCP at the org level so it applies regardless of individual account policies. Only SecurityAdmin and PlatformAdmin roles are exempt. Regular audits scan for roles with these permissions."

> **Tip 3:** "How do SCPs interact with IAM policies?" — "SCPs are guardrails, not grants. They set the maximum permissions for an entire account. Even if a role has AdministratorAccess, if the SCP denies that action, it's denied. SCPs affect all principals in the account EXCEPT the management account. Use them for: region restrictions, preventing audit trail deletion, enforcing encryption, blocking public S3 access. They're the 'outer fence' around all IAM policies in the account."

## ⚡ Cheat Sheet

**Policy Evaluation Order (memorize this)**
1. Explicit Deny (SCP or identity policy) → **DENY** (always wins)
2. SCP Allow required at org level
3. Resource policy Allow (for cross-account)
4. Identity policy Allow
5. No matching Allow → **DENY** (implicit deny by default)

**SCP vs IAM Policy**
- SCPs are guardrails — they limit what can be done, they do NOT grant permissions
- Affects ALL principals in the account EXCEPT the management account root
- SCP `Allow` + IAM `Allow` = allowed; SCP `Deny` + IAM `Allow` = denied
- Use SCPs for: region lock, prevent CloudTrail deletion, require encryption, block public S3

**ABAC vs RBAC Scale Rule**
- RBAC: 1 policy per team × N teams = policy sprawl at >10 teams
- ABAC: 1 policy using `${aws:PrincipalTag/team}` == `${s3:ExistingObjectTag/team}` — scales to 100+ teams
- ABAC prerequisite: tag discipline (all resources must be tagged correctly)

**Privilege Escalation Paths to Block**
- `iam:PassRole` + `lambda:CreateFunction` → create Lambda with admin role
- `iam:CreatePolicyVersion` → overwrite existing policy with admin
- `iam:AttachRolePolicy` → attach `AdministratorAccess` to own role
- Block all of these via SCP; exempt only `SecurityAdmin` and `PlatformAdmin` roles

**Least-Privilege Workflow**
1. Start with AWS managed policy for development
2. After 90 days: IAM Access Analyzer policy generation from CloudTrail (actual usage)
3. Apply permission boundary as outer guardrail
4. Quarterly: `generate_service_last_accessed_details` → remove unused services

**Audit Red Flags**
- Role with `"Resource": "*"` + `"Effect": "Allow"` (wildcard resource)
- Role unused for 90+ days (`RoleLastUsed` check)
- Trust policy allowing `"Principal": "*"` or unknown external account IDs
- Users with both console access and programmatic keys (prefer IAM Identity Center)
