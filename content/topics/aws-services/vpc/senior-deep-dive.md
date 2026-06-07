---
title: "AWS VPC - Senior Deep Dive"
topic: aws-services
subtopic: vpc
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, vpc, architecture, multi-account, hybrid]
---

# AWS VPC — Senior-Level Deep Dive

## Multi-Account VPC Architecture (Transit Gateway Hub-and-Spoke)

```
                    ┌────────────────────┐
                    │   Network Account   │
                    │   Transit Gateway   │
                    └─────────┬──────────┘
          ┌───────────────────┼───────────────────┐
          │                   │                   │
    ┌─────┴─────┐      ┌─────┴─────┐      ┌─────┴─────┐
    │ Data Lake │      │  Compute  │      │ Analytics │
    │  Account  │      │  Account  │      │  Account  │
    │10.0.0.0/16│      │10.1.0.0/16│      │10.2.0.0/16│
    └───────────┘      └───────────┘      └───────────┘
```

```python
import boto3

ec2 = boto3.client('ec2')

# Create Transit Gateway in Network account
tgw = ec2.create_transit_gateway(
    Description='Data Platform Hub',
    Options={
        'AmazonSideAsn': 64512,
        'AutoAcceptSharedAttachments': 'enable',
        'DefaultRouteTableAssociation': 'enable',
        'DefaultRouteTablePropagation': 'enable',
        'DnsSupport': 'enable',
        'VpnEcmpSupport': 'enable'
    }
)

# Share TGW via RAM to member accounts
ram = boto3.client('ram')
ram.create_resource_share(
    name='transit-gateway-share',
    resourceArns=[tgw['TransitGateway']['TransitGatewayArn']],
    principals=['arn:aws:organizations::123456789:organization/o-12345']  # Entire org
)

# Route table segmentation (isolate environments)
# Production route table: only prod VPCs can reach each other
# Dev route table: dev VPCs isolated from prod
prod_rt = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
dev_rt = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
```

---

## Hybrid Connectivity (Direct Connect + VPN)

```
On-Premises Data Center
        │
    Direct Connect (1/10 Gbps dedicated)
        │
┌───────┴────────┐
│ Direct Connect │
│    Gateway     │
└───────┬────────┘
        │
┌───────┴────────┐
│Transit Gateway │──── VPN (backup, encrypted)
└───────┬────────┘
        │
   AWS VPCs (data platform)
```

**Configuration for data platform:**
```python
# Direct Connect for bulk data transfer (high bandwidth, consistent latency)
# VPN as backup (encrypted, internet-based)

# Transit Gateway attachment for Direct Connect
ec2.create_transit_gateway_attachment(
    TransitGatewayId=tgw_id,
    # Direct Connect Gateway handles physical connection
)

# Route on-premises CIDR through Transit Gateway
ec2.create_transit_gateway_route(
    TransitGatewayRouteTableId=prod_rt_id,
    DestinationCidrBlock='172.16.0.0/12',  # On-premises
    TransitGatewayAttachmentId=dx_attachment_id
)
```

**Bandwidth planning for data workloads:**
- Daily 500 GB extract from on-prem → 1 Gbps DX sufficient (transfers in ~1 hour)
- Real-time replication → consider 10 Gbps DX or multiple 1 Gbps connections
- Burst scenarios → DX + VPN failover (VPN limited to ~1.25 Gbps per tunnel)

---

## DNS Resolution (Route 53 Private Hosted Zones)

```python
route53 = boto3.client('route53')

# Create private hosted zone for internal service discovery
route53.create_hosted_zone(
    Name='data.internal.company.com',
    VPC={'VPCRegion': 'us-east-1', 'VPCId': 'vpc-data-lake'},
    CallerReference='data-platform-2024',
    HostedZoneConfig={'PrivateZone': True}
)

# Associate with additional VPCs (cross-account via RAM)
route53.associate_vpc_with_hosted_zone(
    HostedZoneId='Z12345',
    VPC={'VPCRegion': 'us-east-1', 'VPCId': 'vpc-analytics'}
)

# DNS records for data services
route53.change_resource_record_sets(
    HostedZoneId='Z12345',
    ChangeBatch={
        'Changes': [
            {
                'Action': 'CREATE',
                'ResourceRecordSet': {
                    'Name': 'redshift.data.internal.company.com',
                    'Type': 'CNAME',
                    'TTL': 300,
                    'ResourceRecords': [{'Value': 'analytics-cluster.abc123.us-east-1.redshift.amazonaws.com'}]
                }
            },
            {
                'Action': 'CREATE',
                'ResourceRecordSet': {
                    'Name': 'rds-source.data.internal.company.com',
                    'Type': 'CNAME',
                    'TTL': 300,
                    'ResourceRecords': [{'Value': 'source-db.abc123.us-east-1.rds.amazonaws.com'}]
                }
            }
        ]
    }
)
```

**DNS resolution for hybrid (on-prem + AWS):**
- Route 53 Resolver inbound endpoint: on-prem resolves AWS private DNS
- Route 53 Resolver outbound endpoint: AWS resolves on-prem DNS
- Forward on-prem domains to corporate DNS servers

---

## Network Security for Data Platform Services

### Glue in VPC

```python
# Glue connection configuration for VPC access
glue = boto3.client('glue')

glue.create_connection(
    ConnectionInput={
        'Name': 'rds-connection',
        'ConnectionType': 'JDBC',
        'PhysicalConnectionRequirements': {
            'SubnetId': 'subnet-private-1a',
            'SecurityGroupIdList': ['sg-glue-vpc'],
            'AvailabilityZone': 'us-east-1a'
        },
        'ConnectionProperties': {
            'JDBC_CONNECTION_URL': 'jdbc:postgresql://rds-source.data.internal.company.com:5432/source_db',
            'USERNAME': 'glue_reader',
            'PASSWORD': 'stored-in-secrets-manager'
        }
    }
)

# Security group requirements for Glue:
# 1. Self-referencing rule (ALL TCP 0-65535 from itself)
# 2. Outbound to target (RDS port 5432)
# 3. Outbound to S3 (via Gateway endpoint or NAT)
# 4. Outbound to Glue service (via Interface endpoint or NAT)
```

### EMR Networking

```python
# EMR in private subnet with managed scaling
emr_config = {
    'Ec2SubnetIds': ['subnet-private-1a', 'subnet-private-1b'],
    'EmrManagedMasterSecurityGroup': 'sg-emr-master',
    'EmrManagedSlaveSecurityGroup': 'sg-emr-core',
    'ServiceAccessSecurityGroup': 'sg-emr-service',  # EMR service access
}
# EMR service SG needs port 9443 (for cluster management)
# Master SG needs port 8443 from service SG
# Core nodes need all-traffic from master and each other
```

### Redshift Enhanced VPC Routing

```sql
-- Force all Redshift COPY/UNLOAD through VPC (not public internet)
ALTER CLUSTER analytics-cluster ENHANCED VPC ROUTING ON;

-- Requires: VPC endpoint for S3 (Gateway endpoint in route table)
-- Without enhanced routing: COPY from S3 goes through public internet
-- With enhanced routing: COPY stays within AWS private network
```

---

## CIDR Planning for Data Platform

| VPC | CIDR | Purpose | Subnets |
|-----|------|---------|---------|
| data-lake | 10.0.0.0/16 | S3 Gateway endpoints, Glue, Crawlers | /24 per AZ per tier |
| compute | 10.1.0.0/16 | EMR, EKS, Glue jobs | /20 (large for EMR scaling) |
| database | 10.2.0.0/16 | RDS, Redshift, ElastiCache | /24 per AZ |
| analytics | 10.3.0.0/16 | BI tools, notebooks, APIs | /24 per AZ |
| shared-services | 10.4.0.0/16 | DNS, VPN, bastion, monitoring | /24 per AZ |

**CIDR planning rules:**
- Never overlap CIDRs (breaks peering/TGW routing)
- Plan for growth (use /16 per VPC, even if starting small)
- EMR/EKS need large subnets (/20 or bigger) for pod/node scaling
- Reserve space for future VPCs in the supernet (e.g., 10.0.0.0/8)
- Document everything (CIDR sprawl is painful to fix later)

---

## Interview Tips

> **Tip 1:** "Design the network architecture for a multi-account data platform" — "Transit Gateway hub-and-spoke. Network account owns the TGW, shared via RAM to all accounts. Route table segmentation isolates prod from dev. VPC endpoints in each VPC for S3 (free Gateway) and other services (Interface). Direct Connect for on-premises connectivity. CIDR plan with /16 per VPC, no overlaps. Centralized DNS via Route 53 Private Hosted Zones shared across accounts."

> **Tip 2:** "How do you handle Glue/EMR connectivity to RDS?" — "Create a Glue Connection with VPC config (subnet + security group). Critical: the security group needs a self-referencing rule (all TCP from itself) — Glue creates ENIs that must communicate. Route to RDS via private subnet. Route to S3 via Gateway endpoint (or NAT, but that costs money). For EMR: place in private subnet, security groups between master/core/service, enhanced VPC routing for S3 access within the network."

> **Tip 3:** "How do you reduce data transfer costs in a VPC?" — "Three strategies: (1) S3 Gateway endpoints — free, eliminate NAT charges for S3 access (biggest cost saver for data workloads). (2) Keep compute in the same AZ as storage when possible (cross-AZ transfer costs $0.01/GB). (3) VPC peering instead of Transit Gateway for high-bandwidth connections (peering same-AZ is free, TGW charges $0.02/GB). For a data platform processing TBs daily, these can save thousands per month."
