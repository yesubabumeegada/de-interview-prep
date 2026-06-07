---
title: "AWS VPC - Real-World Production Examples"
topic: aws-services
subtopic: vpc
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, vpc, production, data-platform-networking]
---

# AWS VPC — Real-World Production Examples

## Pattern 1: Data Platform VPC Design

```python
import boto3

ec2 = boto3.client('ec2')

# Three-tier subnet architecture for data platform
vpc_design = {
    'VPC': '10.0.0.0/16',
    'subnets': {
        'public': {
            'purpose': 'Bastion host, NAT Gateways, ALB for internal tools',
            'cidrs': ['10.0.1.0/24', '10.0.2.0/24'],  # AZ-a, AZ-b
            'routes': 'Internet Gateway'
        },
        'private-compute': {
            'purpose': 'Glue ENIs, EMR clusters, Lambda, EKS nodes',
            'cidrs': ['10.0.16.0/20', '10.0.32.0/20'],  # /20 for scaling
            'routes': 'NAT Gateway + S3 Gateway Endpoint'
        },
        'private-data': {
            'purpose': 'RDS, Redshift, ElastiCache (isolated)',
            'cidrs': ['10.0.48.0/24', '10.0.49.0/24'],
            'routes': 'No internet (S3 endpoint only)'
        }
    }
}

# Create VPC
vpc = ec2.create_vpc(CidrBlock='10.0.0.0/16')
vpc_id = vpc['Vpc']['VpcId']

# Enable DNS (required for VPC endpoints)
ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={'Value': True})
ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={'Value': True})

# S3 Gateway Endpoint (free, critical for data workloads)
ec2.create_vpc_endpoint(
    VpcId=vpc_id,
    ServiceName='com.amazonaws.us-east-1.s3',
    VpcEndpointType='Gateway',
    RouteTableIds=['rtb-private-compute', 'rtb-private-data']
)

# Security groups
# Glue jobs → can reach RDS and S3 endpoint
# RDS → accepts connections from Glue SG and Bastion SG only
# Bastion → SSH from corporate IP only
```

### Security Group Rules

```python
# Glue security group (self-referencing + outbound to data)
sg_glue = ec2.create_security_group(GroupName='sg-glue', VpcId=vpc_id, Description='Glue ETL jobs')
ec2.authorize_security_group_ingress(GroupId=sg_glue['GroupId'], IpPermissions=[
    {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'UserIdGroupPairs': [{'GroupId': sg_glue['GroupId']}]}
])

# RDS security group (only Glue and bastion can connect)
sg_rds = ec2.create_security_group(GroupName='sg-rds', VpcId=vpc_id, Description='RDS databases')
ec2.authorize_security_group_ingress(GroupId=sg_rds['GroupId'], IpPermissions=[
    {'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'UserIdGroupPairs': [
        {'GroupId': sg_glue['GroupId']},
        {'GroupId': 'sg-bastion'}
    ]}
])

# Redshift security group (analytics tools + Glue for loading)
sg_redshift = ec2.create_security_group(GroupName='sg-redshift', VpcId=vpc_id, Description='Redshift')
ec2.authorize_security_group_ingress(GroupId=sg_redshift['GroupId'], IpPermissions=[
    {'IpProtocol': 'tcp', 'FromPort': 5439, 'ToPort': 5439, 'UserIdGroupPairs': [
        {'GroupId': sg_glue['GroupId']},
        {'GroupId': 'sg-bi-tools'},
        {'GroupId': 'sg-bastion'}
    ]}
])
```

---

## Pattern 2: Glue + RDS Connectivity Setup

```python
# Complete setup for Glue to extract from RDS in VPC

# Step 1: Ensure RDS is in private subnet with proper SG
# (RDS SG must allow inbound 5432 from Glue SG)

# Step 2: Create Glue connection
glue = boto3.client('glue')

glue.create_connection(
    ConnectionInput={
        'Name': 'source-rds-postgresql',
        'ConnectionType': 'JDBC',
        'PhysicalConnectionRequirements': {
            'SubnetId': 'subnet-private-compute-1a',  # Same or routable to RDS subnet
            'SecurityGroupIdList': ['sg-glue'],
            'AvailabilityZone': 'us-east-1a'
        },
        'ConnectionProperties': {
            'JDBC_CONNECTION_URL': 'jdbc:postgresql://mydb.abc123.us-east-1.rds.amazonaws.com:5432/production',
            'USERNAME': 'glue_reader',
            'PASSWORD': '{{resolve:secretsmanager:rds-glue-creds}}'
        }
    }
)

# Step 3: Test connection (verifies network path works)
glue.test_connection(ConnectionName='source-rds-postgresql')

# Step 4: If test fails, common fixes:
troubleshooting = """
1. Self-referencing rule missing on Glue SG
   → Add: All TCP (0-65535) inbound from sg-glue to sg-glue

2. No route to S3 for Glue service communication
   → Add S3 Gateway endpoint to route table
   → OR add NAT Gateway route

3. RDS SG doesn't allow Glue SG
   → Add inbound rule: TCP 5432 from sg-glue

4. Subnet has no available IPs (Glue creates 2+ ENIs)
   → Use larger subnet (/24 minimum recommended)

5. DNS resolution fails for RDS endpoint
   → Ensure EnableDnsSupport and EnableDnsHostnames are True on VPC
"""
```

---

## Pattern 3: Multi-Account Transit Gateway for Data Mesh

```python
# Network account manages Transit Gateway
# Domain accounts attach their VPCs
# Route table segmentation controls access

# Architecture:
# - Network Account: owns TGW, Direct Connect
# - Data Lake Account: S3, Glue Catalog (10.0.0.0/16)
# - Orders Domain Account: ETL workloads (10.1.0.0/16)
# - Analytics Account: BI, Redshift (10.2.0.0/16)
# - On-Premises: corporate data center (172.16.0.0/12)

# TGW route tables for segmentation
route_tables = {
    'production': {
        'associations': ['data-lake-vpc', 'orders-vpc', 'analytics-vpc'],
        'propagations': ['data-lake-vpc', 'orders-vpc', 'analytics-vpc', 'dx-attachment'],
        'static_routes': {'172.16.0.0/12': 'dx-attachment'}  # On-prem
    },
    'development': {
        'associations': ['dev-vpc'],
        'propagations': ['dev-vpc'],
        # No route to production or on-prem (isolated)
    },
    'shared-services': {
        'associations': ['shared-vpc'],
        'propagations': ['shared-vpc', 'data-lake-vpc', 'orders-vpc', 'analytics-vpc'],
        # All environments can reach shared services (DNS, monitoring)
    }
}

# Cost estimate (10 accounts, 15 VPCs):
# TGW: 15 attachments × $0.05/hr = $0.75/hr = $540/month
# Data transfer: 5 TB/month × $0.02/GB = $100/month
# Total: ~$640/month (vs managing 100+ peering connections)
```

---

## Pattern 4: VPC Endpoints to Eliminate NAT Costs

```python
# Before: All AWS service access goes through NAT Gateway
# NAT cost: 2 TB/month data to S3 × $0.045/GB = $90/month (just NAT processing)
# After: S3 Gateway endpoint (free), Interface endpoints for other services

# Cost analysis for a data platform:
cost_comparison = {
    'before_endpoints': {
        'nat_gateway_hourly': 0.045 * 730,  # $32.85/month per NAT
        'nat_data_processing': 2000 * 0.045,  # $90/month for 2TB to S3
        'total_per_month': '$123/month per AZ'
    },
    'after_endpoints': {
        's3_gateway': 0,  # Free
        'glue_interface': 0.01 * 730 * 2,  # $14.60/month (2 AZs)
        'other_interfaces': 0.01 * 730 * 2 * 3,  # $43.80 (3 more endpoints)
        'nat_remaining': 0.045 * 730,  # $32.85 (only for internet access)
        'total_per_month': '$91/month (but no per-GB charge for S3)'
    },
    'monthly_savings': '$90+ (scales with data volume)'
}

# Deploy essential endpoints
essential_endpoints = [
    ('com.amazonaws.us-east-1.s3', 'Gateway'),            # FREE - highest impact
    ('com.amazonaws.us-east-1.dynamodb', 'Gateway'),      # FREE
    ('com.amazonaws.us-east-1.glue', 'Interface'),        # Needed for Glue in VPC
    ('com.amazonaws.us-east-1.logs', 'Interface'),        # CloudWatch Logs
    ('com.amazonaws.us-east-1.monitoring', 'Interface'),  # CloudWatch Metrics
    ('com.amazonaws.us-east-1.sts', 'Interface'),         # AssumeRole calls
    ('com.amazonaws.us-east-1.secretsmanager', 'Interface'),  # Credential retrieval
]

for service, endpoint_type in essential_endpoints:
    if endpoint_type == 'Gateway':
        ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName=service,
            VpcEndpointType='Gateway',
            RouteTableIds=['rtb-private-compute', 'rtb-private-data']
        )
    else:
        ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName=service,
            VpcEndpointType='Interface',
            SubnetIds=['subnet-private-1a', 'subnet-private-1b'],
            SecurityGroupIds=['sg-vpc-endpoints'],
            PrivateDnsEnabled=True
        )
```

---

## Troubleshooting Connectivity Issues

```python
# Common data platform connectivity issues and resolution

troubleshooting_guide = {
    "Glue job timeout (stuck at 'starting')": {
        "cause": "Glue ENI cannot reach Glue service or S3",
        "fix": [
            "Add S3 Gateway endpoint to private subnet route table",
            "OR add NAT Gateway route to private subnet",
            "Verify Glue SG has self-referencing all-TCP rule",
            "Verify subnet has available IP addresses (need 2+ for ENIs)"
        ]
    },
    "Glue cannot connect to RDS": {
        "cause": "Security group or routing issue",
        "fix": [
            "RDS SG must allow inbound from Glue SG on DB port",
            "Glue connection subnet must route to RDS subnet",
            "Test with glue.test_connection() for specific error",
            "Check RDS is not in public subnet with no route back"
        ]
    },
    "EMR nodes cannot download packages": {
        "cause": "No internet access from private subnet",
        "fix": [
            "Add NAT Gateway route for internet access",
            "OR use custom AMI with pre-installed packages",
            "OR use bootstrap action with S3-hosted packages"
        ]
    },
    "Redshift COPY from S3 slow or failing": {
        "cause": "Traffic routing through internet instead of VPC",
        "fix": [
            "Enable Enhanced VPC Routing on cluster",
            "Ensure S3 Gateway endpoint in Redshift subnet route table",
            "Check Redshift SG allows outbound HTTPS (443)"
        ]
    },
    "Cross-account access not working": {
        "cause": "DNS resolution or routing issue",
        "fix": [
            "Verify Transit Gateway propagation includes both VPCs",
            "Check route tables have correct routes to peer CIDR",
            "Verify security groups reference cross-account SG by ID",
            "Ensure DNS resolution across accounts (PHZ association)"
        ]
    }
}
```

---

## Interview Tips

> **Tip 1:** "Design VPC architecture for a data platform" — "Three-tier subnets: public (bastion + NAT), private-compute (Glue/EMR/Lambda with /20 for scaling), private-data (RDS/Redshift, no internet). S3 Gateway endpoint on all private route tables (free, eliminates NAT for data-heavy S3 access). Security groups reference each other (Glue SG → RDS SG). VPC endpoints for Glue, CloudWatch, STS, Secrets Manager. This isolates data workloads while maintaining connectivity."

> **Tip 2:** "How do you troubleshoot Glue jobs stuck in 'starting' state?" — "Three-step checklist: (1) Self-referencing security group rule (Glue ENIs talk to each other — all TCP inbound from itself). (2) Route to S3 — either Gateway endpoint or NAT Gateway in the route table. (3) Available IPs in the subnet (Glue needs 2+ ENIs). Most common: people forget the self-referencing rule. Use VPC Flow Logs to see rejected packets if still stuck."

> **Tip 3:** "How do you reduce VPC networking costs for data workloads?" — "Three high-impact changes: (1) S3 Gateway endpoint eliminates NAT processing charges ($0.045/GB saved on all S3 traffic — huge for data platforms). (2) Keep Glue/EMR in same AZ as data sources (cross-AZ is $0.01/GB). (3) Use Transit Gateway route table segmentation to avoid unnecessary data paths. For a platform moving 10 TB/month through S3, the Gateway endpoint alone saves $450/month."
