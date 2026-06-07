---
title: "AWS VPC - Intermediate"
topic: aws-services
subtopic: vpc
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, vpc, networking, endpoints, peering]
---

# AWS VPC — Intermediate Concepts

## VPC Endpoints — Access AWS Services Without Internet

### Gateway Endpoints (S3 and DynamoDB only)

```json
{
    "Type": "AWS::EC2::VPCEndpoint",
    "Properties": {
        "VpcId": "vpc-12345",
        "ServiceName": "com.amazonaws.us-east-1.s3",
        "RouteTableIds": ["rtb-private-1", "rtb-private-2"],
        "PolicyDocument": {
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                "Resource": ["arn:aws:s3:::data-lake-*", "arn:aws:s3:::data-lake-*/*"]
            }]
        }
    }
}
```

### Interface Endpoints (Other AWS Services)

```python
import boto3

ec2 = boto3.client('ec2')

# Create interface endpoint for Glue (data engineers need this often)
ec2.create_vpc_endpoint(
    VpcId='vpc-12345',
    ServiceName='com.amazonaws.us-east-1.glue',
    VpcEndpointType='Interface',
    SubnetIds=['subnet-private-1a', 'subnet-private-1b'],
    SecurityGroupIds=['sg-endpoint-access'],
    PrivateDnsEnabled=True  # Access glue.us-east-1.amazonaws.com via private IP
)

# Common endpoints for data platforms:
endpoints_needed = [
    'com.amazonaws.us-east-1.s3',           # Gateway (free)
    'com.amazonaws.us-east-1.dynamodb',     # Gateway (free)
    'com.amazonaws.us-east-1.glue',         # Interface ($0.01/hr/AZ)
    'com.amazonaws.us-east-1.athena',       # Interface
    'com.amazonaws.us-east-1.kinesis-streams',  # Interface
    'com.amazonaws.us-east-1.secretsmanager',   # Interface
    'com.amazonaws.us-east-1.logs',         # Interface (for CloudWatch)
    'com.amazonaws.us-east-1.monitoring',   # Interface (CloudWatch metrics)
    'com.amazonaws.us-east-1.sts',          # Interface (AssumeRole)
]
```

| Endpoint Type | Cost | Supported Services | DNS |
|--------------|------|-------------------|-----|
| Gateway | Free | S3, DynamoDB | Route table entry |
| Interface | $0.01/hr/AZ + data | 100+ services | Private DNS or endpoint-specific |

---

## VPC Peering

```python
# Connect two VPCs for data sharing (same or different accounts)
ec2.create_vpc_peering_connection(
    VpcId='vpc-data-lake',          # Requester VPC
    PeerVpcId='vpc-analytics',       # Accepter VPC
    PeerOwnerId='987654321',         # Different account (optional)
    PeerRegion='us-west-2'           # Cross-region (optional)
)

# After acceptance, add routes in BOTH VPCs:
# Data Lake VPC route table:
ec2.create_route(
    RouteTableId='rtb-data-lake-private',
    DestinationCidrBlock='10.1.0.0/16',  # Analytics VPC CIDR
    VpcPeeringConnectionId='pcx-12345'
)
# Analytics VPC route table:
ec2.create_route(
    RouteTableId='rtb-analytics-private',
    DestinationCidrBlock='10.0.0.0/16',  # Data Lake VPC CIDR
    VpcPeeringConnectionId='pcx-12345'
)
```

**Peering limitations:**
- No transitive routing (A↔B and B↔C doesn't mean A↔C)
- CIDRs cannot overlap
- Max 125 peering connections per VPC
- For complex topologies → use Transit Gateway

---

## Transit Gateway — Hub-and-Spoke Networking

```
        ┌──────────────┐
        │Transit Gateway│
        └──────┬───────┘
    ┌──────────┼──────────┐
    │          │          │
┌───┴───┐ ┌───┴───┐ ┌───┴───┐
│VPC-ETL│ │VPC-DB │ │VPC-BI │
└───────┘ └───────┘ └───────┘
```

All VPCs connect to Transit Gateway — automatic full mesh connectivity without managing N*(N-1)/2 peering connections.

---

## NAT Gateway vs NAT Instance

| Feature | NAT Gateway | NAT Instance |
|---------|-------------|--------------|
| Managed | Yes (AWS) | No (you manage EC2) |
| Bandwidth | Up to 100 Gbps | Instance-type dependent |
| Availability | Multi-AZ (managed) | Single EC2 (you add HA) |
| Cost | $0.045/hr + $0.045/GB | EC2 instance cost |
| Use case | Production | Dev/testing (cost savings) |

> **Data platform consideration:** NAT Gateway cost can be significant for data transfer. If Glue/EMR nodes download large datasets from the internet, NAT charges add up. Use VPC endpoints for AWS services to avoid NAT entirely.

---

## Network ACLs vs Security Groups

| Aspect | Security Group | Network ACL |
|--------|---------------|-------------|
| Level | Instance/ENI | Subnet |
| State | Stateful (return traffic auto-allowed) | Stateless (explicit both directions) |
| Rules | Allow only | Allow and Deny |
| Evaluation | All rules evaluated | Rules evaluated in order (first match) |
| Default | Deny all inbound, allow all outbound | Allow all |

```python
# Security group for Glue connection (access RDS in VPC)
ec2.create_security_group(
    GroupName='glue-rds-access',
    Description='Allow Glue to connect to RDS',
    VpcId='vpc-12345'
)

# Glue needs a self-referencing rule (Glue ENIs talk to each other)
ec2.authorize_security_group_ingress(
    GroupId='sg-glue',
    IpPermissions=[{
        'IpProtocol': 'tcp',
        'FromPort': 0,
        'ToPort': 65535,
        'UserIdGroupPairs': [{'GroupId': 'sg-glue'}]  # Self-reference
    }]
)

# Allow Glue SG to access RDS SG
ec2.authorize_security_group_ingress(
    GroupId='sg-rds',
    IpPermissions=[{
        'IpProtocol': 'tcp',
        'FromPort': 5432,
        'ToPort': 5432,
        'UserIdGroupPairs': [{'GroupId': 'sg-glue'}]
    }]
)
```

---

## VPC Flow Logs

```python
# Enable flow logs for troubleshooting connectivity
ec2.create_flow_log(
    ResourceId='vpc-12345',
    ResourceType='VPC',
    TrafficType='ALL',  # ACCEPT, REJECT, or ALL
    LogDestinationType='s3',
    LogDestination='arn:aws:s3:::vpc-flow-logs-bucket/data-platform/',
    LogFormat='${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}'
)

# Query flow logs with Athena to debug connectivity
flow_log_query = """
SELECT srcaddr, dstaddr, dstport, action, COUNT(*) as packet_count
FROM vpc_flow_logs
WHERE action = 'REJECT'
  AND dstport IN (5432, 3306, 6379)  -- Database ports
  AND day = '2024/01/15'
GROUP BY srcaddr, dstaddr, dstport, action
ORDER BY packet_count DESC
LIMIT 20
"""
```

---

## AWS PrivateLink — Expose Services Privately

```python
# PrivateLink: expose your internal data API to other VPCs/accounts
# without internet, peering, or Transit Gateway

# Provider: create an endpoint service backed by NLB
ec2.create_vpc_endpoint_service_configuration(
    NetworkLoadBalancerArns=['arn:aws:elasticloadbalancing:us-east-1:123456789:loadbalancer/net/data-api/12345'],
    AcceptanceRequired=True  # Manual approval of consumers
)

# Consumer: create interface endpoint to provider's service
ec2.create_vpc_endpoint(
    VpcId='vpc-consumer',
    ServiceName='com.amazonaws.vpce.us-east-1.vpce-svc-12345',
    VpcEndpointType='Interface',
    SubnetIds=['subnet-consumer-1'],
    SecurityGroupIds=['sg-consumer-endpoint']
)
```

---

## Interview Tips

> **Tip 1:** "How do you secure network access for a data platform?" — "Layer defense: (1) VPC endpoints for AWS service access (no internet transit). (2) Security groups on compute (Glue, EMR, Lambda) allow only required ports. (3) Network ACLs as subnet-level backup. (4) No public subnets for data workloads. (5) VPC Flow Logs to detect and audit unexpected traffic patterns. Gateway endpoints for S3/DynamoDB are free and eliminate NAT costs."

> **Tip 2:** "When do you use VPC peering vs Transit Gateway?" — "Peering for 2-3 VPCs with simple connectivity needs (no transitive routing). Transit Gateway for 4+ VPCs, hub-and-spoke topology, or when you need centralized routing policies. Transit Gateway costs $0.05/hr per attachment + $0.02/GB — for high-bandwidth data transfers between VPCs, peering is cheaper since peering data transfer is free within the same AZ."

> **Tip 3:** "How does Glue connect to resources in a VPC?" — "Create a Glue Connection with VPC, subnet, and security group. The security group MUST have a self-referencing rule (Glue creates ENIs that communicate with each other). The subnet needs a route to the target (RDS, Redshift) and either a NAT Gateway or VPC endpoints for S3/Glue service access. Common debugging: Glue job stuck = missing self-referencing rule or no route to S3."
