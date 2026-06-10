---
title: "AWS VPC - Scenario Questions"
topic: aws-services
subtopic: vpc
content_type: scenario_question
tags: [aws, vpc, interview, scenarios, networking]
---

# Scenario Questions — AWS VPC

<article data-difficulty="junior">

## 🟢 Junior: Glue Job Can't Connect to RDS

**Scenario:** Your Glue ETL job needs to read from an RDS PostgreSQL instance in a private subnet. The job fails with "Connection timed out." What's wrong and how do you fix it?

<details>
<summary>✅ Solution</summary>

**Root cause:** Glue runs outside your VPC by default. It can't reach private resources without a VPC connection.

**Fix: Configure Glue Connection with VPC settings**

```python
glue.create_connection(
    ConnectionInput={
        'Name': 'rds-vpc-connection',
        'ConnectionType': 'JDBC',
        'ConnectionProperties': {
            'JDBC_CONNECTION_URL': 'jdbc:postgresql://mydb.xxx.rds.amazonaws.com:5432/production',
            'USERNAME': 'glue_user',
            'PASSWORD': '...'
        },
        'PhysicalConnectionRequirements': {
            'SubnetId': 'subnet-abc123',           # Same subnet as RDS (or routable)
            'SecurityGroupIdList': ['sg-glue123'],  # Security group allowing outbound
            'AvailabilityZone': 'us-east-1a'
        }
    }
)
```

**Security group rules needed:**
- **Glue SG (sg-glue123):** Outbound → TCP 5432 to RDS SG
- **RDS SG:** Inbound → TCP 5432 FROM sg-glue123
- **Glue SG:** Also needs outbound to S3 (via VPC endpoint or NAT Gateway)

**Additional requirement:** Glue in a VPC needs a **NAT Gateway** or **S3 VPC Endpoint** to access S3 (for reading/writing data and accessing the Glue Catalog service endpoint).

**Checklist when Glue can't connect to private resources:**
1. ✅ Glue Connection configured with correct VPC/subnet
2. ✅ Security group allows Glue → RDS on the correct port
3. ✅ RDS security group allows inbound from Glue SG
4. ✅ NAT Gateway or VPC endpoints for S3/Glue service access
5. ✅ Subnet has route to NAT Gateway (if using NAT)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is an Amazon VPC and why does it matter for data engineering?**
A: A VPC (Virtual Private Cloud) is a logically isolated network in AWS where you control IP ranges, subnets, routing, and security. For data engineers, VPC matters because it determines network access to databases (RDS, Redshift), streaming services (MSK), and compute (EMR, Glue), and provides the network isolation required for security compliance.

**Q: What is the difference between public and private subnets?**
A: A public subnet has a route to an Internet Gateway, allowing resources with public IPs to send/receive internet traffic. A private subnet has no route to the internet — resources communicate within the VPC or via NAT Gateway for outbound-only internet access. Production databases and compute should live in private subnets.

**Q: What is a NAT Gateway and when do you need it?**
A: A NAT Gateway enables resources in private subnets to initiate outbound internet connections (e.g., downloading Python packages, calling external APIs) without being directly reachable from the internet. It's required when your Lambda, EMR, or Glue resources in private subnets need to reach AWS services or the internet.

**Q: What are VPC Endpoints and why are they important for data pipelines?**
A: VPC Endpoints allow private connectivity to AWS services (S3, DynamoDB, Glue, STS, Secrets Manager) without routing traffic through the public internet. Gateway Endpoints are for S3 and DynamoDB (free); Interface Endpoints (powered by PrivateLink) are for most other AWS services (hourly + data processing cost). Using VPC Endpoints improves security and often reduces data transfer costs.

**Q: What is the difference between Security Groups and NACLs (Network ACLs)?**
A: Security Groups are stateful firewalls at the resource level — a rule allowing inbound traffic automatically allows the response outbound. NACLs are stateless firewalls at the subnet level — you must explicitly allow inbound AND outbound traffic separately. Security Groups are the primary access control mechanism; NACLs add a coarse subnet-level layer.

**Q: How do you allow a Glue job to access an RDS database in a private subnet?**
A: Create a Glue JDBC connection specifying the VPC, subnet, and security group. The security group attached to Glue must allow outbound to the RDS port (e.g., 5432 for PostgreSQL). The RDS security group must allow inbound from the Glue security group. Both resources must be in the same VPC or connected VPCs (via peering or Transit Gateway).

**Q: What is VPC Peering and when would you use it?**
A: VPC Peering connects two VPCs privately so resources can communicate using private IP addresses. Use it when your data pipeline in one VPC needs to access a database or service in another VPC (e.g., a shared services VPC). Peering does not support transitive routing — for hub-and-spoke architectures with many VPCs, use AWS Transit Gateway.

**Q: What is AWS PrivateLink and how does it relate to VPC Interface Endpoints?**
A: PrivateLink is the technology powering Interface Endpoints — it creates an Elastic Network Interface (ENI) in your VPC with a private IP that routes traffic to an AWS service or a partner service without traversing the internet. It's the preferred secure connectivity model for accessing AWS APIs from private subnets.

---

## 💼 Interview Tips

- Always recommend private subnets for production data resources: RDS, Redshift, MSK, and EMR should never have public IPs. Interviewers expect this as a baseline security stance.
- Senior interviewers probe VPC Endpoint configuration: S3 Gateway Endpoints should be configured in every VPC with data workloads — they're free, improve security (traffic stays within AWS network), and can reduce NAT Gateway data processing costs significantly.
- Demonstrate understanding of the most common network misconfiguration: Glue or Lambda in a private subnet cannot reach S3 without either a NAT Gateway or an S3 VPC Endpoint. Knowing this saves hours of debugging in production.
- Mention the security group self-reference pattern for cluster communication: EMR and MSK nodes need to communicate with each other. Add a security group rule allowing all traffic from the same security group (self-referencing) to enable intra-cluster communication.
- Know DNS resolution considerations for Interface Endpoints: `enableDnsHostnames` and `enableDnsSupport` must be enabled on the VPC for private DNS names of AWS services to resolve to the Interface Endpoint IP — a common gotcha.
- Avoid the mistake of using VPC Peering for large-scale multi-VPC connectivity: peering doesn't scale to many VPCs (no transitive routing). Describe Transit Gateway as the scalable alternative for enterprise data platform networking.
