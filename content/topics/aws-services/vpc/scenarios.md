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
