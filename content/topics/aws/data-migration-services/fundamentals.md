---
title: "AWS Data Migration Services Fundamentals"
description: "Overview of AWS DMS, SCT, Snow Family, and DataSync for migrating databases and files to AWS"
content_type: study_material
topic: aws
subtopic: data-migration-services
layer: fundamentals
difficulty_level: junior
tags: [aws, dms, database-migration, sct, snowball, datasync, migration, data-engineering]
---

# AWS Data Migration Services Fundamentals

## Why Data Migration Matters for Data Engineers

Data migrations are among the most high-stakes projects in a data engineer's career. Moving databases, data warehouses, or file systems to AWS involves risk of data loss, downtime, and compatibility issues. Understanding AWS's migration toolset helps you design safer, faster migrations.

AWS provides a family of services specifically for migration:
- **AWS Database Migration Service (DMS)** — database migrations with continuous replication
- **AWS Schema Conversion Tool (SCT)** — schema translation between database engines
- **AWS Snow Family** — physical devices for bulk data transfer when network is too slow
- **AWS DataSync** — file and object storage migration over the network

---

## AWS Database Migration Service (DMS) Overview

AWS DMS is a managed service that migrates databases to AWS quickly and securely. During migration, the source database remains fully operational, minimizing downtime.

### Core Concepts

**Replication Instance:**
A managed EC2 instance that runs the DMS replication software. It reads data from the source, applies transformations, and writes to the target.

```
Source Database  →  [DMS Replication Instance]  →  Target Database
(on-premises)         (in AWS VPC)                  (AWS RDS/Aurora/Redshift)
```

**Endpoints:**
Connection definitions for the source and target databases. Each endpoint specifies:
- Database engine type (MySQL, PostgreSQL, Oracle, SQL Server, etc.)
- Hostname, port, username, password
- SSL settings, extra connection attributes

**Replication Task:**
Defines what to migrate and how. A task references one source endpoint and one target endpoint, and specifies the tables to migrate and the migration type.

### Supported Sources and Targets

**Source databases (on-premises or cloud):**
- Oracle Database
- Microsoft SQL Server
- MySQL / MariaDB
- PostgreSQL
- IBM Db2
- SAP ASE (Sybase)
- MongoDB / DocumentDB
- Amazon Aurora (for cross-region migration)
- Amazon RDS (any engine)
- S3 (as a source for bulk load)
- Azure SQL Database / Azure SQL Managed Instance

**Target databases:**
- Amazon Aurora (MySQL and PostgreSQL compatible)
- Amazon RDS (any engine)
- Amazon Redshift
- Amazon DynamoDB
- Amazon S3
- Amazon Kinesis Data Streams
- Amazon MSK (Kafka)
- Amazon OpenSearch Service
- Amazon DocumentDB

### DMS Architecture Components

```
┌─────────────────────────────────────────────┐
│                AWS Region                   │
│                                             │
│  Source     DMS Replication    Target       │
│  Endpoint → Instance         → Endpoint     │
│                                             │
│  [Source DB]  [Repl. Instance]  [Target DB] │
│  on-prem/EC2  m4.large (e.g.)   RDS/Aurora  │
└─────────────────────────────────────────────┘
```

**Network connectivity options:**
- Direct internet connection (not recommended for production)
- AWS Direct Connect (dedicated network connection, recommended)
- VPN over internet
- For cloud-to-cloud: within VPC or VPC peering

---

## DMS Migration Types

### Full Load

Copies all existing data from source to target at a point in time.

- Best for: initial migration of data where brief downtime is acceptable
- The source remains readable but new changes after the snapshot point are not captured
- Fastest type — reads full tables in parallel

**When to use:** Development migrations, one-time historical data loads, migrations where you can pause the source system briefly.

### CDC (Change Data Capture) Only

Captures and replicates only ongoing changes (INSERT, UPDATE, DELETE) after a specified point.

- Reads changes from the source database's transaction log
- Applies them to the target in near-real-time (typically seconds of lag)
- **Does not** load initial data — assumes target already has a baseline

**When to use:** After completing a full load, to keep the target in sync while you finalize cutover. Also useful for ongoing replication (not just migration).

### Full Load + CDC (Most Common Pattern)

The standard migration approach:

1. **Full Load phase:** Copies all existing data to the target
2. **CDC phase:** Begins capturing changes from the source during and after the full load
3. **Catch-up:** DMS applies buffered CDC changes to bring the target up to date
4. **Cutover:** Once the target is in sync (lag ≈ 0), switch application traffic to the target

```
Time →
|--Full Load (hours)--|--CDC catch-up (minutes)--|--Cutover--|
Source: Active         Active                     Switch traffic
Target: Loading        Catching up                Now primary
```

---

## AWS Schema Conversion Tool (SCT)

When migrating between different database engines (heterogeneous migration), the schema must be translated. SCT automates this process.

### What SCT Does

- **Analyzes** the source database schema (tables, indexes, views, stored procedures, functions, triggers)
- **Converts** the schema to the target database's dialect
- **Reports** conversion complexity and items that cannot be automatically converted
- **Generates** DDL scripts for the target database

### SCT Assessment Reports

Before starting a migration, SCT generates an assessment report:

```
Schema Conversion Assessment Summary
=====================================
Conversion actions:
  Automatic conversion:     847 items (89%)
  With minor changes:        65 items (7%)
  Requires manual action:    40 items (4%)

Estimated effort: 35 person-days
```

Items requiring manual action typically include:
- Stored procedures with engine-specific syntax
- Proprietary functions with no direct equivalent
- Custom data types
- Complex triggers

### Heterogeneous vs. Homogeneous Migrations

| Type | Example | SCT Needed? | DMS Complexity |
|------|---------|------------|----------------|
| Homogeneous | MySQL → MySQL RDS | No | Low |
| Heterogeneous | Oracle → Aurora PostgreSQL | Yes | High |
| Data warehouse | Teradata → Redshift | Yes (SCT for DW) | Medium |

**SCT for data warehouse migrations** also converts ETL code (BTEQ scripts, stored procedures) and has specific features for Teradata, Oracle DW, and Netezza migrations to Redshift.

---

## AWS Snow Family

When network bandwidth is insufficient for large-scale migrations, AWS provides physical devices to ship data.

### Snowball Edge Storage Optimized

**Specs:**
- 80 TB of usable storage
- 40 vCPUs of compute
- 10 GbE and 25 GbE network interfaces
- Ruggedized, tamper-evident enclosure

**Use case:** Migrate 10–80 TB of data. AWS ships the device to you; you copy data onto it and ship it back. AWS uploads data to your S3 bucket.

**Transfer speed comparison:**
```
100 Mbps internet:  80 TB / (100 Mbps = 12.5 MB/s) = ~75 days (unacceptable)
Snowball Edge:      Load locally at ~250 MB/s + shipping = ~6 days total
```

### Snowball Edge Compute Optimized

**Specs:**
- 42 TB of usable storage
- 52 vCPUs, optional NVIDIA Tesla V100 GPU
- Runs EC2 instances and Lambda functions on the device

**Use case:** Edge computing in disconnected environments (factories, ships, remote sites) + data transfer. Also used for pre-processing data before transfer to AWS.

### AWS Snowcone

**Specs:**
- 8 TB of usable storage
- 2 vCPUs, 4 GB RAM
- 4.5 lbs, fits in a backpack

**Use case:** Small-scale data transfer or edge computing in highly constrained environments (space-limited, power-limited).

### AWS Snowmobile

**Specs:**
- 100 PB per Snowmobile
- 45-foot shipping container pulled by a semi truck
- AWS manages logistics, security, and data transfer

**Use case:** Exabyte-scale migrations. AWS drives the truck to your data center, you load it, they drive it to AWS.

**Cost-benefit threshold:** Generally worth considering when you have >10 PB to transfer and network transfer would take years.

### Snow Family Decision Guide

```
Data Volume      → Device
< 1 TB           → Direct internet transfer
1 TB – 10 TB     → Direct Connect or internet (evaluate based on bandwidth)
10 TB – 80 TB    → Snowball Edge Storage Optimized (single device)
80 TB – 8 PB     → Multiple Snowball Edge devices
> 10 PB          → Snowmobile
```

### Snow Family Transfer Process

1. **Order:** Request device through AWS Console
2. **Device delivery:** AWS ships to your location (5-7 business days)
3. **Data copy:** Connect device to your network, copy data using AWS OpsHub or S3-compatible CLI
4. **Return:** Ship device back to AWS (prepaid label included)
5. **Upload:** AWS ingests data to your S3 bucket, sends completion notification
6. **Secure erase:** Device is cryptographically wiped and reused

---

## AWS DataSync

DataSync is a fully managed data transfer service for moving large amounts of file and object data over the network to AWS.

### What DataSync Transfers

- **File systems:** NFS, SMB, HDFS
- **Object storage:** S3-compatible endpoints
- **AWS services:** S3, EFS, FSx for Windows, FSx for Lustre

### DataSync vs. DMS

| Characteristic | DataSync | DMS |
|---------------|---------|-----|
| Data type | Files, objects | Databases (structured) |
| Protocol | NFS, SMB, HDFS, S3 | Database protocols (JDBC, etc.) |
| Transformation | None (byte-for-byte copy) | Schema conversion, filtering |
| CDC | No | Yes |
| Use case | File system migration | Database migration |

### DataSync Architecture

```
Source NAS/HDFS → [DataSync Agent (VM)] → [AWS DataSync Service] → Target S3/EFS
   On-premises        On-premises network              AWS
```

**DataSync Agent:** A virtual machine deployed on-premises that reads from the source and sends data to the DataSync service. The agent handles compression, encryption, and retry logic.

### DataSync Key Features

**Automated data integrity verification:**
DataSync verifies that data transferred to the target matches the source using checksums. This is critical for compliance migrations.

**Bandwidth throttling:**
Control how much network bandwidth DataSync uses during business hours:
```json
{
  "ScheduleExpression": "rate(1 day)",
  "Options": {
    "BytesPerSecond": 10485760  // 10 MB/s during business hours
  }
}
```

**Incremental transfers:**
After the initial transfer, DataSync can run incrementally — transferring only files that have changed since the last run. This is useful for keeping an S3 copy of an on-premises file system in sync during a transition period.

**Filtering:**
Include or exclude files based on patterns:
```json
{
  "Includes": [{"FilterType": "SIMPLE_PATTERN", "Value": "*.csv|*.parquet"}],
  "Excludes": [{"FilterType": "SIMPLE_PATTERN", "Value": "*_temp*"}]
}
```

### DataSync Use Cases for Data Engineers

1. **Migrate on-premises Hadoop HDFS to S3:** DataSync has native HDFS support
2. **Sync NAS file shares to S3 during cutover period**
3. **Migrate file-based ETL inputs/outputs from on-prem to AWS**
4. **Move large datasets from one S3 bucket to another (cross-account, cross-region)**

---

## Choosing the Right Migration Service

```
What are you migrating?
│
├─ Structured database (rows/columns)?
│   └─ AWS DMS (+SCT if different engine)
│
├─ Files, file systems, or HDFS?
│   ├─ Network available and data < a few TB?
│   │   └─ AWS DataSync
│   └─ Data > 10 TB or limited bandwidth?
│       └─ AWS Snow Family (Snowball Edge)
│
└─ Large unstructured data with very limited/no network?
    └─ AWS Snow Family
```

---

## Key Takeaways

- DMS has three components: replication instances, endpoints, and replication tasks
- Three migration types: Full Load only, CDC only, Full Load + CDC (most common for live migrations)
- SCT translates schemas between different database engines — essential for heterogeneous migrations (Oracle → PostgreSQL, SQL Server → MySQL)
- Snow Family provides physical devices for bulk data transfer when network is insufficient
- DataSync is for file and object migrations (NFS, SMB, HDFS to S3/EFS), not database migrations
- DMS is for structured database migrations with support for CDC (ongoing change capture)
