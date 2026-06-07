---
title: "NiFi Controller Services - Intermediate Concepts"
topic: nifi
subtopic: controller-services
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [nifi, controller-services, caching, lookup, ssl, aws-credentials]
---

# NiFi Controller Services — Intermediate Concepts

## Distributed Map Cache

A cluster-wide key-value store for deduplication, lookups, and cross-processor communication:

```mermaid
graph TD
    subgraph "Distributed Map Cache Architecture"
        SERVER[DistributedMapCacheServer<br>Port: 4557<br>Runs on one node<br>Stores key-value pairs]
        
        CLIENT1[DistributedMapCacheClientService<br>Node 1 processors]
        CLIENT2[DistributedMapCacheClientService<br>Node 2 processors]
        CLIENT3[DistributedMapCacheClientService<br>Node 3 processors]
    end
    
    CLIENT1 --> SERVER
    CLIENT2 --> SERVER
    CLIENT3 --> SERVER
    
    style SERVER fill:#fff9c4
```

```
# Server configuration:
DistributedMapCacheServer:
  Port: 4557
  Max Cache Entries: 1000000
  Eviction Strategy: Least Recently Used (LRU)
  Persistence Directory: /opt/nifi/cache-data   # Survives restarts!

# Client configuration (used by processors):
DistributedMapCacheClientService:
  Server Hostname: nifi-node-1
  Server Port: 4557
  Communication Timeout: 30 sec

# Used by:
# - DetectDuplicate (deduplication via cache)
# - Wait/Notify (cross-processor signaling)
# - Custom processors (shared state)
```

### DetectDuplicate with Cache

```
DetectDuplicate:
  Distributed Cache Service: DistributedMapCacheClientService
  Cache Entry Identifier: ${kafka.key}    # Unique business key
  Age Off Duration: 24 hours              # Entries expire after 24h
  Cache The Entry Identifier: true
  
# How it works:
# 1. Check if kafka.key exists in cache
# 2. If exists → route to "duplicate" relationship
# 3. If not exists → add to cache → route to "non-duplicate"
# 4. After 24h: entry expires → same key would be treated as new
```

## Lookup Services

Services that provide data for LookupRecord enrichment:

### SimpleDatabaseLookupService

```
SimpleDatabaseLookupService:
  Database Connection Pooling Service: PostgreSQL_Pool
  Table Name: dim_customer
  Lookup Key Column(s): customer_id
  Lookup Value Column(s): customer_name, email, segment, tier
  
# Used in LookupRecord processor:
# FlowFile record has: {"customer_id": "C001", "amount": 99.99}
# Lookup adds: {"customer_id": "C001", "customer_name": "Alice", 
#               "email": "alice@co.com", "segment": "enterprise", 
#               "tier": "gold", "amount": 99.99}
```

### RestLookupService

```
RestLookupService:
  URL: https://api.company.com/customers/${lookup_key}
  Record Reader: JsonTreeReader
  Record Path: /data
  
# Makes REST API call per lookup
# CAUTION: Can be slow for high-volume lookups!
# Better: preload to cache or database
```

### PropertiesFileLookupService (Static Mappings)

```
PropertiesFileLookupService:
  Configuration File: /opt/nifi/conf/region_mapping.properties
  
# region_mapping.properties:
# US-EAST=North America
# US-WEST=North America  
# EU-WEST=Europe
# AP-SOUTH=Asia Pacific

# Fast static lookups (loaded in memory, refreshed on file change)
```

## SSL Context Service

For encrypted connections (HTTPS, SFTP, Kafka SSL):

```
StandardSSLContextService:
  Keystore Filename: /opt/nifi/certs/nifi-keystore.jks
  Keystore Password: ${keystore_password}
  Keystore Type: JKS
  Truststore Filename: /opt/nifi/certs/nifi-truststore.jks
  Truststore Password: ${truststore_password}
  Truststore Type: JKS
  TLS Protocol: TLS 1.2

# Referenced by:
# - InvokeHTTP (HTTPS calls)
# - ConsumeKafka (Kafka SSL/TLS)
# - PutSFTP (SFTP over SSL)
# - Site-to-Site (cluster communication)
```

## AWS Credentials Service

```
AWSCredentialsProviderControllerService:
  # Option 1: Static credentials (not recommended for production):
  Access Key ID: AKIAIOSFODNN7EXAMPLE
  Secret Access Key: ${aws_secret_key}
  
  # Option 2: IAM Role (recommended for EC2/ECS):
  Use Default Credentials Provider Chain: true
  # NiFi uses instance role → no credentials in config!
  
  # Option 3: Assume Role:
  Assume Role ARN: arn:aws:iam::123456789:role/nifi-data-pipeline
  Assume Role Session Name: nifi-etl
  
# Referenced by ALL S3/SQS/SNS/Kinesis processors
```

## Controller Service Dependencies

Services can reference other services:

```mermaid
graph TD
    SSL[SSLContextService<br>Certs + Keys]
    
    DB[DBCPConnectionPool<br>References: SSL<br>For encrypted DB connections]
    
    KAFKA[KafkaClientService<br>References: SSL<br>For Kafka SSL/TLS]
    
    SCHEMA[HortonworksSchemaRegistry<br>References: SSL<br>HTTPS to registry]
    
    READER[AvroReader<br>References: SCHEMA<br>Gets schemas from registry]
    
    SSL --> DB
    SSL --> KAFKA
    SSL --> SCHEMA
    SCHEMA --> READER
    
    style SSL fill:#ffcdd2
    style SCHEMA fill:#fff9c4
```

**Enable order matters:**
1. Enable SSL first (no dependencies)
2. Enable Schema Registry (depends on SSL)
3. Enable Readers/Writers (depend on Schema Registry)
4. Enable DB Pool (depends on SSL)
5. Start processors (depend on all services)

## Parameter Contexts for Controller Services

```
# Parameter Context: "production-db"
#   db.host = prod-db.company.com
#   db.port = 5432
#   db.name = warehouse
#   db.user = nifi_etl
#   db.password = *** (sensitive!)

# Controller Service uses parameters:
DBCPConnectionPool:
  Database Connection URL: jdbc:postgresql://#{db.host}:#{db.port}/#{db.name}
  Database User: #{db.user}
  Password: #{db.password}

# Switch environments by changing Parameter Context:
# DEV: db.host = dev-db.internal → different database
# PROD: db.host = prod-db.company.com → production database
# Same service config, different parameter values!
```

## Interview Tips

> **Tip 1:** "How do you handle database connections in NiFi?" — DBCPConnectionPool controller service. Configure ONCE with connection URL, credentials, pool size. ALL database processors (PutDatabaseRecord, ExecuteSQLRecord, LookupRecord) reference this single service. Pool manages connection lifecycle — borrow, return, validate. Set Max Connections >= sum of all referencing processors' concurrent tasks.

> **Tip 2:** "How does NiFi integrate with Schema Registry?" — HortonworksSchemaRegistry (or AvroSchemaRegistry) controller service connects to Confluent/Hortonworks registry. Record Readers/Writers reference this service to look up schemas by name. Processors specify schema via `${attribute}` for dynamic resolution. Enables schema evolution — new versions in registry, no NiFi flow changes.

> **Tip 3:** "How do you do deduplication across a NiFi cluster?" — DistributedMapCacheServer (runs on one node) + DistributedMapCacheClient (on each node). DetectDuplicate processor uses the cache to check if a key was seen before. All nodes share the same cache → cluster-wide deduplication. Set Age Off Duration for cache entries to prevent unbounded growth.
