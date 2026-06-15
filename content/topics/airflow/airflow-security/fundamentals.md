---
title: "Airflow Security - Fundamentals"
topic: airflow
subtopic: airflow-security
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, security, rbac, authentication, encryption, fernet]
---

# Airflow Security — Fundamentals

## 🎯 Analogy

Think of Airflow security like a hospital building: the front desk verifies who you are (authentication), your badge determines which floors you can access (RBAC), and patient records are encrypted at rest so even a stolen file cabinet is useless without the key.

---

## Authentication Backends Overview

Airflow supports multiple authentication mechanisms configured in `webserver_config.py`. The backend controls how users prove their identity when logging into the UI or API.

| Backend | Best For | How It Works |
|---|---|---|
| **Password** | Dev/small teams | Username + password stored in Airflow DB (hashed) |
| **LDAP** | Enterprises with AD | Validates credentials against LDAP/Active Directory server |
| **OAuth2** | Cloud-native orgs | Delegates to Google, GitHub, Okta — SSO flow |
| **Kerberos** | Hadoop/on-prem enterprises | Ticket-based, no password transmitted |

The default for new installs is the **FAB (Flask-AppBuilder) database auth** — users are created in Airflow's own database.

```python
# webserver_config.py — default password auth
from flask_appbuilder.security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False  # Only admins create accounts
AUTH_USER_REGISTRATION_ROLE = "Viewer"
```

---

## Role-Based Access Control (RBAC)

Airflow uses **Flask-AppBuilder (FAB)** as its security framework. Every user is assigned one or more roles, and roles contain a set of permissions.

### Built-in Roles

| Role | What They Can Do |
|---|---|
| **Admin** | Everything — manage users, connections, pools, all DAGs |
| **Op** | Run DAGs, manage connections/variables/pools, cannot manage users |
| **User** | View and trigger DAGs, cannot modify connections or variables |
| **Viewer** | Read-only — see DAG runs and logs, cannot trigger anything |
| **Public** | Unauthenticated access (disabled by default) |

```
Admin > Op > User > Viewer > Public
```

> **Interview Tip:** Know that Admin is the only role that can create/delete users. If you accidentally revoke Admin from yourself, you need to use the CLI to recover.

### FAB Security Model

FAB controls access through **permissions on views and models**:
- Permissions: `can_read`, `can_edit`, `can_create`, `can_delete`
- Resources: `DAG`, `DagRun`, `Task`, `Connection`, `Variable`, `Pool`, `Log`

```python
# Check what permissions a role has via CLI
airflow roles list
airflow roles export roles.json
```

---

## webserver_config.py Basics

This file is the central place to configure Airflow's web security. Located at `$AIRFLOW_HOME/webserver_config.py`.

```python
# webserver_config.py
import os
from flask_appbuilder.security.manager import AUTH_DB

# Authentication type
AUTH_TYPE = AUTH_DB

# Allow user self-registration? (False in production)
AUTH_USER_REGISTRATION = False

# Default role for new registered users
AUTH_USER_REGISTRATION_ROLE = "Viewer"

# Session cookie security
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None  # CSRF tokens don't expire

# Secret key for Flask sessions — CHANGE THIS IN PRODUCTION
SECRET_KEY = os.environ.get("WEBSERVER_SECRET_KEY", "changeme")
```

> **Security Warning:** Never use the default `SECRET_KEY`. If it leaks, attackers can forge session cookies and bypass authentication entirely.

---

## Fernet Key — Encryption for Connections and Variables

Airflow stores connections and variables in the metadata database. Without a Fernet key, **passwords are stored in plaintext**.

### What Is Fernet?

Fernet is a symmetric encryption scheme from the `cryptography` library. It uses AES-128-CBC with HMAC-SHA256. Airflow uses it to encrypt:
- Connection `password` and `extra` fields
- Variable `value` fields (only if marked sensitive)

### Generating a Fernet Key

```bash
# Generate a key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Output: ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
```

### Configuring the Fernet Key

```ini
# airflow.cfg
[core]
fernet_key = ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
```

Or via environment variable (preferred):

```bash
export AIRFLOW__CORE__FERNET_KEY="ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg="
```

### What Happens Without a Fernet Key?

```
# If no fernet_key is set, connections look like this in the DB:
password: "my_database_password"  # PLAINTEXT — visible to anyone with DB access

# With fernet_key:
password: "gAAAAABk..."  # Encrypted — useless without the key
```

> **Critical:** If you lose the Fernet key, **all encrypted connections and variables become permanently unrecoverable**. Treat it like a root password.

---

## Why Encrypt Secrets at Rest?

The metadata database (typically PostgreSQL) stores everything about your Airflow instance. Without encryption:

1. A rogue employee with DB read access sees all passwords
2. A database backup file contains all credentials in plaintext
3. A SQL injection vulnerability exposes every connection

With Fernet encryption, the DB only stores ciphertext — the Fernet key must be separately protected (environment variable, secrets manager, Kubernetes secret).

---

## Key Takeaways

- Airflow supports Password, LDAP, OAuth2, and Kerberos auth backends
- FAB provides role-based access with 5 built-in roles (Admin > Op > User > Viewer > Public)
- `webserver_config.py` is the central security configuration file
- Fernet key encrypts connection passwords and variable values in the metadata DB
- Never run production Airflow without a Fernet key — plaintext credentials are a critical vulnerability
