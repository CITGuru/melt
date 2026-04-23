# Service authentication

Sync's background loops (CDC reader + policy refresh) call Snowflake on a timer with no human in the loop. They authenticate with a dedicated service credential that lives in `[snowflake]` — **separate from** the credentials each driver supplies at login time.

## Pick one credential mechanism

| Field | When to use |
|---|---|
| `pat` | Inline PAT. Simplest; good for docker-compose with env substitution. |
| `pat_file` | File path. Preferred for Kubernetes Secret volume mounts. |
| `private_key` | Inline RSA PEM. Rarely used — PEM line-wrapping in TOML is awkward. |
| `private_key_file` | Path to `.p8` / `.pem`. Production-grade; what Snowflake's docs lead with. |

Exactly one non-empty. More than one = config error at startup. Zero = config error when sync starts (proxy-only mode doesn't need it).

## Snowflake-side setup (one-time)

Create a dedicated service user with the minimum grants sync needs:

```sql
USE ROLE SECURITYADMIN;

-- Dedicated role + warehouse for sync so compute billing and
-- permission changes stay isolated from user workloads.
CREATE ROLE IF NOT EXISTS MELT_SYNC_ROLE;
CREATE WAREHOUSE IF NOT EXISTS MELT_SYNC_WH
    WAREHOUSE_SIZE     = XSMALL
    AUTO_SUSPEND       = 60
    AUTO_RESUME        = TRUE;
GRANT USAGE, OPERATE ON WAREHOUSE MELT_SYNC_WH TO ROLE MELT_SYNC_ROLE;

-- Service user
CREATE USER IF NOT EXISTS MELT_SYNC_USER
    DEFAULT_ROLE         = MELT_SYNC_ROLE
    DEFAULT_WAREHOUSE    = MELT_SYNC_WH
    MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE MELT_SYNC_ROLE TO USER MELT_SYNC_USER;

-- Grants (read side)
GRANT USAGE          ON DATABASE ANALYTICS                  TO ROLE MELT_SYNC_ROLE;
GRANT USAGE          ON ALL SCHEMAS IN DATABASE ANALYTICS   TO ROLE MELT_SYNC_ROLE;
GRANT USAGE          ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE MELT_SYNC_ROLE;
GRANT SELECT         ON ALL TABLES IN DATABASE ANALYTICS    TO ROLE MELT_SYNC_ROLE;
GRANT SELECT         ON FUTURE TABLES IN DATABASE ANALYTICS TO ROLE MELT_SYNC_ROLE;

-- Policy-refresh loop reads this:
-- (Standard edition: account_usage.policy_references; Enterprise: information_schema)
GRANT SELECT ON INFORMATION_SCHEMA.POLICY_REFERENCES TO ROLE MELT_SYNC_ROLE;

-- Grants (write side — auto-create Snowflake STREAMs on bootstrap)
GRANT CREATE STREAM ON ALL SCHEMAS IN DATABASE ANALYTICS    TO ROLE MELT_SYNC_ROLE;
GRANT CREATE STREAM ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE MELT_SYNC_ROLE;

-- Optional — required ONLY when [sync.lazy].auto_enable_change_tracking = true.
-- Without this, bootstrap fails loudly for tables that don't already
-- have change tracking on, and operators enable it themselves.
GRANT APPLY CHANGE TRACKING ON ALL SCHEMAS IN DATABASE ANALYTICS
    TO ROLE MELT_SYNC_ROLE;
GRANT APPLY CHANGE TRACKING ON FUTURE SCHEMAS IN DATABASE ANALYTICS
    TO ROLE MELT_SYNC_ROLE;
```

## Option A — PAT

```sql
ALTER USER MELT_SYNC_USER
    ADD PROGRAMMATIC ACCESS TOKEN melt_sync
    EXPIRY_DAYS = 90;
```

Snowflake returns the token once. Paste into `[snowflake].pat` (or drop in a file and point `pat_file` at it).

## Option B — key-pair

```bash
# Operator machine
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out melt_sync.p8
openssl rsa -in melt_sync.p8 -pubout   # copy the BASE64 output
```

```sql
-- Paste the base64 public key (minus the -----BEGIN/END----- lines)
ALTER USER MELT_SYNC_USER SET RSA_PUBLIC_KEY = 'MIIBI...';
```

Then in `melt.toml`:

```toml
[snowflake]
private_key_file = "/etc/melt/melt_sync.p8"
user             = "MELT_SYNC_USER"
role             = "MELT_SYNC_ROLE"
warehouse        = "MELT_SYNC_WH"
```

Melt signs a fresh 1-hour JWT per ~50 minutes, exchanges it for a Snowflake session token, caches.

## Session context (optional but recommended)

```toml
[snowflake]
# ... credential ...
role      = "MELT_SYNC_ROLE"
warehouse = "MELT_SYNC_WH"
database  = ""           # optional; sync uses fully-qualified names
schema    = ""
```

When set, every sync-issued statement includes these fields in the request body and Snowflake executes under them. Omit to inherit the service user's `DEFAULT_*` settings — works, but leaves sync behavior tied to operations on the user's defaults.

## Rotation

- **PAT:** issue a new PAT, swap the value/file, restart melt (or let the token cache expire within 24h).
- **Key-pair:** generate a new key, register its public half on the user, swap the file. The token cache refreshes on its next tick.
