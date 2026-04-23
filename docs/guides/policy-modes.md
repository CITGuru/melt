# Policy modes

Snowflake's row-access and masking policies don't apply to Lake reads (the data is plain Parquet). Melt has three modes for handling this, configured in `[snowflake.policy]`:

| Mode | What it does | Operator burden | Safety |
|---|---|---|---|
| `passthrough` *(default)* | Sync marks every table that has a policy attached. Router refuses to route any marked table to Lake. | None | Tables with policies always go to Snowflake. Safe. |
| `allowlist` | Only tables in `[snowflake.policy].tables` are Lake-eligible. Everything else passes through. | High — operator audits each table. | Most conservative. New tables don't accidentally leak. |
| `enforce` | Sync translates each row-access policy body into a DuckDB `WHERE` clause and exposes a `<table>__melt_filtered` view. Router rewrites table refs to use the view. | Medium — requires policies to use the supported DSL subset. | Best-effort. Tables whose body uses unsupported DSL fall back to a passthrough marker. |

## `enforce` supported DSL subset

`enforce` supports:

- `CURRENT_ROLE()`
- `CURRENT_USER()`
- `IS_ROLE_IN_SESSION('R')`
- basic comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`)
- `IN (...)`
- boolean operators (`AND`, `OR`, `NOT`)

Custom UDFs and `IS_DATABASE_ROLE_IN_SESSION` keep the table on the passthrough marker as a safe fallback — the router treats them as unknown and forces Snowflake passthrough for that table.

## Refresh cadence

The policy-refresh sync loop runs on `[snowflake.policy].refresh_interval` (default 60s). Between refreshes the router uses the last snapshot of markers / filtered views. If a new policy is attached upstream and your dashboard queries the affected table in the intervening window, Melt will still route it to Lake until the next refresh — tune the interval based on how fresh your policy state needs to be.
