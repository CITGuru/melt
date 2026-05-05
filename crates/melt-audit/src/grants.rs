//! `--print-grants` — paste-and-go role-creation snippet from spec §2.

/// The exact SQL printed on `--print-grants`. Operators copy this into
/// a Snowflake worksheet to provision the read-only `MELT_AUDIT` role.
pub const GRANTS_SQL: &str = "\
-- melt audit — required Snowflake grants (read-only, one role)
CREATE ROLE IF NOT EXISTS MELT_AUDIT;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE MELT_AUDIT;
-- melt audit reads ONLY:
--   SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
--   SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GRANT USAGE ON WAREHOUSE <WAREHOUSE_NAME> TO ROLE MELT_AUDIT;
GRANT ROLE MELT_AUDIT TO USER <USER_NAME>;
";
