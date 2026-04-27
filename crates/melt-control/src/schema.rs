//! DDL for the Postgres control-plane tables.
//!
//! Applied idempotently on every `ControlCatalog::connect`. `CREATE
//! TABLE IF NOT EXISTS` handles fresh installs; the `ALTER TABLE ...
//! ADD COLUMN IF NOT EXISTS` block migrates existing deployments
//! forward without a separate migration runner. This is cheap enough
//! to do on every startup.

use std::time::Duration;

use melt_core::TableRef;
use melt_snowflake::SnapshotId;

/// Per-table sync output produced by `melt-ducklake` / `melt-iceberg`
/// `sync_table`. Lives here so both crates can depend on it without
/// a cross-crate dependency.
pub struct SyncReport {
    pub table: TableRef,
    pub snapshot: SnapshotId,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    pub bytes_written: u64,
    pub elapsed: Duration,
}

/// SQL Melt runs on every connect. Includes the state-machine
/// migration for `melt_table_stats` — idempotent.
///
/// `sync_state` defaults to `'active'` so rows written before this
/// migration remain routable; new rows inserted by the router get
/// `'pending'` explicitly.
pub const CATALOG_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS melt_table_stats (
    database     TEXT NOT NULL,
    schema       TEXT NOT NULL,
    name         TEXT NOT NULL,
    bytes        BIGINT NOT NULL DEFAULT 0,
    rows_count   BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name)
);

ALTER TABLE melt_table_stats
    ADD COLUMN IF NOT EXISTS sync_state       TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN IF NOT EXISTS source           TEXT NOT NULL DEFAULT 'discovered',
    ADD COLUMN IF NOT EXISTS discovered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_queried_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS bootstrap_error  TEXT,
    ADD COLUMN IF NOT EXISTS object_kind      TEXT NOT NULL DEFAULT 'base_table',
    ADD COLUMN IF NOT EXISTS view_strategy    TEXT;

-- Named constraints are only added if missing; ignore duplicate-name errors.
DO $$
BEGIN
    BEGIN
        ALTER TABLE melt_table_stats
            ADD CONSTRAINT melt_table_stats_sync_state_check
            CHECK (sync_state IN ('pending','bootstrapping','active','quarantined'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    -- Source check widens over time. Drop + recreate so upgrades from
    -- earlier schemas pick up new variants. `view_dependency` was added
    -- when sync gained view decomposition; `remote` was added with the
    -- dual-execution router (operator-declared never-synced tables).
    ALTER TABLE melt_table_stats DROP CONSTRAINT IF EXISTS melt_table_stats_source_check;
    BEGIN
        ALTER TABLE melt_table_stats
            ADD CONSTRAINT melt_table_stats_source_check
            CHECK (source IN ('include','discovered','view_dependency','remote'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        ALTER TABLE melt_table_stats
            ADD CONSTRAINT melt_table_stats_object_kind_check
            CHECK (object_kind IN ('base_table','view','secure_view','materialized_view','external_table','unknown'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        ALTER TABLE melt_table_stats
            ADD CONSTRAINT melt_table_stats_view_strategy_check
            CHECK (view_strategy IS NULL OR view_strategy IN ('decomposed','stream_on_view'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END$$;

CREATE INDEX IF NOT EXISTS melt_table_stats_sync_state_idx
    ON melt_table_stats (sync_state);

CREATE INDEX IF NOT EXISTS melt_table_stats_last_queried_idx
    ON melt_table_stats (last_queried_at)
    WHERE source = 'discovered';

CREATE TABLE IF NOT EXISTS melt_sync_progress (
    database         TEXT NOT NULL,
    schema           TEXT NOT NULL,
    name             TEXT NOT NULL,
    last_snapshot    BIGINT NOT NULL,
    last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name)
);

-- `source` distinguishes markers written by the policy-refresh loop
-- (`'discovered'`) from operator overrides (`'manual'`). The refresh
-- loop's retain sweep only touches `'discovered'` rows, so manual
-- pins survive ticks where Snowflake reports zero protected tables.
CREATE TABLE IF NOT EXISTS melt_policy_markers (
    database     TEXT        NOT NULL,
    schema       TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    policy_name  TEXT        NOT NULL,
    policy_kind  TEXT        NOT NULL,
    source       TEXT        NOT NULL DEFAULT 'discovered' CHECK (source IN ('discovered','manual')),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name, policy_name)
);

CREATE TABLE IF NOT EXISTS melt_policy_views (
    database     TEXT        NOT NULL,
    schema       TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    view_name    TEXT        NOT NULL,
    duckdb_where TEXT        NOT NULL,
    source_body  TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name)
);

-- ── View-aware sync ─────────────────────────────────────────────
-- `melt_view_dependencies` records which base tables (or intermediate
-- views) a decomposed view depends on. Populated by the bootstrap
-- path; consulted by demotion to ref-count `view_dependency` rows and
-- by the drift-rescan loop to cascade re-bootstrap when a base table
-- is invalidated.
CREATE TABLE IF NOT EXISTS melt_view_dependencies (
    parent_db     TEXT NOT NULL,
    parent_schema TEXT NOT NULL,
    parent_name   TEXT NOT NULL,
    dep_db        TEXT NOT NULL,
    dep_schema    TEXT NOT NULL,
    dep_name      TEXT NOT NULL,
    dep_kind      TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_db, parent_schema, parent_name, dep_db, dep_schema, dep_name)
);

DO $$ BEGIN
    BEGIN
        ALTER TABLE melt_view_dependencies
            ADD CONSTRAINT melt_view_deps_dep_kind_check
            CHECK (dep_kind IN ('base_table','view'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END$$;

CREATE INDEX IF NOT EXISTS melt_view_deps_dep_idx
    ON melt_view_dependencies (dep_db, dep_schema, dep_name);

-- `melt_view_bodies` stores the Snowflake view DDL plus its
-- DuckDB-translated form (for `decomposed`) or NULL (for
-- `stream_on_view`). `body_checksum` drives drift detection; sync
-- re-hashes the live body on each rescan and re-bootstraps on
-- mismatch.
CREATE TABLE IF NOT EXISTS melt_view_bodies (
    database       TEXT NOT NULL,
    schema         TEXT NOT NULL,
    name           TEXT NOT NULL,
    snowflake_body TEXT NOT NULL,
    duckdb_body    TEXT,
    body_checksum  TEXT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name)
);
"#;
