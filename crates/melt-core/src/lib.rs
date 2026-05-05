//! `melt-core` — the contract every other crate agrees on.
//!
//! This crate intentionally has zero internal dependencies on other Melt
//! crates. It defines:
//!
//! - The [`StorageBackend`] trait — the seam between the proxy and a
//!   pluggable lakehouse backend (DuckLake, Iceberg, …).
//! - Shared types: [`TableRef`], [`QueryContext`], [`SessionInfo`],
//!   [`Route`] / [`RouteKind`], policy types.
//! - The unified [`MeltError`] enum + `Result` alias.
//! - [`RouterCache`] — the trait sync subsystems use to invalidate the
//!   router's TTL caches without depending on `melt-router`.
//!

pub mod backend;
pub mod config;
pub mod error;
pub mod hybrid;
pub mod policy;
pub mod reload;
pub mod route;
pub mod session;
pub mod stream;
pub mod sync;
pub mod table;
pub mod translate;

pub use backend::{BackendKind, NoopRouterCache, RouterCache, StorageBackend};
pub use config::{
    HybridParityCompareMode, MeltConfigShared, MetricsConfigShared, ProxyConfig, ProxyLimits,
    ResolvedS3Credentials, RouterConfig, S3Config,
};
pub use error::{CatalogError, MeltError, Result};
pub use hybrid::{
    AttachRewrite, BridgeDirection, BridgeStrategy, HybridPlan, NodeKind, Placement, PlanNode,
    RemoteFragment, TableSourceRegistry,
};
pub use policy::{PolicyConfig, PolicyKind, PolicyMode, ProtectedTable};
pub use reload::{ReloadError, ReloadResponse, SkippedField};
pub use route::{HybridReason, LakeReason, PassthroughReason, Route, RouteKind};
pub use session::{SessionId, SessionInfo};
pub use stream::RecordBatchStream;
pub use sync::{
    DiscoveryCatalog, LazyDiscoverConfig, MatchOutcome, ObjectKind, SyncConfig, SyncSource,
    SyncState, SyncTableMatcher, ViewStrategy, ViewsConfig,
};
pub use table::{QueryContext, TableRef};
