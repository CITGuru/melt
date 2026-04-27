//! `melt-proxy` — Snowflake-compatible HTTP server.

pub mod execution;
pub mod handlers;
pub mod hybrid_cache;
pub mod hybrid_parity;
pub mod response;
pub mod result_store;
pub mod server;
pub mod session;
pub mod shutdown;
pub mod tls;

pub use hybrid_cache::FragmentCache;
pub use server::{serve, ProxyState, SharedMatcher};
