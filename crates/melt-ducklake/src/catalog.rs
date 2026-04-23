//! DuckLake's view of the control catalog is the shared Postgres
//! client. This module exists only as a back-compat shim — all real
//! state-machine logic lives in `melt-control`.

pub use melt_control::{ControlCatalog as CatalogClient, MarkerRow, StatusSnapshot};
