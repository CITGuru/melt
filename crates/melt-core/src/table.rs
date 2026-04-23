use serde::{Deserialize, Serialize};

use crate::session::SessionId;

/// Fully-qualified reference to a table in the lake / Snowflake.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub database: String,
    pub schema: String,
    pub name: String,
}

impl TableRef {
    pub fn new(
        database: impl Into<String>,
        schema: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            database: database.into(),
            schema: schema.into(),
            name: name.into(),
        }
    }

    /// Parse a dotted reference (`db.schema.name`). Accepts 1-, 2-, or
    /// 3-part identifiers; the first two parts default to `defaults`
    /// when missing. Returns `None` if `s` is empty or has more than
    /// three components.
    pub fn parse_dotted(
        s: &str,
        default_db: Option<&str>,
        default_schema: Option<&str>,
    ) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').filter(|p| !p.is_empty()).collect();
        match parts.len() {
            1 => Some(TableRef::new(default_db?, default_schema?, parts[0])),
            2 => Some(TableRef::new(default_db?, parts[0], parts[1])),
            3 => Some(TableRef::new(parts[0], parts[1], parts[2])),
            _ => None,
        }
    }

    pub fn fqn(&self) -> String {
        format!("{}.{}.{}", self.database, self.schema, self.name)
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.fqn())
    }
}

/// Per-query execution context handed to backends. Lets the backend
/// resolve unqualified names or apply session-scoped settings.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub session_id: SessionId,
    pub role: Option<String>,
    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

impl QueryContext {
    pub fn from_session(session: &crate::session::SessionInfo) -> Self {
        Self {
            session_id: session.id.clone(),
            role: session.role.clone(),
            warehouse: session.warehouse.clone(),
            database: session.database.clone(),
            schema: session.schema.clone(),
        }
    }
}
