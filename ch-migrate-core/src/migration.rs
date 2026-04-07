use serde::Deserialize;
use std::borrow::Cow;

/// A single migration definition — either embedded at compile time or loaded at runtime.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Integer version extracted from filename prefix (e.g. 20240101120000).
    pub version: i64,
    /// Human-readable description derived from filename (underscores replaced with spaces).
    pub description: Cow<'static, str>,
    /// Raw SQL content of the migration.
    pub sql: Cow<'static, str>,
    /// SHA-256 checksum of the SQL content (32 bytes).
    pub checksum: Cow<'static, [u8]>,
}

/// A migration record as stored in the ClickHouse tracking table.
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct AppliedMigration {
    pub version: i64,
    pub description: String,
    /// Hex-encoded SHA-256 checksum.
    pub checksum: String,
    /// Epoch seconds (ClickHouse DateTime).
    pub installed_on: u32,
    pub success: bool,
    pub execution_time_ms: u64,
}
