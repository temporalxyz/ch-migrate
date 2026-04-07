use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MigrateError {
    #[error("invalid identifier {value:?}: must match [a-zA-Z_][a-zA-Z0-9_]*")]
    InvalidIdentifier { value: String },

    #[error("migration {version} ({description}) contains no SQL statements")]
    EmptyMigration { version: i64, description: String },

    #[error("migration {version} has been altered: expected checksum {expected}, found {actual}")]
    ChecksumMismatch {
        version: i64,
        expected: String,
        actual: String,
    },

    #[error("migration {version} was previously applied but is missing from the migration source")]
    MissingMigration { version: i64 },

    #[error("migration {version} ({description}) failed: {source}")]
    MigrationFailed {
        version: i64,
        description: String,
        source: clickhouse::error::Error,
    },

    #[error("failed to create tracking table: {0}")]
    TrackingTableError(clickhouse::error::Error),

    #[error("failed to query applied migrations: {0}")]
    QueryError(clickhouse::error::Error),

    #[error("failed to record migration: {0}")]
    RecordError(clickhouse::error::Error),

    #[error("corrupt checksum in tracking table for version {version}: {message}")]
    CorruptChecksum { version: i64, message: String },

    #[error("error resolving migrations from {path}: {message}")]
    ResolveError { path: String, message: String },

    #[error("invalid migration filename {filename}: {reason}")]
    InvalidFilename { filename: String, reason: String },

    #[error("duplicate migration version {version} found in files: {file1}, {file2}")]
    DuplicateVersion {
        version: i64,
        file1: String,
        file2: String,
    },
}
