use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::checksum;
use crate::error::MigrateError;
use crate::migration::{AppliedMigration, Migration};
use crate::source;

/// Result of running migrations.
#[must_use]
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct MigrateReport {
    /// Number of migrations applied in this run.
    pub applied: usize,
    /// Number of migrations skipped because they were already applied.
    pub skipped: usize,
    /// Versions of migrations applied in this run, in order.
    pub applied_versions: Vec<i64>,
}

/// The migration runner. Holds a set of migrations and configuration for the
/// tracking table and optional cluster support.
#[derive(Debug, Clone)]
pub struct Migrator {
    migrations: Cow<'static, [Migration]>,
    table_name: Cow<'static, str>,
    cluster: Option<Cow<'static, str>>,
}

/// Check that a value is a safe SQL identifier: `[a-zA-Z_][a-zA-Z0-9_]*`.
fn validate_identifier(value: &str) -> Result<(), MigrateError> {
    let mut chars = value.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');

    if valid {
        Ok(())
    } else {
        Err(MigrateError::InvalidIdentifier {
            value: value.to_owned(),
        })
    }
}

/// Escape a string value for use inside single-quoted SQL literals.
/// Escapes backslashes (ClickHouse defaults to `backslash_is_escape = 1`)
/// and single quotes (doubled per SQL standard).
fn escape_string_literal(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// Strip lines that are only SQL comments (`-- ...`), returning the
/// remaining text. Preserves inline comments within SQL statements.
fn strip_comment_lines(sql: &str) -> String {
    sql.lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

impl Migrator {
    /// Default table name for the tracking table.
    pub const DEFAULT_TABLE_NAME: &'static str = "_ch_migrations";

    /// Construct a Migrator from compile-time-embedded migrations.
    /// Used by the `migrate!()` proc macro.
    pub const fn new(migrations: &'static [Migration]) -> Self {
        Self {
            migrations: Cow::Borrowed(migrations),
            table_name: Cow::Borrowed(Self::DEFAULT_TABLE_NAME),
            cluster: None,
        }
    }

    /// Construct from runtime-discovered migrations on disk (used by CLI).
    pub async fn from_directory(dir: &Path) -> Result<Self, MigrateError> {
        let migrations = source::resolve(dir)
            .await?
            .into_iter()
            .map(|(m, _path)| m)
            .collect::<Vec<_>>();
        Ok(Self {
            migrations: Cow::Owned(migrations),
            table_name: Cow::Borrowed(Self::DEFAULT_TABLE_NAME),
            cluster: None,
        })
    }

    /// Set the cluster name for ON CLUSTER support.
    pub fn with_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(Cow::Owned(cluster.into()));
        self
    }

    /// Set a custom tracking table name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Cow::Owned(name.into());
        self
    }

    /// The migrations held by this migrator.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// Apply all pending migrations.
    ///
    /// Multi-statement migrations are split on `;` and executed individually.
    /// This splitting is naive and does not handle semicolons inside string
    /// literals or comments. Prefer one statement per migration file.
    ///
    /// If a multi-statement migration partially succeeds, the completed
    /// statements are **not** rolled back (ClickHouse has no DDL transactions).
    /// Write idempotent DDL (`IF NOT EXISTS`, `IF EXISTS`) to make retries safe.
    pub async fn run(&self, client: &clickhouse::Client) -> Result<MigrateReport, MigrateError> {
        self.validate_config()?;
        let mut report = MigrateReport::default();

        // Step 1: Ensure the tracking table exists.
        let ddl = self.create_tracking_table_sql();
        client
            .query(&ddl)
            .execute()
            .await
            .map_err(MigrateError::TrackingTableError)?;

        // Step 2: Fetch all previously applied migrations.
        let applied = self.fetch_applied(client).await?;
        let applied_map: HashMap<i64, &AppliedMigration> =
            applied.iter().map(|a| (a.version, a)).collect();

        // Step 3: Validate checksums of applied migrations.
        self.validate_applied(&applied_map)?;

        // Step 4: Apply pending migrations in version order.
        for migration in self.migrations.iter() {
            if applied_map.contains_key(&migration.version) {
                report.skipped += 1;
                continue;
            }

            let start = Instant::now();

            // Strip comment-only lines, then split on semicolons.
            let stripped = strip_comment_lines(&migration.sql);
            let statements = split_statements(&stripped);
            if statements.is_empty() {
                return Err(MigrateError::EmptyMigration {
                    version: migration.version,
                    description: migration.description.to_string(),
                });
            }
            for stmt in &statements {
                client
                    .query(stmt)
                    .execute()
                    .await
                    .map_err(|e| MigrateError::MigrationFailed {
                        version: migration.version,
                        description: migration.description.to_string(),
                        source: e,
                    })?;
            }

            let elapsed_ms = start.elapsed().as_millis() as u64;

            // Step 5: Record success (INSERT-after-success pattern).
            self.record_migration(client, migration, elapsed_ms).await?;

            report.applied += 1;
            report.applied_versions.push(migration.version);
        }

        Ok(report)
    }

    /// Show status of all migrations (for `info` command).
    pub async fn status(
        &self,
        client: &clickhouse::Client,
    ) -> Result<Vec<MigrationStatus>, MigrateError> {
        self.validate_config()?;

        // Create table if it doesn't exist so the query doesn't fail.
        let ddl = self.create_tracking_table_sql();
        client
            .query(&ddl)
            .execute()
            .await
            .map_err(MigrateError::TrackingTableError)?;

        let applied = self.fetch_applied(client).await?;
        let applied_map: HashMap<i64, AppliedMigration> =
            applied.into_iter().map(|a| (a.version, a)).collect();

        let mut statuses = Vec::with_capacity(self.migrations.len());
        for migration in self.migrations.iter() {
            match applied_map.get(&migration.version) {
                Some(record) => {
                    statuses.push(MigrationStatus::Applied {
                        version: migration.version,
                        description: migration.description.to_string(),
                        installed_on: record.installed_on,
                        execution_time_ms: record.execution_time_ms,
                    });
                }
                None => {
                    statuses.push(MigrationStatus::Pending {
                        version: migration.version,
                        description: migration.description.to_string(),
                    });
                }
            }
        }

        Ok(statuses)
    }

    /// Validate that table_name and cluster are safe identifiers.
    fn validate_config(&self) -> Result<(), MigrateError> {
        validate_identifier(&self.table_name)?;
        if let Some(cluster) = &self.cluster {
            validate_identifier(cluster)?;
        }
        Ok(())
    }

    fn create_tracking_table_sql(&self) -> String {
        let table = &self.table_name;
        let columns = "\
            version          Int64,\
            description      String,\
            checksum         String,\
            installed_on     DateTime DEFAULT now(),\
            success          Bool,\
            execution_time_ms UInt64";

        // ReplacingMergeTree(installed_on) lets a later INSERT for the same
        // version supersede an earlier one (e.g. a manual fix-up row), with
        // SELECT ... FINAL collapsing to the most recent installed_on per
        // version. Plain MergeTree does not support FINAL.
        match &self.cluster {
            None => format!(
                "CREATE TABLE IF NOT EXISTS {table} ({columns}) \
                 ENGINE = ReplacingMergeTree(installed_on) ORDER BY version"
            ),
            Some(cluster) => format!(
                "CREATE TABLE IF NOT EXISTS {table} \
                 ON CLUSTER '{cluster}' ({columns}) \
                 ENGINE = ReplicatedReplacingMergeTree(\
                 '/clickhouse/tables/{{shard}}/{table}', '{{replica}}', installed_on) \
                 ORDER BY version"
            ),
        }
    }

    async fn fetch_applied(
        &self,
        client: &clickhouse::Client,
    ) -> Result<Vec<AppliedMigration>, MigrateError> {
        let table = &self.table_name;
        let query = format!(
            "SELECT version, description, checksum, installed_on, \
                    success, execution_time_ms \
             FROM {table} FINAL \
             WHERE success = true \
             ORDER BY version ASC"
        );
        client
            .query(&query)
            .fetch_all::<AppliedMigration>()
            .await
            .map_err(MigrateError::QueryError)
    }

    fn validate_applied(
        &self,
        applied_map: &HashMap<i64, &AppliedMigration>,
    ) -> Result<(), MigrateError> {
        let source_map: HashMap<i64, &Migration> =
            self.migrations.iter().map(|m| (m.version, m)).collect();

        for (&version, applied) in applied_map {
            let source = source_map
                .get(&version)
                .ok_or(MigrateError::MissingMigration { version })?;

            let applied_bytes = checksum::from_hex(&applied.checksum).map_err(|e| {
                MigrateError::CorruptChecksum {
                    version,
                    message: e.to_string(),
                }
            })?;

            if applied_bytes != *source.checksum {
                return Err(MigrateError::ChecksumMismatch {
                    version,
                    expected: checksum::to_hex(&applied_bytes),
                    actual: checksum::to_hex(&source.checksum),
                });
            }
        }

        Ok(())
    }

    async fn record_migration(
        &self,
        client: &clickhouse::Client,
        migration: &Migration,
        execution_time_ms: u64,
    ) -> Result<(), MigrateError> {
        let table = &self.table_name;
        let checksum_hex = checksum::to_hex(&migration.checksum);
        let desc = escape_string_literal(&migration.description);
        let sql = format!(
            "INSERT INTO {table} \
             (version, description, checksum, success, execution_time_ms) \
             VALUES ({}, '{}', '{}', true, {})",
            migration.version, desc, checksum_hex, execution_time_ms,
        );
        client
            .query(&sql)
            .execute()
            .await
            .map_err(MigrateError::RecordError)
    }
}

/// Status of a single migration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MigrationStatus {
    Pending {
        version: i64,
        description: String,
    },
    Applied {
        version: i64,
        description: String,
        installed_on: u32,
        execution_time_ms: u64,
    },
}

/// Split SQL text into individual statements on `;` boundaries.
/// Strips empty/whitespace-only segments.
///
/// **Caveat:** this is a naive split that does not handle semicolons inside
/// string literals or comments. Prefer one statement per migration file.
fn split_statements(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_single_statement() {
        let stmts = split_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn split_multiple_statements() {
        let stmts = split_statements("CREATE TABLE t (id UInt64); INSERT INTO t VALUES (1);");
        assert_eq!(
            stmts,
            vec!["CREATE TABLE t (id UInt64)", "INSERT INTO t VALUES (1)"]
        );
    }

    #[test]
    fn split_handles_trailing_whitespace() {
        let stmts = split_statements("SELECT 1;\n\n  ");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn strip_comment_lines_removes_comments() {
        let sql = "-- header comment\nSELECT 1;\n-- footer";
        let stripped = strip_comment_lines(sql);
        assert_eq!(stripped, "SELECT 1;");
    }

    #[test]
    fn strip_comment_lines_all_comments_becomes_empty() {
        let sql = "-- just a comment\n-- another one\n";
        let stripped = strip_comment_lines(sql);
        let stmts = split_statements(&stripped);
        assert!(stmts.is_empty());
    }

    #[test]
    fn strip_comment_lines_preserves_inline_sql() {
        let sql = "CREATE TABLE t (id UInt64) -- inline comment\n";
        let stripped = strip_comment_lines(sql);
        assert_eq!(stripped, "CREATE TABLE t (id UInt64) -- inline comment");
    }

    #[test]
    fn create_tracking_table_single_node() {
        let m = Migrator::new(&[]);
        let sql = m.create_tracking_table_sql();
        assert!(sql.contains("ReplacingMergeTree(installed_on)"));
        assert!(!sql.contains("ON CLUSTER"));
        assert!(!sql.contains("Replicated"));
    }

    #[test]
    fn create_tracking_table_clustered() {
        let m = Migrator::new(&[]).with_cluster("my_cluster");
        let sql = m.create_tracking_table_sql();
        assert!(sql.contains("ON CLUSTER 'my_cluster'"));
        assert!(sql.contains("ReplicatedReplacingMergeTree"));
        assert!(sql.contains("installed_on"));
        assert!(sql.contains("{shard}"));
        assert!(sql.contains("{replica}"));
    }

    #[test]
    fn validate_identifier_valid() {
        assert!(validate_identifier("_ch_migrations").is_ok());
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("T1").is_ok());
    }

    #[test]
    fn validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("1bad").is_err());
        assert!(validate_identifier("no spaces").is_err());
        assert!(validate_identifier("drop;--").is_err());
        assert!(validate_identifier("Robert'; DROP TABLE students--").is_err());
    }

    #[test]
    fn escape_string_literal_quotes() {
        assert_eq!(escape_string_literal("it's a test"), "it''s a test");
        assert_eq!(escape_string_literal("no quotes"), "no quotes");
    }

    #[test]
    fn escape_string_literal_backslashes() {
        assert_eq!(
            escape_string_literal("path\\to\\thing"),
            "path\\\\to\\\\thing"
        );
        assert_eq!(
            escape_string_literal("mixed\\and'quotes"),
            "mixed\\\\and''quotes"
        );
    }
}
