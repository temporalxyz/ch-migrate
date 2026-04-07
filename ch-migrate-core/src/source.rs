use std::path::{Path, PathBuf};

use crate::checksum;
use crate::error::MigrateError;
use crate::migration::Migration;
use std::borrow::Cow;

/// Parse a migration filename into (version, description).
///
/// Expected format: `{VERSION}_{DESCRIPTION}.sql`
/// where VERSION is a positive i64 and DESCRIPTION uses underscores between words.
pub fn parse_filename(filename: &str) -> Result<(i64, String), MigrateError> {
    let stem = filename
        .strip_suffix(".sql")
        .ok_or_else(|| MigrateError::InvalidFilename {
            filename: filename.to_owned(),
            reason: "must end with .sql".into(),
        })?;

    let (version_str, description_raw) =
        stem.split_once('_')
            .ok_or_else(|| MigrateError::InvalidFilename {
                filename: filename.to_owned(),
                reason: "expected format {VERSION}_{DESCRIPTION}.sql".into(),
            })?;

    let version: i64 = version_str
        .parse()
        .map_err(|_| MigrateError::InvalidFilename {
            filename: filename.to_owned(),
            reason: format!("version prefix {:?} is not a valid integer", version_str),
        })?;

    if version <= 0 {
        return Err(MigrateError::InvalidFilename {
            filename: filename.to_owned(),
            reason: "version must be a positive integer".into(),
        });
    }

    if description_raw.is_empty() {
        return Err(MigrateError::InvalidFilename {
            filename: filename.to_owned(),
            reason: "description cannot be empty".into(),
        });
    }

    // Validate description contains only safe characters.
    if !description_raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(MigrateError::InvalidFilename {
            filename: filename.to_owned(),
            reason:
                "description may only contain alphanumeric characters, underscores, and hyphens"
                    .into(),
        });
    }

    let description = description_raw.replace('_', " ");

    Ok((version, description))
}

/// Discover and load migrations from a directory (async).
pub async fn resolve(dir: &Path) -> Result<Vec<(Migration, PathBuf)>, MigrateError> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|e| MigrateError::ResolveError {
            path: dir.display().to_string(),
            message: e.to_string(),
        })?;

    let mut migrations = Vec::new();

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| MigrateError::ResolveError {
            path: dir.display().to_string(),
            message: e.to_string(),
        })?
    {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        let filename =
            entry
                .file_name()
                .into_string()
                .map_err(|name| MigrateError::ResolveError {
                    path: dir.display().to_string(),
                    message: format!("non-UTF-8 filename: {}", name.to_string_lossy()),
                })?;
        let (version, description) = parse_filename(&filename)?;
        let sql =
            tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| MigrateError::ResolveError {
                    path: path.display().to_string(),
                    message: e.to_string(),
                })?;
        let chk = checksum::compute(&sql);

        migrations.push((
            Migration {
                version,
                description: Cow::Owned(description),
                sql: Cow::Owned(sql),
                checksum: Cow::Owned(chk),
            },
            path,
        ));
    }

    migrations.sort_by_key(|(m, _)| m.version);
    check_duplicates(&migrations)?;

    Ok(migrations)
}

/// Discover and load migrations from a directory (blocking, for use in proc macros).
pub fn resolve_blocking(dir: &Path) -> Result<Vec<(Migration, PathBuf)>, MigrateError> {
    let entries = std::fs::read_dir(dir).map_err(|e| MigrateError::ResolveError {
        path: dir.display().to_string(),
        message: e.to_string(),
    })?;

    let mut migrations = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| MigrateError::ResolveError {
            path: dir.display().to_string(),
            message: e.to_string(),
        })?;

        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        let filename =
            entry
                .file_name()
                .into_string()
                .map_err(|name| MigrateError::ResolveError {
                    path: dir.display().to_string(),
                    message: format!("non-UTF-8 filename: {}", name.to_string_lossy()),
                })?;
        let (version, description) = parse_filename(&filename)?;
        let sql = std::fs::read_to_string(&path).map_err(|e| MigrateError::ResolveError {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        let chk = checksum::compute(&sql);

        migrations.push((
            Migration {
                version,
                description: Cow::Owned(description),
                sql: Cow::Owned(sql),
                checksum: Cow::Owned(chk),
            },
            path,
        ));
    }

    migrations.sort_by_key(|(m, _)| m.version);
    check_duplicates(&migrations)?;

    Ok(migrations)
}

fn check_duplicates(migrations: &[(Migration, PathBuf)]) -> Result<(), MigrateError> {
    for window in migrations.windows(2) {
        if window[0].0.version == window[1].0.version {
            return Err(MigrateError::DuplicateVersion {
                version: window[0].0.version,
                file1: window[0].1.display().to_string(),
                file2: window[1].1.display().to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_filename() {
        let (v, d) = parse_filename("0001_create_users_table.sql").unwrap();
        assert_eq!(v, 1);
        assert_eq!(d, "create users table");
    }

    #[test]
    fn parse_timestamp_filename() {
        let (v, d) = parse_filename("20260331143052_add_events.sql").unwrap();
        assert_eq!(v, 20260331143052);
        assert_eq!(d, "add events");
    }

    #[test]
    fn parse_missing_extension() {
        assert!(parse_filename("0001_test.txt").is_err());
    }

    #[test]
    fn parse_missing_underscore() {
        assert!(parse_filename("0001.sql").is_err());
    }

    #[test]
    fn parse_non_integer_version() {
        assert!(parse_filename("abc_test.sql").is_err());
    }

    #[test]
    fn parse_zero_version() {
        assert!(parse_filename("0_test.sql").is_err());
    }

    #[test]
    fn parse_empty_description() {
        assert!(parse_filename("1_.sql").is_err());
    }

    #[test]
    fn parse_invalid_description_chars() {
        assert!(parse_filename("1_has spaces.sql").is_err());
        assert!(parse_filename("1_path/traversal.sql").is_err());
        assert!(parse_filename("1_semi;colon.sql").is_err());
    }

    #[test]
    fn parse_hyphens_in_description() {
        let (v, d) = parse_filename("1_my-migration.sql").unwrap();
        assert_eq!(v, 1);
        assert_eq!(d, "my-migration");
    }
}
