use std::path::PathBuf;
use std::process;

use ch_migrate_core::migrator::{MigrationStatus, Migrator};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ch-migrate", version, about = "ClickHouse migration tool")]
struct Cli {
    /// ClickHouse HTTP URL
    #[arg(long, env = "CH_MIGRATE_URL", default_value = "http://localhost:8123")]
    url: String,

    /// ClickHouse database name
    #[arg(long, env = "CH_MIGRATE_DATABASE")]
    database: Option<String>,

    /// ClickHouse user
    #[arg(long, env = "CH_MIGRATE_USER")]
    user: Option<String>,

    /// ClickHouse password
    #[arg(long, env = "CH_MIGRATE_PASSWORD")]
    password: Option<String>,

    /// Cluster name for ON CLUSTER support
    #[arg(long, env = "CH_MIGRATE_CLUSTER")]
    cluster: Option<String>,

    /// Migration tracking table name
    #[arg(long, default_value = "_ch_migrations")]
    table_name: String,

    /// Path to migrations directory
    #[arg(long, short = 'd', default_value = "migrations")]
    migrations_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new migration file
    Add {
        /// Migration description (words joined with underscores in filename)
        description: Vec<String>,
    },
    /// Apply all pending migrations
    Run,
    /// Show status of all migrations
    Info,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Add { description } => {
            cmd_add(&cli.migrations_dir, &description)?;
        }
        Commands::Run => {
            let client = build_client(&cli);
            let migrator = build_migrator(&cli).await?;
            let report = migrator.run(&client).await?;

            if report.applied == 0 {
                println!("No pending migrations.");
            } else {
                println!("Applied {} migration(s).", report.applied);
            }
        }
        Commands::Info => {
            let client = build_client(&cli);
            let migrator = build_migrator(&cli).await?;
            let statuses = migrator.status(&client).await?;
            print_info(&statuses);
        }
    }

    Ok(())
}

fn build_client(cli: &Cli) -> clickhouse::Client {
    let mut client = clickhouse::Client::default().with_url(&cli.url);

    if let Some(db) = &cli.database {
        client = client.with_database(db);
    }
    if let Some(user) = &cli.user {
        client = client.with_user(user);
    }
    if let Some(password) = &cli.password {
        client = client.with_password(password);
    }

    client
}

async fn build_migrator(cli: &Cli) -> Result<Migrator, ch_migrate_core::MigrateError> {
    let mut migrator = Migrator::from_directory(&cli.migrations_dir).await?;

    if cli.table_name != Migrator::DEFAULT_TABLE_NAME {
        migrator = migrator.with_table_name(cli.table_name.clone());
    }
    if let Some(cluster) = &cli.cluster {
        migrator = migrator.with_cluster(cluster.clone());
    }

    Ok(migrator)
}

/// Validate that a description word is safe to use in a filename.
/// Only allows alphanumeric characters, hyphens, and underscores.
fn validate_description_word(word: &str) -> Result<(), Box<dyn std::error::Error>> {
    if word.is_empty() {
        return Err("description word cannot be empty".into());
    }
    if !word
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!(
            "invalid description word {:?}: \
             only alphanumeric, hyphens, and underscores allowed",
            word
        )
        .into());
    }
    Ok(())
}

fn cmd_add(dir: &PathBuf, description: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if description.is_empty() {
        return Err("description is required".into());
    }

    for word in description {
        validate_description_word(word)?;
    }

    // Ensure the migrations directory exists.
    std::fs::create_dir_all(dir)?;

    let now = chrono::Utc::now();
    let version = now.format("%Y%m%d%H%M%S").to_string();
    let desc_snake = description.join("_");
    let filename = format!("{version}_{desc_snake}.sql");
    let path = dir.join(&filename);

    let desc_human = description.join(" ");
    let iso = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let content = format!(
        "-- Migration: {desc_human}\n\
         -- Created: {iso}\n\
         \n\
         -- Write your SQL here. Example:\n\
         -- CREATE TABLE IF NOT EXISTS my_table (\n\
         --     id UInt64\n\
         -- ) ENGINE = MergeTree()\n\
         -- ORDER BY id;\n"
    );

    if path.exists() {
        return Err(format!("file already exists: {}", path.display()).into());
    }

    std::fs::write(&path, content)?;
    println!("Created: {}", path.display());

    Ok(())
}

fn print_info(statuses: &[MigrationStatus]) {
    if statuses.is_empty() {
        println!("No migrations found.");
        return;
    }

    println!(
        "{:<20} {:<40} {:<10} {:<12}",
        "Version", "Description", "Status", "Duration"
    );
    println!("{}", "-".repeat(82));

    for s in statuses {
        match s {
            MigrationStatus::Pending {
                version,
                description,
            } => {
                println!(
                    "{:<20} {:<40} {:<10} {:<12}",
                    version, description, "Pending", "-"
                );
            }
            MigrationStatus::Applied {
                version,
                description,
                execution_time_ms,
                ..
            } => {
                let duration = format!("{}ms", execution_time_ms);
                println!(
                    "{:<20} {:<40} {:<10} {:<12}",
                    version, description, "Applied", duration
                );
            }
            _ => {}
        }
    }
}
