//! Treeline CLI - Personal finance in your terminal

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod output;

use commands::{backup, compact, demo, doctor, encrypt, import, new, plugin, query, remove, setup, status, sync, tag};

/// Treeline - personal finance in your terminal
#[derive(Parser)]
#[command(name = "tl", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show account status and summary
    Status {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Sync accounts and transactions from integrations
    Sync {
        /// Integration name (optional, syncs all if not specified)
        integration: Option<String>,
        /// Preview changes without applying
        #[arg(long)]
        dry_run: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Execute SQL query against the database
    Query {
        /// SQL query to execute
        sql: Option<String>,
        /// Read SQL from file
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Output format
        #[arg(long, default_value = "table")]
        format: String,
        /// Output as JSON (shorthand for --format json)
        #[arg(long)]
        json: bool,
    },

    /// Apply tags to transactions
    Tag {
        /// Comma-separated tags to apply
        tags: String,
        /// Transaction IDs to tag
        #[arg(long, value_delimiter = ',')]
        ids: Vec<String>,
        /// Replace existing tags instead of appending
        #[arg(long)]
        replace: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Create new records
    New {
        #[command(subcommand)]
        command: new::NewCommands,
    },

    /// Backfill historical data
    Backfill {
        #[command(subcommand)]
        command: commands::backfill::BackfillCommands,
    },

    /// Import transactions from CSV
    Import {
        /// Path to CSV file
        file: Option<PathBuf>,
        /// Account ID to import into
        #[arg(long)]
        account_id: Option<String>,
        /// Preview without importing
        #[arg(long)]
        preview: bool,
        /// Use saved import profile
        #[arg(long)]
        profile: Option<String>,
        /// Save settings as profile
        #[arg(long)]
        save_profile: Option<String>,
        /// List saved profiles
        #[arg(long)]
        list_profiles: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
        /// Column name for transaction date
        #[arg(long)]
        date_column: Option<String>,
        /// Column name for transaction description
        #[arg(long)]
        description_column: Option<String>,
        /// Column name for amount (single amount column)
        #[arg(long)]
        amount_column: Option<String>,
        /// Column name for debit amounts (accounting format)
        #[arg(long)]
        debit_column: Option<String>,
        /// Column name for credit amounts (accounting format)
        #[arg(long)]
        credit_column: Option<String>,
        /// Negate debit values (when debits are shown as positive in CSV)
        #[arg(long)]
        debit_negative: bool,
        /// Flip signs on all amounts (for credit card statements)
        #[arg(long)]
        flip_signs: bool,
    },

    /// Remove an integration
    Remove {
        /// Integration name to remove
        name: String,
        /// Skip confirmation prompt
        #[arg(long, short)]
        force: bool,
    },

    /// Set up a new integration
    Setup {
        /// Integration type (simplefin, demo)
        integration: String,
        /// SimpleFIN setup token
        #[arg(long)]
        token: Option<String>,
    },

    /// Manage backups
    Backup {
        #[command(subcommand)]
        command: backup::BackupCommands,
    },

    /// Compact the database
    Compact {
        /// Skip creating safety backup
        #[arg(long)]
        skip_backup: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Run database health checks
    Doctor {
        /// Show verbose output
        #[arg(long, short)]
        verbose: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Encrypt the database
    Encrypt {
        /// Subcommand (status) or encrypt the database
        #[command(subcommand)]
        command: Option<encrypt::EncryptCommands>,
        /// Password for encryption
        #[arg(short, long)]
        password: Option<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Decrypt the database
    Decrypt {
        /// Password for decryption
        #[arg(short, long)]
        password: Option<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Manage demo mode
    Demo {
        #[command(subcommand)]
        command: Option<demo::DemoCommands>,
    },

    /// Manage plugins
    Plugin {
        #[command(subcommand)]
        command: plugin::PluginCommands,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = run(cli);

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Status { json } => status::run(json),
        Commands::Sync { integration, dry_run, json } => sync::run(integration, dry_run, json),
        Commands::Query { sql, file, format, json } => {
            let fmt = if json { "json".to_string() } else { format };
            query::run(sql.as_deref(), file.as_deref(), &fmt)
        }
        Commands::Tag { tags, ids, replace, json } => tag::run(&tags, ids, replace, json),
        Commands::New { command } => new::run(command),
        Commands::Backfill { command } => commands::backfill::run(command),
        Commands::Import { file, account_id, preview, profile, save_profile, list_profiles, json,
                           date_column, description_column, amount_column, debit_column, credit_column,
                           debit_negative, flip_signs } => {
            import::run(file, account_id, preview, profile, save_profile, list_profiles, json,
                       date_column, description_column, amount_column, debit_column, credit_column,
                       debit_negative, flip_signs)
        }
        Commands::Remove { name, force } => remove::run(&name, force),
        Commands::Setup { integration, token } => setup::run(&integration, token),
        Commands::Backup { command } => backup::run(command),
        Commands::Compact { skip_backup, json } => compact::run(skip_backup, json),
        Commands::Doctor { verbose, json } => doctor::run(verbose, json),
        Commands::Encrypt { command, password, json } => encrypt::run(command, password, json),
        Commands::Decrypt { password, json } => encrypt::run_decrypt(password, json),
        Commands::Demo { command } => demo::run(command),
        Commands::Plugin { command } => plugin::run(command),
    }
}
