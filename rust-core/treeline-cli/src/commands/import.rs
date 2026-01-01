//! Import command - import transactions from CSV

use std::path::PathBuf;

use anyhow::Result;
use colored::Colorize;
use comfy_table::{Table, ContentArrangement};

use super::get_context;
use treeline_core::config::ColumnMappings;
use treeline_core::services::import::ImportOptions;

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: Option<PathBuf>,
    account_id: Option<String>,
    preview: bool,
    profile: Option<String>,
    save_profile: Option<String>,
    list_profiles: bool,
    json: bool,
    date_column: Option<String>,
    description_column: Option<String>,
    amount_column: Option<String>,
    debit_column: Option<String>,
    credit_column: Option<String>,
    debit_negative: bool,
    flip_signs: bool,
) -> Result<()> {
    let ctx = get_context()?;

    // List profiles
    if list_profiles {
        let profiles = ctx.import_service.list_profiles()?;

        if json {
            println!("{}", serde_json::to_string_pretty(&profiles)?);
        } else {
            if profiles.is_empty() {
                println!("No saved profiles.");
            } else {
                println!("Saved import profiles:");
                for (name, profile) in &profiles {
                    println!();
                    println!("  {}", name.green());
                    println!("    Date: {}", profile.column_mappings.date);
                    if profile.column_mappings.debit.is_some() || profile.column_mappings.credit.is_some() {
                        if let Some(ref d) = profile.column_mappings.debit {
                            println!("    Debit: {}", d);
                        }
                        if let Some(ref c) = profile.column_mappings.credit {
                            println!("    Credit: {}", c);
                        }
                    } else {
                        println!("    Amount: {}", profile.column_mappings.amount);
                    }
                    if let Some(ref desc) = profile.column_mappings.description {
                        println!("    Description: {}", desc);
                    }
                    if profile.options.flip_signs {
                        println!("    Options: flip_signs");
                    }
                    if profile.options.debit_negative {
                        println!("    Options: debit_negative");
                    }
                }
            }
        }
        return Ok(());
    }

    // Require file path for import
    let file_path = file.ok_or_else(|| anyhow::anyhow!("File path required for import"))?;
    let account = account_id.ok_or_else(|| anyhow::anyhow!("--account-id required for import"))?;

    // Check if any column args were provided
    let has_column_args = date_column.is_some()
        || description_column.is_some()
        || amount_column.is_some()
        || debit_column.is_some()
        || credit_column.is_some();

    // Build column mappings from CLI args or profile
    let (mut mappings, using_profile, profile_options) = if let Some(profile_name) = &profile {
        let profile = ctx.import_service.get_profile(profile_name)?
            .ok_or_else(|| anyhow::anyhow!("Profile not found: {}", profile_name))?;
        let opts = ImportOptions {
            flip_signs: profile.options.flip_signs,
            debit_negative: profile.options.debit_negative,
        };
        (profile.column_mappings, Some(profile_name.clone()), Some(opts))
    } else if !has_column_args {
        // Auto-detect columns from CSV
        let detected = ctx.import_service.detect_columns(&file_path)?;

        // Build mappings from detected columns
        let mut m = ColumnMappings::default();
        if let Some(date) = detected.date {
            m.date = date;
        }
        if let Some(amount) = detected.amount {
            m.amount = amount;
        }
        if let Some(desc) = detected.description {
            m.description = Some(desc);
        }
        if let Some(debit) = detected.debit {
            m.debit = Some(debit);
        }
        if let Some(credit) = detected.credit {
            m.credit = Some(credit);
        }

        if !json {
            println!("{}", "Auto-detected columns:".cyan());
            println!("  Date: {}", m.date);
            if m.debit.is_some() || m.credit.is_some() {
                if let Some(ref d) = m.debit {
                    println!("  Debit: {}", d);
                }
                if let Some(ref c) = m.credit {
                    println!("  Credit: {}", c);
                }
            } else {
                println!("  Amount: {}", m.amount);
            }
            if let Some(ref desc) = m.description {
                println!("  Description: {}", desc);
            }
            println!();
        }

        (m, None, None)
    } else {
        // Use CLI-provided column args
        (ColumnMappings::default(), None, None)
    };

    // Override with CLI-provided column mappings
    if let Some(col) = date_column {
        mappings.date = col;
    }
    if let Some(col) = description_column {
        mappings.description = Some(col);
    }
    if let Some(col) = amount_column {
        mappings.amount = col;
    }
    if let Some(col) = debit_column {
        mappings.debit = Some(col);
    }
    if let Some(col) = credit_column {
        mappings.credit = Some(col);
    }

    // Build import options - CLI flags override profile options
    let options = ImportOptions {
        debit_negative: if debit_negative { true } else { profile_options.as_ref().map(|o| o.debit_negative).unwrap_or(false) },
        flip_signs: if flip_signs { true } else { profile_options.as_ref().map(|o| o.flip_signs).unwrap_or(false) },
    };

    let result = ctx.import_service.import(&file_path, &account, &mappings, &options, preview)?;

    // Save profile if requested
    if let Some(profile_name) = save_profile {
        ctx.import_service.save_profile(&profile_name, &mappings, &options)?;
        println!("Profile '{}' saved", profile_name);
    }

    // Print profile usage message
    if let Some(profile_name) = using_profile {
        if !json {
            println!("Using profile '{}'", profile_name);
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    if preview {
        println!("{}", "PREVIEW MODE - No changes applied".yellow());
        println!();

        if let Some(transactions) = &result.transactions {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec!["Date", "Amount", "Description"]);

            for tx in transactions.iter().take(10) {
                table.add_row(vec![
                    &tx.date,
                    &tx.amount,
                    tx.description.as_deref().unwrap_or("-"),
                ]);
            }

            println!("{}", table);

            if transactions.len() > 10 {
                println!("... and {} more", transactions.len() - 10);
            }
        }
    } else {
        println!("{}", "Import complete".green());
    }

    println!();
    println!("  Imported: {}", result.imported);
    println!("  Skipped: {}", result.skipped);

    Ok(())
}
