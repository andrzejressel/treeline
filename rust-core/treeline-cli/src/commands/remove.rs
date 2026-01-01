//! Remove command - remove an integration

use anyhow::Result;
use colored::Colorize;
use dialoguer::Confirm;

use super::get_context;

pub fn run(name: &str, force: bool) -> Result<()> {
    let ctx = get_context()?;

    // Check if integration exists (case-insensitive, matching Python behavior)
    let integrations = ctx.sync_service.list_integrations()?;
    let name_lower = name.to_lowercase();
    let matched_integration = integrations.iter()
        .find(|i| i.name.to_lowercase() == name_lower);

    if matched_integration.is_none() {
        eprintln!("{}", format!("Integration '{}' not found", name).red());
        if !integrations.is_empty() {
            let names: Vec<_> = integrations.iter().map(|i| i.name.as_str()).collect();
            eprintln!("{}", format!("Configured integrations: {}", names.join(", ")).dimmed());
        } else {
            eprintln!("{}", "No integrations configured".dimmed());
        }
        std::process::exit(1);
    }

    // Use the actual integration name from the database
    let actual_name = &matched_integration.unwrap().name;

    // Confirm removal unless --force
    if !force {
        println!("\n{}", format!("This will remove the '{}' integration.", actual_name).yellow());
        println!("{}\n", "Your synced data will remain in the database.".dimmed());

        if !Confirm::new()
            .with_prompt("Are you sure?")
            .default(false)
            .interact()?
        {
            println!("{}\n", "Cancelled".dimmed());
            return Ok(());
        }
    }

    ctx.sync_service.remove_integration(actual_name)?;
    println!("\n{} Integration '{}' removed\n", "✓".green(), name);

    Ok(())
}
