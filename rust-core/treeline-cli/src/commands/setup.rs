//! Setup command - set up a new integration

use anyhow::Result;
use colored::Colorize;
use dialoguer::Input;

use super::get_context;

pub fn run(integration: &str, token: Option<String>) -> Result<()> {
    let ctx = get_context()?;
    let treeline_dir = super::get_treeline_dir();
    let config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();

    match integration.to_lowercase().as_str() {
        "demo" => {
            // Redirect to demo on command
            println!("Use 'tl demo on' to enable demo mode.");
        }
        "simplefin" => {
            // Block SimpleFIN setup in demo mode
            if config.demo_mode {
                println!("SimpleFIN setup is blocked in demo mode. Use 'tl demo off' first.");
                std::process::exit(1);
            }

            let setup_token = match token {
                Some(t) => t,
                None => Input::new()
                    .with_prompt("SimpleFIN setup token")
                    .interact_text()?,
            };

            ctx.sync_service.setup_simplefin(&setup_token)?;
            println!("{} SimpleFIN integration set up", "Success!".green());
            println!("Run 'tl sync' to sync your accounts.");
        }
        _ => {
            anyhow::bail!("Unknown integration type: {}. Available: simplefin, demo", integration);
        }
    }

    Ok(())
}
