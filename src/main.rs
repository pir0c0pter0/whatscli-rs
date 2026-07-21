use anyhow::Result;
use whatscli::app::App;
use whatscli::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("-h" | "--help") => {
            println!(
                "WhatsCLI {}\n\nUsage: whatscli\n\nRun the terminal client. Type /help inside the app for key bindings and /commands for commands.",
                whatscli::VERSION
            );
            return Ok(());
        }
        Some("-V" | "--version") => {
            println!("WhatsCLI {}", whatscli::VERSION);
            return Ok(());
        }
        _ => {}
    }
    let config = Config::load()?;
    whatscli::logging::init(&config)?;
    for warning in &config.startup_warnings {
        log::warn!("{warning}");
    }
    log::info!("application starting");
    App::new(config).await?.run().await
}
