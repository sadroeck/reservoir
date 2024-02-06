mod dam;

use crate::dam::DamCommand;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    pub cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Dam-related commands
    Dam(DamCommand),
}

fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt().init();

    // Parse the command line args
    let cli = Cli::parse();

    // Dispatch commands
    match cli.cmd {
        Commands::Dam(dam) => dam.execute(),
    }
}
