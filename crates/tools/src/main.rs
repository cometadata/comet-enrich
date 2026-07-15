//! comet-enrich developer tooling.

mod compare;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// comet-enrich developer tools.
#[derive(Parser, Debug)]
#[command(name = "tools", about = "comet-enrich developer tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Compare enrichment output between the new and original systems.
    Compare(compare::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Compare(args) => compare::run(&args),
    }
}
