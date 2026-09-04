//! comet-enrich developer tooling.
//!
//! Commands for benchmarking matches and inspecting source duplicates.

mod bench;
mod duplicate_dois;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use log::LevelFilter;

/// comet-enrich developer tools.
#[derive(Parser, Debug)]
#[command(name = "tools", about = "comet-enrich developer tools")]
struct Cli {
    /// Log verbosity: off, error, warn, info, debug, or trace.
    #[arg(long, default_value_t = LevelFilter::Info, global = true)]
    log_level: LevelFilter,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Find repeated DOIs and their source months in one snapshot.
    DuplicateDois(duplicate_dois::Args),
    /// Benchmark a Marple match endpoint (throughput and latency).
    Bench(bench::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    simple_logger::SimpleLogger::new()
        .with_level(cli.log_level)
        .init()
        .context("initialising logger")?;

    match cli.command {
        Command::DuplicateDois(args) => duplicate_dois::run(&args),
        Command::Bench(args) => {
            // Only the benchmark needs an async runtime.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building tokio runtime")?;
            rt.block_on(bench::run(args))
        }
    }
}
