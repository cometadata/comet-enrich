//! The CLI surface shared across methods: the common argument groups, logging setup, and the
//! clap mirror of the core pipeline [`Stage`].
//!
//! [`CommonArgs`] is flattened into every method; [`LookupArgs`] is flattened into the methods
//! that resolve inputs against an external match service.

use anyhow::Result;
use clap::{Args, Subcommand};
use comet_enrichment_core::{LookupConfig, RunOptions, SCHEMA, Stage, schema};
use log::LevelFilter;
use std::path::PathBuf;

/// Flags shared by every enrichment method.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Input directory (globbed for `**/*.jsonl.gz`).
    #[arg(short, long, value_name = "DIR")]
    pub input: PathBuf,

    /// Output JSONL file.
    #[arg(short, long, value_name = "FILE")]
    pub output: PathBuf,

    /// Provenance config (enrichment_metadata.yaml).
    #[arg(long, value_name = "FILE")]
    pub enrichment: PathBuf,

    /// Worker threads (0 = one per CPU).
    #[arg(short, long, default_value_t = 0, value_name = "N")]
    pub threads: usize,

    /// Emitted records per writer batch.
    #[arg(short, long, default_value_t = 5000, value_name = "N")]
    pub batch_size: usize,

    /// Validate output against this schema instead of the one built into the binary.
    #[arg(long, value_name = "FILE", conflicts_with = "no_validate")]
    pub schema: Option<PathBuf>,

    /// Skip output schema validation.
    #[arg(long)]
    pub no_validate: bool,

    /// Log level (OFF, ERROR, WARN, INFO, DEBUG, TRACE).
    #[arg(long, default_value_t = LevelFilter::Info, value_name = "LEVEL")]
    pub log_level: LevelFilter,
}

impl CommonArgs {
    /// Build the [`RunOptions`] these flags describe.
    #[must_use]
    pub fn run_options(&self) -> RunOptions {
        RunOptions {
            input: self.input.clone(),
            output: self.output.clone(),
            enrichment: self.enrichment.clone(),
            threads: self.threads,
            batch_size: self.batch_size,
        }
    }

    /// Resolve the output validator: none if `--no-validate`, the `--schema` file if given,
    /// otherwise the schema embedded in core ([`comet_enrichment_core::SCHEMA`]).
    ///
    /// # Errors
    /// Returns an error if the chosen schema cannot be read or compiled.
    pub fn validator(&self) -> Result<Option<jsonschema::JSONSchema>> {
        if self.no_validate {
            return Ok(None);
        }
        let v = match &self.schema {
            Some(path) => schema::compile(path)?,
            None => schema::compile_str(SCHEMA)?,
        };
        Ok(Some(v))
    }
}

/// Flags shared by methods that resolve inputs against an external match service (ROR).
#[derive(Args, Debug, Clone)]
pub struct LookupArgs {
    /// Base URL of the match service (Marple).
    #[arg(long, value_name = "URL", default_value = "http://localhost:8000")]
    pub match_url: String,

    /// ROR data dump (JSON) used to reconcile matches.
    #[arg(long, value_name = "FILE")]
    pub ror_data: PathBuf,

    /// Directory for intermediate stage artifacts. A temporary one is used if omitted.
    #[arg(long, value_name = "DIR")]
    pub work_dir: Option<PathBuf>,

    /// Inputs per match-service bulk request.
    #[arg(long, default_value_t = 50, value_name = "N")]
    pub ror_batch_size: usize,

    /// Concurrent in-flight match requests.
    #[arg(long, default_value_t = 50, value_name = "N")]
    pub concurrency: usize,

    /// Match-service request timeout (seconds).
    #[arg(long, default_value_t = 30, value_name = "SECS")]
    pub timeout: u64,

    /// Ignore any existing work-dir artifacts and run every stage from scratch.
    #[arg(long)]
    pub restart: bool,
}

/// CLI mirror of the core pipeline [`Stage`]: names a single stage to run on its own.
#[derive(Subcommand, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageArg {
    /// Scan the corpus and collect the unique inputs to look up.
    Extract,
    /// Resolve the unique inputs against the match service.
    Query,
    /// Join matches back onto records and emit enrichment records.
    Reconcile,
}

impl From<StageArg> for Stage {
    fn from(stage: StageArg) -> Self {
        match stage {
            StageArg::Extract => Stage::Extract,
            StageArg::Query => Stage::Query,
            StageArg::Reconcile => Stage::Reconcile,
        }
    }
}

impl From<&LookupArgs> for LookupConfig {
    fn from(lookup: &LookupArgs) -> Self {
        LookupConfig {
            match_url: lookup.match_url.clone(),
            ror_data: lookup.ror_data.clone(),
            work_dir: lookup.work_dir.clone(),
            ror_batch_size: lookup.ror_batch_size,
            concurrency: lookup.concurrency,
            timeout: lookup.timeout,
            restart: lookup.restart,
        }
    }
}

/// Initialise process-wide logging at the given level.
///
/// # Errors
/// Returns an error if a logger has already been installed.
pub fn init_logging(level: LevelFilter) -> Result<()> {
    simple_logger::SimpleLogger::new().with_level(level).init()?;
    Ok(())
}
