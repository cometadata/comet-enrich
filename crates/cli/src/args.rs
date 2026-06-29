//! Shared CLI arguments for `comet-enrich`.

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use comet_enrichment_core::{LookupConfig, RunOptions, SCHEMA, SourceRelease, Stage, schema};
use log::LevelFilter;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Input and output paths used by every method.
#[derive(Args, Debug, Clone)]
pub struct IoArgs {
    /// Input directory containing DataCite `*.jsonl.gz` files.
    #[arg(short, long, value_name = "DIR", help_heading = "Input/output")]
    pub input: PathBuf,

    /// Output directory for enrichment records (writes enrichments/part_NNNN.jsonl.gz).
    #[arg(short, long, value_name = "DIR", help_heading = "Input/output")]
    pub output: PathBuf,

    /// YAML provenance metadata for the enrichment records.
    #[arg(long, value_name = "FILE", help_heading = "Input/output")]
    pub provenance: PathBuf,

    /// Release date of a data source, as name=YYYY-MM-DD (repeatable), e.g. datacite=2024-01-01.
    #[arg(
        long = "source-release-date",
        value_name = "NAME=YYYY-MM-DD",
        value_parser = parse_source_release,
        help_heading = "Input/output"
    )]
    pub source_release_date: Vec<(String, String)>,
}

impl IoArgs {
    /// Convert CLI arguments into core run options.
    #[must_use]
    pub fn run_options(&self, run: &RunArgs) -> RunOptions {
        RunOptions {
            input: self.input.clone(),
            output: self.output.clone(),
            threads: run.threads,
            batch_size: run.batch_size,
        }
    }

    /// Build the `sources` map recorded in the run manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the same source name is given more than once.
    pub fn sources(&self) -> Result<BTreeMap<String, SourceRelease>> {
        let mut sources = BTreeMap::new();
        for (name, release_date) in &self.source_release_date {
            if sources
                .insert(
                    name.clone(),
                    SourceRelease {
                        release_date: release_date.clone(),
                    },
                )
                .is_some()
            {
                bail!("duplicate --source-release-date for source `{name}`");
            }
        }
        Ok(sources)
    }
}

/// Parse a `name=YYYY-MM-DD` source-release-date argument.
fn parse_source_release(s: &str) -> Result<(String, String), String> {
    let (name, date) = s
        .split_once('=')
        .ok_or_else(|| format!("expected name=YYYY-MM-DD, got `{s}`"))?;
    if name.is_empty() {
        return Err(format!("source name is empty in `{s}`"));
    }
    if !is_iso_date(date) {
        return Err(format!("expected YYYY-MM-DD date, got `{date}`"));
    }
    Ok((name.to_owned(), date.to_owned()))
}

/// Cheap `YYYY-MM-DD` shape check (digits and dashes in the right positions).
fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        })
}

/// Run and validation options used by every method.
#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    /// Worker threads. Use 0 for all available CPUs.
    #[arg(
        short,
        long,
        default_value_t = 0,
        value_name = "N",
        help_heading = "Options"
    )]
    pub threads: usize,

    /// Enrichment records per writer batch.
    #[arg(
        short,
        long,
        default_value_t = 5000,
        value_name = "N",
        help_heading = "Options"
    )]
    pub batch_size: usize,

    /// Validate output against this JSON Schema instead of the built-in schema.
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with = "no_validate",
        help_heading = "Options"
    )]
    pub schema: Option<PathBuf>,

    /// Skip output schema validation.
    #[arg(long, help_heading = "Options")]
    pub no_validate: bool,

    /// Minimum log level
    #[arg(long, default_value_t = LevelFilter::Info, value_name = "LEVEL", help_heading = "Options")]
    pub log_level: LevelFilter,
}

impl RunArgs {
    /// Build the output validator for this run.
    ///
    /// Returns `None` when `--no-validate` is set. Otherwise uses `--schema`, or the
    /// built-in schema when no custom schema is provided.
    ///
    /// # Errors
    ///
    /// Returns an error if the selected schema cannot be read or compiled.
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

/// ROR lookup options used by `affiliations` and `funders`.
#[derive(Args, Debug, Clone)]
pub struct LookupArgs {
    /// Base URL of the ROR match service.
    #[arg(
        long,
        value_name = "URL",
        default_value = "http://localhost:8000",
        help_heading = "ROR matching"
    )]
    pub ror_service_url: String,

    /// ROR registry JSON used to reconcile matched IDs.
    #[arg(long, value_name = "FILE", help_heading = "ROR matching")]
    pub ror_file: PathBuf,

    /// Inputs per ROR match-service request.
    #[arg(
        long,
        default_value_t = 50,
        value_name = "N",
        help_heading = "ROR matching"
    )]
    pub ror_batch_size: usize,

    /// Concurrent ROR match-service requests.
    #[arg(
        long,
        default_value_t = 50,
        value_name = "N",
        help_heading = "ROR matching"
    )]
    pub ror_concurrency: usize,

    /// ROR match-service request timeout in seconds.
    #[arg(
        long,
        default_value_t = 30,
        value_name = "SECS",
        help_heading = "ROR matching"
    )]
    pub ror_timeout: u64,

    /// Ignore existing stage outputs and rerun from the start.
    #[arg(long, help_heading = "Options")]
    pub from_scratch: bool,
}

/// Pipeline stage to run on its own.
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
            ror_service_url: lookup.ror_service_url.clone(),
            ror_file: lookup.ror_file.clone(),
            ror_batch_size: lookup.ror_batch_size,
            ror_concurrency: lookup.ror_concurrency,
            ror_timeout: lookup.ror_timeout,
            from_scratch: lookup.from_scratch,
        }
    }
}

/// Initialise process-wide logging.
///
/// # Errors
///
/// Returns an error if a logger has already been installed.
pub fn init_logging(level: LevelFilter) -> Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(level)
        .init()?;
    Ok(())
}
