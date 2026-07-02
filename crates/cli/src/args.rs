//! Shared CLI arguments for `comet-enrich`.

use anyhow::{Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use comet_enrich_core::{
    DEFAULT_OUTPUT_PART_SIZE_MIB, DEFAULT_OUTPUT_WRITER_LANES, HashBits, LookupConfig, RunOptions,
    SCHEMA, SourceRelease, Stage, schema,
};
use log::LevelFilter;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Input and output paths used by every method.
#[derive(Args, Debug, Clone)]
pub struct IoArgs {
    /// Input directory containing DataCite `*.jsonl.gz` files.
    #[arg(short, long, value_name = "DIR", help_heading = "Input/output")]
    pub input: PathBuf,

    /// Output directory for enrichment records (writes rolling enrichments/part_NNNN.jsonl.gz).
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
            output_part_size_bytes: run.output_part_size_mib.saturating_mul(1024 * 1024),
            output_writer_lanes: run.output_writer_lanes,
        }
    }

    /// Build the manifest `sources` map.
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

/// Parse `name=YYYY-MM-DD`.
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

/// Check the `YYYY-MM-DD` shape.
fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        })
}

/// Parse a positive integer CLI value.
fn parse_positive<T: TryFrom<u64>>(s: &str) -> Result<T, String> {
    let n = s
        .parse::<u64>()
        .map_err(|e| format!("expected positive integer, got `{s}`: {e}"))?;
    if n == 0 {
        return Err(format!("expected positive integer, got `{s}`"));
    }
    T::try_from(n).map_err(|_| format!("value out of range: `{s}`"))
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

    /// Target compressed size in MiB for each final enrichment output part.
    #[arg(
        long,
        default_value_t = DEFAULT_OUTPUT_PART_SIZE_MIB,
        value_name = "MIB",
        value_parser = parse_positive::<u64>,
        help_heading = "Options"
    )]
    pub output_part_size_mib: u64,

    /// Parallel writer lanes for final enrichment output. Records route to lanes by DOI hash.
    #[arg(
        long,
        default_value_t = DEFAULT_OUTPUT_WRITER_LANES,
        value_name = "N",
        value_parser = parse_positive::<usize>,
        help_heading = "Options"
    )]
    pub output_writer_lanes: usize,

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
    /// Build the output validator.
    ///
    /// # Errors
    ///
    /// Returns an error if the selected schema cannot be read or compiled.
    pub fn validator(&self) -> Result<Option<jsonschema::Validator>> {
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

    /// Dedup hash width. Use 128 if a 64-bit collision is reported.
    #[arg(
        long,
        value_enum,
        default_value_t = HashBitsArg::Bits64,
        value_name = "BITS",
        help_heading = "ROR matching"
    )]
    pub hash_bits: HashBitsArg,

    /// Ignore existing stage outputs and rerun from the start.
    #[arg(long, help_heading = "Options")]
    pub from_scratch: bool,
}

/// Dedup hash width.
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HashBitsArg {
    /// xxh3-64, 16 hex chars.
    #[default]
    #[value(name = "64")]
    Bits64,
    /// xxh3-128 (32 hex chars).
    #[value(name = "128")]
    Bits128,
}

impl From<HashBitsArg> for HashBits {
    fn from(arg: HashBitsArg) -> Self {
        match arg {
            HashBitsArg::Bits64 => HashBits::Bits64,
            HashBitsArg::Bits128 => HashBits::Bits128,
        }
    }
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
            ror_batch_size: lookup.ror_batch_size,
            ror_concurrency: lookup.ror_concurrency,
            ror_timeout: lookup.ror_timeout,
            hash_bits: lookup.hash_bits.into(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::assert_err_contains;

    fn run_args(no_validate: bool, schema: Option<PathBuf>) -> RunArgs {
        RunArgs {
            threads: 2,
            batch_size: 7,
            output_part_size_mib: 3,
            output_writer_lanes: 4,
            schema,
            no_validate,
            log_level: LevelFilter::Off,
        }
    }

    #[test]
    fn parse_source_release_requires_source_and_iso_date() {
        assert_eq!(
            parse_source_release("datacite=2024-01-01").unwrap(),
            ("datacite".to_owned(), "2024-01-01".to_owned())
        );

        assert_err_contains(
            parse_source_release("datacite:2024-01-01"),
            "expected name=YYYY-MM-DD",
        );
        assert_err_contains(parse_source_release("=2024-01-01"), "source name is empty");
        assert_err_contains(
            parse_source_release("datacite=20240101"),
            "expected YYYY-MM-DD",
        );
    }

    #[test]
    fn io_args_run_options_copies_io_and_run_values() {
        let io = IoArgs {
            input: PathBuf::from("input"),
            output: PathBuf::from("output"),
            provenance: PathBuf::from("prov.yaml"),
            source_release_date: Vec::new(),
        };
        let run = run_args(true, None);

        let opts = io.run_options(&run);

        assert_eq!(opts.input, PathBuf::from("input"));
        assert_eq!(opts.output, PathBuf::from("output"));
        assert_eq!(opts.threads, 2);
        assert_eq!(opts.batch_size, 7);
        assert_eq!(opts.output_part_size_bytes, 3 * 1024 * 1024);
        assert_eq!(opts.output_writer_lanes, 4);
    }

    #[test]
    fn run_args_validator_respects_disabled_builtin_and_custom_schema_modes() {
        assert!(run_args(true, None).validator().unwrap().is_none());
        assert!(run_args(false, None).validator().unwrap().is_some());

        assert_err_contains(
            run_args(false, Some(PathBuf::from("__missing_schema__.json"))).validator(),
            "reading schema __missing_schema__.json",
        );
    }

    #[test]
    fn hash_bits_arg_converts_to_core_hash_bits() {
        assert_eq!(HashBits::from(HashBitsArg::Bits64), HashBits::Bits64);
        assert_eq!(HashBits::from(HashBitsArg::Bits128), HashBits::Bits128);
    }

    #[test]
    fn stage_arg_converts_to_core_stage() {
        assert_eq!(Stage::from(StageArg::Extract), Stage::Extract);
        assert_eq!(Stage::from(StageArg::Query), Stage::Query);
        assert_eq!(Stage::from(StageArg::Reconcile), Stage::Reconcile);
    }
}
