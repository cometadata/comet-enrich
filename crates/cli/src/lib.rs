//! CLI for comet-enrich.
//!
//! Parses command-line arguments, builds the selected enrichment method, and runs it
//! through [comet_enrichment_core::run].

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

pub mod args;

use anyhow::Result;
use args::{IoArgs, LookupArgs, RunArgs, StageArg, init_logging};
use clap::{Parser, Subcommand};
use comet_enrichment_core::{EnrichmentMethod, EnrichmentTemplate, RunStats};
use std::path::PathBuf;

/// Command-line arguments for comet-enrich.
///
/// Each method reads DataCite *.jsonl.gz files and writes JSONL enrichment records.
#[derive(Parser, Debug)]
#[command(name = "comet-enrich", version, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub method: Method,
}

/// Enrichment method to run.
#[derive(Subcommand, Debug)]
pub enum Method {
    /// Reclassify resource types from types.resourceType.
    ResourceTypeGeneral(ResourceTypeGeneralArgs),

    /// Match creator affiliation strings to ROR IDs.
    ///
    /// Runs the extract, query, and reconcile stages. Omit the stage to run the full
    /// pipeline. Existing stage outputs are reused from `--work-dir` unless
    /// `--from-scratch` is used.
    Affiliations(RorLookupArgs),

    /// Match funder names to ROR IDs.
    ///
    /// Runs the extract, query, and reconcile stages. Omit the stage to run the full
    /// pipeline. Existing stage outputs are reused from `--work-dir` unless
    /// `--from-scratch` is used.
    Funders(RorLookupArgs),
}

/// Reclassify resource types from `types.resourceType`.
#[derive(clap::Args, Debug)]
pub struct ResourceTypeGeneralArgs {
    #[command(flatten)]
    pub io: IoArgs,

    /// YAML rules for mapping free-text resourceType values to resourceTypeGeneral
    #[arg(long, value_name = "FILE", help_heading = "Input/output")]
    pub rules: PathBuf,

    #[command(flatten)]
    pub run: RunArgs,
}

/// Arguments shared by the ROR lookup methods.
#[derive(clap::Args, Debug)]
pub struct RorLookupArgs {
    #[command(flatten)]
    pub io: IoArgs,

    #[command(flatten)]
    pub lookup: LookupArgs,

    #[command(flatten)]
    pub run: RunArgs,

    /// Run a single stage instead of the whole pipeline.
    #[command(subcommand)]
    pub stage: Option<StageArg>,
}

impl Method {
    /// Shared run options for the selected method.
    fn run_args(&self) -> &RunArgs {
        match self {
            Method::ResourceTypeGeneral(a) => &a.run,
            Method::Affiliations(a) | Method::Funders(a) => &a.run,
        }
    }

    /// Shared input/output options for the selected method.
    fn io(&self) -> &IoArgs {
        match self {
            Method::ResourceTypeGeneral(a) => &a.io,
            Method::Affiliations(a) | Method::Funders(a) => &a.io,
        }
    }
}

/// Run the selected enrichment method.
pub fn run(cli: Cli) -> Result<()> {
    init_logging(cli.method.run_args().log_level)?;
    // Validate provenance before scanning the corpus or calling the ROR service.
    let template = comet_enrichment_core::load_template(&cli.method.io().provenance)?;
    match cli.method {
        Method::ResourceTypeGeneral(a) => {
            let method = rtg::ResourceTypeGeneral::try_new(rtg::Config {
                rules: a.rules.clone(),
            })?;
            run_method("resource-type-general", &method, &a.io, &a.run, &template)
        }
        Method::Affiliations(a) => {
            let method = affiliations::Affiliations::try_new((&a.lookup).into())?;
            run_method("affiliations", &method, &a.io, &a.run, &template)
        }
        Method::Funders(a) => {
            let method = funders::Funders::try_new((&a.lookup).into())?;
            run_method("funders", &method, &a.io, &a.run, &template)
        }
    }
}

/// Run a configured method and log the summary.
///
/// # Errors
/// Propagates any error from [`comet_enrichment_core::run`] (including schema compilation).
fn run_method<M: EnrichmentMethod>(
    name: &str,
    method: &M,
    io: &IoArgs,
    run: &RunArgs,
    template: &EnrichmentTemplate,
) -> Result<()> {
    let stats =
        comet_enrichment_core::run(method, &io.run_options(run), template, run.validator()?)?;
    report(name, &stats);
    Ok(())
}

/// Log the summary counters for a completed run.
fn report(method: &str, stats: &RunStats) {
    log::info!(
        "{method}: {} files processed ({} failed), {} records scanned, {} emitted, {} malformed",
        stats.files_processed,
        stats.files_failed,
        stats.records_scanned,
        stats.emitted,
        stats.lines_malformed,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(args)
    }

    #[test]
    fn affiliations_defaults() {
        let cli = parse(&[
            "comet-enrich",
            "affiliations",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "e.yaml",
            "--ror-file",
            "ror.json",
        ])
        .unwrap();
        let Method::Affiliations(a) = cli.method else {
            panic!("expected affiliations");
        };
        assert_eq!(a.lookup.ror_concurrency, 50);
        assert_eq!(a.lookup.ror_batch_size, 50);
        assert_eq!(a.run.threads, 0);
        assert_eq!(a.run.log_level, log::LevelFilter::Info);
        assert!(a.stage.is_none());
    }

    #[test]
    fn naming_a_stage_selects_it() {
        let cli = parse(&[
            "comet-enrich",
            "affiliations",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "e.yaml",
            "--ror-file",
            "ror.json",
            "query",
        ])
        .unwrap();
        let Method::Affiliations(a) = cli.method else {
            panic!("expected affiliations");
        };
        assert_eq!(a.stage, Some(StageArg::Query));
    }

    #[test]
    fn schema_and_no_validate_conflict() {
        let res = parse(&[
            "comet-enrich",
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "e.yaml",
            "--rules",
            "r.yaml",
            "--schema",
            "s.json",
            "--no-validate",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn resource_type_rejects_lookup_flags() {
        let res = parse(&[
            "comet-enrich",
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "e.yaml",
            "--rules",
            "r.yaml",
            "--ror-service-url",
            "http://x",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn resource_type_requires_rules() {
        let res = parse(&[
            "comet-enrich",
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "e.yaml",
        ]);
        assert!(res.is_err());
    }
}
