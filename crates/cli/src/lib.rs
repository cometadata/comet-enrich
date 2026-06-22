//! The `comet-enrich` CLI: parse arguments and dispatch to the selected method.
//!
//! Each enrichment method lives in its own crate, exposing an [`EnrichmentMethod`]
//! implementation and the config it needs. This crate defines the command-line surface —
//! the shared argument groups and per-method flags — and turns parsed arguments into a
//! configured method that [`comet_enrichment_core::run`] executes.

// Brand names (DataCite, ROR, COMET, …) recur in the docs as prose, not code identifiers.
#![allow(clippy::doc_markdown)]

pub mod args;

use anyhow::Result;
use args::{CommonArgs, LookupArgs, StageArg, init_logging};
use clap::{Parser, Subcommand};
use comet_enrichment_core::{EnrichmentMethod, RunStats};
use std::path::PathBuf;

/// Produce DataCite enrichment records from a DataCite snapshot.
///
/// Each method reads a directory of `*.jsonl.gz` shards and writes enrichment records as JSONL.
#[derive(Parser, Debug)]
#[command(name = "comet-enrich", version, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub method: Method,
}

/// The available enrichment methods.
#[derive(Subcommand, Debug)]
pub enum Method {
    /// Correct each record's resourceTypeGeneral from its free-text resourceType.
    ResourceTypeGeneral(ResourceTypeGeneralArgs),
    /// Match creator affiliation strings to ROR IDs.
    ///
    /// Runs `extract` → `query` → `reconcile` against the match service. Omit the stage to run
    /// the whole pipeline (resuming completed stages from `--work-dir`); name a stage to run it.
    Affiliations(RorLookupArgs),
    /// Match funder names to ROR IDs.
    ///
    /// Runs `extract` → `query` → `reconcile` against the match service. Omit the stage to run
    /// the whole pipeline (resuming completed stages from `--work-dir`); name a stage to run it.
    Funders(RorLookupArgs),
}

/// Reclassify `types.resourceTypeGeneral` over a DataCite snapshot.
///
/// A pure transform: each record's `resourceType` is fuzzy-matched against the DataCite
/// vocabulary and a corrected `resourceTypeGeneral` is emitted as an enrichment.
#[derive(clap::Args, Debug)]
pub struct ResourceTypeGeneralArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Reclassification rules (reclassification_rules.yaml).
    #[arg(long, value_name = "FILE")]
    pub rules: PathBuf,
}

/// Shared arguments for the ROR-lookup methods (`affiliations`, `funders`): the common flags,
/// the match-service lookup flags, and an optional single-stage selector. The per-method help
/// text lives on the [`Method`] variants.
#[derive(clap::Args, Debug)]
pub struct RorLookupArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[command(flatten)]
    pub lookup: LookupArgs,

    /// Run a single stage instead of the whole pipeline.
    #[command(subcommand)]
    pub stage: Option<StageArg>,
}

impl Method {
    /// The argument group every method shares.
    fn common(&self) -> &CommonArgs {
        match self {
            Method::ResourceTypeGeneral(a) => &a.common,
            Method::Affiliations(a) | Method::Funders(a) => &a.common,
        }
    }
}

/// Run the selected enrichment method from parsed CLI arguments.
pub fn run(cli: Cli) -> Result<()> {
    init_logging(cli.method.common().log_level)?;
    match cli.method {
        Method::ResourceTypeGeneral(a) => {
            let method = rtg::ResourceTypeGeneral::try_new(rtg::Config {
                rules: a.rules.clone(),
            })?;
            run_method("resource-type-general", &method, &a.common)
        }
        Method::Affiliations(a) => {
            let method = affiliations::Affiliations::try_new((&a.lookup).into())?;
            run_method("affiliations", &method, &a.common)
        }
        Method::Funders(a) => {
            let method = funders::Funders::try_new((&a.lookup).into())?;
            run_method("funders", &method, &a.common)
        }
    }
}

/// Run a configured `method` over its inputs and log the result.
///
/// # Errors
/// Propagates any error from [`comet_enrichment_core::run`] (including schema compilation).
fn run_method<M: EnrichmentMethod>(name: &str, method: &M, common: &CommonArgs) -> Result<()> {
    let stats = comet_enrichment_core::run(method, &common.run_options(), common.validator()?)?;
    report(name, &stats);
    Ok(())
}

/// Log the headline counters from a completed run.
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
            "--enrichment",
            "e.yaml",
            "--ror-data",
            "ror.json",
        ])
        .unwrap();
        let Method::Affiliations(a) = cli.method else {
            panic!("expected affiliations");
        };
        assert_eq!(a.lookup.concurrency, 50);
        assert_eq!(a.lookup.ror_batch_size, 50);
        assert_eq!(a.common.threads, 0);
        assert_eq!(a.common.log_level, log::LevelFilter::Info);
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
            "--enrichment",
            "e.yaml",
            "--ror-data",
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
            "--enrichment",
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
            "--enrichment",
            "e.yaml",
            "--rules",
            "r.yaml",
            "--match-url",
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
            "--enrichment",
            "e.yaml",
        ]);
        assert!(res.is_err());
    }
}
