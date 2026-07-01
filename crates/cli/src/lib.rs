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
use comet_enrichment_core::{
    EnrichmentMethod, EnrichmentTemplate, HashInfo, LookupConfig, Manifest, MarpleClient, MatchHit,
    MatchService, RunMeta, RunStats, Stage, StageTimings, exit_status, pipeline_complete,
    run_staged,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

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
    /// pipeline. Intermediate files are written to a `.work` directory inside the output
    /// directory, and existing stage outputs there are reused unless `--from-scratch` is used.
    Affiliations(RorLookupArgs),

    /// Match funder names to ROR IDs.
    ///
    /// Runs the extract, query, and reconcile stages. Omit the stage to run the full
    /// pipeline. Intermediate files are written to a `.work` directory inside the output
    /// directory, and existing stage outputs there are reused unless `--from-scratch` is used.
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
            // Skip reasons where the extractor did not select the record.
            run_method(
                "resource-type-general",
                &method,
                &a.io,
                &a.run,
                &template,
                &["not_in_scope", "malformed_types"],
            )
        }
        Method::Affiliations(a) => {
            let method = affiliations::Affiliations::try_new((&a.lookup).into())?;
            run_lookup_method(
                "affiliations",
                &method,
                &a.io,
                &a.lookup,
                &a.run,
                &template,
                "affiliation",
                a.stage,
            )
        }
        Method::Funders(a) => {
            let method = funders::Funders::try_new((&a.lookup).into())?;
            run_lookup_method(
                "funders", &method, &a.io, &a.lookup, &a.run, &template, "funder", a.stage,
            )
        }
    }
}

/// Run a configured method, write the run manifest, and log the summary.
///
/// `out_of_scope` lists the method's skip reasons that mean a record was not
/// selected by the extractor; they are excluded from the manifest's in-scope count.
///
/// # Errors
/// Propagates any error from [`comet_enrichment_core::run`] (including schema
/// compilation), from building the manifest sources, or from writing the manifest.
fn run_method<M: EnrichmentMethod>(
    name: &str,
    method: &M,
    io: &IoArgs,
    run: &RunArgs,
    template: &EnrichmentTemplate,
    out_of_scope: &[&str],
) -> Result<()> {
    let validator = run.validator()?;
    // Validate all CLI metadata before any work, so a typo (e.g. a duplicate
    // source) cannot waste a full corpus pass or leave artifacts without a manifest.
    let sources = io.sources()?;

    let started = Instant::now();
    let stats =
        comet_enrichment_core::run(method, &io.run_options(run), template, validator.as_ref())?;
    let total_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let meta = RunMeta {
        method_name: name.to_owned(),
        method_version: env!("CARGO_PKG_VERSION"),
        sources,
    };
    let timings = StageTimings {
        total: Some(total_ms),
        ..StageTimings::default()
    };
    // The transform path is a single complete pass; it is `partial` only if a read
    // failure or a schema-validation failure lost data.
    let manifest_status = exit_status(stats.files_failed, stats.schema_failures, 0, true);
    Manifest::build(&stats, &meta, out_of_scope, &timings, manifest_status).write(&io.output)?;

    report_stats(name, &stats);
    Ok(())
}

/// Run a lookup method through the staged pipeline and write the run manifest.
///
/// Drives extract → query → reconcile (or the single `stage` when given) against the
/// Marple match service, then records the result plus the dedup-hash identity in the
/// manifest.
///
/// # Errors
/// Propagates any error from schema compilation, source validation, building the
/// match client, the staged run itself, or writing the manifest.
#[allow(clippy::too_many_arguments)]
fn run_lookup_method<M>(
    name: &str,
    method: &M,
    io: &IoArgs,
    lookup: &LookupArgs,
    run: &RunArgs,
    template: &EnrichmentTemplate,
    task: &str,
    stage: Option<StageArg>,
) -> Result<()>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize + DeserializeOwned,
    M::Lookup: Serialize + DeserializeOwned + From<MatchHit> + Send + Sync + 'static,
{
    let validator = run.validator()?;
    // Validate all CLI metadata before any work (a typo must not waste a corpus pass).
    let sources = io.sources()?;

    let cfg: LookupConfig = lookup.into();
    let svc: Arc<dyn MatchService> = Arc::new(MarpleClient::from_config(&cfg)?);
    let only_stage: Option<Stage> = stage.map(Into::into);

    let report = run_staged(
        method,
        &io.run_options(run),
        &cfg,
        &svc,
        template,
        validator.as_ref(),
        task,
        only_stage,
    )?;

    let meta = RunMeta {
        method_name: name.to_owned(),
        method_version: env!("CARGO_PKG_VERSION"),
        sources,
    };
    // `partial` if any data-losing condition occurred (read/schema/match failures) or
    // the pipeline did not complete every stage (e.g. a single-stage debug run).
    let match_errors = report
        .match_
        .as_ref()
        .map_or(0, |m| m.failure_taxonomy.error);
    let complete = pipeline_complete(&io.output);
    let manifest_status = exit_status(
        report.counters.files_failed,
        report.counters.schema_failures,
        match_errors,
        complete,
    );
    let stats = report.counters.clone();
    if complete {
        Manifest::from_report(
            &meta,
            manifest_status,
            report,
            HashInfo::from(cfg.hash_bits),
        )
        .write(&io.output)?;
    } else {
        log::info!("staged pipeline incomplete; not writing manifest.json");
    }

    report_stats(name, &stats);
    Ok(())
}

/// Log the summary counters for a completed run.
fn report_stats(method: &str, stats: &RunStats) {
    log::info!(
        "{method}: {} files processed ({} failed), {} records scanned, {} emitted, {} failed validation, {} malformed",
        stats.files_processed,
        stats.files_failed,
        stats.records_scanned,
        stats.emitted,
        stats.schema_failures,
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
        assert_eq!(a.lookup.hash_bits, args::HashBitsArg::Bits64);
        assert!(a.stage.is_none());
    }

    #[test]
    fn hash_bits_parses_and_rejects_other_widths() {
        let cli = parse(&[
            "comet-enrich",
            "funders",
            "-i",
            "in",
            "-o",
            "out",
            "--provenance",
            "e.yaml",
            "--ror-file",
            "ror.json",
            "--hash-bits",
            "128",
        ])
        .unwrap();
        let Method::Funders(a) = cli.method else {
            panic!("expected funders");
        };
        assert_eq!(a.lookup.hash_bits, args::HashBitsArg::Bits128);

        // Only 64 and 128 are valid widths.
        assert!(
            parse(&[
                "comet-enrich",
                "funders",
                "-i",
                "in",
                "-o",
                "out",
                "--provenance",
                "e.yaml",
                "--ror-file",
                "ror.json",
                "--hash-bits",
                "256",
            ])
            .is_err()
        );
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

    /// Build a reclassifier CLI with the given trailing args appended.
    fn parse_rtg(extra: &[&str]) -> Result<Cli, clap::Error> {
        let mut args = vec![
            "comet-enrich",
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out",
            "--provenance",
            "e.yaml",
            "--rules",
            "r.yaml",
        ];
        args.extend_from_slice(extra);
        parse(&args)
    }

    #[test]
    fn duplicate_source_release_date_is_rejected() {
        // The duplicate is caught by sources(), which run_method calls before the
        // run, so a metadata typo fails before any output is written.
        let cli = parse_rtg(&[
            "--source-release-date",
            "datacite=2024-01-01",
            "--source-release-date",
            "datacite=2024-02-01",
        ])
        .unwrap();
        assert!(cli.method.io().sources().is_err());
    }

    #[test]
    fn distinct_source_release_dates_build_the_map() {
        let cli = parse_rtg(&[
            "--source-release-date",
            "datacite=2024-01-01",
            "--source-release-date",
            "ror=2024-04-11",
        ])
        .unwrap();
        let sources = cli.method.io().sources().unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources["datacite"].release_date, "2024-01-01");
        assert_eq!(sources["ror"].release_date, "2024-04-11");
    }
}
