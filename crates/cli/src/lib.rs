//! CLI for comet-enrich.
//!
//! Parses command-line arguments, builds the selected enrichment method, and runs it
//! through [comet_enrich_core::run].

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

pub mod args;

use anyhow::Result;
use args::{IoArgs, LookupArgs, RunArgs, StageArg, init_logging};
use clap::{CommandFactory, Parser, Subcommand};
use comet_enrich_core::{
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
    pub command: Command,
}

/// Installation instructions shown by `comet-enrich completions --help`.
const COMPLETIONS_HELP: &str = "\
Installation:
  Load at shell startup (always matches the installed binary):
    bash:  add to ~/.bashrc:                  source <(comet-enrich completions bash)
    zsh:   add to ~/.zshrc (after compinit):  source <(comet-enrich completions zsh)
    fish:  add to ~/.config/fish/config.fish: comet-enrich completions fish | source

  Or install once (regenerate after upgrading):
    bash:  comet-enrich completions bash > ~/.local/share/bash-completion/completions/comet-enrich
    zsh:   comet-enrich completions zsh > <dir on $fpath>/_comet-enrich
    fish:  comet-enrich completions fish > ~/.config/fish/completions/comet-enrich.fish";

/// Subcommand to run: an enrichment method, or a shell-completions generator.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Reclassify resource types from types.resourceType.
    ResourceTypeGeneral(ResourceTypeGeneralArgs),

    /// Match creator affiliation strings to ROR IDs.
    ///
    /// Runs the extract, query, and reconcile stages.
    Affiliations(AffiliationsArgs),

    /// Match funder names to ROR IDs.
    ///
    /// Runs the extract, query, and reconcile stages.
    Funders(FundersArgs),

    /// Generate a shell completion script on stdout.
    #[command(after_long_help = COMPLETIONS_HELP)]
    Completions(CompletionsArgs),
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

/// Arguments for the completions subcommand.
#[derive(clap::Args, Debug)]
pub struct CompletionsArgs {
    /// Shell to generate completions for.
    #[arg(value_enum)]
    pub shell: clap_complete::Shell,
}

/// Arguments for the affiliations method.
#[derive(clap::Args, Debug)]
pub struct AffiliationsArgs {
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

/// Arguments for the funders method.
#[derive(clap::Args, Debug)]
pub struct FundersArgs {
    #[command(flatten)]
    pub io: IoArgs,

    #[command(flatten)]
    pub lookup: LookupArgs,

    /// ROR registry JSON used to build the Crossref Funder ID to ROR crosswalk.
    #[arg(long, value_name = "FILE", help_heading = "ROR matching")]
    pub ror_file: PathBuf,

    #[command(flatten)]
    pub run: RunArgs,

    /// Run a single stage instead of the whole pipeline.
    #[command(subcommand)]
    pub stage: Option<StageArg>,
}

/// Run the selected subcommand.
pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        // Completions write the script to stdout and skip logging and provenance setup.
        Command::Completions(a) => {
            clap_complete::generate(
                a.shell,
                &mut Cli::command(),
                "comet-enrich",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        Command::ResourceTypeGeneral(a) => {
            let template = setup(&a.run, &a.io)?;
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
        Command::Affiliations(a) => {
            let template = setup(&a.run, &a.io)?;
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
        Command::Funders(a) => {
            let template = setup(&a.run, &a.io)?;
            let method = funders::Funders::try_new(funders::Config {
                lookup: (&a.lookup).into(),
                ror_file: a.ror_file.clone(),
            })?;
            run_lookup_method(
                "funders", &method, &a.io, &a.lookup, &a.run, &template, "funder", a.stage,
            )
        }
    }
}

/// Initialise logging and load provenance.
fn setup(run: &RunArgs, io: &IoArgs) -> Result<EnrichmentTemplate> {
    init_logging(run.log_level)?;
    comet_enrich_core::load_template(&io.provenance)
}

/// Run a transform method and write its manifest.
///
/// # Errors
/// Propagates any error from [`comet_enrich_core::run`] (including schema
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
    // Validate CLI metadata before scanning the corpus.
    let sources = io.sources()?;

    let started = Instant::now();
    let stats = comet_enrich_core::run(method, &io.run_options(run), template, validator.as_ref())?;
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
    // The transform path is complete unless it lost input or output records.
    let manifest_status = exit_status(stats.files_failed, stats.schema_failures, 0, true);
    Manifest::build(&stats, &meta, out_of_scope, &timings, manifest_status).write(&io.output)?;

    report_stats(name, &stats);
    Ok(())
}

/// Run a lookup method and write its manifest.
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
    // Validate CLI metadata before scanning the corpus.
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
    // Mark partial for data loss or incomplete staged runs.
    let match_errors = report
        .match_
        .as_ref()
        .map_or(0, |m| m.failure_taxonomy.lost());
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
        ])
        .unwrap();
        let Command::Affiliations(a) = cli.command else {
            panic!("expected affiliations");
        };
        assert_eq!(a.lookup.ror_concurrency, 50);
        assert_eq!(a.lookup.ror_batch_size, 50);
        assert_eq!(a.run.threads, 0);
        assert_eq!(a.run.output_part_size_mib, 256);
        assert_eq!(a.run.output_writer_lanes, 1);
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
        let Command::Funders(a) = cli.command else {
            panic!("expected funders");
        };
        assert_eq!(a.lookup.hash_bits, args::HashBitsArg::Bits128);

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
            "query",
        ])
        .unwrap();
        let Command::Affiliations(a) = cli.command else {
            panic!("expected affiliations");
        };
        assert_eq!(a.stage, Some(StageArg::Query));
    }

    #[test]
    fn ror_file_is_required_for_funders_and_rejected_for_affiliations() {
        let base = |method: &'static str| {
            vec![
                "comet-enrich",
                method,
                "-i",
                "in",
                "-o",
                "out",
                "--provenance",
                "e.yaml",
            ]
        };

        assert!(parse(&base("funders")).is_err());

        let mut funders = base("funders");
        funders.extend_from_slice(&["--ror-file", "ror.json"]);
        assert!(parse(&funders).is_ok());

        let mut affiliations = base("affiliations");
        affiliations.extend_from_slice(&["--ror-file", "ror.json"]);
        assert!(parse(&affiliations).is_err());
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
    fn output_part_options_parse_and_reject_zero() {
        let cli =
            parse_rtg(&["--output-part-size-mib", "16", "--output-writer-lanes", "4"]).unwrap();
        let Command::ResourceTypeGeneral(a) = cli.command else {
            panic!("expected resource-type-general");
        };
        assert_eq!(a.run.output_part_size_mib, 16);
        assert_eq!(a.run.output_writer_lanes, 4);

        assert!(parse_rtg(&["--output-part-size-mib", "0"]).is_err());
        assert!(parse_rtg(&["--output-writer-lanes", "0"]).is_err());
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
    fn completions_parses_each_shell_and_rejects_unknown() {
        for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
            let cli = parse(&["comet-enrich", "completions", shell]).unwrap();
            assert!(matches!(cli.command, Command::Completions(_)));
        }
        assert!(parse(&["comet-enrich", "completions", "tcsh"]).is_err());
        assert!(parse(&["comet-enrich", "completions"]).is_err());
    }

    fn rtg_args(cli: Cli) -> ResourceTypeGeneralArgs {
        let Command::ResourceTypeGeneral(a) = cli.command else {
            panic!("expected resource-type-general");
        };
        a
    }

    #[test]
    fn duplicate_source_release_date_is_rejected() {
        let cli = parse_rtg(&[
            "--source-release-date",
            "datacite=2024-01-01",
            "--source-release-date",
            "datacite=2024-02-01",
        ])
        .unwrap();
        assert!(rtg_args(cli).io.sources().is_err());
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
        let sources = rtg_args(cli).io.sources().unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources["datacite"].release_date, "2024-01-01");
        assert_eq!(sources["ror"].release_date, "2024-04-11");
    }
}
