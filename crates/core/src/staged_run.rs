//! Staged runner for lookup methods.
//!
//! Completed stages leave markers under `<output>/.work`, allowing later runs to
//! resume from the first incomplete stage.

mod extract;
mod fingerprint;
mod planning;
mod query;
mod reconcile;
mod report;

#[cfg(test)]
mod tests;

pub use planning::{Stage, WorkDir, pipeline_complete, stages_to_run};

use crate::artifact_lifecycle as lifecycle;
use crate::dedup::HashBits;
use crate::fanout::input_files;
use crate::manifest::{Report, StageTimings};
use crate::match_service::{MatchHit, MatchService};
use crate::method::EnrichmentMethod;
use crate::options::RunOptions;
use crate::template::EnrichmentTemplate;

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

/// Match-service configuration for a lookup method.
pub struct LookupConfig {
    /// Base URL of the ROR match service.
    pub ror_service_url: String,
    /// Inputs per match-service request.
    pub ror_batch_size: usize,
    /// Concurrent match-service requests.
    pub ror_concurrency: usize,
    /// Match-service request timeout in seconds.
    pub ror_timeout: u64,
    /// Width of the content-addressed dedup hash. Fixed for a whole run: the runner
    /// keys `inputs.jsonl`/`lookups.jsonl` at this width and a method's `extract`
    /// hashes occurrences at the same width so `map_back` can index the results.
    pub hash_bits: HashBits,
    /// Ignore existing stage outputs and rerun from the start.
    pub from_scratch: bool,
}

/// Scratch subdirectory for staged intermediates.
pub const WORK_DIR: &str = ".work";

// ---------------------------------------------------------------------------
// On-disk contract
// ---------------------------------------------------------------------------

const EXTRACTIONS_DIR: &str = "extractions";
const INPUTS_FILE: &str = "inputs.jsonl";
const LOOKUPS_FILE: &str = "lookups.jsonl";
const LOOKUPS_FAILED_FILE: &str = "lookups.failed.jsonl";
const HASH_BITS_FILE: &str = "hash.bits";
const INPUTS_FINGERPRINT_FILE: &str = "inputs.fingerprint.json";
const EXTRACT_STATS_FILE: &str = "extract.stats.json";
const RECONCILE_STATS_FILE: &str = "reconcile.stats.json";

/// Run a lookup method through the staged pipeline.
///
/// With `only_stage`, that stage runs and its predecessors must already be
/// complete. Otherwise the runner resumes from the first incomplete stage.
///
/// # Errors
///
/// Returns an error for invalid stage options, missing input, hash-width
/// mismatches, missing predecessor stages, I/O errors, hash collisions, or
/// match-service batch failures.
#[allow(clippy::too_many_arguments)]
pub fn run_staged<M>(
    method: &M,
    io: &RunOptions,
    cfg: &LookupConfig,
    svc: &Arc<dyn MatchService>,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::Validator>,
    task: &str,
    only_stage: Option<Stage>,
) -> Result<Report>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize + DeserializeOwned,
    M::Lookup: Serialize + DeserializeOwned + From<MatchHit> + Send + Sync + 'static,
{
    if cfg.from_scratch && only_stage.is_some() {
        bail!(
            "--from-scratch cannot be combined with a single stage; \
             run the full pipeline with --from-scratch, or rerun the stage without it"
        );
    }

    let wd = WorkDir::for_output(&io.output);
    let work_path = wd.path.as_path();

    // Plan the stages before touching anything on disk.
    let mut stages = if let Some(stage) = only_stage {
        planning::ensure_predecessors_done(&wd, stage)?;
        vec![stage]
    } else {
        stages_to_run(work_path, cfg.from_scratch)
    };

    if only_stage.is_none() && stages.is_empty() {
        let reconciled = report::read_reconcile_stats(work_path, true)?;
        if reconciled.source_id != template.source_id() {
            stages.push(Stage::Reconcile);
        }
    }

    // Validate the input corpus before clearing any artifacts, so a mistyped
    // input path cannot destroy a previous run's outputs.
    if stages.contains(&Stage::Extract) {
        input_files(&io.input)?;
    } else if only_stage.is_none() {
        // When extract is skipped, verify the input still matches the saved
        // fingerprint. Single-stage runs only use existing work artifacts.
        fingerprint::validate_input_fingerprint(work_path, &io.input)?;
    }

    if cfg.from_scratch {
        lifecycle::clear_run_outputs(&io.output)?;
        lifecycle::remove_dir_if_exists(work_path)?;
    }

    fs::create_dir_all(work_path)
        .with_context(|| format!("creating work dir {}", work_path.display()))?;

    // Pin the hash width on the first run, or refuse a resume that asks for a
    // different one (a width mismatch silently breaks the hash join).
    planning::pin_or_validate_hash_bits(work_path, cfg.hash_bits, cfg.from_scratch)?;

    let mut timings = StageTimings::default();
    let run_start = Instant::now();

    for stage in stages {
        planning::prepare_stage_rerun(&wd, stage, work_path, &io.output)?;
        let started = Instant::now();
        match stage {
            Stage::Extract => {
                extract::run_extract(method, io, work_path, cfg.hash_bits)?;
                timings.extract = Some(planning::elapsed_ms(started));
            }
            Stage::Query => {
                query::run_query::<M::Lookup>(svc.clone(), cfg, work_path, task)?;
                timings.query = Some(planning::elapsed_ms(started));
            }
            Stage::Reconcile => {
                reconcile::run_reconcile(method, io, work_path, template, validator)?;
                timings.reconcile = Some(planning::elapsed_ms(started));
            }
        }
        lifecycle::write_marker(&wd.marker_path(stage))
            .with_context(|| format!("writing {} marker", stage.marker()))?;
    }

    timings.total = Some(planning::elapsed_ms(run_start));
    // Read sidecars so resumed runs report stages skipped this invocation.
    report::build_report(work_path, &wd, timings)
}

/// Read non-empty JSONL rows from an optional file.
fn for_each_jsonl<T: DeserializeOwned>(path: &Path, mut f: impl FnMut(T)) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: T = serde_json::from_str(&line).context("parsing jsonl row")?;
        f(row);
    }
    Ok(())
}
