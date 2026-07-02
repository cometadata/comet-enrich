//! Staged runner for lookup methods.
//!
//! Completed stages leave markers under `<output>/.work`, allowing later runs to
//! resume from the first incomplete stage.

use crate::artifact_lifecycle as lifecycle;
use crate::dedup::{DedupStore, HashBits};
use crate::fanout::{
    FileError, input_files, make_pool, own_skips, progress_bar, scan_jsonl_records, sorted_glob,
};
use crate::manifest::{
    Coverage, HistogramBucket, MatchFailureTaxonomy, MatchSummary, Report, StageTimings, Validation,
};
use crate::match_service::{MatchHit, MatchService};
use crate::method::EnrichmentMethod;
use crate::options::{RunOptions, RunStats};
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::{
    ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, FailureSink, ParallelRollingWriter, RecordBatcher,
};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

// ---------------------------------------------------------------------------
// Stage planning
// ---------------------------------------------------------------------------

/// One stage of a lookup pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Scan the corpus and collect the unique inputs to look up.
    Extract,
    /// Resolve the unique inputs against the match service.
    Query,
    /// Join matches back onto records and emit enrichment records.
    Reconcile,
}

impl Stage {
    /// Stages in execution order.
    pub const ALL: [Stage; 3] = [Stage::Extract, Stage::Query, Stage::Reconcile];

    /// Marker file written when this stage completes.
    #[must_use]
    pub fn marker(self) -> &'static str {
        match self {
            Stage::Extract => "extract.done",
            Stage::Query => "query.done",
            Stage::Reconcile => "reconcile.done",
        }
    }
}

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

/// Work directory for a staged lookup run.
pub struct WorkDir {
    pub path: PathBuf,
}

impl WorkDir {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The work directory for a run output directory.
    #[must_use]
    pub fn for_output(output_dir: &Path) -> Self {
        Self::new(output_dir.join(WORK_DIR))
    }

    #[must_use]
    pub fn marker_path(&self, stage: Stage) -> PathBuf {
        self.path.join(stage.marker())
    }

    /// Return whether the stage marker exists.
    #[must_use]
    pub fn is_complete(&self, stage: Stage) -> bool {
        self.marker_path(stage).exists()
    }

    /// Return whether every stage of the pipeline has completed.
    #[must_use]
    pub fn all_complete(&self) -> bool {
        Stage::ALL.iter().all(|&s| self.is_complete(s))
    }
}

/// Return the stages that should run.
///
/// Completed leading stages are skipped. Once a stage needs to run, all later
/// stages run too, because rerunning an earlier stage invalidates later outputs.
#[must_use]
pub fn stages_to_run(work_dir: &Path, from_scratch: bool) -> Vec<Stage> {
    if from_scratch {
        return Stage::ALL.to_vec();
    }
    let wd = WorkDir::new(work_dir);
    Stage::ALL
        .iter()
        .skip_while(|&&s| wd.is_complete(s))
        .copied()
        .collect()
}

// ---------------------------------------------------------------------------
// On-disk contract
// ---------------------------------------------------------------------------

const EXTRACTIONS_DIR: &str = "extractions";
const INPUTS_FILE: &str = "inputs.jsonl";
const LOOKUPS_FILE: &str = "lookups.jsonl";
const LOOKUPS_FAILED_FILE: &str = "lookups.failed.jsonl";
const HASH_BITS_FILE: &str = "hash.bits";
const EXTRACT_STATS_FILE: &str = "extract.stats.json";
const RECONCILE_STATS_FILE: &str = "reconcile.stats.json";

/// Match-confidence histogram edges. The last bucket includes `1.0`.
const HISTOGRAM_EDGES: [f64; 6] = [0.0, 0.5, 0.7, 0.8, 0.9, 1.0];

/// One `inputs.jsonl` row.
#[derive(Clone, Deserialize)]
struct InputRecord {
    hash: String,
    value: String,
}

/// One `lookups.jsonl` row.
#[derive(Serialize, Deserialize)]
struct LookupRow<L> {
    value: String,
    hash: String,
    #[serde(flatten)]
    lookup: L,
}

/// Failed lookup kind: the service answered and found nothing.
const FAIL_KIND_NO_MATCH: &str = "no_match";
/// Failed lookup kind: the input was never resolved.
const FAIL_KIND_ERROR: &str = "error";

/// One `lookups.failed.jsonl` row.
#[derive(Serialize)]
struct FailedRow<'a> {
    value: &'a str,
    hash: &'a str,
    kind: &'static str,
    error: &'a str,
}

/// Extract-stage counters persisted for resumed runs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ExtractStats {
    files_processed: u64,
    files_failed: u64,
    records_scanned: u64,
    lines_malformed: u64,
    in_scope_units: u64,
    skipped: BTreeMap<String, u64>,
}

/// Reconcile-stage counters persisted for resumed runs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ReconcileStats {
    emitted: u64,
    schema_failures: u64,
}

/// Per-file extract result, reduced across the corpus.
#[derive(Default)]
struct ExtractAgg {
    dedup: DedupStore,
    records_scanned: u64,
    lines_malformed: u64,
    in_scope_units: u64,
    skipped: BTreeMap<&'static str, u64>,
}

impl ExtractAgg {
    fn merge(mut self, other: ExtractAgg) -> ExtractAgg {
        self.dedup.merge(other.dedup);
        self.records_scanned += other.records_scanned;
        self.lines_malformed += other.lines_malformed;
        self.in_scope_units += other.in_scope_units;
        for (reason, n) in other.skipped {
            *self.skipped.entry(reason).or_default() += n;
        }
        self
    }
}

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
    let stages = if let Some(stage) = only_stage {
        ensure_predecessors_done(&wd, stage)?;
        vec![stage]
    } else {
        stages_to_run(work_path, cfg.from_scratch)
    };

    // Validate the input corpus before clearing any artifacts, so a mistyped
    // input path cannot destroy a previous run's outputs.
    if stages.contains(&Stage::Extract) {
        input_files(&io.input)?;
    }

    if cfg.from_scratch {
        lifecycle::clear_run_outputs(&io.output)?;
        lifecycle::remove_dir_if_exists(work_path)?;
    }

    fs::create_dir_all(work_path)
        .with_context(|| format!("creating work dir {}", work_path.display()))?;

    // Pin the hash width on the first run, or refuse a resume that asks for a
    // different one (a width mismatch silently breaks the hash join).
    pin_or_validate_hash_bits(work_path, cfg.hash_bits, cfg.from_scratch)?;

    let mut timings = StageTimings::default();
    let run_start = Instant::now();

    for stage in stages {
        prepare_stage_rerun(&wd, stage, work_path, &io.output)?;
        let started = Instant::now();
        match stage {
            Stage::Extract => {
                run_extract(method, io, work_path, cfg.hash_bits)?;
                timings.extract = Some(elapsed_ms(started));
            }
            Stage::Query => {
                run_query::<M::Lookup>(svc.clone(), cfg, work_path, task)?;
                timings.query = Some(elapsed_ms(started));
            }
            Stage::Reconcile => {
                run_reconcile(method, io, work_path, template, validator)?;
                timings.reconcile = Some(elapsed_ms(started));
            }
        }
        lifecycle::write_marker(&wd.marker_path(stage))
            .with_context(|| format!("writing {} marker", stage.marker()))?;
    }

    timings.total = Some(elapsed_ms(run_start));
    // Read sidecars so resumed runs report stages skipped this invocation.
    build_report(work_path, &wd, timings)
}

/// Whether a staged run directory has completed all stages.
#[must_use]
pub fn pipeline_complete(output_dir: &Path) -> bool {
    WorkDir::for_output(output_dir).all_complete()
}

/// Pin the dedup-hash width in the run dir, or validate it against a resume.
fn pin_or_validate_hash_bits(work: &Path, hash_bits: HashBits, from_scratch: bool) -> Result<()> {
    let path = work.join(HASH_BITS_FILE);
    if path.exists() && !from_scratch {
        let pinned =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        let pinned = pinned.trim();
        if pinned != hash_bits.as_str() {
            bail!(
                "hash-width mismatch: run dir {} is pinned to {pinned}, but --hash-bits requested {}; \
                 resuming with a different width would silently break the hash join (use --from-scratch to rerun)",
                work.display(),
                hash_bits.as_str(),
            );
        }
        Ok(())
    } else {
        fs::write(&path, hash_bits.as_str())
            .with_context(|| format!("pinning hash width to {}", path.display()))
    }
}

/// Require predecessor stages for an explicit single-stage run.
fn ensure_predecessors_done(wd: &WorkDir, stage: Stage) -> Result<()> {
    let needed: &[Stage] = match stage {
        Stage::Extract => &[],
        Stage::Query => &[Stage::Extract],
        Stage::Reconcile => &[Stage::Extract, Stage::Query],
    };
    for &dep in needed {
        if !wd.is_complete(dep) {
            bail!(
                "cannot run {} stage: {} has not completed (missing {})",
                stage.marker().trim_end_matches(".done"),
                dep.marker().trim_end_matches(".done"),
                dep.marker(),
            );
        }
    }
    Ok(())
}

/// Clear markers and artifacts invalidated by rerunning `stage`.
fn prepare_stage_rerun(wd: &WorkDir, stage: Stage, work: &Path, output: &Path) -> Result<()> {
    clear_markers_from(wd, stage)?;
    match stage {
        Stage::Extract => {
            clear_extract_artifacts(work)?;
            clear_query_artifacts(work)?;
            clear_reconcile_artifacts(work, output)?;
        }
        Stage::Query => {
            clear_query_artifacts(work)?;
            clear_reconcile_artifacts(work, output)?;
        }
        Stage::Reconcile => {
            clear_reconcile_artifacts(work, output)?;
        }
    }
    Ok(())
}

fn clear_markers_from(wd: &WorkDir, stage: Stage) -> Result<()> {
    let stages: &[Stage] = match stage {
        Stage::Extract => &Stage::ALL,
        Stage::Query => &[Stage::Query, Stage::Reconcile],
        Stage::Reconcile => &[Stage::Reconcile],
    };
    for &stage in stages {
        lifecycle::remove_file_if_exists(&wd.marker_path(stage))?;
    }
    Ok(())
}

fn clear_extract_artifacts(work: &Path) -> Result<()> {
    lifecycle::recreate_dir(&work.join(EXTRACTIONS_DIR))?;
    lifecycle::remove_file_if_exists(&work.join(INPUTS_FILE))?;
    lifecycle::remove_file_if_exists(&work.join(EXTRACT_STATS_FILE))?;
    Ok(())
}

fn clear_query_artifacts(work: &Path) -> Result<()> {
    lifecycle::remove_file_if_exists(&work.join(LOOKUPS_FILE))?;
    lifecycle::remove_file_if_exists(&work.join(LOOKUPS_FAILED_FILE))?;
    Ok(())
}

fn clear_reconcile_artifacts(work: &Path, output: &Path) -> Result<()> {
    lifecycle::remove_file_if_exists(&work.join(RECONCILE_STATS_FILE))?;
    lifecycle::clear_run_outputs(output)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Extract
// ---------------------------------------------------------------------------

/// Write extractions and unique lookup inputs.
fn run_extract<M>(method: &M, io: &RunOptions, work: &Path, hash_bits: HashBits) -> Result<()>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize,
{
    let files = input_files(&io.input)?;
    log::info!("extract: {} input files", files.len());

    let extractions_dir = work.join(EXTRACTIONS_DIR);
    fs::create_dir_all(&extractions_dir)
        .with_context(|| format!("creating {}", extractions_dir.display()))?;

    let files_failed = AtomicU64::new(0);
    let pb = progress_bar(files.len() as u64)?;
    let pool = make_pool(io.threads)?;
    let agg = pool.install(|| {
        files
            .par_iter()
            .enumerate()
            .map(|(idx, path)| {
                pb.set_message(format!(
                    "extract: {}",
                    path.file_name().unwrap().to_string_lossy()
                ));
                let agg = match extract_one_file(idx, path, &extractions_dir, method) {
                    Ok(agg) => Ok(agg),
                    Err(FileError::Read(e)) => {
                        log::error!("file error {}: {e}", path.display());
                        files_failed.fetch_add(1, Ordering::Relaxed);
                        Ok(ExtractAgg::default())
                    }
                    Err(FileError::Fatal(e)) => Err(e),
                };
                pb.inc(1);
                agg
            })
            .try_reduce(ExtractAgg::default, |a, b| Ok(a.merge(b)))
    })?;
    pb.finish_with_message("extract: done");

    agg.dedup
        .write_jsonl(&work.join(INPUTS_FILE), hash_bits)
        .context("writing inputs.jsonl")?;

    let files_failed = files_failed.load(Ordering::Relaxed);
    let stats = ExtractStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: agg.records_scanned,
        lines_malformed: agg.lines_malformed,
        in_scope_units: agg.in_scope_units,
        skipped: own_skips(agg.skipped),
    };
    let json = serde_json::to_string(&stats).context("serializing extract stats")?;
    fs::write(work.join(EXTRACT_STATS_FILE), json).context("writing extract.stats.json")?;
    log::info!(
        "extract: {} records scanned, {} unique inputs",
        stats.records_scanned,
        agg.dedup.len()
    );
    Ok(())
}

fn extract_one_file<M>(
    idx: usize,
    path: &Path,
    extractions_dir: &Path,
    method: &M,
) -> Result<ExtractAgg, FileError>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize,
{
    let f = File::open(path).map_err(|e| FileError::Read(e.into()))?;
    let reader = BufReader::new(GzDecoder::new(f));

    let part_path = extractions_dir.join(format!("part_{idx:04}.jsonl"));
    let file = File::create(&part_path)
        .with_context(|| format!("creating {}", part_path.display()))
        .map_err(FileError::Fatal)?;
    let mut part = BufWriter::new(file);

    let mut dedup = DedupStore::new();
    let mut in_scope_units: u64 = 0;
    let mut skipped: BTreeMap<&'static str, u64> = BTreeMap::new();

    let tally = scan_jsonl_records(reader, |rec| {
        match method.extract(rec) {
            crate::method::Extracted::Skip(reason) => {
                *skipped.entry(reason).or_default() += 1;
            }
            crate::method::Extracted::Items(items) => {
                for item in items {
                    // Each extraction is one in-scope unit for coverage.
                    in_scope_units += 1;
                    for input in method.inputs(&item) {
                        dedup.insert(input);
                    }
                    serde_json::to_writer(&mut part, &item)
                        .context("serializing extraction")
                        .map_err(FileError::Fatal)?;
                    part.write_all(b"\n")
                        .context("writing extraction")
                        .map_err(FileError::Fatal)?;
                }
            }
        }
        Ok(())
    })?;

    part.flush()
        .with_context(|| format!("flushing {}", part_path.display()))
        .map_err(FileError::Fatal)?;
    Ok(ExtractAgg {
        dedup,
        records_scanned: tally.scanned,
        lines_malformed: tally.malformed,
        in_scope_units,
        skipped,
    })
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

/// Resolve inputs and write lookup result files.
fn run_query<L>(
    svc: Arc<dyn MatchService>,
    cfg: &LookupConfig,
    work: &Path,
    task: &str,
) -> Result<()>
where
    L: Serialize + From<MatchHit> + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    rt.block_on(query_async::<L>(svc, cfg, work, task))
}

async fn query_async<L>(
    svc: Arc<dyn MatchService>,
    cfg: &LookupConfig,
    work: &Path,
    task: &str,
) -> Result<()>
where
    L: Serialize + From<MatchHit> + Send + 'static,
{
    let inputs = read_inputs(&work.join(INPUTS_FILE))?;

    // Query reruns as a whole stage, so previous result files are rewritten.
    let matches_w = Arc::new(AsyncMutex::new(create_line_writer(
        &work.join(LOOKUPS_FILE),
    )?));
    let failed_w = Arc::new(AsyncMutex::new(create_line_writer(
        &work.join(LOOKUPS_FAILED_FILE),
    )?));

    if inputs.is_empty() {
        log::info!("query: nothing to resolve");
        matches_w.lock().await.flush()?;
        failed_w.lock().await.flush()?;
        return Ok(());
    }
    log::info!("query: {} inputs to resolve", inputs.len());
    let pb = progress_bar(inputs.len() as u64)?;
    pb.set_message("query");

    let semaphore = Arc::new(Semaphore::new(cfg.ror_concurrency.max(1)));
    let task = task.to_owned();

    let batches: Vec<Vec<InputRecord>> = inputs
        .chunks(cfg.ror_batch_size.max(1))
        .map(<[InputRecord]>::to_vec)
        .collect();

    let mut handles = Vec::with_capacity(batches.len());
    for batch in batches {
        let svc = Arc::clone(&svc);
        let matches_w = Arc::clone(&matches_w);
        let failed_w = Arc::clone(&failed_w);
        let semaphore = Arc::clone(&semaphore);
        let task = task.clone();
        let pb = pb.clone();

        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore not closed");
            let values: Vec<String> = batch.iter().map(|r| r.value.clone()).collect();

            match svc.match_bulk(&values, &task).await {
                Ok(results) => {
                    let mut hits: Vec<String> = Vec::new();
                    let mut misses: Vec<String> = Vec::new();
                    for (rec, res) in batch.iter().zip(results) {
                        match res {
                            Some((id, confidence)) => {
                                let row = LookupRow {
                                    value: rec.value.clone(),
                                    hash: rec.hash.clone(),
                                    lookup: L::from(MatchHit { id, confidence }),
                                };
                                hits.push(serde_json::to_string(&row)?);
                            }
                            None => misses.push(serde_json::to_string(&FailedRow {
                                value: &rec.value,
                                hash: &rec.hash,
                                kind: FAIL_KIND_NO_MATCH,
                                error: "no match",
                            })?),
                        }
                    }
                    write_lines(&matches_w, &hits).await?;
                    write_lines(&failed_w, &misses).await?;
                }
                Err(e) => {
                    // Whole-batch failures are lost inputs.
                    let error = format!("batch error: {e}");
                    let lines: Vec<String> = batch
                        .iter()
                        .map(|rec| {
                            serde_json::to_string(&FailedRow {
                                value: &rec.value,
                                hash: &rec.hash,
                                kind: FAIL_KIND_ERROR,
                                error: &error,
                            })
                        })
                        .collect::<Result<_, _>>()?;
                    write_lines(&failed_w, &lines).await?;
                }
            }
            pb.inc(batch.len() as u64);
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await.context("query task panicked")??;
    }
    pb.finish_with_message("query: done");

    matches_w.lock().await.flush()?;
    failed_w.lock().await.flush()?;
    Ok(())
}

fn read_inputs(path: &Path) -> Result<Vec<InputRecord>> {
    let mut rows = Vec::new();
    for_each_jsonl(path, |row: InputRecord| rows.push(row))?;
    Ok(rows)
}

/// Create a JSONL writer, truncating any prior file.
fn create_line_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    Ok(BufWriter::new(file))
}

async fn write_lines(writer: &AsyncMutex<BufWriter<File>>, lines: &[String]) -> Result<()> {
    if lines.is_empty() {
        return Ok(());
    }
    let mut w = writer.lock().await;
    for line in lines {
        w.write_all(line.as_bytes())?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Join lookups onto extractions and write enrichment records.
fn run_reconcile<M>(
    method: &M,
    io: &RunOptions,
    work: &Path,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::Validator>,
) -> Result<()>
where
    M: EnrichmentMethod,
    M::Extraction: DeserializeOwned,
    M::Lookup: DeserializeOwned + Send + Sync,
{
    let lookups = load_lookups::<M::Lookup>(&work.join(LOOKUPS_FILE))?;
    log::info!("reconcile: {} lookups loaded", lookups.len());

    let parts = sorted_glob(&format!(
        "{}/part_*.jsonl",
        work.join(EXTRACTIONS_DIR).to_string_lossy()
    ))?;

    let enrich_dir = io.output.join(ENRICHMENTS_DIR);
    fs::create_dir_all(&enrich_dir)
        .with_context(|| format!("creating {}", enrich_dir.display()))?;
    let failures = Mutex::new(FailureSink::create(
        &io.output.join(ENRICHMENTS_FAILED_FILE),
    ));
    let writer = ParallelRollingWriter::create(
        &enrich_dir,
        validator,
        &failures,
        io.output_part_size_bytes,
        io.output_writer_lanes,
    )?;

    let pb = progress_bar(parts.len() as u64)?;
    let pool = make_pool(io.threads)?;
    pool.install(|| {
        parts.par_iter().try_for_each(|path| {
            pb.set_message(format!(
                "reconcile: {}",
                path.file_name().unwrap().to_string_lossy()
            ));
            reconcile_one_part(path, &lookups, method, template, &writer, io.batch_size)?;
            pb.inc(1);
            Ok::<(), anyhow::Error>(())
        })
    })?;
    pb.finish_with_message("reconcile: done");

    let emitted = writer.finish()?;
    let mut failures = failures.lock().unwrap();
    failures.flush()?;
    let stats = ReconcileStats {
        emitted,
        schema_failures: failures.records_failed,
    };
    let json = serde_json::to_string(&stats).context("serializing reconcile stats")?;
    fs::write(work.join(RECONCILE_STATS_FILE), json).context("writing reconcile.stats.json")?;
    log::info!(
        "reconcile: {} records emitted, {} schema failures",
        stats.emitted,
        stats.schema_failures
    );
    Ok(())
}

fn load_lookups<L: DeserializeOwned>(path: &Path) -> Result<crate::method::Lookups<L>> {
    let mut map = crate::method::Lookups::new();
    for_each_jsonl(path, |row: LookupRow<L>| {
        map.insert(row.hash, row.lookup);
    })?;
    Ok(map)
}

fn reconcile_one_part<M>(
    path: &Path,
    lookups: &crate::method::Lookups<M::Lookup>,
    method: &M,
    template: &EnrichmentTemplate,
    writer: &ParallelRollingWriter<'_>,
    batch_size: usize,
) -> Result<()>
where
    M: EnrichmentMethod,
    M::Extraction: DeserializeOwned,
{
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut batcher = RecordBatcher::new(writer, batch_size);

    for line in reader.lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let extraction: M::Extraction =
            serde_json::from_str(&line).context("parsing extraction row")?;
        for parts in method.map_back(extraction, lookups) {
            batcher.push(build_enrichment_record(template, parts))?;
        }
    }
    batcher.finish()
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Assemble a [`Report`] from persisted stage stats.
fn build_report(work: &Path, wd: &WorkDir, timings: StageTimings) -> Result<Report> {
    let extract: ExtractStats = read_stats(&work.join(EXTRACT_STATS_FILE), "extract.stats.json")?;
    let reconcile: ReconcileStats =
        read_stats(&work.join(RECONCILE_STATS_FILE), "reconcile.stats.json")?;

    let counters = RunStats {
        files_processed: extract.files_processed,
        files_failed: extract.files_failed,
        records_scanned: extract.records_scanned,
        lines_malformed: extract.lines_malformed,
        emitted: reconcile.emitted,
        schema_failures: reconcile.schema_failures,
        skipped: extract.skipped,
    };

    let match_ = if wd.is_complete(Stage::Query) {
        Some(build_match_summary(work)?)
    } else {
        None
    };

    Ok(Report {
        counters,
        coverage: Coverage::new(extract.in_scope_units, reconcile.emitted),
        match_,
        validation: Validation::new(reconcile.emitted, reconcile.schema_failures),
        stage_timings_ms: timings,
    })
}

/// Read a persisted stats sidecar, defaulting to empty when the stage hasn't run.
fn read_stats<T: DeserializeOwned + Default>(path: &Path, what: &str) -> Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parsing {what}"))
}

/// One `lookups.jsonl` row, read back only for its confidence (other fields ignored).
#[derive(Deserialize)]
struct ConfidenceRow {
    #[serde(default)]
    confidence: Option<f64>,
}

/// Failed lookup row fields needed for reporting.
#[derive(Deserialize)]
struct FailureRow {
    /// Absent on rows written by builds that predate the `kind` field.
    #[serde(default)]
    kind: Option<String>,
    error: String,
}

/// Compute the match-quality block from lookup artifacts.
#[allow(clippy::cast_precision_loss)]
fn build_match_summary(work: &Path) -> Result<MatchSummary> {
    let unique_inputs = count_lines(&work.join(INPUTS_FILE))?;

    let mut matched: u64 = 0;
    let mut buckets = vec![0u64; HISTOGRAM_EDGES.len() - 1];
    for_each_jsonl(&work.join(LOOKUPS_FILE), |row: ConfidenceRow| {
        matched += 1;
        if let Some(c) = row.confidence {
            buckets[histogram_bucket(c)] += 1;
        }
    })?;

    let mut taxonomy = MatchFailureTaxonomy::default();
    for_each_jsonl(&work.join(LOOKUPS_FAILED_FILE), |row: FailureRow| {
        classify_failure(row.kind.as_deref(), &row.error, &mut taxonomy);
    })?;

    let confidence_histogram = HISTOGRAM_EDGES
        .windows(2)
        .zip(&buckets)
        .map(|(edge, &count)| HistogramBucket {
            min: edge[0],
            max: edge[1],
            count,
        })
        .collect();

    let match_rate = if unique_inputs == 0 {
        0.0
    } else {
        matched as f64 / unique_inputs as f64
    };

    Ok(MatchSummary {
        unique_inputs,
        matched,
        match_rate,
        confidence_histogram,
        failure_taxonomy: taxonomy,
    })
}

/// Index of the histogram bucket a confidence falls in (clamped to the range).
fn histogram_bucket(c: f64) -> usize {
    // Edges are ascending; the last bucket is inclusive of the upper bound.
    for i in (0..HISTOGRAM_EDGES.len() - 1).rev() {
        if c >= HISTOGRAM_EDGES[i] {
            return i;
        }
    }
    0
}

/// Bin one failed lookup row.
///
/// Missing legacy `kind` values count as errors, never no-matches.
fn classify_failure(kind: Option<&str>, error: &str, taxonomy: &mut MatchFailureTaxonomy) {
    if kind == Some(FAIL_KIND_NO_MATCH) {
        taxonomy.no_match += 1;
        return;
    }
    let lower = error.to_ascii_lowercase();
    if lower.contains("timeout") || lower.contains("timed out") {
        taxonomy.timeout += 1;
    } else {
        taxonomy.error += 1;
    }
}

/// Count the non-empty lines in a JSONL file without parsing them (absent → 0).
fn count_lines(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut n = 0;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if !line.trim().is_empty() {
            n += 1;
        }
    }
    Ok(n)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::MANIFEST_FILE;
    use crate::match_service::{FakeMatchService, RorLookup};
    use crate::method::{EnrichmentAction, EnrichmentParts, Extracted, Lookups};
    use crate::provenance::EnrichmentTemplate;

    use async_trait::async_trait;
    use comet_enrich_test_support::{
        assert_close, assert_err_contains, gz_input_fixture, gz_parts_fixture,
        read_enrichment_parts, write_gz_lines,
    };
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::path::PathBuf;

    struct TestMethod {
        hash_bits: HashBits,
    }

    #[derive(Serialize, Deserialize)]
    struct TestExtraction {
        doi: String,
        name: String,
        name_hash: String,
    }

    impl EnrichmentMethod for TestMethod {
        type Extraction = TestExtraction;
        type Lookup = RorLookup;

        fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
            let doi = record.get("id").and_then(Value::as_str).unwrap_or("");
            let name = record
                .pointer("/attributes/name")
                .and_then(Value::as_str)
                .unwrap_or("");
            if doi.is_empty() || name.is_empty() {
                return Extracted::Skip("no_name");
            }
            Extracted::Items(vec![TestExtraction {
                doi: doi.to_owned(),
                name: name.to_owned(),
                name_hash: crate::dedup::hash_input(name, self.hash_bits),
            }])
        }

        fn inputs(&self, extraction: &Self::Extraction) -> Vec<String> {
            vec![extraction.name.clone()]
        }

        fn map_back(
            &self,
            extraction: Self::Extraction,
            lookups: &Lookups<Self::Lookup>,
        ) -> Vec<EnrichmentParts> {
            match lookups.get(&extraction.name_hash) {
                Some(hit) => vec![EnrichmentParts {
                    doi: extraction.doi,
                    action: EnrichmentAction::UpdateChild,
                    field: "fundingReferences",
                    original: json!({ "name": extraction.name }),
                    enriched: json!({
                        "name": extraction.name,
                        "funderIdentifier": hit.ror_id,
                        "confidence": hit.confidence,
                    }),
                }],
                None => Vec::new(),
            }
        }
    }

    fn template() -> EnrichmentTemplate {
        EnrichmentTemplate {
            contributors: json!([]),
            resources: json!([]),
        }
    }

    fn fake_service() -> Arc<dyn MatchService> {
        let mut map = HashMap::new();
        map.insert(
            "MIT".to_owned(),
            ("https://ror.org/042nb2s44".to_owned(), 0.99),
        );
        map.insert(
            "NSF".to_owned(),
            ("https://ror.org/021nxhr62".to_owned(), 0.95),
        );
        Arc::new(FakeMatchService::new(map))
    }

    struct PanickingMatchService;

    #[async_trait]
    impl MatchService for PanickingMatchService {
        async fn match_bulk(
            &self,
            _inputs: &[String],
            _task: &str,
        ) -> Result<Vec<Option<(String, f64)>>> {
            panic!("simulated query panic");
        }
    }

    fn sample_records() -> Vec<Value> {
        vec![
            json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } }),
            json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } }),
            json!({ "id": "10.1/unknown", "attributes": { "name": "Unknown University" } }),
            json!({ "id": "10.1/empty", "attributes": {} }),
        ]
    }

    fn cfg(hash_bits: HashBits, from_scratch: bool) -> LookupConfig {
        LookupConfig {
            ror_service_url: "http://unused".to_owned(),
            ror_batch_size: 2,
            ror_concurrency: 2,
            ror_timeout: 30,
            hash_bits,
            from_scratch,
        }
    }

    struct TestRun {
        _dir: tempfile::TempDir,
        input: PathBuf,
        output: PathBuf,
        method: TestMethod,
        svc: Arc<dyn MatchService>,
        tmpl: EnrichmentTemplate,
    }

    impl TestRun {
        fn new() -> Self {
            Self::from_fixture(gz_input_fixture(&sample_records()))
        }

        fn from_fixture(fixture: (tempfile::TempDir, PathBuf, PathBuf)) -> Self {
            let (dir, input, output) = fixture;
            TestRun {
                _dir: dir,
                input,
                output,
                method: TestMethod {
                    hash_bits: HashBits::Bits64,
                },
                svc: fake_service(),
                tmpl: template(),
            }
        }

        fn opts(&self) -> RunOptions {
            RunOptions {
                input: self.input.clone(),
                output: self.output.clone(),
                threads: 1,
                batch_size: 100,
                output_part_size_bytes: 256 * 1024 * 1024,
                output_writer_lanes: 1,
            }
        }

        fn work(&self) -> PathBuf {
            self.output.join(WORK_DIR)
        }

        fn run(&self, from_scratch: bool) -> Result<Report> {
            self.run_with(&cfg(HashBits::Bits64, from_scratch), None, None)
        }

        fn run_stage(&self, stage: Stage) -> Result<Report> {
            self.run_with(&cfg(HashBits::Bits64, false), None, Some(stage))
        }

        fn run_with(
            &self,
            cfg: &LookupConfig,
            validator: Option<&jsonschema::Validator>,
            only_stage: Option<Stage>,
        ) -> Result<Report> {
            run_staged(
                &self.method,
                &self.opts(),
                cfg,
                &self.svc,
                &self.tmpl,
                validator,
                "funder",
                only_stage,
            )
        }
    }

    #[test]
    fn full_pipeline_produces_contract_and_match_block() {
        let t = TestRun::new();

        let report = t.run(true).unwrap();

        let m = report.match_.expect("match block present");
        assert_eq!(m.unique_inputs, 3);
        assert_eq!(m.matched, 2);
        assert_eq!(m.failure_taxonomy.no_match, 1);
        assert_eq!(m.failure_taxonomy.error, 0);
        let top_bucket = m.confidence_histogram.last().unwrap();
        assert_close(top_bucket.max, 1.0);
        assert_eq!(top_bucket.count, 2);

        assert_eq!(report.counters.records_scanned, 4);
        assert_eq!(report.counters.emitted, 2);
        assert_eq!(report.counters.skipped.get("no_name"), Some(&1));
        assert_eq!(report.coverage.records_in_scope, 3);
        assert_eq!(report.coverage.records_enriched, 2);
        assert_close(report.coverage.coverage_rate, 2.0 / 3.0);

        assert!(report.stage_timings_ms.extract.is_some());
        assert!(report.stage_timings_ms.query.is_some());
        assert!(report.stage_timings_ms.reconcile.is_some());

        let work = t.work();
        for f in [
            "extractions/part_0000.jsonl",
            INPUTS_FILE,
            LOOKUPS_FILE,
            LOOKUPS_FAILED_FILE,
            HASH_BITS_FILE,
            "extract.done",
            "query.done",
            "reconcile.done",
        ] {
            assert!(work.join(f).exists(), "missing work artifact: {f}");
        }
        assert_eq!(
            fs::read_to_string(work.join(HASH_BITS_FILE)).unwrap(),
            "xxh3-64"
        );

        let dois = read_output_dois(&t.output);
        assert_eq!(dois.len(), 2);
        assert!(dois.contains(&"10.1/mit".to_owned()));
        assert!(dois.contains(&"10.1/nsf".to_owned()));
    }

    fn read_output_dois(output: &Path) -> Vec<String> {
        read_enrichment_parts(output)
            .iter()
            .map(|rec| {
                assert_eq!(rec["field"], "fundingReferences");
                assert_eq!(rec["action"], "updateChild");
                rec["doi"].as_str().unwrap().to_owned()
            })
            .collect()
    }

    #[test]
    fn only_stage_extract_runs_just_extract() {
        let t = TestRun::new();

        t.run_stage(Stage::Extract).unwrap();

        let work = t.work();
        assert!(work.join("extract.done").exists());
        assert!(work.join(INPUTS_FILE).exists());
        assert!(!work.join("query.done").exists());
        assert!(!work.join("reconcile.done").exists());
        assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
    }

    #[test]
    fn from_scratch_with_single_stage_errors() {
        let t = TestRun::new();

        assert_err_contains(
            t.run_with(&cfg(HashBits::Bits64, true), None, Some(Stage::Extract)),
            "cannot be combined with a single stage",
        );
    }

    #[test]
    fn empty_input_errors_before_clearing_outputs() {
        let t = TestRun::new();

        t.run(true).unwrap();
        assert_eq!(read_output_dois(&t.output).len(), 2);

        // Emptying the input (as a mistyped --input would) must error before any
        // prior outputs are cleared, even with --from-scratch.
        fs::remove_file(t.input.join("updated_2024-01/part_0000.jsonl.gz")).unwrap();
        assert_err_contains(t.run(true), "no *.jsonl.gz input files found");
        assert_eq!(read_output_dois(&t.output).len(), 2);
    }

    #[test]
    fn only_stage_query_without_extract_errors() {
        let t = TestRun::new();

        assert_err_contains(t.run_stage(Stage::Query), "extract");
    }

    #[test]
    fn resume_after_deleting_reconcile_marker_reproduces_output() {
        let t = TestRun::new();

        t.run(true).unwrap();

        fs::remove_file(t.work().join("reconcile.done")).unwrap();
        let report = t.run(false).unwrap();

        assert_eq!(report.counters.emitted, 2);
        assert!(report.stage_timings_ms.reconcile.is_some());
        assert!(report.stage_timings_ms.extract.is_none());
        assert_eq!(read_output_dois(&t.output).len(), 2);
    }

    #[test]
    fn from_scratch_failure_invalidates_old_downstream_markers() {
        let mut t = TestRun::new();

        t.run(true).unwrap();

        t.svc = Arc::new(PanickingMatchService);
        assert_err_contains(t.run(true), "query task panicked");

        let work = t.work();
        assert!(work.join("extract.done").exists());
        assert!(!work.join("query.done").exists());
        assert!(!work.join("reconcile.done").exists());
        assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
    }

    #[test]
    fn from_scratch_with_fewer_inputs_removes_obsolete_extraction_parts() {
        let first = [json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } })];
        let second = [json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } })];
        let t = TestRun::from_fixture(gz_parts_fixture(&[&first, &second]));

        t.run(true).unwrap();
        assert!(t.work().join("extractions/part_0001.jsonl").exists());
        write_gz_lines(
            &t.output.join(ENRICHMENTS_DIR).join("part_9999.jsonl.gz"),
            &[r#"{"doi":"stale"}"#],
        );
        assert!(
            t.output
                .join(ENRICHMENTS_DIR)
                .join("part_9999.jsonl.gz")
                .exists()
        );

        fs::remove_file(t.input.join("updated_2024-01/part_0001.jsonl.gz")).unwrap();
        t.run(true).unwrap();

        assert!(!t.work().join("extractions/part_0001.jsonl").exists());
        assert!(
            !t.output
                .join(ENRICHMENTS_DIR)
                .join("part_9999.jsonl.gz")
                .exists()
        );
        assert_eq!(read_output_dois(&t.output), vec!["10.1/mit".to_owned()]);
    }

    #[test]
    fn single_stage_extract_invalidates_downstream_artifacts() {
        let t = TestRun::new();

        t.run(true).unwrap();
        fs::write(t.output.join(MANIFEST_FILE), "stale").unwrap();

        t.run_stage(Stage::Extract).unwrap();

        let work = t.work();
        assert!(work.join("extract.done").exists());
        assert!(!work.join("query.done").exists());
        assert!(!work.join("reconcile.done").exists());
        assert!(!work.join(LOOKUPS_FILE).exists());
        assert!(!work.join(RECONCILE_STATS_FILE).exists());
        assert!(!t.output.join(MANIFEST_FILE).exists());
        assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
    }

    #[test]
    fn single_stage_query_invalidates_reconcile_artifacts() {
        let t = TestRun::new();

        t.run(true).unwrap();
        fs::write(t.output.join(MANIFEST_FILE), "stale").unwrap();

        t.run_stage(Stage::Query).unwrap();

        let work = t.work();
        assert!(work.join("extract.done").exists());
        assert!(work.join("query.done").exists());
        assert!(!work.join("reconcile.done").exists());
        assert!(!work.join(RECONCILE_STATS_FILE).exists());
        assert!(!t.output.join(MANIFEST_FILE).exists());
        assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
    }

    #[test]
    fn single_stage_reconcile_replaces_stale_public_outputs() {
        let t = TestRun::new();

        t.run(true).unwrap();
        write_gz_lines(
            &t.output.join(ENRICHMENTS_DIR).join("part_9999.jsonl.gz"),
            &[r#"{"doi":"stale"}"#],
        );
        fs::write(t.output.join(ENRICHMENTS_FAILED_FILE), "stale\n").unwrap();

        t.run_stage(Stage::Reconcile).unwrap();

        assert!(t.work().join("reconcile.done").exists());
        assert!(
            !t.output
                .join(ENRICHMENTS_DIR)
                .join("part_9999.jsonl.gz")
                .exists()
        );
        assert!(!t.output.join(ENRICHMENTS_FAILED_FILE).exists());
        let dois = read_output_dois(&t.output);
        assert_eq!(dois.len(), 2);
        assert!(dois.contains(&"10.1/mit".to_owned()));
        assert!(dois.contains(&"10.1/nsf".to_owned()));
    }

    #[test]
    fn resume_with_mismatched_hash_width_errors() {
        let t = TestRun::new();

        t.run(true).unwrap();

        assert_err_contains(
            t.run_with(&cfg(HashBits::Bits128, false), None, None),
            "hash-width mismatch",
        );
    }

    #[test]
    fn rerun_of_complete_pipeline_keeps_truthful_manifest() {
        let t = TestRun::new();

        let first = t.run(true).unwrap();

        let again = t.run(false).unwrap();

        assert_eq!(again.counters.emitted, first.counters.emitted);
        assert_eq!(again.counters.emitted, 2);
        assert_eq!(
            again.coverage.records_in_scope,
            first.coverage.records_in_scope
        );
        assert_eq!(again.coverage.records_enriched, 2);
        assert_eq!(again.match_.unwrap().matched, 2);
    }

    #[test]
    fn rejecting_validator_surfaces_schema_failures() {
        let t = TestRun::new();
        let schema =
            crate::schema::compile_str(r#"{"type":"object","required":["nope"]}"#).unwrap();

        let report = t
            .run_with(&cfg(HashBits::Bits64, true), Some(&schema), None)
            .unwrap();

        assert_eq!(report.counters.emitted, 0);
        assert_eq!(report.counters.schema_failures, 2);
        assert_eq!(report.validation.schema_failures, 2);
        assert!(t.output.join(ENRICHMENTS_FAILED_FILE).exists());
        assert_eq!(
            crate::exit_status(0, report.counters.schema_failures, 0, true),
            "partial"
        );
    }

    #[test]
    fn batch_error_is_recorded_not_certified_as_success() {
        let mut t = TestRun::new();
        t.svc = Arc::new(FakeMatchService::erroring("marple outage"));

        let report = t.run(true).unwrap();

        let m = report.match_.expect("match block present");
        assert_eq!(m.matched, 0);
        assert_eq!(m.failure_taxonomy.error, 3);
        assert_eq!(m.failure_taxonomy.no_match, 0);
        assert_eq!(report.counters.emitted, 0);
        let status = crate::exit_status(
            report.counters.files_failed,
            0,
            m.failure_taxonomy.lost(),
            true,
        );
        assert_eq!(status, "partial");
    }

    #[test]
    fn batch_timeout_is_lost_data_not_success() {
        let mut t = TestRun::new();
        t.svc = Arc::new(FakeMatchService::erroring("operation timed out"));

        let report = t.run(true).unwrap();

        let m = report.match_.expect("match block present");
        assert_eq!(m.matched, 0);
        assert_eq!(m.failure_taxonomy.timeout, 3);
        assert_eq!(m.failure_taxonomy.error, 0);
        assert_eq!(m.failure_taxonomy.lost(), 3);
        let status = crate::exit_status(0, 0, m.failure_taxonomy.lost(), true);
        assert_eq!(status, "partial");
    }

    #[test]
    fn classify_failure_bins_by_kind_not_message() {
        let mut t = MatchFailureTaxonomy::default();
        classify_failure(Some("no_match"), "no match", &mut t);
        classify_failure(Some("no_match"), "server said: timed out no match", &mut t);
        classify_failure(Some("error"), "batch error: operation timed out", &mut t);
        classify_failure(Some("error"), "batch error: HTTP 500", &mut t);
        classify_failure(Some("error"), "batch error: no match endpoint", &mut t);
        classify_failure(None, "no match", &mut t);

        assert_eq!(t.no_match, 2);
        assert_eq!(t.timeout, 1);
        assert_eq!(t.error, 3);
        assert_eq!(t.lost(), 4);
    }

    #[test]
    fn exit_status_is_success_only_when_clean_and_complete() {
        assert_eq!(crate::exit_status(0, 0, 0, true), "success");
        assert_eq!(crate::exit_status(1, 0, 0, true), "partial");
        assert_eq!(crate::exit_status(0, 1, 0, true), "partial");
        assert_eq!(crate::exit_status(0, 0, 1, true), "partial");
        assert_eq!(crate::exit_status(0, 0, 0, false), "partial");
    }

    #[test]
    fn stages_to_run_restart_runs_everything() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
        assert_eq!(stages_to_run(dir.path(), true), Stage::ALL);
    }

    #[test]
    fn stages_to_run_resume_skips_completed_leading_stages() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
        assert_eq!(
            stages_to_run(dir.path(), false),
            vec![Stage::Query, Stage::Reconcile]
        );
    }

    #[test]
    fn stages_to_run_empty_work_dir_runs_all() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(stages_to_run(dir.path(), false), Stage::ALL);
    }

    #[test]
    fn histogram_bucket_clamps_and_includes_top_edge() {
        assert_eq!(histogram_bucket(0.0), 0);
        assert_eq!(histogram_bucket(0.49), 0);
        assert_eq!(histogram_bucket(0.5), 1);
        assert_eq!(histogram_bucket(0.85), 3);
        assert_eq!(histogram_bucket(0.9), 4);
        assert_eq!(histogram_bucket(1.0), 4);
        assert_eq!(histogram_bucket(1.5), 4);
        assert_eq!(histogram_bucket(-0.1), 0);
    }
}
