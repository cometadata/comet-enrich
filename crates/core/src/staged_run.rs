//! The staged runner that drives a lookup method through extract → query →
//! reconcile, serializing the on-disk stage contract under `<output>/.work`.
//!
//! Where [`crate::reader::run`] is the single-pass transform path, [`run_staged`]
//! is the lookup path: it scans the corpus once (extract), resolves the unique
//! inputs against a [`MatchService`] (query), then joins the matches back onto each
//! extraction and emits enrichment records (reconcile). Each stage writes a `.done`
//! marker; an interrupted run resumes from the first incomplete stage.
//!
//! The runner is generic over the method's `Extraction` and `Lookup` types. It
//! reads inputs out of an opaque extraction through [`EnrichmentMethod::inputs`] and
//! builds a method's `Lookup` from a service result through `From<MatchHit>`, so it
//! never names a method's fields.

use crate::checkpoint::Checkpoint;
use crate::dedup::{DedupStore, HashBits};
use crate::manifest::{
    Coverage, HistogramBucket, MatchFailureTaxonomy, MatchSummary, Report, StageTimings,
    Validation,
};
use crate::match_service::{MatchHit, MatchService};
use crate::method::EnrichmentMethod;
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::reader::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, RunOptions, RunStats};
use crate::staged::{Stage, WorkDir, stages_to_run};
use crate::writer::{FailureSink, PartWriter};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use crate::fanout::{FileError, input_files, make_pool, sorted_glob};
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

/// Scratch directory inside the output dir; excluded from the S3 upload.
pub const WORK_DIR: &str = ".work";
const EXTRACTIONS_DIR: &str = "extractions";
const INPUTS_FILE: &str = "inputs.jsonl";
const LOOKUPS_FILE: &str = "lookups.jsonl";
const LOOKUPS_FAILED_FILE: &str = "lookups.failed.jsonl";
const CHECKPOINT_FILE: &str = "lookups.checkpoint";
const HASH_BITS_FILE: &str = "hash.bits";
const EXTRACT_STATS_FILE: &str = "extract.stats.json";

/// Confidence-histogram bucket edges. Each adjacent pair is one bucket; the last
/// bucket includes the upper bound so a perfect `1.0` is counted.
const HISTOGRAM_EDGES: [f64; 6] = [0.0, 0.5, 0.7, 0.8, 0.9, 1.0];

/// One `inputs.jsonl` row, read back during query.
#[derive(Clone, Deserialize)]
struct InputRecord {
    hash: String,
    value: String,
}

/// One `lookups.jsonl` row: the input value and hash, with the method's `Lookup`
/// fields flattened alongside (so the row reads `{ value, hash, <lookup fields> }`).
#[derive(Serialize, Deserialize)]
struct LookupRow<L> {
    value: String,
    hash: String,
    #[serde(flatten)]
    lookup: L,
}

/// One `lookups.failed.jsonl` row.
#[derive(Serialize)]
struct FailedRow<'a> {
    value: &'a str,
    hash: &'a str,
    error: &'a str,
}

/// Extract-stage counters persisted to `extract.stats.json` so coverage survives a
/// resume that skips extract.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ExtractStats {
    files_processed: u64,
    files_failed: u64,
    records_scanned: u64,
    lines_malformed: u64,
}

/// Per-file extract result, reduced across the corpus.
#[derive(Default)]
struct ExtractAgg {
    dedup: DedupStore,
    records_scanned: u64,
    lines_malformed: u64,
}

impl ExtractAgg {
    fn merge(mut self, other: ExtractAgg) -> ExtractAgg {
        self.dedup.merge(other.dedup);
        self.records_scanned += other.records_scanned;
        self.lines_malformed += other.lines_malformed;
        self
    }
}

/// Run a lookup method through the staged pipeline and return its [`Report`].
///
/// `io` supplies the input and output directories (the output dir is the run dir);
/// the work area is always `<output>/.work`. `cfg` carries the match-service URL,
/// batch size, concurrency, timeout, and the resume flag. `svc` is shared across the
/// query stage's concurrent requests. `hash_bits` selects the dedup-hash width; it
/// is pinned in the run dir on the first run and a mismatched resume is refused.
/// `task` is the match-service task name (`"affiliation"` / `"funder"`). When
/// `only_stage` is set, exactly that stage runs (its predecessors must have already
/// completed); otherwise the runner resumes from the first incomplete stage. The
/// dedup-hash width is taken from `cfg.hash_bits`.
///
/// # Errors
///
/// Returns an error if the work area cannot be created, a hash-width mismatch is
/// detected on resume, a requested single stage's predecessors are missing, or any
/// stage fails (I/O, a hash collision, or the match service erroring a whole batch
/// after retries).
#[allow(clippy::too_many_arguments)]
pub fn run_staged<M>(
    method: &M,
    io: &RunOptions,
    cfg: &crate::LookupConfig,
    svc: &Arc<dyn MatchService>,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::JSONSchema>,
    task: &str,
    only_stage: Option<Stage>,
) -> Result<Report>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize + DeserializeOwned,
    M::Lookup: Serialize + DeserializeOwned + From<MatchHit> + Send + Sync + 'static,
{
    let work_path = io.output.join(WORK_DIR);
    fs::create_dir_all(&work_path)
        .with_context(|| format!("creating work dir {}", work_path.display()))?;
    let wd = WorkDir::new(&work_path);

    // Pin the hash width on the first run, or refuse a resume that asks for a
    // different one (a width mismatch silently breaks the hash join).
    pin_or_validate_hash_bits(&work_path, cfg.hash_bits, cfg.from_scratch)?;

    let stages = match only_stage {
        Some(stage) => {
            ensure_predecessors_done(&wd, stage)?;
            vec![stage]
        }
        None => stages_to_run(&work_path, cfg.from_scratch),
    };

    let mut timings = StageTimings::default();
    let mut emitted: u64 = 0;
    let run_start = Instant::now();

    for stage in stages {
        let started = Instant::now();
        match stage {
            Stage::Extract => {
                run_extract(method, io, &work_path, cfg.hash_bits)?;
                timings.extract = Some(elapsed_ms(started));
            }
            Stage::Query => {
                run_query::<M::Lookup>(svc.clone(), cfg, &work_path, task)?;
                timings.query = Some(elapsed_ms(started));
            }
            Stage::Reconcile => {
                emitted = run_reconcile(method, io, &work_path, template, validator)?;
                timings.reconcile = Some(elapsed_ms(started));
            }
        }
        fs::write(wd.marker_path(stage), b"")
            .with_context(|| format!("writing {} marker", stage.marker()))?;
    }

    timings.total = Some(elapsed_ms(run_start));
    build_report(&work_path, &wd, emitted, timings)
}

/// Pin the dedup-hash width in the run dir, or validate it against a resume.
fn pin_or_validate_hash_bits(work: &Path, hash_bits: HashBits, from_scratch: bool) -> Result<()> {
    let path = work.join(HASH_BITS_FILE);
    if path.exists() && !from_scratch {
        let pinned = fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
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

/// Require the stages before `stage` to have completed, for an explicit single-stage run.
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

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Extract
// ---------------------------------------------------------------------------

/// Scan the corpus, serialize one `Extraction` per line to `extractions/`, and
/// collect the unique lookup inputs into `inputs.jsonl`.
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
    let pool = make_pool(io.threads)?;
    let agg = pool.install(|| {
        files
            .par_iter()
            .enumerate()
            .map(|(idx, path)| match extract_one_file(idx, path, &extractions_dir, method) {
                Ok(agg) => Ok(agg),
                Err(FileError::Read(e)) => {
                    log::error!("file error {}: {e}", path.display());
                    files_failed.fetch_add(1, Ordering::Relaxed);
                    Ok(ExtractAgg::default())
                }
                Err(FileError::Fatal(e)) => Err(e),
            })
            .try_reduce(ExtractAgg::default, |a, b| Ok(a.merge(b)))
    })?;

    agg.dedup
        .write_jsonl(&work.join(INPUTS_FILE), hash_bits)
        .context("writing inputs.jsonl")?;

    let files_failed = files_failed.load(Ordering::Relaxed);
    let stats = ExtractStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: agg.records_scanned,
        lines_malformed: agg.lines_malformed,
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
    let mut records_scanned: u64 = 0;
    let mut lines_malformed: u64 = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(_) => {
                lines_malformed += 1;
                continue;
            }
        };
        let Ok(rec) = serde_json::from_str::<Value>(&line) else {
            lines_malformed += 1;
            continue;
        };
        records_scanned += 1;

        match method.extract(&rec) {
            crate::method::Extracted::Skip(_) => {}
            crate::method::Extracted::Items(items) => {
                for item in items {
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
    }

    part.flush()
        .with_context(|| format!("flushing {}", part_path.display()))
        .map_err(FileError::Fatal)?;
    Ok(ExtractAgg {
        dedup,
        records_scanned,
        lines_malformed,
    })
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

/// Resolve the unique inputs against the match service, writing `lookups.jsonl`,
/// `lookups.failed.jsonl`, and the checkpoint.
fn run_query<L>(
    svc: Arc<dyn MatchService>,
    cfg: &crate::LookupConfig,
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
    cfg: &crate::LookupConfig,
    work: &Path,
    task: &str,
) -> Result<()>
where
    L: Serialize + From<MatchHit> + Send + 'static,
{
    let inputs = read_inputs(&work.join(INPUTS_FILE))?;
    let checkpoint = Checkpoint::open(work.join(CHECKPOINT_FILE), cfg.from_scratch)?;

    // Resume appends to prior results; a fresh run truncates.
    let resume = !cfg.from_scratch;
    let matches_w = Arc::new(AsyncMutex::new(open_line_writer(
        &work.join(LOOKUPS_FILE),
        resume,
    )?));
    let failed_w = Arc::new(AsyncMutex::new(open_line_writer(
        &work.join(LOOKUPS_FAILED_FILE),
        resume,
    )?));

    let to_process: Vec<InputRecord> = inputs
        .into_iter()
        .filter(|r| !checkpoint.is_processed(&r.hash))
        .collect();

    if to_process.is_empty() {
        log::info!("query: nothing to process");
        matches_w.lock().await.flush()?;
        failed_w.lock().await.flush()?;
        checkpoint.save()?;
        return Ok(());
    }
    log::info!("query: {} inputs to resolve", to_process.len());

    let semaphore = Arc::new(Semaphore::new(cfg.ror_concurrency.max(1)));
    let checkpoint = Arc::new(AsyncMutex::new(checkpoint));
    let task = task.to_owned();

    let batches: Vec<Vec<InputRecord>> = to_process
        .chunks(cfg.ror_batch_size.max(1))
        .map(<[InputRecord]>::to_vec)
        .collect();

    let mut handles = Vec::with_capacity(batches.len());
    for batch in batches {
        let svc = Arc::clone(&svc);
        let matches_w = Arc::clone(&matches_w);
        let failed_w = Arc::clone(&failed_w);
        let checkpoint = Arc::clone(&checkpoint);
        let semaphore = Arc::clone(&semaphore);
        let task = task.clone();

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
                                error: "no match",
                            })?),
                        }
                    }
                    write_lines(&matches_w, &hits).await?;
                    write_lines(&failed_w, &misses).await?;
                }
                Err(e) => {
                    let error = format!("batch error: {e}");
                    let lines: Vec<String> = batch
                        .iter()
                        .map(|rec| {
                            serde_json::to_string(&FailedRow {
                                value: &rec.value,
                                hash: &rec.hash,
                                error: &error,
                            })
                        })
                        .collect::<Result<_, _>>()?;
                    write_lines(&failed_w, &lines).await?;
                }
            }

            let mut cp = checkpoint.lock().await;
            for rec in &batch {
                cp.mark_processed(&rec.hash);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await.context("query task panicked")??;
    }

    matches_w.lock().await.flush()?;
    failed_w.lock().await.flush()?;
    // Save once at the end: simplest cadence, and a crash only costs this run's
    // query work (a full rerun regenerates it). Per-batch saves are an O(N) rewrite.
    checkpoint.lock().await.save()?;
    Ok(())
}

fn read_inputs(path: &Path) -> Result<Vec<InputRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut rows = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        rows.push(serde_json::from_str(&line).context("parsing inputs.jsonl row")?);
    }
    Ok(rows)
}

/// Open a JSONL writer, appending when `resume` and the file already exists.
fn open_line_writer(path: &Path, resume: bool) -> Result<BufWriter<File>> {
    let file = if resume && path.exists() {
        fs::OpenOptions::new()
            .append(true)
            .open(path)
            .with_context(|| format!("opening {} for append", path.display()))?
    } else {
        File::create(path).with_context(|| format!("creating {}", path.display()))?
    };
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

/// Join matches back onto each extraction and emit enrichment records. Returns the
/// number of records written.
fn run_reconcile<M>(
    method: &M,
    io: &RunOptions,
    work: &Path,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::JSONSchema>,
) -> Result<u64>
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
    let failures = Mutex::new(FailureSink::create(&io.output.join(ENRICHMENTS_FAILED_FILE))?);

    let emitted = AtomicU64::new(0);
    let pool = make_pool(io.threads)?;
    pool.install(|| {
        parts.par_iter().enumerate().try_for_each(|(idx, path)| {
            let written = reconcile_one_part(
                idx, path, &enrich_dir, &lookups, method, template, validator, &failures,
                io.batch_size,
            )?;
            emitted.fetch_add(written, Ordering::Relaxed);
            Ok::<(), anyhow::Error>(())
        })
    })?;

    failures.lock().unwrap().flush()?;
    Ok(emitted.load(Ordering::Relaxed))
}

fn load_lookups<L: DeserializeOwned>(path: &Path) -> Result<crate::method::Lookups<L>> {
    let mut map = crate::method::Lookups::new();
    if !path.exists() {
        return Ok(map);
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: LookupRow<L> = serde_json::from_str(&line).context("parsing lookups.jsonl row")?;
        map.insert(row.hash, row.lookup);
    }
    Ok(map)
}

#[allow(clippy::too_many_arguments)]
fn reconcile_one_part<M>(
    idx: usize,
    path: &Path,
    enrich_dir: &Path,
    lookups: &crate::method::Lookups<M::Lookup>,
    method: &M,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::JSONSchema>,
    failures: &Mutex<FailureSink>,
    batch_size: usize,
) -> Result<u64>
where
    M: EnrichmentMethod,
    M::Extraction: DeserializeOwned,
{
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(file);

    let part_path = enrich_dir.join(format!("part_{idx:04}.jsonl.gz"));
    let mut part = PartWriter::create(&part_path, validator, failures)?;
    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);

    for line in reader.lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let extraction: M::Extraction =
            serde_json::from_str(&line).context("parsing extraction row")?;
        for parts in method.map_back(extraction, lookups) {
            batch.push(build_enrichment_record(
                template,
                &parts.doi,
                parts.action.as_str(),
                parts.field,
                parts.original,
                parts.enriched,
            ));
            if batch.len() >= batch_size {
                part.write_batch(&batch)?;
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        part.write_batch(&batch)?;
    }
    part.finish()
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Assemble the [`Report`] from the on-disk artifacts plus the reconcile count.
fn build_report(work: &Path, wd: &WorkDir, emitted: u64, timings: StageTimings) -> Result<Report> {
    let stats: ExtractStats = read_extract_stats(&work.join(EXTRACT_STATS_FILE))?;

    let counters = RunStats {
        files_processed: stats.files_processed,
        files_failed: stats.files_failed,
        records_scanned: stats.records_scanned,
        lines_malformed: stats.lines_malformed,
        emitted,
        schema_failures: 0,
        skipped: std::collections::BTreeMap::new(),
    };

    let match_ = if wd.is_complete(Stage::Query) {
        Some(build_match_summary(work)?)
    } else {
        None
    };

    Ok(Report {
        counters,
        // Lookup methods have no out-of-scope skips, so everything scanned is in scope.
        coverage: Coverage::new(stats.records_scanned, emitted),
        match_,
        validation: Validation::new(emitted, 0),
        stage_timings_ms: timings,
    })
}

fn read_extract_stats(path: &Path) -> Result<ExtractStats> {
    if !path.exists() {
        return Ok(ExtractStats::default());
    }
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&body).context("parsing extract.stats.json")
}

/// One `lookups.jsonl` row, read back only for its confidence (other fields ignored).
#[derive(Deserialize)]
struct ConfidenceRow {
    #[serde(default)]
    confidence: Option<f64>,
}

/// One `lookups.failed.jsonl` row, read back only for its error (other fields ignored).
#[derive(Deserialize)]
struct ErrorRow {
    error: String,
}

/// Compute the match-quality block from `inputs.jsonl`, `lookups.jsonl`, and
/// `lookups.failed.jsonl`.
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
    for_each_jsonl(&work.join(LOOKUPS_FAILED_FILE), |row: ErrorRow| {
        classify_failure(&row.error, &mut taxonomy);
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

/// Bin a failure error string into the report taxonomy.
fn classify_failure(error: &str, taxonomy: &mut MatchFailureTaxonomy) {
    let lower = error.to_ascii_lowercase();
    if lower.contains("no match") {
        taxonomy.no_match += 1;
    } else if lower.contains("timeout") || lower.contains("timed out") {
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

/// Deserialize each non-empty JSONL row in `path` into `T` and pass it to `f` (a
/// no-op if the file is absent). `T` should name only the fields it needs; serde
/// skips the rest without materializing them.
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
    use crate::match_service::{FakeMatchService, RorLookup};
    use crate::method::{EnrichmentAction, EnrichmentParts, Extracted, Lookups};
    use crate::provenance::EnrichmentTemplate;
    use std::path::PathBuf;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;
    use std::collections::HashMap;

    /// A funder-shaped test method: one extraction per record carrying the funder
    /// name and its hash; `map_back` enriches matched names.
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

    fn write_gz(path: &Path, records: &[Value]) {
        let file = File::create(path).unwrap();
        let mut gz = GzEncoder::new(file, Compression::default());
        for rec in records {
            gz.write_all(serde_json::to_string(rec).unwrap().as_bytes())
                .unwrap();
            gz.write_all(b"\n").unwrap();
        }
        gz.finish().unwrap();
    }

    fn template() -> EnrichmentTemplate {
        EnrichmentTemplate {
            contributors: json!([]),
            resources: json!([]),
        }
    }

    fn fake_service() -> Arc<dyn MatchService> {
        let mut map = HashMap::new();
        map.insert("MIT".to_owned(), ("https://ror.org/042nb2s44".to_owned(), 0.99));
        map.insert("NSF".to_owned(), ("https://ror.org/021nxhr62".to_owned(), 0.95));
        Arc::new(FakeMatchService::new(map))
    }

    /// Four records: two match, one has no match, one has no funder name (skipped).
    fn sample_records() -> Vec<Value> {
        vec![
            json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } }),
            json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } }),
            json!({ "id": "10.1/unknown", "attributes": { "name": "Unknown University" } }),
            json!({ "id": "10.1/empty", "attributes": {} }),
        ]
    }

    fn cfg(hash_bits: HashBits, from_scratch: bool) -> crate::LookupConfig {
        crate::LookupConfig {
            ror_service_url: "http://unused".to_owned(),
            ror_file: PathBuf::from("unused"),
            ror_batch_size: 2,
            ror_concurrency: 2,
            ror_timeout: 30,
            hash_bits,
            from_scratch,
        }
    }

    /// Lay out an input dir with one gz part and return (input, output) temp roots.
    fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input");
        let output = dir.path().join("output");
        fs::create_dir_all(input.join("updated_2024-01")).unwrap();
        fs::create_dir_all(&output).unwrap();
        write_gz(&input.join("updated_2024-01/part_0000.jsonl.gz"), &sample_records());
        (dir, input, output)
    }

    fn run_opts(input: &Path, output: &Path) -> RunOptions {
        RunOptions {
            input: input.to_path_buf(),
            output: output.to_path_buf(),
            threads: 1,
            batch_size: 100,
        }
    }

    #[test]
    fn full_pipeline_produces_contract_and_match_block() {
        let (_dir, input, output) = fixture();
        let method = TestMethod { hash_bits: HashBits::Bits64 };
        let svc = fake_service();
        let tmpl = template();

        let report = run_staged(
            &method,
            &run_opts(&input, &output),
            &cfg(HashBits::Bits64, true),
            &svc,
            &tmpl,
            None,
            "funder",
            None,
        )
        .unwrap();

        // Match block is filled from the on-disk artifacts.
        let m = report.match_.expect("match block present");
        assert_eq!(m.unique_inputs, 3);
        assert_eq!(m.matched, 2);
        assert_eq!(m.failure_taxonomy.no_match, 1);
        assert_eq!(m.failure_taxonomy.error, 0);
        let top_bucket = m.confidence_histogram.last().unwrap();
        assert!((top_bucket.max - 1.0).abs() < f64::EPSILON);
        assert_eq!(top_bucket.count, 2);

        // Coverage: four scanned, two enriched.
        assert_eq!(report.counters.records_scanned, 4);
        assert_eq!(report.counters.emitted, 2);
        assert_eq!(report.coverage.records_in_scope, 4);
        assert_eq!(report.coverage.records_enriched, 2);

        // All stage timings present.
        assert!(report.stage_timings_ms.extract.is_some());
        assert!(report.stage_timings_ms.query.is_some());
        assert!(report.stage_timings_ms.reconcile.is_some());

        // The on-disk contract exists.
        let work = output.join(WORK_DIR);
        for f in [
            "extractions/part_0000.jsonl",
            INPUTS_FILE,
            LOOKUPS_FILE,
            LOOKUPS_FAILED_FILE,
            CHECKPOINT_FILE,
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

        // The output holds the two enriched records.
        let dois = read_output_dois(&output);
        assert_eq!(dois.len(), 2);
        assert!(dois.contains(&"10.1/mit".to_owned()));
        assert!(dois.contains(&"10.1/nsf".to_owned()));
    }

    fn read_output_dois(output: &Path) -> Vec<String> {
        let mut dois = Vec::new();
        let parts = sorted_glob(&format!(
            "{}/part_*.jsonl.gz",
            output.join(ENRICHMENTS_DIR).to_string_lossy()
        ))
        .unwrap();
        for part in parts {
            let f = File::open(&part).unwrap();
            for line in BufReader::new(flate2::read::GzDecoder::new(f)).lines() {
                let line = line.unwrap();
                if line.trim().is_empty() {
                    continue;
                }
                let rec: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(rec["field"], "fundingReferences");
                assert_eq!(rec["action"], "updateChild");
                dois.push(rec["doi"].as_str().unwrap().to_owned());
            }
        }
        dois
    }

    #[test]
    fn only_stage_extract_runs_just_extract() {
        let (_dir, input, output) = fixture();
        let method = TestMethod { hash_bits: HashBits::Bits64 };
        let svc = fake_service();
        let tmpl = template();

        run_staged(
            &method,
            &run_opts(&input, &output),
            &cfg(HashBits::Bits64, true),
            &svc,
            &tmpl,
            None,
            "funder",
            Some(Stage::Extract),
        )
        .unwrap();

        let work = output.join(WORK_DIR);
        assert!(work.join("extract.done").exists());
        assert!(work.join(INPUTS_FILE).exists());
        assert!(!work.join("query.done").exists());
        assert!(!work.join("reconcile.done").exists());
        assert!(!output.join(ENRICHMENTS_DIR).exists());
    }

    #[test]
    fn only_stage_query_without_extract_errors() {
        let (_dir, input, output) = fixture();
        let method = TestMethod { hash_bits: HashBits::Bits64 };
        let svc = fake_service();
        let tmpl = template();

        let err = run_staged(
            &method,
            &run_opts(&input, &output),
            &cfg(HashBits::Bits64, true),
            &svc,
            &tmpl,
            None,
            "funder",
            Some(Stage::Query),
        )
        .unwrap_err();
        assert!(err.to_string().contains("extract"), "got: {err}");
    }

    #[test]
    fn resume_after_deleting_reconcile_marker_reproduces_output() {
        let (_dir, input, output) = fixture();
        let method = TestMethod { hash_bits: HashBits::Bits64 };
        let svc = fake_service();
        let tmpl = template();
        let opts = run_opts(&input, &output);

        run_staged(&method, &opts, &cfg(HashBits::Bits64, true), &svc, &tmpl, None, "funder", None)
            .unwrap();

        // Drop the reconcile marker and resume: only reconcile should rerun.
        fs::remove_file(output.join(WORK_DIR).join("reconcile.done")).unwrap();
        let report = run_staged(
            &method,
            &opts,
            &cfg(HashBits::Bits64, false),
            &svc,
            &tmpl,
            None,
            "funder",
            None,
        )
        .unwrap();

        assert_eq!(report.counters.emitted, 2);
        assert!(report.stage_timings_ms.reconcile.is_some());
        // Extract was skipped this run, so its timing is absent.
        assert!(report.stage_timings_ms.extract.is_none());
        assert_eq!(read_output_dois(&output).len(), 2);
    }

    #[test]
    fn resume_with_mismatched_hash_width_errors() {
        let (_dir, input, output) = fixture();
        let method = TestMethod { hash_bits: HashBits::Bits64 };
        let svc = fake_service();
        let tmpl = template();
        let opts = run_opts(&input, &output);

        run_staged(&method, &opts, &cfg(HashBits::Bits64, true), &svc, &tmpl, None, "funder", None)
            .unwrap();

        let err = run_staged(
            &method,
            &opts,
            &cfg(HashBits::Bits128, false),
            &svc,
            &tmpl,
            None,
            "funder",
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("hash-width mismatch"), "got: {err}");
    }
}
