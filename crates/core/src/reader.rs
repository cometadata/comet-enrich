//! Reads DataCite JSONL input files and runs enrichment methods over them.
//!
//! [`run`] finds `*.jsonl.gz` files under the input directory and processes them
//! in parallel. Each worker writes the records it emits to its own gzip output
//! part, so there is no shared output writer. Records are validated at the write
//! boundary when a schema validator is provided.

use crate::method::{EnrichmentMethod, Extracted, Lookups};
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::{FailureSink, PartWriter};

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Directory holding the gzip enrichment output parts, written inside the output
/// directory as `enrichments/part_NNNN.jsonl.gz` (one part per input file).
pub const ENRICHMENTS_DIR: &str = "enrichments";
/// File name for records diverted after failing schema validation.
pub const ENRICHMENTS_FAILED_FILE: &str = "enrichments.failed.jsonl";

/// Options for one enrichment run.
pub struct RunOptions {
    /// Directory searched recursively for `*.jsonl.gz` files.
    pub input: PathBuf,
    /// Output directory. The run writes gzip parts under `enrichments/`, plus
    /// `enrichments.failed.jsonl` when records fail validation.
    pub output: PathBuf,
    /// Worker threads. Use `0` for all available CPUs.
    pub threads: usize,
    /// Emitted records per writer batch.
    pub batch_size: usize,
}

/// Counters returned after an enrichment run.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RunStats {
    pub files_processed: u64,
    pub files_failed: u64,
    pub records_scanned: u64,
    pub lines_malformed: u64,
    pub emitted: u64,
    pub schema_failures: u64,
    pub skipped: BTreeMap<&'static str, u64>,
}

#[derive(Default)]
struct Counters {
    records_scanned: AtomicU64,
    lines_malformed: AtomicU64,
    files_failed: AtomicU64,
    emitted: AtomicU64,
}

/// Classifies a per-file failure so the runner can react appropriately.
enum FileError {
    /// The input file could not be read. Counted in [`RunStats::files_failed`];
    /// the run continues with the other files.
    Read(anyhow::Error),
    /// A record could not be written, diverted, or flushed. The output would be
    /// incomplete, so this aborts the whole run.
    Fatal(anyhow::Error),
}

/// Run an enrichment method over all input files.
///
/// The provenance template is supplied by the caller and cloned into each emitted
/// record. If `validator` is set, each record is validated immediately before it
/// is written.
///
/// # Errors
///
/// Returns an error if input files cannot be discovered, the output directory or
/// files cannot be created, the progress bar template is invalid, or the rayon
/// pool cannot be built. A write or flush failure also aborts the run, since the
/// output would be incomplete. Individual file read failures are counted in
/// [`RunStats::files_failed`] and do not stop the run.
pub fn run<M: EnrichmentMethod>(
    method: &M,
    opts: &RunOptions,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::JSONSchema>,
) -> Result<RunStats> {
    let n_threads = if opts.threads == 0 {
        num_cpus::get()
    } else {
        opts.threads
    };
    log::info!("using {n_threads} threads");

    let pattern = format!(
        "{}/**/*.jsonl.gz",
        opts.input.to_string_lossy().trim_end_matches('/')
    );
    let mut files: Vec<PathBuf> = glob(&pattern)?.filter_map(Result::ok).collect();
    // Sort so each input file's index, and therefore its output part name, is
    // stable across runs for a fixed input set.
    files.sort();
    log::info!("found {} input files", files.len());

    let enrich_dir = opts.output.join(ENRICHMENTS_DIR);
    std::fs::create_dir_all(&enrich_dir)
        .with_context(|| format!("creating output dir {}", enrich_dir.display()))?;
    let failed_path = opts.output.join(ENRICHMENTS_FAILED_FILE);
    // One shared failures sink: validation failures are rare, so the mutex is
    // uncontended in practice. Creating it clears any stale failures file.
    let failures = Mutex::new(FailureSink::create(&failed_path)?);

    if files.is_empty() {
        return Ok(RunStats::default());
    }

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
            .progress_chars("#>-"),
    );

    let counters = Counters::default();
    let skipped: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()?;
    // Each worker writes its own gzip part, so there is no shared writer. A read
    // failure is counted and the run continues; a write/flush failure is fatal, so
    // `try_for_each` short-circuits and the error propagates out of the run.
    pool.install(|| {
        files
            .par_iter()
            .enumerate()
            .try_for_each(|(idx, path)| -> Result<()> {
                pb.set_message(format!(
                    "processing {}",
                    path.file_name().unwrap().to_string_lossy()
                ));
                match process_file(
                    idx,
                    path,
                    &enrich_dir,
                    validator,
                    &failures,
                    opts.batch_size,
                    method,
                    template,
                    &counters,
                    &skipped,
                ) {
                    Ok(()) => {}
                    Err(FileError::Read(e)) => {
                        log::error!("file error {}: {e}", path.display());
                        counters.files_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(FileError::Fatal(e)) => return Err(e),
                }
                pb.inc(1);
                Ok(())
            })
    })?;

    // A failures-file flush error means the output is incomplete, so it aborts the run.
    failures.lock().unwrap().flush()?;
    pb.finish_with_message("done");

    let files_failed = counters.files_failed.load(Ordering::Relaxed);
    Ok(RunStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: counters.records_scanned.load(Ordering::Relaxed),
        lines_malformed: counters.lines_malformed.load(Ordering::Relaxed),
        emitted: counters.emitted.load(Ordering::Relaxed),
        schema_failures: failures.lock().unwrap().records_failed,
        skipped: skipped.into_inner().unwrap(),
    })
}

#[allow(clippy::too_many_arguments)]
fn process_file<M: EnrichmentMethod>(
    file_index: usize,
    path: &Path,
    enrich_dir: &Path,
    validator: Option<&jsonschema::JSONSchema>,
    failures: &Mutex<FailureSink>,
    batch_size: usize,
    method: &M,
    template: &EnrichmentTemplate,
    counters: &Counters,
    skipped: &Mutex<BTreeMap<&'static str, u64>>,
) -> Result<(), FileError> {
    let f = File::open(path).map_err(|e| FileError::Read(e.into()))?;
    let reader = BufReader::new(GzDecoder::new(f));

    // One output part per input file, named by the input's index in the sorted glob.
    let part_path = enrich_dir.join(format!("part_{file_index:04}.jsonl.gz"));
    let mut part = PartWriter::create(&part_path, validator, failures).map_err(FileError::Fatal)?;

    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);
    let mut local_skips: BTreeMap<&'static str, u64> = BTreeMap::new();

    // This runner handles the transform path, so there are no external lookups.
    let lookups: Lookups<M::Lookup> = HashMap::new();

    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(_) => {
                counters.lines_malformed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let Ok(rec) = serde_json::from_str::<Value>(&line) else {
            counters.lines_malformed.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        counters.records_scanned.fetch_add(1, Ordering::Relaxed);

        match method.extract(&rec) {
            Extracted::Skip(reason) => {
                *local_skips.entry(reason).or_default() += 1;
            }
            Extracted::Items(items) => {
                for item in items {
                    for parts in method.map_back(item, &lookups) {
                        batch.push(build_enrichment_record(
                            template,
                            &parts.doi,
                            parts.action.as_str(),
                            method.field(),
                            parts.original,
                            parts.enriched,
                        ));
                        if batch.len() >= batch_size {
                            part.write_batch(&batch).map_err(FileError::Fatal)?;
                            batch.clear();
                        }
                    }
                }
            }
        }
    }

    if !batch.is_empty() {
        part.write_batch(&batch).map_err(FileError::Fatal)?;
    }
    let written = part.finish().map_err(FileError::Fatal)?;
    counters.emitted.fetch_add(written, Ordering::Relaxed);
    merge_skips(skipped, local_skips);
    Ok(())
}

fn merge_skips(shared: &Mutex<BTreeMap<&'static str, u64>>, local: BTreeMap<&'static str, u64>) {
    if local.is_empty() {
        return;
    }
    let mut g = shared.lock().unwrap();
    for (k, v) in local {
        *g.entry(k).or_default() += v;
    }
}
