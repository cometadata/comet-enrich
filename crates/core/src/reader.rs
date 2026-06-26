//! Reads DataCite JSONL input files and runs enrichment methods over them.
//!
//! [`run`] finds `*.jsonl.gz` files under the input directory, processes them in
//! parallel, and sends emitted enrichment records to a single writer. Records are
//! validated at the write boundary when a schema validator is provided.

use crate::method::{EnrichmentMethod, Extracted, Lookups};
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::JsonlWriter;

use anyhow::{Context, Result};
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// File name for the main enrichment output, written inside the output directory.
pub const ENRICHMENTS_FILE: &str = "enrichments.jsonl";
/// File name for records diverted after failing schema validation.
pub const ENRICHMENTS_FAILED_FILE: &str = "enrichments.failed.jsonl";

/// Options for one enrichment run.
pub struct RunOptions {
    /// Directory searched recursively for `*.jsonl.gz` files.
    pub input: PathBuf,
    /// Output directory. The run writes `enrichments.jsonl` here, plus
    /// `enrichments.failed.jsonl` when records fail validation.
    pub output: PathBuf,
    /// Worker threads. Use `0` for all available CPUs.
    pub threads: usize,
    /// Emitted records per writer batch.
    pub batch_size: usize,
}

/// Counters returned after an enrichment run.
#[derive(Debug, Default)]
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
    validator: Option<jsonschema::JSONSchema>,
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
    let files: Vec<PathBuf> = glob(&pattern)?.filter_map(Result::ok).collect();
    log::info!("found {} input files", files.len());

    std::fs::create_dir_all(&opts.output)
        .with_context(|| format!("creating output dir {}", opts.output.display()))?;
    let out_path = opts.output.join(ENRICHMENTS_FILE);
    let failed_path = opts.output.join(ENRICHMENTS_FAILED_FILE);
    let writer = Arc::new(Mutex::new(JsonlWriter::create(
        &out_path,
        &failed_path,
        validator,
    )?));

    if files.is_empty() {
        return Ok(RunStats::default());
    }

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
            .progress_chars("#>-"),
    );

    let (tx, rx) = bounded::<Vec<Value>>(n_threads * 4);
    let writer_t = {
        let writer = Arc::clone(&writer);
        // A write or flush failure means the output is incomplete, so bail out: returning
        // drops `rx`, which disconnects the channel and stops the workers sending.
        std::thread::spawn(move || -> Result<()> {
            let mut batches = 0u32;
            while let Ok(batch) = rx.recv() {
                let mut w = writer.lock().unwrap();
                w.write_batch(&batch)?;
                batches += 1;
                if batches % 100 == 0 {
                    w.flush()?;
                }
            }
            writer.lock().unwrap().flush()
        })
    };

    let counters = Counters::default();
    let skipped: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()?;
    pool.install(|| {
        files.par_iter().for_each_with(tx.clone(), |tx_w, path| {
            pb.set_message(format!(
                "processing {}",
                path.file_name().unwrap().to_string_lossy()
            ));
            if let Err(e) = process_file(
                path,
                tx_w,
                opts.batch_size,
                method,
                template,
                &counters,
                &skipped,
            ) {
                log::error!("file error {}: {e}", path.display());
                counters.files_failed.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
        });
    });

    drop(tx);
    let writer_result = writer_t.join().expect("writer thread panicked");
    writer_result?;
    pb.finish_with_message("done");

    let files_failed = counters.files_failed.load(Ordering::Relaxed);
    let (emitted, schema_failures) = {
        let w = writer.lock().unwrap();
        (w.records_written, w.records_failed)
    };
    Ok(RunStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: counters.records_scanned.load(Ordering::Relaxed),
        lines_malformed: counters.lines_malformed.load(Ordering::Relaxed),
        emitted,
        schema_failures,
        skipped: skipped.into_inner().unwrap(),
    })
}

fn process_file<M: EnrichmentMethod>(
    path: &Path,
    tx: &crossbeam_channel::Sender<Vec<Value>>,
    batch_size: usize,
    method: &M,
    template: &EnrichmentTemplate,
    counters: &Counters,
    skipped: &Mutex<BTreeMap<&'static str, u64>>,
) -> Result<()> {
    let f = File::open(path)?;
    let reader = BufReader::new(GzDecoder::new(f));
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
                        if batch.len() >= batch_size && tx.send(std::mem::take(&mut batch)).is_err()
                        {
                            merge_skips(skipped, local_skips);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    if !batch.is_empty() {
        let _ = tx.send(batch);
    }
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
