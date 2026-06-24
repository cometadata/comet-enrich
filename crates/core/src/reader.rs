//! Reads DataCite JSONL input files and runs enrichment methods over them.
//!
//! [`run`] finds `*.jsonl.gz` files under the input directory, processes them in
//! parallel, and sends emitted enrichment records to a single writer. Records are
//! validated at the write boundary when a schema validator is provided.

use crate::method::{EnrichmentMethod, Extracted, Lookups};
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::JsonlWriter;

use anyhow::Result;
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

/// Options for one enrichment run.
pub struct RunOptions {
    /// Directory searched recursively for `*.jsonl.gz` files.
    pub input: PathBuf,
    /// Output JSONL file.
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
    pub skipped: BTreeMap<&'static str, u64>,
}

#[derive(Default)]
struct Counters {
    records_scanned: AtomicU64,
    lines_malformed: AtomicU64,
    emitted: AtomicU64,
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
/// Returns an error if input files cannot be discovered, the output file cannot
/// be created, the progress bar template is invalid, or the rayon pool cannot be
/// built. Individual file read failures are counted in [`RunStats::files_failed`]
/// and do not stop the run.
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

    let writer = Arc::new(Mutex::new(JsonlWriter::create(&opts.output, validator)?));

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
        std::thread::spawn(move || {
            let mut batches = 0u32;
            while let Ok(batch) = rx.recv() {
                let mut w = writer.lock().unwrap();
                if let Err(e) = w.write_batch(&batch) {
                    log::error!("write error: {e}");
                    continue;
                }
                batches += 1;
                if batches % 100 == 0 {
                    if let Err(e) = w.flush() {
                        log::error!("flush error: {e}");
                    }
                }
            }
            if let Err(e) = writer.lock().unwrap().flush() {
                log::error!("final flush error: {e}");
            }
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
    writer_t.join().expect("writer thread panicked");
    pb.finish_with_message("done");

    let files_failed = counters.files_failed.load(Ordering::Relaxed);
    Ok(RunStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: counters.records_scanned.load(Ordering::Relaxed),
        lines_malformed: counters.lines_malformed.load(Ordering::Relaxed),
        emitted: counters.emitted.load(Ordering::Relaxed),
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
                        counters.emitted.fetch_add(1, Ordering::Relaxed);
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
