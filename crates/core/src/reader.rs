//! Reads the `*.jsonl.gz` shards in parallel and runs a method over every record.
//!
//! [`run`] globs the shards and processes them across a rayon pool. Each worker parses
//! records and sends batches of emitted enrichment records through a bounded channel to a
//! single writer thread, which validates each record before writing. Counts land in
//! [`RunStats`]; the method supplies only the per-record logic.

use crate::method::{EnrichmentMethod, Extracted, Lookups};
use crate::provenance::{self, EnrichmentTemplate, build_enrichment_record};
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

/// Inputs to a single enrichment run.
pub struct RunOptions {
    /// Directory globbed for `**/*.jsonl.gz` data shards.
    pub input: PathBuf,
    /// Output JSONL file.
    pub output: PathBuf,
    /// Provenance YAML, loaded into the [`EnrichmentTemplate`].
    pub enrichment: PathBuf,
    /// Worker threads; `0` means `num_cpus`.
    pub threads: usize,
    /// Emitted records per batch handed to the writer.
    pub batch_size: usize,
}

/// Aggregated counters for a run. `skipped` is the per-reason histogram that replaces the
/// method-specific stats fields of the original tool.
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

/// Run `method` over every shard under `opts.input`, writing enrichment records to
/// `opts.output`. Every emitted record is validated at the write boundary iff `validator`
/// is `Some`.
///
/// # Errors
/// Returns an error if the provenance config fails to load, the output file cannot be
/// created, or the rayon pool cannot be built. Per-file read failures are counted in
/// [`RunStats::files_failed`] rather than aborting the run.
pub fn run<M: EnrichmentMethod>(
    method: &M,
    opts: &RunOptions,
    validator: Option<jsonschema::JSONSchema>,
) -> Result<RunStats> {
    let cfg = provenance::load_enrichment(&opts.enrichment)?;
    let template = EnrichmentTemplate::from_config(&cfg);

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
                &template,
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

    // The transform path performs no lookups; this stays empty until the dedup store and
    // resumable HTTP client that back `EnrichmentMethod::lookup` are wired in.
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
