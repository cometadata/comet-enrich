//! The single-pass transform run path.
//!
//! [`run`] finds `*.jsonl.gz` files under the input directory and processes them in
//! parallel: each record is extracted and mapped straight to enrichment records,
//! with no lookup step (contrast [`crate::staged_run::run_staged`]). Final
//! enrichment records are routed to a bounded set of rolling gzip writer lanes and
//! validated at the write boundary when a schema validator is provided.

use crate::artifact_lifecycle as lifecycle;
use crate::fanout::{
    FileError, input_files, make_pool, own_skips, progress_bar, scan_jsonl_records,
};
use crate::method::{EnrichmentMethod, Extracted, Lookups};
use crate::options::{RunOptions, RunStats};
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::{
    ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, FailureSink, ParallelRollingWriter, RecordBatcher,
};

use anyhow::Result;
use flate2::read::GzDecoder;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

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
/// Returns an error if input files cannot be discovered (including when none are
/// found), the output directory or files cannot be created, the progress bar
/// template is invalid, or the rayon pool cannot be built. A write or flush failure also aborts the run, since the
/// output would be incomplete. Individual file read failures are counted in
/// [`RunStats::files_failed`] and do not stop the run.
pub fn run<M: EnrichmentMethod>(
    method: &M,
    opts: &RunOptions,
    template: &EnrichmentTemplate,
    validator: Option<&jsonschema::Validator>,
) -> Result<RunStats> {
    let files = input_files(&opts.input)?;
    log::info!("found {} input files", files.len());

    let enrich_dir = opts.output.join(ENRICHMENTS_DIR);
    lifecycle::clear_run_outputs(&opts.output)?;
    let failed_path = opts.output.join(ENRICHMENTS_FAILED_FILE);
    // Shared sink for schema-validation failures.
    let failures = Mutex::new(FailureSink::create(&failed_path));

    let writer = ParallelRollingWriter::create(
        &enrich_dir,
        validator,
        &failures,
        opts.output_part_size_bytes,
        opts.output_writer_lanes,
    )?;

    let pb = progress_bar(files.len() as u64)?;

    let counters = Counters::default();
    let skipped: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

    let pool = make_pool(opts.threads)?;
    // Workers scan input files in parallel and send emitted records to the rolling
    // output writer. A read failure is counted and the run continues; a write/flush
    // failure is fatal, so `try_for_each` short-circuits and the error propagates.
    pool.install(|| {
        files.par_iter().try_for_each(|path| -> Result<()> {
            pb.set_message(format!(
                "processing {}",
                path.file_name().unwrap().to_string_lossy()
            ));
            match process_file(
                path,
                &writer,
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

    let emitted = writer.finish()?;
    // A failures-file flush error means the output is incomplete, so it aborts the run.
    failures.lock().unwrap().flush()?;
    pb.finish_with_message("done");

    let files_failed = counters.files_failed.load(Ordering::Relaxed);
    Ok(RunStats {
        files_processed: files.len() as u64 - files_failed,
        files_failed,
        records_scanned: counters.records_scanned.load(Ordering::Relaxed),
        lines_malformed: counters.lines_malformed.load(Ordering::Relaxed),
        emitted,
        schema_failures: failures.lock().unwrap().records_failed,
        skipped: own_skips(skipped.into_inner().unwrap()),
    })
}

fn process_file<M: EnrichmentMethod>(
    path: &Path,
    writer: &ParallelRollingWriter<'_>,
    batch_size: usize,
    method: &M,
    template: &EnrichmentTemplate,
    counters: &Counters,
    skipped: &Mutex<BTreeMap<&'static str, u64>>,
) -> Result<(), FileError> {
    let f = File::open(path).map_err(|e| FileError::Read(e.into()))?;
    let reader = BufReader::new(GzDecoder::new(f));

    let mut local_skips: BTreeMap<&'static str, u64> = BTreeMap::new();

    // This runner handles the transform path, so there are no external lookups.
    let lookups: Lookups<M::Lookup> = HashMap::new();
    let mut batcher = RecordBatcher::new(writer, batch_size);

    let tally = scan_jsonl_records(reader, |rec| {
        match method.extract(rec) {
            Extracted::Skip(reason) => {
                *local_skips.entry(reason).or_default() += 1;
            }
            Extracted::Items(items) => {
                for item in items {
                    for parts in method.map_back(item, &lookups) {
                        batcher
                            .push(build_enrichment_record(template, parts))
                            .map_err(FileError::Fatal)?;
                    }
                }
            }
        }
        Ok(())
    })?;

    batcher.finish().map_err(FileError::Fatal)?;
    counters
        .records_scanned
        .fetch_add(tally.scanned, Ordering::Relaxed);
    counters
        .lines_malformed
        .fetch_add(tally.malformed, Ordering::Relaxed);
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
