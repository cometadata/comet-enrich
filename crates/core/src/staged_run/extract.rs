use super::fingerprint::{compute_input_fingerprint, write_input_fingerprint};
use super::{EXTRACT_STATS_FILE, EXTRACTIONS_DIR, INPUTS_FILE};
use crate::artifact_lifecycle as lifecycle;
use crate::dedup::{DedupStore, HashBits};
use crate::fanout::{
    FileError, input_files, make_pool, own_skips, progress_bar, scan_jsonl_records,
};
use crate::method::EnrichmentMethod;
use crate::options::RunOptions;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Extract-stage counters persisted for resumed runs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct ExtractStats {
    pub(super) files_processed: u64,
    pub(super) files_failed: u64,
    pub(super) records_scanned: u64,
    pub(super) lines_malformed: u64,
    pub(super) in_scope_units: u64,
    pub(super) skipped: BTreeMap<String, u64>,
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
/// Write extractions and unique lookup inputs.
pub(super) fn run_extract<M>(
    method: &M,
    io: &RunOptions,
    work: &Path,
    hash_bits: HashBits,
) -> Result<()>
where
    M: EnrichmentMethod,
    M::Extraction: Serialize,
{
    let files = input_files(&io.input)?;
    log::info!("extract: {} input files", files.len());
    let fingerprint = compute_input_fingerprint(&io.input, &files)?;

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
                let agg = match stream_extract_file(idx, path, &extractions_dir, method) {
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
    write_input_fingerprint(work, &fingerprint)?;
    log::info!(
        "extract: {} records scanned, {} unique inputs",
        stats.records_scanned,
        agg.dedup.len()
    );
    Ok(())
}

/// Stream one corpus file through the method, writing its extractions part.
fn stream_extract_file<M>(
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

    let scanned = scan_jsonl_records(reader, |rec| {
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
    });
    let tally = match scanned {
        Ok(tally) => tally,
        Err(e) => {
            // Remove the partial extraction part before counting this file as failed.
            drop(part);
            lifecycle::remove_file_if_exists(&part_path).map_err(FileError::Fatal)?;
            return Err(e);
        }
    };

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
