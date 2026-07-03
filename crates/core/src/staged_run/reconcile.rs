use super::query::LookupRow;
use super::{EXTRACTIONS_DIR, LOOKUPS_FILE, RECONCILE_STATS_FILE, for_each_jsonl};
use crate::fanout::{make_pool, progress_bar};
use crate::method::EnrichmentMethod;
use crate::options::RunOptions;
use crate::provenance::{EnrichmentTemplate, build_enrichment_record};
use crate::writer::{
    ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, FailureSink, ParallelRollingWriter, RecordBatcher,
};

use anyhow::{Context, Result};
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Reconcile-stage counters persisted for resumed runs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct ReconcileStats {
    pub(super) emitted: u64,
    pub(super) schema_failures: u64,
}
/// Join lookups onto extractions and write enrichment records.
pub(super) fn run_reconcile<M>(
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

    let parts = extraction_part_files(&work.join(EXTRACTIONS_DIR))?;

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

pub(super) fn extraction_part_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("reading file type for {}", entry.path().display()))?;
        if file_type.is_file() && is_extraction_part_name(&entry.file_name()) {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn is_extraction_part_name(name: &OsStr) -> bool {
    name.to_str().is_some_and(|name| {
        name.starts_with("part_")
            && Path::new(name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonl"))
    })
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
