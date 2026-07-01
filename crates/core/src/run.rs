//! Shared run options and counters.

use std::collections::BTreeMap;
use std::path::PathBuf;

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
    pub skipped: BTreeMap<String, u64>,
}
