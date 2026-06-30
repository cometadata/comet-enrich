//! Shared helpers for the parallel file fan-out used by both runners.
//!
//! The transform path ([`crate::reader::run`]) and the staged path
//! ([`crate::staged_run::run_staged`]) both discover `*.jsonl.gz` inputs, build a
//! rayon pool, and process each file with the same read-vs-fatal error policy.
//! These helpers keep that shared scaffolding in one place.

use anyhow::{Context, Result};
use glob::glob;
use std::path::{Path, PathBuf};

/// Classifies a per-file failure so a runner can react appropriately.
pub(crate) enum FileError {
    /// The input file could not be read. Counted as a failed file; the run
    /// continues with the other files.
    Read(anyhow::Error),
    /// A record could not be written, diverted, or flushed. The output would be
    /// incomplete, so this aborts the whole run.
    Fatal(anyhow::Error),
}

/// Discover the input `*.jsonl.gz` files under `dir`, recursively, in stable
/// sorted order (so each file's index, and thus its output part name, is stable
/// across runs for a fixed input set).
pub(crate) fn input_files(dir: &Path) -> Result<Vec<PathBuf>> {
    sorted_glob(&format!(
        "{}/**/*.jsonl.gz",
        dir.to_string_lossy().trim_end_matches('/')
    ))
}

/// Glob `pattern` and return the matches in stable sorted order.
pub(crate) fn sorted_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = glob(pattern)?.filter_map(Result::ok).collect();
    files.sort();
    Ok(files)
}

/// Build a rayon pool with `threads` workers, or all available CPUs when
/// `threads == 0`.
pub(crate) fn make_pool(threads: usize) -> Result<rayon::ThreadPool> {
    let n = if threads == 0 {
        num_cpus::get()
    } else {
        threads
    };
    log::info!("using {n} threads");
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build()
        .context("building thread pool")
}
