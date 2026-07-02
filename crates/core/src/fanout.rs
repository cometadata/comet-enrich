//! Shared input-file scanning helpers for the transform and staged runners.

use anyhow::{Context, Result, bail};
use glob::glob;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::BufRead;
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
///
/// # Errors
///
/// Returns an error when no input files are found: an empty corpus is
/// indistinguishable from a mistyped `--input` path, and must not become a
/// clean-looking empty run.
pub(crate) fn input_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let files = sorted_glob(&format!(
        "{}/**/*.jsonl.gz",
        dir.to_string_lossy().trim_end_matches('/')
    ))?;
    if files.is_empty() {
        bail!("no *.jsonl.gz input files found under {}", dir.display());
    }
    Ok(files)
}

/// Glob `pattern` and return the matches in stable sorted order.
pub(crate) fn sorted_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = glob(pattern)?.filter_map(Result::ok).collect();
    files.sort();
    Ok(files)
}

/// Own the `&'static str` skip-reason keys collected during a run.
///
/// Both run paths count skips under `&'static str` reasons while scanning, but the
/// shared [`crate::RunStats::skipped`] shape (and the staged path's JSON stats
/// sidecar) needs owned keys.
pub(crate) fn own_skips(skipped: BTreeMap<&'static str, u64>) -> BTreeMap<String, u64> {
    skipped
        .into_iter()
        .map(|(reason, n)| (reason.to_owned(), n))
        .collect()
}

/// Per-file tally produced while scanning a JSONL input.
#[derive(Default)]
pub(crate) struct ScanTally {
    /// Lines that parsed into a JSON record.
    pub scanned: u64,
    /// Lines that could not be read or parsed (blank lines are ignored, not counted).
    pub malformed: u64,
}

/// Scan a `.jsonl` reader line by line: skip blank lines, count unreadable or
/// unparseable lines as malformed, and hand each parsed JSON record to `on_record`.
///
/// Both run paths share this preamble so the blank/malformed/scanned policy lives in
/// one place. The reader's own per-line errors are tallied, never propagated; only an
/// error returned by `on_record` (a fatal write) stops the scan.
///
/// # Errors
///
/// Returns the error from `on_record` if it fails on any record.
pub(crate) fn scan_jsonl_records<E>(
    reader: impl BufRead,
    mut on_record: impl FnMut(&Value) -> std::result::Result<(), E>,
) -> std::result::Result<ScanTally, E> {
    let mut tally = ScanTally::default();
    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(_) => {
                tally.malformed += 1;
                continue;
            }
        };
        let Ok(rec) = serde_json::from_str::<Value>(&line) else {
            tally.malformed += 1;
            continue;
        };
        tally.scanned += 1;
        on_record(&rec)?;
    }
    Ok(tally)
}

/// Build the standard progress bar over `len` units, shared by every stage so
/// all bars render the same way.
pub(crate) fn progress_bar(len: u64) -> Result<indicatif::ProgressBar> {
    let pb = indicatif::ProgressBar::new(len);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")?
            .progress_chars("#>-"),
    );
    Ok(pb)
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
