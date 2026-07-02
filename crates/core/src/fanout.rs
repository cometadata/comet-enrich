//! Shared input-file scanning helpers for the transform and staged runners.

use anyhow::{Context, Result, bail};
use glob::glob;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Classifies per-file failures.
#[derive(Debug)]
pub(crate) enum FileError {
    /// The input file could not be read. Counted as a failed file; the run
    /// continues with the other files.
    Read(anyhow::Error),
    /// A record could not be written, diverted, or flushed. The output would be
    /// incomplete, so this aborts the whole run.
    Fatal(anyhow::Error),
}

/// Discover input `*.jsonl.gz` files under `dir`, recursively and in sorted order.
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

/// Own the skip-reason keys collected during a run.
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
    /// Lines that could not be parsed as JSON (blank lines are ignored, not counted).
    pub malformed: u64,
}

/// Scan a JSONL reader, skipping blank lines and counting malformed lines.
///
/// # Errors
///
/// Returns [`FileError::Read`] for I/O or decompression errors, and propagates
/// failures from `on_record`.
pub(crate) fn scan_jsonl_records(
    reader: impl BufRead,
    mut on_record: impl FnMut(&Value) -> Result<(), FileError>,
) -> Result<ScanTally, FileError> {
    let mut tally = ScanTally::default();
    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(e) => return Err(FileError::Read(e.into())),
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

/// Build the standard progress bar.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};

    /// Reader that fails after its buffered prefix.
    struct FailAfter {
        data: Cursor<Vec<u8>>,
    }

    impl Read for FailAfter {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self.data.read(buf)? {
                0 => Err(std::io::Error::other("simulated corrupt stream")),
                n => Ok(n),
            }
        }
    }

    #[test]
    fn scan_counts_parse_failures_as_malformed_and_continues() {
        let input = "{bad json\n{\"a\":1}\n\n";
        let mut records = 0;
        let tally = scan_jsonl_records(Cursor::new(input), |_| {
            records += 1;
            Ok(())
        })
        .unwrap();

        assert_eq!(tally.scanned, 1);
        assert_eq!(tally.malformed, 1);
        assert_eq!(records, 1);
    }

    #[test]
    fn scan_fails_the_file_on_read_error() {
        let reader = BufReader::new(FailAfter {
            data: Cursor::new(b"{\"a\":1}\n".to_vec()),
        });
        let mut records = 0;
        let result = scan_jsonl_records(reader, |_| {
            records += 1;
            Ok(())
        });

        assert_eq!(records, 1, "the good prefix is still scanned");
        match result {
            Err(FileError::Read(e)) => {
                assert!(e.to_string().contains("simulated corrupt stream"));
            }
            Err(FileError::Fatal(_)) => panic!("read error must not be fatal"),
            Ok(_) => panic!("read error must fail the file"),
        }
    }
}
