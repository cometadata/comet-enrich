//! Deduplicate DOI records before running enrichment methods.

use crate::datacite::Metadata;
use crate::fanout::open_gz;
use crate::progress::Progress;

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use std::collections::{HashMap, hash_map::Entry};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use xxhash_rust::xxh3::xxh3_128;

/// One record with a DOI, as found in a source file.
pub struct DoiOccurrence {
    pub doi: Box<str>,
    /// `attributes.updated`, when present and valid RFC 3339.
    pub updated: Option<OffsetDateTime>,
    /// Physical line number, starting at one.
    pub line: u64,
}

/// Everything the scanner found in one file.
#[derive(Default)]
pub struct FileScan {
    /// Lines that parsed as JSON.
    pub records: u64,
    pub without_doi: u64,
    pub malformed: u64,
    pub occurrences: Vec<DoiOccurrence>,
}

fn scan_file(path: &Path) -> Result<FileScan> {
    let mut reader = open_gz(path).with_context(|| format!("opening {}", path.display()))?;
    let mut scan = FileScan::default();
    let mut line = String::new();
    let mut number = 0;
    loop {
        line.clear();
        if reader
            .read_line(&mut line)
            .with_context(|| format!("reading {} after line {number}", path.display()))?
            == 0
        {
            break;
        }
        number += 1;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(metadata) = serde_json::from_str::<Metadata>(&line) else {
            scan.malformed += 1;
            continue;
        };
        scan.records += 1;
        let Some(doi) = metadata.doi() else {
            scan.without_doi += 1;
            continue;
        };
        scan.occurrences.push(DoiOccurrence {
            doi: doi.into(),
            updated: metadata
                .updated()
                .and_then(|text| OffsetDateTime::parse(text, &Rfc3339).ok()),
            line: number,
        });
    }
    Ok(scan)
}

/// Scan files in parallel, handing each finished file to `on_file` one at a time, in any order.
///
/// Each worker buffers at most one file. A file that cannot be read is passed as `Err`;
/// the caller decides what that means. An error from `on_file` stops the scan.
///
/// # Errors
///
/// Returns the first error from `on_file`.
pub fn scan_doi_occurrences(
    files: &[PathBuf],
    pool: &rayon::ThreadPool,
    on_file: impl FnMut(usize, Result<FileScan>) -> Result<()> + Send,
) -> Result<()> {
    let on_file = Mutex::new(on_file);
    pool.install(|| {
        files.par_iter().enumerate().try_for_each(|(file, path)| {
            let scan = scan_file(path);
            on_file.lock().unwrap()(file, scan)
        })
    })
}

/// One candidate for a DOI, ordered so the greater occurrence is the winner.
///
/// Field order defines the rule: latest `updated`, then the later file, then
/// the later line. This is the single definition of the winning rule; the
/// `duplicate-dois` tool reports the same choice via [`winner`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Occurrence {
    pub updated: Option<OffsetDateTime>,
    pub file: usize,
    pub line: u64,
}

/// Index of the occurrence enrichment keeps, or `None` when any candidate
/// lacks a usable `updated`, since enrichment refuses to choose then.
///
/// `sorted` must be ordered by file then line, so that on equal timestamps the
/// last maximum matches the incremental `candidate > previous` rule in
/// [`find_duplicate_lines`].
#[must_use]
pub fn winner(sorted: &[Occurrence]) -> Option<usize> {
    if sorted.iter().any(|o| o.updated.is_none()) {
        return None;
    }
    sorted
        .iter()
        .enumerate()
        .max_by_key(|(_, o)| **o)
        .map(|(idx, _)| idx)
}

/// Return sorted duplicate line numbers to skip for each sorted input file.
///
/// A file that cannot be read contributes no candidates; the extraction pass reports it.
pub(crate) fn find_duplicate_lines(
    files: &[PathBuf],
    pool: &rayon::ThreadPool,
) -> Result<Vec<Vec<u64>>> {
    let mut progress = Progress::new("Deduplicating DOI records", Some(files.len() as u64))?;
    let mut winners: HashMap<u128, Occurrence> = HashMap::new();
    let mut losers: Vec<Vec<u64>> = vec![Vec::new(); files.len()];
    scan_doi_occurrences(files, pool, |file, scan| {
        progress.file_finished();
        let scan = match scan {
            Ok(scan) => scan,
            Err(error) => {
                log::warn!(
                    "DOI deduplication skipped {}: {error:#}",
                    files[file].display()
                );
                return Ok(());
            }
        };
        for found in scan.occurrences {
            let candidate = Occurrence {
                updated: found.updated,
                file,
                line: found.line,
            };
            let loser = match winners.entry(xxh3_128(found.doi.as_bytes())) {
                Entry::Vacant(entry) => {
                    entry.insert(candidate);
                    continue;
                }
                Entry::Occupied(mut entry) => {
                    let previous = *entry.get();
                    if previous.updated.is_none() || candidate.updated.is_none() {
                        bail!(
                            "cannot select latest occurrence for DOI {}: missing or invalid updated at {}:{} or {}:{}",
                            found.doi,
                            files[previous.file].display(),
                            previous.line,
                            files[file].display(),
                            candidate.line,
                        );
                    }
                    if candidate > previous {
                        entry.insert(candidate)
                    } else {
                        candidate
                    }
                }
            };
            losers[loser.file].push(loser.line);
        }
        Ok(())
    })?;
    for lines in &mut losers {
        lines.sort_unstable();
    }
    log::info!(
        "DOI deduplication: {} duplicate occurrences skipped",
        losers.iter().map(Vec::len).sum::<usize>(),
    );
    progress.finish();
    Ok(losers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::make_pool;
    use comet_enrich_test_support::{assert_err_contains, write_gz_lines};
    use serde_json::json;

    #[test]
    fn find_duplicate_lines_ranks_timestamps_then_source_locations() {
        let early = Some("2026-09-01T00:00:00Z");
        let middle = Some("2026-09-02T00:00:00Z");
        let late = Some("2026-09-03T00:00:00Z");
        let cases = [
            ("latest", [late, early, middle], Some([vec![], vec![1, 3]])),
            ("last file", [late, late, early], Some([vec![2], vec![3]])),
            ("last line", [late, late, late], Some([vec![2], vec![1]])),
            (
                "offset tie",
                [early, Some("2026-09-01T01:00:00+01:00"), early],
                Some([vec![2], vec![1]]),
            ),
            (
                "subseconds",
                [Some("2026-09-01T00:00:00.001Z"), early, early],
                Some([vec![], vec![1, 3]]),
            ),
            ("missing date", [None, early, late], None),
            ("invalid date", [early, Some("invalid"), late], None),
        ];
        for (label, dates, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let files = vec![
                dir.path().join("part_0000.jsonl.gz"),
                dir.path().join("part_0001.jsonl.gz"),
            ];
            let records: Vec<String> = dates
                .iter()
                .enumerate()
                .map(|(idx, updated)| {
                    json!({
                        "id": if idx == 0 { json!(42) } else { json!("10.1/a") },
                        "attributes": {"doi": "10.1/a", "updated": updated, "label": idx}
                    })
                    .to_string()
                })
                .collect();
            write_gz_lines(&files[0], &["", &records[0]]);
            write_gz_lines(&files[1], &[&records[1], "{malformed", &records[2]]);
            for threads in [1, 3] {
                let pool = make_pool(threads).unwrap();
                let result = find_duplicate_lines(&files, &pool);
                if let Some(expected) = &expected {
                    assert_eq!(&result.unwrap(), expected, "{label}");
                } else {
                    assert_err_contains(result, "cannot select latest occurrence for DOI 10.1/a");
                }
            }
        }
    }
}
