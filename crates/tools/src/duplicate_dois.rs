//! Find repeated DOIs within one source snapshot and report where each occurrence lives.

use anyhow::{Context, Result, ensure};
use comet_enrich_core::{
    Occurrence, Progress, ensure_disjoint, input_files, make_pool, scan_doi_occurrences, winner,
};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// One snapshot containing updated_YYYY-MM directories.
    #[arg(value_name = "SNAPSHOT", value_hint = clap::ValueHint::DirPath)]
    snapshot: PathBuf,

    /// Write the JSON report to this file.
    #[arg(long, value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
    output: PathBuf,

    /// Worker threads for source scanning; zero uses all available CPUs.
    #[arg(long, default_value_t = 0, value_name = "N")]
    threads: usize,
}

/// One `*.jsonl.gz` part beneath a month directory.
struct SourceFile {
    path: PathBuf,
    /// Path relative to the snapshot root, used in reports.
    relative: String,
}

impl SourceFile {
    /// The `updated_YYYY-MM` directory name.
    fn month(&self) -> &str {
        self.relative.split('/').next().unwrap_or_default()
    }
}

struct Location {
    file: usize,
    line: u64,
    updated: Option<OffsetDateTime>,
}

#[derive(Default)]
struct Scan {
    locations: HashMap<Box<str>, Vec<Location>>,
    records: u64,
    without_doi: u64,
    malformed: u64,
}

pub(crate) fn run(args: &Args) -> Result<()> {
    let files = snapshot_files(&args.snapshot)?;
    // The report must not replace a part it is about to describe.
    ensure_disjoint(&args.output, &[("SNAPSHOT", &args.snapshot)])?;
    let scan = scan_snapshot(&files, args.threads)?;
    let report = scan.report(&files);
    report.write_json(&args.output)?;
    let mut output = BufWriter::new(io::stdout().lock());
    report.write_summary(&mut output)?;
    writeln!(output, "Wrote {}", args.output.display())?;
    output.flush().context("writing duplicate summary")
}

fn is_month(name: &str) -> bool {
    name.strip_prefix("updated_")
        .is_some_and(|month| month.len() == 7 && month.as_bytes()[4] == b'-')
}

/// Every part beneath an `updated_YYYY-MM` directory, in sorted path order.
fn snapshot_files(root: &Path) -> Result<Vec<SourceFile>> {
    let files: Vec<SourceFile> = input_files(root)?
        .into_iter()
        .filter_map(|path| {
            let relative = path.strip_prefix(root).ok()?.to_str()?.to_owned();
            is_month(relative.split('/').next()?).then_some(SourceFile { path, relative })
        })
        .collect();
    ensure!(
        !files.is_empty(),
        "no updated_YYYY-MM directories in {}; pass one snapshot root",
        root.display()
    );
    Ok(files)
}

fn scan_snapshot(files: &[SourceFile], threads: usize) -> Result<Scan> {
    let paths: Vec<PathBuf> = files.iter().map(|file| file.path.clone()).collect();
    let pool = make_pool(threads)?;
    let mut progress = Progress::new("Scanning", Some(files.len() as u64))?;
    let mut scan = Scan::default();
    scan_doi_occurrences(&paths, &pool, |file, found| {
        let found = found?;
        progress.file_started(&paths[file]);
        for occurrence in found.occurrences {
            scan.locations
                .entry(occurrence.doi)
                .or_default()
                .push(Location {
                    file,
                    line: occurrence.line,
                    updated: occurrence.updated,
                });
        }
        scan.records += found.records;
        scan.without_doi += found.without_doi;
        scan.malformed += found.malformed;
        progress.records_processed(found.records);
        progress.file_finished();
        Ok(())
    })?;
    progress.finish();
    Ok(scan)
}

#[derive(Serialize)]
struct Report<'a> {
    summary: Summary,
    /// Distinct duplicated DOIs with at least one occurrence in each month directory.
    by_month: BTreeMap<&'a str, u64>,
    dois: Vec<DoiReport<'a>>,
}

#[derive(Serialize)]
struct Summary {
    scanned_records: u64,
    records_without_doi: u64,
    malformed_lines: u64,
    distinct_dois: usize,
    duplicated_dois: usize,
    extra_occurrences: u64,
}

#[derive(Serialize)]
struct DoiReport<'a> {
    doi: &'a str,
    occurrences: usize,
    locations: Vec<LocationReport<'a>>,
}

#[derive(Serialize)]
struct LocationReport<'a> {
    month: &'a str,
    path: &'a str,
    line: u64,
    updated: Option<String>,
    /// The occurrence enrichment keeps: latest `updated`, then last file and line.
    selected: bool,
}

impl Scan {
    fn report<'a>(&'a self, files: &'a [SourceFile]) -> Report<'a> {
        let mut repeated: Vec<(&str, &Vec<Location>)> = self
            .locations
            .iter()
            .filter(|(_, locations)| locations.len() > 1)
            .map(|(doi, locations)| (doi.as_ref(), locations))
            .collect();
        repeated.sort_unstable_by_key(|(doi, _)| *doi);
        let mut by_month = BTreeMap::new();
        let mut extra_occurrences = 0;
        let dois = repeated
            .into_iter()
            .map(|(doi, locations)| {
                let mut locations: Vec<&Location> = locations.iter().collect();
                locations.sort_by_key(|location| (location.file, location.line));
                extra_occurrences += locations.len() as u64 - 1;
                let months: HashSet<&str> = locations
                    .iter()
                    .map(|location| files[location.file].month())
                    .collect();
                for month in months {
                    *by_month.entry(month).or_default() += 1;
                }
                // Same rule as enrichment's DOI deduplication, which refuses to
                // choose when a competing timestamp is unusable.
                let candidates: Vec<Occurrence> = locations
                    .iter()
                    .map(|location| Occurrence {
                        updated: location.updated,
                        file: location.file,
                        line: location.line,
                    })
                    .collect();
                let selected = winner(&candidates);
                DoiReport {
                    doi,
                    occurrences: locations.len(),
                    locations: locations
                        .into_iter()
                        .enumerate()
                        .map(|(idx, location)| LocationReport {
                            month: files[location.file].month(),
                            path: &files[location.file].relative,
                            line: location.line,
                            updated: location
                                .updated
                                .and_then(|updated| updated.format(&Rfc3339).ok()),
                            selected: selected == Some(idx),
                        })
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        Report {
            summary: Summary {
                scanned_records: self.records,
                records_without_doi: self.without_doi,
                malformed_lines: self.malformed,
                distinct_dois: self.locations.len(),
                duplicated_dois: dois.len(),
                extra_occurrences,
            },
            by_month,
            dois,
        }
    }
}

impl Report<'_> {
    fn write_json(&self, path: &Path) -> Result<()> {
        let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self)
            .with_context(|| format!("writing {}", path.display()))?;
        writeln!(writer)?;
        writer
            .flush()
            .with_context(|| format!("writing {}", path.display()))
    }

    fn write_summary(&self, out: &mut impl Write) -> Result<()> {
        let summary = &self.summary;
        writeln!(out, "Scanned records: {}", summary.scanned_records)?;
        writeln!(out, "Records without DOI: {}", summary.records_without_doi)?;
        writeln!(out, "Malformed lines: {}", summary.malformed_lines)?;
        writeln!(out, "Distinct DOIs: {}", summary.distinct_dois)?;
        writeln!(out, "Duplicated DOIs: {}", summary.duplicated_dois)?;
        writeln!(out, "Extra occurrences: {}", summary.extra_occurrences)?;
        writeln!(out)?;
        writeln!(out, "Duplicated DOIs by month:")?;
        let mut months: Vec<(&str, u64)> = self
            .by_month
            .iter()
            .map(|(month, count)| (*month, *count))
            .collect();
        months.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        if months.is_empty() {
            writeln!(out, "  (none)")?;
        }
        for (month, count) in months {
            writeln!(out, "  {month}  {count}")?;
        }
        Ok(())
    }
}
