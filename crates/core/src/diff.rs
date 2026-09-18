//! Diff two completed enrichment runs into asserted/retracted/superseded events.
//!
//! Both sides are read from `<dir>/enrichments/*.jsonl.gz`; `<dir>/manifest.json`
//! supplies the method name and exit status. Both must be complete runs written
//! by a current comet-enrich: every record carries an enrichment content `key`,
//! and no content key appears twice on one side.
//!
//! Three passes: index the old side, stream the new side emitting `asserted`
//! and `superseded`, then re-read the old side emitting `retracted`. Each
//! index holds a content key and a canonical `enrichedValue` hash per record.
//! A missing key, a repeated key, a malformed line, or a side that is not a
//! successful run fails the diff.

use crate::artifact_lifecycle as lifecycle;
use crate::fanout::{FileError, list_jsonl_gz, open_gz, scan_jsonl_records};
use crate::key::{embedded_key, enriched_value_hash};
use crate::manifest::{EXIT_SUCCESS, MANIFEST_FILE, SourceRelease, write_manifest_json};
use crate::progress::Progress;
use crate::writer::{ENRICHMENTS_DIR, ParallelRollingWriter};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};
use std::path::{Path, PathBuf};

/// Version of the diff manifest layout.
pub const DIFF_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Options for one diff run.
pub struct DiffOptions {
    pub old: PathBuf,
    pub new: PathBuf,
    pub output: PathBuf,
    pub output_part_size_bytes: u64,
    pub output_writer_lanes: usize,
}

/// Counters for one diff run.
///
/// The per-side record counts are reported under each side in the manifest
/// (`old.records`, `new.records`) rather than repeated among the event counters.
#[derive(Debug, Default, Clone, Serialize)]
pub struct DiffStats {
    #[serde(skip)]
    pub old_records: u64,
    #[serde(skip)]
    pub new_records: u64,
    pub asserted: u64,
    pub retracted: u64,
    pub superseded: u64,
    pub unchanged: u64,
}

/// The subset of a side's `manifest.json` the diff reads.
#[derive(Debug, Clone, Deserialize)]
pub struct SideManifest {
    pub method: SideMethod,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceRelease>,
    /// `success` or `partial`. Absent only in manifests written before the
    /// field existed, which the diff refuses.
    pub exit_status: Option<String>,
}

/// Method identity from a side's manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct SideMethod {
    pub name: String,
    pub version: String,
}

/// Result of a diff run.
#[derive(Debug)]
pub struct DiffOutcome {
    pub stats: DiffStats,
    pub old_manifest: SideManifest,
    pub new_manifest: SideManifest,
}

/// Content key to canonical `enrichedValue` hash for one side.
type Index = HashMap<u128, u128>;

fn read_side_manifest(dir: &Path) -> Result<SideManifest> {
    let path = dir.join(MANIFEST_FILE);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Require a successful run so missing records are not mistaken for retractions.
fn ensure_successful(side: &str, manifest: &SideManifest) -> Result<()> {
    match manifest.exit_status.as_deref() {
        Some(EXIT_SUCCESS) => Ok(()),
        Some(status) => bail!(
            "the {side} release is not a complete run: manifest exit_status is `{status}` (expected `{EXIT_SUCCESS}`)"
        ),
        None => bail!(
            "the {side} release manifest has no exit_status; both sides must be produced by a current comet-enrich"
        ),
    }
}

/// List sorted enrichment parts. An empty directory is valid; a missing directory is an error.
fn side_files(side_dir: &Path) -> Result<Vec<PathBuf>> {
    let dir = side_dir.join(ENRICHMENTS_DIR);
    if !dir.is_dir() {
        bail!("enrichments path is not a directory: {}", dir.display());
    }
    list_jsonl_gz(&dir)
}

/// The event a diff record carries; the strings are pinned by the schema.
#[derive(Clone, Copy)]
enum DiffEvent {
    Asserted,
    Superseded,
    Retracted,
}

impl DiffEvent {
    fn as_str(self) -> &'static str {
        match self {
            DiffEvent::Asserted => "asserted",
            DiffEvent::Superseded => "superseded",
            DiffEvent::Retracted => "retracted",
        }
    }
}

fn doi_of(rec: &Value) -> &str {
    rec.get("doi").and_then(Value::as_str).unwrap_or("<no doi>")
}

/// Invoke `f` for every record in every gz part of one side and return the
/// record count.
///
/// A completed release must be fully readable, so a read error or a malformed
/// line fails the diff instead of being counted.
fn for_each_record(
    files: &[PathBuf],
    phase: &'static str,
    mut f: impl FnMut(Value, &Path) -> Result<()>,
) -> Result<u64> {
    let mut progress = Progress::new(phase, Some(files.len() as u64))?;
    let mut scanned = 0u64;
    for path in files {
        progress.file_started(path);
        let reader = open_gz(path).with_context(|| format!("opening {}", path.display()))?;
        let tally = scan_jsonl_records(reader, &[], |rec| {
            f(rec, path).map_err(FileError::Fatal)?;
            progress.record_processed();
            Ok(())
        })
        .map_err(|e| match e {
            FileError::Read(e) | FileError::Fatal(e) => e,
        })
        .with_context(|| format!("reading {}", path.display()))?;
        if tally.malformed > 0 {
            bail!(
                "{} malformed line(s) in {}",
                tally.malformed,
                path.display()
            );
        }
        scanned += tally.scanned;
        progress.file_finished();
    }
    progress.finish();
    Ok(scanned)
}

/// The record with `event` appended, preserving field order. The record's own
/// `key` was validated by [`embedded_key`] and passes through untouched.
fn event_record(mut rec: Value, event: DiffEvent) -> Value {
    if let Some(m) = rec.as_object_mut() {
        m.insert("event".into(), Value::String(event.as_str().to_owned()));
    }
    rec
}

/// Record `rec` in `index`, failing if its key was already seen on this side.
/// Returns the content key and the canonical `enrichedValue` hash.
fn insert_unique(index: &mut Index, rec: &Value, side: &str, path: &Path) -> Result<(u128, u128)> {
    let key = embedded_key(rec).with_context(|| format!("in the {side} release"))?;
    let value = enriched_value_hash(rec);
    match index.entry(key) {
        Entry::Occupied(_) => bail!(
            "key {key:032x} (doi `{}`) appears twice in the {side} release; repeated in {}",
            doi_of(rec),
            path.display()
        ),
        Entry::Vacant(slot) => {
            slot.insert(value);
        }
    }
    Ok((key, value))
}

/// Pass 1: index the old side.
fn index_old(files: &[PathBuf], stats: &mut DiffStats) -> Result<Index> {
    let mut index = Index::new();
    stats.old_records = for_each_record(files, "Indexing old [1/3]", |rec, path| {
        insert_unique(&mut index, &rec, "old", path)?;
        Ok(())
    })?;
    Ok(index)
}

/// Pass 2: stream the new side, emitting `asserted` and `superseded`.
fn emit_new(
    files: &[PathBuf],
    old: &Index,
    writer: &ParallelRollingWriter<'_>,
    stats: &mut DiffStats,
) -> Result<Index> {
    let mut index = Index::new();
    stats.new_records = for_each_record(files, "Comparing new [2/3]", |rec, path| {
        let (key, value) = insert_unique(&mut index, &rec, "new", path)?;
        let event = match old.get(&key) {
            None => {
                stats.asserted += 1;
                DiffEvent::Asserted
            }
            Some(&prev) if prev == value => {
                stats.unchanged += 1;
                return Ok(());
            }
            Some(_) => {
                stats.superseded += 1;
                DiffEvent::Superseded
            }
        };
        writer.push(&event_record(rec, event))
    })?;
    Ok(index)
}

/// Pass 3: re-read the old side, emitting `retracted` for keys absent from new.
///
/// Pass 1 proved every old key unique, so each retraction emits once.
fn emit_retracted(
    files: &[PathBuf],
    new: &Index,
    writer: &ParallelRollingWriter<'_>,
    stats: &mut DiffStats,
) -> Result<()> {
    for_each_record(files, "Finding retractions [3/3]", |rec, _| {
        // Pass 1 already validated every old key, so this cannot fail.
        let key = embedded_key(&rec)?;
        if new.contains_key(&key) {
            return Ok(());
        }
        stats.retracted += 1;
        writer.push(&event_record(rec, DiffEvent::Retracted))
    })?;
    Ok(())
}

/// Run the diff and write `enrichments/` under `opts.output`.
///
/// # Errors
///
/// Fails on unreadable inputs, malformed lines, mismatched method names, a
/// side that is not a successful run, a record without a key, a key that
/// appears twice on one side, or any write failure.
pub fn run_diff(opts: &DiffOptions) -> Result<DiffOutcome> {
    lifecycle::ensure_disjoint(&opts.output, &[("old", &opts.old), ("new", &opts.new)])?;
    let old_manifest = read_side_manifest(&opts.old)?;
    let new_manifest = read_side_manifest(&opts.new)?;
    if old_manifest.method.name != new_manifest.method.name {
        bail!(
            "method mismatch: old is `{}`, new is `{}`",
            old_manifest.method.name,
            new_manifest.method.name
        );
    }
    ensure_successful("old", &old_manifest)?;
    ensure_successful("new", &new_manifest)?;

    let old_files = side_files(&opts.old)?;
    let new_files = side_files(&opts.new)?;

    lifecycle::clear_run_outputs(&opts.output)?;

    let writer = ParallelRollingWriter::create(
        &opts.output.join(ENRICHMENTS_DIR),
        None,
        opts.output_part_size_bytes,
        opts.output_writer_lanes,
    )?;

    let mut stats = DiffStats::default();
    let old = index_old(&old_files, &mut stats)?;
    let new = emit_new(&new_files, &old, &writer, &mut stats)?;
    emit_retracted(&old_files, &new, &writer, &mut stats)?;
    let progress = Progress::new("Finalizing output", None)?;
    writer.finish()?;
    progress.finish();

    Ok(DiffOutcome {
        stats,
        old_manifest,
        new_manifest,
    })
}

/// Manifest written at the diff output root.
#[derive(Serialize)]
pub struct DiffManifest {
    pub schema_version: u32,
    pub method: DiffMethodInfo,
    pub old: DiffSideInfo,
    pub new: DiffSideInfo,
    pub artifact_paths: DiffArtifactPaths,
    pub counters: DiffStats,
    pub timings_ms: DiffTimings,
}

/// Method identity for a diff: both sides' versions and the diff tool's own.
#[derive(Serialize)]
pub struct DiffMethodInfo {
    pub name: String,
    pub old_version: String,
    pub new_version: String,
    pub diff_tool_version: &'static str,
}

/// One side's provenance and size.
#[derive(Serialize)]
pub struct DiffSideInfo {
    pub sources: BTreeMap<String, SourceRelease>,
    pub records: u64,
}

/// Paths the diff produced, relative to the output directory.
#[derive(Serialize)]
pub struct DiffArtifactPaths {
    pub enrichments: String,
}

/// Wall-clock timings for the diff run.
#[derive(Serialize)]
pub struct DiffTimings {
    pub total: u64,
}

impl DiffManifest {
    /// Assemble the manifest from a finished diff run.
    #[must_use]
    pub fn build(outcome: &DiffOutcome, diff_tool_version: &'static str, total_ms: u64) -> Self {
        DiffManifest {
            schema_version: DIFF_MANIFEST_SCHEMA_VERSION,
            method: DiffMethodInfo {
                name: outcome.new_manifest.method.name.clone(),
                old_version: outcome.old_manifest.method.version.clone(),
                new_version: outcome.new_manifest.method.version.clone(),
                diff_tool_version,
            },
            old: DiffSideInfo {
                sources: outcome.old_manifest.sources.clone(),
                records: outcome.stats.old_records,
            },
            new: DiffSideInfo {
                sources: outcome.new_manifest.sources.clone(),
                records: outcome.stats.new_records,
            },
            artifact_paths: DiffArtifactPaths {
                enrichments: format!("{ENRICHMENTS_DIR}/"),
            },
            counters: outcome.stats.clone(),
            timings_ms: DiffTimings { total: total_ms },
        }
    }

    /// Serialize the manifest to `<output_dir>/manifest.json` (pretty JSON).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the file write fails.
    pub fn write(&self, output_dir: &Path) -> Result<()> {
        write_manifest_json(self, output_dir, "diff manifest")
    }
}
