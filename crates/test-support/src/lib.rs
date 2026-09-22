//! Test helpers for gzip fixtures, config paths, and fake match services.

// JSONL, gzip, and DataCite are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

pub use comet_enrich_core::FakeMatchService;
use comet_enrich_core::{
    EnrichmentAction, EnrichmentParts, EnrichmentRecord, EnrichmentTemplate, RunOptions,
    enrichment_content_key,
};

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

/// Input subdirectory used by the DataCite snapshot layout the runners expect.
pub const INPUT_SUBDIR: &str = "updated_2024-01";

/// Source id used by every fixture-driven test.
pub const SOURCE_ID: &str = "10.82461/bpzr-jd55";

/// Method name used by fixture-driven diff tests.
pub const METHOD: &str = "test-method";

/// Run options every fixture-driven test uses: one writer lane and a small batch.
#[must_use]
pub fn run_options(input: PathBuf, output: PathBuf, threads: usize) -> RunOptions {
    RunOptions {
        input,
        output,
        threads,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    }
}

/// An enrichment record for `doi` with its content key, as a run for
/// [`SOURCE_ID`] would write it.
#[must_use]
pub fn enrichment_record(
    method: &str,
    doi: &str,
    field: &'static str,
    action: EnrichmentAction,
    original: &Value,
    enriched: &Value,
) -> Value {
    let parts = EnrichmentParts {
        doi: doi.to_owned(),
        action,
        field,
        original: original.clone(),
        enriched: enriched.clone(),
    };
    EnrichmentRecord::new(&enrichment_template(), method, parts)
        .to_value()
        .unwrap()
}

/// Write a completed run directory: one `enrichments/part_NNNN.jsonl.gz` per
/// record slice plus a `manifest.json` for [`METHOD`], with `exit_status` when
/// given.
pub fn write_run_dir(dir: &Path, parts: &[&[Value]], exit_status: Option<&str>) {
    let enrich = dir.join("enrichments");
    fs::create_dir_all(&enrich).unwrap();
    for (idx, records) in parts.iter().enumerate() {
        write_gz_part(&enrich.join(format!("part_{idx:04}.jsonl.gz")), records);
    }
    let emitted: usize = parts.iter().map(|records| records.len()).sum();
    let mut manifest = json!({
        "schema_version": 1,
        "method": {"name": METHOD, "version": env!("CARGO_PKG_VERSION")},
        "source_id": SOURCE_ID,
        "sources": {"datacite": {"release_date": "2026-01-02"}},
        "report": {"counters": {"emitted": emitted}},
    });
    if let Some(status) = exit_status {
        manifest["exit_status"] = json!(status);
    }
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

/// Record template built from [`SOURCE_ID`].
#[must_use]
pub fn enrichment_template() -> EnrichmentTemplate {
    EnrichmentTemplate::new(SOURCE_ID).unwrap()
}

/// Write `records` as newline-delimited JSON into a gzip part at `path`.
///
/// Missing parent directories are created.
pub fn write_gz_part(path: &Path, records: &[Value]) {
    create_parent(path);
    let mut gz = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    for rec in records {
        gz.write_all(serde_json::to_string(rec).unwrap().as_bytes())
            .unwrap();
        gz.write_all(b"\n").unwrap();
    }
    gz.finish().unwrap();
}

/// Write raw newline-delimited `lines` into a gzip part at `path`.
///
/// Lines are written verbatim, so fixtures can include blanks or malformed entries.
pub fn write_gz_lines(path: &Path, lines: &[&str]) {
    create_parent(path);
    let mut gz = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    gz.write_all(lines.join("\n").as_bytes()).unwrap();
    gz.finish().unwrap();
}

fn create_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).unwrap();
        }
    }
}

/// Build a single-part gzip input fixture.
#[must_use]
pub fn gz_input_fixture(records: &[Value]) -> (TempDir, PathBuf, PathBuf) {
    gz_parts_fixture(&[records])
}

/// Build a gzip input fixture with one part per record slice.
#[must_use]
pub fn gz_parts_fixture(parts: &[&[Value]]) -> (TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    fs::create_dir_all(&output).unwrap();
    for (idx, records) in parts.iter().enumerate() {
        write_gz_part(
            &input
                .join(INPUT_SUBDIR)
                .join(format!("part_{idx:04}.jsonl.gz")),
            records,
        );
    }
    (dir, input, output)
}

/// Read one gzip file into a string.
#[must_use]
pub fn read_gz_string(path: &Path) -> String {
    let mut s = String::new();
    MultiGzDecoder::new(File::open(path).unwrap())
        .read_to_string(&mut s)
        .unwrap();
    s
}

/// Read every gzip part under `<output>/enrichments/` into enrichment records.
///
/// Record order across parts is not stable.
#[must_use]
pub fn read_enrichment_parts(output: &Path) -> Vec<Value> {
    let mut recs = Vec::new();
    for entry in fs::read_dir(output.join("enrichments")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("gz") {
            continue;
        }
        recs.extend(
            read_gz_string(&path)
                .lines()
                .map(|l| serde_json::from_str(l).unwrap()),
        );
    }
    recs
}

/// Enrichment records under `<output>/enrichments/`, keyed by DOI.
///
/// Panics if two records share a DOI, so tests stay one record per DOI.
#[must_use]
pub fn enrichments_by_doi(output: &Path) -> HashMap<String, Value> {
    let mut by_doi = HashMap::new();
    for rec in read_enrichment_parts(output) {
        let doi = rec["doi"].as_str().unwrap().to_owned();
        assert!(
            by_doi.insert(doi.clone(), rec).is_none(),
            "duplicate doi {doi}"
        );
    }
    by_doi
}

/// Assert a record's `contentKey` is the expected enrichment content key for
/// `method` and `action`.
#[track_caller]
pub fn assert_content_key(rec: &Value, method: &str, action: EnrichmentAction) {
    let want = enrichment_content_key(
        method,
        rec["doi"].as_str().unwrap(),
        rec["field"].as_str().unwrap(),
        action,
        &rec["originalValue"],
        &rec["enrichedValue"],
    );
    assert_eq!(
        rec["contentKey"],
        json!(want),
        "contentKey mismatch for doi {}",
        rec["doi"]
    );
}

/// Absolute path to a file under the workspace `configs/` directory.
#[must_use]
pub fn config_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../configs")
        .join(rel)
}

/// Assert two floats are equal within a small tolerance.
#[track_caller]
pub fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
}

/// Assert that `result` is an `Err` whose `Display` text contains `needle`.
#[track_caller]
pub fn assert_err_contains<T, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
    needle: &str,
) {
    match result {
        Ok(_) => panic!("expected Err containing {needle:?}, got Ok"),
        Err(e) => {
            let text = e.to_string();
            assert!(
                text.contains(needle),
                "error {text:?} did not contain {needle:?}"
            );
        }
    }
}
