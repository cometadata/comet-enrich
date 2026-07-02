//! Test helpers for workspace integration tests.
//!
//! Writes gzip fixtures, reads enrichment output parts, and locates config files.

// JSONL, gzip, and DataCite are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde_json::Value;
use tempfile::TempDir;

/// Input subdirectory used by the DataCite snapshot layout the runners expect.
const INPUT_SUBDIR: &str = "updated_2024-01";

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

/// Write raw `lines` (joined by newlines) into a gzip part at `path`.
///
/// Unlike [`write_gz_part`], the lines are written verbatim, so a fixture may include
/// blank or deliberately malformed entries. Missing parent directories are created.
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
    GzDecoder::new(File::open(path).unwrap())
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
