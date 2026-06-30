//! Shared, dependency-light test helpers for the comet-enrich workspace.
//!
//! These helpers were previously duplicated across crates' test modules: gzip input
//! fixtures, gzip readers, config-file paths, and a couple of assertion helpers.
//! Centralising them keeps tests across crates building fixtures and asserting
//! failures the same way.
//!
//! This crate is dev-only (`publish = false`) and intentionally does **not** depend on
//! `comet-enrichment-core`: staying free of workspace types lets a crate's own unit
//! tests use it without creating a dev-dependency cycle. Helpers that need core types
//! (e.g. a `MatchService` fake or a `RunOptions` builder) stay in `core` itself.

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

/// Lay out a single-part gzip input tree and return the temp dir plus the input and
/// output roots.
///
/// Layout: `<tmp>/input/updated_2024-01/part_0000.jsonl.gz` holding `records`, and an
/// empty `<tmp>/output`. The returned [`TempDir`] must be kept alive for the duration
/// of the test (bind it, e.g. `let (_dir, input, output) = gz_input_fixture(..)`).
#[must_use]
pub fn gz_input_fixture(records: &[Value]) -> (TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    fs::create_dir_all(&output).unwrap();
    write_gz_part(
        &input.join(INPUT_SUBDIR).join("part_0000.jsonl.gz"),
        records,
    );
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
/// `enrichments` mirrors `comet_enrichment_core::ENRICHMENTS_DIR`; it is inlined so
/// this helper crate stays free of a `core` dependency (which would otherwise create a
/// dev-dependency cycle for `core`'s own tests).
///
/// Record order across parts is not stable, so callers compare sets, not order.
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
///
/// Resolved from this crate's manifest dir (`crates/test-support`), which sits at the
/// same depth as every other crate, so `../../configs` always points at the workspace
/// config tree regardless of which crate calls this.
#[must_use]
pub fn config_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../configs")
        .join(rel)
}

/// Assert two floats are equal within a small tolerance (`1e-9`).
///
/// Use this for computed values such as coverage rates; do not compare them with
/// `f64::EPSILON`, which is far tighter than these values warrant.
#[track_caller]
pub fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
}

/// Assert that `result` is an `Err` whose `Display` text contains `needle`.
///
/// One idiom for "this should fail with a message naming X", replacing ad-hoc
/// `unwrap_err().to_string().contains(..)` chains.
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
