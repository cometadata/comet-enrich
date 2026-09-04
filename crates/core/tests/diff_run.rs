//! End-to-end tests for `diff::run_diff` over two completed run directories.

use comet_enrich_core::{DiffManifest, DiffOptions, EnrichmentAction, run_diff};
use comet_enrich_test_support::{
    METHOD, keyed_record, read_enrichment_parts, write_gz_lines, write_run_dir,
};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

/// Write a minimal successful run: `enrichments/part_0000.jsonl.gz` + `manifest.json`.
fn write_run(dir: &Path, records: &[Value]) {
    write_run_with_status(dir, records, Some("success"));
}

/// Write a run whose manifest carries `exit_status`, or omits it for `None`.
fn write_run_with_status(dir: &Path, records: &[Value], exit_status: Option<&str>) {
    write_run_dir(dir, &[records], exit_status);
}

/// Write a successful run that emitted nothing: `manifest.json` and an empty
/// `enrichments/` directory, exactly as the rolling writer leaves it.
fn write_empty_run(dir: &Path) {
    write_run_dir(dir, &[], Some("success"));
}

/// A keyed update record for `doi` with the given original/enriched types.
fn record(doi: &str, original: &str, enriched: &str) -> Value {
    keyed_record(
        METHOD,
        doi,
        "types",
        EnrichmentAction::Update,
        &json!({"resourceTypeGeneral": original}),
        &json!({"resourceTypeGeneral": enriched}),
    )
}

fn record_without_key(doi: &str, original: &str, enriched: &str) -> Value {
    let mut rec = record(doi, original, enriched);
    rec.as_object_mut().unwrap().remove("key");
    rec
}

fn dirs() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let (old, new, out) = (
        tmp.path().join("old"),
        tmp.path().join("new"),
        tmp.path().join("out"),
    );
    (tmp, old, new, out)
}

fn diff_options(old: &Path, new: &Path, out: &Path) -> DiffOptions {
    DiffOptions {
        old: old.to_path_buf(),
        new: new.to_path_buf(),
        output: out.to_path_buf(),
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    }
}

/// Read all diff event records back, sorted by (doi, event).
fn read_events(out: &Path) -> Vec<Value> {
    let mut recs = read_enrichment_parts(out);
    recs.sort_by_key(|r| {
        (
            r["doi"].as_str().unwrap().to_owned(),
            r["event"].as_str().unwrap().to_owned(),
        )
    });
    recs
}

#[test]
fn classifies_asserted_retracted_superseded_and_unchanged() {
    let (_tmp, old, new, out) = dirs();
    write_run(
        &old,
        &[
            record("10.1/unchanged", "Text", "Dataset"),
            record("10.1/superseded", "Text", "Dataset"),
            record("10.1/retracted", "Text", "Dataset"),
        ],
    );
    write_run(
        &new,
        &[
            record("10.1/unchanged", "Text", "Dataset"),
            record("10.1/superseded", "Text", "Software"),
            record("10.1/asserted", "Text", "Dataset"),
        ],
    );

    let outcome = run_diff(&diff_options(&old, &new, &out)).unwrap();

    let s = &outcome.stats;
    assert_eq!((s.old_records, s.new_records), (3, 3));
    assert_eq!(
        (s.asserted, s.retracted, s.superseded, s.unchanged),
        (1, 1, 1, 1)
    );
    let events = read_events(&out);
    let summary: Vec<(&str, &str)> = events
        .iter()
        .map(|e| (e["doi"].as_str().unwrap(), e["event"].as_str().unwrap()))
        .collect();
    assert_eq!(
        summary,
        [
            ("10.1/asserted", "asserted"),
            ("10.1/retracted", "retracted"),
            ("10.1/superseded", "superseded"),
        ]
    );
    // Superseded carries the new answer; retracted carries the old record.
    assert_eq!(
        events[2]["enrichedValue"]["resourceTypeGeneral"],
        json!("Software")
    );
    assert!(
        events
            .iter()
            .all(|e| e["key"].as_str().unwrap().len() == 32)
    );
    assert!(!out.join("diff.failed.jsonl").exists());
}

#[test]
fn value_comparison_ignores_object_key_order() {
    let (_tmp, old, new, out) = dirs();
    // Same enrichedValue, different insertion order on each side.
    let mut old_rec = record("10.1/order", "Text", "Dataset");
    old_rec["enrichedValue"] =
        serde_json::from_str(r#"{"resourceTypeGeneral":"Dataset","bibtex":"misc"}"#).unwrap();
    let mut new_rec = record("10.1/order", "Text", "Dataset");
    new_rec["enrichedValue"] =
        serde_json::from_str(r#"{"bibtex":"misc","resourceTypeGeneral":"Dataset"}"#).unwrap();
    write_run(&old, &[old_rec]);
    write_run(&new, &[new_rec]);

    let outcome = run_diff(&diff_options(&old, &new, &out)).unwrap();

    assert_eq!(outcome.stats.unchanged, 1);
    assert_eq!(outcome.stats.superseded, 0);
}

#[test]
fn repeated_key_on_either_side_fails_the_diff() {
    let clean = record("10.1/a", "Text", "Dataset");
    let same_answer = vec![clean.clone(), clean.clone()];
    let different_answer = vec![
        record("10.1/b", "Text", "Dataset"),
        record("10.1/b", "Text", "Software"),
    ];
    for repeated in [&same_answer, &different_answer] {
        let doi = repeated[0]["doi"].as_str().unwrap();
        for side in ["old", "new"] {
            let (_tmp, old, new, out) = dirs();
            let (old_recs, new_recs) = if side == "old" {
                (repeated.clone(), vec![clean.clone()])
            } else {
                (vec![clean.clone()], repeated.clone())
            };
            write_run(&old, &old_recs);
            write_run(&new, &new_recs);

            let err = format!(
                "{:#}",
                run_diff(&diff_options(&old, &new, &out)).unwrap_err()
            );

            assert!(err.contains("appears twice"), "{side}: {err}");
            assert!(err.contains(side), "{side}: {err}");
            assert!(err.contains(doi), "{side}: {err}");
            assert!(!out.join("manifest.json").exists());
        }
    }
}

#[test]
fn record_without_key_on_either_side_is_a_hard_error() {
    for side in ["old", "new"] {
        let (_tmp, old, new, out) = dirs();
        let keyless = record_without_key("10.1/x", "Text", "Dataset");
        let keyed = record("10.1/y", "Text", "Dataset");
        if side == "old" {
            write_run(&old, &[keyless]);
            write_run(&new, &[keyed]);
        } else {
            write_run(&old, &[keyed]);
            write_run(&new, &[keyless]);
        }

        let err = format!(
            "{:#}",
            run_diff(&diff_options(&old, &new, &out)).unwrap_err()
        );

        assert!(err.contains("no key"), "{side}: {err}");
        assert!(err.contains("10.1/x"), "{side}: {err}");
        assert!(err.contains(side), "{side}: {err}");
    }
}

#[test]
fn malformed_line_fails_the_diff() {
    let (_tmp, old, new, out) = dirs();
    let rec = record("10.1/a", "Text", "Dataset");
    write_run(&old, std::slice::from_ref(&rec));
    write_run(&new, std::slice::from_ref(&rec));
    let line = record("10.1/b", "Text", "Dataset").to_string();
    write_gz_lines(
        &new.join("enrichments/part_0001.jsonl.gz"),
        &[line.as_str(), "{not json"],
    );

    let err = format!(
        "{:#}",
        run_diff(&diff_options(&old, &new, &out)).unwrap_err()
    );

    assert!(err.contains("malformed"), "got: {err}");
}

#[test]
fn mismatched_method_names_are_rejected() {
    let (_tmp, old, new, out) = dirs();
    write_run(&old, &[]);
    write_run(&new, &[]);
    let manifest = fs::read_to_string(old.join("manifest.json")).unwrap();
    fs::write(
        old.join("manifest.json"),
        manifest.replace("test-method", "other-method"),
    )
    .unwrap();

    let err = run_diff(&diff_options(&old, &new, &out))
        .unwrap_err()
        .to_string();

    assert!(err.contains("method"), "got: {err}");
}

#[test]
fn diff_manifest_records_method_counters_and_sources() {
    let (_tmp, old, new, out) = dirs();
    write_run(&old, &[record("10.1/retracted", "Text", "Dataset")]);
    write_run(&new, &[record("10.1/asserted", "Text", "Dataset")]);

    let outcome = run_diff(&diff_options(&old, &new, &out)).unwrap();
    DiffManifest::build(&outcome, "0.4.0", 5)
        .write(&out)
        .unwrap();

    let m: Value =
        serde_json::from_str(&fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(m["schema_version"], json!(1));
    assert_eq!(m["method"]["name"], json!(METHOD));
    assert_eq!(m["method"]["diff_tool_version"], json!("0.4.0"));
    assert!(m.get("exit_status").is_none());
    assert_eq!(m["counters"]["asserted"], json!(1));
    assert_eq!(m["counters"]["retracted"], json!(1));
    assert_eq!(m["artifact_paths"], json!({"enrichments": "enrichments/"}));
    assert_eq!(
        m["old"]["sources"]["datacite"]["release_date"],
        json!("2026-01-02")
    );
    assert_eq!(m["timings_ms"]["total"], json!(5));
}

#[test]
fn output_overlapping_an_input_is_refused_before_anything_is_deleted() {
    let (_tmp, old, new, _out) = dirs();
    write_run(&old, &[record("10.1/a", "Text", "Dataset")]);
    write_run(&new, &[record("10.1/a", "Text", "Dataset")]);

    let err = format!(
        "{:#}",
        run_diff(&diff_options(&old, &new, &new)).unwrap_err()
    );

    assert!(err.contains("--output overlaps --new"), "got: {err}");
    assert!(new.join("manifest.json").is_file());
    assert!(new.join("enrichments/part_0000.jsonl.gz").is_file());
}

#[test]
fn partial_new_side_is_refused() {
    let (_tmp, old, new, out) = dirs();
    let rec = record("10.1/a", "Text", "Dataset");
    write_run(&old, std::slice::from_ref(&rec));
    write_run_with_status(&new, &[rec], Some("partial"));

    let err = format!(
        "{:#}",
        run_diff(&diff_options(&old, &new, &out)).unwrap_err()
    );

    assert!(err.contains("partial"), "got: {err}");
    assert!(err.contains("new"), "got: {err}");
}

#[test]
fn partial_old_side_is_refused() {
    let (_tmp, old, new, out) = dirs();
    let rec = record("10.1/a", "Text", "Dataset");
    write_run_with_status(&old, std::slice::from_ref(&rec), Some("partial"));
    write_run(&new, &[rec]);

    let err = format!(
        "{:#}",
        run_diff(&diff_options(&old, &new, &out)).unwrap_err()
    );

    assert!(err.contains("partial"), "got: {err}");
    assert!(err.contains("old"), "got: {err}");
}

#[test]
fn side_without_exit_status_is_refused() {
    for side in ["old", "new"] {
        let (_tmp, old, new, out) = dirs();
        let rec = record("10.1/a", "Text", "Dataset");
        let (old_status, new_status) = if side == "old" {
            (None, Some("success"))
        } else {
            (Some("success"), None)
        };
        write_run_with_status(&old, std::slice::from_ref(&rec), old_status);
        write_run_with_status(&new, &[rec], new_status);

        let err = format!(
            "{:#}",
            run_diff(&diff_options(&old, &new, &out)).unwrap_err()
        );

        assert!(err.contains("exit_status"), "{side}: {err}");
        assert!(err.contains(side), "{side}: {err}");
    }
}

#[test]
fn empty_new_side_retracts_every_old_record() {
    let (_tmp, old, new, out) = dirs();
    write_run(&old, &[record("10.1/a", "Text", "Dataset")]);
    write_empty_run(&new);

    let outcome = run_diff(&diff_options(&old, &new, &out)).unwrap();

    assert_eq!(outcome.stats.retracted, 1);
    assert_eq!(outcome.stats.new_records, 0);
    assert_eq!(read_events(&out)[0]["event"], json!("retracted"));
}

#[test]
fn empty_old_side_asserts_every_new_record() {
    let (_tmp, old, new, out) = dirs();
    write_empty_run(&old);
    write_run(&new, &[record("10.1/a", "Text", "Dataset")]);

    let outcome = run_diff(&diff_options(&old, &new, &out)).unwrap();

    assert_eq!(outcome.stats.asserted, 1);
    assert_eq!(outcome.stats.old_records, 0);
}

#[test]
fn missing_enrichments_directory_is_an_error() {
    let (_tmp, old, new, out) = dirs();
    write_run(&old, &[record("10.1/a", "Text", "Dataset")]);
    write_empty_run(&new);
    fs::remove_dir(new.join("enrichments")).unwrap();

    let err = format!(
        "{:#}",
        run_diff(&diff_options(&old, &new, &out)).unwrap_err()
    );

    assert!(err.contains("not a directory"), "got: {err}");
}
