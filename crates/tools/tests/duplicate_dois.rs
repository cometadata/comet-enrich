use comet_enrich_test_support::write_gz_lines;
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(root: &Path, output: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tools"))
        .arg("duplicate-dois")
        .arg(root)
        .arg("--output")
        .arg(output)
        .output()
        .unwrap()
}

#[test]
fn reports_all_locations_and_marks_the_selected_one() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("snapshot");
    let output = tmp.path().join("dups.json");
    write_gz_lines(
        &root.join("updated_2026-07/part_0000.jsonl.gz"),
        &[
            r#"{"id":"10.1/dup","attributes":{"updated":"2026-07-18T00:00:00Z"}}"#,
            "",
            r#"{"id":" " ,"attributes":{"doi":"10.1/dup","updated":"2026-07-18T00:00:00Z"}}"#,
        ],
    );
    write_gz_lines(
        &root.join("updated_2026-07/part_0001.jsonl.gz"),
        &[r#"{"id":"10.1/dup","attributes":{"updated":"2026-07-01T00:00:00Z"}}"#],
    );
    write_gz_lines(
        &root.join("updated_2026-08/part_0000.jsonl.gz"),
        &[
            r#"{"id":"10.1/dup","attributes":{"updated":"2026-08-02T00:00:00Z"}}"#,
            r#"{"id":"10.1/unique"}"#,
            "{}",
            "{malformed",
        ],
    );
    write_gz_lines(&root.join("updated_2026-08/part_0001.jsonl.gz"), &[]);

    let result = run(&root, &output);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    // Progress and other log lines share stdout with the summary.
    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("Scanning: complete"), "{stdout}");
    assert!(
        stdout.ends_with(&format!(
            concat!(
                "Scanned records: 6\nRecords without DOI: 1\nMalformed lines: 1\n",
                "Distinct DOIs: 2\nDuplicated DOIs: 1\nExtra occurrences: 3\n",
                "\nDuplicated DOIs by month:\n  updated_2026-07  1\n  updated_2026-08  1\n",
                "Wrote {}\n",
            ),
            output.display()
        )),
        "{stdout}"
    );
    let report: Value = serde_json::from_str(&fs::read_to_string(&output).unwrap()).unwrap();
    assert_eq!(
        report,
        json!({
            "summary": {
                "scanned_records": 6,
                "records_without_doi": 1,
                "malformed_lines": 1,
                "distinct_dois": 2,
                "duplicated_dois": 1,
                "extra_occurrences": 3,
            },
            "by_month": {"updated_2026-07": 1, "updated_2026-08": 1},
            "dois": [{
                "doi": "10.1/dup",
                "occurrences": 4,
                "locations": [
                    {"month": "updated_2026-07", "path": "updated_2026-07/part_0000.jsonl.gz", "line": 1, "updated": "2026-07-18T00:00:00Z", "selected": false},
                    {"month": "updated_2026-07", "path": "updated_2026-07/part_0000.jsonl.gz", "line": 3, "updated": "2026-07-18T00:00:00Z", "selected": false},
                    {"month": "updated_2026-07", "path": "updated_2026-07/part_0001.jsonl.gz", "line": 1, "updated": "2026-07-01T00:00:00Z", "selected": false},
                    {"month": "updated_2026-08", "path": "updated_2026-08/part_0000.jsonl.gz", "line": 1, "updated": "2026-08-02T00:00:00Z", "selected": true},
                ],
            }],
        })
    );
}

#[test]
fn rejects_a_parent_of_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let output = tmp.path().join("dups.json");
    write_gz_lines(
        &tmp.path()
            .join("snapshot/updated_2026-07/part_0000.jsonl.gz"),
        &["{}"],
    );
    let result = run(tmp.path(), &output);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("no updated_YYYY-MM directories"));
    assert!(!output.exists());
}

#[test]
fn refuses_to_write_the_report_over_a_source_part() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("snapshot");
    let part = root.join("updated_2026-07/part_0000.jsonl.gz");
    write_gz_lines(&part, &[r#"{"id":"10.1/a"}"#]);
    let before = fs::read(&part).unwrap();

    let result = run(&root, &part);

    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("--output overlaps SNAPSHOT"), "{stderr}");
    assert_eq!(fs::read(&part).unwrap(), before, "source part was modified");
}
