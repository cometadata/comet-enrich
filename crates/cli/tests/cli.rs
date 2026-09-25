use assert_cmd::Command;
use comet_enrich_core::EnrichmentAction;
use comet_enrich_test_support::{
    METHOD, SOURCE_ID, config_path, enrichment_record, gz_input_fixture, read_enrichment_parts,
    write_run_dir,
};
use predicates::prelude::*;
use serde_json::{Value, json};
use std::fs;

fn cli() -> Command {
    Command::cargo_bin("comet-enrich").unwrap()
}

fn rules() -> String {
    config_path("reclassification_rules.yaml")
        .to_string_lossy()
        .into_owned()
}

#[test]
fn cli_help_lists_every_method() {
    cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("resource-type"))
        .stdout(predicate::str::contains("affiliations"))
        .stdout(predicate::str::contains("funders"));
}

#[test]
fn cli_completions_emit_shell_scripts() {
    cli()
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("_comet-enrich"))
        .stdout(predicate::str::contains("complete"));
    cli()
        .args(["completions", "zsh"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("#compdef comet-enrich"));
    cli()
        .args(["completions", "fish"])
        .assert()
        .success()
        .stdout(predicate::str::contains("complete -c comet-enrich"))
        .stdout(predicate::str::contains("affiliations"));
}

#[test]
fn cli_completions_help_shows_install_instructions() {
    cli()
        .args(["completions", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "source <(comet-enrich completions bash)",
        ))
        .stdout(predicate::str::contains(
            "~/.config/fish/completions/comet-enrich.fish",
        ));
}

#[test]
fn cli_stage_option_listed() {
    cli()
        .args(["affiliations", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--stage <STAGE>"))
        .stdout(predicate::str::contains("query"));
}

#[test]
fn cli_funders_validates_ror_file() {
    cli()
        .args([
            "funders",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--source-id",
            SOURCE_ID,
            "--ror-file",
            "ror.json",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ror.json"))
        .stderr(predicate::str::contains("not yet implemented").not());
}

#[test]
fn cli_affiliations_constructs_and_validates_input() {
    cli()
        .args([
            "affiliations",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--source-id",
            SOURCE_ID,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("input path is not a directory"));
}

#[test]
fn cli_resource_type_general_loads_rules() {
    cli()
        .args([
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--source-id",
            SOURCE_ID,
            "--rules",
            "r.yaml",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("reading r.yaml"))
        .stderr(predicate::str::contains("not yet implemented").not());
}

#[test]
fn cli_resource_type_general_runs_and_writes_manifest() {
    let (_dir, input, output) = gz_input_fixture(&[json!({
        "id": "10.x/1",
        "attributes": {
            "types": {
                "resourceType": "Dataset",
                "resourceTypeGeneral": "Other"
            }
        }
    })]);

    let input = input.to_string_lossy().into_owned();
    let output_arg = output.to_string_lossy().into_owned();
    let rules = rules();
    cli()
        .args([
            "resource-type-general",
            "-i",
            input.as_str(),
            "-o",
            output_arg.as_str(),
            "--source-id",
            SOURCE_ID,
            "--rules",
            rules.as_str(),
            "--source-release-date",
            "datacite=2024-01-01",
            "--threads",
            "1",
            "--batch-size",
            "100",
        ])
        .assert()
        .success();

    let records = read_enrichment_parts(&output);
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]["enrichedValue"]["resourceTypeGeneral"],
        json!("Dataset")
    );
    assert_eq!(records[0]["sourceId"], json!(SOURCE_ID));

    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["exit_status"], json!("success"));
    assert_eq!(manifest["source_id"], json!(SOURCE_ID));
    assert_eq!(
        manifest["sources"]["datacite"]["release_date"],
        json!("2024-01-01")
    );
    assert_eq!(manifest["report"]["counters"]["records_scanned"], json!(1));
    assert_eq!(manifest["report"]["counters"]["emitted"], json!(1));
}

#[test]
fn cli_rejects_malformed_source_id_at_parse_time() {
    cli()
        .args([
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--source-id",
            "not-a-doi",
            "--rules",
            "r.yaml",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--source-id"))
        .stderr(predicate::str::contains("not-a-doi"));
}

#[test]
fn cli_missing_args_are_rejected() {
    cli().arg("resource-type-general").assert().failure();
}

fn diff_record(doi: &str) -> Value {
    enrichment_record(
        METHOD,
        doi,
        "types",
        EnrichmentAction::Update,
        &json!({"resourceTypeGeneral": "Text"}),
        &json!({"resourceTypeGeneral": "Dataset"}),
    )
}

/// Write a complete run: one part per record slice plus a successful manifest.
fn write_diff_side(dir: &std::path::Path, parts: &[&[Value]]) {
    write_run_dir(dir, parts, Some("success"));
}

fn diff_args(old: &std::path::Path, new: &std::path::Path, out: &std::path::Path) -> Vec<String> {
    [
        "diff",
        "--old",
        old.to_str().unwrap(),
        "--new",
        new.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]
    .map(str::to_owned)
    .to_vec()
}

#[test]
fn cli_diff_reports_progress_and_writes_events() {
    let tmp = tempfile::tempdir().unwrap();
    let old = tmp.path().join("old");
    let new = tmp.path().join("new");
    let out = tmp.path().join("out");
    write_diff_side(
        &old,
        &[
            &[diff_record("10.1/kept")],
            &[diff_record("10.1/retracted")],
        ],
    );
    write_diff_side(
        &new,
        &[&[diff_record("10.1/kept"), diff_record("10.1/asserted")]],
    );

    let result = cli().args(diff_args(&old, &new, &out)).assert().success();

    let output = String::from_utf8_lossy(&result.get_output().stdout);
    for phase in [
        "Indexing old",
        "Comparing new",
        "Finding retractions",
        "Finalizing output",
    ] {
        assert!(output.contains(phase), "{output}");
    }
    assert!(output.contains("elapsed"), "{output}");
    let events = read_enrichment_parts(&out);
    assert_eq!(events.len(), 2);
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    assert!(manifest.get("exit_status").is_none());
    assert_eq!(manifest["counters"]["asserted"], json!(1));
    assert_eq!(manifest["counters"]["retracted"], json!(1));
    assert_eq!(manifest["counters"]["unchanged"], json!(1));
    assert_eq!(manifest["old"]["records"], json!(2));
    assert_eq!(manifest["new"]["records"], json!(2));
    assert!(manifest["counters"].get("old_records").is_none());
}

#[test]
fn cli_diff_refuses_a_side_with_a_repeated_content_key() {
    let tmp = tempfile::tempdir().unwrap();
    let old = tmp.path().join("old");
    let new = tmp.path().join("new");
    let out = tmp.path().join("out");
    let rec = diff_record("10.1/twice");
    let twice = std::slice::from_ref(&rec);
    write_diff_side(&old, &[twice, twice]);
    write_diff_side(&new, &[twice]);

    cli()
        .args(diff_args(&old, &new, &out))
        .assert()
        .failure()
        .stderr(predicate::str::contains("appears twice in the old release"))
        .stderr(predicate::str::contains("10.1/twice"))
        .stderr(predicate::str::contains("part_0001.jsonl.gz"));
    assert!(!out.join("manifest.json").exists());
}

#[test]
fn cli_partial_run_writes_manifest_and_exits_non_zero() {
    let (_dir, input, output) = gz_input_fixture(&[json!({
        "id": "10.x/1",
        "attributes": {
            "types": {
                "resourceType": "Dataset",
                "resourceTypeGeneral": "Other"
            }
        }
    })]);
    // A second input part that is not valid gzip is counted as a failed file.
    fs::write(input.join("zz_corrupt.jsonl.gz"), b"this is not gzip").unwrap();

    let input_arg = input.to_string_lossy().into_owned();
    let output_arg = output.to_string_lossy().into_owned();
    let rules = rules();
    cli()
        .args([
            "resource-type-general",
            "-i",
            input_arg.as_str(),
            "-o",
            output_arg.as_str(),
            "--source-id",
            SOURCE_ID,
            "--rules",
            rules.as_str(),
            "--source-release-date",
            "datacite=2024-01-01",
            "--threads",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("partial"));

    // The good part and the manifest are still written for debugging.
    let records = read_enrichment_parts(&output);
    assert_eq!(records.len(), 1);
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["exit_status"], json!("partial"));
    assert_eq!(manifest["report"]["counters"]["files_failed"], json!(1));
}

#[test]
fn cli_standalone_extract_exits_zero_without_a_manifest() {
    let (_tmp, input, output) =
        gz_input_fixture(&[json!({ "id": "10.1/a", "attributes": { "name": "MIT" } })]);

    // Extract never contacts the match service, so a closed port is fine.
    cli()
        .args([
            "affiliations",
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--source-id",
            SOURCE_ID,
            "--stage",
            "extract",
            "--ror-service-url",
            "http://127.0.0.1:9",
        ])
        .assert()
        .success();

    assert!(output.join(".work/extract.done").exists());
    assert!(!output.join("manifest.json").exists());
}
