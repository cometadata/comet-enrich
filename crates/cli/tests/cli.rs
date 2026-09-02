use assert_cmd::Command;
use comet_enrich_test_support::{SOURCE_ID, config_path, gz_input_fixture, read_enrichment_parts};
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
