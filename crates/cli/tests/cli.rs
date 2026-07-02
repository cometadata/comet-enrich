use assert_cmd::Command;
use comet_test_support::{config_path, gz_input_fixture, read_enrichment_parts};
use predicates::prelude::*;
use serde_json::{Value, json};
use std::fs;

fn cli() -> Command {
    Command::cargo_bin("comet-enrich").unwrap()
}

/// Path to a committed provenance file used by CLI integration tests.
fn provenance(method: &str) -> String {
    config_path(&format!("provenance/{method}.yaml"))
        .to_string_lossy()
        .into_owned()
}

/// Path to a committed rules file used by CLI integration tests.
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
    // Bash defines and registers a completion function.
    cli()
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("_comet-enrich"))
        .stdout(predicate::str::contains("complete"));
    // Zsh scripts start with the compdef header.
    cli()
        .args(["completions", "zsh"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("#compdef comet-enrich"));
    // Fish registers per-command completions, including the subcommands.
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
fn cli_stage_help_displays() {
    cli()
        .args(["affiliations", "query", "--help"])
        .assert()
        .success();
}

#[test]
fn cli_lookup_methods_report_unimplemented() {
    // Lookup methods parse successfully before failing in their constructors.
    let cases: [(&str, &[&str]); 2] = [
        ("affiliations", &["--ror-file", "ror.json"]),
        ("funders", &["--ror-file", "ror.json"]),
    ];
    for (method, extra) in cases {
        let prov = provenance(method);
        let mut args = vec![
            method,
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            prov.as_str(),
        ];
        args.extend_from_slice(extra);
        cli()
            .args(&args)
            .assert()
            .failure()
            .stderr(predicate::str::contains(format!(
                "{method}: not yet implemented"
            )));
    }
}

#[test]
fn cli_resource_type_general_loads_rules() {
    // `resource-type-general` should load its rules file before failing.
    cli()
        .args([
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            provenance("resource_type_general").as_str(),
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
    let provenance = provenance("resource_type_general");
    let rules = rules();
    cli()
        .args([
            "resource-type-general",
            "-i",
            input.as_str(),
            "-o",
            output_arg.as_str(),
            "--provenance",
            provenance.as_str(),
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

    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["exit_status"], json!("success"));
    assert_eq!(
        manifest["sources"]["datacite"]["release_date"],
        json!("2024-01-01")
    );
    assert_eq!(manifest["report"]["counters"]["records_scanned"], json!(1));
    assert_eq!(manifest["report"]["counters"]["emitted"], json!(1));
}

#[test]
fn cli_validates_provenance_before_method_files() {
    // Provenance errors should be reported before method-specific files are read.
    cli()
        .args([
            "resource-type-general",
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--provenance",
            "nope.yaml",
            "--rules",
            "r.yaml",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("nope.yaml"))
        .stderr(predicate::str::contains("reading r.yaml").not());
}

#[test]
fn cli_missing_args_are_rejected() {
    cli().arg("resource-type-general").assert().failure();
}
