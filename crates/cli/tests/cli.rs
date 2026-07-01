use assert_cmd::Command;
use predicates::prelude::*;

fn cli() -> Command {
    Command::cargo_bin("comet-enrich").unwrap()
}

/// Path to a committed provenance file used by CLI integration tests.
fn provenance(method: &str) -> String {
    format!(
        "{}/../../configs/provenance/{method}.yaml",
        env!("CARGO_MANIFEST_DIR")
    )
}

#[test]
fn help_lists_every_method() {
    cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("resource-type"))
        .stdout(predicate::str::contains("affiliations"))
        .stdout(predicate::str::contains("funders"));
}

#[test]
fn a_stage_has_its_own_help() {
    cli()
        .args(["affiliations", "query", "--help"])
        .assert()
        .success();
}

#[test]
fn lookup_methods_report_unimplemented() {
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
fn resource_type_general_is_wired() {
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
fn provenance_is_validated_before_the_method() {
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
fn missing_args_are_rejected() {
    cli().arg("resource-type-general").assert().failure();
}
