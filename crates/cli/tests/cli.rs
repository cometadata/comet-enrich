use assert_cmd::Command;
use predicates::prelude::*;

fn cli() -> Command {
    Command::cargo_bin("comet-enrich").unwrap()
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
fn every_method_reports_unimplemented() {
    let cases: [(&str, &[&str]); 3] = [
        ("resource-type-general", &["--rules", "r.yaml"]),
        ("affiliations", &["--ror-data", "ror.json"]),
        ("funders", &["--ror-data", "ror.json"]),
    ];
    for (method, extra) in cases {
        let mut args = vec![
            method,
            "-i",
            "in",
            "-o",
            "out.jsonl",
            "--enrichment",
            "e.yaml",
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
fn missing_args_are_rejected() {
    cli().arg("resource-type-general").assert().failure();
}
