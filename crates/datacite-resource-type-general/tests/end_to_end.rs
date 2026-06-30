//! End-to-end tests for the DataCite `resourceTypeGeneral` reclassifier.
//!
//! These tests run the real [`ResourceTypeGeneral`] method through `core::run`,
//! using the project rules, provenance template, and output schema. The input
//! records are inlined and gzipped during the test so the fixture stays readable
//! without committing a binary data file.

// Brand names such as DataCite are prose, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use comet_enrich_datacite_resource_type_general::{Config, ResourceTypeGeneral};
use comet_enrichment_core::{
    Manifest, RunMeta, RunOptions, RunStats, SourceRelease, StageTimings, run,
};
use comet_test_support::{assert_close, config_path, read_enrichment_parts, write_gz_lines};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;

/// Skip reasons the reclassifier uses for records its extractor did not select.
const OUT_OF_SCOPE: &[&str] = &["not_in_scope", "malformed_types"];

/// Representative DataCite records covering matches, skips, typo correction,
/// camelCase handling, and compact/space-separated type names.
const INPUT_RECORDS: &[&str] = &[
    r#"{"id":"10.x/1","attributes":{"types":{"resourceType":"Journal article","resourceTypeGeneral":"Text","bibtex":"article","schemaOrg":"ScholarlyArticle"}}}"#,
    r#"{"id":"10.x/2","attributes":{"types":{"resourceType":"Dataset","resourceTypeGeneral":"Other","ris":"DATA"}}}"#,
    r#"{"id":"10.x/3","attributes":{"types":{"resourceType":"Software","resourceTypeGeneral":null}}}"#,
    r#"{"id":"10.x/4","attributes":{"types":{"resourceType":"Image","resourceTypeGeneral":"Image"}}}"#,
    r#"{"id":"10.x/5","attributes":{"types":{"resourceType":"Text","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/6","attributes":{"types":{"resourceType":"Completely unrelated nonsense","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/7","attributes":{"types":{"resourceType":"Sofware","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/8","attributes":{"types":{"resourceType":"bookChapter","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/9","attributes":{"types":{"resourceType":"Workflow","resourceTypeGeneral":"Other"}}}"#,
    r#"{"id":"10.x/10","attributes":{"types":{"resourceType":"Preprint","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/11","attributes":{"types":{"resourceType":"Conference Paper","resourceTypeGeneral":"Text"}}}"#,
    r#"{"id":"10.x/12","attributes":{"types":{"resourceType":"ConferencePaper","resourceTypeGeneral":"Text"}}}"#,
];

/// Expected `(doi, resourceTypeGeneral)` updates.
///
/// Records 4, 5, and 6 are intentionally skipped: one is out of scope, one is
/// redundant, and one has no match.
const EXPECTED_EMITTED: &[(&str, &str)] = &[
    ("10.x/1", "JournalArticle"),
    ("10.x/2", "Dataset"),
    ("10.x/3", "Software"),
    ("10.x/7", "Software"),
    ("10.x/8", "BookChapter"),
    ("10.x/9", "Workflow"),
    ("10.x/10", "Preprint"),
    ("10.x/11", "ConferencePaper"),
    ("10.x/12", "ConferencePaper"),
];

/// Run the real reclassifier over the inlined fixture.
///
/// Returns the temp dir (kept alive by the caller), the output directory, and the
/// run counters.
fn run_reclassifier() -> (tempfile::TempDir, PathBuf, RunStats) {
    let dir = tempfile::tempdir().unwrap();
    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        INPUT_RECORDS,
    );

    let output = dir.path().join("out");
    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 1,
        batch_size: 100,
    };

    let template =
        comet_enrichment_core::load_template(config_path("provenance/resource_type_general.yaml"))
            .unwrap();
    let method = ResourceTypeGeneral::try_new(Config {
        rules: config_path("reclassification_rules.yaml"),
    })
    .unwrap();
    let validator =
        comet_enrichment_core::schema::compile(&config_path("schema/enrichment_input_schema.json"))
            .unwrap();
    let stats = run(&method, &opts, &template, Some(&validator)).unwrap();
    (dir, output, stats)
}

#[test]
fn reclassifier_matches_golden_outcomes() {
    // Checks the full run path: read gzipped input, apply the real method,
    // validate writer output against the schema, and verify emitted updates and
    // skip counts.
    let (_dir, output, stats) = run_reclassifier();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 12);
    assert_eq!(stats.lines_malformed, 0);
    assert_eq!(stats.emitted, 9);
    assert_eq!(stats.schema_failures, 0);
    assert_eq!(stats.skipped.get("not_in_scope"), Some(&1));
    assert_eq!(stats.skipped.get("redundant"), Some(&1));
    assert_eq!(stats.skipped.get("no_match"), Some(&1));

    let recs = read_enrichment_parts(&output);
    assert_eq!(recs.len(), 9);

    let mut emitted: HashMap<String, String> = HashMap::new();
    for rec in &recs {
        assert_eq!(rec["field"], json!("types"));
        assert_eq!(rec["action"], json!("update"));
        let doi = rec["doi"].as_str().unwrap().to_string();
        let rtg = rec["enrichedValue"]["resourceTypeGeneral"]
            .as_str()
            .unwrap()
            .to_string();
        emitted.insert(doi, rtg);
    }

    assert_eq!(emitted.len(), EXPECTED_EMITTED.len());
    for (doi, rtg) in EXPECTED_EMITTED {
        assert_eq!(
            emitted.get(*doi).map(String::as_str),
            Some(*rtg),
            "doi {doi}"
        );
    }
}

#[test]
fn reclassifier_writes_run_manifest() {
    // The transform path produces a manifest.json with the stats nested under
    // `report`, no `match` block, and no content hash / provenance fingerprint.
    let (_dir, output, stats) = run_reclassifier();

    let mut sources = BTreeMap::new();
    sources.insert(
        "datacite".to_owned(),
        SourceRelease {
            release_date: "2024-01-01".to_owned(),
        },
    );
    let meta = RunMeta {
        method_name: "resource-type-general".to_owned(),
        method_version: env!("CARGO_PKG_VERSION"),
        sources,
    };
    let timings = StageTimings {
        total: Some(1),
        ..StageTimings::default()
    };
    Manifest::build(&stats, &meta, OUT_OF_SCOPE, &timings, "success")
        .write(&output)
        .unwrap();

    let raw = fs::read_to_string(output.join("manifest.json")).unwrap();
    let m: Value = serde_json::from_str(&raw).unwrap();

    // Envelope.
    assert_eq!(m["schema_version"], json!(1));
    assert_eq!(m["method"]["name"], json!("resource-type-general"));
    assert_eq!(m["method"]["version"], json!(env!("CARGO_PKG_VERSION")));
    assert_eq!(
        m["sources"]["datacite"]["release_date"],
        json!("2024-01-01")
    );
    assert_eq!(m["exit_status"], json!("success"));
    assert_eq!(m["artifact_paths"]["enrichments"], json!("enrichments/"));
    assert_eq!(
        m["artifact_paths"]["enrichments_failed"],
        json!("enrichments.failed.jsonl")
    );

    // Stats block.
    let report = &m["report"];
    assert_eq!(report["counters"]["records_scanned"], json!(12));
    assert_eq!(report["counters"]["emitted"], json!(9));
    assert_eq!(report["counters"]["skipped"]["not_in_scope"], json!(1));
    // records_in_scope = 12 scanned - 1 not_in_scope - 0 malformed_types.
    assert_eq!(report["coverage"]["records_in_scope"], json!(11));
    assert_eq!(report["coverage"]["records_enriched"], json!(9));
    let rate = report["coverage"]["coverage_rate"].as_f64().unwrap();
    assert_close(rate, 9.0 / 11.0);
    assert_eq!(report["validation"]["emitted"], json!(9));
    assert_eq!(report["validation"]["schema_failures"], json!(0));
    assert!(report["stage_timings_ms"]["total"].is_u64());

    // No match block on the transform path; no hash fields yet.
    assert!(report.get("match").is_none());
    assert!(!raw.contains("content_hash"));
    assert!(!raw.contains("provenance_fingerprint"));
}
