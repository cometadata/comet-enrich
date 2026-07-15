//! End-to-end tests for the funders staged pipeline.

// Brand names such as DataCite are prose, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use comet_enrich_core::{
    HashBits, HashInfo, LookupConfig, Manifest, MatchService, Report, RunMeta, RunOptions,
    SourceRelease, load_template, run_staged, schema,
};
use comet_enrich_datacite_funders::{Config, Funders};
use comet_enrich_test_support::{
    FakeMatchService, assert_close, config_path, gz_input_fixture, read_enrichment_parts,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const NSF_ROR: &str = "https://ror.org/021nxhr62";
const DOE_ROR: &str = "https://ror.org/01bj3aw27";

const MINIMAL_ROR_DUMP: &str = r#"[
  {"id": "https://ror.org/021nxhr62",
   "names": [{"value": "National Science Foundation", "types": ["ror_display"]}],
   "external_ids": [{"type": "fundref", "all": ["100000001"], "preferred": "100000001"}]}
]"#;

fn input_records() -> Vec<Value> {
    vec![
        json!({"id": "10.x/matched", "attributes": {"fundingReferences": [
            {"funderName": "NSF", "awardNumber": "ABC-123", "awardTitle": "A Grant",
             "awardUri": "https://example.com/grant", "weirdKey": "kept"}
        ]}}),
        json!({"id": "10.x/asserted", "attributes": {"fundingReferences": [
            {"funderName": "NSF", "funderIdentifier": "021nxhr62",
             "funderIdentifierType": "ROR"}
        ]}}),
        json!({"id": "10.x/crosswalk", "attributes": {"fundingReferences": [
            {"funderName": "National Science Foundation",
             "funderIdentifier": "https://doi.org/10.13039/100000001",
             "funderIdentifierType": "Crossref Funder ID"}
        ]}}),
        json!({"id": "10.x/unmapped", "attributes": {"fundingReferences": [
            {"funderName": "Department of Energy",
             "funderIdentifier": "999999999",
             "funderIdentifierType": "Crossref Funder ID"}
        ]}}),
        json!({"id": "10.x/unmatched", "attributes": {"fundingReferences": [
            {"funderName": "Unknown Funder"}
        ]}}),
        json!({"id": "10.x/multi", "attributes": {"fundingReferences": [
            {"funderName": "NSF"},
            {"funderName": "Mystery Trust"}
        ]}}),
        json!({"id": "10.x/no-refs", "attributes": {"titles": [{"title": "No funding"}]}}),
        json!({"attributes": {"fundingReferences": [{"funderName": "NSF"}]}}),
    ]
}

fn fake_service() -> Arc<dyn MatchService> {
    let mut map = HashMap::new();
    map.insert("NSF".to_owned(), (NSF_ROR.to_owned(), 0.99));
    map.insert(
        "National Science Foundation".to_owned(),
        (NSF_ROR.to_owned(), 0.97),
    );
    map.insert(
        "Department of Energy".to_owned(),
        (DOE_ROR.to_owned(), 0.95),
    );
    Arc::new(FakeMatchService::new(map))
}

fn cfg() -> LookupConfig {
    LookupConfig {
        ror_service_url: "http://unused".to_owned(),
        ror_batch_size: 2,
        ror_concurrency: 2,
        ror_timeout: 30,
        hash_bits: HashBits::Bits64,
        from_scratch: true,
    }
}

fn run_pipeline() -> (tempfile::TempDir, PathBuf, Report) {
    let (dir, input, output) = gz_input_fixture(&input_records());
    let ror_file = dir.path().join("ror.json");
    fs::write(&ror_file, MINIMAL_ROR_DUMP).unwrap();
    let opts = RunOptions {
        input,
        output: output.clone(),
        threads: 1,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };

    let method = Funders::try_new(Config {
        lookup: cfg(),
        ror_file,
        legacy_ror_resolution: false,
    })
    .unwrap();
    let svc = fake_service();
    let template = load_template(config_path("provenance/funders.yaml")).unwrap();
    let validator = schema::compile(&config_path("schema/enrichment_input_schema.json")).unwrap();

    let report = run_staged(
        &method,
        &opts,
        &cfg(),
        &svc,
        &template,
        Some(&validator),
        "funder",
        None,
    )
    .unwrap();
    (dir, output, report)
}

fn records_by_doi(output: &Path) -> HashMap<String, Value> {
    read_enrichment_parts(output)
        .into_iter()
        .map(|rec| (rec["doi"].as_str().unwrap().to_owned(), rec))
        .collect()
}

#[test]
fn funders_staged_pipeline_matches_golden_outcomes() {
    let (_dir, output, report) = run_pipeline();

    // Coverage is per funding reference.
    assert_eq!(report.counters.records_scanned, 8);
    assert_eq!(
        report.counters.skipped.get("no_funding_references"),
        Some(&1)
    );
    assert_eq!(report.counters.skipped.get("no_doi"), Some(&1));
    assert_eq!(report.counters.emitted, 3);
    assert_eq!(report.counters.schema_failures, 0);
    assert_eq!(report.coverage.records_in_scope, 7);
    assert_eq!(report.coverage.records_enriched, 3);
    assert_close(report.coverage.coverage_rate, 3.0 / 7.0);

    // NSF is deduplicated; excluded names are still queried.
    let m = report.match_.expect("match block present");
    assert_eq!(m.unique_inputs, 5);
    assert_eq!(m.matched, 3);
    assert_close(m.match_rate, 3.0 / 5.0);
    assert_eq!(m.failure_taxonomy.no_match, 2);
    assert_eq!(m.failure_taxonomy.error, 0);
    assert_eq!(
        m.confidence_histogram.iter().map(|b| b.count).sum::<u64>(),
        3
    );

    // Resume artifacts are written.
    let work = output.join(".work");
    for artifact in [
        "extractions/part_0000.jsonl",
        "inputs.jsonl",
        "lookups.jsonl",
        "lookups.failed.jsonl",
        "extract.done",
        "query.done",
        "reconcile.done",
    ] {
        assert!(
            work.join(artifact).exists(),
            "missing work artifact: {artifact}"
        );
    }
    assert_eq!(
        fs::read_to_string(work.join("hash.bits")).unwrap(),
        "xxh3-64"
    );

    let records = records_by_doi(&output);
    assert_eq!(records.len(), 3);
    // Already-resolved and unmatched references do not emit.
    assert!(!records.contains_key("10.x/asserted"));
    assert!(!records.contains_key("10.x/crosswalk"));
    assert!(!records.contains_key("10.x/unmatched"));

    // Matched references keep original fields and gain ROR keys.
    let matched = &records["10.x/matched"];
    assert_eq!(matched["field"], json!("fundingReferences"));
    assert_eq!(matched["action"], json!("updateChild"));
    assert_eq!(
        matched["originalValue"],
        json!({"funderName": "NSF", "awardNumber": "ABC-123", "awardTitle": "A Grant",
               "awardUri": "https://example.com/grant", "weirdKey": "kept"})
    );
    assert_eq!(
        matched["enrichedValue"],
        json!({"funderName": "NSF", "awardNumber": "ABC-123", "awardTitle": "A Grant",
               "awardUri": "https://example.com/grant", "weirdKey": "kept",
               "funderIdentifier": NSF_ROR, "funderIdentifierType": "ROR",
               "schemeUri": "https://ror.org"})
    );

    // Unmapped Crossref IDs can be replaced by a name match.
    let unmapped = &records["10.x/unmapped"];
    assert_eq!(
        unmapped["originalValue"]["funderIdentifier"],
        json!("999999999")
    );
    assert_eq!(
        unmapped["originalValue"]["funderIdentifierType"],
        json!("Crossref Funder ID")
    );
    assert_eq!(
        unmapped["enrichedValue"]["funderIdentifier"],
        json!(DOE_ROR)
    );
    assert_eq!(
        unmapped["enrichedValue"]["funderIdentifierType"],
        json!("ROR")
    );
    assert_eq!(
        unmapped["enrichedValue"]["schemeUri"],
        json!("https://ror.org")
    );

    // Only the matched reference emits.
    let multi = &records["10.x/multi"];
    assert_eq!(multi["originalValue"], json!({"funderName": "NSF"}));
    assert_eq!(multi["enrichedValue"]["funderName"], json!("NSF"));
    assert_eq!(multi["enrichedValue"]["funderIdentifier"], json!(NSF_ROR));
}

#[test]
fn funders_pipeline_writes_lookup_manifest() {
    let (_dir, output, report) = run_pipeline();

    let mut sources = BTreeMap::new();
    sources.insert(
        "datacite".to_owned(),
        SourceRelease {
            release_date: "2024-01-01".to_owned(),
        },
    );
    let meta = RunMeta {
        method_name: "funders".to_owned(),
        method_version: env!("CARGO_PKG_VERSION"),
        sources,
    };
    Manifest::from_report(&meta, "success", report, HashInfo::from(HashBits::Bits64))
        .write(&output)
        .unwrap();

    let raw = fs::read_to_string(output.join("manifest.json")).unwrap();
    let m: Value = serde_json::from_str(&raw).unwrap();

    assert_eq!(m["schema_version"], json!(1));
    assert_eq!(m["method"]["name"], json!("funders"));
    assert_eq!(m["hash"]["algorithm"], json!("xxh3"));
    assert_eq!(m["hash"]["bits"], json!(64));
    assert_eq!(m["exit_status"], json!("success"));
    assert_eq!(m["report"]["match"]["unique_inputs"], json!(5));
    assert_eq!(m["report"]["match"]["matched"], json!(3));
    assert_eq!(m["report"]["validation"]["emitted"], json!(3));
    assert_eq!(m["report"]["validation"]["schema_failures"], json!(0));
}
