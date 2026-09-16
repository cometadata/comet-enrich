//! End-to-end tests for the affiliations staged pipeline.

// Brand names such as DataCite are prose, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use comet_enrich_core::{
    EnrichmentAction, HashBits, LookupConfig, MatchService, Report, SCHEMA, run_staged, schema,
};
use comet_enrich_datacite_affiliations::Affiliations;
use comet_enrich_test_support::{
    FakeMatchService, SOURCE_ID, assert_close, assert_record_key, enrichment_template,
    gz_input_fixture, read_enrichment_parts, records_by_doi, run_options,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const MIT_ROR: &str = "https://ror.org/042nb2s44";
const STANFORD_ROR: &str = "https://ror.org/00f54p054";
const HELMHOLTZ_ROR: &str = "https://ror.org/03x1xhg60";
const OXFORD_ROR: &str = "https://ror.org/052gg0110";

fn oxford_with_ror() -> Value {
    json!({"name": "University of Oxford",
        "affiliationIdentifier": OXFORD_ROR,
        "affiliationIdentifierScheme": "ROR",
        "schemeUri": "https://ror.org"})
}

fn input_records() -> Vec<Value> {
    vec![
        json!({"id": "10.x/klemm", "attributes": {"creators": [
            {"name": "Klemm, Anna", "nameType": "Personal", "givenName": "Anna",
             "familyName": "Klemm", "lang": "en",
             "nameIdentifiers": [
                {"nameIdentifier": "0000-0001-2345-6789",
                 "nameIdentifierScheme": "ORCID",
                 "schemeUri": "https://orcid.org"}
             ],
             "affiliation": [oxford_with_ror(), {"name": "MIT"}]}
        ]}}),
        json!({"id": "10.x/contrib", "attributes": {"contributors": [
            {"name": "Editor One", "contributorType": "Editor",
             "affiliation": [{"name": "MIT"}]}
        ]}}),
        json!({"id": "10.x/twins", "attributes": {"creators": [
            {"name": "Twin, Tim", "affiliation": [{"name": "MIT"}]},
            {"name": "Twin, Tim", "affiliation": [{"name": "MIT"}]}
        ]}}),
        json!({"id": "10.x/existing-only", "attributes": {"creators": [
            {"name": "Solo, Sam", "affiliation": [
                {"name": "Helmholtz Zentrum",
                 "affiliationIdentifier": HELMHOLTZ_ROR,
                 "affiliationIdentifierScheme": "ROR"}
            ]}
        ]}}),
        json!({"id": "10.x/unmatched", "attributes": {"creators": [
            {"name": "Doe, John", "affiliation": [{"name": "Unknown Institute"}]}
        ]}}),
        json!({"id": "10.x/string", "attributes": {"creators": [
            {"name": "Roe, Rachel", "affiliation": ["Stanford University"]}
        ]}}),
        json!({"id": "10.x/no-affil", "attributes": {"creators": [
            {"name": "Poe, Pat"}
        ]}}),
        json!({"attributes": {"creators": [
            {"name": "Noe, Nat", "affiliation": [{"name": "MIT"}]}
        ]}}),
    ]
}

fn fake_service() -> Arc<dyn MatchService> {
    let mut map = HashMap::new();
    map.insert("MIT".to_owned(), (MIT_ROR.to_owned(), 0.99));
    map.insert(
        "Stanford University".to_owned(),
        (STANFORD_ROR.to_owned(), 0.95),
    );
    map.insert(
        "Helmholtz Zentrum".to_owned(),
        (HELMHOLTZ_ROR.to_owned(), 0.9),
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
    let opts = run_options(input, output.clone(), 1);

    let method = Affiliations::try_new(cfg()).unwrap();
    let svc = fake_service();
    let template = enrichment_template();
    let validator = schema::compile_str(SCHEMA).unwrap();

    let report = run_staged(
        &method,
        &opts,
        &cfg(),
        &svc,
        &template,
        Some(&validator),
        "affiliation",
        None,
    )
    .unwrap();
    (dir, output, report)
}

#[test]
fn affiliations_staged_pipeline_matches_golden_outcomes() {
    let (_dir, output, report) = run_pipeline();

    for rec in read_enrichment_parts(&output) {
        assert_eq!(rec["sourceId"], json!(SOURCE_ID));
    }

    // Coverage is per extraction unit.
    assert_eq!(report.counters.records_scanned, 8);
    assert_eq!(report.counters.skipped.get("no_affiliations"), Some(&1));
    assert_eq!(report.counters.skipped.get("no_doi"), Some(&1));
    assert_eq!(report.counters.emitted, 4);
    assert_eq!(report.counters.duplicate_records, 0);
    assert_eq!(report.counters.duplicate_enrichments, 1);
    assert_eq!(report.counters.schema_failures, 0);
    assert_eq!(report.coverage.records_in_scope, 7);
    assert_eq!(report.coverage.records_enriched, 4);
    assert_close(report.coverage.coverage_rate, 4.0 / 7.0);

    // MIT is deduplicated; Oxford and Unknown Institute do not match.
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

    let records = records_by_doi(&output);
    assert_eq!(records.len(), 4);
    for rec in records.values() {
        assert_record_key(rec, "affiliations", EnrichmentAction::UpdateChild);
    }
    assert!(!records.contains_key("10.x/existing-only"));
    assert!(!records.contains_key("10.x/unmatched"));

    // Existing Oxford object stays verbatim; MIT is rewritten.
    let klemm = &records["10.x/klemm"];
    assert_eq!(klemm["field"], json!("creators"));
    assert_eq!(klemm["action"], json!("updateChild"));
    assert_eq!(
        klemm["originalValue"]["affiliation"],
        json!([oxford_with_ror(), {"name": "MIT"}])
    );
    assert_eq!(
        klemm["enrichedValue"]["affiliation"],
        json!([oxford_with_ror(), {
            "name": "MIT",
            "affiliationIdentifier": MIT_ROR,
            "affiliationIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org"
        }])
    );
    for value in [&klemm["originalValue"], &klemm["enrichedValue"]] {
        assert_eq!(value["lang"], json!("en"));
        assert_eq!(
            value["nameIdentifiers"][0]["nameIdentifier"],
            json!("0000-0001-2345-6789")
        );
    }

    let contrib = &records["10.x/contrib"];
    assert_eq!(contrib["field"], json!("contributors"));
    assert_eq!(contrib["originalValue"]["contributorType"], json!("Editor"));
    assert_eq!(contrib["enrichedValue"]["contributorType"], json!("Editor"));
    assert_eq!(
        contrib["enrichedValue"]["affiliation"][0]["affiliationIdentifier"],
        json!(MIT_ROR)
    );

    // String affiliation keeps a name-only original value.
    let string = &records["10.x/string"];
    assert_eq!(
        string["originalValue"]["affiliation"],
        json!([{"name": "Stanford University"}])
    );
    assert_eq!(
        string["enrichedValue"]["affiliation"],
        json!([{
            "name": "Stanford University",
            "affiliationIdentifier": STANFORD_ROR,
            "affiliationIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org"
        }])
    );

    // Two identical creators in one record emit once.
    let all = read_enrichment_parts(&output);
    assert_eq!(all.iter().filter(|r| r["doi"] == "10.x/twins").count(), 1);
    let twins = &records["10.x/twins"];
    assert_eq!(
        twins["enrichedValue"]["affiliation"][0]["affiliationIdentifier"],
        json!(MIT_ROR)
    );
}
