//! End-to-end tests for the DataCite `resourceTypeGeneral` reclassifier.
//!
//! These tests run the real [`ResourceTypeGeneral`] method through `core::run`,
//! using the project rules, provenance template, and output schema. The input
//! records are inlined and gzipped during the test so the fixture stays readable
//! without committing a binary data file.

// Brand names such as DataCite are prose, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use comet_enrich_datacite_resource_type_general::{Config, ResourceTypeGeneral};
use comet_enrichment_core::{RunOptions, run};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Method rules, provenance template, and schema used by the real pipeline.
const RULES_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../configs/reclassification_rules.yaml"
);
const ENRICHMENT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../configs/provenance/resource_type_general.yaml"
);
const SCHEMA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../configs/schema/enrichment_input_schema.json"
);

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

#[test]
fn reclassifier_matches_golden_outcomes() {
    // Checks the full run path: read gzipped input, apply the real method,
    // validate writer output against the schema, and verify emitted updates and
    // skip counts.
    let dir = tempfile::tempdir().unwrap();

    let input_dir = dir.path().join("input/updated_2024-01");
    fs::create_dir_all(&input_dir).unwrap();
    let file = input_dir.join("part_0000.jsonl.gz");
    let f = fs::File::create(&file).unwrap();
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(INPUT_RECORDS.join("\n").as_bytes()).unwrap();
    enc.finish().unwrap();

    let output = dir.path().join("out");
    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 1,
        batch_size: 5000,
    };

    let template = comet_enrichment_core::load_template(ENRICHMENT_PATH).unwrap();
    let method = ResourceTypeGeneral::try_new(Config {
        rules: PathBuf::from(RULES_PATH),
    })
    .unwrap();
    let validator = comet_enrichment_core::schema::compile(Path::new(SCHEMA_PATH)).unwrap();
    let stats = run(&method, &opts, &template, Some(validator)).unwrap();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 12);
    assert_eq!(stats.lines_malformed, 0);
    assert_eq!(stats.emitted, 9);
    assert_eq!(stats.schema_failures, 0);
    assert_eq!(stats.skipped.get("not_in_scope"), Some(&1));
    assert_eq!(stats.skipped.get("redundant"), Some(&1));
    assert_eq!(stats.skipped.get("no_match"), Some(&1));

    let body = fs::read_to_string(output.join(comet_enrichment_core::ENRICHMENTS_FILE)).unwrap();
    let recs: Vec<Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
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
