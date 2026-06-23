//! End-to-end exercise of the resource-type reclassifier through `core::run`.
//!
//! Drives the real [`ResourceTypeGeneral`] method, the ported `reclassification_rules.yaml`,
//! and `enrichment_metadata.yaml` over the same dataset the source tool's golden e2e fixture
//! used (12 DataCite records → 9 emitted). The records are inlined and gzipped at test time,
//! so there is no committed binary fixture; the per-record expectations reproduce the source
//! `golden.jsonl`. Emitted records are schema-validated at the writer boundary.

// Brand names (DataCite, …) recur in the docs as prose, not code identifiers.
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

/// Ported configs and schema, relative to this crate.
const RULES_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../configs/reclassification_rules.yaml");
const ENRICHMENT_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../configs/enrichment_metadata.yaml");
const SCHEMA_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../schema/enrichment_input_schema.json");

/// The source repo's `tests/fixtures/e2e/input` records, verbatim.
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

/// `(doi, enriched resourceTypeGeneral)` for the 9 records the source `golden.jsonl` emits.
/// Records 4 (out-of-scope), 5 (redundant), and 6 (no-match) are skipped.
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
    let dir = tempfile::tempdir().unwrap();

    // Input shard, nested like the real data layout, gzip-compressed.
    let shard_dir = dir.path().join("input/updated_2024-01");
    fs::create_dir_all(&shard_dir).unwrap();
    let shard = shard_dir.join("part_0000.jsonl.gz");
    let f = fs::File::create(&shard).unwrap();
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(INPUT_RECORDS.join("\n").as_bytes()).unwrap();
    enc.finish().unwrap();

    let output = dir.path().join("out.jsonl");
    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        enrichment: PathBuf::from(ENRICHMENT_PATH),
        threads: 1,
        batch_size: 5000,
    };

    let method = ResourceTypeGeneral::try_new(Config { rules: PathBuf::from(RULES_PATH) }).unwrap();
    let validator = comet_enrichment_core::schema::compile(Path::new(SCHEMA_PATH)).unwrap();
    let stats = run(&method, &opts, Some(validator)).unwrap();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 12);
    assert_eq!(stats.lines_malformed, 0);
    assert_eq!(stats.emitted, 9);
    assert_eq!(stats.skipped.get("not_in_scope"), Some(&1));
    assert_eq!(stats.skipped.get("redundant"), Some(&1));
    assert_eq!(stats.skipped.get("no_match"), Some(&1));

    let body = fs::read_to_string(&output).unwrap();
    let recs: Vec<Value> = body.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(recs.len(), 9);

    let mut emitted: HashMap<String, String> = HashMap::new();
    for rec in &recs {
        assert_eq!(rec["field"], json!("types"));
        assert_eq!(rec["action"], json!("update"));
        let doi = rec["doi"].as_str().unwrap().to_string();
        let rtg = rec["enrichedValue"]["resourceTypeGeneral"].as_str().unwrap().to_string();
        emitted.insert(doi, rtg);
    }

    assert_eq!(emitted.len(), EXPECTED_EMITTED.len());
    for (doi, rtg) in EXPECTED_EMITTED {
        assert_eq!(emitted.get(*doi).map(String::as_str), Some(*rtg), "doi {doi}");
    }
}
