//! End-to-end test for running an enrichment method through `core::run`.

use comet_enrichment_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups, RunOptions, run,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::path::Path;

/// Schema file used to validate records written by the test.
const SCHEMA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../configs/schema/enrichment_input_schema.json"
);

/// Test method that rewrites `resourceTypeGeneral` to `"Dataset"`.
struct DatasetTagger;

impl EnrichmentMethod for DatasetTagger {
    type Extraction = EnrichmentParts;
    type Lookup = ();

    fn field(&self) -> &'static str {
        "types"
    }

    fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
        let Some(types) = record.get("attributes").and_then(|a| a.get("types")) else {
            return Extracted::Skip("malformed_types");
        };
        if !types.is_object() {
            return Extracted::Skip("malformed_types");
        }
        let Some(rt) = types.get("resourceType").and_then(Value::as_str) else {
            return Extracted::Skip("no_resource_type");
        };
        if rt.is_empty() {
            return Extracted::Skip("no_resource_type");
        }
        let Some(doi) = record.get("id").and_then(Value::as_str) else {
            return Extracted::Skip("no_doi");
        };
        let mut enriched = types.clone();
        enriched["resourceTypeGeneral"] = json!("Dataset");
        Extracted::Items(vec![EnrichmentParts {
            doi: doi.to_string(),
            action: EnrichmentAction::Update,
            original: types.clone(),
            enriched,
        }])
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        _lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        vec![extraction]
    }
}

const ENRICHMENT_YAML: &str = r#"
contributors:
  - name: COMET
    nameType: Organizational
    contributorType: Producer
resources:
  - relatedIdentifier: "10.82461/bpzr-jd55"
    relatedIdentifierType: DOI
    relationType: IsDocumentedBy
    resourceTypeGeneral: Project
  - relatedIdentifier: "https://huggingface.co/datasets/cometadata/example"
    relatedIdentifierType: URL
    relationType: IsDerivedFrom
    resourceTypeGeneral: Dataset
"#;

#[test]
fn run_drives_transform_end_to_end() {
    let dir = tempfile::tempdir().unwrap();

    // Match the nested layout used by DataCite snapshots.
    let input_dir = dir.path().join("input/updated_2024-01");
    fs::create_dir_all(&input_dir).unwrap();
    let lines = [
        r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Journal article","resourceTypeGeneral":"Text"}}}"#,
        r#"{"id":"10.1/b","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#,
        r#"{"id":"10.1/c","attributes":{"types":{}}}"#, // skipped: no_resource_type
        "",                                             // blank line: ignored, not malformed
        "{not valid json",                              // malformed
    ];
    let file = input_dir.join("part_0000.jsonl.gz");
    let f = fs::File::create(&file).unwrap();
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(lines.join("\n").as_bytes()).unwrap();
    enc.finish().unwrap();

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out.jsonl");

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 1,
        batch_size: 5000,
    };

    // Validate records using the same schema check as a normal run.
    let validator = comet_enrichment_core::schema::compile(Path::new(SCHEMA_PATH)).unwrap();
    let stats = run(&DatasetTagger, &opts, &template, Some(validator)).unwrap();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 3);
    assert_eq!(stats.lines_malformed, 1);
    assert_eq!(stats.emitted, 2);
    assert_eq!(stats.skipped.get("no_resource_type"), Some(&1));

    let body = fs::read_to_string(&output).unwrap();
    let recs: Vec<Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs.len(), 2);
    for rec in &recs {
        assert_eq!(rec["field"], json!("types"));
        assert_eq!(rec["action"], json!("update"));
        assert_eq!(
            rec["enrichedValue"]["resourceTypeGeneral"],
            json!("Dataset")
        );
        assert!(rec["doi"].as_str().unwrap().starts_with("10.1/"));
    }
}
