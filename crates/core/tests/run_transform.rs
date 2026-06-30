//! End-to-end test for running an enrichment method through `core::run`.

use comet_enrichment_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups, RunOptions, run,
};
use comet_test_support::{config_path, read_enrichment_parts, write_gz_lines};
use serde_json::{Value, json};
use std::fs;

/// Test method that rewrites `resourceTypeGeneral` to `"Dataset"`.
struct DatasetTagger;

impl EnrichmentMethod for DatasetTagger {
    type Extraction = EnrichmentParts;
    type Lookup = ();

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
            field: "types",
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
    let lines = [
        r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Journal article","resourceTypeGeneral":"Text"}}}"#,
        r#"{"id":"10.1/b","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#,
        r#"{"id":"10.1/c","attributes":{"types":{}}}"#, // skipped: no_resource_type
        "",                                             // blank line: ignored, not malformed
        "{not valid json",                              // malformed
    ];
    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &lines,
    );

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out");

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 1,
        batch_size: 100,
    };

    // Validate records using the same schema check as a normal run.
    let validator =
        comet_enrichment_core::schema::compile(&config_path("schema/enrichment_input_schema.json"))
            .unwrap();
    let stats = run(&DatasetTagger, &opts, &template, Some(&validator)).unwrap();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 3);
    assert_eq!(stats.lines_malformed, 1);
    assert_eq!(stats.emitted, 2);
    assert_eq!(stats.schema_failures, 0);
    assert_eq!(stats.skipped.get("no_resource_type"), Some(&1));

    let recs = read_enrichment_parts(&output);
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

#[test]
fn write_failure_fails_the_run() {
    // A failed write must abort the run rather than being logged and dropped. We force
    // the divert path to fail by putting a directory where the failures file should go:
    // opening it for writing then errors the moment a record is diverted.
    let dir = tempfile::tempdir().unwrap();

    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();

    let output = dir.path().join("out");
    // Block the failures file: a directory here cannot be opened for writing.
    fs::create_dir_all(output.join(comet_enrichment_core::ENRICHMENTS_FAILED_FILE)).unwrap();

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 1,
        batch_size: 100,
    };

    // A validator that rejects every record, so the one emitted record is diverted.
    let validator = comet_enrichment_core::schema::compile_str(
        r#"{"type":"object","required":["__never_present__"]}"#,
    )
    .unwrap();
    let result = run(&DatasetTagger, &opts, &template, Some(&validator));
    assert!(result.is_err(), "write failure should fail the run");
}
