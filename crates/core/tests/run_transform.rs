//! End-to-end exercise of the transform path through `core::run`.
//!
//! The only in-repo test of the full `EnrichmentMethod` pipeline: a small pure-transform
//! mock method runs over a gzipped JSONL shard, and we assert on the emitted records (also
//! schema-validated at the writer boundary) and the resulting `RunStats`.

use comet_enrichment_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups, RunOptions, run,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::path::Path;

/// Authoritative schema copy at the workspace root, relative to this crate.
const SCHEMA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schema/enrichment_input_schema.json"
);

/// Pure-transform method: rewrites every record's `resourceTypeGeneral` to `"Dataset"`.
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
enrichment:
  sources:
    - name: COMET
      nameType: Organizational
      contributorType: Producer
  resources:
    - relatedIdentifier: "10.82461/bpzr-jd55"
      relatedIdentifierType: DOI
      relationType: IsDocumentedBy
      resourceTypeGeneral: Project
"#;

#[test]
fn run_drives_transform_path_end_to_end() {
    let dir = tempfile::tempdir().unwrap();

    // Input shard, nested like the real data layout, gzip-compressed.
    let shard_dir = dir.path().join("input/updated_2024-01");
    fs::create_dir_all(&shard_dir).unwrap();
    let lines = [
        r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Journal article","resourceTypeGeneral":"Text"}}}"#,
        r#"{"id":"10.1/b","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#,
        r#"{"id":"10.1/c","attributes":{"types":{}}}"#, // skipped: no_resource_type
        "",                                             // blank line: ignored, not malformed
        "{not valid json",                              // malformed
    ];
    let shard = shard_dir.join("part_0000.jsonl.gz");
    let f = fs::File::create(&shard).unwrap();
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(lines.join("\n").as_bytes()).unwrap();
    enc.finish().unwrap();

    let enrichment = dir.path().join("enrichment.yaml");
    fs::write(&enrichment, ENRICHMENT_YAML).unwrap();
    let output = dir.path().join("out.jsonl");

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        enrichment,
        threads: 1,
        batch_size: 5000,
    };

    // Exercise the schema validator at the write boundary.
    let validator = comet_enrichment_core::schema::compile(Path::new(SCHEMA_PATH)).unwrap();
    let stats = run(&DatasetTagger, &opts, Some(validator)).unwrap();

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
