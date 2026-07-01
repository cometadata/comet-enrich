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

fn enrichment_part_names(output: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(output.join(comet_enrichment_core::ENRICHMENTS_DIR))
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().and_then(|e| e.to_str()) == Some("gz"))
                .then(|| path.file_name().unwrap().to_string_lossy().into_owned())
        })
        .collect();
    names.sort();
    names
}

fn assert_contiguous_part_names(names: &[String]) {
    let expected: Vec<String> = (0..names.len())
        .map(|idx| format!("part_{idx:04}.jsonl.gz"))
        .collect();
    assert_eq!(names, expected);
}

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
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
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
fn many_input_files_with_small_output_write_one_part_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input/updated_2024-01");
    for idx in 0..3 {
        write_gz_lines(
            &input.join(format!("part_{idx:04}.jsonl.gz")),
            &[&format!(
                r#"{{"id":"10.1/{idx}","attributes":{{"types":{{"resourceType":"Spreadsheet"}}}}}}"#
            )],
        );
    }

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out");

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 2,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 3);
    assert_eq!(
        enrichment_part_names(&output),
        vec!["part_0000.jsonl.gz".to_owned()]
    );
    assert_eq!(read_enrichment_parts(&output).len(), 3);
}

#[test]
fn small_output_part_size_rolls_into_contiguous_parts() {
    let dir = tempfile::tempdir().unwrap();
    let lines: Vec<String> = (0..20)
        .map(|idx| {
            format!(
                r#"{{"id":"10.1/{idx}","attributes":{{"types":{{"resourceType":"Spreadsheet"}}}}}}"#
            )
        })
        .collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &line_refs,
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
        output_part_size_bytes: 1,
        output_writer_lanes: 1,
    };

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    let names = enrichment_part_names(&output);

    assert_eq!(stats.emitted, 20);
    assert!(
        names.len() > 1,
        "tiny part target should roll output, got {names:?}"
    );
    assert_contiguous_part_names(&names);
    assert_eq!(read_enrichment_parts(&output).len(), 20);
}

#[test]
fn parallel_writer_lanes_publish_global_part_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let lines: Vec<String> = (0..40)
        .map(|idx| {
            format!(
                r#"{{"id":"10.2/{idx}","attributes":{{"types":{{"resourceType":"Spreadsheet"}}}}}}"#
            )
        })
        .collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &line_refs,
    );

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out");

    let opts = RunOptions {
        input: dir.path().join("input"),
        output: output.clone(),
        threads: 4,
        batch_size: 100,
        output_part_size_bytes: 1,
        output_writer_lanes: 4,
    };

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    let names = enrichment_part_names(&output);

    assert_eq!(stats.emitted, 40);
    assert_contiguous_part_names(&names);
    assert!(
        !output
            .join(comet_enrichment_core::ENRICHMENTS_DIR)
            .join(".tmp")
            .exists()
    );
    assert_eq!(read_enrichment_parts(&output).len(), 40);
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
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };

    // A validator that rejects every record, so the one emitted record is diverted.
    let validator = comet_enrichment_core::schema::compile_str(
        r#"{"type":"object","required":["__never_present__"]}"#,
    )
    .unwrap();
    let result = run(&DatasetTagger, &opts, &template, Some(&validator));
    assert!(result.is_err(), "write failure should fail the run");
}

#[test]
fn rerun_with_fewer_inputs_removes_stale_enrichment_parts() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    write_gz_lines(
        &input.join("updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );
    write_gz_lines(
        &input.join("updated_2024-01/part_0001.jsonl.gz"),
        &[r#"{"id":"10.1/b","attributes":{"types":{"resourceType":"Table"}}}"#],
    );

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out");
    let opts = RunOptions {
        input: input.clone(),
        output: output.clone(),
        threads: 1,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };

    run(&DatasetTagger, &opts, &template, None).unwrap();
    assert_eq!(read_enrichment_parts(&output).len(), 2);
    write_gz_lines(
        &output
            .join(comet_enrichment_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz"),
        &[r#"{"doi":"stale"}"#],
    );
    assert!(
        output
            .join(comet_enrichment_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );

    fs::remove_file(input.join("updated_2024-01/part_0001.jsonl.gz")).unwrap();
    fs::write(output.join("manifest.json"), "stale").unwrap();
    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 1);
    assert!(!output.join("manifest.json").exists());
    assert!(
        !output
            .join(comet_enrichment_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );
    let recs = read_enrichment_parts(&output);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["doi"], json!("10.1/a"));
}

#[test]
fn empty_input_rerun_removes_stale_transform_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    write_gz_lines(
        &input.join("updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );

    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrichment_core::load_template(&provenance).unwrap();
    let output = dir.path().join("out");
    let opts = RunOptions {
        input: input.clone(),
        output: output.clone(),
        threads: 1,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };

    run(&DatasetTagger, &opts, &template, None).unwrap();
    assert_eq!(read_enrichment_parts(&output).len(), 1);

    fs::remove_file(input.join("updated_2024-01/part_0000.jsonl.gz")).unwrap();
    fs::write(output.join("manifest.json"), "stale").unwrap();
    fs::write(
        output.join(comet_enrichment_core::ENRICHMENTS_FAILED_FILE),
        "stale\n",
    )
    .unwrap();

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 0);
    assert!(!output.join("manifest.json").exists());
    assert!(
        !output
            .join(comet_enrichment_core::ENRICHMENTS_FAILED_FILE)
            .exists()
    );
    assert!(
        !output
            .join(comet_enrichment_core::ENRICHMENTS_DIR)
            .join(".tmp")
            .exists()
    );
    assert_eq!(read_enrichment_parts(&output), Vec::<Value>::new());
}
