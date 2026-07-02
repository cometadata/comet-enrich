//! End-to-end test for running an enrichment method through `core::run`.

use comet_enrich_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, EnrichmentTemplate, Extracted, Lookups,
    RunOptions, run,
};
use comet_enrich_test_support::{config_path, read_enrichment_parts, write_gz_lines};
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

/// Write the shared provenance file into `dir` and build default run options over
/// its `input`/`out` layout: one thread, batch 100, one 256 MiB writer lane.
/// Tests that vary threads, lanes, or part size mutate the returned options.
fn transform_setup(dir: &tempfile::TempDir) -> (EnrichmentTemplate, RunOptions) {
    let provenance = dir.path().join("enrichment.yaml");
    fs::write(&provenance, ENRICHMENT_YAML).unwrap();
    let template = comet_enrich_core::load_template(&provenance).unwrap();
    let opts = RunOptions {
        input: dir.path().join("input"),
        output: dir.path().join("out"),
        threads: 1,
        batch_size: 100,
        output_part_size_bytes: 256 * 1024 * 1024,
        output_writer_lanes: 1,
    };
    (template, opts)
}

fn enrichment_part_names(output: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(output.join(comet_enrich_core::ENRICHMENTS_DIR))
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

    let (template, opts) = transform_setup(&dir);

    // Validate records using the same schema check as a normal run.
    let validator =
        comet_enrich_core::schema::compile(&config_path("schema/enrichment_input_schema.json"))
            .unwrap();
    let stats = run(&DatasetTagger, &opts, &template, Some(&validator)).unwrap();

    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_scanned, 3);
    assert_eq!(stats.lines_malformed, 1);
    assert_eq!(stats.emitted, 2);
    assert_eq!(stats.schema_failures, 0);
    assert_eq!(stats.skipped.get("no_resource_type"), Some(&1));

    let recs = read_enrichment_parts(&opts.output);
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

    let (template, mut opts) = transform_setup(&dir);
    opts.threads = 2;

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 3);
    assert_eq!(
        enrichment_part_names(&opts.output),
        vec!["part_0000.jsonl.gz".to_owned()]
    );
    assert_eq!(read_enrichment_parts(&opts.output).len(), 3);
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

    let (template, mut opts) = transform_setup(&dir);
    opts.output_part_size_bytes = 1;

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    let names = enrichment_part_names(&opts.output);

    assert_eq!(stats.emitted, 20);
    assert!(
        names.len() > 1,
        "tiny part target should roll output, got {names:?}"
    );
    assert_contiguous_part_names(&names);
    assert_eq!(read_enrichment_parts(&opts.output).len(), 20);
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

    let (template, mut opts) = transform_setup(&dir);
    opts.threads = 4;
    opts.output_part_size_bytes = 1;
    opts.output_writer_lanes = 4;

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    let names = enrichment_part_names(&opts.output);

    assert_eq!(stats.emitted, 40);
    assert_contiguous_part_names(&names);
    assert!(
        !opts
            .output
            .join(comet_enrich_core::ENRICHMENTS_DIR)
            .join(".tmp")
            .exists()
    );
    assert_eq!(read_enrichment_parts(&opts.output).len(), 40);
}

#[test]
fn write_failure_fails_the_run() {
    // A blocked output path must abort the run rather than being logged and dropped.
    // A directory where the failures file should go cannot be cleared or written, so
    // the run fails loudly. (The divert-write failure itself is unit-tested in
    // writer.rs.)
    let dir = tempfile::tempdir().unwrap();

    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );

    let (template, opts) = transform_setup(&dir);
    // Block the failures file: a directory here cannot be cleared or written.
    fs::create_dir_all(opts.output.join(comet_enrich_core::ENRICHMENTS_FAILED_FILE)).unwrap();

    // A validator that rejects every record, so the one emitted record is diverted.
    let validator = comet_enrich_core::schema::compile_str(
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

    let (template, opts) = transform_setup(&dir);
    let output = opts.output.clone();

    run(&DatasetTagger, &opts, &template, None).unwrap();
    assert_eq!(read_enrichment_parts(&output).len(), 2);
    write_gz_lines(
        &output
            .join(comet_enrich_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz"),
        &[r#"{"doi":"stale"}"#],
    );
    assert!(
        output
            .join(comet_enrich_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );

    fs::remove_file(input.join("updated_2024-01/part_0001.jsonl.gz")).unwrap();
    fs::write(output.join("manifest.json"), "stale").unwrap();
    fs::write(
        output.join(comet_enrich_core::ENRICHMENTS_FAILED_FILE),
        "stale\n",
    )
    .unwrap();
    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 1);
    assert!(!output.join("manifest.json").exists());
    assert!(
        !output
            .join(comet_enrich_core::ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );
    // A clean rerun clears the stale failures file and creates no new one.
    assert!(
        !output
            .join(comet_enrich_core::ENRICHMENTS_FAILED_FILE)
            .exists()
    );
    let recs = read_enrichment_parts(&output);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["doi"], json!("10.1/a"));
}

#[test]
fn empty_input_rerun_errors_and_leaves_outputs_untouched() {
    // An empty input directory is indistinguishable from a mistyped --input path,
    // so the run must fail before clearing any prior outputs.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    write_gz_lines(
        &input.join("updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );

    let (template, opts) = transform_setup(&dir);
    let output = opts.output.clone();

    run(&DatasetTagger, &opts, &template, None).unwrap();
    assert_eq!(read_enrichment_parts(&output).len(), 1);

    fs::remove_file(input.join("updated_2024-01/part_0000.jsonl.gz")).unwrap();
    let err = run(&DatasetTagger, &opts, &template, None)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("no *.jsonl.gz input files found"),
        "got: {err}"
    );
    assert_eq!(read_enrichment_parts(&output).len(), 1);
}
