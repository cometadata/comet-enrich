//! End-to-end test for running an enrichment method through `core::run`.

use comet_enrich_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, EnrichmentTemplate, Extracted, Lookups,
    RunOptions, SCHEMA, run, schema,
};
use comet_enrich_test_support::{
    SOURCE_ID, enrichment_template, read_enrichment_parts, run_options, write_gz_lines,
    write_gz_part,
};
use serde_json::{Value, json};
use std::fs;

/// Test method that rewrites `resourceTypeGeneral` to `"Dataset"`.
struct DatasetTagger;

impl EnrichmentMethod for DatasetTagger {
    type Extraction = EnrichmentParts;
    type Lookup = ();

    fn name(&self) -> &'static str {
        "dataset-tagger"
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
        let parts = || EnrichmentParts {
            doi: doi.to_string(),
            action: EnrichmentAction::Update,
            field: "types",
            original: types.clone(),
            enriched: enriched.clone(),
        };
        // A record flagged `duplicate` stands in for one whose items repeat.
        let repeat = record.pointer("/attributes/duplicate") == Some(&json!(true));
        Extracted::Items(if repeat {
            vec![parts(), parts()]
        } else {
            vec![parts()]
        })
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        _lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        vec![extraction]
    }
}

/// Build the shared record template and default run options over the `dir`
/// `input`/`out` layout: one thread, batch 100, one 256 MiB writer lane.
/// Tests that vary threads, lanes, or part size mutate the returned options.
fn transform_setup(dir: &tempfile::TempDir) -> (EnrichmentTemplate, RunOptions) {
    let template = enrichment_template();
    let opts = run_options(dir.path().join("input"), dir.path().join("out"), 1);
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
    let validator = schema::compile_str(SCHEMA).unwrap();
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
        assert_eq!(rec["sourceId"], json!(SOURCE_ID));
    }
}

#[test]
fn corrupt_input_file_is_counted_failed_not_malformed() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input/updated_2024-01");
    write_gz_lines(
        &input.join("part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );
    fs::create_dir_all(&input).unwrap();
    fs::write(input.join("part_0001.jsonl.gz"), b"not gzip").unwrap();

    let (template, opts) = transform_setup(&dir);
    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.files_failed, 1);
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.lines_malformed, 0);
    assert_eq!(stats.emitted, 1);
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

#[test]
fn output_equal_to_input_is_refused_and_input_survives() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let part = input.join("part_0000.jsonl.gz");
    write_gz_lines(
        &part,
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );
    let (template, mut opts) = transform_setup(&dir);
    opts.output = input.clone();

    let err = format!(
        "{:#}",
        run(&DatasetTagger, &opts, &template, None).unwrap_err()
    );

    assert!(err.contains("--output overlaps --input"), "got: {err}");
    assert!(part.is_file(), "input part was deleted");
}

#[test]
fn output_holding_staged_work_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    write_gz_lines(
        &dir.path().join("input/updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/a","attributes":{"types":{"resourceType":"Spreadsheet"}}}"#],
    );
    let (template, opts) = transform_setup(&dir);
    // A staged run left its work directory and a marker behind.
    let marker = opts.output.join(".work").join("reconcile.done");
    fs::create_dir_all(marker.parent().unwrap()).unwrap();
    fs::write(&marker, "0.4.0").unwrap();

    let err = format!(
        "{:#}",
        run(&DatasetTagger, &opts, &template, None).unwrap_err()
    );

    assert!(err.contains(".work"), "got: {err}");
    assert!(marker.is_file(), "staged marker was removed");
}

#[test]
fn run_drops_repeated_content_keys_within_one_record() {
    let dir = tempfile::tempdir().unwrap();
    let (template, opts) = transform_setup(&dir);
    write_gz_lines(
        &opts.input.join("part_0000.jsonl.gz"),
        &[
            &json!({"id": "10.1/twice", "attributes": {"duplicate": true, "types": {"resourceType": "x"}}})
                .to_string(),
            &json!({"id": "10.1/once", "attributes": {"types": {"resourceType": "y"}}}).to_string(),
        ],
    );

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();

    assert_eq!(stats.emitted, 2);
    assert_eq!(stats.duplicate_enrichments, 1);
    let mut dois: Vec<String> = read_enrichment_parts(&opts.output)
        .iter()
        .map(|rec| rec["doi"].as_str().unwrap().to_owned())
        .collect();
    dois.sort();
    assert_eq!(dois, ["10.1/once", "10.1/twice"]);
}

#[test]
fn run_selects_source_winners_and_excludes_duplicates_from_coverage() {
    use comet_enrich_core::{Manifest, RunMeta, StageTimings};

    let dir = tempfile::tempdir().unwrap();
    let (template, mut opts) = transform_setup(&dir);
    opts.threads = 2;
    let record = |updated: &str, label: &str| {
        json!({
            "id": "10.1/a",
            "attributes": {"updated": updated, "types": {"resourceType": label}}
        })
        .to_string()
    };
    write_gz_lines(
        &opts.input.join("part_0000.jsonl.gz"),
        &[&record("2026-09-02T00:00:00Z", "first")],
    );
    write_gz_lines(
        &opts.input.join("part_0001.jsonl.gz"),
        &[
            &record("2026-09-01T00:00:00Z", "older"),
            &record("2026-09-02T00:00:00Z", "last"),
            &json!({"id":"10.1/empty","attributes":{"types":{}}}).to_string(),
        ],
    );

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    let output = read_enrichment_parts(&opts.output);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0]["originalValue"]["resourceType"], "last");
    assert_eq!(stats.records_scanned, 4);
    assert_eq!(stats.duplicate_records, 2);
    let meta = RunMeta {
        method_name: "dataset-tagger".to_owned(),
        method_version: "test",
        source_id: SOURCE_ID.to_owned(),
        sources: std::collections::BTreeMap::new(),
    };
    let manifest = Manifest::build(
        &stats,
        &meta,
        &["no_resource_type"],
        &StageTimings::default(),
        "success",
    );
    assert_eq!(manifest.report.coverage.records_in_scope, 1);
    assert_eq!(manifest.report.coverage.records_enriched, 1);
}

#[test]
fn run_reads_every_member_of_a_concatenated_gzip_part() {
    let dir = tempfile::tempdir().unwrap();
    let (template, opts) = transform_setup(&dir);
    let record =
        |doi: &str| json!({"id": doi, "attributes": {"types": {"resourceType": "Dataset"}}});
    let members = [dir.path().join("a.gz"), dir.path().join("b.gz")];
    write_gz_part(&members[0], &[record("10.1/a")]);
    write_gz_part(&members[1], &[record("10.1/b"), record("10.1/c")]);
    let bytes = [
        fs::read(&members[0]).unwrap(),
        fs::read(&members[1]).unwrap(),
    ]
    .concat();
    fs::create_dir_all(&opts.input).unwrap();
    fs::write(opts.input.join("part_0000.jsonl.gz"), bytes).unwrap();

    let stats = run(&DatasetTagger, &opts, &template, None).unwrap();
    assert_eq!(stats.records_scanned, 3);
    assert_eq!(read_enrichment_parts(&opts.output).len(), 3);
}
