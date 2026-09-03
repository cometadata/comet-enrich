use super::reconcile::extraction_part_files;
use super::report::{classify_failure, histogram_bucket};
use super::*;
use crate::dedup::HashBits;
use crate::manifest::{MANIFEST_FILE, MatchFailureTaxonomy, Report};
use crate::match_service::{FakeMatchService, MatchError, MatchOutcome, RorLookup};
use crate::method::{EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
use crate::options::RunOptions;
use crate::template::EnrichmentTemplate;
use crate::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};

use anyhow::Result;
use async_trait::async_trait;
use comet_enrich_test_support::{
    assert_close, assert_err_contains, gz_input_fixture, gz_parts_fixture, read_enrichment_parts,
    write_gz_lines,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct TestMethod {
    hash_bits: HashBits,
}

#[derive(Serialize, Deserialize)]
struct TestExtraction {
    doi: String,
    name: String,
    name_hash: String,
}

impl EnrichmentMethod for TestMethod {
    type Extraction = TestExtraction;
    type Lookup = RorLookup;

    fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
        let doi = record.get("id").and_then(Value::as_str).unwrap_or("");
        let name = record
            .pointer("/attributes/name")
            .and_then(Value::as_str)
            .unwrap_or("");
        if doi.is_empty() || name.is_empty() {
            return Extracted::Skip("no_name");
        }
        Extracted::Items(vec![TestExtraction {
            doi: doi.to_owned(),
            name: name.to_owned(),
            name_hash: crate::dedup::hash_input(name, self.hash_bits),
        }])
    }

    fn inputs(&self, extraction: &Self::Extraction) -> Vec<String> {
        vec![extraction.name.clone()]
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        match lookups.get(&extraction.name_hash) {
            Some(hit) => vec![EnrichmentParts {
                doi: extraction.doi,
                action: EnrichmentAction::UpdateChild,
                field: "fundingReferences",
                original: json!({ "name": extraction.name }),
                enriched: json!({
                    "name": extraction.name,
                    "funderIdentifier": hit.ror_id,
                    "confidence": hit.confidence,
                }),
            }],
            None => Vec::new(),
        }
    }
}

fn template() -> EnrichmentTemplate {
    EnrichmentTemplate::new("10.82461/bpzr-jd55").unwrap()
}

fn fake_service() -> Arc<dyn MatchService> {
    let mut map = HashMap::new();
    map.insert(
        "MIT".to_owned(),
        ("https://ror.org/042nb2s44".to_owned(), 0.99),
    );
    map.insert(
        "NSF".to_owned(),
        ("https://ror.org/021nxhr62".to_owned(), 0.95),
    );
    Arc::new(FakeMatchService::new(map))
}

struct PanickingMatchService;

#[async_trait]
impl MatchService for PanickingMatchService {
    async fn match_bulk(&self, _inputs: &[String], _task: &str) -> Result<Vec<MatchOutcome>> {
        panic!("simulated query panic");
    }
}

fn sample_records() -> Vec<Value> {
    vec![
        json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } }),
        json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } }),
        json!({ "id": "10.1/unknown", "attributes": { "name": "Unknown University" } }),
        json!({ "id": "10.1/empty", "attributes": {} }),
    ]
}

fn cfg(hash_bits: HashBits, from_scratch: bool) -> LookupConfig {
    LookupConfig {
        ror_service_url: "http://unused".to_owned(),
        ror_batch_size: 2,
        ror_concurrency: 2,
        ror_timeout: 30,
        hash_bits,
        from_scratch,
    }
}

struct TestRun {
    _dir: tempfile::TempDir,
    input: PathBuf,
    output: PathBuf,
    method: TestMethod,
    svc: Arc<dyn MatchService>,
    tmpl: EnrichmentTemplate,
}

impl TestRun {
    fn new() -> Self {
        Self::from_fixture(gz_input_fixture(&sample_records()))
    }

    fn from_fixture(fixture: (tempfile::TempDir, PathBuf, PathBuf)) -> Self {
        let (dir, input, output) = fixture;
        TestRun {
            _dir: dir,
            input,
            output,
            method: TestMethod {
                hash_bits: HashBits::Bits64,
            },
            svc: fake_service(),
            tmpl: template(),
        }
    }

    fn opts(&self) -> RunOptions {
        RunOptions {
            input: self.input.clone(),
            output: self.output.clone(),
            threads: 1,
            batch_size: 100,
            output_part_size_bytes: 256 * 1024 * 1024,
            output_writer_lanes: 1,
        }
    }

    fn work(&self) -> PathBuf {
        self.output.join(WORK_DIR)
    }

    fn run(&self, from_scratch: bool) -> Result<Report> {
        self.run_with(&cfg(HashBits::Bits64, from_scratch), None, None)
    }

    fn run_stage(&self, stage: Stage) -> Result<Report> {
        self.run_with(&cfg(HashBits::Bits64, false), None, Some(stage))
    }

    fn run_with(
        &self,
        cfg: &LookupConfig,
        validator: Option<&jsonschema::Validator>,
        only_stage: Option<Stage>,
    ) -> Result<Report> {
        self.run_with_template(&self.tmpl, cfg, validator, only_stage)
    }

    fn run_with_template(
        &self,
        template: &EnrichmentTemplate,
        cfg: &LookupConfig,
        validator: Option<&jsonschema::Validator>,
        only_stage: Option<Stage>,
    ) -> Result<Report> {
        run_staged(
            &self.method,
            &self.opts(),
            cfg,
            &self.svc,
            template,
            validator,
            "funder",
            only_stage,
        )
    }
}

#[test]
fn full_pipeline_produces_contract_and_match_block() {
    let t = TestRun::new();

    let report = t.run(true).unwrap();

    let m = report.match_.expect("match block present");
    assert_eq!(m.unique_inputs, 3);
    assert_eq!(m.matched, 2);
    assert_eq!(m.failure_taxonomy.no_match, 1);
    assert_eq!(m.failure_taxonomy.error, 0);
    let top_bucket = m.confidence_histogram.last().unwrap();
    assert_close(top_bucket.max, 1.0);
    assert_eq!(top_bucket.count, 2);

    assert_eq!(report.counters.records_scanned, 4);
    assert_eq!(report.counters.emitted, 2);
    assert_eq!(report.counters.skipped.get("no_name"), Some(&1));
    assert_eq!(report.coverage.records_in_scope, 3);
    assert_eq!(report.coverage.records_enriched, 2);
    assert_close(report.coverage.coverage_rate, 2.0 / 3.0);

    assert!(report.stage_timings_ms.extract.is_some());
    assert!(report.stage_timings_ms.query.is_some());
    assert!(report.stage_timings_ms.reconcile.is_some());

    let work = t.work();
    for f in [
        "extractions/part_0000.jsonl",
        INPUTS_FILE,
        INPUTS_FINGERPRINT_FILE,
        LOOKUPS_FILE,
        LOOKUPS_FAILED_FILE,
        HASH_BITS_FILE,
        "extract.done",
        "query.done",
        "reconcile.done",
    ] {
        assert!(work.join(f).exists(), "missing work artifact: {f}");
    }
    assert_eq!(
        fs::read_to_string(work.join(HASH_BITS_FILE)).unwrap(),
        "xxh3-64"
    );

    let dois = read_output_dois(&t.output);
    assert_eq!(dois.len(), 2);
    assert!(dois.contains(&"10.1/mit".to_owned()));
    assert!(dois.contains(&"10.1/nsf".to_owned()));
}

fn read_output_dois(output: &Path) -> Vec<String> {
    read_enrichment_parts(output)
        .iter()
        .map(|rec| {
            assert_eq!(rec["field"], "fundingReferences");
            assert_eq!(rec["action"], "updateChild");
            rec["doi"].as_str().unwrap().to_owned()
        })
        .collect()
}

#[test]
fn only_stage_extract_runs_just_extract() {
    let t = TestRun::new();

    t.run_stage(Stage::Extract).unwrap();

    let work = t.work();
    assert!(work.join("extract.done").exists());
    assert!(work.join(INPUTS_FILE).exists());
    assert!(!work.join("query.done").exists());
    assert!(!work.join("reconcile.done").exists());
    assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
}

#[test]
fn from_scratch_with_single_stage_errors() {
    let t = TestRun::new();

    assert_err_contains(
        t.run_with(&cfg(HashBits::Bits64, true), None, Some(Stage::Extract)),
        "cannot be combined with a single stage",
    );
}

#[test]
fn empty_input_errors_before_clearing_outputs() {
    let t = TestRun::new();

    t.run(true).unwrap();
    assert_eq!(read_output_dois(&t.output).len(), 2);

    // Emptying the input (as a mistyped --input would) must error before any
    // prior outputs are cleared, even with --from-scratch.
    fs::remove_file(t.input.join("updated_2024-01/part_0000.jsonl.gz")).unwrap();
    assert_err_contains(t.run(true), "no *.jsonl.gz input files found");
    assert_eq!(read_output_dois(&t.output).len(), 2);
}

#[test]
fn only_stage_query_without_extract_errors() {
    let t = TestRun::new();

    assert_err_contains(t.run_stage(Stage::Query), "extract");
}

#[test]
fn resume_after_deleting_reconcile_marker_reproduces_output() {
    let t = TestRun::new();

    t.run(true).unwrap();

    fs::remove_file(t.work().join("reconcile.done")).unwrap();
    let report = t.run(false).unwrap();

    assert_eq!(report.counters.emitted, 2);
    assert!(report.stage_timings_ms.reconcile.is_some());
    assert!(report.stage_timings_ms.extract.is_none());
    assert_eq!(read_output_dois(&t.output).len(), 2);
}

#[test]
fn from_scratch_failure_invalidates_old_downstream_markers() {
    let mut t = TestRun::new();

    t.run(true).unwrap();

    t.svc = Arc::new(PanickingMatchService);
    assert_err_contains(t.run(true), "query task panicked");

    let work = t.work();
    assert!(work.join("extract.done").exists());
    assert!(!work.join("query.done").exists());
    assert!(!work.join("reconcile.done").exists());
    assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
}

#[test]
fn corrupt_input_file_is_counted_failed_not_hung() {
    let first = [json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } })];
    let second = [json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } })];
    let t = TestRun::from_fixture(gz_parts_fixture(&[&first, &second]));

    // Corrupt gzip should fail the file, not hang the scan.
    fs::write(
        t.input.join("updated_2024-01/part_0001.jsonl.gz"),
        b"not gzip",
    )
    .unwrap();
    let report = t.run(true).unwrap();

    assert_eq!(report.counters.files_failed, 1);
    assert_eq!(report.counters.files_processed, 1);
    assert!(t.work().join("extractions/part_0000.jsonl").exists());
    assert!(
        !t.work().join("extractions/part_0001.jsonl").exists(),
        "partial extraction part must be removed"
    );
    assert_eq!(read_output_dois(&t.output), vec!["10.1/mit".to_owned()]);
    assert_eq!(
        crate::exit_status(report.counters.files_failed, 0, 0, true),
        "partial"
    );
}

#[test]
fn from_scratch_with_fewer_inputs_removes_obsolete_extraction_parts() {
    let first = [json!({ "id": "10.1/mit", "attributes": { "name": "MIT" } })];
    let second = [json!({ "id": "10.1/nsf", "attributes": { "name": "NSF" } })];
    let t = TestRun::from_fixture(gz_parts_fixture(&[&first, &second]));

    t.run(true).unwrap();
    assert!(t.work().join("extractions/part_0001.jsonl").exists());
    write_gz_lines(
        &t.output.join(ENRICHMENTS_DIR).join("part_9999.jsonl.gz"),
        &[r#"{"doi":"stale"}"#],
    );
    assert!(
        t.output
            .join(ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );

    fs::remove_file(t.input.join("updated_2024-01/part_0001.jsonl.gz")).unwrap();
    t.run(true).unwrap();

    assert!(!t.work().join("extractions/part_0001.jsonl").exists());
    assert!(
        !t.output
            .join(ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );
    assert_eq!(read_output_dois(&t.output), vec!["10.1/mit".to_owned()]);
}

#[test]
fn single_stage_extract_invalidates_downstream_artifacts() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::write(t.output.join(MANIFEST_FILE), "stale").unwrap();

    t.run_stage(Stage::Extract).unwrap();

    let work = t.work();
    assert!(work.join("extract.done").exists());
    assert!(!work.join("query.done").exists());
    assert!(!work.join("reconcile.done").exists());
    assert!(!work.join(LOOKUPS_FILE).exists());
    assert!(!work.join(RECONCILE_STATS_FILE).exists());
    assert!(!t.output.join(MANIFEST_FILE).exists());
    assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
}

#[test]
fn single_stage_query_invalidates_reconcile_artifacts() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::write(t.output.join(MANIFEST_FILE), "stale").unwrap();

    t.run_stage(Stage::Query).unwrap();

    let work = t.work();
    assert!(work.join("extract.done").exists());
    assert!(work.join("query.done").exists());
    assert!(!work.join("reconcile.done").exists());
    assert!(!work.join(RECONCILE_STATS_FILE).exists());
    assert!(!t.output.join(MANIFEST_FILE).exists());
    assert_eq!(read_output_dois(&t.output), Vec::<String>::new());
}

#[test]
fn single_stage_reconcile_replaces_stale_public_outputs() {
    let t = TestRun::new();

    t.run(true).unwrap();
    write_gz_lines(
        &t.output.join(ENRICHMENTS_DIR).join("part_9999.jsonl.gz"),
        &[r#"{"doi":"stale"}"#],
    );
    fs::write(t.output.join(ENRICHMENTS_FAILED_FILE), "stale\n").unwrap();

    t.run_stage(Stage::Reconcile).unwrap();

    assert!(t.work().join("reconcile.done").exists());
    assert!(
        !t.output
            .join(ENRICHMENTS_DIR)
            .join("part_9999.jsonl.gz")
            .exists()
    );
    assert!(!t.output.join(ENRICHMENTS_FAILED_FILE).exists());
    let dois = read_output_dois(&t.output);
    assert_eq!(dois.len(), 2);
    assert!(dois.contains(&"10.1/mit".to_owned()));
    assert!(dois.contains(&"10.1/nsf".to_owned()));
}

#[test]
fn extraction_part_files_treats_glob_metacharacters_as_literal_path_chars() {
    let dir = tempfile::tempdir().unwrap();
    let extractions = dir.path().join("extract[ions]?*");
    fs::create_dir_all(&extractions).unwrap();
    fs::write(extractions.join("part_0001.jsonl"), "").unwrap();
    fs::write(extractions.join("part_0000.jsonl"), "").unwrap();
    fs::write(extractions.join("part_0002.json"), "").unwrap();
    fs::write(extractions.join("other_0003.jsonl"), "").unwrap();
    fs::create_dir_all(extractions.join("part_nested.jsonl")).unwrap();

    let parts = extraction_part_files(&extractions).unwrap();

    assert_eq!(
        parts,
        vec![
            extractions.join("part_0000.jsonl"),
            extractions.join("part_0001.jsonl")
        ]
    );
}

fn copy_tree(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), &to).unwrap();
        }
    }
}

#[test]
fn rerun_of_complete_pipeline_with_changed_input_errors() {
    let t = TestRun::new();

    t.run(true).unwrap();

    // Completed markers must not hide a changed input corpus.
    write_gz_lines(
        &t.input.join("updated_2024-01/part_0001.jsonl.gz"),
        &[r#"{"id":"10.1/new","attributes":{"name":"MIT"}}"#],
    );

    assert_err_contains(t.run(false), "--from-scratch");
    // The previous outputs are left untouched.
    assert_eq!(read_output_dois(&t.output).len(), 2);
}

#[test]
fn resume_with_size_changed_input_errors() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::remove_file(t.work().join("reconcile.done")).unwrap();

    // Same file name, different content (and so compressed size).
    write_gz_lines(
        &t.input.join("updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/mit","attributes":{"name":"MIT"}}"#],
    );

    assert_err_contains(t.run(false), "size-changed");
}

#[test]
fn resume_with_same_size_crc_changed_input_errors() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::remove_file(t.work().join("reconcile.done")).unwrap();

    let path = t.input.join("updated_2024-01/part_0000.jsonl.gz");
    let original_size = fs::metadata(&path).unwrap().len();
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::End(-8)).unwrap();
    let mut crc = [0_u8; 4];
    file.read_exact(&mut crc).unwrap();
    let changed = u32::from_le_bytes(crc) ^ 1;
    file.seek(SeekFrom::End(-8)).unwrap();
    file.write_all(&changed.to_le_bytes()).unwrap();
    drop(file);
    assert_eq!(fs::metadata(&path).unwrap().len(), original_size);

    assert_err_contains(t.run(false), "crc-changed");
}

#[test]
fn resume_with_relocated_corpus_succeeds() {
    let mut t = TestRun::new();

    t.run(true).unwrap();

    // The fingerprint is independent of the input root path.
    let moved = t.input.parent().unwrap().join("input_moved");
    copy_tree(&t.input, &moved);
    t.input = moved;
    fs::remove_file(t.work().join("reconcile.done")).unwrap();

    let report = t.run(false).unwrap();

    assert_eq!(report.counters.emitted, 2);
}

#[test]
fn resume_without_fingerprint_errors() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::remove_file(t.work().join(INPUTS_FINGERPRINT_FILE)).unwrap();

    assert_err_contains(t.run(false), "missing inputs.fingerprint.json");
}

#[test]
fn single_stage_reconcile_ignores_input_change() {
    let t = TestRun::new();

    t.run(true).unwrap();

    // Single-stage reconcile uses the existing work dir artifacts.
    write_gz_lines(
        &t.input.join("updated_2024-01/part_0001.jsonl.gz"),
        &[r#"{"id":"10.1/new","attributes":{"name":"MIT"}}"#],
    );

    let report = t.run_stage(Stage::Reconcile).unwrap();

    assert_eq!(report.counters.emitted, 2);
}

#[test]
fn resume_with_mismatched_hash_width_errors() {
    let t = TestRun::new();

    t.run(true).unwrap();

    assert_err_contains(
        t.run_with(&cfg(HashBits::Bits128, false), None, None),
        "hash-width mismatch",
    );
}

fn reconcile_stats(t: &TestRun) -> Value {
    serde_json::from_str(&fs::read_to_string(t.work().join(RECONCILE_STATS_FILE)).unwrap()).unwrap()
}

/// Warnings captured from the `log` facade. The buffer is shared by every
/// test in the binary, so search it for the ids a test uses rather than
/// asserting on its length.
static WARNINGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct WarningCapture;

impl log::Log for WarningCapture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            WARNINGS.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

/// Install the warning capture; later calls are no-ops.
fn capture_warnings() {
    log::set_logger(&WarningCapture).ok();
    log::set_max_level(log::LevelFilter::Warn);
}

fn warnings_mentioning(needle: &str) -> Vec<String> {
    WARNINGS
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains(needle))
        .cloned()
        .collect()
}

#[test]
fn rerun_of_complete_pipeline_keeps_truthful_manifest() {
    let t = TestRun::new();

    let first = t.run(true).unwrap();

    let again = t.run(false).unwrap();

    assert!(again.stage_timings_ms.extract.is_none());
    assert!(again.stage_timings_ms.query.is_none());
    assert!(again.stage_timings_ms.reconcile.is_none());
    assert_eq!(again.counters.emitted, first.counters.emitted);
    assert_eq!(again.counters.emitted, 2);
    assert_eq!(
        again.coverage.records_in_scope,
        first.coverage.records_in_scope
    );
    assert_eq!(again.coverage.records_enriched, 2);
    assert_eq!(again.match_.unwrap().matched, 2);
    assert_eq!(reconcile_stats(&t)["source_id"], template().source_id());
}

#[test]
fn rerun_of_complete_pipeline_with_removed_input_errors() {
    let t = TestRun::new();
    t.run(true).unwrap();

    // An unchanged source id is an ordinary rerun, which still needs the corpus.
    fs::remove_dir_all(&t.input).unwrap();

    assert_err_contains(t.run(false), "input path is not a directory");
    assert_eq!(read_output_dois(&t.output).len(), 2);
}

#[test]
fn resume_with_changed_source_id_reruns_only_reconcile() {
    capture_warnings();
    let t = TestRun::new();
    t.run(true).unwrap();

    let changed_source_id = "10.82461/other-run";
    let changed = EnrichmentTemplate::new(changed_source_id).unwrap();
    let report = t
        .run_with_template(&changed, &cfg(HashBits::Bits64, false), None, None)
        .unwrap();

    assert!(report.stage_timings_ms.extract.is_none());
    assert!(report.stage_timings_ms.query.is_none());
    assert!(report.stage_timings_ms.reconcile.is_some());
    assert_eq!(report.counters.emitted, 2);
    let records = read_enrichment_parts(&t.output);
    assert_eq!(records.len(), 2);
    for record in records {
        assert_eq!(record["sourceId"], changed_source_id);
    }
    assert_eq!(reconcile_stats(&t)["source_id"], changed_source_id);
    let warnings = warnings_mentioning(changed_source_id);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("source id changed from `10.82461/bpzr-jd55` to `10.82461/other-run`"),
        "{warnings:?}"
    );
    assert!(
        warnings[0].contains("the input corpus is not re-read"),
        "{warnings:?}"
    );
}

#[test]
fn resume_with_uppercase_variant_of_source_id_is_a_noop() {
    capture_warnings();
    let t = TestRun::new();
    let lower = EnrichmentTemplate::new("10.82461/noop-case").unwrap();
    t.run_with_template(&lower, &cfg(HashBits::Bits64, true), None, None)
        .unwrap();
    let before = read_enrichment_parts(&t.output);

    // DOI names are case-insensitive, so this is the same source id.
    let upper = EnrichmentTemplate::new("10.82461/NOOP-CASE").unwrap();
    let report = t
        .run_with_template(&upper, &cfg(HashBits::Bits64, false), None, None)
        .unwrap();

    assert!(report.stage_timings_ms.extract.is_none());
    assert!(report.stage_timings_ms.query.is_none());
    assert!(report.stage_timings_ms.reconcile.is_none());
    assert_eq!(read_enrichment_parts(&t.output), before);
    assert_eq!(reconcile_stats(&t)["source_id"], "10.82461/noop-case");
    assert_eq!(warnings_mentioning("noop-case"), Vec::<String>::new());
}

#[test]
fn resume_with_changed_source_id_does_not_need_input_corpus() {
    let t = TestRun::new();
    t.run(true).unwrap();

    // Re-stamping reads only work artifacts, so a deleted corpus must not
    // block it.
    fs::remove_dir_all(&t.input).unwrap();

    let changed_source_id = "10.82461/no-corpus";
    let changed = EnrichmentTemplate::new(changed_source_id).unwrap();
    let report = t
        .run_with_template(&changed, &cfg(HashBits::Bits64, false), None, None)
        .unwrap();

    assert!(report.stage_timings_ms.extract.is_none());
    assert!(report.stage_timings_ms.query.is_none());
    assert!(report.stage_timings_ms.reconcile.is_some());
    let records = read_enrichment_parts(&t.output);
    assert_eq!(records.len(), 2);
    for record in records {
        assert_eq!(record["sourceId"], changed_source_id);
    }
}

#[test]
fn resume_with_changed_source_id_and_replaced_corpus_errors() {
    capture_warnings();
    let t = TestRun::new();
    t.run(true).unwrap();
    let before = read_enrichment_parts(&t.output);

    // A corpus that is present must still match the fingerprint: reconcile
    // would otherwise rebuild from the extractions of a different snapshot
    // and the manifest would describe data the run never read.
    write_gz_lines(
        &t.input.join("updated_2024-01/part_0000.jsonl.gz"),
        &[r#"{"id":"10.1/mit","attributes":{"name":"MIT"}}"#],
    );

    let changed = EnrichmentTemplate::new("10.82461/replaced-corpus").unwrap();
    assert_err_contains(
        t.run_with_template(&changed, &cfg(HashBits::Bits64, false), None, None),
        "does not match",
    );
    assert_eq!(read_enrichment_parts(&t.output), before);
    assert_eq!(reconcile_stats(&t)["source_id"], template().source_id());
    // The re-stamp never happens, so it must not be announced.
    assert_eq!(warnings_mentioning("replaced-corpus"), Vec::<String>::new());
}

#[test]
fn resume_with_changed_source_id_and_mismatched_hash_width_errors_without_warning() {
    capture_warnings();
    let t = TestRun::new();
    t.run(true).unwrap();

    let changed = EnrichmentTemplate::new("10.82461/width-mismatch").unwrap();
    assert_err_contains(
        t.run_with_template(&changed, &cfg(HashBits::Bits128, false), None, None),
        "hash-width mismatch",
    );
    // The re-stamp never happens, so it must not be announced.
    assert_eq!(warnings_mentioning("width-mismatch"), Vec::<String>::new());
}

#[test]
fn resume_with_legacy_reconcile_stats_reruns_reconcile() {
    capture_warnings();
    let t = TestRun::new();
    t.run(true).unwrap();

    let path = t.work().join(RECONCILE_STATS_FILE);
    let mut stats: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    stats.as_object_mut().unwrap().remove("source_id");
    fs::write(&path, serde_json::to_string(&stats).unwrap()).unwrap();

    let report = t.run(false).unwrap();

    assert!(report.stage_timings_ms.extract.is_none());
    assert!(report.stage_timings_ms.query.is_none());
    assert!(report.stage_timings_ms.reconcile.is_some());
    let refreshed: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(refreshed["source_id"], template().source_id());
    let warnings = warnings_mentioning("has no recorded source id");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("to stamp the output with source id `10.82461/bpzr-jd55`"),
        "{warnings:?}"
    );
    assert!(
        warnings[0].contains("the input corpus is not re-read"),
        "{warnings:?}"
    );
}

#[test]
fn completed_extract_requires_stats_sidecar() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::remove_file(t.work().join(EXTRACT_STATS_FILE)).unwrap();

    assert_err_contains(t.run(false), "extract.stats.json is missing");
}

#[test]
fn completed_reconcile_requires_stats_sidecar() {
    let t = TestRun::new();

    t.run(true).unwrap();
    fs::remove_file(t.work().join(RECONCILE_STATS_FILE)).unwrap();

    assert_err_contains(t.run(false), "reconcile.stats.json is missing");
}

#[test]
fn rejecting_validator_surfaces_schema_failures() {
    let t = TestRun::new();
    let schema = crate::schema::compile_str(r#"{"type":"object","required":["nope"]}"#).unwrap();

    let report = t
        .run_with(&cfg(HashBits::Bits64, true), Some(&schema), None)
        .unwrap();

    assert_eq!(report.counters.emitted, 0);
    assert_eq!(report.counters.schema_failures, 2);
    assert_eq!(report.validation.schema_failures, 2);
    assert!(t.output.join(ENRICHMENTS_FAILED_FILE).exists());
    assert_eq!(
        crate::exit_status(0, report.counters.schema_failures, 0, true),
        "partial"
    );
}

#[test]
fn batch_error_is_recorded_not_certified_as_success() {
    let mut t = TestRun::new();
    t.svc = Arc::new(FakeMatchService::erroring("marple outage"));

    let report = t.run(true).unwrap();

    let m = report.match_.expect("match block present");
    assert_eq!(m.matched, 0);
    assert_eq!(m.failure_taxonomy.error, 3);
    assert_eq!(m.failure_taxonomy.no_match, 0);
    assert_eq!(report.counters.emitted, 0);
    let status = crate::exit_status(
        report.counters.files_failed,
        0,
        m.failure_taxonomy.lost(),
        true,
    );
    assert_eq!(status, "partial");
}

#[test]
fn batch_timeout_is_lost_data_not_success() {
    let mut t = TestRun::new();
    t.svc = Arc::new(FakeMatchService::erroring("operation timed out"));

    let report = t.run(true).unwrap();

    let m = report.match_.expect("match block present");
    assert_eq!(m.matched, 0);
    assert_eq!(m.failure_taxonomy.timeout, 3);
    assert_eq!(m.failure_taxonomy.error, 0);
    assert_eq!(m.failure_taxonomy.lost(), 3);
    let status = crate::exit_status(0, 0, m.failure_taxonomy.lost(), true);
    assert_eq!(status, "partial");
}

#[test]
fn item_error_is_recorded_not_certified_as_no_match() {
    let mut t = TestRun::new();
    let mut matches = HashMap::new();
    matches.insert(
        "MIT".to_owned(),
        ("https://ror.org/042nb2s44".to_owned(), 0.99),
    );
    matches.insert(
        "NSF".to_owned(),
        ("https://ror.org/021nxhr62".to_owned(), 0.95),
    );
    let mut item_errors = HashMap::new();
    item_errors.insert(
        "Unknown University".to_owned(),
        MatchError {
            code: "opensearch_rejected".to_owned(),
            message: "OpenSearch rejected this search after retries".to_owned(),
        },
    );
    t.svc = Arc::new(FakeMatchService::with_item_errors(matches, item_errors));

    let report = t.run(true).unwrap();

    let m = report.match_.expect("match block present");
    assert_eq!(m.matched, 2);
    assert_eq!(m.failure_taxonomy.error, 1);
    assert_eq!(m.failure_taxonomy.no_match, 0);
    assert_eq!(m.failure_taxonomy.lost(), 1);
    let status = crate::exit_status(0, 0, m.failure_taxonomy.lost(), true);
    assert_eq!(status, "partial");
}

#[test]
fn query_covers_every_input_exactly_once_with_more_batches_than_workers() {
    let records: Vec<Value> = (0..6)
        .map(|i| json!({ "id": format!("10.1/{i}"), "attributes": { "name": format!("Org {i}") } }))
        .collect();
    let t = TestRun::from_fixture(gz_input_fixture(&records));

    let mut c = cfg(HashBits::Bits64, true);
    c.ror_batch_size = 1;
    c.ror_concurrency = 2;
    let report = t.run_with(&c, None, None).unwrap();

    assert_eq!(report.match_.unwrap().unique_inputs, 6);

    // Every unique input is claimed by exactly one worker batch.
    let mut hashes = Vec::new();
    for file in [LOOKUPS_FILE, LOOKUPS_FAILED_FILE] {
        for line in fs::read_to_string(t.work().join(file)).unwrap().lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            hashes.push(row["hash"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(hashes.len(), 6, "no input may be dropped or claimed twice");
    hashes.sort();
    hashes.dedup();
    assert_eq!(hashes.len(), 6);
}

#[test]
fn query_with_more_workers_than_batches_completes() {
    let t = TestRun::new();

    let mut c = cfg(HashBits::Bits64, true);
    c.ror_batch_size = 100; // all three inputs fit in one batch
    c.ror_concurrency = 50;
    let report = t.run_with(&c, None, None).unwrap();

    assert_eq!(report.match_.unwrap().matched, 2);
}

#[test]
fn classify_failure_bins_by_kind_not_message() {
    let mut t = MatchFailureTaxonomy::default();
    classify_failure(Some("no_match"), "no match", &mut t);
    classify_failure(Some("no_match"), "server said: timed out no match", &mut t);
    classify_failure(Some("error"), "batch error: operation timed out", &mut t);
    classify_failure(Some("error"), "batch error: HTTP 500", &mut t);
    classify_failure(Some("error"), "batch error: no match endpoint", &mut t);
    classify_failure(None, "no match", &mut t);

    assert_eq!(t.no_match, 2);
    assert_eq!(t.timeout, 1);
    assert_eq!(t.error, 3);
    assert_eq!(t.lost(), 4);
}

#[test]
fn exit_status_is_success_only_when_clean_and_complete() {
    assert_eq!(crate::exit_status(0, 0, 0, true), "success");
    assert_eq!(crate::exit_status(1, 0, 0, true), "partial");
    assert_eq!(crate::exit_status(0, 1, 0, true), "partial");
    assert_eq!(crate::exit_status(0, 0, 1, true), "partial");
    assert_eq!(crate::exit_status(0, 0, 0, false), "partial");
}

#[test]
fn stages_to_run_restart_runs_everything() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
    assert_eq!(stages_to_run(dir.path(), true), Stage::ALL);
}

#[test]
fn stages_to_run_resume_skips_completed_leading_stages() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
    assert_eq!(
        stages_to_run(dir.path(), false),
        vec![Stage::Query, Stage::Reconcile]
    );
}

#[test]
fn stages_to_run_empty_work_dir_runs_all() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(stages_to_run(dir.path(), false), Stage::ALL);
}

#[test]
fn histogram_bucket_clamps_and_includes_top_edge() {
    assert_eq!(histogram_bucket(0.0), 0);
    assert_eq!(histogram_bucket(0.49), 0);
    assert_eq!(histogram_bucket(0.5), 1);
    assert_eq!(histogram_bucket(0.85), 3);
    assert_eq!(histogram_bucket(0.9), 4);
    assert_eq!(histogram_bucket(1.0), 4);
    assert_eq!(histogram_bucket(1.5), 4);
    assert_eq!(histogram_bucket(-0.1), 0);
}
