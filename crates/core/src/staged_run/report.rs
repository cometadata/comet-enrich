use super::extract::ExtractStats;
use super::planning::{Stage, WorkDir};
use super::query::FAIL_KIND_NO_MATCH;
use super::reconcile::ReconcileStats;
use super::{
    EXTRACT_STATS_FILE, INPUTS_FILE, LOOKUPS_FAILED_FILE, LOOKUPS_FILE, RECONCILE_STATS_FILE,
    for_each_jsonl,
};
use crate::manifest::{
    Coverage, HistogramBucket, MatchFailureTaxonomy, MatchSummary, Report, StageTimings, Validation,
};
use crate::options::RunStats;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Match-confidence histogram edges. The last bucket includes `1.0`.
const HISTOGRAM_EDGES: [f64; 6] = [0.0, 0.5, 0.7, 0.8, 0.9, 1.0];
/// Assemble a [`Report`] from persisted stage stats.
pub(super) fn build_report(work: &Path, wd: &WorkDir, timings: StageTimings) -> Result<Report> {
    let extract: ExtractStats = read_stats(
        &work.join(EXTRACT_STATS_FILE),
        "extract.stats.json",
        wd.is_complete(Stage::Extract),
    )?;
    let reconcile: ReconcileStats = read_stats(
        &work.join(RECONCILE_STATS_FILE),
        "reconcile.stats.json",
        wd.is_complete(Stage::Reconcile),
    )?;

    let counters = RunStats {
        files_processed: extract.files_processed,
        files_failed: extract.files_failed,
        records_scanned: extract.records_scanned,
        lines_malformed: extract.lines_malformed,
        emitted: reconcile.emitted,
        schema_failures: reconcile.schema_failures,
        skipped: extract.skipped,
    };

    let match_ = if wd.is_complete(Stage::Query) {
        Some(build_match_summary(work)?)
    } else {
        None
    };

    Ok(Report {
        counters,
        coverage: Coverage::new(extract.in_scope_units, reconcile.emitted),
        match_,
        validation: Validation::new(reconcile.emitted, reconcile.schema_failures),
        stage_timings_ms: timings,
    })
}

/// Read a persisted stats sidecar, defaulting to empty when the stage hasn't run.
fn read_stats<T: DeserializeOwned + Default>(path: &Path, what: &str, required: bool) -> Result<T> {
    if !path.exists() {
        if required {
            bail!(
                "{what} is missing even though its stage is marked complete ({})",
                path.display()
            );
        }
        return Ok(T::default());
    }
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parsing {what}"))
}

/// One `lookups.jsonl` row, read back only for its confidence (other fields ignored).
#[derive(Deserialize)]
struct ConfidenceRow {
    #[serde(default)]
    confidence: Option<f64>,
}

/// Failed lookup row fields needed for reporting.
#[derive(Deserialize)]
struct FailureRow {
    /// Absent on rows written by builds that predate the `kind` field.
    #[serde(default)]
    kind: Option<String>,
    error: String,
}

/// Compute the match-quality block from lookup artifacts.
#[allow(clippy::cast_precision_loss)]
fn build_match_summary(work: &Path) -> Result<MatchSummary> {
    let unique_inputs = count_lines(&work.join(INPUTS_FILE))?;

    let mut matched: u64 = 0;
    let mut buckets = vec![0u64; HISTOGRAM_EDGES.len() - 1];
    for_each_jsonl(&work.join(LOOKUPS_FILE), |row: ConfidenceRow| {
        matched += 1;
        if let Some(c) = row.confidence {
            buckets[histogram_bucket(c)] += 1;
        }
    })?;

    let mut taxonomy = MatchFailureTaxonomy::default();
    for_each_jsonl(&work.join(LOOKUPS_FAILED_FILE), |row: FailureRow| {
        classify_failure(row.kind.as_deref(), &row.error, &mut taxonomy);
    })?;

    let confidence_histogram = HISTOGRAM_EDGES
        .windows(2)
        .zip(&buckets)
        .map(|(edge, &count)| HistogramBucket {
            min: edge[0],
            max: edge[1],
            count,
        })
        .collect();

    let match_rate = if unique_inputs == 0 {
        0.0
    } else {
        matched as f64 / unique_inputs as f64
    };

    Ok(MatchSummary {
        unique_inputs,
        matched,
        match_rate,
        confidence_histogram,
        failure_taxonomy: taxonomy,
    })
}

/// Index of the histogram bucket a confidence falls in (clamped to the range).
pub(super) fn histogram_bucket(c: f64) -> usize {
    // Edges are ascending; the last bucket is inclusive of the upper bound.
    for i in (0..HISTOGRAM_EDGES.len() - 1).rev() {
        if c >= HISTOGRAM_EDGES[i] {
            return i;
        }
    }
    0
}

/// Bin one failed lookup row.
///
/// Missing legacy `kind` values count as errors, never no-matches.
pub(super) fn classify_failure(
    kind: Option<&str>,
    error: &str,
    taxonomy: &mut MatchFailureTaxonomy,
) {
    if kind == Some(FAIL_KIND_NO_MATCH) {
        taxonomy.no_match += 1;
        return;
    }
    let lower = error.to_ascii_lowercase();
    if lower.contains("timeout") || lower.contains("timed out") {
        taxonomy.timeout += 1;
    } else {
        taxonomy.error += 1;
    }
}

/// Count the non-empty lines in a JSONL file without parsing them (absent → 0).
fn count_lines(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut n = 0;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if !line.trim().is_empty() {
            n += 1;
        }
    }
    Ok(n)
}
