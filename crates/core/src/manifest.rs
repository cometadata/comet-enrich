//! Run manifest written at the end of an enrichment run.

// Manifest/Report are the public names of this module's primary types.
#![allow(clippy::module_name_repetitions)]

use crate::dedup::HashBits;
use crate::options::RunStats;
use crate::writer::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// File name of the run manifest, written inside the output directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Manifest schema version. Additive-only within a major version.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Run manifest with metadata, artifact paths, status, and stats.
#[derive(Debug, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub method: MethodInfo,
    /// Dedup-hash identity. Present only for lookup methods (the staged path);
    /// the transform path leaves it unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<HashInfo>,
    /// DOI name of the enrichment project, written to every record as `sourceId`.
    pub source_id: String,
    pub sources: BTreeMap<String, SourceRelease>,
    pub exit_status: String,
    pub artifact_paths: ArtifactPaths,
    pub report: Report,
}

/// Method identity recorded in the manifest.
#[derive(Debug, Serialize)]
pub struct MethodInfo {
    pub name: String,
    pub version: &'static str,
}

/// Dedup hash used by a lookup run.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct HashInfo {
    pub algorithm: &'static str,
    pub bits: u32,
}

impl From<HashBits> for HashInfo {
    fn from(bits: HashBits) -> Self {
        HashInfo {
            algorithm: "xxh3",
            bits: match bits {
                HashBits::Bits64 => 64,
                HashBits::Bits128 => 128,
            },
        }
    }
}

/// One data source and the date of the release the run consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRelease {
    pub release_date: String,
}

/// Paths the run produced, relative to the output directory.
#[derive(Debug, Serialize)]
pub struct ArtifactPaths {
    pub enrichments: String,
    pub enrichments_failed: String,
}

/// Quantitative run summary.
#[derive(Debug, Serialize)]
pub struct Report {
    pub counters: RunStats,
    pub coverage: Coverage,
    /// Present only for methods with a query stage (filled by the staged runner).
    #[serde(rename = "match", skip_serializing_if = "Option::is_none")]
    pub match_: Option<MatchSummary>,
    pub validation: Validation,
    /// Crate version that completed each stage. Present only for the staged
    /// path; `null` for a stage whose marker predates 0.4.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_versions: Option<StageVersions>,
    pub stage_timings_ms: StageTimings,
}

/// Crate version recorded by each completed stage.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StageVersions {
    pub extract: Option<String>,
    pub query: Option<String>,
    pub reconcile: Option<String>,
}

/// How much of the in-scope input was enriched.
#[derive(Debug, Serialize)]
pub struct Coverage {
    pub records_in_scope: u64,
    pub records_enriched: u64,
    pub coverage_rate: f64,
}

impl Coverage {
    /// Build coverage counts, using `0.0` for an empty in-scope corpus.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn new(records_in_scope: u64, records_enriched: u64) -> Self {
        let coverage_rate = if records_in_scope == 0 {
            0.0
        } else {
            records_enriched as f64 / records_in_scope as f64
        };
        Coverage {
            records_in_scope,
            records_enriched,
            coverage_rate,
        }
    }
}

/// Schema-validation outcome at the writer boundary.
#[derive(Debug, Serialize)]
pub struct Validation {
    pub emitted: u64,
    pub schema_failures: u64,
}

impl Validation {
    #[must_use]
    pub fn new(emitted: u64, schema_failures: u64) -> Self {
        Validation {
            emitted,
            schema_failures,
        }
    }
}

/// Match-service quality block.
#[derive(Debug, Serialize)]
pub struct MatchSummary {
    pub unique_inputs: u64,
    pub matched: u64,
    pub match_rate: f64,
    pub confidence_histogram: Vec<HistogramBucket>,
    pub failure_taxonomy: MatchFailureTaxonomy,
}

/// One bucket of the match-confidence histogram.
#[derive(Debug, Serialize)]
pub struct HistogramBucket {
    pub min: f64,
    pub max: f64,
    pub count: u64,
}

/// Why unique inputs failed to match.
#[derive(Debug, Default, Serialize)]
pub struct MatchFailureTaxonomy {
    pub no_match: u64,
    pub timeout: u64,
    pub error: u64,
}

impl MatchFailureTaxonomy {
    /// Inputs that were never resolved.
    #[must_use]
    pub fn lost(&self) -> u64 {
        self.timeout + self.error
    }
}

/// Manifest `exit_status` for a run that made a complete pass with no data loss.
pub const EXIT_SUCCESS: &str = "success";
/// Manifest `exit_status` for a run that lost data (a failed file, a source
/// line that could not be parsed, a schema failure, or a lost lookup),
/// emitted no records, or did not complete all stages.
pub const EXIT_PARTIAL: &str = "partial";

/// Derive a run's manifest `exit_status`.
#[must_use]
pub fn exit_status(
    files_failed: u64,
    lines_malformed: u64,
    schema_failures: u64,
    match_errors: u64,
    pipeline_complete: bool,
    emitted: u64,
) -> &'static str {
    // We currently treat an empty enrichment result as an error to prevent a bug
    // from causing the diff to retract all previous enrichments. However, an empty
    // result can be valid: if an upstream provider has accepted all enrichments
    // from the previous release, the current release may produce none. This may
    // need to be revisited again in the future.
    if stage_exit_status(files_failed, lines_malformed, schema_failures, match_errors)
        == EXIT_PARTIAL
        || !pipeline_complete
        || emitted == 0
    {
        EXIT_PARTIAL
    } else {
        EXIT_SUCCESS
    }
}

/// Derive a standalone stage's status from errors alone. Extract and query
/// need not complete the pipeline or emit enrichment records to succeed.
#[must_use]
pub fn stage_exit_status(
    files_failed: u64,
    lines_malformed: u64,
    schema_failures: u64,
    match_errors: u64,
) -> &'static str {
    if files_failed > 0 || lines_malformed > 0 || schema_failures > 0 || match_errors > 0 {
        EXIT_PARTIAL
    } else {
        EXIT_SUCCESS
    }
}

/// Wall time per stage, in milliseconds.
#[derive(Debug, Default, Clone, Serialize)]
pub struct StageTimings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extract: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconcile: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
}

/// Caller-supplied metadata folded into the manifest by [`Manifest::build`].
pub struct RunMeta {
    pub method_name: String,
    pub method_version: &'static str,
    /// DOI name of the enrichment project, in the ASCII-lowercase form written
    /// to every record's `sourceId`.
    pub source_id: String,
    pub sources: BTreeMap<String, SourceRelease>,
}

impl Manifest {
    /// Build the run manifest for a transform method.
    #[must_use]
    pub fn build(
        stats: &RunStats,
        meta: &RunMeta,
        out_of_scope: &[&str],
        timings: &StageTimings,
        exit_status: &str,
    ) -> Self {
        let out_of_scope_total: u64 = out_of_scope
            .iter()
            .filter_map(|reason| stats.skipped.get(*reason).copied())
            .sum();
        let records_in_scope = stats
            .records_scanned
            .saturating_sub(stats.duplicate_records)
            .saturating_sub(out_of_scope_total);

        let report = Report {
            counters: stats.clone(),
            coverage: Coverage::new(records_in_scope, stats.emitted),
            match_: None,
            validation: Validation::new(stats.emitted, stats.schema_failures),
            stage_versions: None,
            stage_timings_ms: timings.clone(),
        };
        Self::envelope(meta, None, exit_status, report)
    }

    /// Build the run manifest for a lookup method.
    #[must_use]
    pub fn from_report(meta: &RunMeta, exit_status: &str, report: Report, hash: HashInfo) -> Self {
        Self::envelope(meta, Some(hash), exit_status, report)
    }

    /// Wrap a finished [`Report`] in a [`Manifest`].
    fn envelope(meta: &RunMeta, hash: Option<HashInfo>, exit_status: &str, report: Report) -> Self {
        Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            method: MethodInfo {
                name: meta.method_name.clone(),
                version: meta.method_version,
            },
            hash,
            source_id: meta.source_id.clone(),
            sources: meta.sources.clone(),
            exit_status: exit_status.to_owned(),
            artifact_paths: ArtifactPaths {
                enrichments: format!("{ENRICHMENTS_DIR}/"),
                enrichments_failed: ENRICHMENTS_FAILED_FILE.to_owned(),
            },
            report,
        }
    }

    /// Serialize the manifest to `<output_dir>/manifest.json` (pretty JSON).
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be serialized or the file cannot be
    /// written.
    pub fn write(&self, output_dir: &Path) -> Result<()> {
        write_manifest_json(self, output_dir, "manifest")
    }
}

/// Serialize `value` as pretty JSON to `<output_dir>/manifest.json`.
///
/// `what` names the manifest kind in the serialization error context.
pub(crate) fn write_manifest_json<T: Serialize>(
    value: &T,
    output_dir: &Path,
    what: &str,
) -> Result<()> {
    let path = output_dir.join(MANIFEST_FILE);
    let json =
        serde_json::to_string_pretty(value).with_context(|| format!("serializing {what}"))?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::{SOURCE_ID, assert_close};
    use serde_json::json;

    #[test]
    fn hash_info_from_hash_bits_records_width() {
        let hash = HashInfo::from(HashBits::Bits128);

        assert_eq!(hash.algorithm, "xxh3");
        assert_eq!(hash.bits, 128);
    }

    #[test]
    fn coverage_new_zero_in_scope_has_zero_rate() {
        let coverage = Coverage::new(0, 5);

        assert_eq!(coverage.records_in_scope, 0);
        assert_eq!(coverage.records_enriched, 5);
        assert_close(coverage.coverage_rate, 0.0);
    }

    #[test]
    fn manifest_from_report_wraps_supplied_report_and_hash() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "datacite".to_owned(),
            SourceRelease {
                release_date: "2024-01-01".to_owned(),
            },
        );
        let meta = RunMeta {
            method_name: "affiliations".to_owned(),
            method_version: "test-version",
            source_id: SOURCE_ID.to_owned(),
            sources,
        };
        let report = Report {
            counters: RunStats {
                records_scanned: 3,
                emitted: 2,
                ..RunStats::default()
            },
            coverage: Coverage::new(3, 2),
            match_: None,
            validation: Validation::new(2, 0),
            stage_versions: None,
            stage_timings_ms: StageTimings {
                query: Some(10),
                ..StageTimings::default()
            },
        };

        let manifest = Manifest::from_report(
            &meta,
            EXIT_SUCCESS,
            report,
            HashInfo::from(HashBits::Bits128),
        );
        let value = serde_json::to_value(manifest).unwrap();

        assert_eq!(value["method"]["name"], json!("affiliations"));
        assert_eq!(value["source_id"], json!(SOURCE_ID));
        assert_eq!(
            value["sources"]["datacite"]["release_date"],
            json!("2024-01-01")
        );
        assert_eq!(value["exit_status"], json!("success"));
        assert_eq!(value["hash"]["algorithm"], json!("xxh3"));
        assert_eq!(value["hash"]["bits"], json!(128));
        assert_eq!(
            value["artifact_paths"]["enrichments"],
            json!("enrichments/")
        );
        assert_eq!(
            value["artifact_paths"]["enrichments_failed"],
            json!("enrichments.failed.jsonl")
        );
        assert_eq!(value["report"]["counters"]["records_scanned"], json!(3));
        assert_eq!(value["report"]["validation"]["emitted"], json!(2));
        assert_eq!(value["report"]["stage_timings_ms"]["query"], json!(10));
    }
}
