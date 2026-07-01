//! Run manifest written at the end of an enrichment run.

// Manifest/Report are the public names of this module's primary types.
#![allow(clippy::module_name_repetitions)]

use crate::dedup::HashBits;
use crate::run::RunStats;
use crate::writer::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};

use anyhow::{Context, Result};
use serde::Serialize;
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

/// The content-addressed dedup hash a lookup run used. A mismatched width across a
/// resume silently breaks the hash join, so it is pinned and recorded here.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct HashInfo {
    pub algorithm: &'static str,
    pub bits: u32,
}

impl From<HashBits> for HashInfo {
    fn from(bits: HashBits) -> Self {
        HashInfo {
            algorithm: bits.as_str(),
            bits: match bits {
                HashBits::Bits64 => 64,
                HashBits::Bits128 => 128,
            },
        }
    }
}

/// One data source and the date of the release the run consumed.
#[derive(Debug, Clone, Serialize)]
pub struct SourceRelease {
    pub release_date: String,
}

/// Paths the run produced, relative to the output directory.
#[derive(Debug, Serialize)]
pub struct ArtifactPaths {
    pub enrichments: String,
    pub enrichments_failed: String,
}

/// The standardized stats block: everything quantitative about the run.
#[derive(Debug, Serialize)]
pub struct Report {
    pub counters: RunStats,
    pub coverage: Coverage,
    /// Present only for methods with a query stage (filled by the staged runner).
    #[serde(rename = "match", skip_serializing_if = "Option::is_none")]
    pub match_: Option<MatchSummary>,
    pub validation: Validation,
    pub stage_timings_ms: StageTimings,
}

/// How much of the in-scope corpus the method enriched.
///
/// "In scope" is record-level on the transform path (records the extractor
/// selected) and unit-level on the staged path (each extraction — a person, a
/// funding reference). `records_enriched` is the count of enrichment records
/// emitted; since each unit yields at most one record, `coverage_rate` stays a true
/// fraction in `[0, 1]`.
#[derive(Debug, Serialize)]
pub struct Coverage {
    pub records_in_scope: u64,
    pub records_enriched: u64,
    pub coverage_rate: f64,
}

impl Coverage {
    /// Coverage with the enrichment rate derived from the two counts (an empty
    /// in-scope corpus yields a rate of `0.0` rather than dividing by zero).
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
    pub schema_failure_taxonomy: BTreeMap<String, u64>,
}

impl Validation {
    /// Validation counts with an empty failure taxonomy.
    #[must_use]
    pub fn new(emitted: u64, schema_failures: u64) -> Self {
        Validation {
            emitted,
            schema_failures,
            schema_failure_taxonomy: BTreeMap::new(),
        }
    }
}

/// Match-service quality block. Populated by the staged runner; defined here so
/// the report shape is stable across methods.
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

/// Manifest `exit_status` for a run that made a complete pass with no data loss.
pub const EXIT_SUCCESS: &str = "success";
/// Manifest `exit_status` for a run that lost data or did not complete all stages.
pub const EXIT_PARTIAL: &str = "partial";

/// Derive a run's manifest `exit_status`.
///
/// A run is [`EXIT_SUCCESS`] only when it made a complete pass with no data-losing
/// condition. Any input-file read failure, schema-validation failure, match-service
/// batch error, or an incomplete staged pipeline downgrades it to [`EXIT_PARTIAL`],
/// so the manifest never certifies a lossy run as a full success.
#[must_use]
pub fn exit_status(
    files_failed: u64,
    schema_failures: u64,
    match_errors: u64,
    pipeline_complete: bool,
) -> &'static str {
    if files_failed > 0 || schema_failures > 0 || match_errors > 0 || !pipeline_complete {
        EXIT_PARTIAL
    } else {
        EXIT_SUCCESS
    }
}

/// Wall time per stage, in milliseconds. The transform path sets only `total`;
/// the staged runner fills the per-stage fields.
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
    pub sources: BTreeMap<String, SourceRelease>,
}

impl Manifest {
    /// Build the run manifest from the run counters and caller metadata.
    ///
    /// `out_of_scope` lists the skip reasons that mean the method's extractor did
    /// not select a record; those are subtracted from `records_scanned` to get
    /// `records_in_scope`. `records_enriched` is `emitted` on the transform path
    /// (one enrichment per record).
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
        let records_in_scope = stats.records_scanned.saturating_sub(out_of_scope_total);

        let report = Report {
            counters: stats.clone(),
            coverage: Coverage::new(records_in_scope, stats.emitted),
            match_: None,
            validation: Validation::new(stats.emitted, stats.schema_failures),
            stage_timings_ms: timings.clone(),
        };
        Self::envelope(meta, None, exit_status, report)
    }

    /// Build the run manifest for a lookup method from a [`Report`] the staged
    /// runner already assembled (coverage, match block, validation, stage timings).
    ///
    /// `hash` records the dedup-hash width the run was pinned to.
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
        let path = output_dir.join(MANIFEST_FILE);
        let json = serde_json::to_string_pretty(self).context("serializing manifest")?;
        std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}
