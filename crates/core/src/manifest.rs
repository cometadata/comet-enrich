//! Run manifest written at the end of an enrichment run.
//!
//! Each run writes a single `manifest.json` to the output directory: an audit
//! envelope (method identity, data sources, artifact paths, exit status) with all
//! the quantitative stats nested under a [`Report`] block (raw counters, coverage,
//! match quality, validation, and stage timings).
//!
//! The transform path (`reader::run`) produces a manifest with no `match` block;
//! the staged runner fills it in a later stage. A data-source `content_hash` and a
//! provenance fingerprint are intentionally not produced yet — they are deferred to
//! the hashing module.

// Manifest/Report are the public names of this module's primary types.
#![allow(clippy::module_name_repetitions)]

use crate::reader::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, RunStats};

use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

/// File name of the run manifest, written inside the output directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Manifest schema version. Additive-only within a major version.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// The run manifest: an audit envelope around a nested stats [`Report`].
#[derive(Debug, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub method: MethodInfo,
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
#[derive(Debug, Serialize)]
pub struct Coverage {
    pub records_in_scope: u64,
    pub records_enriched: u64,
    pub coverage_rate: f64,
}

/// Schema-validation outcome at the writer boundary.
#[derive(Debug, Serialize)]
pub struct Validation {
    pub emitted: u64,
    pub schema_failures: u64,
    pub schema_failure_taxonomy: BTreeMap<String, u64>,
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
    #[allow(clippy::cast_precision_loss)]
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
        let records_enriched = stats.emitted;
        let coverage_rate = if records_in_scope == 0 {
            0.0
        } else {
            records_enriched as f64 / records_in_scope as f64
        };

        Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            method: MethodInfo {
                name: meta.method_name.clone(),
                version: meta.method_version,
            },
            sources: meta.sources.clone(),
            exit_status: exit_status.to_owned(),
            artifact_paths: ArtifactPaths {
                enrichments: format!("{ENRICHMENTS_DIR}/"),
                enrichments_failed: ENRICHMENTS_FAILED_FILE.to_owned(),
            },
            report: Report {
                counters: stats.clone(),
                coverage: Coverage {
                    records_in_scope,
                    records_enriched,
                    coverage_rate,
                },
                match_: None,
                validation: Validation {
                    emitted: stats.emitted,
                    schema_failures: stats.schema_failures,
                    schema_failure_taxonomy: BTreeMap::new(),
                },
                stage_timings_ms: timings.clone(),
            },
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
