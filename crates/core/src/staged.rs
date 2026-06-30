//! Stage planning for lookup methods.
//!
//! Lookup methods run as extract, query, and reconcile stages. Each completed
//! stage writes a marker file in the work directory, allowing later runs to skip
//! completed leading stages unless `from_scratch` is set.

use crate::dedup::HashBits;
use std::path::{Path, PathBuf};

/// One stage of a lookup pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Scan the corpus and collect the unique inputs to look up.
    Extract,
    /// Resolve the unique inputs against the match service.
    Query,
    /// Join matches back onto records and emit enrichment records.
    Reconcile,
}

impl Stage {
    /// Stages in execution order.
    pub const ALL: [Stage; 3] = [Stage::Extract, Stage::Query, Stage::Reconcile];

    /// Marker file written when this stage completes.
    #[must_use]
    pub fn marker(self) -> &'static str {
        match self {
            Stage::Extract => "extract.done",
            Stage::Query => "query.done",
            Stage::Reconcile => "reconcile.done",
        }
    }
}

/// Match-service configuration for a lookup method.
pub struct LookupConfig {
    /// Base URL of the ROR match service.
    pub ror_service_url: String,
    /// ROR registry dataset used to reconcile matched IDs.
    pub ror_file: PathBuf,
    /// Inputs per match-service request.
    pub ror_batch_size: usize,
    /// Concurrent match-service requests.
    pub ror_concurrency: usize,
    /// Match-service request timeout in seconds.
    pub ror_timeout: u64,
    /// Width of the content-addressed dedup hash. Fixed for a whole run: the runner
    /// keys `inputs.jsonl`/`lookups.jsonl` at this width and a method's `extract`
    /// hashes occurrences at the same width so `map_back` can index the results.
    pub hash_bits: HashBits,
    /// Ignore existing stage outputs and rerun from the start.
    pub from_scratch: bool,
}

/// Work directory for a staged lookup run.
pub struct WorkDir {
    pub path: PathBuf,
}

impl WorkDir {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn marker_path(&self, stage: Stage) -> PathBuf {
        self.path.join(stage.marker())
    }

    /// Return whether the stage marker exists.
    #[must_use]
    pub fn is_complete(&self, stage: Stage) -> bool {
        self.marker_path(stage).exists()
    }

    /// Return whether every stage of the pipeline has completed.
    #[must_use]
    pub fn all_complete(&self) -> bool {
        Stage::ALL.iter().all(|&s| self.is_complete(s))
    }
}

/// Return the stages that should run.
///
/// Completed leading stages are skipped. Once a stage needs to run, all later
/// stages run too, because rerunning an earlier stage invalidates later outputs.
#[must_use]
pub fn stages_to_run(work_dir: &Path, from_scratch: bool) -> Vec<Stage> {
    if from_scratch {
        return Stage::ALL.to_vec();
    }
    let wd = WorkDir::new(work_dir);
    Stage::ALL
        .iter()
        .skip_while(|&&s| wd.is_complete(s))
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn restart_runs_everything() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
        assert_eq!(stages_to_run(dir.path(), true), Stage::ALL);
    }

    #[test]
    fn resume_skips_completed_leading_stages() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(Stage::Extract.marker()), "").unwrap();
        assert_eq!(
            stages_to_run(dir.path(), false),
            vec![Stage::Query, Stage::Reconcile]
        );
    }

    #[test]
    fn empty_work_dir_runs_all() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(stages_to_run(dir.path(), false), Stage::ALL);
    }
}
