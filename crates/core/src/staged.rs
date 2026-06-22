//! Staged execution for lookup methods (extract → query → reconcile).
//!
//! Lookup methods are too expensive to redo from scratch during local debugging, so each
//! stage drops a marker in the work dir on success and a unified run skips the stages already
//! marked complete. The stage bodies live with each method and aren't wired yet; the work-dir
//! layout and resume planning live here.

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
    /// The stages in execution order.
    pub const ALL: [Stage; 3] = [Stage::Extract, Stage::Query, Stage::Reconcile];

    /// Name of the marker file written when this stage completes.
    #[must_use]
    pub fn marker(self) -> &'static str {
        match self {
            Stage::Extract => "extract.done",
            Stage::Query => "query.done",
            Stage::Reconcile => "reconcile.done",
        }
    }
}

/// Connection and batching configuration for a lookup method's match-service calls.
///
/// Built by the CLI from its lookup flags and handed to a lookup method's constructor. The
/// match pipeline that consumes it is not wired yet.
pub struct LookupConfig {
    /// Base URL of the match service (Marple).
    pub match_url: String,
    /// ROR data dump (JSON) used to reconcile matches.
    pub ror_data: PathBuf,
    /// Directory for intermediate stage artifacts; a temporary one is used if omitted.
    pub work_dir: Option<PathBuf>,
    /// Inputs per match-service bulk request.
    pub ror_batch_size: usize,
    /// Concurrent in-flight match requests.
    pub concurrency: usize,
    /// Match-service request timeout (seconds).
    pub timeout: u64,
    /// Ignore any existing work-dir artifacts and run every stage from scratch.
    pub restart: bool,
}

/// A directory holding a lookup run's intermediate artifacts and stage markers.
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

    /// Whether `stage`'s marker is present.
    #[must_use]
    pub fn is_complete(&self, stage: Stage) -> bool {
        self.marker_path(stage).exists()
    }
}

/// The stages a unified run should execute: skip the already-complete leading stages unless
/// `restart` is set. Once a stage runs, every later stage runs too (a re-run of an earlier
/// stage invalidates the ones after it).
#[must_use]
pub fn stages_to_run(work_dir: &Path, restart: bool) -> Vec<Stage> {
    if restart {
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
