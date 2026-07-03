use super::{
    EXTRACT_STATS_FILE, EXTRACTIONS_DIR, HASH_BITS_FILE, INPUTS_FILE, INPUTS_FINGERPRINT_FILE,
    LOOKUPS_FAILED_FILE, LOOKUPS_FILE, RECONCILE_STATS_FILE, WORK_DIR,
};
use crate::artifact_lifecycle as lifecycle;
use crate::dedup::HashBits;

use anyhow::{Context, Result, bail};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

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

/// Work directory for a staged lookup run.
pub struct WorkDir {
    pub path: PathBuf,
}

impl WorkDir {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The work directory for a run output directory.
    #[must_use]
    pub fn for_output(output_dir: &Path) -> Self {
        Self::new(output_dir.join(WORK_DIR))
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
/// Whether a staged run directory has completed all stages.
#[must_use]
pub fn pipeline_complete(output_dir: &Path) -> bool {
    WorkDir::for_output(output_dir).all_complete()
}

/// Pin the dedup-hash width in the run dir, or validate it against a resume.
pub(super) fn pin_or_validate_hash_bits(
    work: &Path,
    hash_bits: HashBits,
    from_scratch: bool,
) -> Result<()> {
    let path = work.join(HASH_BITS_FILE);
    if path.exists() && !from_scratch {
        let pinned =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        let pinned = pinned.trim();
        if pinned != hash_bits.as_str() {
            bail!(
                "hash-width mismatch: run dir {} is pinned to {pinned}, but --hash-bits requested {}; \
                 resuming with a different width would silently break the hash join (use --from-scratch to rerun)",
                work.display(),
                hash_bits.as_str(),
            );
        }
        Ok(())
    } else {
        fs::write(&path, hash_bits.as_str())
            .with_context(|| format!("pinning hash width to {}", path.display()))
    }
}
/// Require predecessor stages for an explicit single-stage run.
pub(super) fn ensure_predecessors_done(wd: &WorkDir, stage: Stage) -> Result<()> {
    let needed: &[Stage] = match stage {
        Stage::Extract => &[],
        Stage::Query => &[Stage::Extract],
        Stage::Reconcile => &[Stage::Extract, Stage::Query],
    };
    for &dep in needed {
        if !wd.is_complete(dep) {
            bail!(
                "cannot run {} stage: {} has not completed (missing {})",
                stage.marker().trim_end_matches(".done"),
                dep.marker().trim_end_matches(".done"),
                dep.marker(),
            );
        }
    }
    Ok(())
}

/// Clear markers and artifacts invalidated by rerunning `stage`.
pub(super) fn prepare_stage_rerun(
    wd: &WorkDir,
    stage: Stage,
    work: &Path,
    output: &Path,
) -> Result<()> {
    clear_markers_from(wd, stage)?;
    match stage {
        Stage::Extract => {
            clear_extract_artifacts(work)?;
            clear_query_artifacts(work)?;
            clear_reconcile_artifacts(work, output)?;
        }
        Stage::Query => {
            clear_query_artifacts(work)?;
            clear_reconcile_artifacts(work, output)?;
        }
        Stage::Reconcile => {
            clear_reconcile_artifacts(work, output)?;
        }
    }
    Ok(())
}

fn clear_markers_from(wd: &WorkDir, stage: Stage) -> Result<()> {
    let stages: &[Stage] = match stage {
        Stage::Extract => &Stage::ALL,
        Stage::Query => &[Stage::Query, Stage::Reconcile],
        Stage::Reconcile => &[Stage::Reconcile],
    };
    for &stage in stages {
        lifecycle::remove_file_if_exists(&wd.marker_path(stage))?;
    }
    Ok(())
}

fn clear_extract_artifacts(work: &Path) -> Result<()> {
    lifecycle::recreate_dir(&work.join(EXTRACTIONS_DIR))?;
    lifecycle::remove_file_if_exists(&work.join(INPUTS_FILE))?;
    lifecycle::remove_file_if_exists(&work.join(INPUTS_FINGERPRINT_FILE))?;
    lifecycle::remove_file_if_exists(&work.join(EXTRACT_STATS_FILE))?;
    Ok(())
}

fn clear_query_artifacts(work: &Path) -> Result<()> {
    lifecycle::remove_file_if_exists(&work.join(LOOKUPS_FILE))?;
    lifecycle::remove_file_if_exists(&work.join(LOOKUPS_FAILED_FILE))?;
    Ok(())
}

fn clear_reconcile_artifacts(work: &Path, output: &Path) -> Result<()> {
    lifecycle::remove_file_if_exists(&work.join(RECONCILE_STATS_FILE))?;
    lifecycle::clear_run_outputs(output)
}

pub(super) fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}
