//! Filesystem helpers for run artifacts.

use crate::manifest::MANIFEST_FILE;
use crate::writer::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};
use anyhow::{Context, Result};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

/// Clear public outputs from a previous run.
pub(crate) fn clear_run_outputs(output: &Path) -> Result<()> {
    remove_file_if_exists(&output.join(MANIFEST_FILE))?;
    recreate_dir(&output.join(ENRICHMENTS_DIR))?;
    remove_file_if_exists(&output.join(ENRICHMENTS_FAILED_FILE))?;
    Ok(())
}

/// Remove a file if it exists.
pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

/// Remove a directory tree if it exists.
pub(crate) fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

/// Remove and recreate a directory.
pub(crate) fn recreate_dir(path: &Path) -> Result<()> {
    remove_dir_if_exists(path)?;
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))
}

/// Publish a marker file via temporary file and rename.
pub(crate) fn write_marker(path: &Path) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, b"").with_context(|| format!("writing {}", tmp.display()))?;
    remove_file_if_exists(path)?;
    fs::rename(&tmp, path).with_context(|| format!("publishing marker {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::assert_err_contains;

    #[test]
    fn remove_dir_if_exists_missing_directory_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");

        remove_dir_if_exists(&missing).unwrap();
    }

    #[test]
    fn remove_dir_if_exists_file_path_reports_error() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        fs::write(&file, b"not a directory").unwrap();

        assert_err_contains(remove_dir_if_exists(&file), "removing");
    }
}
