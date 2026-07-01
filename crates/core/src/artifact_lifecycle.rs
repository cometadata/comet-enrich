//! Filesystem helpers for run artifacts.

use anyhow::{Context, Result};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

/// Remove a file if it exists.
///
/// Missing files are fine. Other errors are surfaced because they normally mean
/// the output path is blocked by permissions or by a directory where a file should
/// be.
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

/// Atomically publish a marker file.
///
/// The caller should have invalidated the marker before the stage starts. Writing
/// via a temporary file avoids exposing the final marker path before the write has
/// succeeded.
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
    use comet_test_support::assert_err_contains;

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
