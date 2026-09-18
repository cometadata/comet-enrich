//! Filesystem helpers for run artifacts.

use crate::manifest::MANIFEST_FILE;
use crate::writer::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};
use anyhow::{Context, Result, bail};
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

/// Refuse an output path that overlaps any input, or two inputs that overlap.
///
/// Every run clears its output before reading its inputs, so an output that
/// aliases an input would destroy the data about to be read. Paths are compared
/// in canonical form so symlinks and `..` segments cannot hide an overlap. Each
/// input must exist; `output` need not, in which case symlinks and `..` are
/// still resolved through its non-existent tail.
///
/// `inputs` pairs each path with its CLI flag name (without the leading dashes)
/// so the error names both sides, e.g. `--output overlaps --new`.
pub(crate) fn ensure_disjoint(output: &Path, inputs: &[(&str, &Path)]) -> Result<()> {
    let output_canon = soft_canonicalize::soft_canonicalize(output)
        .with_context(|| format!("resolving --output {}", output.display()))?;
    let mut inputs_canon = Vec::with_capacity(inputs.len());
    for (label, path) in inputs {
        let canon = fs::canonicalize(path)
            .with_context(|| format!("resolving --{label} {}", path.display()))?;
        inputs_canon.push((*label, canon));
    }

    for (label, canon) in &inputs_canon {
        if output_canon.starts_with(canon) || canon.starts_with(&output_canon) {
            bail!(
                "--output overlaps --{label}: {} and {}",
                output.display(),
                canon.display()
            );
        }
    }
    for (i, (label_a, a)) in inputs_canon.iter().enumerate() {
        for (label_b, b) in &inputs_canon[i + 1..] {
            if a.starts_with(b) || b.starts_with(a) {
                bail!("--{label_a} overlaps --{label_b}: {}", a.display());
            }
        }
    }
    Ok(())
}

/// Publish a marker file via temporary file and rename. The body is the crate
/// version that completed the stage; markers written before 0.4 are empty.
pub(crate) fn write_marker(path: &Path) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, env!("CARGO_PKG_VERSION"))
        .with_context(|| format!("writing {}", tmp.display()))?;
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

    /// A temp root with two existing input dirs `a` and `b`.
    fn inputs() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        (root, a, b)
    }

    #[test]
    fn ensure_disjoint_accepts_separate_paths_even_when_output_is_missing() {
        let (root, a, b) = inputs();
        let out = root.path().join("not-yet-created").join("out");

        ensure_disjoint(&out, &[("old", &a), ("new", &b)]).unwrap();
    }

    #[test]
    fn ensure_disjoint_rejects_output_equal_to_an_input() {
        let (_root, a, b) = inputs();

        let err = ensure_disjoint(&b, &[("old", &a), ("new", &b)]).unwrap_err();

        let msg = format!("{err:#}");
        assert!(msg.contains("--output overlaps --new"), "{msg}");
    }

    #[test]
    fn ensure_disjoint_rejects_output_inside_an_input() {
        let (_root, a, b) = inputs();
        let out = a.join("nested").join("out");

        assert_err_contains(
            ensure_disjoint(&out, &[("old", &a), ("new", &b)]),
            "--output overlaps --old",
        );
    }

    #[test]
    fn ensure_disjoint_rejects_input_inside_output() {
        let (root, a, _b) = inputs();
        let out = root.path().to_path_buf();

        assert_err_contains(
            ensure_disjoint(&out, &[("input", &a)]),
            "--output overlaps --input",
        );
    }

    #[test]
    fn ensure_disjoint_rejects_two_equal_inputs() {
        let (root, a, _b) = inputs();
        let out = root.path().join("out");

        assert_err_contains(
            ensure_disjoint(&out, &[("old", &a), ("new", &a)]),
            "--old overlaps --new",
        );
    }

    #[cfg(unix)]
    #[test]
    fn ensure_disjoint_sees_through_a_symlink_alias_of_an_input() {
        let (root, a, b) = inputs();
        let link = root.path().join("alias");
        std::os::unix::fs::symlink(&a, &link).unwrap();
        // The output does not exist yet; only its symlinked parent does.
        let out = link.join("out");

        assert_err_contains(
            ensure_disjoint(&out, &[("old", &a), ("new", &b)]),
            "--output overlaps --old",
        );
    }

    #[test]
    fn ensure_disjoint_resolves_dotdot_in_a_missing_output_tail() {
        let (_root, a, b) = inputs();
        // "a/missing/.." resolves back to "a" even though "missing" does not exist.
        let out = a.join("missing").join("..");

        assert_err_contains(
            ensure_disjoint(&out, &[("old", &a), ("new", &b)]),
            "--output overlaps --old",
        );
    }

    #[test]
    fn ensure_disjoint_reports_a_missing_input() {
        let (root, a, _b) = inputs();
        let out = root.path().join("out");
        let missing = root.path().join("missing");

        assert_err_contains(
            ensure_disjoint(&out, &[("old", &a), ("new", &missing)]),
            "--new",
        );
    }
}
