//! Resumable checkpoint of already-processed lookup inputs.
//!
//! A run records the hash of each input it has resolved against the match service;
//! a later run loads the checkpoint and skips inputs whose hash is already present,
//! so an interrupted query stage can resume rather than restart. The on-disk format
//! is newline-separated hex hashes (as produced by [`crate::hash_input`]); the
//! checkpoint itself is hash-agnostic.
//!
//! Ported from the prototype query stage. The *cadence* of [`Checkpoint::save`]
//! (once at the end vs incrementally per batch) is the staged runner's decision.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// A set of processed input hashes, persisted to a `*.checkpoint` file.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    path: PathBuf,
    processed: HashSet<String>,
}

impl Checkpoint {
    /// Open the checkpoint for `path`.
    ///
    /// With `from_scratch`, starts empty and overwrites any existing file on the
    /// next [`Checkpoint::save`]. Otherwise loads the existing processed-hash set
    /// (or starts empty if the file does not exist) so a run can resume. Taking the
    /// resume decision as a required argument means a resumed run cannot accidentally
    /// start empty and truncate prior progress.
    ///
    /// # Errors
    ///
    /// Returns an error if `from_scratch` is false and an existing file cannot be read.
    pub fn open<P: AsRef<Path>>(path: P, from_scratch: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut processed = HashSet::new();

        if !from_scratch && path.exists() {
            let file =
                File::open(&path).with_context(|| format!("opening {}", path.display()))?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line.with_context(|| format!("reading {}", path.display()))?;
                // Trim so CRLF or stray-whitespace lines don't become bogus hashes.
                let hash = line.trim();
                if !hash.is_empty() {
                    processed.insert(hash.to_owned());
                }
            }
        }

        Ok(Self { path, processed })
    }

    /// Mark an input hash as processed (in memory; persist with [`Checkpoint::save`]).
    pub fn mark_processed(&mut self, hash: &str) {
        self.processed.insert(hash.to_owned());
    }

    /// Whether an input hash has already been processed.
    #[must_use]
    pub fn is_processed(&self, hash: &str) -> bool {
        self.processed.contains(hash)
    }

    /// Write the processed-hash set to the checkpoint file, one hash per line.
    ///
    /// Rewrites the whole file atomically: the full set is written to a sibling
    /// `.tmp` file which is then renamed over the checkpoint, so a crash mid-write
    /// leaves the previous checkpoint intact rather than an empty/partial file.
    ///
    /// # Errors
    ///
    /// Returns an error if the temp file cannot be created/written or the rename
    /// fails.
    pub fn save(&self) -> Result<()> {
        // Sibling temp file (same directory => same filesystem, required for an
        // atomic rename). Append `.tmp`; `with_extension` would replace `.checkpoint`.
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);

        {
            let file =
                File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
            let mut writer = BufWriter::new(file);
            for hash in &self.processed {
                writeln!(writer, "{hash}")?;
            }
            writer
                .flush()
                .with_context(|| format!("flushing {}", tmp.display()))?;
        }

        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("replacing {}", self.path.display()))?;
        Ok(())
    }

    /// Number of processed hashes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.processed.len()
    }

    /// Whether no hashes have been processed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.processed.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_and_query() {
        let mut cp = Checkpoint::open("unused.checkpoint", true).unwrap();
        assert!(cp.is_empty());
        assert!(!cp.is_processed("abc"));
        cp.mark_processed("abc");
        cp.mark_processed("abc"); // idempotent
        assert!(cp.is_processed("abc"));
        assert_eq!(cp.len(), 1);
        assert!(!cp.is_empty());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lookups.checkpoint");

        let mut cp = Checkpoint::open(&path, true).unwrap();
        cp.mark_processed("02ad37d94c7ac3af");
        cp.mark_processed("5cfc385e6671f0a657c3834cafabbb94");
        cp.save().unwrap();

        let loaded = Checkpoint::open(&path, false).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.is_processed("02ad37d94c7ac3af"));
        assert!(loaded.is_processed("5cfc385e6671f0a657c3834cafabbb94"));
        assert!(!loaded.is_processed("deadbeefdeadbeef"));
    }

    #[test]
    fn save_replaces_existing_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lookups.checkpoint");

        // First save.
        let mut cp = Checkpoint::open(&path, true).unwrap();
        cp.mark_processed("aaaa");
        cp.save().unwrap();

        // Resume, add more, save again over the existing file.
        let mut cp = Checkpoint::open(&path, false).unwrap();
        cp.mark_processed("bbbb");
        cp.save().unwrap();

        // The rewrite holds the full union, and the temp file is gone.
        let loaded = Checkpoint::open(&path, false).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.is_processed("aaaa"));
        assert!(loaded.is_processed("bbbb"));
        let temps: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .filter(|n| n.to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(temps.is_empty(), "leftover temp files: {temps:?}");
    }

    #[test]
    fn from_scratch_ignores_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lookups.checkpoint");
        std::fs::write(&path, "abc\ndef\n").unwrap();
        let cp = Checkpoint::open(&path, true).unwrap();
        assert!(cp.is_empty());
        assert!(!cp.is_processed("abc"));
    }

    #[test]
    fn open_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let cp = Checkpoint::open(dir.path().join("absent.checkpoint"), false).unwrap();
        assert!(cp.is_empty());
    }

    #[test]
    fn load_ignores_blank_and_whitespace_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lookups.checkpoint");
        // CRLF endings, a blank line, and a whitespace-only line.
        std::fs::write(&path, "abc\r\n\n   \ndef\n").unwrap();
        let cp = Checkpoint::open(&path, false).unwrap();
        assert_eq!(cp.len(), 2);
        assert!(cp.is_processed("abc"));
        assert!(cp.is_processed("def"));
    }
}
