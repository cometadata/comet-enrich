//! JSONL writer for enrichment records.
//!
//! Valid records go to the main output. Records that fail schema validation are
//! diverted to a separate failures file with their validator errors attached, so
//! one bad record does not abort the whole run.

use anyhow::{Context, Result};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct JsonlWriter {
    inner: BufWriter<File>,
    /// Failures file, opened on the first diverted record.
    failed_path: PathBuf,
    failed: Option<BufWriter<File>>,
    pub records_written: u64,
    pub records_failed: u64,
    validator: Option<jsonschema::JSONSchema>,
}

impl JsonlWriter {
    /// Create a JSONL writer.
    ///
    /// Records are validated before writing when `validator` is provided. Records
    /// that fail validation are diverted to `failed`, which is created on the first
    /// failure. Any failures file left over from a previous run into the same output
    /// directory is cleared first, so a clean run leaves no failures file.
    ///
    /// # Errors
    ///
    /// Returns an error if the output file cannot be created or a stale failures file
    /// cannot be removed.
    pub fn create(
        output: &Path,
        failed: &Path,
        validator: Option<jsonschema::JSONSchema>,
    ) -> Result<Self> {
        // A clean run leaves no failures file, so clear any left over from a previous
        // run; ensure_failed recreates it only if this run diverts a record.
        if failed.is_file() {
            std::fs::remove_file(failed)
                .with_context(|| format!("clearing stale {}", failed.display()))?;
        }
        Ok(Self {
            inner: open_buffered(output, 256 * 1024)?,
            failed_path: failed.to_path_buf(),
            failed: None,
            records_written: 0,
            records_failed: 0,
            validator,
        })
    }

    /// Write records as JSONL, validating each one first when a validator is set.
    ///
    /// Records that fail validation are written to the failures file as
    /// `{"record": <record>, "errors": [...]}` rather than aborting.
    ///
    /// # Errors
    ///
    /// Returns an error if a record cannot be written or the failures file cannot
    /// be created.
    pub fn write_batch(&mut self, batch: &[Value]) -> Result<()> {
        for rec in batch {
            // Resolve validation into an owned result so the borrow of `validator`
            // ends before we touch `inner` / `failed`.
            let errors: Option<Vec<String>> = self
                .validator
                .as_ref()
                .and_then(|v| v.validate(rec).err())
                .map(|errs| errs.map(|e| e.to_string()).collect());

            if let Some(msgs) = errors {
                let entry = serde_json::json!({ "record": rec.clone(), "errors": msgs });
                let w = self.ensure_failed()?;
                serde_json::to_writer(&mut *w, &entry)?;
                w.write_all(b"\n")?;
                self.records_failed += 1;
            } else {
                serde_json::to_writer(&mut self.inner, rec)?;
                self.inner.write_all(b"\n")?;
                self.records_written += 1;
            }
        }
        Ok(())
    }

    /// Open the failures file on first use.
    fn ensure_failed(&mut self) -> Result<&mut BufWriter<File>> {
        if self.failed.is_none() {
            self.failed = Some(open_buffered(&self.failed_path, 64 * 1024)?);
        }
        Ok(self.failed.as_mut().expect("failures writer just created"))
    }

    /// Flush buffered output.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying flush fails.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        if let Some(f) = &mut self.failed {
            f.flush()?;
        }
        Ok(())
    }
}

/// Open `path` for writing, wrapped in a sized buffer.
fn open_buffered(path: &Path, capacity: usize) -> Result<BufWriter<File>> {
    let f = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    Ok(BufWriter::with_capacity(capacity, f))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn jsonl_writer_writes_one_per_line() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.jsonl");
        let failed = dir.path().join("out.failed.jsonl");
        {
            let mut w = JsonlWriter::create(&out, &failed, None).unwrap();
            w.write_batch(&[json!({"a":1}), json!({"b":2})]).unwrap();
            w.flush().unwrap();
        }
        let s = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<_> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"a":1}"#);
        assert_eq!(lines[1], r#"{"b":2}"#);
        // No failures, so the failures file is never created.
        assert!(!failed.exists());
    }

    #[test]
    fn invalid_records_are_diverted() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.jsonl");
        let failed = dir.path().join("out.failed.jsonl");
        // Minimal schema: an object that requires property "a".
        let schema = crate::schema::compile_str(r#"{"type":"object","required":["a"]}"#).unwrap();

        let mut w = JsonlWriter::create(&out, &failed, Some(schema)).unwrap();
        w.write_batch(&[json!({"a":1}), json!({"b":2})]).unwrap();
        w.flush().unwrap();

        assert_eq!(w.records_written, 1);
        assert_eq!(w.records_failed, 1);

        let main = std::fs::read_to_string(&out).unwrap();
        assert_eq!(main.lines().count(), 1);
        assert_eq!(main.lines().next().unwrap(), r#"{"a":1}"#);

        let fail = std::fs::read_to_string(&failed).unwrap();
        let entry: Value = serde_json::from_str(fail.lines().next().unwrap()).unwrap();
        assert_eq!(entry["record"], json!({"b":2}));
        assert!(!entry["errors"].as_array().unwrap().is_empty());
    }

    #[test]
    fn rerun_clears_stale_failures_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.jsonl");
        let failed = dir.path().join("out.failed.jsonl");
        let schema_text = r#"{"type":"object","required":["a"]}"#;

        // First run diverts a record, so the failures file is left on disk.
        {
            let schema = crate::schema::compile_str(schema_text).unwrap();
            let mut w = JsonlWriter::create(&out, &failed, Some(schema)).unwrap();
            w.write_batch(&[json!({"b":2})]).unwrap();
            w.flush().unwrap();
        }
        assert!(failed.exists());

        // Rerun into the same paths with no failures: the stale file must be gone.
        {
            let schema = crate::schema::compile_str(schema_text).unwrap();
            let mut w = JsonlWriter::create(&out, &failed, Some(schema)).unwrap();
            w.write_batch(&[json!({"a":1})]).unwrap();
            w.flush().unwrap();
        }
        assert!(
            !failed.exists(),
            "stale failures file must be cleared on rerun"
        );
    }
}
