//! Writers for enrichment records.
//!
//! Valid records go to a per-input gzip output part. Records that fail schema
//! validation are diverted to a single shared failures file with their validator
//! errors attached, so one bad record does not abort the whole run.

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Directory holding the gzip enrichment output parts, written inside the output
/// directory as `enrichments/part_NNNN.jsonl.gz` (one part per input file).
pub const ENRICHMENTS_DIR: &str = "enrichments";
/// File name for records diverted after failing schema validation.
pub const ENRICHMENTS_FAILED_FILE: &str = "enrichments.failed.jsonl";

/// Shared sink for records that fail schema validation.
///
/// One failures file serves the whole run, so all workers share a single
/// [`FailureSink`] behind a [`Mutex`]. Validation failures are rare, so lock
/// contention is negligible. The file is created lazily on the first diverted
/// record.
pub struct FailureSink {
    /// Path the failures file is created at on first use.
    failed_path: PathBuf,
    /// Failures file, opened on the first diverted record.
    failed: Option<BufWriter<File>>,
    pub records_failed: u64,
}

impl FailureSink {
    /// Create a failures sink.
    ///
    /// Any failures file left over from a previous run into the same output
    /// directory is cleared first, so a clean run leaves no failures file;
    /// [`FailureSink::divert`] recreates it only if this run diverts a record.
    ///
    /// # Errors
    ///
    /// Returns an error if a stale failures file cannot be removed.
    pub fn create(failed: &Path) -> Result<Self> {
        // A clean run leaves no failures file, so clear any left over from a previous
        // run; divert recreates it only if this run diverts a record.
        if failed.is_file() {
            std::fs::remove_file(failed)
                .with_context(|| format!("clearing stale {}", failed.display()))?;
        }
        Ok(Self {
            failed_path: failed.to_path_buf(),
            failed: None,
            records_failed: 0,
        })
    }

    /// Divert a record that failed validation, recording its validator errors.
    ///
    /// Writes `{"record": <record>, "errors": [...]}` and opens the failures file
    /// on first use.
    ///
    /// # Errors
    ///
    /// Returns an error if the failures file cannot be created or written.
    pub fn divert(&mut self, record: &Value, errors: &[String]) -> Result<()> {
        let entry = serde_json::json!({ "record": record, "errors": errors });
        let w = self.ensure_failed()?;
        serde_json::to_writer(&mut *w, &entry)?;
        w.write_all(b"\n")?;
        self.records_failed += 1;
        Ok(())
    }

    /// Open the failures file on first use.
    fn ensure_failed(&mut self) -> Result<&mut BufWriter<File>> {
        if self.failed.is_none() {
            self.failed = Some(open_buffered(&self.failed_path, 64 * 1024)?);
        }
        Ok(self.failed.as_mut().expect("failures writer just created"))
    }

    /// Flush buffered failures output. A no-op when no record was diverted.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying flush fails.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(f) = &mut self.failed {
            f.flush()?;
        }
        Ok(())
    }
}

/// Writes schema-valid enrichment records to one gzip output part.
///
/// Each worker owns one `PartWriter` for the input file it processes, so parts are
/// written in parallel with no shared output writer. Records are buffered with
/// [`PartWriter::push`] and flushed to the part in `batch_size` chunks; each record
/// is validated before writing when a validator is provided, and failures are
/// diverted to the shared [`FailureSink`].
pub struct PartWriter<'a> {
    inner: GzEncoder<BufWriter<File>>,
    validator: Option<&'a jsonschema::JSONSchema>,
    failures: &'a Mutex<FailureSink>,
    /// Records buffered since the last flush; drained in `batch_size` chunks.
    batch: Vec<Value>,
    /// Flush threshold: the batch is written once it reaches this many records.
    batch_size: usize,
    pub records_written: u64,
}

impl<'a> PartWriter<'a> {
    /// Create a gzip output part at `path`, flushing buffered records to it in
    /// chunks of `batch_size`.
    ///
    /// # Errors
    ///
    /// Returns an error if the part file cannot be created.
    pub fn create(
        path: &Path,
        validator: Option<&'a jsonschema::JSONSchema>,
        failures: &'a Mutex<FailureSink>,
        batch_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            inner: GzEncoder::new(open_buffered(path, 256 * 1024)?, Compression::default()),
            validator,
            failures,
            batch: Vec::with_capacity(batch_size),
            batch_size: batch_size.max(1),
            records_written: 0,
        })
    }

    /// Buffer one record, flushing the batch to the part once it reaches
    /// `batch_size` records.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing the batch fails (a record cannot be written or
    /// the failures file cannot be created).
    pub fn push(&mut self, record: Value) -> Result<()> {
        self.batch.push(record);
        if self.batch.len() >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Write the buffered batch to the part, keeping its allocation for reuse.
    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let mut batch = std::mem::take(&mut self.batch);
        let result = self.write_batch(&batch);
        batch.clear();
        self.batch = batch;
        result
    }

    /// Write records as JSONL, validating each one first when a validator is set.
    ///
    /// Valid records are written to this part. Records that fail validation are
    /// diverted to the shared failures sink rather than aborting.
    fn write_batch(&mut self, batch: &[Value]) -> Result<()> {
        for rec in batch {
            // Resolve validation into an owned result so the borrow of `validator`
            // ends before we touch `inner` / `failures`.
            let errors: Option<Vec<String>> = self
                .validator
                .and_then(|v| v.validate(rec).err())
                .map(|errs| errs.map(|e| e.to_string()).collect());

            if let Some(msgs) = errors {
                self.failures.lock().unwrap().divert(rec, &msgs)?;
            } else {
                serde_json::to_writer(&mut self.inner, rec)?;
                self.inner.write_all(b"\n")?;
                self.records_written += 1;
            }
        }
        Ok(())
    }

    /// Flush any buffered records, finish the gzip stream, and flush the part to
    /// disk, returning the number of records written.
    ///
    /// # Errors
    ///
    /// Returns an error if a buffered record cannot be written, the gzip trailer
    /// cannot be written, or the underlying file cannot be flushed.
    pub fn finish(mut self) -> Result<u64> {
        // Flush records still buffered below the batch threshold.
        self.flush_batch()?;
        // `finish` writes the gzip trailer into the BufWriter, which may still hold
        // those bytes, so flush it explicitly rather than relying on Drop (which
        // would swallow the error).
        let mut buf = self.inner.finish()?;
        buf.flush()?;
        Ok(self.records_written)
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
    use comet_test_support::read_gz_string;
    use serde_json::json;

    /// Set up a temp dir with a part path, a failed-records path, and an open
    /// `FailureSink` — the scaffolding shared by the writer tests.
    fn writer_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, Mutex<FailureSink>) {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("part_0000.jsonl.gz");
        let failed = dir.path().join("out.failed.jsonl");
        let failures = Mutex::new(FailureSink::create(&failed).unwrap());
        (dir, part, failed, failures)
    }

    #[test]
    fn part_writer_writes_one_per_line() {
        let (_dir, part, failed, failures) = writer_fixture();
        {
            let mut w = PartWriter::create(&part, None, &failures, 5000).unwrap();
            w.write_batch(&[json!({"a":1}), json!({"b":2})]).unwrap();
            assert_eq!(w.finish().unwrap(), 2);
        }
        let s = read_gz_string(&part);
        let lines: Vec<_> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"a":1}"#);
        assert_eq!(lines[1], r#"{"b":2}"#);
        // No failures, so the failures file is never created.
        assert!(!failed.exists());
    }

    #[test]
    fn push_buffers_and_flushes_every_record() {
        // batch_size 2: pushing three records flushes the first two at the
        // threshold and the third on finish, so none are lost at the boundary.
        let (_dir, part, failed, failures) = writer_fixture();
        let written = {
            let mut w = PartWriter::create(&part, None, &failures, 2).unwrap();
            w.push(json!({"n":1})).unwrap();
            w.push(json!({"n":2})).unwrap();
            w.push(json!({"n":3})).unwrap();
            w.finish().unwrap()
        };
        assert_eq!(written, 3);
        let lines: Vec<String> = read_gz_string(&part).lines().map(str::to_owned).collect();
        assert_eq!(lines, vec![r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#]);
        assert!(!failed.exists());
    }

    #[test]
    fn invalid_records_are_diverted() {
        let (_dir, part, failed, failures) = writer_fixture();
        // Minimal schema: an object that requires property "a".
        let schema = crate::schema::compile_str(r#"{"type":"object","required":["a"]}"#).unwrap();

        let records_written = {
            let mut w = PartWriter::create(&part, Some(&schema), &failures, 5000).unwrap();
            w.write_batch(&[json!({"a":1}), json!({"b":2})]).unwrap();
            w.finish().unwrap()
        };
        failures.lock().unwrap().flush().unwrap();

        assert_eq!(records_written, 1);
        assert_eq!(failures.lock().unwrap().records_failed, 1);

        let main = read_gz_string(&part);
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
        let part = dir.path().join("part_0000.jsonl.gz");
        let failed = dir.path().join("out.failed.jsonl");
        let schema_text = r#"{"type":"object","required":["a"]}"#;

        // First run diverts a record, so the failures file is left on disk.
        {
            let schema = crate::schema::compile_str(schema_text).unwrap();
            let failures = Mutex::new(FailureSink::create(&failed).unwrap());
            {
                let mut w = PartWriter::create(&part, Some(&schema), &failures, 5000).unwrap();
                w.write_batch(&[json!({"b":2})]).unwrap();
                w.finish().unwrap();
            }
            failures.lock().unwrap().flush().unwrap();
        }
        assert!(failed.exists());

        // Rerun into the same paths with no failures: the stale file must be gone.
        {
            let schema = crate::schema::compile_str(schema_text).unwrap();
            let failures = Mutex::new(FailureSink::create(&failed).unwrap());
            {
                let mut w = PartWriter::create(&part, Some(&schema), &failures, 5000).unwrap();
                w.write_batch(&[json!({"a":1})]).unwrap();
                w.finish().unwrap();
            }
            failures.lock().unwrap().flush().unwrap();
        }
        assert!(
            !failed.exists(),
            "stale failures file must be cleared on rerun"
        );
    }
}
