//! JSONL writer for enrichment records.

use anyhow::{Context, Result};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct JsonlWriter {
    inner: BufWriter<File>,
    pub records_written: u64,
    validator: Option<jsonschema::JSONSchema>,
}

impl JsonlWriter {
    /// Create a JSONL writer.
    ///
    /// Records are validated before writing when `validator` is provided.
    ///
    /// # Errors
    ///
    /// Returns an error if the output file cannot be created.
    pub fn create<P: AsRef<Path>>(
        path: P,
        validator: Option<jsonschema::JSONSchema>,
    ) -> Result<Self> {
        let f = File::create(path.as_ref())
            .with_context(|| format!("creating {}", path.as_ref().display()))?;
        Ok(Self {
            inner: BufWriter::with_capacity(256 * 1024, f),
            records_written: 0,
            validator,
        })
    }

    /// Write records as JSONL, validating each one first if configured.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or a record cannot be written.
    pub fn write_batch(&mut self, batch: &[Value]) -> Result<()> {
        for rec in batch {
            if let Some(v) = &self.validator {
                if let Err(errors) = v.validate(rec) {
                    let msgs: Vec<String> = errors.map(|e| e.to_string()).collect();
                    anyhow::bail!("schema violation: {msgs:?}");
                }
            }
            serde_json::to_writer(&mut self.inner, rec)?;
            self.inner.write_all(b"\n")?;
            self.records_written += 1;
        }
        Ok(())
    }

    /// Flush buffered output.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying flush fails.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn jsonl_writer_writes_one_per_line() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut w = JsonlWriter::create(tmp.path(), None).unwrap();
            w.write_batch(&[json!({"a":1}), json!({"b":2})]).unwrap();
            w.flush().unwrap();
        }
        let s = std::fs::read_to_string(tmp.path()).unwrap();
        let lines: Vec<_> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"a":1}"#);
        assert_eq!(lines[1], r#"{"b":2}"#);
    }
}
