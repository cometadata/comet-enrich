//! Embedded enrichment schema and validator helpers.
//!
//! The built-in schema covers full and diff records. Callers can compile a
//! custom schema with [`compile`].

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;

/// Built-in enrichment JSON Schema.
pub const SCHEMA: &str = include_str!("../../../configs/enrichment_input_schema.json");

/// Read and compile a schema file.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or compiled.
pub fn compile(path: &Path) -> Result<jsonschema::Validator> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schema {}", path.display()))?;
    compile_str(&text)
}

/// Compile a schema from JSON text.
///
/// # Errors
///
/// Returns an error if the text cannot be parsed or compiled.
pub fn compile_str(text: &str) -> Result<jsonschema::Validator> {
    let schema_val: Value = serde_json::from_str(text).context("parsing schema")?;
    jsonschema::validator_for(&schema_val).map_err(|e| anyhow::anyhow!("schema compile: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::assert_err_contains;

    #[test]
    fn compile_str_invalid_json_reports_schema_parse_context() {
        assert_err_contains(compile_str("{"), "parsing schema");
    }

    #[test]
    fn compile_missing_file_reports_path() {
        assert_err_contains(
            compile(Path::new("__missing_schema__.json")),
            "reading schema __missing_schema__.json",
        );
    }

    #[test]
    fn schema_accepts_optional_event_and_rejects_unknown_events() {
        let validator = compile_str(SCHEMA).unwrap();
        let mut rec = serde_json::json!({
            "doi": "10.1/x",
            "action": "update",
            "field": "types",
            "originalValue": {"resourceTypeGeneral": "Text"},
            "enrichedValue": {"resourceTypeGeneral": "Dataset"},
            "sourceId": "10.82461/bpzr-jd55",
            "contentKey": "0860ed77af682e5bbe343af4f5e0347c",
        });
        assert!(validator.is_valid(&rec));
        for event in ["asserted", "retracted", "superseded"] {
            rec["event"] = serde_json::json!(event);
            assert!(validator.is_valid(&rec), "{event} should validate");
        }
        rec["event"] = serde_json::json!("bogus");
        assert!(!validator.is_valid(&rec));
    }
}
