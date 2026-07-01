//! Embedded enrichment-input schema and validator helpers.
//!
//! The built-in schema is used for output validation and for the DataCite
//! vocabulary values in [`crate::datacite_enums`]. Callers can compile a custom
//! schema with [`compile`].

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;

/// Built-in enrichment-input JSON Schema.
pub const SCHEMA: &str = include_str!("../../../configs/schema/enrichment_input_schema.json");

/// Read and compile a schema file.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or compiled.
pub fn compile(path: &Path) -> Result<jsonschema::JSONSchema> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schema {}", path.display()))?;
    compile_str(&text)
}

/// Compile a schema from JSON text.
///
/// # Errors
///
/// Returns an error if the text cannot be parsed or compiled.
pub fn compile_str(text: &str) -> Result<jsonschema::JSONSchema> {
    let schema_val: Value = serde_json::from_str(text).context("parsing schema")?;
    jsonschema::JSONSchema::compile(&schema_val).map_err(|e| anyhow::anyhow!("schema compile: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_test_support::assert_err_contains;

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
}
