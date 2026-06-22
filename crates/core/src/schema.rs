//! The embedded enrichment-input JSON Schema and helpers to compile it into a validator.
//!
//! [`SCHEMA`] is the canonical schema, baked into core so that output-record validation and the
//! DataCite vocabulary sets ([`crate::datacite_enums`]) read from one source of truth. Callers
//! may still supply an alternate schema by path via [`compile`] (e.g. the CLI's `--schema` flag).
//! A schema bump therefore requires a core rebuild.

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;

/// The canonical enrichment-input JSON Schema, embedded at build time. The authoritative copy
/// lives at the workspace root `schema/enrichment_input_schema.json`.
pub const SCHEMA: &str = include_str!("../../../schema/enrichment_input_schema.json");

/// Read and compile the enrichment-input schema at `path` into a reusable validator.
///
/// # Errors
/// Returns an error if the file cannot be read, parsed, or compiled.
pub fn compile(path: &Path) -> Result<jsonschema::JSONSchema> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schema {}", path.display()))?;
    compile_str(&text)
}

/// Compile a schema from its JSON text. Used for the copy embedded in the `comet-enrich` binary.
///
/// # Errors
/// Returns an error if the text cannot be parsed or compiled.
pub fn compile_str(text: &str) -> Result<jsonschema::JSONSchema> {
    let schema_val: Value = serde_json::from_str(text).context("parsing schema")?;
    jsonschema::JSONSchema::compile(&schema_val).map_err(|e| anyhow::anyhow!("schema compile: {e}"))
}
