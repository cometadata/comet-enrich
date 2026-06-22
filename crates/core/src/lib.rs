//! Shared core for the COMET enrichment methods.
//!
//! Each method reads a directory of `*.jsonl.gz` DataCite shards, builds enrichment records,
//! and writes them as JSONL. The shared read -> build -> validate -> write pipeline lives
//! here; a method supplies only its own per-record logic by implementing [`EnrichmentMethod`].

// Brand and format names (DataCite, COMET, JSONL, xxh3, …) recur throughout the docs as
// prose, not code identifiers.
#![allow(clippy::doc_markdown)]

pub mod datacite_enums;
pub mod method;
pub mod provenance;
pub mod reader;
pub mod schema;
pub mod staged;
pub mod writer;

pub use method::{EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
pub use provenance::{EnrichmentTemplate, build_enrichment_record};
pub use reader::{RunOptions, RunStats, run};
pub use schema::SCHEMA;
pub use staged::{LookupConfig, Stage, WorkDir, stages_to_run};
pub use writer::JsonlWriter;
