//! Common code for COMET enrichment methods.
//!
//! This crate provides the common code for reading DataCite *.jsonl.gz files,
//! building enrichment records, validating them, and writing JSONL output.
//! Individual enrichment crates provide the method-specific logic by implementing
//! [EnrichmentMethod].

// DataCite, COMET, JSONL, and xxh3 are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

pub mod datacite_enums;
pub mod manifest;
pub mod method;
pub mod provenance;
pub mod reader;
pub mod schema;
pub mod staged;
pub mod writer;

pub use manifest::{Manifest, Report, RunMeta, SourceRelease, StageTimings};
pub use method::{EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
pub use provenance::{EnrichmentTemplate, build_enrichment_record, load_template};
pub use reader::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE, RunOptions, RunStats, run};
pub use schema::SCHEMA;
pub use staged::{LookupConfig, Stage, WorkDir, stages_to_run};
