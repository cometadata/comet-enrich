//! Common code for COMET enrichment methods.
//!
//! This crate provides the common code for reading DataCite *.jsonl.gz files,
//! building enrichment records, validating them, and writing JSONL output.
//! Individual enrichment crates provide the method-specific logic by implementing
//! [EnrichmentMethod].

// DataCite, COMET, JSONL, and xxh3 are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

pub mod datacite_enums;
pub mod dedup;
mod fanout;
pub mod manifest;
pub mod match_service;
pub mod method;
pub mod provenance;
pub mod run;
pub mod schema;
pub mod staged;
pub mod staged_run;
pub mod transform;
pub mod writer;

pub use dedup::{DedupStore, HashBits, hash_input};
pub use manifest::{HashInfo, Manifest, Report, RunMeta, SourceRelease, StageTimings, exit_status};
pub use match_service::{MarpleClient, MatchHit, MatchService, RorLookup};
pub use method::{EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
pub use provenance::{EnrichmentTemplate, build_enrichment_record, load_template};
pub use run::{RunOptions, RunStats};
pub use schema::SCHEMA;
pub use staged::{LookupConfig, Stage, WorkDir, stages_to_run};
pub use staged_run::{pipeline_complete, run_staged};
pub use transform::run;
pub use writer::{ENRICHMENTS_DIR, ENRICHMENTS_FAILED_FILE};
