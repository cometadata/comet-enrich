//! DataCite affiliation matching.
//!
//! Extracts creator affiliation strings from DataCite records and maps them to
//! ROR organizations through the configured matching service.
//!

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrichment_core::{
    EnrichmentMethod, EnrichmentParts, Extracted, LookupConfig, Lookups, RorLookup,
};
use serde_json::Value;

/// Matches DataCite creator affiliation strings to ROR organizations.
///
/// The method extracts affiliation text from creator metadata, queries the
/// configured ROR matching service, and maps accepted matches back into
/// enrichment parts.
pub struct Affiliations;

impl Affiliations {
    /// Builds the affiliation matcher from its lookup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the matcher cannot be constructed from the supplied
    /// configuration.
    pub fn try_new(config: LookupConfig) -> Result<Self> {
        drop(config);
        bail!("affiliations: not yet implemented")
    }
}

impl EnrichmentMethod for Affiliations {
    type Extraction = Value;
    type Lookup = RorLookup;

    fn extract(&self, _record: &Value) -> Extracted<Self::Extraction> {
        unimplemented!()
    }

    fn map_back(
        &self,
        _extraction: Self::Extraction,
        _lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        unimplemented!()
    }
}
