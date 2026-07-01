//! DataCite funder matching.
//!
//! Extracts funder names from DataCite funding references and maps them to ROR
//! organizations through the configured matching service.

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrichment_core::{
    EnrichmentMethod, EnrichmentParts, Extracted, LookupConfig, Lookups, RorLookup,
};
use serde_json::Value;

/// Matches DataCite funder names to ROR organizations.
///
/// The method extracts funder names from DataCite funding references, queries
/// the configured ROR matching service, and maps accepted matches back into
/// enrichment parts.
pub struct Funders;

impl Funders {
    /// Builds the funder matcher from its lookup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the matcher cannot be constructed from the supplied
    /// configuration.
    pub fn try_new(config: LookupConfig) -> Result<Self> {
        drop(config);
        bail!("funders: not yet implemented")
    }
}

impl EnrichmentMethod for Funders {
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
