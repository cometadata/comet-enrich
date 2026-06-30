//! `funders` method: match DataCite funder names to ROR IDs.
//!
//! Stub: the [`EnrichmentMethod`] implementation is in place and the method takes a
//! [`LookupConfig`]; the matching pipeline isn't wired yet, so [`Funders::try_new`]
//! returns an error.

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
    // Placeholder stub types so the staged-runner wiring compiles; the real
    // funding-reference extraction lands when the method is ported (PLAN.md Stage 8).
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
