//! DataCite affiliation matching stub.
//!

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrich_core::{
    EnrichmentMethod, EnrichmentParts, Extracted, LookupConfig, Lookups, RorLookup,
};
use serde_json::Value;

/// Affiliation matcher, not yet implemented.
pub struct Affiliations;

impl Affiliations {
    /// Build the affiliation matcher.
    ///
    /// # Errors
    ///
    /// Always returns an error until the method is implemented.
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
