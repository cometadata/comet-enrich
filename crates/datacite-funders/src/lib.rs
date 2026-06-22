//! `funders` method: match DataCite funder names to ROR IDs.
//!
//! Stub: the [`EnrichmentMethod`] implementation is in place and the method takes a
//! [`LookupConfig`]; the matching pipeline isn't wired yet, so [`Funders::try_new`]
//! returns an error.

// Brand names (DataCite, ROR, …) recur in the docs as prose, not code identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrichment_core::{EnrichmentMethod, EnrichmentParts, Extracted, LookupConfig, Lookups};
use serde_json::Value;

/// Match funder names to ROR IDs.
///
/// Runs `extract` → `query` → `reconcile` against the match service.
pub struct Funders;

impl Funders {
    /// Build the method from its lookup configuration.
    ///
    /// # Errors
    /// Not implemented yet — always returns an error.
    pub fn try_new(config: LookupConfig) -> Result<Self> {
        drop(config); // stub: the real constructor will consume the config
        bail!("funders: not yet implemented")
    }
}

impl EnrichmentMethod for Funders {
    type Extraction = EnrichmentParts;
    type Lookup = ();

    fn field(&self) -> &'static str {
        unimplemented!()
    }

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
