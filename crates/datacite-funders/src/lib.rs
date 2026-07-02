//! DataCite funder matching stub.

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrich_core::{
    EnrichmentMethod, EnrichmentParts, Extracted, LookupConfig, Lookups, RorLookup,
};
use serde_json::Value;
use std::path::PathBuf;

/// Configuration for the funder matcher.
pub struct Config {
    /// Lookup configuration.
    pub lookup: LookupConfig,
    /// ROR registry JSON for the Crossref Funder ID to ROR crosswalk.
    pub ror_file: PathBuf,
}

/// Funder matcher, not yet implemented.
pub struct Funders;

impl Funders {
    /// Build the funder matcher.
    ///
    /// # Errors
    ///
    /// Always returns an error until the method is implemented.
    pub fn try_new(config: Config) -> Result<Self> {
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
