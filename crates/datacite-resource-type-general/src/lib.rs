//! `resource-type` method: reclassify DataCite `types.resourceTypeGeneral`.
//!
//! Stub: the [`EnrichmentMethod`] implementation and config are in place; the reclassifier
//! logic isn't wired yet, so [`ResourceTypeGeneral::try_new`] returns an error.

// Brand names (DataCite, …) recur in the docs as prose, not code identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Result, bail};
use comet_enrichment_core::{EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
use serde_json::Value;
use std::path::PathBuf;

/// Method-specific configuration, built by the CLI from its arguments.
pub struct Config {
    /// Reclassification rules (reclassification_rules.yaml).
    pub rules: PathBuf,
}

/// Reclassify `types.resourceTypeGeneral` over a DataCite snapshot.
///
/// A pure transform: each record's `resourceType` is fuzzy-matched against the DataCite
/// vocabulary and a corrected `resourceTypeGeneral` is emitted as an enrichment.
pub struct ResourceTypeGeneral;

impl ResourceTypeGeneral {
    /// Build the method from its configuration.
    ///
    /// # Errors
    /// Not implemented yet — always returns an error.
    pub fn try_new(config: Config) -> Result<Self> {
        drop(config); // stub: the real constructor will consume the config
        bail!("resource-type-general: not yet implemented")
    }
}

impl EnrichmentMethod for ResourceTypeGeneral {
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
