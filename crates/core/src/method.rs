//! [`EnrichmentMethod`], the trait each enrichment method implements.
//!
//! A method has three hooks:
//!
//! 1. [`extract`](EnrichmentMethod::extract) — produce zero or more extractions for a
//!    record, or a skip with a reason label.
//! 2. [`lookup`](EnrichmentMethod::lookup) — optional; resolve the unique deduplicated
//!    inputs against an external service. Pure-transform methods (e.g. the resource-type
//!    reclassifier) omit it and take the default no-op.
//! 3. [`map_back`](EnrichmentMethod::map_back) — join the lookups onto an extraction and
//!    produce the enrichment value parts.
//!
//! A method returns only the value parts ([`EnrichmentParts`]); the provenance template is
//! wrapped around them by [`build_enrichment_record`](crate::provenance::build_enrichment_record).
//!
//! Only the pure-transform path (no lookups) is wired into [`run`](crate::reader::run)
//! today; the dedup store and resumable HTTP client behind `lookup` aren't built yet.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// The enrichment action for one record — the closed set the enrichment-input schema allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentAction {
    /// Replace the entire top-level `field`.
    Update,
    /// Replace one child object within `field`.
    UpdateChild,
    /// Insert a new object into `field`.
    Insert,
    /// Remove a child object from `field`.
    DeleteChild,
}

impl EnrichmentAction {
    /// The schema string for this action (e.g. `"updateChild"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            EnrichmentAction::Update => "update",
            EnrichmentAction::UpdateChild => "updateChild",
            EnrichmentAction::Insert => "insert",
            EnrichmentAction::DeleteChild => "deleteChild",
        }
    }
}

/// The value parts of one enrichment, before the provenance template is applied.
pub struct EnrichmentParts {
    pub doi: String,
    /// The enrichment action for this record. Set per record so one method can emit a mix
    /// (e.g. insert/update/deleteChild).
    pub action: EnrichmentAction,
    pub original: Value,
    pub enriched: Value,
}

/// The result of running [`EnrichmentMethod::extract`] over a single record.
pub enum Extracted<E> {
    /// Zero or more extractions to carry forward to `map_back`.
    Items(Vec<E>),
    /// Nothing to emit; counted under this reason label in the run's skip histogram.
    Skip(&'static str),
}

/// Lookups resolved per unique input, keyed by content hash (xxh3). Empty on the
/// transform path.
pub type Lookups<L> = HashMap<String, L>;

pub trait EnrichmentMethod: Sync {
    /// Intermediate extraction carried from `extract` to `map_back`.
    type Extraction: Send;
    /// The result `lookup` resolves per unique input. `()` for pure transforms.
    type Lookup: Send;

    /// The top-level DataCite field this method enriches, e.g. `"types"`.
    fn field(&self) -> &'static str;

    /// Produce extractions (or a skip reason) for one input record.
    fn extract(&self, record: &Value) -> Extracted<Self::Extraction>;

    /// Resolve the unique deduplicated inputs against an external service (optional).
    ///
    /// Defaults to no lookups, which is correct for pure-transform methods. The dedup
    /// store and resumable HTTP client behind this hook aren't wired into
    /// [`run`](crate::reader::run) yet.
    ///
    /// # Errors
    /// Implementations return an error if the external resolution fails.
    fn lookup(&self, _inputs: &[String]) -> Result<Lookups<Self::Lookup>> {
        Ok(HashMap::new())
    }

    /// Join `lookups` onto one extraction and produce the enrichment value parts.
    /// On the transform path `lookups` is empty.
    fn map_back(
        &self,
        extraction: Self::Extraction,
        lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts>;
}
