//! [EnrichmentMethod], the trait implemented by each enrichment method.
//!
//! An enrichment method extracts values from DataCite records, optionally resolves
//! them through a lookup step, then maps the results back into enrichment records.
//! Methods return only the enrichment value parts; provenance is added later by
//! build_enrichment_record.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Action to apply to the enriched field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentAction {
    /// Replace the whole top-level field.
    Update,
    /// Replace one child object within the field.
    UpdateChild,
    /// Insert a new object into the field.
    Insert,
    /// Remove a child object from the field.
    DeleteChild,
}

impl EnrichmentAction {
    /// Return the schema value for this action.
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

/// Value fields for one enrichment record, before provenance is added.
pub struct EnrichmentParts {
    pub doi: String,
    /// Action to apply for this record.
    pub action: EnrichmentAction,
    /// Top-level DataCite field this record enriches, such as `"types"`. Set per
    /// record so a method can target different fields across records (for example
    /// affiliations emits both `"creators"` and `"contributors"`).
    pub field: &'static str,
    pub original: Value,
    pub enriched: Value,
}

/// Output from extracting values from one DataCite record.
pub enum Extracted<E> {
    /// Extractions to pass to `map_back`.
    Items(Vec<E>),
    /// Skip this record and count it under the given reason.
    Skip(&'static str),
}

/// Lookup results keyed by input hash.
///
/// Empty for methods that do not use external lookups.
pub type Lookups<L> = HashMap<String, L>;

/// Enrichment method implementation.
pub trait EnrichmentMethod: Sync {
    /// Intermediate value carried from `extract` to `map_back`.
    type Extraction: Send;
    /// Lookup result for one unique input. Use `()` for methods without lookups.
    type Lookup: Send;

    /// Extract values from one input record.
    fn extract(&self, record: &Value) -> Extracted<Self::Extraction>;

    /// Unique lookup inputs contributed by one extraction.
    ///
    /// The staged runner collects these across the corpus, deduplicates them, and
    /// resolves them through the match service; `map_back` re-derives the same hash
    /// to index the results. Transform methods keep the default (no inputs).
    fn inputs(&self, _extraction: &Self::Extraction) -> Vec<String> {
        Vec::new()
    }

    /// Resolve unique extracted inputs through an external service.
    ///
    /// Methods without lookups can use the default implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if lookup resolution fails.
    fn lookup(&self, _inputs: &[String]) -> Result<Lookups<Self::Lookup>> {
        Ok(HashMap::new())
    }

    /// Map one extraction back into enrichment value fields.
    fn map_back(
        &self,
        extraction: Self::Extraction,
        lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TransformOnly;

    impl EnrichmentMethod for TransformOnly {
        type Extraction = ();
        type Lookup = ();

        fn extract(&self, _record: &Value) -> Extracted<Self::Extraction> {
            Extracted::Items(vec![()])
        }

        fn map_back(
            &self,
            _extraction: Self::Extraction,
            _lookups: &Lookups<Self::Lookup>,
        ) -> Vec<EnrichmentParts> {
            Vec::new()
        }
    }

    // Transform-only methods intentionally use `Lookup = ()`; this test pins that
    // the generic `Lookups<L>` API still works for that no-lookup case.
    #[allow(clippy::zero_sized_map_values)]
    #[test]
    fn enrichment_method_default_lookup_hooks_are_empty() {
        let method = TransformOnly;
        let lookups = Lookups::new();

        assert!(
            matches!(method.extract(&Value::Null), Extracted::Items(items) if items == vec![()])
        );
        assert!(method.inputs(&()).is_empty());
        assert!(method.lookup(&["x".to_owned()]).unwrap().is_empty());
        assert!(method.map_back((), &lookups).is_empty());
    }

    #[test]
    fn enrichment_action_matches_schema_values() {
        assert_eq!(EnrichmentAction::Update.as_str(), "update");
        assert_eq!(EnrichmentAction::UpdateChild.as_str(), "updateChild");
        assert_eq!(EnrichmentAction::Insert.as_str(), "insert");
        assert_eq!(EnrichmentAction::DeleteChild.as_str(), "deleteChild");
    }
}
