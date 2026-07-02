//! DataCite resource type reclassification.
//!
//! Reclassifies `types.resourceTypeGeneral` from the free-text
//! `types.resourceType` value in DataCite records.
//!
//! Records are processed when their current `resourceTypeGeneral` value is in
//! scope according to the configured rules. Matching uses the DataCite resource
//! type vocabulary, typo corrections, redundancy exclusions, and threshold from
//! `reclassification_rules.yaml`.

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

mod config;
mod matcher;

use anyhow::Result;
use comet_enrich_core::{EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, Lookups};
use matcher::{MatchOutcome, Matcher};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::PathBuf;

/// Configuration for resource type reclassification.
pub struct Config {
    /// Path to the reclassification rules YAML file.
    pub rules: PathBuf,
}

/// Resource type values that may be reclassified.
///
/// A record is in scope when its current `resourceTypeGeneral` value is listed
/// in the rules. Missing values are allowed only when `null` appears in the
/// configured scope.
struct Scope {
    allow_null: bool,
    allow_values: HashSet<String>,
}

impl Scope {
    fn from_config(cfg: &config::ScopeConfig) -> Self {
        let mut allow_null = false;
        let mut allow_values = HashSet::new();
        for t in &cfg.target_resource_type_general {
            match t {
                None => allow_null = true,
                Some(s) => {
                    allow_values.insert(s.clone());
                }
            }
        }
        Scope {
            allow_null,
            allow_values,
        }
    }

    fn allows(&self, rtg: Option<&str>) -> bool {
        match rtg {
            None => self.allow_null,
            Some(s) => self.allow_values.contains(s),
        }
    }
}

/// Reclassifies DataCite `types.resourceTypeGeneral` values.
///
/// The method compares each record's free-text `resourceType` value with the
/// configured DataCite resource type vocabulary. When the accepted match differs
/// from the current `resourceTypeGeneral`, it emits an updated `types` object as
/// an enrichment.
pub struct ResourceTypeGeneral {
    matcher: Matcher,
    scope: Scope,
}

impl ResourceTypeGeneral {
    /// Builds the reclassification method from its configuration.
    ///
    /// This loads and validates the rules YAML, then constructs the matcher and
    /// scope used by the method.
    ///
    /// # Errors
    ///
    /// Returns an error if the rules file cannot be read, parsed, or validated.
    pub fn try_new(config: Config) -> Result<Self> {
        let rules = config::load_rules(config.rules)?;
        let matcher = Matcher::from_config(&rules);
        let scope = Scope::from_config(&rules.scope);
        Ok(Self { matcher, scope })
    }
}

impl EnrichmentMethod for ResourceTypeGeneral {
    type Extraction = EnrichmentParts;
    type Lookup = ();

    fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
        let Some(attributes) = record.get("attributes").filter(|v| !v.is_null()) else {
            return Extracted::Skip("malformed_types");
        };
        let Some(types) = attributes.get("types").filter(|v| v.is_object()) else {
            return Extracted::Skip("malformed_types");
        };

        let rtg = types.get("resourceTypeGeneral").and_then(Value::as_str);
        if !self.scope.allows(rtg) {
            return Extracted::Skip("not_in_scope");
        }

        let rt = match types.get("resourceType").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s,
            _ => return Extracted::Skip("no_resource_type"),
        };

        let matched = match self.matcher.fuzzy_match(rt) {
            MatchOutcome::NoMatch => return Extracted::Skip("no_match"),
            MatchOutcome::Redundant => return Extracted::Skip("redundant"),
            MatchOutcome::Matched(s) => s,
        };

        if Some(matched.as_str()) == rtg {
            return Extracted::Skip("no_change");
        }

        let doi = match record.get("id").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return Extracted::Skip("no_doi"),
        };

        let mut enriched = types.clone();
        enriched["resourceTypeGeneral"] = json!(matched);
        Extracted::Items(vec![EnrichmentParts {
            doi,
            action: EnrichmentAction::Update,
            field: "types",
            original: types.clone(),
            enriched,
        }])
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        _lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        vec![extraction]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_method(scope_targets: Vec<Option<String>>) -> ResourceTypeGeneral {
        let rules = config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["JournalArticle".into(), "Dataset".into(), "Text".into()],
            typo_corrections: std::collections::HashMap::new(),
            redundancy_exclusions: vec![config::RedundancyRuleConfig {
                normalized: vec!["text".into(), "txt".into()],
                matches: vec!["Text".into(), "Other".into()],
            }],
            scope: config::ScopeConfig {
                target_resource_type_general: scope_targets,
            },
        };
        let matcher = Matcher::from_config(&rules);
        let scope = Scope::from_config(&rules.scope);
        ResourceTypeGeneral { matcher, scope }
    }

    fn scope_text_other_null() -> Vec<Option<String>> {
        vec![Some("Text".into()), Some("Other".into()), None]
    }

    #[test]
    fn extract_emits_on_match() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Journal article", "resourceTypeGeneral": "Text"}}});
        match m.extract(&rec) {
            Extracted::Items(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].doi, "10.5281/x");
                assert_eq!(items[0].action, EnrichmentAction::Update);
                assert_eq!(
                    items[0].enriched["resourceTypeGeneral"],
                    json!("JournalArticle")
                );
                assert_eq!(items[0].original["resourceTypeGeneral"], json!("Text"));
            }
            Extracted::Skip(r) => panic!("expected Items, got skip {r}"),
        }
    }

    #[test]
    fn extract_skips_out_of_scope() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Dataset", "resourceTypeGeneral": "Image"}}});
        assert!(matches!(m.extract(&rec), Extracted::Skip("not_in_scope")));
    }

    #[test]
    fn extract_skips_redundant() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Text", "resourceTypeGeneral": "Text"}}});
        assert!(matches!(m.extract(&rec), Extracted::Skip("redundant")));
    }

    #[test]
    fn extract_skips_no_match() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Completely unrelated string", "resourceTypeGeneral": "Other"}}});
        assert!(matches!(m.extract(&rec), Extracted::Skip("no_match")));
    }

    #[test]
    fn extract_handles_null_rtg() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Dataset"}}});
        match m.extract(&rec) {
            Extracted::Items(items) => {
                assert_eq!(items[0].enriched["resourceTypeGeneral"], json!("Dataset"));
            }
            Extracted::Skip(r) => panic!("expected Items, got skip {r}"),
        }
    }

    #[test]
    fn extract_skips_no_change() {
        let m = test_method(vec![Some("Dataset".into())]);
        let rec = json!({"id": "10.5281/x", "attributes": {
            "types": {"resourceType": "Dataset", "resourceTypeGeneral": "Dataset"}}});
        assert!(matches!(m.extract(&rec), Extracted::Skip("no_change")));
    }

    #[test]
    fn extract_skips_malformed_types() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"id": "10.5281/x", "attributes": {}});
        assert!(matches!(
            m.extract(&rec),
            Extracted::Skip("malformed_types")
        ));
    }

    #[test]
    fn extract_skips_no_doi() {
        let m = test_method(scope_text_other_null());
        let rec = json!({"attributes": {
            "types": {"resourceType": "Journal article", "resourceTypeGeneral": "Text"}}});
        assert!(matches!(m.extract(&rec), Extracted::Skip("no_doi")));
    }
}
