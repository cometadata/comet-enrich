//! Rules for reclassifying DataCite `types.resourceTypeGeneral` values.
//!
//! The rules file controls the fuzzy-match threshold, the reference vocabulary,
//! typo corrections, redundancy exclusions, and the set of existing
//! `resourceTypeGeneral` values that may be overwritten.
//!
//! Reference values, redundancy matches, and scope entries are checked against
//! [`RESOURCE_TYPE_GENERAL`](comet_enrich_core::datacite_enums::RESOURCE_TYPE_GENERAL)
//! from `core`, so invalid DataCite type names are rejected when the rules are loaded.

use anyhow::{Context, Result, bail};
use comet_enrich_core::datacite_enums;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct RulesConfig {
    pub threshold: f64,
    pub reference_values: Vec<String>,
    pub typo_corrections: HashMap<String, String>,
    pub redundancy_exclusions: Vec<RedundancyRuleConfig>,
    pub scope: ScopeConfig,
}

#[derive(Debug, Deserialize)]
pub struct RedundancyRuleConfig {
    pub normalized: Vec<String>,
    pub matches: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeConfig {
    pub target_resource_type_general: Vec<Option<String>>,
}

/// Load the reclassification rules from a YAML file.
///
/// # Errors
///
/// Returns an error if the file cannot be read, cannot be parsed as YAML, or
/// contains values that are not valid for this method.
pub fn load_rules<P: AsRef<Path>>(path: P) -> Result<RulesConfig> {
    let text = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("reading {}", path.as_ref().display()))?;
    let cfg: RulesConfig = serde_yaml_ng::from_str(&text)
        .with_context(|| format!("parsing {}", path.as_ref().display()))?;
    validate_rules(&cfg)?;
    Ok(cfg)
}

fn validate_rules(cfg: &RulesConfig) -> Result<()> {
    if !(0.0..=1.0).contains(&cfg.threshold) {
        bail!("threshold must be in [0, 1], got {}", cfg.threshold);
    }
    for rv in &cfg.reference_values {
        if !datacite_enums::RESOURCE_TYPE_GENERAL.contains(rv.as_str()) {
            bail!("reference_values contains unknown DataCite type: {rv:?}");
        }
    }
    for rule in &cfg.redundancy_exclusions {
        for m in &rule.matches {
            if !datacite_enums::RESOURCE_TYPE_GENERAL.contains(m.as_str()) {
                bail!("redundancy_exclusions.matches contains unknown type: {m:?}");
            }
        }
    }
    for s in cfg.scope.target_resource_type_general.iter().flatten() {
        if !datacite_enums::RESOURCE_TYPE_GENERAL.contains(s.as_str()) {
            bail!("scope.target contains unknown type: {s:?}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_rules_parses_sample_yaml() {
        // Checks that a normal rules file parses into the expected Rust structs,
        // including nullable scope entries.
        let yaml = r"
threshold: 0.85
reference_values:
  - Dataset
  - Text
  - Other
typo_corrections:
  otput: output
  sofware: software
redundancy_exclusions:
  - normalized: [text, txt]
    matches: [Text, Other]
scope:
  target_resource_type_general:
    - Text
    - Other
    - null
";
        let cfg: RulesConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!((cfg.threshold - 0.85).abs() < f64::EPSILON);
        assert_eq!(cfg.reference_values.len(), 3);
        assert_eq!(
            cfg.typo_corrections.get("otput"),
            Some(&"output".to_string())
        );
        assert_eq!(cfg.redundancy_exclusions.len(), 1);
        assert_eq!(
            cfg.scope.target_resource_type_general,
            vec![Some("Text".into()), Some("Other".into()), None]
        );
        validate_rules(&cfg).unwrap();
    }

    #[test]
    fn validate_rules_rejects_unknown_reference_value() {
        // Checks that reference values must come from the DataCite
        // resourceTypeGeneral vocabulary.
        let yaml = r"
threshold: 0.85
reference_values: [Dataset, FakeType]
typo_corrections: {}
redundancy_exclusions: []
scope:
  target_resource_type_general: [Text]
";
        let cfg: RulesConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let err = validate_rules(&cfg).unwrap_err().to_string();
        assert!(err.contains("FakeType"));
    }

    #[test]
    fn validate_rules_rejects_bad_threshold() {
        // Checks that the fuzzy-match threshold must be between 0 and 1,
        // inclusive.
        let yaml = r"
threshold: 1.5
reference_values: [Dataset]
typo_corrections: {}
redundancy_exclusions: []
scope:
  target_resource_type_general: [Text]
";
        let cfg: RulesConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(validate_rules(&cfg).is_err());
    }
}
