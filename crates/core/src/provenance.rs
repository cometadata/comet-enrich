//! Provenance metadata: the enrichment template, its config, and the record builder.
//!
//! Every enrichment record carries a static block of provenance — the `contributors`
//! (sources) and `resources` describing how the enrichment was produced. That block is
//! loaded once from a YAML config into an [`EnrichmentTemplate`] and cloned into each
//! emitted record by [`build_enrichment_record`].

use crate::datacite_enums;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct EnrichmentConfig {
    pub enrichment: EnrichmentBlock,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EnrichmentBlock {
    pub sources: Vec<SourceConfig>,
    pub resources: Vec<ResourceConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    #[serde(rename = "nameType", skip_serializing_if = "Option::is_none")]
    pub name_type: Option<String>,
    #[serde(rename = "contributorType")]
    pub contributor_type: String,
    #[serde(rename = "givenName", skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    #[serde(rename = "familyName", skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    #[serde(rename = "nameIdentifiers", skip_serializing_if = "Option::is_none")]
    pub name_identifiers: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliation: Option<Vec<serde_json::Value>>,
}

// Field order here is the emitted JSON key order (serde follows declaration order);
// keep it aligned with the enrichment-input schema.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ResourceConfig {
    #[serde(rename = "relatedIdentifier")]
    pub related_identifier: String,
    #[serde(rename = "relatedIdentifierType")]
    pub related_identifier_type: String,
    #[serde(rename = "relationType")]
    pub relation_type: String,
    #[serde(rename = "relatedMetadataScheme", skip_serializing_if = "Option::is_none")]
    pub related_metadata_scheme: Option<String>,
    #[serde(rename = "schemeUri", skip_serializing_if = "Option::is_none")]
    pub scheme_uri: Option<String>,
    #[serde(rename = "resourceTypeGeneral", skip_serializing_if = "Option::is_none")]
    pub resource_type_general: Option<String>,
}

/// The static provenance block, pre-rendered to JSON once per run.
pub struct EnrichmentTemplate {
    pub contributors: Value,
    pub resources: Value,
}

impl EnrichmentTemplate {
    #[must_use]
    pub fn from_config(cfg: &EnrichmentConfig) -> Self {
        // The structs serialize to exactly the JSON shape these records need: `serde(rename)`
        // gives the camelCase keys and `skip_serializing_if` omits absent optionals. Key order
        // follows struct declaration order. Serialization is infallible for these plain structs.
        let contributors = Value::Array(
            cfg.enrichment
                .sources
                .iter()
                .map(|s| serde_json::to_value(s).expect("SourceConfig serializes"))
                .collect(),
        );
        let resources = Value::Array(
            cfg.enrichment
                .resources
                .iter()
                .map(|r| serde_json::to_value(r).expect("ResourceConfig serializes"))
                .collect(),
        );

        EnrichmentTemplate {
            contributors,
            resources,
        }
    }
}

/// Build one enrichment record by wrapping the value parts with the provenance template.
///
/// `field` is the top-level DataCite field the method enriches (e.g. `"types"`); `action` is
/// the per-record enrichment action (e.g. `"update"`, `"updateChild"`). Key order is fixed and
/// asserted by the tests.
#[must_use]
pub fn build_enrichment_record(
    template: &EnrichmentTemplate,
    doi: &str,
    action: &str,
    field: &str,
    original_value: Value,
    enriched_value: Value,
) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("doi".into(), json!(doi));
    m.insert("contributors".into(), template.contributors.clone());
    m.insert("resources".into(), template.resources.clone());
    m.insert("action".into(), json!(action));
    m.insert("field".into(), json!(field));
    m.insert("originalValue".into(), original_value);
    m.insert("enrichedValue".into(), enriched_value);
    Value::Object(m)
}

/// Load and validate the provenance YAML at `path`.
///
/// # Errors
/// Returns an error if the file cannot be read, parsed, or fails validation.
pub fn load_enrichment<P: AsRef<Path>>(path: P) -> Result<EnrichmentConfig> {
    let text = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("reading {}", path.as_ref().display()))?;
    let cfg: EnrichmentConfig = serde_yaml::from_str(&text)
        .with_context(|| format!("parsing {}", path.as_ref().display()))?;
    validate_enrichment(&cfg)?;
    Ok(cfg)
}

/// Validate provenance config against the DataCite controlled vocabularies.
///
/// # Errors
/// Returns an error if sources/resources are empty or use unknown vocabulary terms.
pub fn validate_enrichment(cfg: &EnrichmentConfig) -> Result<()> {
    if cfg.enrichment.sources.is_empty() {
        bail!("enrichment.sources must have at least one entry");
    }
    if cfg.enrichment.resources.is_empty() {
        bail!("enrichment.resources must have at least one entry");
    }
    for s in &cfg.enrichment.sources {
        if !datacite_enums::CONTRIBUTOR_TYPE.contains(s.contributor_type.as_str()) {
            bail!("unknown contributorType: {:?}", s.contributor_type);
        }
        if let Some(nt) = &s.name_type {
            if !datacite_enums::NAME_TYPE.contains(nt.as_str()) {
                bail!("unknown nameType: {nt:?}");
            }
        }
    }
    for r in &cfg.enrichment.resources {
        if !datacite_enums::RELATION_TYPE.contains(r.relation_type.as_str()) {
            bail!("unknown relationType: {:?}", r.relation_type);
        }
        if !datacite_enums::RELATED_IDENTIFIER_TYPE.contains(r.related_identifier_type.as_str()) {
            bail!(
                "unknown relatedIdentifierType: {:?}",
                r.related_identifier_type
            );
        }
        if let Some(rtg) = &r.resource_type_general {
            if !datacite_enums::RESOURCE_TYPE_GENERAL.contains(rtg.as_str()) {
                bail!("unknown resourceTypeGeneral in resources: {rtg:?}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_template() -> EnrichmentTemplate {
        let yaml = r#"
enrichment:
  sources:
    - name: COMET
      contributorType: Producer
  resources:
    - relatedIdentifier: "10.x/y"
      relatedIdentifierType: DOI
      relationType: IsDocumentedBy
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        EnrichmentTemplate::from_config(&cfg)
    }

    #[test]
    fn record_keys_are_in_declared_order() {
        let t = simple_template();
        let rec = build_enrichment_record(
            &t,
            "10.5281/x",
            "update",
            "types",
            json!({"a":1}),
            json!({"a":2}),
        );
        let s = serde_json::to_string(&rec).unwrap();
        let order = [
            "doi",
            "contributors",
            "resources",
            "action",
            "field",
            "originalValue",
            "enrichedValue",
        ];
        let positions: Vec<_> = order
            .iter()
            .map(|k| s.find(&format!("\"{k}\":")).unwrap())
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(positions, sorted, "top-level keys out of order: {s}");
    }

    #[test]
    fn field_preservation_keeps_all_subfields() {
        let t = simple_template();
        let original = json!({
            "resourceType": "Journal article",
            "resourceTypeGeneral": "Text",
            "bibtex": "article",
            "citeproc": "article-journal",
            "schemaOrg": "ScholarlyArticle",
            "ris": "JOUR"
        });
        let mut enriched = original.clone();
        enriched["resourceTypeGeneral"] = json!("JournalArticle");
        let rec = build_enrichment_record(
            &t,
            "10.x/y",
            "update",
            "types",
            original.clone(),
            enriched.clone(),
        );
        for k in ["resourceType", "bibtex", "citeproc", "schemaOrg", "ris"] {
            assert_eq!(rec["originalValue"][k], original[k]);
            assert_eq!(rec["enrichedValue"][k], original[k]);
        }
        assert_eq!(rec["originalValue"]["resourceTypeGeneral"], json!("Text"));
        assert_eq!(
            rec["enrichedValue"]["resourceTypeGeneral"],
            json!("JournalArticle")
        );
    }

    #[test]
    fn load_enrichment_parses_sample_yaml() {
        let yaml = r#"
enrichment:
  sources:
    - name: "COMET"
      nameType: "Organizational"
      contributorType: "Producer"
  resources:
    - relatedIdentifier: "10.82461/bpzr-jd55"
      relatedIdentifierType: "DOI"
      relationType: "IsDocumentedBy"
      resourceTypeGeneral: "Project"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.enrichment.sources[0].name, "COMET");
        assert_eq!(
            cfg.enrichment.resources[0].related_identifier,
            "10.82461/bpzr-jd55"
        );
        validate_enrichment(&cfg).unwrap();
    }

    #[test]
    fn validate_enrichment_rejects_unknown_contributor_type() {
        let yaml = r"
enrichment:
  sources:
    - name: COMET
      contributorType: BadType
  resources:
    - relatedIdentifier: x
      relatedIdentifierType: DOI
      relationType: IsDocumentedBy
";
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(validate_enrichment(&cfg).is_err());
    }
}
