//! Provenance config and record-building helpers.
//!
//! A provenance YAML file describes the contributors and resources added to every
//! enrichment record. The file is loaded once into an [`EnrichmentTemplate`], then
//! reused while records are written.

use crate::datacite_enums;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;

// Reject unknown YAML keys so typos fail at load time instead of being ignored.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentConfig {
    pub contributors: Vec<ContributorConfig>,
    pub resources: Vec<ResourceConfig>,
}

// Serde serializes fields in declaration order. Keep this aligned with the
// contributors shape in the enrichment schema.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ContributorConfig {
    pub name: String,
    #[serde(rename = "nameType", skip_serializing_if = "Option::is_none")]
    pub name_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
    #[serde(rename = "contributorType")]
    pub contributor_type: String,
    #[serde(rename = "givenName", skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    #[serde(rename = "familyName", skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    #[serde(rename = "nameIdentifiers", skip_serializing_if = "Option::is_none")]
    pub name_identifiers: Option<Vec<NameIdentifier>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliation: Option<Vec<Affiliation>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NameIdentifier {
    #[serde(rename = "nameIdentifier")]
    pub name_identifier: String,
    #[serde(rename = "nameIdentifierScheme")]
    pub name_identifier_scheme: String,
    #[serde(rename = "schemeUri", skip_serializing_if = "Option::is_none")]
    pub scheme_uri: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Affiliation {
    pub name: String,
    #[serde(
        rename = "affiliationIdentifier",
        skip_serializing_if = "Option::is_none"
    )]
    pub affiliation_identifier: Option<String>,
    #[serde(
        rename = "affiliationIdentifierScheme",
        skip_serializing_if = "Option::is_none"
    )]
    pub affiliation_identifier_scheme: Option<String>,
    #[serde(rename = "schemeUri", skip_serializing_if = "Option::is_none")]
    pub scheme_uri: Option<String>,
}

// Serde serializes fields in declaration order. Keep this aligned with the
// resources shape in the enrichment schema.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    #[serde(rename = "relatedIdentifier")]
    pub related_identifier: String,
    #[serde(rename = "relatedIdentifierType")]
    pub related_identifier_type: String,
    #[serde(rename = "relationType")]
    pub relation_type: String,
    #[serde(
        rename = "relatedMetadataScheme",
        skip_serializing_if = "Option::is_none"
    )]
    pub related_metadata_scheme: Option<String>,
    #[serde(rename = "schemeUri", skip_serializing_if = "Option::is_none")]
    pub scheme_uri: Option<String>,
    #[serde(
        rename = "resourceTypeGeneral",
        skip_serializing_if = "Option::is_none"
    )]
    pub resource_type_general: Option<String>,
}

/// Provenance values pre-rendered as JSON for reuse while writing records.
pub struct EnrichmentTemplate {
    pub contributors: Value,
    pub resources: Value,
}

impl EnrichmentTemplate {
    /// Build a reusable template from a provenance config.
    #[must_use]
    pub fn from_config(cfg: &EnrichmentConfig) -> Self {
        // The config structs already match the output JSON shape through their
        // serde rename and skip_serializing_if attributes.
        let contributors = Value::Array(
            cfg.contributors
                .iter()
                .map(|c| serde_json::to_value(c).expect("ContributorConfig serializes"))
                .collect(),
        );
        let resources = Value::Array(
            cfg.resources
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

/// Build one enrichment record from method output and the shared provenance template.
///
/// `field` is the top-level DataCite field being enriched, such as `"types"`.
/// Key order is fixed and covered by tests.
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

/// Load provenance YAML and render it for use in emitted records.
///
/// This is called before scanning input files so provenance errors fail quickly.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_template<P: AsRef<Path>>(path: P) -> Result<EnrichmentTemplate> {
    let cfg = load_enrichment(path)?;
    Ok(EnrichmentTemplate::from_config(&cfg))
}

/// Load and validate provenance YAML.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_enrichment<P: AsRef<Path>>(path: P) -> Result<EnrichmentConfig> {
    let text = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("reading {}", path.as_ref().display()))?;
    let cfg: EnrichmentConfig = serde_yaml::from_str(&text)
        .with_context(|| format!("parsing {}", path.as_ref().display()))?;
    validate_enrichment(&cfg)
        .with_context(|| format!("invalid provenance in {}", path.as_ref().display()))?;
    Ok(cfg)
}

/// Format valid vocabulary values for validation error messages.
fn valid_values(set: &std::collections::HashSet<String>) -> String {
    let mut v: Vec<&str> = set.iter().map(String::as_str).collect();
    v.sort_unstable();
    v.join(", ")
}

/// Add an error for an unknown controlled-vocabulary value.
fn push_unknown(
    problems: &mut Vec<String>,
    entry: &str,
    field: &str,
    value: &str,
    valid: &std::collections::HashSet<String>,
) {
    problems.push(format!(
        "{entry}: unknown {field} {value:?}\n      (valid: {})",
        valid_values(valid)
    ));
}

/// Validate provenance against the COMET Enrichment Data Model.
///
/// Checks DataCite controlled-vocabulary fields and the required COMET provenance
/// entries. All problems are collected and returned together so the config can be
/// fixed in one pass.
///
/// # Errors
///
/// Returns an error listing every validation problem found.
pub fn validate_enrichment(cfg: &EnrichmentConfig) -> Result<()> {
    let mut problems: Vec<String> = Vec::new();

    if cfg.contributors.is_empty() {
        problems.push("contributors must have at least one entry".into());
    }
    if cfg.resources.is_empty() {
        problems.push("resources must have at least one entry".into());
    }

    for (i, c) in cfg.contributors.iter().enumerate() {
        let who = format!("contributors[{i}] {:?}", c.name);
        if !datacite_enums::CONTRIBUTOR_TYPE.contains(c.contributor_type.as_str()) {
            push_unknown(
                &mut problems,
                &who,
                "contributorType",
                &c.contributor_type,
                &datacite_enums::CONTRIBUTOR_TYPE,
            );
        }
        if let Some(nt) = &c.name_type {
            if !datacite_enums::NAME_TYPE.contains(nt.as_str()) {
                push_unknown(
                    &mut problems,
                    &who,
                    "nameType",
                    nt,
                    &datacite_enums::NAME_TYPE,
                );
            }
        }
    }

    for (i, r) in cfg.resources.iter().enumerate() {
        let what = format!("resources[{i}] {:?}", r.related_identifier);
        if !datacite_enums::RELATION_TYPE.contains(r.relation_type.as_str()) {
            push_unknown(
                &mut problems,
                &what,
                "relationType",
                &r.relation_type,
                &datacite_enums::RELATION_TYPE,
            );
        }
        if !datacite_enums::RELATED_IDENTIFIER_TYPE.contains(r.related_identifier_type.as_str()) {
            push_unknown(
                &mut problems,
                &what,
                "relatedIdentifierType",
                &r.related_identifier_type,
                &datacite_enums::RELATED_IDENTIFIER_TYPE,
            );
        }
        if let Some(rtg) = &r.resource_type_general {
            if !datacite_enums::RESOURCE_TYPE_GENERAL.contains(rtg.as_str()) {
                push_unknown(
                    &mut problems,
                    &what,
                    "resourceTypeGeneral",
                    rtg,
                    &datacite_enums::RESOURCE_TYPE_GENERAL,
                );
            }
        }
    }

    // Required provenance entries from the COMET Enrichment Data Model.
    let has_comet_producer = cfg.contributors.iter().any(|c| {
        c.name == "COMET"
            && c.name_type.as_deref() == Some("Organizational")
            && c.contributor_type == "Producer"
    });
    if !has_comet_producer {
        problems.push("missing required contributor: COMET / Organizational / Producer".into());
    }

    let has_project_doc = cfg.resources.iter().any(|r| {
        r.relation_type == "IsDocumentedBy" && r.resource_type_general.as_deref() == Some("Project")
    });
    if !has_project_doc {
        problems.push(
            "missing required resource: IsDocumentedBy / Project (Enrichment Project documentation)"
                .into(),
        );
    }

    let has_derived_dataset = cfg.resources.iter().any(|r| {
        r.relation_type == "IsDerivedFrom" && r.resource_type_general.as_deref() == Some("Dataset")
    });
    if !has_derived_dataset {
        problems
            .push("missing required resource: IsDerivedFrom / Dataset (enriched dataset)".into());
    }

    if !problems.is_empty() {
        bail!("\n  - {}", problems.join("\n  - "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_template() -> EnrichmentTemplate {
        let yaml = r#"
contributors:
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

    /// Minimal config satisfying the full model: COMET Producer plus both required resources.
    const VALID_YAML: &str = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "10.82461/bpzr-jd55"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
  - relatedIdentifier: "https://huggingface.co/datasets/cometadata/example"
    relatedIdentifierType: "URL"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
"#;

    #[test]
    fn load_enrichment_parses_sample_yaml() {
        let cfg: EnrichmentConfig = serde_yaml::from_str(VALID_YAML).unwrap();
        assert_eq!(cfg.contributors[0].name, "COMET");
        assert_eq!(cfg.resources[0].related_identifier, "10.82461/bpzr-jd55");
        validate_enrichment(&cfg).unwrap();
    }

    #[test]
    fn validate_enrichment_rejects_unknown_contributor_type() {
        // Valid base config plus one contributor with a bad contributorType.
        let yaml = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
  - name: "x"
    contributorType: "BadType"
resources:
  - relatedIdentifier: "10.82461/x"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
  - relatedIdentifier: "10.1234/d"
    relatedIdentifierType: "DOI"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        let err = validate_enrichment(&cfg).unwrap_err().to_string();
        assert!(err.contains("BadType"), "got: {err}");
        assert!(err.contains("contributors[1]"), "got: {err}");
    }

    #[test]
    fn typed_contributors_round_trip_to_expected_json() {
        let yaml = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
  - name: "eLife Sciences Publications"
    nameType: "Organizational"
    lang: "en"
    contributorType: "Producer"
    nameIdentifiers:
      - nameIdentifier: "https://ror.org/04rjz5883"
        schemeUri: "https://ror.org/"
        nameIdentifierScheme: "ROR"
  - name: "Buttrick, Adam"
    nameType: "Personal"
    affiliation:
      - name: "California Digital Library"
        affiliationIdentifier: "https://ror.org/03yrm5c26"
        affiliationIdentifierScheme: "ROR"
    contributorType: "DataCurator"
    nameIdentifiers:
      - nameIdentifier: "https://orcid.org/0000-0003-1507-1031"
        schemeUri: "https://orcid.org"
        nameIdentifierScheme: "ORCID"
resources:
  - relatedIdentifier: "10.82461/m8a8-m211"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
  - relatedIdentifier: "10.1234/example_dataset"
    relatedIdentifierType: "DOI"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        validate_enrichment(&cfg).unwrap();
        let template = EnrichmentTemplate::from_config(&cfg);

        // The community contributor keeps `lang` and the typed ROR identifier.
        assert_eq!(
            template.contributors[1],
            json!({
                "name": "eLife Sciences Publications",
                "nameType": "Organizational",
                "lang": "en",
                "contributorType": "Producer",
                "nameIdentifiers": [{
                    "nameIdentifier": "https://ror.org/04rjz5883",
                    "nameIdentifierScheme": "ROR",
                    "schemeUri": "https://ror.org/"
                }]
            })
        );
        // The curator keeps the typed affiliation and ORCID identifier.
        assert_eq!(
            template.contributors[2],
            json!({
                "name": "Buttrick, Adam",
                "nameType": "Personal",
                "contributorType": "DataCurator",
                "affiliation": [{
                    "name": "California Digital Library",
                    "affiliationIdentifier": "https://ror.org/03yrm5c26",
                    "affiliationIdentifierScheme": "ROR"
                }],
                "nameIdentifiers": [{
                    "nameIdentifier": "https://orcid.org/0000-0003-1507-1031",
                    "nameIdentifierScheme": "ORCID",
                    "schemeUri": "https://orcid.org"
                }]
            })
        );
    }

    #[test]
    fn rejects_unknown_field() {
        // A typo should fail at parse time, not disappear from the output.
        let yaml = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorTpe: "Producer"
resources:
  - relatedIdentifier: "10.82461/x"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
"#;
        let err = serde_yaml::from_str::<EnrichmentConfig>(yaml).unwrap_err();
        assert!(err.to_string().contains("unknown field"), "got: {err}");
    }

    #[test]
    fn validate_enrichment_reports_all_problems() {
        let yaml = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
  - name: "eLife"
    nameType: "Organizational"
    contributorType: "Producr"
resources:
  - relatedIdentifier: "10.82461/x"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
  - relatedIdentifier: "10.1234/d"
    relatedIdentifierType: "DOI"
    relationType: "IsDervedFrom"
    resourceTypeGeneral: "Dataset"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(yaml).unwrap();
        let err = validate_enrichment(&cfg).unwrap_err().to_string();
        // Report both invalid controlled-vocabulary values in one error.
        assert!(err.contains("contributors[1]"), "got: {err}");
        assert!(err.contains("Producr"), "got: {err}");
        assert!(err.contains("resources[1]"), "got: {err}");
        assert!(err.contains("IsDervedFrom"), "got: {err}");
    }

    #[test]
    fn validate_enrichment_enforces_required_entries() {
        // Missing the derived Dataset resource.
        let no_dataset = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "10.82461/x"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(no_dataset).unwrap();
        let err = validate_enrichment(&cfg).unwrap_err().to_string();
        assert!(err.contains("IsDerivedFrom / Dataset"), "got: {err}");

        // Missing the Project documentation resource.
        let no_project = r#"
contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "10.1234/d"
    relatedIdentifierType: "DOI"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(no_project).unwrap();
        let err = validate_enrichment(&cfg).unwrap_err().to_string();
        assert!(err.contains("IsDocumentedBy / Project"), "got: {err}");

        // Missing the COMET Producer contributor.
        let no_comet = r#"
contributors:
  - name: "eLife"
    nameType: "Organizational"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "10.82461/x"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
  - relatedIdentifier: "10.1234/d"
    relatedIdentifierType: "DOI"
    relationType: "IsDerivedFrom"
    resourceTypeGeneral: "Dataset"
"#;
        let cfg: EnrichmentConfig = serde_yaml::from_str(no_comet).unwrap();
        let err = validate_enrichment(&cfg).unwrap_err().to_string();
        assert!(
            err.contains("COMET / Organizational / Producer"),
            "got: {err}"
        );
    }
}
