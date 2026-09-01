//! Validated run-level values copied into every enrichment record.
//!
//! The [`EnrichmentTemplate`] is built once from CLI arguments and reused while
//! records are written.

use crate::identifiers::is_valid_doi_name;
use crate::method::EnrichmentParts;
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

/// Values that are the same for every record in a run.
#[derive(Debug, Clone)]
pub struct EnrichmentTemplate {
    source_id: String,
}

impl EnrichmentTemplate {
    /// Build a template from the DOI name of the enrichment project, such as
    /// `10.1234/example`.
    ///
    /// # Errors
    ///
    /// Returns an error if `source_id` does not match DOI name syntax, such as
    /// `10.1234/example`.
    pub fn new(source_id: &str) -> Result<Self> {
        if !is_valid_doi_name(source_id) {
            return Err(anyhow!(
                "source id must be a DOI name such as 10.1234/example, got `{source_id}`"
            ));
        }
        Ok(Self {
            source_id: source_id.to_owned(),
        })
    }

    /// DOI name of the enrichment project that produced the records, such as
    /// `10.1234/example`.
    #[must_use]
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
}

/// Build one enrichment record.
///
/// Key order matches the enrichment schema and is covered by tests.
#[must_use]
pub fn build_enrichment_record(template: &EnrichmentTemplate, parts: EnrichmentParts) -> Value {
    let mut m = serde_json::Map::with_capacity(6);
    m.insert("doi".into(), Value::String(parts.doi));
    m.insert("action".into(), json!(parts.action.as_str()));
    m.insert("field".into(), json!(parts.field));
    m.insert("originalValue".into(), parts.original);
    m.insert("enrichedValue".into(), parts.enriched);
    m.insert("sourceId".into(), Value::String(template.source_id.clone()));
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::EnrichmentAction;

    const SOURCE_ID: &str = "10.82461/bpzr-jd55";

    fn template() -> EnrichmentTemplate {
        EnrichmentTemplate::new(SOURCE_ID).unwrap()
    }

    #[test]
    fn new_preserves_the_source_doi_name() {
        assert_eq!(template().source_id(), SOURCE_ID);
    }

    #[test]
    fn new_rejects_non_doi_source_id() {
        let err = EnrichmentTemplate::new("not-a-doi")
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be a DOI"), "got: {err}");
        assert!(err.contains("not-a-doi"), "got: {err}");
    }

    #[test]
    fn new_rejects_source_id_with_surrounding_whitespace() {
        assert!(EnrichmentTemplate::new(" 10.82461/bpzr-jd55").is_err());
        assert!(EnrichmentTemplate::new("10.82461/bpzr-jd55 ").is_err());
    }

    fn parts(original: Value, enriched: Value) -> EnrichmentParts {
        EnrichmentParts {
            doi: "10.5281/x".to_owned(),
            action: EnrichmentAction::Update,
            field: "types",
            original,
            enriched,
        }
    }

    #[test]
    fn record_keys_are_in_declared_order() {
        let rec = build_enrichment_record(&template(), parts(json!({"a":1}), json!({"a":2})));
        let s = serde_json::to_string(&rec).unwrap();
        let order = [
            "doi",
            "action",
            "field",
            "originalValue",
            "enrichedValue",
            "sourceId",
        ];
        let positions: Vec<_> = order
            .iter()
            .map(|k| s.find(&format!("\"{k}\":")).unwrap())
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(positions, sorted, "top-level keys out of order: {s}");
        assert_eq!(
            rec.as_object().unwrap().len(),
            order.len(),
            "unexpected keys: {s}"
        );
    }

    #[test]
    fn record_source_id_comes_from_template() {
        let rec = build_enrichment_record(&template(), parts(json!({"a":1}), json!({"a":2})));
        assert_eq!(rec["sourceId"], json!(SOURCE_ID));
    }

    #[test]
    fn field_preservation_keeps_all_subfields() {
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
        let rec = build_enrichment_record(&template(), parts(original.clone(), enriched));
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
}
