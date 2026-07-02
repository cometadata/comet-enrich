//! DataCite affiliation matching.
//!
//! Matches creator and contributor affiliations to ROR IDs via the match
//! service. A person is emitted only when at least one affiliation gains a new
//! ROR match.

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

mod parser;

use anyhow::Result;
use comet_enrich_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, HashBits, LookupConfig,
    Lookups, RorLookup,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Where a person appeared in the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordField {
    Creators,
    Contributors,
}

impl RecordField {
    /// The enrichment record's `field` value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Creators => "creators",
            Self::Contributors => "contributors",
        }
    }
}

/// One creator or contributor and their affiliation entries.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersonExtraction {
    pub doi: String,
    pub field: RecordField,
    pub idx: usize,
    /// Person object with the `affiliation` key removed.
    pub source_raw: Value,
    pub affiliations: Vec<AffiliationOccurrence>,
}

/// One affiliation entry on a person.
#[derive(Debug, Serialize, Deserialize)]
pub struct AffiliationOccurrence {
    pub affiliation: String,
    /// Hash of `affiliation`, used as the lookup join key.
    pub affiliation_hash: String,
    /// Original affiliation object; absent for plain-string entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affiliation_raw: Option<Value>,
    /// Existing ROR identifier on this entry, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_ror_id: Option<String>,
}

/// Matches DataCite creator and contributor affiliations to ROR IDs.
pub struct Affiliations {
    hash_bits: HashBits,
}

impl Affiliations {
    /// Build the affiliation matcher.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "method constructors take their config by value, uniformly across the CLI"
    )]
    pub fn try_new(config: LookupConfig) -> Result<Self> {
        Ok(Self {
            hash_bits: config.hash_bits,
        })
    }
}

fn raw_or_fallback(occurrence: &AffiliationOccurrence) -> Value {
    occurrence
        .affiliation_raw
        .clone()
        .unwrap_or_else(|| json!({ "name": occurrence.affiliation }))
}

fn person_with_affiliations(source_raw: &Value, affiliations: Vec<Value>) -> Value {
    let mut person = source_raw.as_object().cloned().unwrap_or_default();
    person.insert("affiliation".to_owned(), Value::Array(affiliations));
    Value::Object(person)
}

impl EnrichmentMethod for Affiliations {
    type Extraction = PersonExtraction;
    type Lookup = RorLookup;

    fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
        let Some(doi) = parser::extract_doi(record) else {
            return Extracted::Skip("no_doi");
        };
        let persons = parser::parse_persons(doi, record, self.hash_bits);
        if persons.is_empty() {
            return Extracted::Skip("no_affiliations");
        }
        Extracted::Items(persons)
    }

    fn inputs(&self, extraction: &Self::Extraction) -> Vec<String> {
        extraction
            .affiliations
            .iter()
            .map(|a| a.affiliation.clone())
            .collect()
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        // Emit only when this person gains a new ROR match. Once emitted,
        // rewrite every matched affiliation and preserve the rest.
        let has_new_match = extraction
            .affiliations
            .iter()
            .any(|a| a.existing_ror_id.is_none() && lookups.contains_key(&a.affiliation_hash));
        if !has_new_match {
            return Vec::new();
        }

        let original: Vec<Value> = extraction
            .affiliations
            .iter()
            .map(raw_or_fallback)
            .collect();
        let enriched: Vec<Value> = extraction
            .affiliations
            .iter()
            .map(|a| match lookups.get(&a.affiliation_hash) {
                Some(hit) => json!({
                    "name": a.affiliation,
                    "affiliationIdentifier": hit.ror_id,
                    "affiliationIdentifierScheme": "ROR",
                    "schemeUri": "https://ror.org"
                }),
                None => raw_or_fallback(a),
            })
            .collect();

        vec![EnrichmentParts {
            doi: extraction.doi,
            action: EnrichmentAction::UpdateChild,
            field: extraction.field.as_str(),
            original: person_with_affiliations(&extraction.source_raw, original),
            enriched: person_with_affiliations(&extraction.source_raw, enriched),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use comet_enrich_core::hash_input;

    const MIT_ROR: &str = "https://ror.org/042nb2s44";
    const OXFORD_ROR: &str = "https://ror.org/052gg0110";

    fn method() -> Affiliations {
        Affiliations {
            hash_bits: HashBits::Bits64,
        }
    }

    fn occurrence(
        name: &str,
        raw: Option<Value>,
        existing_ror_id: Option<&str>,
    ) -> AffiliationOccurrence {
        AffiliationOccurrence {
            affiliation: name.to_owned(),
            affiliation_hash: hash_input(name, HashBits::Bits64),
            affiliation_raw: raw,
            existing_ror_id: existing_ror_id.map(String::from),
        }
    }

    fn person(
        field: RecordField,
        source_raw: Value,
        affiliations: Vec<AffiliationOccurrence>,
    ) -> PersonExtraction {
        PersonExtraction {
            doi: "10.1234/abcd".to_owned(),
            field,
            idx: 0,
            source_raw,
            affiliations,
        }
    }

    fn lookups(pairs: &[(&str, &str, f64)]) -> Lookups<RorLookup> {
        pairs
            .iter()
            .map(|(name, ror_id, confidence)| {
                (
                    hash_input(name, HashBits::Bits64),
                    RorLookup {
                        ror_id: (*ror_id).to_owned(),
                        confidence: *confidence,
                    },
                )
            })
            .collect()
    }

    fn oxford_with_ror() -> Value {
        json!({"name": "University of Oxford",
            "affiliationIdentifier": OXFORD_ROR,
            "affiliationIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org"})
    }

    #[test]
    fn try_new_builds_from_lookup_config() {
        let config = LookupConfig {
            ror_service_url: "http://localhost:8000".to_owned(),
            ror_batch_size: 50,
            ror_concurrency: 1,
            ror_timeout: 30,
            hash_bits: HashBits::Bits64,
            from_scratch: false,
        };
        assert!(Affiliations::try_new(config).is_ok());
    }

    #[test]
    fn record_field_as_str_matches_schema_values() {
        assert_eq!(RecordField::Creators.as_str(), "creators");
        assert_eq!(RecordField::Contributors.as_str(), "contributors");
        // Keep serde names aligned with output field names.
        for field in [RecordField::Creators, RecordField::Contributors] {
            assert_eq!(serde_json::to_value(field).unwrap(), json!(field.as_str()));
        }
    }

    #[test]
    fn extract_skips_record_without_doi() {
        let record = json!({"attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [{"name": "MIT"}]}
        ]}});
        assert!(matches!(
            method().extract(&record),
            Extracted::Skip("no_doi")
        ));
    }

    #[test]
    fn extract_skips_record_without_usable_affiliations() {
        let no_creators = json!({"id": "10.1234/x", "attributes": {}});
        let no_affiliations = json!({"id": "10.1234/x", "attributes": {"creators": [
            {"name": "Doe, Jane"}
        ]}});
        for record in [no_creators, no_affiliations] {
            assert!(matches!(
                method().extract(&record),
                Extracted::Skip("no_affiliations")
            ));
        }
    }

    #[test]
    fn extract_emits_one_item_per_person() {
        let record = json!({"id": "10.1234/x", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [{"name": "MIT"}, {"name": "Oxford"}]},
            {"name": "Smith, John", "affiliation": [{"name": "MIT"}]}
        ]}});
        match method().extract(&record) {
            Extracted::Items(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].affiliations.len(), 2);
            }
            Extracted::Skip(r) => panic!("expected Items, got skip {r}"),
        }
    }

    #[test]
    fn inputs_returns_one_string_per_occurrence() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![
                occurrence("MIT", None, None),
                occurrence("University of Oxford", None, None),
            ],
        );
        assert_eq!(
            method().inputs(&p),
            vec!["MIT".to_owned(), "University of Oxford".to_owned()]
        );
    }

    #[test]
    fn map_back_emits_enriched_person_for_new_match() {
        let source = json!({"name": "Doe, Jane", "nameIdentifiers": [
            {"nameIdentifier": "0000-0001-2345-6789", "nameIdentifierScheme": "ORCID"}
        ]});
        let p = person(
            RecordField::Creators,
            source.clone(),
            vec![occurrence("MIT", Some(json!({"name": "MIT"})), None)],
        );

        let parts = method().map_back(p, &lookups(&[("MIT", MIT_ROR, 0.99)]));

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].doi, "10.1234/abcd");
        assert_eq!(parts[0].action, EnrichmentAction::UpdateChild);
        assert_eq!(parts[0].field, "creators");
        assert_eq!(
            parts[0].original["nameIdentifiers"],
            source["nameIdentifiers"]
        );
        assert_eq!(parts[0].original["affiliation"], json!([{"name": "MIT"}]));
        assert_eq!(
            parts[0].enriched["nameIdentifiers"],
            source["nameIdentifiers"]
        );
        assert_eq!(
            parts[0].enriched["affiliation"],
            json!([{
                "name": "MIT",
                "affiliationIdentifier": MIT_ROR,
                "affiliationIdentifierScheme": "ROR",
                "schemeUri": "https://ror.org"
            }])
        );
    }

    #[test]
    fn map_back_returns_empty_without_matches() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![occurrence("Unknown Institution", None, None)],
        );
        assert_eq!(method().map_back(p, &lookups(&[])).len(), 0);
    }

    #[test]
    fn map_back_keeps_existing_ror_affiliation_in_original_value() {
        // Regression: existing ROR affiliation survives when a sibling matches.
        let p = person(
            RecordField::Creators,
            json!({"name": "Klemm, Anna"}),
            vec![
                occurrence(
                    "University of Oxford",
                    Some(oxford_with_ror()),
                    Some(OXFORD_ROR),
                ),
                occurrence("MIT", Some(json!({"name": "MIT"})), None),
            ],
        );

        let parts = method().map_back(p, &lookups(&[("MIT", MIT_ROR, 0.99)]));

        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0].original["affiliation"],
            json!([oxford_with_ror(), {"name": "MIT"}])
        );
        assert_eq!(
            parts[0].enriched["affiliation"],
            json!([oxford_with_ror(), {
                "name": "MIT",
                "affiliationIdentifier": MIT_ROR,
                "affiliationIdentifierScheme": "ROR",
                "schemeUri": "https://ror.org"
            }])
        );
    }

    #[test]
    fn map_back_skips_person_whose_only_matches_have_existing_ror() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![occurrence(
                "University of Oxford",
                Some(oxford_with_ror()),
                Some(OXFORD_ROR),
            )],
        );
        let parts = method().map_back(p, &lookups(&[("University of Oxford", OXFORD_ROR, 0.9)]));
        assert_eq!(parts.len(), 0);
    }

    #[test]
    fn map_back_rewrites_matched_affiliation_that_had_existing_ror() {
        // A new sibling match opens the person; existing ROR matches are rewritten too.
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![
                occurrence(
                    "University of Oxford",
                    Some(oxford_with_ror()),
                    Some(OXFORD_ROR),
                ),
                occurrence("MIT", Some(json!({"name": "MIT"})), None),
            ],
        );

        let parts = method().map_back(
            p,
            &lookups(&[
                ("University of Oxford", OXFORD_ROR, 0.9),
                ("MIT", MIT_ROR, 0.99),
            ]),
        );

        assert_eq!(
            parts[0].enriched["affiliation"][0],
            json!({
                "name": "University of Oxford",
                "affiliationIdentifier": OXFORD_ROR,
                "affiliationIdentifierScheme": "ROR",
                "schemeUri": "https://ror.org"
            })
        );
    }

    #[test]
    fn map_back_preserves_unmatched_identifiers() {
        let isni_lab = json!({"name": "Some Lab",
            "affiliationIdentifier": "https://isni.org/isni/0000000121901201",
            "affiliationIdentifierScheme": "ISNI",
            "schemeUri": "https://isni.org"});
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![
                occurrence(
                    "University of Oxford",
                    Some(json!({"name": "University of Oxford"})),
                    None,
                ),
                occurrence("Some Lab", Some(isni_lab.clone()), None),
            ],
        );

        let parts = method().map_back(p, &lookups(&[("University of Oxford", OXFORD_ROR, 0.9)]));

        assert_eq!(parts[0].enriched["affiliation"][1], isni_lab);
    }

    #[test]
    fn map_back_preserves_unknown_person_fields() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane", "lang": "en"}),
            vec![occurrence("MIT", None, None)],
        );
        let parts = method().map_back(p, &lookups(&[("MIT", MIT_ROR, 0.99)]));
        assert_eq!(parts[0].original["lang"], "en");
        assert_eq!(parts[0].enriched["lang"], "en");
    }

    #[test]
    fn map_back_sets_field_from_record_field() {
        let creator = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![occurrence("MIT", None, None)],
        );
        let contributor = person(
            RecordField::Contributors,
            json!({"name": "Doe, Jane", "contributorType": "Supervisor"}),
            vec![occurrence("MIT", None, None)],
        );
        let matches = lookups(&[("MIT", MIT_ROR, 0.99)]);

        let creator_parts = method().map_back(creator, &matches);
        let contributor_parts = method().map_back(contributor, &matches);

        assert_eq!(creator_parts[0].field, "creators");
        assert_eq!(contributor_parts[0].field, "contributors");
        assert_eq!(
            contributor_parts[0].original["contributorType"],
            "Supervisor"
        );
        assert_eq!(
            contributor_parts[0].enriched["contributorType"],
            "Supervisor"
        );
    }

    #[test]
    fn map_back_uses_name_fallback_for_string_affiliations() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![
                occurrence("MIT", None, None),
                occurrence("Unknown Institution", None, None),
            ],
        );

        let parts = method().map_back(p, &lookups(&[("MIT", MIT_ROR, 0.99)]));

        assert_eq!(parts[0].original["affiliation"][0], json!({"name": "MIT"}));
        assert_eq!(
            parts[0].original["affiliation"][1],
            json!({"name": "Unknown Institution"})
        );
        assert_eq!(
            parts[0].enriched["affiliation"][1],
            json!({"name": "Unknown Institution"})
        );
    }

    #[test]
    fn map_back_ignores_confidence() {
        let p = person(
            RecordField::Creators,
            json!({"name": "Doe, Jane"}),
            vec![occurrence("MIT", None, None)],
        );
        let parts = method().map_back(p, &lookups(&[("MIT", MIT_ROR, 0.01)]));
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0].enriched["affiliation"][0]["affiliationIdentifier"],
            MIT_ROR
        );
    }
}
