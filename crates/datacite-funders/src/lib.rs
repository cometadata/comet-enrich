//! DataCite funder matching.
//!
//! Matches funding reference names to ROR IDs. References that already resolve
//! to ROR are skipped.

// DataCite, ROR, and COMET are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

mod crosswalk;
mod identifiers;
mod parser;

use anyhow::{Context, Result};
use comet_enrich_core::{
    EnrichmentAction, EnrichmentMethod, EnrichmentParts, Extracted, HashBits, LookupConfig,
    Lookups, RorLookup,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Configuration for funder matching.
pub struct Config {
    pub lookup: LookupConfig,
    /// ROR registry JSON used to map Crossref Funder IDs to ROR.
    pub ror_file: PathBuf,
}

/// One funding reference extracted for matching.
#[derive(Debug, Serialize, Deserialize)]
pub struct FundingExtraction {
    pub doi: String,
    pub funding_ref_idx: usize,
    pub funder_name: String,
    /// Hash of `funder_name`, used as the lookup join key.
    pub funder_name_hash: String,
    /// Existing identifier after ROR/Fundref normalization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_identifier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_identifier_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub award_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub award_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub award_uri: Option<String>,
    /// Original funding reference object.
    pub original_funding_reference: Value,
}

/// Matches DataCite funder names to ROR IDs.
pub struct Funders {
    hash_bits: HashBits,
    ror_file: PathBuf,
    crosswalk: OnceLock<HashMap<String, String>>,
}

impl Funders {
    /// Build the funder matcher and validate the ROR registry path.
    pub fn try_new(config: Config) -> Result<Self> {
        File::open(&config.ror_file)
            .with_context(|| format!("opening ROR registry {}", config.ror_file.display()))?;
        Ok(Self {
            hash_bits: config.lookup.hash_bits,
            ror_file: config.ror_file,
            crosswalk: OnceLock::new(),
        })
    }

    /// Crossref Funder ID to ROR map, parsed once.
    fn crosswalk(&self) -> &HashMap<String, String> {
        self.crosswalk
            .get_or_init(|| crosswalk::load(&self.ror_file).unwrap_or_else(|e| panic!("{e:#}")))
    }

    fn has_existing_resolution(&self, extraction: &FundingExtraction) -> bool {
        let (Some(id), Some(id_type)) = (
            extraction.existing_identifier.as_deref(),
            extraction.existing_identifier_type.as_deref(),
        ) else {
            return false;
        };
        if id_type.eq_ignore_ascii_case("ROR") {
            return true;
        }
        id_type.eq_ignore_ascii_case("Crossref Funder ID") && self.crosswalk().contains_key(id)
    }
}

impl EnrichmentMethod for Funders {
    type Extraction = FundingExtraction;
    type Lookup = RorLookup;

    fn extract(&self, record: &Value) -> Extracted<Self::Extraction> {
        let Some(doi) = parser::extract_doi(record) else {
            return Extracted::Skip("no_doi");
        };
        let refs = parser::parse_funding_references(doi, record, self.hash_bits);
        if refs.is_empty() {
            return Extracted::Skip("no_funding_references");
        }
        Extracted::Items(refs)
    }

    fn inputs(&self, extraction: &Self::Extraction) -> Vec<String> {
        vec![extraction.funder_name.clone()]
    }

    fn map_back(
        &self,
        extraction: Self::Extraction,
        lookups: &Lookups<Self::Lookup>,
    ) -> Vec<EnrichmentParts> {
        // Skip references that already resolve to ROR.
        if self.has_existing_resolution(&extraction) {
            return Vec::new();
        }
        let Some(hit) = lookups.get(&extraction.funder_name_hash) else {
            return Vec::new();
        };

        let original = extraction.original_funding_reference;
        let mut enriched = original.clone();
        if let Some(obj) = enriched.as_object_mut() {
            obj.insert("funderIdentifier".to_owned(), json!(hit.ror_id));
            obj.insert("funderIdentifierType".to_owned(), json!("ROR"));
            obj.insert("schemeUri".to_owned(), json!("https://ror.org"));
        }

        vec![EnrichmentParts {
            doi: extraction.doi,
            action: EnrichmentAction::UpdateChild,
            field: "fundingReferences",
            original,
            enriched,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use comet_enrich_core::hash_input;
    use comet_enrich_test_support::assert_err_contains;
    use std::io::Write;

    const NSF_ROR: &str = "https://ror.org/021nxhr62";
    const DOE_ROR: &str = "https://ror.org/01bj3aw27";

    const MINIMAL_ROR_DUMP: &str = r#"[
      {"id": "https://ror.org/021nxhr62",
       "names": [{"value": "National Science Foundation", "types": ["ror_display"]}],
       "external_ids": [{"type": "fundref", "all": ["100000001"], "preferred": "100000001"}]}
    ]"#;

    fn ror_dump_file() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(MINIMAL_ROR_DUMP.as_bytes()).unwrap();
        file
    }

    fn lookup_config() -> LookupConfig {
        LookupConfig {
            ror_service_url: "http://localhost:8000".to_owned(),
            ror_batch_size: 50,
            ror_concurrency: 1,
            ror_timeout: 30,
            hash_bits: HashBits::Bits64,
            from_scratch: false,
        }
    }

    fn method(dump: &tempfile::NamedTempFile) -> Funders {
        Funders::try_new(Config {
            lookup: lookup_config(),
            ror_file: dump.path().to_path_buf(),
        })
        .unwrap()
    }

    fn extraction(
        name: &str,
        identifier: Option<(&str, &str)>,
        original: Value,
    ) -> FundingExtraction {
        FundingExtraction {
            doi: "10.1234/abcd".to_owned(),
            funding_ref_idx: 0,
            funder_name: name.to_owned(),
            funder_name_hash: hash_input(name, HashBits::Bits64),
            existing_identifier: identifier.map(|(id, _)| id.to_owned()),
            existing_identifier_type: identifier.map(|(_, t)| t.to_owned()),
            award_number: None,
            award_title: None,
            award_uri: None,
            original_funding_reference: original,
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

    #[test]
    fn try_new_builds_from_config() {
        let dump = ror_dump_file();
        assert!(
            Funders::try_new(Config {
                lookup: lookup_config(),
                ror_file: dump.path().to_path_buf(),
            })
            .is_ok()
        );
    }

    #[test]
    fn try_new_errors_on_missing_ror_file() {
        assert_err_contains(
            Funders::try_new(Config {
                lookup: lookup_config(),
                ror_file: PathBuf::from("__missing_ror__.json"),
            }),
            "opening ROR registry",
        );
    }

    #[test]
    fn extract_skips_record_without_doi() {
        let dump = ror_dump_file();
        let record = json!({"attributes": {"fundingReferences": [{"funderName": "NSF"}]}});
        assert!(matches!(
            method(&dump).extract(&record),
            Extracted::Skip("no_doi")
        ));
    }

    #[test]
    fn extract_skips_record_without_funding_references() {
        let dump = ror_dump_file();
        let no_refs = json!({"id": "10.1234/x", "attributes": {}});
        let no_usable_names = json!({"id": "10.1234/x", "attributes": {"fundingReferences": [
            {"funderName": ""}
        ]}});
        for record in [no_refs, no_usable_names] {
            assert!(matches!(
                method(&dump).extract(&record),
                Extracted::Skip("no_funding_references")
            ));
        }
    }

    #[test]
    fn extract_emits_one_item_per_funding_reference() {
        let dump = ror_dump_file();
        let record = json!({"id": "10.1234/x", "attributes": {"fundingReferences": [
            {"funderName": "NSF"},
            {"funderName": "DOE"}
        ]}});
        match method(&dump).extract(&record) {
            Extracted::Items(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].funder_name, "NSF");
                assert_eq!(items[1].funder_name, "DOE");
            }
            Extracted::Skip(r) => panic!("expected Items, got skip {r}"),
        }
    }

    #[test]
    fn inputs_returns_funder_name() {
        let dump = ror_dump_file();
        let x = extraction("NSF", None, json!({"funderName": "NSF"}));
        assert_eq!(method(&dump).inputs(&x), vec!["NSF".to_owned()]);
    }

    #[test]
    fn map_back_emits_enriched_reference_for_match() {
        let dump = ror_dump_file();
        let original = json!({
            "funderName": "NSF",
            "awardNumber": "ABC-123",
            "awardTitle": "A Grant",
            "weirdKey": "kept",
            "schemeUri": "https://example.com"
        });
        let x = extraction("NSF", None, original.clone());

        let parts = method(&dump).map_back(x, &lookups(&[("NSF", NSF_ROR, 0.99)]));

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].doi, "10.1234/abcd");
        assert_eq!(parts[0].action, EnrichmentAction::UpdateChild);
        assert_eq!(parts[0].field, "fundingReferences");
        assert_eq!(parts[0].original, original);
        assert_eq!(
            parts[0].enriched,
            json!({
                "funderName": "NSF",
                "awardNumber": "ABC-123",
                "awardTitle": "A Grant",
                "weirdKey": "kept",
                "schemeUri": "https://ror.org",
                "funderIdentifier": NSF_ROR,
                "funderIdentifierType": "ROR"
            })
        );
    }

    #[test]
    fn map_back_returns_empty_without_match() {
        let dump = ror_dump_file();
        let x = extraction(
            "Unknown Funder",
            None,
            json!({"funderName": "Unknown Funder"}),
        );
        assert_eq!(method(&dump).map_back(x, &lookups(&[])).len(), 0);
    }

    #[test]
    fn map_back_skips_reference_with_asserted_ror() {
        let dump = ror_dump_file();
        for id_type in ["ROR", "ror"] {
            let x = extraction(
                "NSF",
                Some((NSF_ROR, id_type)),
                json!({"funderName": "NSF"}),
            );
            let parts = method(&dump).map_back(x, &lookups(&[("NSF", NSF_ROR, 0.99)]));
            assert_eq!(parts.len(), 0, "type {id_type}");
        }
    }

    #[test]
    fn map_back_skips_reference_with_crosswalk_mapped_crossref_id() {
        let dump = ror_dump_file();
        let x = extraction(
            "NSF",
            Some(("100000001", "Crossref Funder ID")),
            json!({"funderName": "NSF"}),
        );
        let parts = method(&dump).map_back(x, &lookups(&[("NSF", NSF_ROR, 0.99)]));
        assert_eq!(parts.len(), 0);
    }

    #[test]
    fn map_back_enriches_reference_with_unmapped_crossref_id() {
        let dump = ror_dump_file();
        let x = extraction(
            "DOE",
            Some(("999999999", "Crossref Funder ID")),
            json!({"funderName": "DOE", "funderIdentifier": "999999999",
                   "funderIdentifierType": "Crossref Funder ID"}),
        );

        let parts = method(&dump).map_back(x, &lookups(&[("DOE", DOE_ROR, 0.9)]));

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].enriched["funderIdentifier"], DOE_ROR);
        assert_eq!(parts[0].enriched["funderIdentifierType"], "ROR");
        assert_eq!(parts[0].original["funderIdentifier"], "999999999");
    }

    #[test]
    fn map_back_enriches_reference_with_non_ror_identifier() {
        let dump = ror_dump_file();
        let x = extraction(
            "NSF",
            Some(("0000 0001 2345 6789", "ISNI")),
            json!({"funderName": "NSF"}),
        );
        let parts = method(&dump).map_back(x, &lookups(&[("NSF", NSF_ROR, 0.99)]));
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn map_back_ignores_confidence() {
        let dump = ror_dump_file();
        let x = extraction("NSF", None, json!({"funderName": "NSF"}));
        let parts = method(&dump).map_back(x, &lookups(&[("NSF", NSF_ROR, 0.01)]));
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].enriched["funderIdentifier"], NSF_ROR);
    }
}
