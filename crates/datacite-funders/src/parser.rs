//! Parser for DataCite funding references.
//!
//! Funder-name hashes use the original string bytes: no trimming or case folding.

use crate::FundingExtraction;
use crate::identifiers::{IdentifierScheme, normalize_fundref, normalize_ror, sniff_identifier};
use comet_enrich_core::{HashBits, hash_input};
use serde_json::Value;

/// DOI from the top-level `id`, falling back to `attributes.doi`.
pub(crate) fn extract_doi(record: &Value) -> Option<&str> {
    record
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| record.pointer("/attributes/doi").and_then(Value::as_str))
}

/// Extract one [`FundingExtraction`] per funding reference with a funder name.
pub(crate) fn parse_funding_references(
    doi: &str,
    record: &Value,
    hash_bits: HashBits,
) -> Vec<FundingExtraction> {
    let Some(Value::Array(refs)) = record.pointer("/attributes/fundingReferences") else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for (idx, entry) in refs.iter().enumerate() {
        // Empty names are skipped; whitespace-only names are kept.
        let Some(funder_name) = entry.get("funderName").and_then(Value::as_str) else {
            continue;
        };
        if funder_name.is_empty() {
            continue;
        }

        let (existing_identifier, existing_identifier_type) = resolve_identifier(entry);

        out.push(FundingExtraction {
            doi: doi.to_owned(),
            funding_ref_idx: idx,
            funder_name: funder_name.to_owned(),
            funder_name_hash: hash_input(funder_name, hash_bits),
            existing_identifier,
            existing_identifier_type,
            award_number: string_field(entry, "awardNumber"),
            award_title: string_field(entry, "awardTitle"),
            award_uri: string_field(entry, "awardUri"),
            original_funding_reference: entry.clone(),
        });
    }
    out
}

fn string_field(entry: &Value, key: &str) -> Option<String> {
    entry.get(key).and_then(Value::as_str).map(String::from)
}

/// Normalize known ROR/Fundref values; keep raw identifiers otherwise.
fn resolve_identifier(entry: &Value) -> (Option<String>, Option<String>) {
    let raw = entry.get("funderIdentifier").and_then(Value::as_str);
    let stated_type = entry.get("funderIdentifierType").and_then(Value::as_str);

    let Some(raw) = raw.filter(|s| !s.trim().is_empty()) else {
        return (None, None);
    };

    if let Some((scheme, canonical)) = sniff_identifier(raw) {
        let type_str = match scheme {
            IdentifierScheme::Ror => "ROR",
            IdentifierScheme::Fundref => "Crossref Funder ID",
        };
        return (Some(canonical), Some(type_str.to_owned()));
    }

    match stated_type {
        Some(t) if t.eq_ignore_ascii_case("ROR") => match normalize_ror(raw) {
            Some(canonical) => (Some(canonical), Some("ROR".to_owned())),
            None => (Some(raw.to_owned()), Some(t.to_owned())),
        },
        Some(t) if t.eq_ignore_ascii_case("Crossref Funder ID") => match normalize_fundref(raw) {
            Some(canonical) => (Some(canonical), Some("Crossref Funder ID".to_owned())),
            None => (Some(raw.to_owned()), Some(t.to_owned())),
        },
        Some(t) => (Some(raw.to_owned()), Some(t.to_owned())),
        None => (Some(raw.to_owned()), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn single_doi(funding_refs: Value) -> Value {
        let mut record = json!({"id": "10.1234/test", "attributes": {"doi": "10.1234/test"}});
        record["attributes"]["fundingReferences"] = funding_refs;
        record
    }

    fn parse(record: &Value) -> Vec<FundingExtraction> {
        parse_funding_references(extract_doi(record).unwrap(), record, HashBits::Bits64)
    }

    #[test]
    fn parse_returns_empty_without_funding_references() {
        for record in [
            json!({"id": "10.1234/test", "attributes": {}}),
            json!({"id": "10.1234/test", "attributes": {"fundingReferences": "not an array"}}),
            json!({"id": "10.1234/test", "attributes": {"fundingReferences": {}}}),
        ] {
            assert_eq!(parse(&record).len(), 0);
        }
    }

    #[test]
    fn parse_returns_empty_for_empty_array() {
        assert_eq!(parse(&single_doi(json!([]))).len(), 0);
    }

    #[test]
    fn parse_skips_entries_without_usable_funder_name() {
        let record = single_doi(json!([
            {"funderName": ""},
            {"awardNumber": "ABC-123"},
            {"funderName": 42},
            {"funderName": "Real Funder"}
        ]));

        let refs = parse(&record);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].funder_name, "Real Funder");
        // The source index is preserved, not renumbered.
        assert_eq!(refs[0].funding_ref_idx, 3);
    }

    #[test]
    fn parse_keeps_whitespace_only_funder_name() {
        let refs = parse(&single_doi(json!([{"funderName": "  "}])));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].funder_name, "  ");
    }

    #[test]
    fn parse_hashes_funder_name_verbatim() {
        let refs = parse(&single_doi(json!([{"funderName": "NSF"}])));
        assert_eq!(
            refs[0].funder_name_hash,
            hash_input("NSF", HashBits::Bits64)
        );
        assert_eq!(refs[0].funder_name_hash.len(), 16);
    }

    #[test]
    fn parse_emits_one_record_per_funding_reference() {
        let record = single_doi(json!([
            {"funderName": "NSF"},
            {"funderName": "DOE"},
            {"funderName": "NIH"}
        ]));

        let refs = parse(&record);

        assert_eq!(refs.len(), 3);
        assert_eq!(
            refs.iter().map(|r| r.funding_ref_idx).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(refs[1].funder_name, "DOE");
    }

    #[test]
    fn parse_preserves_award_fields() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "awardNumber": "ABC-123",
            "awardTitle": "A Grant",
            "awardUri": "https://example.com/grant"
        }]));

        let refs = parse(&record);

        assert_eq!(refs[0].award_number, Some("ABC-123".to_owned()));
        assert_eq!(refs[0].award_title, Some("A Grant".to_owned()));
        assert_eq!(
            refs[0].award_uri,
            Some("https://example.com/grant".to_owned())
        );
    }

    #[test]
    fn parse_reads_mixed_case_award_uri_key_only() {
        // DataCite JSON uses `awardUri`, not `awardURI`.
        let record = single_doi(json!([{
            "funderName": "NSF",
            "awardURI": "https://example.com/grant"
        }]));

        let refs = parse(&record);

        assert_eq!(refs[0].award_uri, None);
        assert_eq!(
            refs[0].original_funding_reference["awardURI"],
            "https://example.com/grant"
        );
    }

    #[test]
    fn parse_preserves_original_funding_reference() {
        let entry = json!({
            "funderName": "NSF",
            "schemeUri": "https://example.com",
            "weirdExtraKey": "kept"
        });
        let refs = parse(&single_doi(json!([entry])));
        assert_eq!(refs[0].original_funding_reference, entry);
    }

    #[test]
    fn parse_normalizes_bare_ror_identifier() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "021nxhr62",
            "funderIdentifierType": "ROR"
        }]));

        let refs = parse(&record);

        assert_eq!(
            refs[0].existing_identifier,
            Some("https://ror.org/021nxhr62".to_owned())
        );
        assert_eq!(refs[0].existing_identifier_type, Some("ROR".to_owned()));
    }

    #[test]
    fn parse_normalizes_ror_url_identifier() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "http://www.ror.org/021nxhr62/",
            "funderIdentifierType": "ROR"
        }]));
        assert_eq!(
            parse(&record)[0].existing_identifier,
            Some("https://ror.org/021nxhr62".to_owned())
        );
    }

    #[test]
    fn parse_normalizes_fundref_doi_url_identifier() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "https://doi.org/10.13039/100000001",
            "funderIdentifierType": "Crossref Funder ID"
        }]));

        let refs = parse(&record);

        assert_eq!(refs[0].existing_identifier, Some("100000001".to_owned()));
        assert_eq!(
            refs[0].existing_identifier_type,
            Some("Crossref Funder ID".to_owned())
        );
    }

    #[test]
    fn parse_overrides_mislabeled_ror_as_crossref() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "https://ror.org/021nxhr62",
            "funderIdentifierType": "Crossref Funder ID"
        }]));

        let refs = parse(&record);

        assert_eq!(
            refs[0].existing_identifier,
            Some("https://ror.org/021nxhr62".to_owned())
        );
        assert_eq!(refs[0].existing_identifier_type, Some("ROR".to_owned()));
    }

    #[test]
    fn parse_overrides_mislabeled_crossref_as_ror() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "10.13039/100000001",
            "funderIdentifierType": "ROR"
        }]));

        let refs = parse(&record);

        assert_eq!(refs[0].existing_identifier, Some("100000001".to_owned()));
        assert_eq!(
            refs[0].existing_identifier_type,
            Some("Crossref Funder ID".to_owned())
        );
    }

    #[test]
    fn parse_keeps_raw_value_when_normalization_fails() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "not a valid id",
            "funderIdentifierType": "ROR"
        }]));

        let refs = parse(&record);

        assert_eq!(
            refs[0].existing_identifier,
            Some("not a valid id".to_owned())
        );
        assert_eq!(refs[0].existing_identifier_type, Some("ROR".to_owned()));
    }

    #[test]
    fn parse_passes_through_isni_identifier() {
        let record = single_doi(json!([{
            "funderName": "Some Funder",
            "funderIdentifier": "0000 0001 2345 6789",
            "funderIdentifierType": "ISNI"
        }]));

        let refs = parse(&record);

        assert_eq!(
            refs[0].existing_identifier,
            Some("0000 0001 2345 6789".to_owned())
        );
        assert_eq!(refs[0].existing_identifier_type, Some("ISNI".to_owned()));
    }

    #[test]
    fn parse_leaves_identifier_none_for_name_only() {
        let refs = parse(&single_doi(json!([{"funderName": "NSF"}])));
        assert_eq!(refs[0].existing_identifier, None);
        assert_eq!(refs[0].existing_identifier_type, None);
    }

    #[test]
    fn parse_leaves_identifier_none_for_blank_identifier() {
        let record = single_doi(json!([{
            "funderName": "NSF",
            "funderIdentifier": "   ",
            "funderIdentifierType": "ROR"
        }]));
        let refs = parse(&record);
        assert_eq!(refs[0].existing_identifier, None);
        assert_eq!(refs[0].existing_identifier_type, None);
    }

    #[test]
    fn extract_doi_falls_back_to_attributes_doi() {
        assert_eq!(
            extract_doi(&json!({"attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/attr")
        );
        assert_eq!(
            extract_doi(&json!({"id": "10.1234/id", "attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/id")
        );
        assert_eq!(extract_doi(&json!({"attributes": {}})), None);
    }
}
