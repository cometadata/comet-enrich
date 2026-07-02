//! Parser for DataCite creator and contributor affiliations.
//!
//! Affiliation hashes use the original string bytes: no trimming or case folding.

use crate::{AffiliationOccurrence, PersonExtraction, RecordField};
use comet_enrich_core::{HashBits, hash_input};
use serde_json::Value;

/// DOI from the top-level `id`, falling back to `attributes.doi`.
pub(crate) fn extract_doi(record: &Value) -> Option<&str> {
    record
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| record.pointer("/attributes/doi").and_then(Value::as_str))
}

fn affiliation_name(entry: &Value) -> Option<&str> {
    match entry {
        Value::String(s) => Some(s),
        Value::Object(_) => entry.get("name").and_then(Value::as_str),
        _ => None,
    }
}

fn existing_ror_id(entry: &Value) -> Option<String> {
    let scheme = entry
        .get("affiliationIdentifierScheme")
        .and_then(Value::as_str)?;
    if scheme.eq_ignore_ascii_case("ROR") {
        entry
            .get("affiliationIdentifier")
            .and_then(Value::as_str)
            .map(String::from)
    } else {
        None
    }
}

fn occurrence(entry: &Value, hash_bits: HashBits) -> Option<AffiliationOccurrence> {
    let name = affiliation_name(entry)?;
    if name.is_empty() {
        return None;
    }
    Some(AffiliationOccurrence {
        affiliation: name.to_owned(),
        affiliation_hash: hash_input(name, hash_bits),
        affiliation_raw: entry.is_object().then(|| entry.clone()),
        existing_ror_id: existing_ror_id(entry),
    })
}

/// Extract people from creators, then contributors.
pub(crate) fn parse_persons(
    doi: &str,
    record: &Value,
    hash_bits: HashBits,
) -> Vec<PersonExtraction> {
    let mut persons = Vec::new();
    for (pointer, field) in [
        ("/attributes/creators", RecordField::Creators),
        ("/attributes/contributors", RecordField::Contributors),
    ] {
        if let Some(Value::Array(arr)) = record.pointer(pointer) {
            parse_field(doi, arr, field, hash_bits, &mut persons);
        }
    }
    persons
}

fn parse_field(
    doi: &str,
    persons: &[Value],
    field: RecordField,
    hash_bits: HashBits,
    out: &mut Vec<PersonExtraction>,
) {
    for (idx, person) in persons.iter().enumerate() {
        // Skip malformed person entries.
        if person.get("name").and_then(Value::as_str).is_none() {
            continue;
        }
        let Some(Value::Array(entries)) = person.get("affiliation") else {
            continue;
        };

        let affiliations: Vec<AffiliationOccurrence> = entries
            .iter()
            .filter_map(|entry| occurrence(entry, hash_bits))
            .collect();
        if affiliations.is_empty() {
            continue;
        }

        let mut source_raw = person.clone();
        if let Some(obj) = source_raw.as_object_mut() {
            obj.remove("affiliation");
        }

        out.push(PersonExtraction {
            doi: doi.to_owned(),
            field,
            idx,
            source_raw,
            affiliations,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn parse(record: &Value) -> Vec<PersonExtraction> {
        parse_persons(extract_doi(record).unwrap(), record, HashBits::Bits64)
    }

    #[test]
    fn parse_groups_affiliations_per_person() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "givenName": "Jane", "familyName": "Doe",
             "affiliation": [{"name": "University of Oxford"}, {"name": "MIT"}]},
            {"name": "Smith, John", "affiliation": [{"name": "Stanford University"}]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons.len(), 2);
        assert_eq!(persons[0].doi, "10.1234/test");
        assert_eq!(persons[0].field, RecordField::Creators);
        assert_eq!(persons[0].idx, 0);
        assert_eq!(persons[0].source_raw["name"], "Doe, Jane");
        assert_eq!(persons[0].affiliations.len(), 2);
        assert_eq!(
            persons[0].affiliations[0].affiliation,
            "University of Oxford"
        );
        assert_eq!(
            persons[0].affiliations[0].affiliation_hash,
            hash_input("University of Oxford", HashBits::Bits64)
        );
        assert_eq!(persons[0].affiliations[0].affiliation_hash.len(), 16);
        assert_eq!(persons[0].affiliations[1].affiliation, "MIT");
        assert_eq!(persons[1].idx, 1);
        assert_eq!(persons[1].source_raw["name"], "Smith, John");
        assert_eq!(
            persons[1].affiliations[0].affiliation,
            "Stanford University"
        );
    }

    #[test]
    fn parse_skips_person_without_affiliation_array() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "No Affiliation Author"}
        ]}});
        assert_eq!(parse(&record).len(), 0);
    }

    #[test]
    fn parse_accepts_string_affiliation() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Author Name", "affiliation": ["Simple Affiliation String"]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons.len(), 1);
        assert_eq!(
            persons[0].affiliations[0].affiliation,
            "Simple Affiliation String"
        );
        assert_eq!(persons[0].affiliations[0].affiliation_raw, None);
    }

    #[test]
    fn parse_extracts_existing_ror_id_verbatim() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [
                {"name": "University of Oxford",
                 "affiliationIdentifier": "https://ror.org/052gg0110",
                 "affiliationIdentifierScheme": "ROR"}
            ]}
        ]}});

        let persons = parse(&record);

        assert_eq!(
            persons[0].affiliations[0].existing_ror_id,
            Some("https://ror.org/052gg0110".to_owned())
        );
    }

    #[test]
    fn parse_leaves_existing_ror_id_none_without_identifier() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [{"name": "MIT"}]}
        ]}});
        assert_eq!(parse(&record)[0].affiliations[0].existing_ror_id, None);
    }

    #[test]
    fn parse_ignores_non_ror_identifier_scheme() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [
                {"name": "Some Org",
                 "affiliationIdentifier": "grid.123456.7",
                 "affiliationIdentifierScheme": "GRID"},
                {"name": "Other Org",
                 "affiliationIdentifier": "https://ror.org/052gg0110",
                 "affiliationIdentifierScheme": "ror"}
            ]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons[0].affiliations[0].existing_ror_id, None);
        // Scheme matching is case-insensitive.
        assert_eq!(
            persons[0].affiliations[1].existing_ror_id,
            Some("https://ror.org/052gg0110".to_owned())
        );
    }

    #[test]
    fn parse_preserves_name_identifiers_in_source_raw() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "givenName": "Jane", "familyName": "Doe",
             "nameIdentifiers": [
                {"nameIdentifier": "0000-0001-2345-6789",
                 "nameIdentifierScheme": "ORCID",
                 "schemeUri": "https://orcid.org"}
             ],
             "affiliation": [{"name": "University of Oxford"}]}
        ]}});

        let persons = parse(&record);

        let name_ids = persons[0].source_raw["nameIdentifiers"].as_array().unwrap();
        assert_eq!(name_ids.len(), 1);
        assert_eq!(name_ids[0]["nameIdentifier"], "0000-0001-2345-6789");
        assert_eq!(name_ids[0]["nameIdentifierScheme"], "ORCID");
    }

    #[test]
    fn parse_preserves_full_affiliation_object() {
        let entry = json!({"name": "University of Oxford",
            "affiliationIdentifier": "https://isni.org/isni/0000000121901201",
            "affiliationIdentifierScheme": "ISNI",
            "schemeUri": "https://isni.org"});
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": [entry]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons[0].affiliations[0].affiliation_raw, Some(entry));
        assert_eq!(persons[0].affiliations[0].existing_ror_id, None);
    }

    #[test]
    fn parse_strips_affiliation_from_source_raw() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "nameType": "Personal", "givenName": "Jane",
             "familyName": "Doe", "lang": "en",
             "nameIdentifiers": [
                {"nameIdentifier": "0000-0001-2345-6789",
                 "nameIdentifierScheme": "ORCID",
                 "schemeUri": "https://orcid.org"}
             ],
             "affiliation": [{"name": "University of Oxford"}]}
        ]}});

        let persons = parse(&record);

        let raw = &persons[0].source_raw;
        assert_eq!(raw["name"], "Doe, Jane");
        assert_eq!(raw["nameType"], "Personal");
        assert_eq!(raw["givenName"], "Jane");
        assert_eq!(raw["familyName"], "Doe");
        assert_eq!(raw["lang"], "en");
        assert!(raw["nameIdentifiers"].is_array());
        assert_eq!(raw.get("affiliation"), None);
    }

    #[test]
    fn parse_extracts_contributor_with_type() {
        let record = json!({"id": "10.1234/test", "attributes": {
        "creators": [],
        "contributors": [
            {"name": "Doe, Jane", "givenName": "Jane", "familyName": "Doe",
             "contributorType": "Supervisor",
             "affiliation": [{"name": "University of Oxford"}]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons.len(), 1);
        assert_eq!(persons[0].field, RecordField::Contributors);
        assert_eq!(persons[0].source_raw["name"], "Doe, Jane");
        assert_eq!(persons[0].source_raw["contributorType"], "Supervisor");
        assert_eq!(
            persons[0].affiliations[0].affiliation,
            "University of Oxford"
        );
        assert_eq!(persons[0].source_raw.get("affiliation"), None);
    }

    #[test]
    fn parse_orders_creators_before_contributors() {
        let record = json!({"id": "10.1234/test", "attributes": {
        "creators": [
            {"name": "Creator One", "affiliation": [{"name": "Harvard University"}]}
        ],
        "contributors": [
            {"name": "Contributor One", "contributorType": "Editor",
             "affiliation": [{"name": "Stanford University"}]}
        ]}});

        let persons = parse(&record);

        assert_eq!(persons.len(), 2);
        assert_eq!(persons[0].field, RecordField::Creators);
        assert_eq!(persons[0].idx, 0);
        assert_eq!(persons[0].source_raw["name"], "Creator One");
        assert_eq!(persons[1].field, RecordField::Contributors);
        assert_eq!(persons[1].idx, 0);
        assert_eq!(persons[1].source_raw["name"], "Contributor One");
        assert_eq!(persons[1].source_raw["contributorType"], "Editor");
    }

    #[test]
    fn parse_skips_contributor_without_affiliations() {
        let record = json!({"id": "10.1234/test", "attributes": {
        "creators": [],
        "contributors": [
            {"name": "No Affiliation Contributor", "contributorType": "Editor"}
        ]}});
        assert_eq!(parse(&record).len(), 0);
    }

    #[test]
    fn parse_drops_empty_name_and_keeps_whitespace_name() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"name": "Doe, Jane", "affiliation": ["", "  ", {"name": ""}]}
        ]}});

        let persons = parse(&record);

        // Whitespace names are still input values.
        assert_eq!(persons.len(), 1);
        assert_eq!(persons[0].affiliations.len(), 1);
        assert_eq!(persons[0].affiliations[0].affiliation, "  ");
    }

    #[test]
    fn parse_skips_person_without_name() {
        let record = json!({"id": "10.1234/test", "attributes": {"creators": [
            {"givenName": "Jane", "affiliation": [{"name": "MIT"}]}
        ]}});
        assert_eq!(parse(&record).len(), 0);
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
