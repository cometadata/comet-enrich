//! DataCite controlled-vocabulary sets, derived from the embedded enrichment-input schema
//! ([`crate::schema::SCHEMA`]) so the lists can never drift from the schema that validates
//! output records. These sets back the provenance-config validation in
//! [`crate::provenance::validate_enrichment`].

use crate::schema::SCHEMA;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::LazyLock;

/// The embedded schema, parsed once; the vocab sets read their `enum` arrays from its
/// `definitions` block.
static SCHEMA_DOC: LazyLock<Value> =
    LazyLock::new(|| serde_json::from_str(SCHEMA).expect("embedded schema is valid JSON"));

/// Collect the string values of `definitions.<name>.enum`, or `definitions.<name>.anyOf[*].enum`
/// when the enum is wrapped in an `anyOf` (as `nameType` is, to allow `null`).
///
/// Panics if the definition or its enum is missing — `SCHEMA` is a build-time constant, so a
/// miss is a programming error, not bad input.
fn vocab(name: &str) -> HashSet<String> {
    let def = &SCHEMA_DOC["definitions"][name];
    let enum_vals = def
        .get("enum")
        .or_else(|| {
            def.get("anyOf")
                .and_then(Value::as_array)
                .and_then(|variants| variants.iter().find_map(|v| v.get("enum")))
        })
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("schema definition `{name}` has no enum"));
    enum_vals
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("non-string enum value in `{name}`"))
                .to_owned()
        })
        .collect()
}

pub static RESOURCE_TYPE_GENERAL: LazyLock<HashSet<String>> =
    LazyLock::new(|| vocab("resourceTypeGeneral"));

pub static CONTRIBUTOR_TYPE: LazyLock<HashSet<String>> =
    LazyLock::new(|| vocab("contributorTypes"));

pub static RELATION_TYPE: LazyLock<HashSet<String>> = LazyLock::new(|| vocab("relationTypes"));

pub static RELATED_IDENTIFIER_TYPE: LazyLock<HashSet<String>> =
    LazyLock::new(|| vocab("relatedIdentifierTypes"));

pub static NAME_TYPE: LazyLock<HashSet<String>> = LazyLock::new(|| vocab("nameType"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vocab_sets_derive_from_schema() {
        assert!(RESOURCE_TYPE_GENERAL.contains("Dataset"));
        assert!(CONTRIBUTOR_TYPE.contains("Producer"));
        assert!(RELATION_TYPE.contains("IsDocumentedBy"));
        assert!(RELATED_IDENTIFIER_TYPE.contains("DOI"));
        // `nameType` is the wrapped (anyOf) shape; the `null` variant must not leak in.
        assert_eq!(*NAME_TYPE, HashSet::from(["Organizational".into(), "Personal".into()]));
    }
}
