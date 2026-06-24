//! DataCite vocabulary values used by provenance validation.
//!
//! The values are read from the embedded enrichment schema, so validation uses the
//! same controlled vocabularies as output schema validation.

use crate::schema::SCHEMA;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::LazyLock;

/// Embedded schema parsed once for vocabulary lookups.
static SCHEMA_DOC: LazyLock<Value> =
    LazyLock::new(|| serde_json::from_str(SCHEMA).expect("embedded schema is valid JSON"));

/// Read the string enum values from a schema definition.
///
/// Some definitions store the enum directly. Others, such as `nameType`, wrap it
/// in `anyOf` to allow `null`.
///
/// # Panics
///
/// Panics if the schema definition is missing, has no enum, or contains a
/// non-string enum value. The schema is embedded in the crate, so this indicates
/// a programming error.
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
        assert_eq!(
            *NAME_TYPE,
            HashSet::from(["Organizational".into(), "Personal".into()])
        );
    }
}
