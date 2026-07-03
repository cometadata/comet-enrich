//! Shared helpers for DataCite JSON records.

use serde_json::Value;

/// Return the record DOI, preferring top-level `id` over `attributes.doi`.
///
/// Empty and whitespace-only strings are treated as absent.
#[must_use]
pub fn doi(record: &Value) -> Option<&str> {
    non_blank_str(record.get("id")).or_else(|| non_blank_str(record.pointer("/attributes/doi")))
}

fn non_blank_str(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn doi_prefers_top_level_id() {
        assert_eq!(
            doi(&json!({"id": "10.1234/id", "attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/id")
        );
    }

    #[test]
    fn doi_falls_back_to_attributes_doi() {
        assert_eq!(
            doi(&json!({"attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/attr")
        );
    }

    #[test]
    fn doi_ignores_blank_candidates() {
        assert_eq!(
            doi(&json!({"id": "", "attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/attr")
        );
        assert_eq!(
            doi(&json!({"id": "   ", "attributes": {"doi": "10.1234/attr"}})),
            Some("10.1234/attr")
        );
        assert_eq!(doi(&json!({"id": "", "attributes": {"doi": " "}})), None);
    }

    #[test]
    fn doi_returns_none_without_usable_doi() {
        assert_eq!(doi(&json!({"attributes": {}})), None);
        assert_eq!(doi(&json!({"id": 123, "attributes": {"doi": null}})), None);
    }
}
