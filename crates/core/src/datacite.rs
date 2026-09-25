//! Shared helpers for DataCite JSON records.

use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::borrow::Cow;

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

/// The fields DOI deduplication reads from a record line without building a [`Value`].
///
/// [`Metadata::doi`] follows the same rule as [`doi`].
#[derive(Deserialize)]
pub struct Metadata<'a> {
    #[serde(borrow)]
    id: Option<MaybeStr<'a>>,
    #[serde(borrow)]
    attributes: Option<Attributes<'a>>,
}

#[derive(Deserialize)]
struct Attributes<'a> {
    #[serde(borrow)]
    doi: Option<MaybeStr<'a>>,
    #[serde(borrow)]
    updated: Option<MaybeStr<'a>>,
}

/// A string field, or any other JSON value, which is ignored like [`Value::as_str`] does.
#[derive(Deserialize)]
#[serde(untagged)]
enum MaybeStr<'a> {
    Str(#[serde(borrow)] Cow<'a, str>),
    Other(IgnoredAny),
}

impl MaybeStr<'_> {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            Self::Other(_) => None,
        }
    }

    fn non_blank(&self) -> Option<&str> {
        self.as_str().filter(|text| !text.trim().is_empty())
    }
}

impl Metadata<'_> {
    /// The record DOI, preferring top-level `id` over `attributes.doi`.
    #[must_use]
    pub fn doi(&self) -> Option<&str> {
        self.id
            .as_ref()
            .and_then(MaybeStr::non_blank)
            .or_else(|| self.attributes.as_ref()?.doi.as_ref()?.non_blank())
    }

    /// The raw `attributes.updated` string, or `None` when it is not a string.
    #[must_use]
    pub fn updated(&self) -> Option<&str> {
        self.attributes.as_ref()?.updated.as_ref()?.as_str()
    }
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

    #[test]
    fn metadata_agrees_with_doi() {
        let records = [
            json!({"id": "10.1234/id", "attributes": {"doi": "10.1234/attr"}}),
            json!({"attributes": {"doi": "10.1234/attr", "updated": "2026-01-01T00:00:00Z"}}),
            json!({"id": " ", "attributes": {"doi": "10.1234/attr"}}),
            json!({"id": 123, "attributes": {"doi": "10.1234/attr"}}),
            json!({"attributes": {"doi": "10.1234/attr", "updated": 42}}),
            json!({"id": {"nested": true}, "attributes": {"doi": null}}),
            json!(["not", "an", "object"]),
        ];
        for record in records {
            let line = record.to_string();
            let metadata = serde_json::from_str::<Metadata>(&line).ok();
            assert_eq!(
                metadata.as_ref().and_then(Metadata::doi),
                doi(&record),
                "{line}"
            );
            assert_eq!(
                metadata.as_ref().and_then(Metadata::updated),
                record
                    .pointer("/attributes/updated")
                    .and_then(Value::as_str),
                "{line}"
            );
        }
    }
}
