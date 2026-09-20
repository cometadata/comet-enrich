//! The enrichment record: the one type that owns the wire shape of every line
//! in `enrichments/*.jsonl.gz`.
//!
//! Property names and order come from the struct declaration. The JSON Schema
//! in `configs/enrichment_input_schema.json` is the external contract, and the
//! tests here check that the two agree. Methods produce [`EnrichmentParts`];
//! [`EnrichmentRecord::new`] adds the run-level `sourceId` and the enrichment
//! content key.

use crate::content_key::{canonical_bytes, enrichment_content_key};
use crate::method::{EnrichmentAction, EnrichmentParts};
use crate::template::EnrichmentTemplate;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use xxhash_rust::xxh3::xxh3_128;

/// What a diff release says changed for a content key relative to the
/// previous release. The strings are pinned by the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffEvent {
    /// Present now, absent before: apply.
    Asserted,
    /// Same content key, different `enrichedValue`: replace in place.
    Superseded,
    /// Present before, absent now: remove.
    Retracted,
}

impl DiffEvent {
    /// The schema value for this event.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DiffEvent::Asserted => "asserted",
            DiffEvent::Superseded => "superseded",
            DiffEvent::Retracted => "retracted",
        }
    }
}

/// One enrichment record, as written to and read from a release.
///
/// `original_value` and `enriched_value` are `Null` when the schema leaves
/// them out (inserts have no original, deletions no enriched value) and are
/// then omitted from the JSON. `event` is present only in diff releases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnrichmentRecord {
    /// Target DOI of the enrichment.
    pub doi: String,
    /// Action to apply to `field`.
    pub action: EnrichmentAction,
    /// Top-level DataCite field this record enriches.
    pub field: String,
    /// Original value of the field or child, `Null` for inserts.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub original_value: Value,
    /// Enriched value of the field or child, `Null` for deletions.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub enriched_value: Value,
    /// DOI name of the enrichment project that produced the record.
    pub source_id: String,
    /// Enrichment content key: 32 lowercase hex chars, see
    /// [`enrichment_content_key`].
    pub content_key: String,
    /// What changed for this content key; diff releases only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<DiffEvent>,
}

impl EnrichmentRecord {
    /// Build a record from a method's value parts plus the run-level values.
    ///
    /// `method_name` is hashed into the content key, so it must be the stable
    /// name from [`crate::method::EnrichmentMethod::name`].
    #[must_use]
    pub fn new(template: &EnrichmentTemplate, method_name: &str, parts: EnrichmentParts) -> Self {
        let content_key = enrichment_content_key(
            method_name,
            &parts.doi,
            parts.field,
            parts.action,
            &parts.original,
            &parts.enriched,
        );
        Self {
            doi: parts.doi,
            action: parts.action,
            field: parts.field.to_owned(),
            original_value: parts.original,
            enriched_value: parts.enriched,
            source_id: template.source_id().to_owned(),
            content_key,
            event: None,
        }
    }

    /// Parse a record read back from a release.
    ///
    /// # Errors
    ///
    /// Returns an error naming the DOI if the record has no `contentKey`, its
    /// content key is not 32 lowercase hex chars, or its shape does not match
    /// the schema (missing or unknown properties, wrong types).
    pub fn from_value(rec: Value) -> Result<Self> {
        let doi = rec
            .get("doi")
            .and_then(Value::as_str)
            .unwrap_or("<no doi>")
            .to_owned();
        if rec.get("contentKey").is_none() {
            bail!("record has no contentKey (doi `{doi}`)");
        }
        let record: Self = serde_json::from_value(rec)
            .with_context(|| format!("malformed enrichment record (doi `{doi}`)"))?;
        record.parse_content_key()?;
        Ok(record)
    }

    /// The record as a JSON value, for schema validation and writing.
    ///
    /// # Errors
    ///
    /// Returns an error only if a value field holds something `serde_json`
    /// cannot represent, which the `Value` type rules out in practice.
    pub fn to_value(&self) -> Result<Value> {
        serde_json::to_value(self).context("serializing enrichment record")
    }

    /// The content key parsed from its 32 lowercase hex chars.
    ///
    /// # Errors
    ///
    /// Returns an error naming the DOI if the key is not 32 lowercase hex chars.
    pub fn parse_content_key(&self) -> Result<u128> {
        let key = &self.content_key;
        let is_lower_hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
        if key.len() != 32 || !key.chars().all(is_lower_hex) {
            bail!("malformed contentKey `{key}` (doi `{}`)", self.doi);
        }
        u128::from_str_radix(key, 16)
            .with_context(|| format!("malformed contentKey `{key}` (doi `{}`)", self.doi))
    }

    /// Hash of the canonical `enrichedValue` (`Null` when absent).
    #[must_use]
    pub fn enriched_value_hash(&self) -> u128 {
        xxh3_128(&canonical_bytes(&self.enriched_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SCHEMA, compile_str};
    use serde_json::json;

    const SOURCE_ID: &str = "10.82461/bpzr-jd55";
    const METHOD: &str = "test-method";

    fn template() -> EnrichmentTemplate {
        EnrichmentTemplate::new(SOURCE_ID).unwrap()
    }

    fn parts(action: EnrichmentAction, original: Value, enriched: Value) -> EnrichmentParts {
        EnrichmentParts {
            doi: "10.5281/x".to_owned(),
            action,
            field: "types",
            original,
            enriched,
        }
    }

    fn update_record() -> EnrichmentRecord {
        EnrichmentRecord::new(
            &template(),
            METHOD,
            parts(EnrichmentAction::Update, json!({"a": 1}), json!({"a": 2})),
        )
    }

    fn validator() -> jsonschema::Validator {
        compile_str(SCHEMA).unwrap()
    }

    /// The wire format is frozen: property names, order, and the content key
    /// for this input.
    #[test]
    fn update_record_serializes_in_declared_order() {
        let s = serde_json::to_string(&update_record()).unwrap();
        assert_eq!(
            s,
            concat!(
                r#"{"doi":"10.5281/x","action":"update","field":"types","#,
                r#""originalValue":{"a":1},"enrichedValue":{"a":2},"#,
                r#""sourceId":"10.82461/bpzr-jd55","#,
                r#""contentKey":"76eb771822e972c0b5f8b5c6a21630f0"}"#
            )
        );
    }

    #[test]
    fn new_computes_the_content_key() {
        let original = json!({"resourceTypeGeneral": "Text"});
        let enriched = json!({"resourceTypeGeneral": "Dataset"});
        let rec = EnrichmentRecord::new(
            &template(),
            METHOD,
            parts(EnrichmentAction::Update, original.clone(), enriched.clone()),
        );
        let want = enrichment_content_key(
            METHOD,
            "10.5281/x",
            "types",
            EnrichmentAction::Update,
            &original,
            &enriched,
        );
        assert_eq!(rec.content_key, want);
        assert_eq!(rec.source_id, SOURCE_ID);
        assert_eq!(rec.event, None);
    }

    #[test]
    fn insert_record_omits_original_value_and_validates() {
        let rec = EnrichmentRecord::new(
            &template(),
            METHOD,
            parts(EnrichmentAction::Insert, Value::Null, json!({"a": 2})),
        );
        let value = rec.to_value().unwrap();
        assert!(value.get("originalValue").is_none(), "{value}");
        assert_eq!(value["enrichedValue"], json!({"a": 2}));
        assert!(validator().is_valid(&value));
        let back = EnrichmentRecord::from_value(value.clone()).unwrap();
        assert_eq!(back, rec);
        assert_eq!(back.to_value().unwrap(), value);
    }

    #[test]
    fn delete_record_omits_enriched_value_and_validates() {
        let rec = EnrichmentRecord::new(
            &template(),
            METHOD,
            parts(EnrichmentAction::DeleteChild, json!({"a": 1}), Value::Null),
        );
        let value = rec.to_value().unwrap();
        assert!(value.get("enrichedValue").is_none(), "{value}");
        assert!(validator().is_valid(&value));
        let back = EnrichmentRecord::from_value(value.clone()).unwrap();
        assert_eq!(back, rec);
        assert_eq!(back.to_value().unwrap(), value);
    }

    #[test]
    fn update_record_validates_against_the_schema() {
        let value = update_record().to_value().unwrap();
        assert!(validator().is_valid(&value));
        let back = EnrichmentRecord::from_value(value.clone()).unwrap();
        assert_eq!(back.to_value().unwrap(), value);
    }

    #[test]
    fn event_is_absent_by_default_and_serialized_last_when_set() {
        let mut rec = update_record();
        assert!(!serde_json::to_string(&rec).unwrap().contains("event"));

        rec.event = Some(DiffEvent::Superseded);
        let s = serde_json::to_string(&rec).unwrap();
        assert!(s.ends_with(r#","event":"superseded"}"#), "{s}");
        let value = rec.to_value().unwrap();
        assert!(validator().is_valid(&value));
        assert_eq!(EnrichmentRecord::from_value(value).unwrap(), rec);
    }

    #[test]
    fn diff_event_serde_agrees_with_as_str() {
        for event in [
            DiffEvent::Asserted,
            DiffEvent::Superseded,
            DiffEvent::Retracted,
        ] {
            let json = serde_json::to_value(event).unwrap();
            assert_eq!(json, json!(event.as_str()));
            let back: DiffEvent = serde_json::from_value(json).unwrap();
            assert_eq!(back, event);
        }
    }

    #[test]
    fn parse_content_key_returns_the_hex_value() {
        let mut rec = update_record();
        rec.content_key = "0860ed77af682e5bbe343af4f5e0347c".to_owned();
        assert_eq!(
            rec.parse_content_key().unwrap(),
            0x0860_ed77_af68_2e5b_be34_3af4_f5e0_347c
        );
    }

    #[test]
    fn parse_content_key_rejects_wrong_length_and_uppercase() {
        let mut rec = update_record();
        for bad in ["nope", "0860ED77AF682E5BBE343AF4F5E0347C", ""] {
            rec.content_key = bad.to_owned();
            let err = rec.parse_content_key().unwrap_err().to_string();
            assert!(
                err.contains("malformed contentKey") && err.contains("10.5281/x"),
                "{bad:?}: {err}"
            );
        }
    }

    #[test]
    fn from_value_rejects_a_record_without_content_key() {
        let mut value = update_record().to_value().unwrap();
        value.as_object_mut().unwrap().remove("contentKey");
        let err = format!("{:#}", EnrichmentRecord::from_value(value).unwrap_err());
        assert!(
            err.contains("no contentKey") && err.contains("10.5281/x"),
            "{err}"
        );
    }

    #[test]
    fn from_value_rejects_a_malformed_content_key() {
        let mut value = update_record().to_value().unwrap();
        value["contentKey"] = json!("nope");
        let err = format!("{:#}", EnrichmentRecord::from_value(value).unwrap_err());
        assert!(err.contains("malformed contentKey"), "{err}");
    }

    #[test]
    fn from_value_rejects_unknown_properties_and_bad_types() {
        let mut extra = update_record().to_value().unwrap();
        extra["key"] = json!("0860ed77af682e5bbe343af4f5e0347c");
        let err = format!("{:#}", EnrichmentRecord::from_value(extra).unwrap_err());
        assert!(err.contains("10.5281/x") && err.contains("key"), "{err}");

        let mut wrong = update_record().to_value().unwrap();
        wrong["action"] = json!("explode");
        assert!(EnrichmentRecord::from_value(wrong).is_err());
    }

    #[test]
    fn enriched_value_hash_is_canonical_and_null_when_absent() {
        let mut a = update_record();
        a.enriched_value = json!({"b": 1, "a": {"d": 2, "c": 3}});
        let mut b = update_record();
        b.enriched_value = json!({"a": {"c": 3, "d": 2}, "b": 1});
        assert_eq!(a.enriched_value_hash(), b.enriched_value_hash());

        let mut absent = update_record();
        absent.enriched_value = Value::Null;
        assert_eq!(
            absent.enriched_value_hash(),
            xxh3_128(&canonical_bytes(&Value::Null))
        );
    }
}
