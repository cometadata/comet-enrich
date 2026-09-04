//! Identity keys and duplicate detection for enrichment records.
//!
//! See `docs/architecture.md`, sections "Identity keys" and "Duplicate enrichments".

use crate::method::EnrichmentAction;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use xxhash_rust::xxh3::xxh3_128;

/// RFC 8785 (JCS) canonical bytes of a JSON value.
///
/// Numbers use IEEE-754 doubles, so integers beyond 2^53 can lose precision.
/// Object keys sort by UTF-16 code units.
///
/// Accepts borrowed serializable inputs without building an intermediate `Value`.
#[must_use]
pub(crate) fn canonical_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    // The only failure modes are NaN/Infinity, which serde_json never
    // produces, and non-string object keys, which callers never pass.
    serde_json_canonicalizer::to_vec(value).expect("JCS serialization of a JSON value")
}

/// Identity key for one enrichment record: 32 lowercase hexadecimal characters.
#[must_use]
pub fn enrichment_key(
    method: &str,
    doi: &str,
    field: &str,
    action: EnrichmentAction,
    original: &Value,
    enriched: &Value,
) -> String {
    let value = if action == EnrichmentAction::Insert {
        enriched
    } else {
        original
    };
    // A tuple serializes as a JSON array, so the canonical bytes are those of
    // `[method, doi, field, action, value]` without copying `value`.
    let input = (method, doi, field, action.as_str(), value);
    format!("{:032x}", xxh3_128(&canonical_bytes(&input)))
}

/// The record's embedded `key`, parsed from its 32 hex chars.
///
/// # Errors
///
/// Returns an error if the key is missing or is not 32 hex chars.
pub(crate) fn embedded_key(rec: &Value) -> Result<u128> {
    let doi = rec.get("doi").and_then(Value::as_str).unwrap_or("<no doi>");
    let key = rec
        .get("key")
        .with_context(|| format!("record has no key (doi `{doi}`)"))?;
    let hex = key
        .as_str()
        .filter(|k| k.len() == 32)
        .with_context(|| format!("malformed key {key} (doi `{doi}`)"))?;
    u128::from_str_radix(hex, 16).with_context(|| format!("malformed key `{hex}` (doi `{doi}`)"))
}

/// Hash of the record's canonical `enrichedValue` (Null when absent).
#[must_use]
pub(crate) fn enriched_value_hash(rec: &Value) -> u128 {
    let ev = rec.get("enrichedValue").unwrap_or(&Value::Null);
    xxh3_128(&canonical_bytes(ev))
}

/// Tracks enrichment keys and values accepted for the current DOI.
///
/// DOI deduplication leaves one source record per DOI. Each worker processes
/// that record's enrichments consecutively, including all its extraction rows
/// in the staged pipeline. This lets the window reset when the DOI changes.
#[derive(Debug, Default)]
pub(crate) struct KeyWindow {
    doi: String,
    /// `(key, canonical enrichedValue bytes)` for each record admitted under `doi`.
    seen: Vec<(String, Vec<u8>)>,
}

impl KeyWindow {
    /// Returns true for a new key and false for a duplicate with the same
    /// canonical `enrichedValue`.
    ///
    /// # Errors
    ///
    /// Returns an error if the same key has a different `enrichedValue`
    /// within the current DOI.
    pub(crate) fn admit(&mut self, rec: &Value) -> Result<bool> {
        let doi = rec.get("doi").and_then(Value::as_str).unwrap_or_default();
        let key = rec.get("key").and_then(Value::as_str).unwrap_or_default();
        let enriched = canonical_bytes(rec.get("enrichedValue").unwrap_or(&Value::Null));
        if doi != self.doi {
            doi.clone_into(&mut self.doi);
            self.seen.clear();
        }
        if let Some((_, previous)) = self.seen.iter().find(|(seen, _)| seen == key) {
            if *previous == enriched {
                return Ok(false);
            }
            bail!("key {key} (doi `{doi}`) has two different enrichedValues in one source record");
        }
        self.seen.push((key.to_owned(), enriched));
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::EnrichmentAction;
    use serde_json::json;

    #[test]
    fn canonical_bytes_sorts_object_keys_recursively() {
        let v = json!({"b": {"d": 2, "c": 3}, "a": 1});
        assert_eq!(canonical_bytes(&v), br#"{"a":1,"b":{"c":3,"d":2}}"#);
    }

    #[test]
    fn canonical_bytes_uses_ecmascript_number_form() {
        // RFC 8785 serializes numbers like ECMAScript: 1.0 becomes 1.
        let v = json!([1.0, 10, 0.5]);
        assert_eq!(canonical_bytes(&v), b"[1,10,0.5]");
    }

    /// RFC 8785 Appendix B: IEEE-754 doubles and their canonical text.
    #[test]
    fn canonical_bytes_matches_rfc8785_appendix_b_numbers() {
        let cases: &[(u64, &str)] = &[
            (0x0000_0000_0000_0000, "0"),
            (0x8000_0000_0000_0000, "0"),
            (0x0000_0000_0000_0001, "5e-324"),
            (0x8000_0000_0000_0001, "-5e-324"),
            (0x7fef_ffff_ffff_ffff, "1.7976931348623157e+308"),
            (0xffef_ffff_ffff_ffff, "-1.7976931348623157e+308"),
            (0x4340_0000_0000_0000, "9007199254740992"),
            (0xc340_0000_0000_0000, "-9007199254740992"),
            (0x4430_0000_0000_0000, "295147905179352830000"),
            (0x44b5_2d02_c7e1_4af6, "1e+23"),
            (0x444b_1ae4_d6e2_ef50, "1e+21"),
            (0x3eb0_c6f7_a0b5_ed8d, "0.000001"),
            (0x41b3_de43_5555_5555, "333333333.3333333"),
            (0xbecb_f647_612f_3696, "-0.0000033333333333333333"),
            (0x4314_3ff3_c1cb_0959, "1424953923781206.2"),
        ];
        for (bits, expected) in cases {
            let v = json!(f64::from_bits(*bits));
            assert_eq!(
                String::from_utf8(canonical_bytes(&v)).unwrap(),
                *expected,
                "bits {bits:#x}"
            );
        }
    }

    /// Integers beyond 2^53 are doubles in JCS, so an integer literal and the
    /// equal float literal must canonicalize to the same bytes.
    #[test]
    fn canonical_bytes_treats_large_integers_as_doubles() {
        let as_int: serde_json::Value = serde_json::from_str("1000000000000000128").unwrap();
        let as_float: serde_json::Value = serde_json::from_str("1.000000000000000128e18").unwrap();

        assert_eq!(canonical_bytes(&as_int), canonical_bytes(&as_float));
        assert_eq!(canonical_bytes(&as_int), b"1000000000000000100");
    }

    /// RFC 8785 sorts keys by UTF-16 code units: U+10000 (a surrogate pair
    /// starting 0xD800) sorts before U+E000, the opposite of code-point order.
    #[test]
    fn canonical_bytes_sorts_keys_by_utf16_code_units() {
        let v = json!({"\u{e000}": 2, "\u{10000}": 1});
        assert_eq!(
            String::from_utf8(canonical_bytes(&v)).unwrap(),
            "{\"\u{10000}\":1,\"\u{e000}\":2}"
        );
    }

    /// Existing inputs must continue to produce the same keys.
    #[test]
    fn enrichment_key_golden_value_is_frozen() {
        let key = enrichment_key(
            "funders",
            "10.5281/zenodo.123",
            "fundingReferences",
            EnrichmentAction::Update,
            &json!({"funderName": "NSF", "awardNumber": 1_000_000_000_000_000_128_u64, "z": "\u{e9}"}),
            &json!({"funderName": "National Science Foundation"}),
        );
        assert_eq!(key, "810af53b93c1ecc4faf19742aef0040f");
    }

    #[test]
    fn canonical_bytes_keeps_unicode_literal() {
        let v = json!({"name": "Universit\u{e9}"});
        assert_eq!(
            String::from_utf8(canonical_bytes(&v)).unwrap(),
            "{\"name\":\"Universit\u{e9}\"}"
        );
    }

    fn stamped(doi: &str, key: &str, enriched: &Value) -> Value {
        json!({"doi": doi, "key": key, "enrichedValue": enriched})
    }

    #[test]
    fn key_window_drops_identical_repeat_within_one_doi() {
        let mut window = KeyWindow::default();
        let rec = stamped("10.1/a", "k1", &json!({"name": "NSF"}));
        assert!(window.admit(&rec).unwrap());
        assert!(!window.admit(&rec).unwrap());
        assert!(
            window
                .admit(&stamped("10.1/a", "k2", &json!({"name": "NSF"})))
                .unwrap()
        );
    }

    #[test]
    fn key_window_treats_jcs_equal_values_as_duplicates() {
        let mut window = KeyWindow::default();
        assert!(
            window
                .admit(&stamped("10.1/a", "k1", &json!({"n": 1})))
                .unwrap()
        );
        assert!(
            !window
                .admit(&stamped("10.1/a", "k1", &json!({"n": 1.0})))
                .unwrap()
        );
    }

    #[test]
    fn key_window_fails_on_same_key_with_different_enriched_value() {
        let mut window = KeyWindow::default();
        window
            .admit(&stamped("10.1/a", "k1", &json!({"name": "NSF"})))
            .unwrap();
        let err = window
            .admit(&stamped("10.1/a", "k1", &json!({"name": "NIH"})))
            .unwrap_err()
            .to_string();
        assert!(err.contains("k1") && err.contains("10.1/a"), "got: {err}");
    }

    fn key(action: EnrichmentAction, original: &serde_json::Value) -> String {
        enrichment_key(
            "funders",
            "10.5281/zenodo.123",
            "fundingReferences",
            action,
            original,
            &json!({"funderName": "NSF"}),
        )
    }

    #[test]
    fn key_is_32_lowercase_hex_chars() {
        let k = key(EnrichmentAction::UpdateChild, &json!({"funderName": "nsf"}));
        assert_eq!(k.len(), 32);
        assert!(
            k.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn key_is_deterministic_and_ignores_original_key_order() {
        let a = key(
            EnrichmentAction::UpdateChild,
            &json!({"funderName": "nsf", "awardNumber": "1"}),
        );
        let b = key(
            EnrichmentAction::UpdateChild,
            &json!({"awardNumber": "1", "funderName": "nsf"}),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn key_changes_with_each_input_component() {
        let base = key(EnrichmentAction::UpdateChild, &json!({"funderName": "nsf"}));
        let other_original = key(EnrichmentAction::UpdateChild, &json!({"funderName": "nih"}));
        let other_action = key(EnrichmentAction::DeleteChild, &json!({"funderName": "nsf"}));
        let other_method = enrichment_key(
            "affiliations",
            "10.5281/zenodo.123",
            "fundingReferences",
            EnrichmentAction::UpdateChild,
            &json!({"funderName": "nsf"}),
            &json!({"funderName": "NSF"}),
        );
        let other_doi = enrichment_key(
            "funders",
            "10.5281/zenodo.999",
            "fundingReferences",
            EnrichmentAction::UpdateChild,
            &json!({"funderName": "nsf"}),
            &json!({"funderName": "NSF"}),
        );
        for other in [&other_original, &other_action, &other_method, &other_doi] {
            assert_ne!(&base, other);
        }
    }

    #[test]
    fn update_key_ignores_the_enriched_value() {
        let a = enrichment_key(
            "resource-type-general",
            "10.1/x",
            "types",
            EnrichmentAction::Update,
            &json!({"resourceTypeGeneral": "Text"}),
            &json!({"resourceTypeGeneral": "Dataset"}),
        );
        let b = enrichment_key(
            "resource-type-general",
            "10.1/x",
            "types",
            EnrichmentAction::Update,
            &json!({"resourceTypeGeneral": "Text"}),
            &json!({"resourceTypeGeneral": "Software"}),
        );
        assert_eq!(a, b);
    }

    /// Prints the golden vector table. Run with
    /// `cargo test -p comet-enrich-core print_golden_vectors -- --ignored --nocapture`
    /// to generate expected keys for new cases. Append them to
    /// `golden_vectors_are_frozen` without replacing existing expected keys.
    #[test]
    #[ignore = "generator for the frozen vectors"]
    fn print_golden_vectors() {
        for (i, (method, doi, field, action, original, enriched)) in
            golden_inputs().iter().enumerate()
        {
            let k = enrichment_key(method, doi, field, *action, original, enriched);
            println!("vector {i}: \"{k}\"");
        }
    }

    #[allow(clippy::type_complexity)]
    fn golden_inputs() -> Vec<(
        &'static str,
        &'static str,
        &'static str,
        EnrichmentAction,
        serde_json::Value,
        serde_json::Value,
    )> {
        vec![
            (
                "resource-type-general",
                "10.5281/zenodo.123",
                "types",
                EnrichmentAction::Update,
                json!({"resourceTypeGeneral": "Text", "resourceType": "Journal article"}),
                json!({"resourceTypeGeneral": "JournalArticle", "resourceType": "Journal article"}),
            ),
            (
                "funders",
                "10.5281/ZENODO.123",
                "fundingReferences",
                EnrichmentAction::UpdateChild,
                json!({"funderName": "National Science Foundation"}),
                json!({"funderName": "National Science Foundation", "funderIdentifier": "https://ror.org/021nxhr62", "funderIdentifierType": "ROR"}),
            ),
            (
                "affiliations",
                "10.1/x",
                "creators",
                EnrichmentAction::UpdateChild,
                json!({"name": "Doe, J.", "affiliation": [{"name": "Universit\u{e9} de Montr\u{e9}al"}]}),
                json!({"name": "Doe, J.", "affiliation": [{"name": "Universit\u{e9} de Montr\u{e9}al", "affiliationIdentifier": "https://ror.org/0161xgx34", "affiliationIdentifierScheme": "ROR"}]}),
            ),
            (
                "funders",
                "10.1/x",
                "fundingReferences",
                EnrichmentAction::Insert,
                serde_json::Value::Null,
                json!({"funderName": "NSF", "awardNumber": 7, "fraction": 0.5}),
            ),
            (
                "funders",
                "10.1/x",
                "fundingReferences",
                EnrichmentAction::DeleteChild,
                json!({"funderName": "Duplicate Funder"}),
                serde_json::Value::Null,
            ),
        ]
    }

    /// Preserve existing expected keys for compatibility. Add new vectors
    /// without replacing old expectations.
    #[test]
    fn golden_vectors_are_frozen() {
        let expected = [
            "0860ed77af682e5bbe343af4f5e0347c",
            "20324e63c3468e33b6654086c4f523e3",
            "4e9de3b2398155778a869300beb89c05",
            "cc54ce6712ea0fffeb1bdfb36c2b67d6",
            "5103673506a69e8d227d810a673bb670",
        ];
        for ((method, doi, field, action, original, enriched), want) in
            golden_inputs().iter().zip(expected)
        {
            assert_eq!(
                enrichment_key(method, doi, field, *action, original, enriched),
                want
            );
        }
    }

    #[test]
    fn insert_key_uses_the_enriched_value() {
        let a = enrichment_key(
            "funders",
            "10.1/x",
            "fundingReferences",
            EnrichmentAction::Insert,
            &serde_json::Value::Null,
            &json!({"funderName": "NSF"}),
        );
        let b = enrichment_key(
            "funders",
            "10.1/x",
            "fundingReferences",
            EnrichmentAction::Insert,
            &serde_json::Value::Null,
            &json!({"funderName": "NIH"}),
        );
        assert_ne!(a, b);
    }

    fn keyed_record() -> Value {
        json!({
            "doi": "10.5281/zenodo.123",
            "action": "updateChild",
            "field": "fundingReferences",
            "originalValue": {"funderName": "nsf"},
            "enrichedValue": {"funderName": "NSF"},
        })
    }

    #[test]
    fn embedded_key_reads_the_key_field() {
        let mut rec = keyed_record();
        rec["key"] = json!("0860ed77af682e5bbe343af4f5e0347c");
        let k = embedded_key(&rec).unwrap();
        assert_eq!(k, 0x0860_ed77_af68_2e5b_be34_3af4_f5e0_347c);
    }

    #[test]
    fn embedded_key_rejects_missing_or_malformed_keys() {
        let err = format!("{:#}", embedded_key(&keyed_record()).unwrap_err());
        assert!(
            err.contains("no key") && err.contains("10.5281/zenodo.123"),
            "{err}"
        );
        let mut rec = keyed_record();
        rec["key"] = json!("nope");
        assert!(embedded_key(&rec).is_err());
    }

    #[test]
    fn enriched_value_hash_is_canonical_and_null_when_absent() {
        let a = json!({"enrichedValue": {"b": 1, "a": {"d": 2, "c": 3}}});
        let b = json!({"enrichedValue": {"a": {"c": 3, "d": 2}, "b": 1}});
        assert_eq!(enriched_value_hash(&a), enriched_value_hash(&b));
        assert_eq!(
            enriched_value_hash(&json!({"doi": "10.1/a"})),
            enriched_value_hash(&json!({"enrichedValue": null}))
        );
    }
}
