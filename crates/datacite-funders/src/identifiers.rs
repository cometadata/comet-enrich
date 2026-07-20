//! Identifier scheme sniffing for funder references.

use comet_enrich_core::identifiers::{normalize_fundref, normalize_ror};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentifierScheme {
    Ror,
    Fundref,
}

/// Return the scheme and normalized value when exactly one scheme matches.
pub(crate) fn sniff_identifier(s: &str) -> Option<(IdentifierScheme, String)> {
    match (normalize_ror(s), normalize_fundref(s)) {
        (Some(canonical), None) => Some((IdentifierScheme::Ror, canonical)),
        (None, Some(canonical)) => Some((IdentifierScheme::Fundref, canonical)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NSF_ROR: &str = "https://ror.org/021nxhr62";

    #[test]
    fn sniff_identifier_picks_ror_for_ror_value() {
        assert_eq!(
            sniff_identifier("021nxhr62"),
            Some((IdentifierScheme::Ror, NSF_ROR.to_owned()))
        );
    }

    #[test]
    fn sniff_identifier_picks_fundref_for_fundref_value() {
        assert_eq!(
            sniff_identifier("10.13039/100000001"),
            Some((IdentifierScheme::Fundref, "100000001".to_owned()))
        );
    }

    #[test]
    fn sniff_identifier_returns_none_on_unknown() {
        for value in ["", "something else", "0000 0001 2345 6789"] {
            assert_eq!(sniff_identifier(value), None, "{value:?}");
        }
    }
}
