//! Normalize ROR and Crossref Funder IDs.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentifierScheme {
    Ror,
    Fundref,
}

/// Normalize accepted ROR forms to `https://ror.org/<id>`.
pub(crate) fn normalize_ror(s: &str) -> Option<String> {
    let lower = s.trim().trim_end_matches('/').to_ascii_lowercase();

    let rest = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))
        .unwrap_or(&lower);
    let rest = rest.strip_prefix("www.").unwrap_or(rest);
    let rest = rest.strip_prefix("ror.org/").unwrap_or(rest);

    is_valid_ror_id(rest).then(|| format!("https://ror.org/{rest}"))
}

/// Normalize accepted Crossref Funder ID forms to bare digits.
pub(crate) fn normalize_fundref(s: &str) -> Option<String> {
    let trimmed = s.trim().trim_end_matches('/');

    let digits = match trimmed.find("10.13039/") {
        Some(pos) => &trimmed[pos + "10.13039/".len()..],
        None => trimmed,
    };

    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.to_owned())
}

/// Return the scheme and normalized value when exactly one scheme matches.
pub(crate) fn sniff_identifier(s: &str) -> Option<(IdentifierScheme, String)> {
    match (normalize_ror(s), normalize_fundref(s)) {
        (Some(canonical), None) => Some((IdentifierScheme::Ror, canonical)),
        (None, Some(canonical)) => Some((IdentifierScheme::Fundref, canonical)),
        _ => None,
    }
}

fn is_valid_ror_id(s: &str) -> bool {
    s.len() == 9
        && s.starts_with('0')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NSF_ROR: &str = "https://ror.org/021nxhr62";

    #[test]
    fn normalize_ror_accepts_bare_id() {
        assert_eq!(normalize_ror("021nxhr62"), Some(NSF_ROR.to_owned()));
    }

    #[test]
    fn normalize_ror_accepts_hosted_forms() {
        for form in [
            "ror.org/021nxhr62",
            "www.ror.org/021nxhr62",
            "https://ror.org/021nxhr62",
            "http://ror.org/021nxhr62",
        ] {
            assert_eq!(normalize_ror(form), Some(NSF_ROR.to_owned()), "{form}");
        }
    }

    #[test]
    fn normalize_ror_lowercases_and_strips_trailing_slash() {
        assert_eq!(
            normalize_ror("https://ROR.org/021NXHR62/"),
            Some(NSF_ROR.to_owned())
        );
        assert_eq!(normalize_ror("021NXHR62"), Some(NSF_ROR.to_owned()));
    }

    #[test]
    fn normalize_ror_trims_whitespace() {
        assert_eq!(normalize_ror("  021nxhr62  "), Some(NSF_ROR.to_owned()));
    }

    #[test]
    fn normalize_ror_rejects_non_ror_values() {
        for value in [
            "",
            "National Science Foundation",
            "10.13039/100000001",
            "12345",
            "12345678",
            "1234567890",
            "a21nxhr62",
            "021nxhr6!",
        ] {
            assert_eq!(normalize_ror(value), None, "{value:?}");
        }
    }

    #[test]
    fn normalize_fundref_accepts_bare_digits() {
        assert_eq!(normalize_fundref("100000001"), Some("100000001".to_owned()));
        assert_eq!(
            normalize_fundref("501100001780"),
            Some("501100001780".to_owned())
        );
    }

    #[test]
    fn normalize_fundref_accepts_crossref_prefix() {
        assert_eq!(
            normalize_fundref("10.13039/100000001"),
            Some("100000001".to_owned())
        );
    }

    #[test]
    fn normalize_fundref_accepts_doi_urls() {
        for form in [
            "doi.org/10.13039/100000001",
            "https://doi.org/10.13039/100000001",
            "http://dx.doi.org/10.13039/100000001",
        ] {
            assert_eq!(
                normalize_fundref(form),
                Some("100000001".to_owned()),
                "{form}"
            );
        }
    }

    #[test]
    fn normalize_fundref_trims_whitespace_and_trailing_slash() {
        assert_eq!(
            normalize_fundref("  10.13039/100000001/  "),
            Some("100000001".to_owned())
        );
    }

    #[test]
    fn normalize_fundref_rejects_non_fundref_values() {
        for value in [
            "",
            "021nxhr62",
            "https://ror.org/021nxhr62",
            "National Science Foundation",
            "10.1234/something",
            "abc",
        ] {
            assert_eq!(normalize_fundref(value), None, "{value:?}");
        }
    }

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
