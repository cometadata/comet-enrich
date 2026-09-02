//! Normalize and validate ROR, Crossref Funder, and DOI identifiers.

use regex::Regex;
use std::sync::LazyLock;

/// Scheme URI for ROR identifiers in DataCite records.
pub const ROR_SCHEME_URI: &str = "https://ror.org";

/// Normalize accepted ROR forms to `https://ror.org/<id>`.
#[must_use]
pub fn normalize_ror(s: &str) -> Option<String> {
    let lower = s.trim().trim_end_matches('/').to_ascii_lowercase();

    let rest = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))
        .unwrap_or(&lower);
    let rest = rest.strip_prefix("www.").unwrap_or(rest);
    let rest = rest.strip_prefix("ror.org/").unwrap_or(rest);

    is_valid_ror_id(rest).then(|| format!("{ROR_SCHEME_URI}/{rest}"))
}

/// Normalize accepted Crossref Funder ID forms to bare digits.
#[must_use]
pub fn normalize_fundref(s: &str) -> Option<String> {
    let trimmed = s.trim().trim_end_matches('/');

    let digits = match trimmed.find("10.13039/") {
        Some(pos) => &trimmed[pos + "10.13039/".len()..],
        None => trimmed,
    };

    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.to_owned())
}

static DOI_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\A10\.[0-9]+(?:\.[0-9]+)*/.+\z").expect("DOI regex compiles"));

/// Check whether a value is a valid DOI name, such as `10.1234/example`.
///
/// Surrounding whitespace and control characters anywhere in the value are
/// rejected. The suffix is otherwise opaque: internal spaces and non-ASCII
/// characters are accepted.
#[must_use]
pub fn is_valid_doi_name(s: &str) -> bool {
    s.trim() == s && !s.chars().any(char::is_control) && DOI_RE.is_match(s)
}

/// Validate a bare ROR ID: `0` + 6 Crockford base32 chars + a 2-digit
/// ISO/IEC 7064 MOD 97-10 checksum, so single-character corruptions are
/// always rejected. The encoding and checksum are described at
/// <https://ror.readme.io/docs/identifier>.
#[must_use]
pub fn is_valid_ror_id(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 9 || b[0] != b'0' || !b[7].is_ascii_digit() || !b[8].is_ascii_digit() {
        return false;
    }
    let mut n: u64 = 0;
    for &c in &b[1..7] {
        let Some(v) = crockford_value(c) else {
            return false;
        };
        n = n * 32 + v;
    }
    let check = u64::from(b[7] - b'0') * 10 + u64::from(b[8] - b'0');
    // Recompute rather than test `(n*100 + check) % 97 == 1`: the congruence
    // also holds for check values 97 apart (01 vs 98, 99 vs 02).
    check == 98 - (n * 100) % 97
}

/// Crockford base32 digit value; `None` for `i`, `l`, `o`, `u` and anything
/// else outside the alphabet.
fn crockford_value(c: u8) -> Option<u64> {
    match c {
        b'0'..=b'9' => Some(u64::from(c - b'0')),
        b'a'..=b'h' => Some(u64::from(c - b'a') + 10),
        b'j' | b'k' => Some(u64::from(c - b'j') + 18),
        b'm' | b'n' => Some(u64::from(c - b'm') + 20),
        b'p'..=b't' => Some(u64::from(c - b'p') + 22),
        b'v'..=b'z' => Some(u64::from(c - b'v') + 27),
        _ => None,
    }
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
            // Letter where the 2-digit checksum belongs.
            "021nxhr6x",
            // i, l, o, u are not in the Crockford base32 alphabet.
            "0iaaaaa42",
            "0laaaaa42",
            "0oaaaaa42",
            "0uaaaaa42",
            "0||||||42",
            // Valid shape, wrong checksum.
            "021nxhr63",
        ] {
            assert_eq!(normalize_ror(value), None, "{value:?}");
        }
    }

    #[test]
    fn crockford_value_maps_alphabet() {
        // One check per boundary of each contiguous run in the alphabet.
        for (c, v) in [
            (b'0', 0),
            (b'9', 9),
            (b'a', 10),
            (b'h', 17),
            (b'j', 18),
            (b'k', 19),
            (b'm', 20),
            (b'n', 21),
            (b'p', 22),
            (b't', 26),
            (b'v', 27),
            (b'z', 31),
        ] {
            assert_eq!(crockford_value(c), Some(v), "{}", c as char);
        }
    }

    #[test]
    fn crockford_value_rejects_excluded_chars() {
        for c in *b"ilouA|! " {
            assert_eq!(crockford_value(c), None, "{}", c as char);
        }
    }

    #[test]
    fn is_valid_ror_id_accepts_real_ids() {
        // NSF, and the example ID from ROR's identifier documentation.
        for id in ["021nxhr62", "02mhbdp94"] {
            assert!(is_valid_ror_id(id), "{id}");
        }
    }

    #[test]
    fn is_valid_ror_id_rejects_structural_failures() {
        for id in ["", "021nxhr6", "021nxhr622", "121nxhr62", "021nxhr6x"] {
            assert!(!is_valid_ror_id(id), "{id:?}");
        }
    }

    #[test]
    fn ror_id_checksum_is_unique_per_body() {
        let valid: Vec<String> = (0..100)
            .map(|i| format!("021nxhr{i:02}"))
            .filter(|id| is_valid_ror_id(id))
            .collect();
        assert_eq!(valid, ["021nxhr62"]);
    }

    #[test]
    fn single_character_mutations_are_rejected() {
        let id = b"021nxhr62";
        for pos in 0..id.len() {
            for &c in b"0123456789abcdefghijklmnopqrstuvwxyz" {
                if id[pos] == c {
                    continue;
                }
                let mut mutated = *id;
                mutated[pos] = c;
                let mutated = std::str::from_utf8(&mutated).unwrap();
                assert!(!is_valid_ror_id(mutated), "{mutated}");
            }
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
    fn is_valid_doi_name_accepts_numeric_prefixes() {
        for value in [
            "10.23/x",
            "10.82461/bpzr-jd55",
            "10.500.100/segmented-prefix",
        ] {
            assert!(is_valid_doi_name(value), "rejected {value:?}");
        }
    }

    #[test]
    fn is_valid_doi_name_rejects_surrounding_whitespace() {
        assert!(!is_valid_doi_name(" 10.1/x"));
        assert!(!is_valid_doi_name("10.1/x "));
    }

    #[test]
    fn is_valid_doi_name_rejects_control_characters() {
        for bad in ["10.1/a\tb", "10.1/a\rb", "10.1/a\x00b", "10.1/\x1b"] {
            assert!(!is_valid_doi_name(bad), "accepted {bad:?}");
        }
    }

    #[test]
    fn is_valid_doi_name_treats_suffix_as_opaque() {
        for value in [
            "10.23/F72B-0103-B361-071E-08F3",
            "10.1234/日本語",
            "10.1234/with internal spaces",
            "10.1002/(sici)37:3/4<197::aid-hrm2>3.0.co;2-#",
        ] {
            assert!(is_valid_doi_name(value), "rejected {value:?}");
        }
    }

    #[test]
    fn is_valid_doi_name_rejects_other_shapes() {
        for bad in [
            "",
            "   ",
            "10.82461",
            "10.82461/",
            "10.x/y",
            "10./y",
            "10.123..456/x",
            "https://doi.org/10.1/x",
            "doi:10.1/x",
        ] {
            assert!(!is_valid_doi_name(bad), "accepted {bad:?}");
        }
    }
}
