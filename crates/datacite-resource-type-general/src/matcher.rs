//! Match free-text `resourceType` values to DataCite type names.
//!
//! Matching starts with normalized exact matches, then tries a few common
//! formatting variants before falling back to Levenshtein similarity. Matches
//! listed as redundant are reported separately so callers can avoid emitting
//! unnecessary changes.

use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use strsim::normalized_levenshtein;

static CAMEL_RE1: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([a-z0-9])([A-Z])").unwrap());
static CAMEL_RE2: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([A-Z]+)([A-Z][a-z])").unwrap());

pub fn tokenize_camelcase(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let s1 = CAMEL_RE1.replace_all(text, "$1 $2");
    let s2 = CAMEL_RE2.replace_all(&s1, "$1 $2");
    s2.split_whitespace().map(str::to_string).collect()
}

static PUNCT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"[-_.,;:!?()\[\]{}'"/\\]"#).unwrap());

pub fn smart_normalize(text: &str, typo_corrections: &HashMap<String, String>) -> String {
    if text.is_empty() {
        return String::new();
    }
    let lower = text.to_lowercase();
    let cleaned = PUNCT_RE.replace_all(&lower, " ");
    let mut out = String::with_capacity(cleaned.len());
    for word in cleaned.split_whitespace() {
        let corrected = typo_corrections.get(word).map_or(word, String::as_str);
        out.push_str(corrected);
    }
    out
}

pub struct Matcher {
    pub threshold: f64,
    pub typo_corrections: HashMap<String, String>,
    pub normalized_to_original: HashMap<String, String>,
    #[allow(dead_code)]
    pub normalized_values: HashSet<String>,
    pub normalized_values_sorted: Vec<String>,
    pub redundancy_exclusions: Vec<RedundancyRule>,
}

pub struct RedundancyRule {
    pub normalized: HashSet<String>,
    pub matches: HashSet<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MatchOutcome {
    Matched(String),
    Redundant,
    NoMatch,
}

impl Matcher {
    pub fn from_config(cfg: &crate::config::RulesConfig) -> Self {
        let typo_corrections = cfg.typo_corrections.clone();
        let mut normalized_values = HashSet::new();
        let mut normalized_to_original = HashMap::new();
        for original in &cfg.reference_values {
            let n = smart_normalize(original, &typo_corrections);
            normalized_values.insert(n.clone());
            normalized_to_original.insert(n, original.clone());
        }
        let redundancy_exclusions = cfg
            .redundancy_exclusions
            .iter()
            .map(|r| RedundancyRule {
                normalized: r.normalized.iter().cloned().collect(),
                matches: r.matches.iter().cloned().collect(),
            })
            .collect();
        let mut normalized_values_sorted: Vec<String> = normalized_values.iter().cloned().collect();
        normalized_values_sorted.sort();
        Matcher {
            threshold: cfg.threshold,
            typo_corrections,
            normalized_to_original,
            normalized_values,
            normalized_values_sorted,
            redundancy_exclusions,
        }
    }

    pub fn fuzzy_match(&self, text: &str) -> MatchOutcome {
        let normalized = smart_normalize(text, &self.typo_corrections);
        if let Some(orig) = self.normalized_to_original.get(&normalized) {
            if self.is_redundant(&normalized, orig) {
                return MatchOutcome::Redundant;
            }
            return MatchOutcome::Matched(orig.clone());
        }
        let tokens: Vec<&str> = text.split_whitespace().collect();
        if tokens.len() > 1 {
            let concat: String = tokens.iter().map(|t| t.trim().to_lowercase()).collect();
            let n = smart_normalize(&concat, &self.typo_corrections);
            if let Some(orig) = self.normalized_to_original.get(&n) {
                if self.is_redundant(&normalized, orig) {
                    return MatchOutcome::Redundant;
                }
                return MatchOutcome::Matched(orig.clone());
            }
        }
        let camel = tokenize_camelcase(text);
        if camel.len() > 1 {
            let concat: String = camel.iter().map(String::as_str).collect();
            let n = smart_normalize(&concat, &self.typo_corrections);
            if let Some(orig) = self.normalized_to_original.get(&n) {
                if self.is_redundant(&normalized, orig) {
                    return MatchOutcome::Redundant;
                }
                return MatchOutcome::Matched(orig.clone());
            }
        }
        let mut best_ratio = 0.0;
        let mut best_ref: Option<&String> = None;
        for n_ref in &self.normalized_values_sorted {
            let ratio = normalized_levenshtein(&normalized, n_ref);
            if ratio > best_ratio && ratio >= self.threshold {
                best_ratio = ratio;
                best_ref = Some(n_ref);
            }
        }
        if let Some(n_ref) = best_ref {
            if let Some(orig) = self.normalized_to_original.get(n_ref) {
                if self.is_redundant(&normalized, orig) {
                    return MatchOutcome::Redundant;
                }
                return MatchOutcome::Matched(orig.clone());
            }
        }
        MatchOutcome::NoMatch
    }

    fn is_redundant(&self, normalized_input: &str, matched: &str) -> bool {
        self.redundancy_exclusions.iter().any(|rule| {
            rule.normalized.contains(normalized_input) && rule.matches.contains(matched)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_camelcase_splits_boundaries() {
        // Checks that camelCase and acronym boundaries are split without
        // changing plain single-token strings.
        assert_eq!(
            tokenize_camelcase("JournalArticle"),
            vec!["Journal", "Article"]
        );
        assert_eq!(
            tokenize_camelcase("XMLHttpRequest"),
            vec!["XML", "Http", "Request"]
        );
        assert_eq!(tokenize_camelcase("plain"), vec!["plain"]);
        assert_eq!(tokenize_camelcase("ABC"), vec!["ABC"]);
        assert_eq!(tokenize_camelcase(""), Vec::<String>::new());
    }

    #[test]
    fn smart_normalize_empty_returns_empty() {
        // Checks that an empty resource type stays empty.
        let typo = HashMap::new();
        assert_eq!(smart_normalize("", &typo), "");
    }

    #[test]
    fn smart_normalize_lowercases_and_concats() {
        // Checks that normalization removes separators and makes matching
        // insensitive to case and punctuation.
        let typo = HashMap::new();
        assert_eq!(smart_normalize("Journal Article", &typo), "journalarticle");
        assert_eq!(smart_normalize("Data-Paper", &typo), "datapaper");
        assert_eq!(smart_normalize("Book/Chapter", &typo), "bookchapter");
        assert_eq!(
            smart_normalize("Conference, Paper!", &typo),
            "conferencepaper"
        );
    }

    #[test]
    fn smart_normalize_applies_typo_corrections() {
        // Checks that configured word-level typo corrections are applied before
        // the words are joined.
        let mut typo = HashMap::new();
        typo.insert("sofware".to_string(), "software".to_string());
        typo.insert("otput".to_string(), "output".to_string());
        assert_eq!(smart_normalize("Sofware", &typo), "software");
        assert_eq!(smart_normalize("otput managment", &typo), "outputmanagment");
    }

    #[test]
    fn matcher_new_builds_normalized_tables() {
        // Checks that the matcher stores normalized lookup keys while preserving
        // the original DataCite type names for output.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["JournalArticle".into(), "Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert!(m.normalized_values.contains("journalarticle"));
        assert!(m.normalized_values.contains("dataset"));
        assert_eq!(
            m.normalized_to_original.get("journalarticle"),
            Some(&"JournalArticle".to_string())
        );
    }

    #[test]
    fn fuzzy_match_exact_normalized() {
        // Checks that direct normalized matches return the original reference
        // value.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["JournalArticle".into(), "Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(
            m.fuzzy_match("Journal article"),
            MatchOutcome::Matched("JournalArticle".into())
        );
        assert_eq!(
            m.fuzzy_match("dataset"),
            MatchOutcome::Matched("Dataset".into())
        );
    }

    #[test]
    fn fuzzy_match_whitespace_concat() {
        // Checks that separated words can still match a compact reference value.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["ConferencePaper".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(
            m.fuzzy_match("CONFERENCE PAPER"),
            MatchOutcome::Matched("ConferencePaper".into())
        );
    }

    #[test]
    fn fuzzy_match_camelcase_concat() {
        // Checks that different camelCase casing still resolves to the same
        // reference value.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["BookChapter".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(
            m.fuzzy_match("bookChapter"),
            MatchOutcome::Matched("BookChapter".into())
        );
    }

    #[test]
    fn fuzzy_match_levenshtein_fallback() {
        // Checks that near misses can match through the Levenshtein fallback,
        // while unrelated text is rejected.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Dataset".into(), "JournalArticle".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(
            m.fuzzy_match("Datasett"),
            MatchOutcome::Matched("Dataset".into())
        );
        assert_eq!(m.fuzzy_match("completely unrelated"), MatchOutcome::NoMatch);
    }

    #[test]
    fn fuzzy_match_levenshtein_respects_threshold() {
        // Checks that the Levenshtein fallback does not match below the
        // configured threshold.
        let cfg = crate::config::RulesConfig {
            threshold: 0.99,
            reference_values: vec!["Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Datasett"), MatchOutcome::NoMatch);
    }

    #[test]
    fn fuzzy_match_redundant_is_excluded() {
        // Checks that matches covered by a redundancy rule are reported as
        // redundant instead of as a normal match.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Text".into(), "Other".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![crate::config::RedundancyRuleConfig {
                normalized: vec!["text".into(), "txt".into()],
                matches: vec!["Text".into(), "Other".into()],
            }],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Text"), MatchOutcome::Redundant);
    }

    #[test]
    fn fuzzy_match_non_redundant_still_matches() {
        // Checks that redundancy rules only block the configured input/match
        // pairs.
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Dataset".into(), "Text".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![crate::config::RedundancyRuleConfig {
                normalized: vec!["text".into()],
                matches: vec!["Text".into()],
            }],
            scope: crate::config::ScopeConfig {
                target_resource_type_general: vec![],
            },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(
            m.fuzzy_match("Dataset"),
            MatchOutcome::Matched("Dataset".into())
        );
    }
}
