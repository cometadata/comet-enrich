//! Fuzzy-match a free-text `resourceType` against the DataCite vocabulary.
//!
//! [`Matcher::fuzzy_match`] runs a small cascade: exact normalized lookup, then a
//! whitespace-concatenated retry, then a camelCase-split retry, then a Levenshtein fallback
//! gated by the configured threshold. Any hit is checked against the redundancy exclusions
//! before it is returned.

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

static PUNCT_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"[-_.,;:!?()\[\]{}'"/\\]"#).unwrap());

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
    // The membership set; fuzzy_match works off the sorted vec, so this is only read in tests.
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
        self.redundancy_exclusions
            .iter()
            .any(|rule| rule.normalized.contains(normalized_input) && rule.matches.contains(matched))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_camelcase_splits_boundaries() {
        assert_eq!(tokenize_camelcase("JournalArticle"), vec!["Journal", "Article"]);
        assert_eq!(tokenize_camelcase("XMLHttpRequest"), vec!["XML", "Http", "Request"]);
        assert_eq!(tokenize_camelcase("plain"), vec!["plain"]);
        assert_eq!(tokenize_camelcase("ABC"), vec!["ABC"]);
        assert_eq!(tokenize_camelcase(""), Vec::<String>::new());
    }

    #[test]
    fn smart_normalize_empty_returns_empty() {
        let typo = HashMap::new();
        assert_eq!(smart_normalize("", &typo), "");
    }

    #[test]
    fn smart_normalize_lowercases_and_concats() {
        let typo = HashMap::new();
        assert_eq!(smart_normalize("Journal Article", &typo), "journalarticle");
        assert_eq!(smart_normalize("Data-Paper", &typo), "datapaper");
        assert_eq!(smart_normalize("Book/Chapter", &typo), "bookchapter");
        assert_eq!(smart_normalize("Conference, Paper!", &typo), "conferencepaper");
    }

    #[test]
    fn smart_normalize_applies_typo_corrections() {
        let mut typo = HashMap::new();
        typo.insert("sofware".to_string(), "software".to_string());
        typo.insert("otput".to_string(), "output".to_string());
        assert_eq!(smart_normalize("Sofware", &typo), "software");
        assert_eq!(smart_normalize("otput managment", &typo), "outputmanagment");
    }

    #[test]
    fn matcher_new_builds_normalized_tables() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["JournalArticle".into(), "Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
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
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["JournalArticle".into(), "Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Journal article"), MatchOutcome::Matched("JournalArticle".into()));
        assert_eq!(m.fuzzy_match("dataset"), MatchOutcome::Matched("Dataset".into()));
    }

    #[test]
    fn fuzzy_match_whitespace_concat() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["ConferencePaper".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("CONFERENCE PAPER"), MatchOutcome::Matched("ConferencePaper".into()));
    }

    #[test]
    fn fuzzy_match_camelcase_concat() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["BookChapter".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("bookChapter"), MatchOutcome::Matched("BookChapter".into()));
    }

    #[test]
    fn fuzzy_match_levenshtein_fallback() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Dataset".into(), "JournalArticle".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Datasett"), MatchOutcome::Matched("Dataset".into()));
        assert_eq!(m.fuzzy_match("completely unrelated"), MatchOutcome::NoMatch);
    }

    #[test]
    fn fuzzy_match_levenshtein_respects_threshold() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.99,
            reference_values: vec!["Dataset".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Datasett"), MatchOutcome::NoMatch);
    }

    #[test]
    fn fuzzy_match_redundant_is_excluded() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Text".into(), "Other".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![crate::config::RedundancyRuleConfig {
                normalized: vec!["text".into(), "txt".into()],
                matches: vec!["Text".into(), "Other".into()],
            }],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Text"), MatchOutcome::Redundant);
    }

    #[test]
    fn fuzzy_match_non_redundant_still_matches() {
        let cfg = crate::config::RulesConfig {
            threshold: 0.85,
            reference_values: vec!["Dataset".into(), "Text".into()],
            typo_corrections: HashMap::new(),
            redundancy_exclusions: vec![crate::config::RedundancyRuleConfig {
                normalized: vec!["text".into()],
                matches: vec!["Text".into()],
            }],
            scope: crate::config::ScopeConfig { target_resource_type_general: vec![] },
        };
        let m = Matcher::from_config(&cfg);
        assert_eq!(m.fuzzy_match("Dataset"), MatchOutcome::Matched("Dataset".into()));
    }
}
