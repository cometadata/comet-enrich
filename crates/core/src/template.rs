//! Validated run-level values copied into every enrichment record.
//!
//! The [`EnrichmentTemplate`] is built once from CLI arguments and reused while
//! records are written.

use crate::identifiers::is_valid_doi_name;
use anyhow::{Result, anyhow};

/// Values that are the same for every record in a run.
#[derive(Debug, Clone)]
pub struct EnrichmentTemplate {
    source_id: String,
}

impl EnrichmentTemplate {
    /// Build a template from the DOI name of the enrichment project, such as
    /// `10.1234/example`. DOI names are case-insensitive, so the value is
    /// stored in ASCII lowercase, the form DataCite uses; non-ASCII characters
    /// are kept as given.
    ///
    /// # Errors
    ///
    /// Returns an error if `source_id` does not match DOI name syntax, such as
    /// `10.1234/example`.
    pub fn new(source_id: &str) -> Result<Self> {
        if !is_valid_doi_name(source_id) {
            return Err(anyhow!(
                "source id must be a DOI name such as 10.1234/example, got `{source_id}`"
            ));
        }
        Ok(Self {
            source_id: source_id.to_ascii_lowercase(),
        })
    }

    /// DOI name of the enrichment project that produced the records, such as
    /// `10.1234/example`, in ASCII lowercase.
    #[must_use]
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE_ID: &str = "10.82461/bpzr-jd55";

    fn template() -> EnrichmentTemplate {
        EnrichmentTemplate::new(SOURCE_ID).unwrap()
    }

    #[test]
    fn new_preserves_the_source_doi_name() {
        assert_eq!(template().source_id(), SOURCE_ID);
    }

    #[test]
    fn new_lowercases_ascii_letters_in_the_source_id() {
        let t = EnrichmentTemplate::new("10.82461/BPZR-JD55").unwrap();
        assert_eq!(t.source_id(), SOURCE_ID);
    }

    #[test]
    fn new_leaves_non_ascii_letters_in_the_source_id_untouched() {
        let t = EnrichmentTemplate::new("10.1234/ÜBER-Ab").unwrap();
        assert_eq!(t.source_id(), "10.1234/Über-ab");
    }

    #[test]
    fn new_rejects_non_doi_source_id() {
        let err = EnrichmentTemplate::new("not-a-doi")
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be a DOI"), "got: {err}");
        assert!(err.contains("not-a-doi"), "got: {err}");
    }

    #[test]
    fn new_rejects_source_id_with_surrounding_whitespace() {
        assert!(EnrichmentTemplate::new(" 10.82461/bpzr-jd55").is_err());
        assert!(EnrichmentTemplate::new("10.82461/bpzr-jd55 ").is_err());
    }
}
