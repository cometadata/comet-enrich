//! Crossref Funder ID to ROR crosswalk from the ROR registry.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct ExternalId {
    #[serde(rename = "type")]
    id_type: String,
    all: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RorRecord {
    id: String,
    #[serde(default)]
    external_ids: Vec<ExternalId>,
}

/// Load fundref external IDs from a ROR v2 registry dump.
///
/// Later records overwrite earlier ones for duplicate Funder IDs.
pub(crate) fn load(path: &Path) -> Result<HashMap<String, String>> {
    let file =
        File::open(path).with_context(|| format!("opening ROR registry {}", path.display()))?;
    let records: Vec<RorRecord> = serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("parsing ROR registry {}", path.display()))?;

    let mut fundref_to_ror = HashMap::new();
    for record in records {
        for external_id in &record.external_ids {
            if external_id.id_type == "fundref" {
                for fundref in &external_id.all {
                    fundref_to_ror.insert(fundref.clone(), record.id.clone());
                }
            }
        }
    }
    Ok(fundref_to_ror)
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::assert_err_contains;
    use std::io::Write;

    const NSF_ROR: &str = "https://ror.org/021nxhr62";

    fn dump_file(json: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();
        file
    }

    #[test]
    fn load_builds_fundref_to_ror_from_all_ids() {
        let dump = dump_file(
            r#"[
              {"id": "https://ror.org/021nxhr62",
               "names": [{"value": "National Science Foundation", "types": ["ror_display"]}],
               "external_ids": [
                 {"type": "fundref", "all": ["100000001", "100005441"], "preferred": "100000001"},
                 {"type": "isni", "all": ["0000 0001 2345 6789"]}
               ]}
            ]"#,
        );

        let crosswalk = load(dump.path()).unwrap();

        assert_eq!(crosswalk.len(), 2);
        assert_eq!(crosswalk["100000001"], NSF_ROR);
        assert_eq!(crosswalk["100005441"], NSF_ROR);
        assert!(!crosswalk.contains_key("0000 0001 2345 6789"));
    }

    #[test]
    fn load_handles_records_without_external_ids() {
        let dump = dump_file(
            r#"[
              {"id": "https://ror.org/bbbbbbbbb",
               "names": [{"value": "Other Org", "types": ["ror_display"]}]}
            ]"#,
        );
        assert!(load(dump.path()).unwrap().is_empty());
    }

    #[test]
    fn load_errors_on_malformed_json() {
        let dump = dump_file("not json");
        assert_err_contains(load(dump.path()), "parsing ROR registry");
    }

    #[test]
    fn load_errors_on_missing_file() {
        assert_err_contains(
            load(Path::new("__missing_ror__.json")),
            "opening ROR registry",
        );
    }
}
