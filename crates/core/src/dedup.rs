//! Dedup store for lookup inputs.
//!
//! Inputs are deduped by value, then written as `{ "hash", "value" }` JSONL rows.
//! Hash collisions between distinct values are errors.

use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

/// Width of the xxh3 dedup hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashBits {
    /// xxh3-64, 16 hex chars.
    #[default]
    Bits64,
    /// xxh3-128, 32 hex chars.
    Bits128,
}

impl HashBits {
    /// Stable label, e.g. for error messages and the run manifest.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            HashBits::Bits64 => "xxh3-64",
            HashBits::Bits128 => "xxh3-128",
        }
    }
}

impl fmt::Display for HashBits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Raw hash of `s`, zero-extended for the 64-bit case.
fn raw_hash(s: &str, bits: HashBits) -> u128 {
    match bits {
        HashBits::Bits64 => u128::from(xxh3_64(s.as_bytes())),
        HashBits::Bits128 => xxh3_128(s.as_bytes()),
    }
}

/// Format a raw hash as zero-padded lowercase hex of the width's length.
fn format_hash(raw: u128, bits: HashBits) -> String {
    match bits {
        HashBits::Bits64 => format!("{raw:016x}"),
        HashBits::Bits128 => format!("{raw:032x}"),
    }
}

/// Hex digest of the raw bytes of `s` at the given width.
///
/// The input is hashed as-is: no trimming, case folding, or normalization.
#[must_use]
pub fn hash_input(s: &str, bits: HashBits) -> String {
    format_hash(raw_hash(s, bits), bits)
}

/// Error if a different value already produced this hash.
fn check_collision<'a>(
    seen: &mut HashMap<u128, &'a str>,
    raw: u128,
    value: &'a str,
    bits: HashBits,
) -> Result<()> {
    if let Some(prev) = seen.insert(raw, value) {
        bail!(
            "{bits} hash collision: {prev:?} and {value:?} both hash to {}; rerun with a wider hash width",
            format_hash(raw, bits)
        );
    }
    Ok(())
}

/// One `inputs.jsonl` row: a unique input and its hash.
#[derive(Serialize)]
struct InputRow<'a> {
    hash: &'a str,
    value: &'a str,
}

/// Accumulates unique lookup inputs in sorted order.
#[derive(Debug, Default, Clone)]
pub struct DedupStore {
    inputs: BTreeSet<String>,
}

impl DedupStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one input, ignoring duplicates.
    pub fn insert(&mut self, input: impl Into<String>) {
        self.inputs.insert(input.into());
    }

    /// Fold another store's inputs into this one.
    pub fn merge(&mut self, other: DedupStore) {
        self.inputs.extend(other.inputs);
    }

    /// Number of unique inputs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    /// Whether the store holds no inputs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    /// Iterate the unique inputs in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.inputs.iter()
    }

    /// Write `{ "hash", "value" }` rows in sorted order.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or written, or if two
    /// distinct values collide to the same hash (see [`check_collision`]).
    pub fn write_jsonl(&self, path: &Path, bits: HashBits) -> Result<()> {
        let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
        let mut w = BufWriter::new(file);
        let mut seen: HashMap<u128, &str> = HashMap::with_capacity(self.inputs.len());
        for value in &self.inputs {
            let raw = raw_hash(value, bits);
            check_collision(&mut seen, raw, value, bits)?;
            let hash = format_hash(raw, bits);
            let row = InputRow { hash: &hash, value };
            serde_json::to_writer(&mut w, &row)
                .with_context(|| format!("writing inputs row for {value:?}"))?;
            w.write_all(b"\n")
                .with_context(|| format!("writing inputs row for {value:?}"))?;
        }
        w.flush()
            .with_context(|| format!("flushing {}", path.display()))?;
        Ok(())
    }
}

impl Extend<String> for DedupStore {
    fn extend<T: IntoIterator<Item = String>>(&mut self, iter: T) {
        self.inputs.extend(iter);
    }
}

impl FromIterator<String> for DedupStore {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self {
            inputs: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::fs;

    #[test]
    fn hash_64_matches_prototype_ground_truth() {
        assert_eq!(
            hash_input(
                "University of Nottingham Vice Chancellor's Scholarship (International) award.",
                HashBits::Bits64
            ),
            "02ad37d94c7ac3af"
        );
    }

    #[test]
    fn hash_128_golden() {
        assert_eq!(
            hash_input("MIT", HashBits::Bits128),
            "5cfc385e6671f0a657c3834cafabbb94"
        );
    }

    #[test]
    fn hash_widths_are_lowercase_hex() {
        let h64 = hash_input("anything", HashBits::Bits64);
        let h128 = hash_input("anything", HashBits::Bits128);
        assert_eq!(h64.len(), 16);
        assert_eq!(h128.len(), 32);
        for h in [&h64, &h128] {
            assert!(
                h.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            );
        }
    }

    #[test]
    fn hash_is_deterministic_and_distinct() {
        for bits in [HashBits::Bits64, HashBits::Bits128] {
            assert_eq!(hash_input("MIT", bits), hash_input("MIT", bits));
            assert_ne!(hash_input("MIT", bits), hash_input("Stanford", bits));
        }
    }

    #[test]
    fn hash_does_not_normalize() {
        assert_ne!(
            hash_input(" MIT", HashBits::Bits64),
            hash_input("MIT", HashBits::Bits64)
        );
        assert_ne!(
            hash_input("mit", HashBits::Bits64),
            hash_input("MIT", HashBits::Bits64)
        );
    }

    #[test]
    fn dedups_and_orders() {
        let mut store = DedupStore::new();
        store.insert("zebra");
        store.insert("apple");
        store.insert("apple");
        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());
        let values: Vec<&String> = store.iter().collect();
        assert_eq!(values, ["apple", "zebra"]);
    }

    #[test]
    fn merge_and_from_iter_combine_and_dedup() {
        let mut a = DedupStore::from_iter(["a".to_owned(), "b".to_owned()]);
        let b = DedupStore::from_iter(["b".to_owned(), "c".to_owned()]);
        a.merge(b);
        let values: Vec<&String> = a.iter().collect();
        assert_eq!(values, ["a", "b", "c"]);
    }

    fn roundtrip(bits: HashBits) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inputs.jsonl");

        let mut store = DedupStore::new();
        store.insert("National Science Foundation");
        store.insert("MIT");
        store.insert("MIT"); // duplicate
        store.write_jsonl(&path, bits).unwrap();

        let body = fs::read_to_string(&path).unwrap();
        let rows: Vec<Value> = body
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();

        assert_eq!(rows.len(), 2);
        let values: Vec<&str> = rows.iter().map(|r| r["value"].as_str().unwrap()).collect();
        assert_eq!(values, ["MIT", "National Science Foundation"]);

        for row in &rows {
            let value = row["value"].as_str().unwrap();
            assert_eq!(row["hash"].as_str().unwrap(), hash_input(value, bits));
        }
    }

    #[test]
    fn write_jsonl_roundtrips_at_64() {
        roundtrip(HashBits::Bits64);
    }

    #[test]
    fn write_jsonl_roundtrips_at_128() {
        roundtrip(HashBits::Bits128);
    }

    #[test]
    fn write_jsonl_empty_store_writes_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inputs.jsonl");
        DedupStore::new()
            .write_jsonl(&path, HashBits::Bits64)
            .unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "");
    }

    #[test]
    fn collision_policy_rejects_distinct_values_with_same_hash() {
        let mut seen: HashMap<u128, &str> = HashMap::new();
        assert!(check_collision(&mut seen, 42, "a", HashBits::Bits64).is_ok());
        assert!(check_collision(&mut seen, 7, "b", HashBits::Bits64).is_ok());
        let err = check_collision(&mut seen, 42, "c", HashBits::Bits64)
            .unwrap_err()
            .to_string();
        assert!(err.contains("collision"), "unexpected error: {err}");
        assert!(
            err.contains("\"a\"") && err.contains("\"c\""),
            "error names both values: {err}"
        );
    }
}
