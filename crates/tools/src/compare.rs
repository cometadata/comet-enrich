//! Compare comet-enrich enrichment output against a re-run of the original
//! standalone tools (their `--enrichment-format` `enrichments.jsonl`).
//!
//! Both sides are treated as a set of enrichment-format JSONL records, keyed on
//! canonical `(doi, action, field, originalValue)` with `enrichedValue` as the
//! compared value. JSON key order is ignored, and any other field (`sourceId` on
//! the new side, `contributors`/`resources` on the old side) never enters the
//! comparison or the printed samples.

// DataCite, ROR, and JSONL are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

use anyhow::{Context, Result, bail};
use flate2::read::MultiGzDecoder;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_128;

/// Compare enrichment output between the new and original systems.
#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// Method being compared (informational): affiliations, funders, or
    /// resource-type-general.
    method: String,

    /// New output: a directory of `part_*.jsonl.gz` (or a single JSONL file).
    #[arg(long, value_name = "PATH")]
    new: PathBuf,

    /// Original output: the re-run tool's `enrichments.jsonl(.gz)` (or a dir).
    #[arg(long, value_name = "PATH")]
    old: PathBuf,

    /// Number of example records to show per difference bucket.
    #[arg(long, default_value_t = 20)]
    sample: usize,

    /// Write every differing record to this file (not just the samples).
    #[arg(long, value_name = "FILE")]
    dump: Option<PathBuf>,

    /// Split the comparison into N passes by key hash to bound memory.
    #[arg(long, default_value_t = 1)]
    shard: u64,
}

/// One key's presence in the OLD side: the (single, deterministic) enriched
/// value hash and how many OLD records carried this key.
struct Slot {
    valuehash: u128,
    count: u32,
}

/// A sampled enrichedValue disagreement for the same input key.
struct Mismatch {
    key: String,
    old_enriched: Option<String>,
    new_enriched: String,
}

#[derive(Default)]
struct Counters {
    old_total: u64,
    new_total: u64,
    old_malformed: u64,
    new_malformed: u64,
    matched: u64,
    only_new: u64,
    only_old: u64,
    mismatch: u64,
}

/// Accumulates comparison results across shards.
struct State {
    cap: usize,
    counters: Counters,
    only_new: Vec<String>,
    only_old: Vec<String>,
    mismatches: Vec<Mismatch>,
    mismatch_idx: HashMap<u128, usize>,
    dump: Option<BufWriter<File>>,
}

pub(crate) fn run(args: &Args) -> Result<()> {
    let known = ["affiliations", "funders", "resource-type-general"];
    if !known.contains(&args.method.as_str()) {
        eprintln!(
            "warning: unrecognised method '{}' (expected one of {known:?}); comparing anyway",
            args.method
        );
    }
    let shards = args.shard.max(1);
    let new_files = collect_files(&args.new).context("collecting --new files")?;
    let old_files = collect_files(&args.old).context("collecting --old files")?;

    let mut state = State::new(args.sample, args.dump.as_deref())?;
    for shard in 0..shards {
        state.process_shard(shard, shards, &new_files, &old_files)?;
    }
    state.finish()?;
    state.report(args);
    Ok(())
}

impl State {
    fn new(cap: usize, dump_path: Option<&Path>) -> Result<Self> {
        let dump = match dump_path {
            Some(path) => Some(BufWriter::new(
                File::create(path).with_context(|| format!("creating {}", path.display()))?,
            )),
            None => None,
        };
        Ok(Self {
            cap,
            counters: Counters::default(),
            only_new: Vec::new(),
            only_old: Vec::new(),
            mismatches: Vec::new(),
            mismatch_idx: HashMap::new(),
            dump,
        })
    }

    /// Run the three passes (index OLD, classify NEW, collect OLD samples) for
    /// one shard of the key space.
    fn process_shard(
        &mut self,
        shard: u64,
        shards: u64,
        new_files: &[PathBuf],
        old_files: &[PathBuf],
    ) -> Result<()> {
        let in_shard = |kh: u128| kh % u128::from(shards) == u128::from(shard);

        // PASS 1 — index OLD keys for this shard (hashes only, to bound memory).
        let mut old: HashMap<u128, Slot> = HashMap::new();
        let (old_total, old_malformed) = for_each_record(old_files, |rec| {
            let (kh, vh) = key_and_value(rec);
            if in_shard(kh) {
                old.entry(kh).and_modify(|s| s.count += 1).or_insert(Slot {
                    valuehash: vh,
                    count: 1,
                });
            }
            Ok(())
        })?;

        // PASS 2 — stream NEW, classifying against the OLD index.
        let (new_total, new_malformed) = for_each_record(new_files, |rec| {
            let (kh, vh) = key_and_value(rec);
            if in_shard(kh) {
                self.classify_new(rec, kh, vh, &mut old)?;
            }
            Ok(())
        })?;

        // Leftover OLD copies were never consumed by NEW → only-in-old.
        for slot in old.values() {
            self.counters.only_old += u64::from(slot.count);
        }

        // Each pass reads every file, so totals are the same in every shard;
        // record them once.
        if shard == 0 {
            self.counters.old_total = old_total;
            self.counters.old_malformed = old_malformed;
            self.counters.new_total = new_total;
            self.counters.new_malformed = new_malformed;
        }

        self.collect_old_samples(&old, in_shard, old_files)
    }

    /// Classify one NEW record against the OLD index (PASS 2 body).
    fn classify_new(
        &mut self,
        rec: &Value,
        kh: u128,
        vh: u128,
        old: &mut HashMap<u128, Slot>,
    ) -> Result<()> {
        // Key absent, or NEW has more copies of this key than OLD did.
        let Some(slot) = old.get_mut(&kh).filter(|s| s.count > 0) else {
            self.counters.only_new += 1;
            push_sample(&mut self.only_new, self.cap, || sample_compact(rec));
            return dump_record(&mut self.dump, "only_new", rec);
        };
        slot.count -= 1;
        if vh == slot.valuehash {
            self.counters.matched += 1;
            return Ok(());
        }
        self.counters.mismatch += 1;
        if self.mismatches.len() < self.cap && !self.mismatch_idx.contains_key(&kh) {
            self.mismatch_idx.insert(kh, self.mismatches.len());
            self.mismatches.push(Mismatch {
                key: key_compact(rec),
                old_enriched: None,
                new_enriched: enriched_compact(rec),
            });
        }
        dump_record(&mut self.dump, "mismatch_new", rec)
    }

    /// PASS 3 — re-read OLD only to collect samples: only-in-old records and the
    /// OLD side of sampled mismatches. Skipped when nothing needs it.
    fn collect_old_samples(
        &mut self,
        old: &HashMap<u128, Slot>,
        in_shard: impl Fn(u128) -> bool,
        old_files: &[PathBuf],
    ) -> Result<()> {
        let need_old = self.only_old.len() < self.cap && self.counters.only_old > 0;
        let need_mismatch = self.mismatches.iter().any(|m| m.old_enriched.is_none());
        if !need_old && !need_mismatch {
            return Ok(());
        }
        for_each_record(old_files, |rec| {
            let (kh, _) = key_and_value(rec);
            if !in_shard(kh) {
                return Ok(());
            }
            let Some(slot) = old.get(&kh) else {
                return Ok(());
            };
            if slot.count > 0 {
                push_sample(&mut self.only_old, self.cap, || sample_compact(rec));
                dump_record(&mut self.dump, "only_old", rec)?;
            }
            if let Some(&idx) = self.mismatch_idx.get(&kh) {
                if self.mismatches[idx].old_enriched.is_none() {
                    self.mismatches[idx].old_enriched = Some(enriched_compact(rec));
                    dump_record(&mut self.dump, "mismatch_old", rec)?;
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(w) = self.dump.as_mut() {
            w.flush().context("flushing dump file")?;
        }
        Ok(())
    }

    /// Print the comparison summary and samples.
    fn report(&self, args: &Args) {
        let c = &self.counters;
        let clean = c.only_new == 0 && c.only_old == 0 && c.mismatch == 0;
        println!("=== compare: {} ===", args.method);
        println!(
            "  new:  {} records ({} malformed)",
            c.new_total, c.new_malformed
        );
        println!(
            "  old:  {} records ({} malformed)",
            c.old_total, c.old_malformed
        );
        println!("  matched:          {}", c.matched);
        println!("  only in NEW:      {}", c.only_new);
        println!("  only in OLD:      {}", c.only_old);
        println!("  enriched differs: {}", c.mismatch);
        println!(
            "  verdict: {}",
            if clean {
                "CLEAN (no differences)"
            } else {
                "DIFFERENCES FOUND"
            }
        );
        if let Some(path) = &args.dump {
            println!("  full diff written to {}", path.display());
        }

        print_samples("only in NEW (record not present in OLD)", &self.only_new);
        print_samples("only in OLD (record not present in NEW)", &self.only_old);
        if !self.mismatches.is_empty() {
            println!(
                "\n-- enrichedValue differs (same input key) [{}] --",
                self.mismatches.len()
            );
            for m in &self.mismatches {
                println!("  key: {}", m.key);
                println!(
                    "    OLD enriched: {}",
                    m.old_enriched.as_deref().unwrap_or("<missing>")
                );
                println!("    NEW enriched: {}", m.new_enriched);
            }
        }
    }
}

/// Read every JSONL record from `files`, invoking `f` for each parsed record.
/// Gzip (`.gz`) is decoded transparently; malformed lines are counted, not fatal.
/// Returns `(records_parsed, lines_malformed)`.
fn for_each_record(
    files: &[PathBuf],
    mut f: impl FnMut(&Value) -> Result<()>,
) -> Result<(u64, u64)> {
    let mut total = 0u64;
    let mut malformed = 0u64;
    for file in files {
        let handle = File::open(file).with_context(|| format!("opening {}", file.display()))?;
        let reader: Box<dyn Read> = if file.extension().is_some_and(|e| e == "gz") {
            Box::new(MultiGzDecoder::new(handle))
        } else {
            Box::new(handle)
        };
        for line in BufReader::new(reader).lines() {
            let line = line.with_context(|| format!("reading {}", file.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(rec) => {
                    total += 1;
                    f(&rec)?;
                }
                Err(_) => malformed += 1,
            }
        }
    }
    Ok((total, malformed))
}

/// List the JSONL/JSONL.gz files under `path` (or `path` itself if it is a file).
// Enrichment part files are written with lowercase `.jsonl(.gz)` extensions, so a
// case-sensitive suffix match is exactly what we want here.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn collect_files(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if !path.is_dir() {
        bail!("path not found: {}", path.display());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(path).with_context(|| format!("reading dir {}", path.display()))? {
        let p = entry?.path();
        if p.is_file() {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if name.ends_with(".jsonl") || name.ends_with(".jsonl.gz") {
                files.push(p);
            }
        }
    }
    files.sort();
    if files.is_empty() {
        bail!("no .jsonl or .jsonl.gz files under {}", path.display());
    }
    Ok(files)
}

/// The comparison key hash and enriched-value hash for one record.
///
/// key = canonical `(doi, action, field, originalValue)`; every other field is
/// ignored.
fn key_and_value(rec: &Value) -> (u128, u128) {
    let key_str = key_compact(rec);
    let val_str = enriched_compact(rec);
    (xxh3_128(key_str.as_bytes()), xxh3_128(val_str.as_bytes()))
}

/// Recursively sort object keys so key order does not affect equality.
fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let mut out = Map::new();
            for k in keys {
                out.insert(k.clone(), canonicalize(&map[k]));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// The top-level fields that identify one enrichment record, in key order.
const KEY_FIELDS: [&str; 4] = ["doi", "action", "field", "originalValue"];

/// Canonical copies of the named top-level fields (`null` when absent).
fn fields(rec: &Value, names: &[&str]) -> Vec<Value> {
    names
        .iter()
        .map(|k| rec.get(*k).map_or(Value::Null, canonicalize))
        .collect()
}

/// Compact canonical JSON of the comparison key plus `enrichedValue`, for
/// samples. A keyed object (`KEY_FIELDS` order, then `enrichedValue`) rather
/// than a positional array, so a printed sample reads without knowing the
/// column order; every other field is dropped. `serde_json`'s `preserve_order`
/// keeps the keys in insertion order.
fn sample_compact(rec: &Value) -> String {
    let names: Vec<&str> = KEY_FIELDS
        .iter()
        .copied()
        .chain(["enrichedValue"])
        .collect();
    let obj: Map<String, Value> = names
        .iter()
        .map(|&k| k.to_owned())
        .zip(fields(rec, &names))
        .collect();
    serde_json::to_string(&Value::Object(obj)).unwrap_or_default()
}

/// Compact canonical JSON of just the comparison key, as a positional array.
/// This feeds the record hash, so its shape must stay stable.
fn key_compact(rec: &Value) -> String {
    serde_json::to_string(&Value::Array(fields(rec, &KEY_FIELDS))).unwrap_or_default()
}

/// Compact canonical JSON of the record's `enrichedValue`.
fn enriched_compact(rec: &Value) -> String {
    let ev = rec.get("enrichedValue").map_or(Value::Null, canonicalize);
    serde_json::to_string(&ev).unwrap_or_default()
}

/// Push a lazily-built sample if the bucket is below its cap.
fn push_sample(bucket: &mut Vec<String>, cap: usize, build: impl FnOnce() -> String) {
    if bucket.len() < cap {
        bucket.push(build());
    }
}

/// Append a `{bucket, record}` line to the dump file when one is configured.
fn dump_record(dump: &mut Option<BufWriter<File>>, bucket: &str, rec: &Value) -> Result<()> {
    if let Some(w) = dump.as_mut() {
        let record = serde_json::to_string(rec).unwrap_or_default();
        writeln!(w, "{{\"bucket\":\"{bucket}\",\"record\":{record}}}")
            .context("writing dump record")?;
    }
    Ok(())
}

fn print_samples(title: &str, samples: &[String]) {
    if samples.is_empty() {
        return;
    }
    println!("\n-- {title} [showing {}] --", samples.len());
    for s in samples {
        println!("  {s}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn samples_show_only_key_and_enriched_value() {
        let new_side = json!({
            "doi": "10.1/a",
            "action": "update",
            "field": "types",
            "originalValue": {"resourceTypeGeneral": "Text"},
            "enrichedValue": {"resourceTypeGeneral": "Dataset"},
            "sourceId": "10.82461/aaaa-aaaa"
        });
        let mut old_side = new_side.clone();
        old_side.as_object_mut().unwrap().remove("sourceId");
        old_side["contributors"] = json!([{"name": "COMET"}]);
        old_side["resources"] = json!([{"relatedIdentifier": "10.x/y"}]);
        assert_eq!(sample_compact(&new_side), sample_compact(&old_side));

        let mut other_value = new_side.clone();
        other_value["enrichedValue"]["resourceTypeGeneral"] = json!("Software");
        assert_ne!(sample_compact(&new_side), sample_compact(&other_value));

        let sample = sample_compact(&new_side);
        assert!(sample.contains("\"Dataset\""));
        assert!(!sample.contains("sourceId"));
        assert!(sample.contains("\"doi\":"));
        assert!(sample.contains("\"enrichedValue\":"));

        // Labelled, and in exactly this order.
        let parsed: Map<String, Value> = serde_json::from_str(&sample).unwrap();
        let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["doi", "action", "field", "originalValue", "enrichedValue"]
        );
    }

    #[test]
    fn key_is_a_positional_array_with_null_for_absent_fields() {
        let rec = json!({
            "doi": "10.1/a",
            "field": "types",
            "originalValue": {"b": 1, "a": 2},
            "enrichedValue": {"ignored": true}
        });
        assert_eq!(
            key_compact(&rec),
            r#"["10.1/a",null,"types",{"a":2,"b":1}]"#
        );
    }

    #[test]
    fn enriched_value_is_canonical_and_null_when_absent() {
        assert_eq!(enriched_compact(&json!({"doi": "10.1/a"})), "null");
        let rec = json!({"enrichedValue": {"b": 1, "a": {"d": 2, "c": 3}}});
        assert_eq!(enriched_compact(&rec), r#"{"a":{"c":3,"d":2},"b":1}"#);
    }
}
