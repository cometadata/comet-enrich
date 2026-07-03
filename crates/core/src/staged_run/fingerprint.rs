use super::INPUTS_FINGERPRINT_FILE;
use crate::fanout::input_files;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Input fingerprint entry: relative path, compressed size, and gzip trailer CRC32.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct FingerprintFile {
    path: String,
    size: u64,
    gzip_crc32: u32,
}

/// Identity of the input corpus a work dir was built from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InputFingerprint {
    version: u32,
    files: Vec<FingerprintFile>,
}

const FINGERPRINT_VERSION: u32 = 2;

pub(super) fn compute_input_fingerprint(
    input: &Path,
    files: &[PathBuf],
) -> Result<InputFingerprint> {
    let mut out = Vec::with_capacity(files.len());
    for path in files {
        let meta = fs::metadata(path)
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        let rel = path.strip_prefix(input).unwrap_or(path);
        out.push(FingerprintFile {
            path: rel.to_string_lossy().into_owned(),
            size: meta.len(),
            gzip_crc32: read_gzip_crc32(path, meta.len())?,
        });
    }
    Ok(InputFingerprint {
        version: FINGERPRINT_VERSION,
        files: out,
    })
}

fn read_gzip_crc32(path: &Path, size: u64) -> Result<u32> {
    if size < 8 {
        bail!(
            "gzip file {} is too small to contain a trailer",
            path.display()
        );
    }
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    file.seek(SeekFrom::End(-8))
        .with_context(|| format!("seeking gzip trailer in {}", path.display()))?;
    let mut trailer = [0_u8; 8];
    file.read_exact(&mut trailer)
        .with_context(|| format!("reading gzip trailer from {}", path.display()))?;
    Ok(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]))
}

pub(super) fn write_input_fingerprint(work: &Path, fingerprint: &InputFingerprint) -> Result<()> {
    let json = serde_json::to_string(fingerprint).context("serializing input fingerprint")?;
    fs::write(work.join(INPUTS_FINGERPRINT_FILE), json)
        .with_context(|| format!("writing {INPUTS_FINGERPRINT_FILE}"))
}

/// Validate the current input against the fingerprint written by extract.
pub(super) fn validate_input_fingerprint(work: &Path, input: &Path) -> Result<()> {
    let files = input_files(input)?;
    let current = compute_input_fingerprint(input, &files)?;
    let path = work.join(INPUTS_FINGERPRINT_FILE);
    if !path.exists() {
        bail!(
            "missing {INPUTS_FINGERPRINT_FILE} in {}; resuming would report stale outputs as current \
             (use --from-scratch to rerun everything)",
            work.display()
        );
    }
    let body = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let pinned: InputFingerprint = serde_json::from_str(&body)
        .with_context(|| format!("parsing {INPUTS_FINGERPRINT_FILE}"))?;
    if pinned != current {
        bail!(
            "input corpus does not match the one this run dir was built from ({}); \
             resuming would report stale outputs as current \
             (use --from-scratch to rerun everything, or run a single stage subcommand \
             such as `reconcile` to reuse the existing work artifacts)",
            fingerprint_diff(&pinned, &current),
        );
    }
    Ok(())
}

/// Summarize how the current corpus differs from the pinned fingerprint.
fn fingerprint_diff(pinned: &InputFingerprint, current: &InputFingerprint) -> String {
    if pinned.version != current.version {
        return "fingerprint format changed".to_owned();
    }

    let pinned_map: BTreeMap<&str, &FingerprintFile> =
        pinned.files.iter().map(|f| (f.path.as_str(), f)).collect();
    let current_map: BTreeMap<&str, &FingerprintFile> =
        current.files.iter().map(|f| (f.path.as_str(), f)).collect();

    let added: Vec<&str> = current
        .files
        .iter()
        .map(|f| f.path.as_str())
        .filter(|p| !pinned_map.contains_key(p))
        .collect();
    let removed: Vec<&str> = pinned
        .files
        .iter()
        .map(|f| f.path.as_str())
        .filter(|p| !current_map.contains_key(p))
        .collect();
    let changed: Vec<&str> = pinned
        .files
        .iter()
        .filter(|f| {
            current_map
                .get(f.path.as_str())
                .is_some_and(|current| current.size != f.size)
        })
        .map(|f| f.path.as_str())
        .collect();
    let crc_changed: Vec<&str> = pinned
        .files
        .iter()
        .filter(|f| {
            current_map
                .get(f.path.as_str())
                .is_some_and(|current| current.size == f.size && current.gzip_crc32 != f.gzip_crc32)
        })
        .map(|f| f.path.as_str())
        .collect();

    let mut parts = Vec::new();
    for (what, paths) in [
        ("added", &added),
        ("removed", &removed),
        ("size-changed", &changed),
        ("crc-changed", &crc_changed),
    ] {
        if !paths.is_empty() {
            let examples: Vec<&str> = paths.iter().take(3).copied().collect();
            parts.push(format!(
                "{} {what}, e.g. {}",
                paths.len(),
                examples.join(", ")
            ));
        }
    }
    if parts.is_empty() {
        // Same paths and sizes, so the serialized form itself differs.
        "fingerprint format changed".to_owned()
    } else {
        parts.join("; ")
    }
}
