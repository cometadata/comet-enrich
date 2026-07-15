//! Benchmark statistics, response classification, and JSONL input loading.

// Percentile indices are non-negative, and benchmark counts fit exactly in f64.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use super::output::{LatencySummary, PerInputLatency, Results};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// What went wrong with one request or slot: a short kind label used for
/// tallying (e.g. `connect`, `timeout`, `http_503`, `body_parse`), plus an
/// optional sample message for transport-level errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ErrorDetail {
    pub kind: String,
    pub message: Option<String>,
}

impl ErrorDetail {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: None,
        }
    }

    pub fn with_message(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: Some(message.into()),
        }
    }
}

/// One request's classified outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok,
    NoMatch,
    Error(ErrorDetail),
}

/// Running tallies of per-request/per-slot outcomes.
#[derive(Debug, Clone, Default)]
pub(crate) struct Counts {
    pub ok: u64,
    pub no_match: u64,
    pub error: u64,
    /// Error tallies keyed by [`ErrorDetail::kind`].
    pub errors_by_kind: BTreeMap<String, u64>,
    /// First error message seen per kind, for the human-readable summary.
    pub error_samples: BTreeMap<String, String>,
}

impl Counts {
    /// Tally one outcome.
    pub fn record(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Ok => self.ok += 1,
            Outcome::NoMatch => self.no_match += 1,
            Outcome::Error(detail) => {
                self.error += 1;
                *self.errors_by_kind.entry(detail.kind.clone()).or_insert(0) += 1;
                if let Some(message) = detail.message {
                    self.error_samples.entry(detail.kind).or_insert(message);
                }
            }
        }
    }
}

/// One timed request: `(latency_ms, n_records)`.
pub(crate) type Sample = (f64, u64);

/// Row shape of the JSONL input corpus; only `value` is used.
#[derive(Deserialize)]
struct InputRecord {
    value: String,
}

/// Round to `digits` decimal places
pub(super) fn round_to(x: f64, digits: i32) -> f64 {
    let f = 10f64.powi(digits);
    (x * f).round() / f
}

/// Build a benchmark endpoint URL from the configured service base URL.
pub(crate) fn endpoint_url(base_url: &str, endpoint: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    )
}

/// Load values from at most `limit` non-empty JSONL rows.
pub(crate) fn read_inputs(path: &Path, limit: Option<usize>) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut values = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(lim) = limit {
            if values.len() >= lim {
                break;
            }
        }
        let rec: InputRecord = serde_json::from_str(line).context("parsing jsonl row")?;
        values.push(rec.value);
    }
    Ok(values)
}

/// Interpolate a percentile from a sorted, non-empty sample.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let k = (sorted.len() - 1) as f64 * (p / 100.0);
    let lo = k.floor();
    let hi = k.ceil();
    if (lo - hi).abs() < f64::EPSILON {
        return sorted[k as usize];
    }
    let (lo_i, hi_i) = (lo as usize, hi as usize);
    sorted[lo_i] + (sorted[hi_i] - sorted[lo_i]) * (k - lo)
}

/// Latency summary (min/p50/p90/p95/p99/max) of a latency sample, or `None` when
/// empty.
pub(crate) fn latency_summary(latencies_ms: &[f64]) -> Option<LatencySummary> {
    if latencies_ms.is_empty() {
        return None;
    }
    let mut s = latencies_ms.to_vec();
    s.sort_by(f64::total_cmp);
    Some(LatencySummary {
        min: round_to(s[0], 2),
        p50: round_to(percentile(&s, 50.0), 2),
        p90: round_to(percentile(&s, 90.0), 2),
        p95: round_to(percentile(&s, 95.0), 2),
        p99: round_to(percentile(&s, 99.0), 2),
        max: round_to(s[s.len() - 1], 2),
    })
}

/// Aggregate `(latency_ms, n_records)` samples into the results object.
pub(crate) fn compute_results(samples: &[Sample], wall_time_s: f64, counts: &Counts) -> Results {
    let n_requests = samples.len() as u64;
    let n_records: u64 = samples.iter().map(|&(_, n)| n).sum();

    let (records_per_s, requests_per_s) = if wall_time_s > 0.0 {
        (
            Some(round_to(n_records as f64 / wall_time_s, 2)),
            Some(round_to(n_requests as f64 / wall_time_s, 2)),
        )
    } else {
        (None, None)
    };

    let per_input_latency_ms = if samples.iter().any(|&(_, n)| n > 1) {
        let mut per_input: Vec<f64> = samples.iter().map(|&(lat, n)| lat / n as f64).collect();
        per_input.sort_by(f64::total_cmp);
        Some(PerInputLatency {
            p50: round_to(percentile(&per_input, 50.0), 2),
            p95: round_to(percentile(&per_input, 95.0), 2),
        })
    } else {
        None
    };

    let latencies: Vec<f64> = samples.iter().map(|&(lat, _)| lat).collect();
    Results {
        records: n_records,
        requests: n_requests,
        wall_time_s: round_to(wall_time_s, 3),
        records_per_s,
        requests_per_s,
        latency_ms: latency_summary(&latencies),
        ok: counts.ok,
        no_match: counts.no_match,
        error: counts.error,
        errors: counts.errors_by_kind.clone(),
        per_input_latency_ms,
    }
}

/// Classify one `GET /match` response as ok / no_match / error.
pub(crate) fn classify_single(status: u16, payload: Option<&Value>) -> Outcome {
    if status != 200 {
        return Outcome::Error(ErrorDetail::new(format!("http_{status}")));
    }
    let Some(payload) = payload.filter(|p| p.is_object()) else {
        return Outcome::Error(ErrorDetail::new("body_parse"));
    };
    let items = payload
        .get("message")
        .and_then(|m| m.get("items"))
        .and_then(Value::as_array);
    match items {
        Some(items) if !items.is_empty() => Outcome::Ok,
        _ => Outcome::NoMatch,
    }
}

#[cfg(test)]
mod tests {
    // Expected percentile values are exact, so strict equality is intentional.
    #![allow(clippy::float_cmp)]

    use super::*;
    use serde_json::json;
    use std::io::Write;

    #[test]
    fn endpoint_url_handles_base_url_slashes() {
        assert_eq!(
            endpoint_url("http://host:8000", "tasks"),
            "http://host:8000/tasks"
        );
        assert_eq!(
            endpoint_url("http://host:8000/", "tasks"),
            "http://host:8000/tasks"
        );
        assert_eq!(
            endpoint_url("http://host:8000/api/", "match"),
            "http://host:8000/api/match"
        );
        assert_eq!(
            endpoint_url("http://host:8000", "/match/bulk"),
            "http://host:8000/match/bulk"
        );
    }

    #[test]
    fn read_inputs_parses_values_and_limit() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            "{}\n{}\n{}\n\n",
            json!({"hash": "a", "value": "one"}),
            json!({"hash": "b", "value": "two"}),
            json!({"hash": "c", "value": "three"}),
        )
        .unwrap();
        assert_eq!(
            read_inputs(f.path(), None).unwrap(),
            ["one", "two", "three"]
        );
        assert_eq!(read_inputs(f.path(), Some(2)).unwrap(), ["one", "two"]);
        assert!(read_inputs(f.path(), Some(0)).unwrap().is_empty());
    }

    #[test]
    fn latency_summary_percentiles() {
        let latencies: Vec<f64> = (1..=100).map(f64::from).collect();
        let s = latency_summary(&latencies).unwrap();
        assert_eq!(s.min, 1.0);
        assert_eq!(s.max, 100.0);
        assert_eq!(s.p50, 50.5); // interpolated between 50 and 51
        assert_eq!(s.p90, 90.1);
        assert_eq!(s.p99, 99.01);
    }

    #[test]
    fn latency_summary_empty_is_none() {
        assert!(latency_summary(&[]).is_none());
    }

    #[test]
    fn compute_results_bulk_throughput_and_per_input() {
        let samples = [(100.0, 10), (200.0, 10)]; // two bulk requests of 10 inputs each
        let counts = Counts {
            ok: 15,
            no_match: 4,
            error: 1,
            ..Counts::default()
        };
        let r = compute_results(&samples, 2.0, &counts);
        assert_eq!(r.records, 20);
        assert_eq!(r.requests, 2);
        assert_eq!(r.records_per_s, Some(10.0));
        assert_eq!(r.requests_per_s, Some(1.0));
        assert_eq!((r.ok, r.no_match, r.error), (15, 4, 1));
        // per-input latencies are 10.0 and 20.0 -> p50 is 15.0
        assert_eq!(r.per_input_latency_ms.unwrap().p50, 15.0);
    }

    #[test]
    fn compute_results_single_mode_omits_per_input() {
        let counts = Counts {
            ok: 2,
            ..Counts::default()
        };
        let r = compute_results(&[(10.0, 1), (20.0, 1)], 1.0, &counts);
        assert_eq!((r.records, r.requests), (2, 2));
        assert!(r.per_input_latency_ms.is_none());
    }

    #[test]
    fn classify_single_ok() {
        let payload = json!({"message": {"items": [
            {"id": "https://ror.org/02mhbdp94", "confidence": 1.0, "strategies": ["x"]}
        ]}});
        assert_eq!(classify_single(200, Some(&payload)), Outcome::Ok);
    }

    #[test]
    fn classify_single_no_match() {
        assert_eq!(
            classify_single(200, Some(&json!({"message": {"items": []}}))),
            Outcome::NoMatch
        );
        assert_eq!(
            classify_single(200, Some(&json!({"message": {}}))),
            Outcome::NoMatch
        );
    }

    #[test]
    fn classify_single_error_kinds() {
        assert_eq!(
            classify_single(500, Some(&json!({"message": {"items": [{"id": "x"}]}}))),
            Outcome::Error(ErrorDetail::new("http_500"))
        );
        assert_eq!(
            classify_single(429, None),
            Outcome::Error(ErrorDetail::new("http_429"))
        );
        // 200 with an unparseable body
        assert_eq!(
            classify_single(200, None),
            Outcome::Error(ErrorDetail::new("body_parse"))
        );
        assert_eq!(
            classify_single(200, Some(&json!("not an object"))),
            Outcome::Error(ErrorDetail::new("body_parse"))
        );
    }

    #[test]
    fn counts_record_tallies_error_kinds_and_keeps_first_sample() {
        let mut c = Counts::default();
        c.record(Outcome::Ok);
        c.record(Outcome::Error(ErrorDetail::with_message(
            "connect",
            "Too many open files",
        )));
        c.record(Outcome::Error(ErrorDetail::with_message(
            "connect", "later",
        )));
        c.record(Outcome::Error(ErrorDetail::new("http_503")));

        assert_eq!((c.ok, c.no_match, c.error), (1, 0, 3));
        assert_eq!(c.errors_by_kind.get("connect"), Some(&2));
        assert_eq!(c.errors_by_kind.get("http_503"), Some(&1));
        assert_eq!(
            c.error_samples.get("connect").map(String::as_str),
            Some("Too many open files")
        );
        assert!(!c.error_samples.contains_key("http_503"));
    }

    #[test]
    fn compute_results_includes_error_kind_tallies() {
        let mut counts = Counts::default();
        counts.record(Outcome::Error(ErrorDetail::new("connect")));
        counts.record(Outcome::Error(ErrorDetail::new("connect")));
        counts.record(Outcome::Ok);

        let r = compute_results(&[(1.0, 1), (1.0, 1), (1.0, 1)], 1.0, &counts);

        assert_eq!(r.error, 2);
        assert_eq!(r.errors.get("connect"), Some(&2));
    }
}
