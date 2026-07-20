//! Benchmark result types.

use serde::Serialize;
use std::collections::BTreeMap;

/// The top-level output document.
#[derive(Serialize, Debug)]
pub(crate) struct Output {
    pub label: String,
    pub timestamp: String,
    pub config: Config,
    pub server: Server,
    /// `null` when calibration was skipped.
    pub calibration: Option<Calibration>,
    pub results: Results,
}

/// The run configuration
#[derive(Serialize, Debug)]
pub(crate) struct Config {
    pub task: String,
    pub input: String,
    pub output: String,
    pub mode: String,
    pub concurrency: usize,
    /// `null` in single mode (present, not omitted, matching the Python tool).
    pub batch_size: Option<usize>,
    /// `null` when `--limit` was not given.
    pub limit: Option<usize>,
    pub warmup: usize,
    pub base_url: String,
    pub timeout: f64,
    pub keepalive: bool,
    pub skip_calibration: bool,
}

/// Server metadata read from response headers.
#[derive(Serialize, Debug)]
pub(crate) struct Server {
    pub version: Option<String>,
}

/// Client-side calibration against the cheap `/tasks` endpoint.
#[derive(Serialize, Debug)]
pub(crate) struct Calibration {
    pub client_ceiling_rps: Option<f64>,
    pub client_limited: bool,
}

/// Aggregated results of the timed run.
#[derive(Serialize, Debug)]
pub(crate) struct Results {
    pub records: u64,
    pub requests: u64,
    pub wall_time_s: f64,
    pub records_per_s: Option<f64>,
    pub requests_per_s: Option<f64>,
    /// `null` when there were no samples.
    pub latency_ms: Option<LatencySummary>,
    pub ok: u64,
    pub no_match: u64,
    pub error: u64,
    /// Error tallies by kind (`connect`, `timeout`, `http_503`, ...). Omitted
    /// when there were no errors.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, u64>,
    /// Present when a request contains multiple records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_input_latency_ms: Option<PerInputLatency>,
}

/// Per-request latency percentiles, in milliseconds.
#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct LatencySummary {
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

/// Per-input latency percentiles (bulk mode), in milliseconds.
#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct PerInputLatency {
    pub p50: f64,
    pub p95: f64,
}
