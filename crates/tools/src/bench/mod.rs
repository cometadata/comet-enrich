//! Throughput and latency benchmark for Marple match endpoints.

// Marple, ROR, and JSONL are names, not Rust identifiers.
#![allow(clippy::doc_markdown)]

mod client;
mod output;
mod stats;

use client::{CALIBRATION_REQUESTS, calibrate, run_bulk, run_single};
use output::{Calibration, Config, Output, Server};
use stats::{Counts, Sample, compute_results, read_inputs, round_to};

use anyhow::{Context, Result, bail};
use comet_enrich_core::build_http_client;
use reqwest::Client;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    /// One input per `GET /match` request.
    Single,
    /// `--batch-size` inputs per `POST /match/bulk` request.
    Bulk,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Single => "single",
            Mode::Bulk => "bulk",
        }
    }
}

fn parse_timeout_seconds(raw: &str) -> Result<f64, String> {
    let seconds = raw
        .parse::<f64>()
        .map_err(|err| format!("invalid timeout seconds: {err}"))?;
    if !seconds.is_finite() {
        return Err("timeout must be finite seconds".to_owned());
    }
    if seconds <= 0.0 {
        return Err("timeout must be greater than 0 seconds".to_owned());
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| "timeout is too large".to_owned())?;
    Ok(seconds)
}

/// Benchmark a Marple match endpoint (throughput and latency).
#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// Marple task name. The `/tasks` endpoint lists available tasks.
    #[arg(long, value_name = "TASK")]
    task: String,

    /// JSONL file with `{"hash": ..., "value": ...}` per line.
    #[arg(long, value_name = "PATH")]
    input: PathBuf,

    /// Result JSON path.
    #[arg(long, value_name = "PATH")]
    output: PathBuf,

    /// Request mode.
    #[arg(long, default_value = "single")]
    mode: Mode,

    /// Maximum in-flight requests.
    #[arg(long, short = 'c', default_value = "50")]
    concurrency: NonZeroUsize,

    /// Inputs per bulk request; server cap is `MARPLE_MAX_BATCH_SIZE`.
    #[arg(long, default_value = "50")]
    batch_size: NonZeroUsize,

    /// Read at most N inputs.
    #[arg(long)]
    limit: Option<NonZeroUsize>,

    /// Number of untimed warmup inputs.
    #[arg(long, default_value_t = 100)]
    warmup: usize,

    /// Label stored in the result JSON.
    #[arg(long, default_value = "")]
    label: String,

    /// Base URL of the Marple service.
    #[arg(long, default_value = "http://localhost:8000")]
    base_url: String,

    /// Request timeout in seconds.
    #[arg(long, default_value_t = 30.0, value_parser = parse_timeout_seconds)]
    timeout: f64,

    /// Reuse HTTP connections. This reduces ephemeral-port use but may
    /// concentrate requests on fewer Marple workers.
    #[arg(long)]
    keepalive: bool,

    /// Skip the client-ceiling calibration phase.
    #[arg(long)]
    skip_calibration: bool,
}

/// File descriptors reserved for process overhead.
const FD_HEADROOM: u64 = 64;

fn fd_raise_target(soft: u64, hard: u64, needed: u64) -> Option<u64> {
    (soft < needed).then(|| needed.min(hard))
}

/// Ensure the process can open one socket per concurrent request.
fn ensure_fd_capacity(concurrency: usize) -> Result<()> {
    let needed = concurrency as u64 + FD_HEADROOM;
    let (soft, hard) = rlimit::Resource::NOFILE
        .get()
        .context("reading RLIMIT_NOFILE")?;
    if let Some(target) = fd_raise_target(soft, hard, needed) {
        rlimit::Resource::NOFILE
            .set(target, hard)
            .with_context(|| format!("raising open-file soft limit from {soft} to {target}"))?;
        if target < needed {
            bail!(
                "--concurrency {concurrency} needs ~{needed} open files but the hard limit \
                 is {hard}; lower --concurrency or raise the limit (ulimit -n)"
            );
        }
        eprintln!("note: raised open-file soft limit from {soft} to {target}");
    }
    Ok(())
}

/// Return true when at least 20% of records errored.
fn high_error_fraction(error: u64, records: u64) -> bool {
    records > 0 && error * 5 >= records
}

const CLIENT_CEILING_FACTOR: f64 = 2.0;

fn fmt_opt(v: Option<f64>) -> String {
    v.map_or_else(|| "None".to_owned(), |x| x.to_string())
}

fn client_limited(ceiling: Option<f64>, rps: Option<f64>) -> bool {
    matches!(
        (ceiling, rps),
        (Some(c), Some(r)) if c > 0.0 && r > 0.0 && r * CLIENT_CEILING_FACTOR >= c
    )
}

fn now_rfc3339() -> Result<String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .context("formatting timestamp")
}

async fn run_phase(client: &Client, args: &Args, values: &[String]) -> (Vec<Sample>, Counts) {
    let task = args.task.as_str();
    let concurrency = args.concurrency.get();
    match args.mode {
        Mode::Single => run_single(client, &args.base_url, task, values, concurrency).await,
        Mode::Bulk => {
            run_bulk(
                client,
                &args.base_url,
                task,
                values,
                concurrency,
                args.batch_size.get(),
            )
            .await
        }
    }
}

pub(crate) async fn run(args: Args) -> Result<()> {
    let limit = args.limit.map(NonZeroUsize::get);
    let values = read_inputs(&args.input, limit)?;
    if values.len() <= args.warmup {
        bail!(
            "Input has {} values after --limit; need more than --warmup={}",
            values.len(),
            args.warmup
        );
    }
    let (warmup_values, timed_values) = values.split_at(args.warmup);

    ensure_fd_capacity(args.concurrency.get())?;
    let timeout = Duration::from_secs_f64(args.timeout);
    let client = if args.keepalive {
        Client::builder()
            .timeout(timeout)
            .build()
            .context("building HTTP client")?
    } else {
        build_http_client(timeout)?
    };

    let mut calibration = None;
    let mut server_version = None;
    if !args.skip_calibration {
        eprintln!(
            "calibration: {CALIBRATION_REQUESTS} requests to /tasks at concurrency {} ...",
            args.concurrency
        );
        let (ceiling_rps, version) = calibrate(
            &client,
            &args.base_url,
            args.concurrency.get(),
            CALIBRATION_REQUESTS,
        )
        .await;
        server_version = version;
        calibration = Some(Calibration {
            client_ceiling_rps: ceiling_rps.map(|x| round_to(x, 1)),
            client_limited: false,
        });
    }

    if !warmup_values.is_empty() {
        eprintln!("warmup: {} inputs ...", warmup_values.len());
        let _ = run_phase(&client, &args, warmup_values).await;
    }

    eprintln!(
        "timed run: {} inputs, mode={}, concurrency={} ...",
        timed_values.len(),
        args.mode.as_str(),
        args.concurrency
    );
    let t0 = Instant::now();
    let (samples, counts) = run_phase(&client, &args, timed_values).await;
    let wall_time_s = t0.elapsed().as_secs_f64();

    let results = compute_results(&samples, wall_time_s, &counts);
    if let Some(cal) = calibration.as_mut() {
        cal.client_limited = client_limited(cal.client_ceiling_rps, results.requests_per_s);
    }
    for (kind, message) in &counts.error_samples {
        eprintln!("first {kind} error: {message}");
    }

    let doc = Output {
        label: args.label.clone(),
        timestamp: now_rfc3339()?,
        config: Config {
            task: args.task.clone(),
            input: args.input.display().to_string(),
            output: args.output.display().to_string(),
            mode: args.mode.as_str().to_owned(),
            concurrency: args.concurrency.get(),
            batch_size: (args.mode == Mode::Bulk).then_some(args.batch_size.get()),
            limit,
            warmup: args.warmup,
            base_url: args.base_url.clone(),
            timeout: args.timeout,
            keepalive: args.keepalive,
            skip_calibration: args.skip_calibration,
        },
        server: Server {
            version: server_version,
        },
        calibration,
        results,
    };

    write_output(&args.output, &doc)?;
    print_summary(&doc, &args.output);
    Ok(())
}

/// Write the result document as pretty JSON, creating the parent directory.
fn write_output(path: &std::path::Path, doc: &Output) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
    }
    let json = serde_json::to_string_pretty(doc).context("serialising results")?;
    fs::write(path, json).with_context(|| format!("writing {}", path.display()))
}

/// Print the one-line summary
fn print_summary(doc: &Output, output_path: &std::path::Path) {
    let r = &doc.results;
    let lat = r.latency_ms.as_ref();
    println!(
        "{} records in {}s -> {} records/s, {} req/s (p50 {}ms, p95 {}ms, p99 {}ms); \
         ok={} no_match={} error={}",
        r.records,
        r.wall_time_s,
        fmt_opt(r.records_per_s),
        fmt_opt(r.requests_per_s),
        fmt_opt(lat.map(|l| l.p50)),
        fmt_opt(lat.map(|l| l.p95)),
        fmt_opt(lat.map(|l| l.p99)),
        r.ok,
        r.no_match,
        r.error,
    );
    if !r.errors.is_empty() {
        let kinds: Vec<String> = r.errors.iter().map(|(k, n)| format!("{k}={n}")).collect();
        println!("errors by kind: {}", kinds.join(" "));
    }
    if high_error_fraction(r.error, r.records) {
        eprintln!(
            "WARNING: {}/{} records errored - the throughput and latency figures mostly \
             measure failed requests, not the server. See the error breakdown above.",
            r.error, r.records
        );
    }
    if let Some(cal) = &doc.calibration {
        if cal.client_limited {
            eprintln!(
                "WARNING: throughput is within {CLIENT_CEILING_FACTOR}x of the client's own \
                 ceiling ({} req/s) - the client, not the server, may be the bottleneck. \
                 Confirm with a faster client.",
                fmt_opt(cal.client_ceiling_rps)
            );
        }
    }
    println!("results written to {}", output_path.display());
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(flatten)]
        args: Args,
    }

    fn parse_args(extra: &[&str]) -> Result<TestCli, clap::Error> {
        let mut args = vec![
            "test",
            "--task",
            "funder",
            "--input",
            "input.jsonl",
            "--output",
            "out.json",
        ];
        args.extend_from_slice(extra);
        TestCli::try_parse_from(args)
    }

    #[test]
    fn timeout_parser_accepts_positive_finite_seconds() {
        let cli = parse_args(&["--timeout=1.5"]).unwrap();

        assert!((cli.args.timeout - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn timeout_parser_rejects_values_that_duration_cannot_represent() {
        for value in ["-1", "0", "NaN", "inf", "1e20"] {
            assert!(parse_args(&[&format!("--timeout={value}")]).is_err());
        }
    }

    #[test]
    fn limit_parser_rejects_zero() {
        assert!(parse_args(&["--limit", "0"]).is_err());
    }

    #[test]
    fn keepalive_defaults_off() {
        assert!(!parse_args(&[]).unwrap().args.keepalive);
        assert!(parse_args(&["--keepalive"]).unwrap().args.keepalive);
    }

    #[test]
    fn fd_raise_target_covers_needed_within_hard_limit() {
        // Enough already: no change.
        assert_eq!(fd_raise_target(65536, 1_048_576, 1314), None);
        assert_eq!(fd_raise_target(1314, 1_048_576, 1314), None);
        // The 1024-fd default shell with concurrency 1250: raise to needed.
        assert_eq!(fd_raise_target(1024, 524_288, 1314), Some(1314));
        // Hard limit too low: raise as far as allowed (caller then bails).
        assert_eq!(fd_raise_target(1024, 1100, 1314), Some(1100));
    }

    #[test]
    fn high_error_fraction_at_twenty_percent() {
        assert!(!high_error_fraction(0, 0));
        assert!(!high_error_fraction(0, 100));
        assert!(!high_error_fraction(19, 100));
        assert!(high_error_fraction(20, 100));
        assert!(high_error_fraction(98881, 99900));
    }

    #[test]
    fn limit_parser_accepts_nonzero_values() {
        let cli = parse_args(&["--limit", "1"]).unwrap();

        assert_eq!(cli.args.limit.map(NonZeroUsize::get), Some(1));
    }
}
