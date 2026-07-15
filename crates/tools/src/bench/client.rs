//! HTTP client for benchmark runs. Requests are not retried, so failures remain
//! visible in the results.

// Request counts fit exactly in f64 at benchmark scale.
#![allow(clippy::cast_precision_loss)]

use super::stats::{Counts, ErrorDetail, Outcome, Sample, classify_single, endpoint_url};

use comet_enrich_core::{BulkRequest, MatchOutcome, parse_bulk_outcomes};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::future::Future;
use std::time::Instant;

/// Number of calibration requests fired at the cheap `/tasks` endpoint.
pub(crate) const CALIBRATION_REQUESTS: usize = 500;

/// Classify a transport-level failure into a tallying kind plus the full error
/// chain as a sample message, so client-side failures (fd exhaustion, port
/// exhaustion, timeouts) are distinguishable from server errors in the output.
fn transport_detail(e: &reqwest::Error) -> ErrorDetail {
    let kind = if e.is_connect() {
        "connect"
    } else if e.is_timeout() {
        "timeout"
    } else {
        "transport"
    };
    ErrorDetail::with_message(kind, error_chain(e))
}

/// Join an error with its source chain (`a: b: c`), like anyhow's `{:#}`.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(src) = source {
        text.push_str(": ");
        text.push_str(&src.to_string());
        source = src.source();
    }
    text
}

/// Send `req` and read the full body, returning `(status, body)` or the
/// classified transport error (mirrors the Python bench treating `HTTPError`
/// as an error outcome; the bench never retries).
async fn fetch_text(req: reqwest::RequestBuilder) -> Result<(u16, String), ErrorDetail> {
    let resp = req.send().await.map_err(|e| transport_detail(&e))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(|e| transport_detail(&e))?;
    Ok((status, text))
}

/// Time through the response-body read; response classification is excluded.
async fn single_request(client: &Client, url: &str, task: &str, value: &str) -> (f64, Outcome) {
    let t0 = Instant::now();
    let fetched = fetch_text(client.get(url).query(&[("task", task), ("input", value)])).await;
    let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let outcome = match fetched {
        Ok((status, text)) => {
            let payload = serde_json::from_str::<Value>(&text).ok();
            classify_single(status, payload.as_ref())
        }
        Err(detail) => Outcome::Error(detail),
    };
    (latency_ms, outcome)
}

/// Count response-level failures against every input in the batch.
fn classify_bulk(status: u16, text: &str, n: usize) -> Vec<Outcome> {
    if status != 200 {
        return vec![Outcome::Error(ErrorDetail::new(format!("http_{status}"))); n];
    }
    match parse_bulk_outcomes(text, n) {
        Ok(outcomes) => outcomes
            .iter()
            .map(|o| match o {
                MatchOutcome::Match(_) => Outcome::Ok,
                MatchOutcome::NoMatch => Outcome::NoMatch,
                MatchOutcome::Error(_) => Outcome::Error(ErrorDetail::new("item_error")),
            })
            .collect(),
        Err(_) => vec![Outcome::Error(ErrorDetail::new("body_parse")); n],
    }
}

/// One `POST /match/bulk` request: `(latency_ms, one outcome per input)`.
async fn bulk_request(
    client: &Client,
    url: &str,
    task: &str,
    chunk: &[String],
) -> (f64, Vec<Outcome>) {
    let t0 = Instant::now();
    let body = BulkRequest { inputs: chunk };
    let fetched = fetch_text(client.post(url).query(&[("task", task)]).json(&body)).await;
    let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let outcomes = match fetched {
        Ok((status, text)) => classify_bulk(status, &text, chunk.len()),
        Err(detail) => vec![Outcome::Error(detail); chunk.len()],
    };
    (latency_ms, outcomes)
}

/// Drive request futures at fixed concurrency and fold the completed
/// `(latency_ms, outcomes)` pairs into samples and outcome tallies.
async fn run_requests<O>(
    requests: impl Iterator<Item = impl Future<Output = (f64, O)>>,
    concurrency: usize,
) -> (Vec<Sample>, Counts)
where
    O: ExactSizeIterator<Item = Outcome>,
{
    let results: Vec<(f64, O)> = stream::iter(requests)
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut samples = Vec::with_capacity(results.len());
    let mut counts = Counts::default();
    for (latency_ms, outcomes) in results {
        samples.push((latency_ms, outcomes.len() as u64));
        for outcome in outcomes {
            counts.record(outcome);
        }
    }
    (samples, counts)
}

/// `GET /match` once per value at fixed concurrency; returns samples and counts.
pub(crate) async fn run_single(
    client: &Client,
    base_url: &str,
    task: &str,
    values: &[String],
    concurrency: usize,
) -> (Vec<Sample>, Counts) {
    let url = endpoint_url(base_url, "match");
    let url = &url;
    run_requests(
        values.iter().map(|value| async move {
            let (latency_ms, outcome) = single_request(client, url, task, value).await;
            (latency_ms, std::iter::once(outcome))
        }),
        concurrency,
    )
    .await
}

/// `POST /match/bulk` per `batch_size` chunk at fixed concurrency.
pub(crate) async fn run_bulk(
    client: &Client,
    base_url: &str,
    task: &str,
    values: &[String],
    concurrency: usize,
    batch_size: usize,
) -> (Vec<Sample>, Counts) {
    let url = endpoint_url(base_url, "match/bulk");
    let url = &url;
    run_requests(
        values.chunks(batch_size.max(1)).map(|chunk| async move {
            let (latency_ms, outcomes) = bulk_request(client, url, task, chunk).await;
            (latency_ms, outcomes.into_iter())
        }),
        concurrency,
    )
    .await
}

/// Measure request throughput against `/tasks` and return its service version.
pub(crate) async fn calibrate(
    client: &Client,
    base_url: &str,
    concurrency: usize,
    n_requests: usize,
) -> (Option<f64>, Option<String>) {
    let url = endpoint_url(base_url, "tasks");
    let url = &url;
    let t0 = Instant::now();
    let versions: Vec<Option<String>> = stream::iter(0..n_requests)
        .map(|_| async move {
            match client.get(url).send().await {
                Ok(resp) => resp
                    .headers()
                    .get("x-service-version")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned),
                Err(_) => None,
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let wall = t0.elapsed().as_secs_f64();

    let rps = if wall > 0.0 {
        Some(n_requests as f64 / wall)
    } else {
        None
    };
    let version = versions.into_iter().flatten().next();
    (rps, version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_core::build_http_client;
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn owned(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    #[tokio::test]
    async fn run_single_classifies_by_response() {
        let server = MockServer::start().await;
        let ok_body = json!({"message": {"items": [{"id": "x", "confidence": 1.0}]}});
        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "hit"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&ok_body))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "miss"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"message": {"items": []}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "boom"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let values = owned(&["hit", "miss", "boom"]);
        let (samples, counts) = run_single(&client, &server.uri(), "funder", &values, 4).await;

        assert_eq!(samples.len(), 3);
        assert!(samples.iter().all(|&(_, n)| n == 1));
        assert_eq!((counts.ok, counts.no_match, counts.error), (1, 1, 1));
        assert_eq!(counts.errors_by_kind.get("http_500"), Some(&1));
    }

    #[tokio::test]
    async fn run_single_records_connect_errors_with_sample_message() {
        // Pick a local port with no listener.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let values = owned(&["x"]);
        let (_samples, counts) = run_single(&client, &base_url, "funder", &values, 1).await;

        assert_eq!(counts.error, 1);
        assert_eq!(counts.errors_by_kind.get("connect"), Some(&1));
        let sample = counts.error_samples.get("connect").unwrap();
        assert!(
            sample.to_lowercase().contains("connect"),
            "sample message should describe the connect failure: {sample}"
        );
    }

    #[tokio::test]
    async fn run_bulk_classifies_slots_and_records_per_input() {
        let server = MockServer::start().await;
        // One request carries the whole batch; the server returns one slot per input.
        let body = json!({"message": {"items": [
            {"status": "ok", "items": [{"id": "x", "confidence": 1.0}]},
            {"status": "ok", "items": []},
        ]}});
        Mock::given(method("POST"))
            .and(path("/match/bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&server)
            .await;

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let values = owned(&["a", "b"]);
        let (samples, counts) = run_bulk(&client, &server.uri(), "funder", &values, 4, 50).await;

        assert_eq!(samples, vec![(samples[0].0, 2)]); // one request, two records
        assert_eq!((counts.ok, counts.no_match, counts.error), (1, 1, 0));
    }

    #[tokio::test]
    async fn run_bulk_whole_batch_failure_counts_every_input() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/match/bulk"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let values = owned(&["a", "b", "c"]);
        let (_samples, counts) = run_bulk(&client, &server.uri(), "funder", &values, 4, 50).await;

        assert_eq!((counts.ok, counts.no_match, counts.error), (0, 0, 3));
        assert_eq!(counts.errors_by_kind.get("http_500"), Some(&3));
    }
}
