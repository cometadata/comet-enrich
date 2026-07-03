//! Wiremock tests for `MarpleClient`.

use comet_enrich_core::{MarpleClient, MatchService};
use std::time::Duration;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(uri: String) -> MarpleClient {
    MarpleClient::new(uri, Duration::from_secs(30)).unwrap()
}

#[tokio::test]
async fn match_bulk_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .and(query_param("task", "affiliation"))
        .and(body_json(serde_json::json!({ "inputs": ["University of Oxford"] })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [
                { "items": [
                    { "id": "https://ror.org/052gg0110", "confidence": 0.92, "strategies": ["affiliation-single-search"] }
                ], "target_data": "ROR v1.67", "strategy": "affiliation-single-search" }
            ] }
        })))
        .mount(&server)
        .await;

    let out = client(server.uri())
        .match_bulk(&["University of Oxford".to_owned()], "affiliation")
        .await
        .unwrap();
    assert_eq!(
        out,
        vec![Some(("https://ror.org/052gg0110".to_owned(), 0.92))]
    );
}

#[tokio::test]
async fn match_bulk_normalizes_trailing_slash_base_url() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk")) // must be a single slash, not //match/bulk
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [ { "items": [] } ] }
        })))
        .mount(&server)
        .await;

    // Base URL with a trailing slash must still resolve to /match/bulk.
    let client = client(format!("{}/", server.uri()));
    let out = client
        .match_bulk(&["x".to_owned()], "affiliation")
        .await
        .unwrap();
    assert_eq!(out, vec![None]);
}

#[tokio::test]
async fn match_bulk_no_match_returns_none_per_slot() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [ { "items": [] } ] }
        })))
        .mount(&server)
        .await;

    let out = client(server.uri())
        .match_bulk(&["Unknown Institution".to_owned()], "affiliation")
        .await
        .unwrap();
    assert_eq!(out, vec![None]);
}

#[tokio::test]
async fn match_bulk_preserves_order() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [
                { "items": [ { "id": "https://ror.org/aaaaaaaaa", "confidence": 0.91 } ] },
                { "items": [] }
            ] }
        })))
        .mount(&server)
        .await;

    let out = client(server.uri())
        .match_bulk(
            &["Matched Org".to_owned(), "Unknown".to_owned()],
            "affiliation",
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        vec![Some(("https://ror.org/aaaaaaaaa".to_owned(), 0.91)), None]
    );
}

#[tokio::test]
async fn match_bulk_413_does_not_retry() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(413).set_body_string("too big"))
        .expect(1) // verified on drop: a retry would make this fail
        .mount(&server)
        .await;

    let inputs: Vec<String> = (0..200).map(|_| "x".to_owned()).collect();
    let result = client(server.uri())
        .match_bulk(&inputs, "affiliation")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn match_bulk_length_mismatch_is_error() {
    let server = MockServer::start().await;
    // One input, but zero result items -> length mismatch.
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [] }
        })))
        .mount(&server)
        .await;

    let result = client(server.uri())
        .match_bulk(&["One Input".to_owned()], "affiliation")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn match_bulk_parse_error_includes_truncated_body() {
    let server = MockServer::start().await;
    let body = "x".repeat(300);
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let err = client(server.uri())
        .match_bulk(&["One Input".to_owned()], "affiliation")
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("parsing match response"));
    assert!(err.contains(&"x".repeat(200)));
    assert!(!err.contains(&"x".repeat(250)));
}

#[tokio::test]
async fn match_bulk_retries_after_429() {
    let server = MockServer::start().await;
    // First call: 429 with Retry-After: 0 (immediate retry), consumed after one hit.
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    // Second call: success.
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [
                { "items": [ { "id": "https://ror.org/052gg0110", "confidence": 0.88 } ] }
            ] }
        })))
        .mount(&server)
        .await;

    let out = client(server.uri())
        .match_bulk(&["University of Oxford".to_owned()], "affiliation")
        .await
        .unwrap();
    assert_eq!(
        out,
        vec![Some(("https://ror.org/052gg0110".to_owned(), 0.88))]
    );
}

#[tokio::test]
async fn match_bulk_waits_for_numeric_retry_after() {
    let server = MockServer::start().await;
    // First call: 429 asking for a one-second wait, consumed after one hit.
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "1"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [ { "items": [] } ] }
        })))
        .mount(&server)
        .await;

    let started = std::time::Instant::now();
    let out = client(server.uri())
        .match_bulk(&["x".to_owned()], "affiliation")
        .await
        .unwrap();

    assert_eq!(out, vec![None]);
    // The client must wait at least the server-requested second before retrying.
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "retried after only {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn match_bulk_retries_after_503() {
    let server = MockServer::start().await;
    // First call: 503 (consumed after one hit), then success.
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [ { "items": [] } ] }
        })))
        .mount(&server)
        .await;

    let out = client(server.uri())
        .match_bulk(&["x".to_owned()], "affiliation")
        .await
        .unwrap();
    assert_eq!(out, vec![None]);
}

#[tokio::test]
async fn match_bulk_exhausts_retries_on_persistent_5xx() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(500).insert_header("Retry-After", "0"))
        .mount(&server)
        .await;

    let err = client(server.uri())
        .match_bulk(&["x".to_owned()], "affiliation")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("500"), "error should name the status: {err}");
}
