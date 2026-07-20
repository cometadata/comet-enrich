//! ROR match-service client.
//!
//! Sends batches to Marple's `/match/bulk` endpoint and returns one result slot
//! per input.

use crate::LookupConfig;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

/// Maximum attempts per batch (one initial request plus retries). Enough to ride
/// out a brief service blip (e.g. a rolling deploy) without retrying forever.
const MAX_ATTEMPTS: u32 = 4;
/// Upper bound on a single retry wait, so a hostile or misconfigured `Retry-After`
/// cannot stall a worker for hours.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(120);

/// Whether a non-success status is worth retrying: rate limiting, request timeout,
/// or a transient server error. Permanent client errors (400, 404, 413) are not.
fn is_retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || status.is_server_error()
}

/// The server-requested wait from a `Retry-After: <seconds>` header, if present and
/// numeric. The HTTP-date form is not parsed (callers fall back to backoff).
fn retry_after_secs(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get("Retry-After")?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
}

/// Capped exponential backoff for a 0-based retry `attempt`: `2^attempt` seconds,
/// clamped to [`MAX_RETRY_WAIT`].
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(2u64.pow(attempt)).min(MAX_RETRY_WAIT)
}

/// Borrow at most `max` chars of `s`, for including a body snippet in an error.
fn truncate(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// One successful match returned by the match service.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchHit {
    pub id: String,
    pub confidence: f64,
}

/// Error for one input in a bulk response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchError {
    pub code: String,
    pub message: String,
}

/// Result for one input in a bulk response.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchOutcome {
    Match(MatchHit),
    NoMatch,
    Error(MatchError),
}

/// ROR lookup result stored between query and reconcile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorLookup {
    pub ror_id: String,
    pub confidence: f64,
}

impl From<MatchHit> for RorLookup {
    fn from(hit: MatchHit) -> Self {
        RorLookup {
            ror_id: hit.id,
            confidence: hit.confidence,
        }
    }
}

/// Resolves batches of inputs against a match service.
#[async_trait]
pub trait MatchService: Send + Sync {
    /// Resolve one batch, returning one result per input in input order.
    ///
    /// Slots are `Match` for a match, `NoMatch` for no match, or `Error` for
    /// per-input failures. Whole-batch failures return `Err`.
    async fn match_bulk(&self, inputs: &[String], task: &str) -> Result<Vec<MatchOutcome>>;
}

/// Request body for `POST /match/bulk`.
#[derive(Serialize)]
pub struct BulkRequest<'a> {
    pub inputs: &'a [String],
}

#[derive(Deserialize)]
struct BulkResponse {
    message: BulkMessage,
}

#[derive(Deserialize)]
struct BulkMessage {
    items: Vec<BulkOuterItem>,
}

#[derive(Deserialize)]
struct BulkOuterItem {
    #[serde(default)]
    status: Option<String>,
    items: Vec<BulkInnerItem>,
    #[serde(default)]
    error: Option<BulkErrorItem>,
}

#[derive(Deserialize)]
struct BulkInnerItem {
    id: String,
    confidence: f64,
}

#[derive(Deserialize)]
struct BulkErrorItem {
    code: String,
    message: String,
}

fn outcome_from_bulk_item(item: BulkOuterItem) -> Result<MatchOutcome> {
    match item.status.as_deref().unwrap_or("ok") {
        "ok" => Ok(item
            .items
            .into_iter()
            .next()
            .map_or(MatchOutcome::NoMatch, |i| {
                MatchOutcome::Match(MatchHit {
                    id: i.id,
                    confidence: i.confidence,
                })
            })),
        "error" => Ok(MatchOutcome::Error(item.error.map_or_else(
            || MatchError {
                code: "match_service_error".to_owned(),
                message: "match service returned an item-level error".to_owned(),
            },
            |e| MatchError {
                code: e.code,
                message: e.message,
            },
        ))),
        other => Err(anyhow!("unknown bulk item status {other:?}")),
    }
}

/// Build a Marple HTTP client with a per-request timeout.
///
/// Idle connections are not pooled, which distributes requests across workers.
pub fn build_http_client(timeout: Duration) -> Result<Client> {
    Client::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(0)
        .build()
        .context("building HTTP client")
}

/// Parse a `/match/bulk` response into one [`MatchOutcome`] per expected input.
///
/// Fails on invalid JSON, an unexpected result count, or an unknown slot status.
pub fn parse_bulk_outcomes(text: &str, expected_len: usize) -> Result<Vec<MatchOutcome>> {
    let parsed: BulkResponse = serde_json::from_str(text)
        .with_context(|| format!("parsing match response (body: {})", truncate(text, 200)))?;
    if parsed.message.items.len() != expected_len {
        return Err(anyhow!(
            "bulk response length mismatch: got {} results for {} inputs",
            parsed.message.items.len(),
            expected_len
        ));
    }
    parsed
        .message
        .items
        .into_iter()
        .map(outcome_from_bulk_item)
        .collect()
}

/// The real bulk client for the Marple match service.
pub struct MarpleClient {
    client: Client,
    base: Url,
}

impl MarpleClient {
    /// Build a client against `base_url` with a per-request `timeout`.
    ///
    /// # Errors
    ///
    /// Returns an error if `base_url` is not a valid URL, or the underlying HTTP
    /// client cannot be built.
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Result<Self> {
        let base = Url::parse(&base_url.into()).context("invalid match-service URL")?;
        let client = build_http_client(timeout)?;
        Ok(Self { client, base })
    }

    /// Build a client from the lookup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn from_config(cfg: &LookupConfig) -> Result<Self> {
        Self::new(
            cfg.ror_service_url.clone(),
            Duration::from_secs(cfg.ror_timeout),
        )
    }
}

#[async_trait]
impl MatchService for MarpleClient {
    async fn match_bulk(&self, inputs: &[String], task: &str) -> Result<Vec<MatchOutcome>> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("base URL cannot be a base"))?
            .pop_if_empty()
            .extend(["match", "bulk"]);
        let body = BulkRequest { inputs };

        for attempt in 0..MAX_ATTEMPTS {
            match self
                .client
                .post(url.clone())
                .query(&[("task", task)])
                .json(&body)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let text = response.text().await?;
                        return parse_bulk_outcomes(&text, inputs.len());
                    } else if status == StatusCode::PAYLOAD_TOO_LARGE {
                        return Err(anyhow!(
                            "batch size {} exceeds the match-service batch cap (HTTP 413); reduce the per-request batch size",
                            inputs.len()
                        ));
                    } else if is_retryable(status) {
                        if attempt < MAX_ATTEMPTS - 1 {
                            // Honour a numeric `Retry-After` (still capped); otherwise
                            // fall back to exponential backoff.
                            let wait = match retry_after_secs(&response) {
                                Some(secs) => Duration::from_secs(secs).min(MAX_RETRY_WAIT),
                                None => backoff(attempt),
                            };
                            log::warn!("HTTP {status}, retrying in {}s", wait.as_secs());
                            sleep(wait).await;
                            continue;
                        }
                        return Err(anyhow!(
                            "match service returned HTTP {status} after {MAX_ATTEMPTS} attempts"
                        ));
                    }
                    // Permanent non-success status: surface the body for diagnostics.
                    let body = response.text().await.unwrap_or_default();
                    return Err(anyhow!("HTTP {status}: {body}"));
                }
                Err(e) => {
                    if attempt < MAX_ATTEMPTS - 1 {
                        let wait = backoff(attempt);
                        log::warn!("request error, retrying in {}s: {e}", wait.as_secs());
                        sleep(wait).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

        Err(anyhow!("max attempts exceeded"))
    }
}

/// A fake [`MatchService`] for tests.
///
/// In the default mode it resolves inputs from an in-memory map, returning one slot
/// per input in input order. In erroring mode it fails every batch, simulating a
/// sustained service outage.
///
/// Compiled for this crate's own tests and, behind the `test-support` feature, for
/// other crates' tests (re-exported by `comet-enrich-test-support`). It lives here
/// rather than in the test-support crate because this crate's unit tests need an
/// implementation of *their* [`MatchService`] instance, which an external crate
/// built against the regular library cannot provide.
#[cfg(any(test, feature = "test-support"))]
pub struct FakeMatchService {
    matches: std::collections::HashMap<String, (String, f64)>,
    item_errors: std::collections::HashMap<String, MatchError>,
    error: Option<String>,
}

#[cfg(any(test, feature = "test-support"))]
impl FakeMatchService {
    /// Build a fake from a map of `input -> (id, confidence)`.
    #[must_use]
    pub fn new(matches: std::collections::HashMap<String, (String, f64)>) -> Self {
        Self {
            matches,
            item_errors: std::collections::HashMap::new(),
            error: None,
        }
    }

    /// Build a fake with matches and per-input errors.
    #[must_use]
    pub fn with_item_errors(
        matches: std::collections::HashMap<String, (String, f64)>,
        item_errors: std::collections::HashMap<String, MatchError>,
    ) -> Self {
        Self {
            matches,
            item_errors,
            error: None,
        }
    }

    /// Build a fake whose every batch fails with `message`, simulating a sustained
    /// service outage.
    #[must_use]
    pub fn erroring(message: &str) -> Self {
        Self {
            matches: std::collections::HashMap::new(),
            item_errors: std::collections::HashMap::new(),
            error: Some(message.to_owned()),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl MatchService for FakeMatchService {
    async fn match_bulk(&self, inputs: &[String], _task: &str) -> Result<Vec<MatchOutcome>> {
        if let Some(msg) = &self.error {
            anyhow::bail!("{msg}");
        }
        Ok(inputs
            .iter()
            .map(|i| {
                if let Some(error) = self.item_errors.get(i) {
                    MatchOutcome::Error(error.clone())
                } else if let Some((id, confidence)) = self.matches.get(i) {
                    MatchOutcome::Match(MatchHit {
                        id: id.clone(),
                        confidence: *confidence,
                    })
                } else {
                    MatchOutcome::NoMatch
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_enrich_test_support::assert_err_contains;
    use std::collections::HashMap;

    #[tokio::test]
    async fn fake_returns_one_slot_per_input_in_order() {
        let mut map = HashMap::new();
        map.insert(
            "MIT".to_owned(),
            ("https://ror.org/042nb2s44".to_owned(), 0.99),
        );
        map.insert(
            "NSF".to_owned(),
            ("https://ror.org/021nxhr62".to_owned(), 0.97),
        );
        let svc = FakeMatchService::new(map);

        let inputs = vec!["NSF".to_owned(), "unknown".to_owned(), "MIT".to_owned()];
        let out = svc.match_bulk(&inputs, "affiliation").await.unwrap();

        assert_eq!(out.len(), 3);
        assert_eq!(
            out[0],
            MatchOutcome::Match(MatchHit {
                id: "https://ror.org/021nxhr62".to_owned(),
                confidence: 0.97
            })
        );
        assert_eq!(out[1], MatchOutcome::NoMatch);
        assert_eq!(
            out[2],
            MatchOutcome::Match(MatchHit {
                id: "https://ror.org/042nb2s44".to_owned(),
                confidence: 0.99
            })
        );
    }

    #[tokio::test]
    async fn erroring_fake_fails_every_batch() {
        let out = FakeMatchService::erroring("simulated marple outage")
            .match_bulk(&["MIT".to_owned()], "affiliation")
            .await;
        assert!(out.is_err());
    }

    #[test]
    fn parse_bulk_outcomes_maps_slots() {
        let body = serde_json::json!({"message": {"items": [
            {"status": "ok", "items": [{"id": "https://ror.org/02mhbdp94", "confidence": 0.9}]},
            {"status": "ok", "items": []},
            {"status": "error", "error": {"code": "E", "message": "boom"}, "items": []},
        ]}})
        .to_string();
        let out = parse_bulk_outcomes(&body, 3).unwrap();
        assert_eq!(
            out[0],
            MatchOutcome::Match(MatchHit {
                id: "https://ror.org/02mhbdp94".to_owned(),
                confidence: 0.9
            })
        );
        assert_eq!(out[1], MatchOutcome::NoMatch);
        assert_eq!(
            out[2],
            MatchOutcome::Error(MatchError {
                code: "E".to_owned(),
                message: "boom".to_owned()
            })
        );
    }

    #[test]
    fn parse_bulk_outcomes_rejects_length_mismatch() {
        let body = serde_json::json!({"message": {"items": []}}).to_string();
        assert_err_contains(parse_bulk_outcomes(&body, 2), "length mismatch");
    }

    #[test]
    fn parse_bulk_outcomes_rejects_unparseable_body() {
        assert_err_contains(parse_bulk_outcomes("not json", 1), "parsing match response");
    }

    #[test]
    fn parse_bulk_outcomes_rejects_unknown_status() {
        let body = serde_json::json!({"message": {"items": [{"status": "pending", "items": []}]}})
            .to_string();
        assert_err_contains(parse_bulk_outcomes(&body, 1), "unknown bulk item status");
    }

    #[test]
    fn marple_client_new_rejects_invalid_base_url() {
        assert_err_contains(
            MarpleClient::new("not a url", Duration::from_secs(1)),
            "invalid match-service URL",
        );
    }

    #[test]
    fn backoff_grows_exponentially_and_clamps() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(1), Duration::from_secs(2));
        assert_eq!(backoff(2), Duration::from_secs(4));
        // Large attempts clamp to the cap instead of growing into hour-long waits.
        assert_eq!(backoff(10), MAX_RETRY_WAIT);
        assert_eq!(backoff(31), MAX_RETRY_WAIT);
    }
}
