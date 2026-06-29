//! The ROR match service behind a trait, with a real bulk HTTP client and a fake.
//!
//! Lookup methods resolve unique inputs (funder names, affiliation strings) against
//! a "Marple" match service. [`MatchService`] abstracts that call so the staged
//! runner can be driven by a [`FakeMatchService`] in tests while production uses
//! [`MarpleClient`] against the real bulk endpoint.
//!
//! The client is ported from the prototype query stage. The contract: `match_bulk`
//! returns one slot per input **in input order** — `Some((id, confidence))` for a
//! match (first candidate wins) or `None` for no match; a whole-batch failure is an
//! `Err`.

use crate::LookupConfig;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

/// Maximum attempts per batch (one initial request plus retries). Enough to ride
/// out a brief service blip (e.g. a rolling deploy) without retrying forever.
const MAX_RETRIES: u32 = 4;
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

/// Borrow at most `max` chars of `s`, for including a body snippet in an error.
fn truncate(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// Resolves batches of inputs against a match service.
#[async_trait]
pub trait MatchService: Send + Sync {
    /// Resolve one batch. Returns one slot per input, in input order:
    /// `Some((id, confidence))` on a match (first candidate wins), `None` for no
    /// match. A whole-batch failure returns `Err`.
    ///
    /// Results are matched to inputs **positionally**: the implementation validates
    /// only the result count and trusts the service to return results in input order.
    async fn match_bulk(&self, inputs: &[String], task: &str)
    -> Result<Vec<Option<(String, f64)>>>;
}

#[derive(Serialize)]
struct BulkRequest<'a> {
    inputs: &'a [String],
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
    items: Vec<BulkInnerItem>,
}

#[derive(Deserialize)]
struct BulkInnerItem {
    id: String,
    confidence: f64,
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
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("building HTTP client")?;
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
    async fn match_bulk(
        &self,
        inputs: &[String],
        task: &str,
    ) -> Result<Vec<Option<(String, f64)>>> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("base URL cannot be a base"))?
            .pop_if_empty()
            .extend(["match", "bulk"]);
        let body = BulkRequest { inputs };

        for attempt in 0..MAX_RETRIES {
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
                        let parsed: BulkResponse = serde_json::from_str(&text).with_context(|| {
                            format!(
                                "parsing match response (status {status}, body: {})",
                                truncate(&text, 200)
                            )
                        })?;
                        if parsed.message.items.len() != inputs.len() {
                            return Err(anyhow!(
                                "bulk response length mismatch: got {} results for {} inputs",
                                parsed.message.items.len(),
                                inputs.len()
                            ));
                        }
                        return Ok(parsed
                            .message
                            .items
                            .into_iter()
                            .map(|outer| outer.items.into_iter().next().map(|i| (i.id, i.confidence)))
                            .collect());
                    } else if status == StatusCode::PAYLOAD_TOO_LARGE {
                        // Domain-phrased; the CLI layer owns the batch-size flag name.
                        return Err(anyhow!(
                            "batch size {} exceeds the match-service batch cap (HTTP 413); reduce the per-request batch size",
                            inputs.len()
                        ));
                    } else if is_retryable(status) {
                        if attempt < MAX_RETRIES - 1 {
                            let secs =
                                retry_after_secs(&response).unwrap_or(2u64.pow(attempt));
                            let wait = Duration::from_secs(secs).min(MAX_RETRY_WAIT);
                            log::warn!("HTTP {status}, retrying in {}s", wait.as_secs());
                            sleep(wait).await;
                            continue;
                        }
                        return Err(anyhow!(
                            "match service returned HTTP {status} after {MAX_RETRIES} attempts"
                        ));
                    }
                    // Permanent non-success status: surface the body for diagnostics.
                    let body = response.text().await.unwrap_or_default();
                    return Err(anyhow!("HTTP {status}: {body}"));
                }
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        let wait = Duration::from_secs(2u64.pow(attempt)).min(MAX_RETRY_WAIT);
                        log::warn!("request error, retrying in {}s: {e}", wait.as_secs());
                        sleep(wait).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

        Err(anyhow!("max retries exceeded"))
    }
}

/// A fake match service for tests: resolves inputs from an in-memory map.
#[cfg(any(test, feature = "test-support"))]
pub struct FakeMatchService {
    matches: std::collections::HashMap<String, (String, f64)>,
}

#[cfg(any(test, feature = "test-support"))]
impl FakeMatchService {
    /// Build a fake from a map of `input -> (id, confidence)`.
    #[must_use]
    pub fn new(matches: std::collections::HashMap<String, (String, f64)>) -> Self {
        Self { matches }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl MatchService for FakeMatchService {
    async fn match_bulk(
        &self,
        inputs: &[String],
        _task: &str,
    ) -> Result<Vec<Option<(String, f64)>>> {
        Ok(inputs.iter().map(|i| self.matches.get(i).cloned()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn fake_returns_one_slot_per_input_in_order() {
        let mut map = HashMap::new();
        map.insert("MIT".to_owned(), ("https://ror.org/042nb2s44".to_owned(), 0.99));
        map.insert("NSF".to_owned(), ("https://ror.org/021nxhr62".to_owned(), 0.97));
        let svc = FakeMatchService::new(map);

        let inputs = vec!["NSF".to_owned(), "unknown".to_owned(), "MIT".to_owned()];
        let out = svc.match_bulk(&inputs, "affiliation").await.unwrap();

        assert_eq!(out.len(), 3);
        assert_eq!(out[0], Some(("https://ror.org/021nxhr62".to_owned(), 0.97)));
        assert_eq!(out[1], None);
        assert_eq!(out[2], Some(("https://ror.org/042nb2s44".to_owned(), 0.99)));
    }
}
