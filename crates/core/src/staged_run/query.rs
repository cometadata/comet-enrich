use super::{INPUTS_FILE, LOOKUPS_FAILED_FILE, LOOKUPS_FILE, LookupConfig, for_each_jsonl};
use crate::fanout::progress_bar;
use crate::match_service::{MatchHit, MatchOutcome, MatchService};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex as AsyncMutex;

/// One `inputs.jsonl` row.
#[derive(Deserialize)]
struct InputRecord {
    hash: String,
    value: String,
}

/// One `lookups.jsonl` row.
#[derive(Serialize, Deserialize)]
pub(super) struct LookupRow<L> {
    value: String,
    pub(super) hash: String,
    #[serde(flatten)]
    pub(super) lookup: L,
}

/// Failed lookup kind: the service answered and found nothing.
pub(super) const FAIL_KIND_NO_MATCH: &str = "no_match";
/// Failed lookup kind: the input was never resolved.
const FAIL_KIND_ERROR: &str = "error";

/// One `lookups.failed.jsonl` row.
#[derive(Serialize)]
struct FailedRow<'a> {
    value: &'a str,
    hash: &'a str,
    kind: &'static str,
    error: &'a str,
}
/// Resolve inputs and write lookup result files.
pub(super) fn run_query<L>(
    svc: Arc<dyn MatchService>,
    cfg: &LookupConfig,
    work: &Path,
    task: &str,
) -> Result<()>
where
    L: Serialize + From<MatchHit> + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    rt.block_on(query_async::<L>(svc, cfg, work, task))
}

async fn query_async<L>(
    svc: Arc<dyn MatchService>,
    cfg: &LookupConfig,
    work: &Path,
    task: &str,
) -> Result<()>
where
    L: Serialize + From<MatchHit> + Send + 'static,
{
    let inputs = read_inputs(&work.join(INPUTS_FILE))?;

    // Query reruns as a whole stage, so previous result files are rewritten.
    let matches_w = Arc::new(AsyncMutex::new(create_line_writer(
        &work.join(LOOKUPS_FILE),
    )?));
    let failed_w = Arc::new(AsyncMutex::new(create_line_writer(
        &work.join(LOOKUPS_FAILED_FILE),
    )?));

    if inputs.is_empty() {
        log::info!("query: nothing to resolve");
        matches_w.lock().await.flush()?;
        failed_w.lock().await.flush()?;
        return Ok(());
    }
    log::info!("query: {} inputs to resolve", inputs.len());
    let pb = progress_bar(inputs.len() as u64)?;
    pb.set_message("query");

    let task = task.to_owned();

    // Use bounded workers; each worker claims the next batch from the shared input list.
    let inputs = Arc::new(inputs);
    let batch_size = cfg.ror_batch_size.max(1);
    let n_batches = inputs.len().div_ceil(batch_size);
    let workers = cfg.ror_concurrency.max(1).min(n_batches);
    let next_batch = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let svc = Arc::clone(&svc);
        let matches_w = Arc::clone(&matches_w);
        let failed_w = Arc::clone(&failed_w);
        let inputs = Arc::clone(&inputs);
        let next_batch = Arc::clone(&next_batch);
        let task = task.clone();
        let pb = pb.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let start = next_batch
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_mul(batch_size);
                if start >= inputs.len() {
                    break;
                }
                let batch = &inputs[start..(start + batch_size).min(inputs.len())];
                let values: Vec<String> = batch.iter().map(|r| r.value.clone()).collect();

                match svc.match_bulk(&values, &task).await {
                    Ok(results) => {
                        let (hits, misses) = serialize_results::<L>(batch, results)?;
                        write_lines(&matches_w, &hits).await?;
                        write_lines(&failed_w, &misses).await?;
                    }
                    Err(e) => {
                        // Whole-batch failures are lost inputs.
                        let error = format!("batch error: {e}");
                        let lines: Vec<String> = batch
                            .iter()
                            .map(|rec| {
                                serde_json::to_string(&FailedRow {
                                    value: &rec.value,
                                    hash: &rec.hash,
                                    kind: FAIL_KIND_ERROR,
                                    error: &error,
                                })
                            })
                            .collect::<Result<_, _>>()?;
                        write_lines(&failed_w, &lines).await?;
                    }
                }
                pb.inc(batch.len() as u64);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await.context("query task panicked")??;
    }
    pb.finish_with_message("query: done");

    matches_w.lock().await.flush()?;
    failed_w.lock().await.flush()?;
    Ok(())
}

fn serialize_results<L>(
    batch: &[InputRecord],
    results: Vec<MatchOutcome>,
) -> Result<(Vec<String>, Vec<String>)>
where
    L: Serialize + From<MatchHit>,
{
    let mut hits = Vec::new();
    let mut misses = Vec::new();

    for (rec, result) in batch.iter().zip(results) {
        match result {
            MatchOutcome::Match(hit) => {
                let row = LookupRow {
                    value: rec.value.clone(),
                    hash: rec.hash.clone(),
                    lookup: L::from(hit),
                };
                hits.push(serde_json::to_string(&row)?);
            }
            MatchOutcome::NoMatch => {
                misses.push(serde_json::to_string(&FailedRow {
                    value: &rec.value,
                    hash: &rec.hash,
                    kind: FAIL_KIND_NO_MATCH,
                    error: "no match",
                })?);
            }
            MatchOutcome::Error(error) => {
                let message = format!("{}: {}", error.code, error.message);
                misses.push(serde_json::to_string(&FailedRow {
                    value: &rec.value,
                    hash: &rec.hash,
                    kind: FAIL_KIND_ERROR,
                    error: &message,
                })?);
            }
        }
    }

    Ok((hits, misses))
}

fn read_inputs(path: &Path) -> Result<Vec<InputRecord>> {
    let mut rows = Vec::new();
    for_each_jsonl(path, |row: InputRecord| rows.push(row))?;
    Ok(rows)
}

/// Create a JSONL writer, truncating any prior file.
fn create_line_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    Ok(BufWriter::new(file))
}

async fn write_lines(writer: &AsyncMutex<BufWriter<File>>, lines: &[String]) -> Result<()> {
    if lines.is_empty() {
        return Ok(());
    }
    let mut w = writer.lock().await;
    for line in lines {
        w.write_all(line.as_bytes())?;
        w.write_all(b"\n")?;
    }
    Ok(())
}
