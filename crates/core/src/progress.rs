//! Phase progress for whole-corpus passes: a bar on a terminal, periodic log lines otherwise.

use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::{self, IsTerminal};
use std::path::Path;
use std::time::{Duration, Instant};

/// Records between clock checks; keeps the per-record cost to one increment.
const REFRESH_EVERY: u64 = 1 << 16;

pub struct Progress {
    bar: ProgressBar,
    phase: &'static str,
    files: Option<u64>,
    completed: u64,
    records: u64,
    filename: String,
    started: Instant,
    updated: Instant,
    interactive: bool,
}

impl Progress {
    /// Start a phase over `files` inputs, or a spinner when the count is unknown.
    ///
    /// # Errors
    ///
    /// Returns an error if the progress template fails to parse.
    pub fn new(phase: &'static str, files: Option<u64>) -> Result<Self> {
        let interactive = io::stderr().is_terminal() && log::log_enabled!(log::Level::Info);
        let target = if interactive {
            ProgressDrawTarget::stderr()
        } else {
            ProgressDrawTarget::hidden()
        };
        let bar = ProgressBar::with_draw_target(files, target);
        let template = if files.is_some() {
            "[{elapsed_precise}] {spinner} [{bar:40.cyan/blue}] {pos}/{len} files {prefix} {msg}"
        } else {
            "[{elapsed_precise}] {spinner} {prefix}"
        };
        bar.set_style(ProgressStyle::with_template(template)?.progress_chars("#>-"));
        bar.set_prefix(phase);
        if interactive {
            bar.enable_steady_tick(Duration::from_millis(100));
        } else {
            log::info!("{phase}");
        }
        let now = Instant::now();
        Ok(Self {
            bar,
            phase,
            files,
            completed: 0,
            records: 0,
            filename: String::new(),
            started: now,
            updated: now,
            interactive,
        })
    }

    pub fn file_started(&mut self, path: &Path) {
        self.filename = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if self.interactive {
            self.refresh();
        }
    }

    pub fn record_processed(&mut self) {
        self.records += 1;
        if self.records % REFRESH_EVERY != 0 {
            return;
        }
        self.refresh_if_due();
    }

    /// Count a batch of records, such as one file's total.
    pub fn records_processed(&mut self, count: u64) {
        self.records += count;
        self.refresh_if_due();
    }

    fn refresh_if_due(&mut self) {
        let interval = if self.interactive {
            Duration::from_millis(250)
        } else {
            Duration::from_secs(5)
        };
        if log::log_enabled!(log::Level::Info) && self.updated.elapsed() >= interval {
            self.refresh();
        }
    }

    pub fn file_finished(&mut self) {
        self.completed += 1;
        self.bar.set_position(self.completed);
        if self.interactive {
            self.refresh();
        }
    }

    #[allow(clippy::cast_precision_loss)] // Throughput is an approximate display value.
    fn message(&self) -> String {
        let rate = self.records as f64 / self.started.elapsed().as_secs_f64().max(0.001);
        format!(
            "{} records · {rate:.0} records/s · {}",
            self.records, self.filename
        )
    }

    fn refresh(&mut self) {
        let message = self.message();
        if self.interactive {
            self.bar.set_message(message);
        } else {
            log::info!(
                "{}: {}/{} files · {message} · elapsed {:.1}s",
                self.phase,
                self.completed,
                self.files.unwrap_or(0),
                self.started.elapsed().as_secs_f64()
            );
        }
        self.updated = Instant::now();
    }

    /// Clear the bar and log the phase totals. An abandoned phase is cleared on drop instead.
    pub fn finish(&self) {
        self.bar.finish_and_clear();
        if let Some(files) = self.files {
            log::info!(
                "{}: complete · {}/{files} files · {} records · elapsed {:.1}s",
                self.phase,
                self.completed,
                self.records,
                self.started.elapsed().as_secs_f64()
            );
        } else {
            log::info!(
                "{}: complete · elapsed {:.1}s",
                self.phase,
                self.started.elapsed().as_secs_f64()
            );
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.bar.finish_and_clear();
    }
}
