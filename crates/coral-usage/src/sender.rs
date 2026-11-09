//! Sends the reports waiting, once a day, to the collector.

use std::time::Duration;

use coral_telemetry::safe;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::UsageData;

/// The collector's endpoint.
pub const COLLECTOR_URL: &str = "https://telemetry.withcoral.com/v1/report";

/// How long one send may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a running process looks for a finished day. A report goes out
/// within this long after its UTC day ends.
pub const CHECK_INTERVAL: Duration = Duration::from_hours(1);

/// What one send attempt learned.
#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// Every waiting report was stored or refused for good.
    Done,
    /// The collector did not answer, or failed; the rest wait for the next
    /// check.
    Retry,
}

impl UsageData {
    /// Makes each finished day's report, then sends every report waiting,
    /// oldest first. A 2xx stores it; a 4xx but 408 or 429 can never
    /// succeed, so the report is dropped; anything else keeps it and stops
    /// until the next check.
    ///
    /// # Errors
    ///
    /// Returns a database failure.
    pub async fn send_due(
        &self,
        client: &coral_outbound::Client,
        collector: &str,
        now: impl Fn() -> i64,
    ) -> Result<SendOutcome, crate::Error> {
        self.make_due_reports(now()).await?;
        for report in self.unsent().await? {
            let Ok(request) = client.post(collector) else {
                return Ok(SendOutcome::Retry);
            };
            let response = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(report.json)
                .send()
                .await;
            let status = match response {
                Ok(response) => response.status(),
                Err(_error) => return Ok(SendOutcome::Retry),
            };
            if status.is_success() {
                self.record_sent(report.day, now()).await?;
            } else if refused_for_good(status) {
                tracing::warn!(
                    http.response.status_code = safe::number(status.as_u16()),
                    "the usage data collector refused a report; it is dropped"
                );
                self.record_rejected(report.day).await?;
            } else {
                return Ok(SendOutcome::Retry);
            }
        }
        Ok(SendOutcome::Done)
    }

    /// Checks at once, then every `interval`. Abort the handle to stop.
    #[must_use]
    pub fn spawn(&self, client: coral_outbound::Client, interval: Duration) -> JoinHandle<()> {
        let usage = self.clone();
        let mut checks = tokio::time::interval(interval);
        checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(async move {
            loop {
                checks.tick().await;
                if let Err(error) = usage.send_due(&client, COLLECTOR_URL, unix_nanos).await {
                    tracing::info!(
                        error.type = safe::kind(error.code()),
                        "usage data check failed; the next check retries"
                    );
                }
            }
        })
    }
}

/// Whether the collector will refuse the same report again: a 4xx, but a
/// timeout or a rate limit, which pass.
fn refused_for_good(status: reqwest::StatusCode) -> bool {
    status.is_client_error()
        && status != reqwest::StatusCode::REQUEST_TIMEOUT
        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
}

fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(0)
}
