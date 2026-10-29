//! Whether a newer Coral release is published.
//!
//! # Interface
//!
//! [`ReleaseCheck`] reads the release feed through the bootstrap-selected
//! destination policy and keeps the newest stable version in memory.
//! [`ReleaseCheck::newer_release`] answers from that memory and never waits
//! for the network. [`ReleaseCheck::spawn`] refreshes it at boot and then on
//! a fixed interval.
//!
//! The feed is the latest stable release of `withcoral/coral-releases`. The
//! release workflow publishes it last, after it tags the images, so a version
//! the feed names can be pulled, and a failed release stays a draft the feed
//! skips. Release-please publishes to `withcoral/coral` before any image
//! exists, so the check never reads it.
//!
//! A check that fails, or a feed answer that is not a stable version, keeps
//! the last answer. Nothing here reports a failure to a person: the check is a
//! hint, not a health signal.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use coral_telemetry::safe;
use semver::Version;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use url::Url;

/// The latest stable release of Coral.
pub const FEED_URL: &str = "https://api.github.com/repos/withcoral/coral-releases/releases/latest";

/// Each release's page carries its notes.
const NOTES_BASE: &str = "https://github.com/withcoral/coral/releases/tag/";

/// How long one feed request may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a running process reads the feed. GitHub allows 60
/// unauthenticated requests an hour from one address.
pub const CHECK_INTERVAL: Duration = Duration::from_hours(6);

/// The feed's answer carries the release notes.
const BODY_LIMIT: usize = 1 << 20;

/// A published Coral release newer than this process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewerRelease {
    version: Version,
    notes_url: String,
}

impl NewerRelease {
    /// The release's version.
    #[must_use]
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// The release notes page.
    #[must_use]
    pub fn notes_url(&self) -> &str {
        &self.notes_url
    }
}

/// Why one feed read did not produce a version.
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    /// The destination policy rejects the feed.
    #[error("the outbound destination policy rejects the release feed")]
    DestinationRejected,
    /// The request failed or the feed answered with an error status.
    #[error("the release feed request failed: {0}")]
    Http(reqwest::Error),
    /// The body was too large or not the feed's JSON.
    #[error("the release feed answer is unreadable")]
    Unreadable,
    /// The latest release is not a stable `vX.Y.Z` tag.
    #[error("the release feed's latest tag is not a stable version")]
    NotStable,
}

impl CheckError {
    /// The failure's stable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DestinationRejected => "destination_rejected",
            Self::Http(_) => "http",
            Self::Unreadable => "unreadable",
            Self::NotStable => "not_stable",
        }
    }
}

/// The newest published release, read from the feed and kept in memory.
#[derive(Clone)]
pub struct ReleaseCheck {
    inner: Arc<Inner>,
}

struct Inner {
    client: coral_outbound::Client,
    feed: Url,
    current: Version,
    newest: RwLock<Option<Version>>,
}

impl ReleaseCheck {
    /// Prepares a check of `feed` for a process running `current`. Nothing
    /// is read until [`Self::refresh`] or [`Self::spawn`].
    #[must_use]
    pub fn new(client: coral_outbound::Client, feed: Url, current: Version) -> Self {
        Self {
            inner: Arc::new(Inner {
                client,
                feed,
                current,
                newest: RwLock::new(None),
            }),
        }
    }

    /// Prepares a check of the release feed for this build of Coral.
    ///
    /// # Panics
    ///
    /// Never: the feed URL and the Cargo version are valid constants.
    #[must_use]
    pub fn coral(client: coral_outbound::Client) -> Self {
        Self::new(
            client,
            Url::parse(FEED_URL).expect("the feed URL is a valid constant"),
            Version::parse(env!("CARGO_PKG_VERSION")).expect("Cargo versions are semantic"),
        )
    }

    /// The newest release the feed last named, when it is newer than this
    /// process. `None` before the first answer.
    #[must_use]
    pub fn newer_release(&self) -> Option<NewerRelease> {
        let newest = self
            .inner
            .newest
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        (newest > self.inner.current).then(|| NewerRelease {
            notes_url: format!("{NOTES_BASE}v{newest}"),
            version: newest,
        })
    }

    /// Reads the feed once and keeps its version.
    ///
    /// # Errors
    ///
    /// Returns [`CheckError`] when the feed gives no stable version. The
    /// last answer stays.
    pub async fn refresh(&self) -> Result<(), CheckError> {
        let version = self.read_feed().await?;
        *self
            .inner
            .newest
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(version);
        Ok(())
    }

    /// Refreshes at once, then every `interval`. Abort the handle to stop.
    #[must_use]
    pub fn spawn(&self, interval: Duration) -> JoinHandle<()> {
        let check = self.clone();
        let mut starts = tokio::time::interval(interval);
        starts.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(async move {
            loop {
                starts.tick().await;
                if let Err(error) = check.refresh().await {
                    tracing::info!(
                        error.type = safe::kind(error.code()),
                        "release check found no version; the next check retries"
                    );
                }
            }
        })
    }

    async fn read_feed(&self) -> Result<Version, CheckError> {
        let response = self
            .inner
            .client
            .get(self.inner.feed.as_str())
            .map_err(|_rejected| CheckError::DestinationRejected)?
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| match coral_outbound::FetchError::from(error) {
                coral_outbound::FetchError::DestinationRejected => CheckError::DestinationRejected,
                coral_outbound::FetchError::Http(error) => CheckError::Http(error),
            })?;
        let body = coral_outbound::read_bounded_body(response, BODY_LIMIT)
            .await
            .map_err(|_error| CheckError::Unreadable)?;
        let release: FeedRelease =
            serde_json::from_slice(&body).map_err(|_error| CheckError::Unreadable)?;
        stable_version(&release.tag_name).ok_or(CheckError::NotStable)
    }
}

#[derive(serde::Deserialize)]
struct FeedRelease {
    tag_name: String,
}

/// The version of a `vX.Y.Z` tag. A prerelease or build suffix is not one.
fn stable_version(tag: &str) -> Option<Version> {
    let version = Version::parse(tag.strip_prefix('v')?).ok()?;
    (version.pre.is_empty() && version.build.is_empty()).then_some(version)
}

#[cfg(test)]
mod tests;
