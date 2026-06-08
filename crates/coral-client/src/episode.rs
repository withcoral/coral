//! Client-side episode tagging.
//!
//! Carries the active episode id on outgoing Coral calls as the
//! `coral-episode-id` gRPC metadata value, so the server can attribute each
//! query's `coral.query` span to the episode (and thus the intent registered by
//! `OpenEpisode`). Mirrors the trace-context interceptor: an interceptor reads
//! ambient per-task state and injects it into outgoing request metadata.

use std::future::Future;

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;

/// gRPC metadata key the Coral server reads to attribute a call to an episode.
pub const CORAL_EPISODE_ID_METADATA_KEY: &str = "coral-episode-id";

/// Maximum episode id length, in bytes. Kept in sync with the server contract
/// (`coral-app` `episode/id.rs` `EpisodeId::parse`) and the `OpenEpisode` proto.
const MAX_EPISODE_ID_LEN: usize = 128;

/// Whether `id` satisfies the server's `coral-episode-id` contract: non-empty,
/// at most [`MAX_EPISODE_ID_LEN`] bytes, and entirely graphic ASCII
/// (`0x21..=0x7E` — no whitespace, control bytes, or non-ASCII).
///
/// Mirrors `coral-app`'s `EpisodeId::parse` so the client drops ids the server
/// would reject *before* sending them, rather than emitting metadata that gets
/// ignored server-side.
fn is_valid_episode_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_EPISODE_ID_LEN
        && id.bytes().all(|byte| byte.is_ascii_graphic())
}

tokio::task_local! {
    static ACTIVE_EPISODE_ID: String;
}

/// Runs `future` with `episode_id` tagged onto every Coral call it makes.
///
/// While the returned future runs, [`EpisodeIdInterceptor`] attaches the
/// `coral-episode-id` metadata to outgoing requests. Calls made outside any
/// `with_episode_id` scope carry no tag.
pub async fn with_episode_id<F>(episode_id: String, future: F) -> F::Output
where
    F: Future,
{
    ACTIVE_EPISODE_ID.scope(episode_id, future).await
}

/// tonic client interceptor that injects the active episode id (set via
/// [`with_episode_id`]) as the `coral-episode-id` request metadata.
///
/// A no-op when no episode is in scope, or when the active id fails the
/// `coral-episode-id` contract (`is_valid_episode_id`) — episode attribution is
/// best-effort and never fails a call.
#[derive(Clone)]
pub struct EpisodeIdInterceptor;

impl Interceptor for EpisodeIdInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        // `Err` simply means no episode is in scope — the common, untagged case.
        let _scoped = ACTIVE_EPISODE_ID.try_with(|episode_id| {
            // Validate against the server contract before injecting so a tagged
            // call never carries metadata the server would reject/ignore.
            if !is_valid_episode_id(episode_id) {
                // The id is arbitrary caller input, so log its length (a length
                // mismatch is the common cause) rather than the raw value.
                tracing::debug!(
                    episode_id_len = episode_id.len(),
                    "dropping invalid coral-episode-id; call sent untagged"
                );
                return;
            }
            if let Ok(value) = MetadataValue::try_from(episode_id.as_str()) {
                request
                    .metadata_mut()
                    .insert(CORAL_EPISODE_ID_METADATA_KEY, value);
            }
        });
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor as _;

    use super::{
        CORAL_EPISODE_ID_METADATA_KEY, EpisodeIdInterceptor, MAX_EPISODE_ID_LEN, with_episode_id,
    };

    /// Runs the interceptor with `episode_id` in scope and returns the resulting
    /// `coral-episode-id` metadata value, if any.
    async fn tagged_value(episode_id: String) -> Option<String> {
        let request = with_episode_id(episode_id, async {
            let mut interceptor = EpisodeIdInterceptor;
            interceptor
                .call(tonic::Request::new(()))
                .expect("interceptor succeeds")
        })
        .await;
        request
            .metadata()
            .get(CORAL_EPISODE_ID_METADATA_KEY)
            .map(|value| value.to_str().expect("ascii value").to_string())
    }

    #[tokio::test]
    async fn injects_active_episode_id() {
        assert_eq!(
            tagged_value("ep_42".to_string()).await.as_deref(),
            Some("ep_42")
        );
        // A maximum-length id is still valid and injected.
        let max = "a".repeat(MAX_EPISODE_ID_LEN);
        assert_eq!(
            tagged_value(max.clone()).await.as_deref(),
            Some(max.as_str())
        );
    }

    #[tokio::test]
    async fn drops_ids_violating_the_server_contract() {
        let invalid = [
            String::new(),                      // empty
            "   ".to_string(),                  // whitespace only
            "has space".to_string(),            // embedded space
            "tab\tid".to_string(),              // control byte
            "épisode".to_string(),              // non-ASCII
            "a".repeat(MAX_EPISODE_ID_LEN + 1), // over-long
        ];
        for id in invalid {
            assert_eq!(
                tagged_value(id.clone()).await,
                None,
                "invalid id {id:?} must not be injected"
            );
        }
    }

    #[tokio::test]
    async fn no_tag_without_scope() {
        let mut interceptor = EpisodeIdInterceptor;
        let request = interceptor
            .call(tonic::Request::new(()))
            .expect("interceptor succeeds");

        assert!(
            request
                .metadata()
                .get(CORAL_EPISODE_ID_METADATA_KEY)
                .is_none(),
            "untagged calls carry no episode metadata"
        );
    }
}
