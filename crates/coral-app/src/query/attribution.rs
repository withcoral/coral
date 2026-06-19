//! Transport-free attribution for a query's originating context.

use crate::episode::EpisodeId;

/// Attribution metadata parsed at the transport edge and consumed by query spans.
///
/// Today it carries the optional originating episode; the manager stamps
/// `episode.id` on the `coral.query` span so trajectory-memory capture can join
/// a task's queries to the intent registered by `OpenEpisode`. Intent itself is
/// never carried here — only the opaque id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QueryAttribution {
    episode_id: Option<EpisodeId>,
}

impl QueryAttribution {
    pub(crate) fn new(episode_id: Option<EpisodeId>) -> Self {
        Self { episode_id }
    }

    pub(crate) fn episode_id(&self) -> Option<&EpisodeId> {
        self.episode_id.as_ref()
    }
}
