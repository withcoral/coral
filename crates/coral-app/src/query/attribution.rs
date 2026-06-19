//! Transport-free attribution for a query's originating context.

use crate::episode::EpisodeId;

/// Request-scoped attribution threaded from the gRPC service edge into the query
/// manager, so transport concerns (gRPC metadata, OpenTelemetry baggage) stay
/// out of the manager and off the deeper query path.
///
/// Today it carries the optional originating episode; the manager stamps
/// `episode.id` on the `coral.query` span so trajectory-memory capture can join
/// a task's queries to the intent registered by `OpenEpisode`. Intent itself is
/// never carried here — only the opaque id.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryAttribution {
    /// The episode whose intent this query served, when the caller supplied a
    /// valid `coral-episode-id`; `None` for an untagged query.
    pub(crate) episode_id: Option<EpisodeId>,
}
