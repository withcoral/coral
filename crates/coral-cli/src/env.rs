//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow.

#[cfg(feature = "cli-test-server")]
const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";

/// Reads the feature-gated endpoint override used by CLI integration tests.
#[cfg(feature = "cli-test-server")]
#[expect(
    clippy::disallowed_methods,
    reason = "This feature-gated test hook owns the CORAL_ENDPOINT bootstrap override."
)]
#[must_use]
pub fn bootstrap_endpoint() -> Option<String> {
    std::env::var_os(CORAL_ENDPOINT_ENV)
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}

const CORAL_TRACE_PARENT_ENV: &str = "CORAL_TRACE_PARENT";
const CORAL_BENCH_EPISODE_ID_ENV: &str = "CORAL_BENCH_EPISODE_ID";
const CORAL_BENCH_EPISODE_INTENT_ENV: &str = "CORAL_BENCH_EPISODE_INTENT";
const CORAL_BENCH_PARENT_EPISODE_ID_ENV: &str = "CORAL_BENCH_PARENT_EPISODE_ID";

/// Benchmark-owned trajectory-memory episode context for Coral RPC calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkEpisodeEnv {
    /// Client-minted episode id to attach to outgoing Coral calls.
    pub episode_id: String,
    /// Natural-language intent to register for the episode.
    pub intent: String,
    /// Optional parent episode id.
    pub parent_episode_id: Option<String>,
}

/// Reads the optional W3C `traceparent` used to link CLI spans to a parent trace.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_TRACE_PARENT is a CLI-owned per-invocation distributed tracing seed."
)]
#[must_use]
pub fn trace_parent() -> Option<String> {
    std::env::var(CORAL_TRACE_PARENT_ENV).ok()
}

/// Reads the benchmark-owned episode context used to validate trajectory memory.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_BENCH_* is a CLI-owned internal benchmark hook for trajectory-memory validation."
)]
#[must_use]
pub fn benchmark_episode() -> Option<BenchmarkEpisodeEnv> {
    let episode_id = std::env::var(CORAL_BENCH_EPISODE_ID_ENV).ok()?;
    let intent = std::env::var(CORAL_BENCH_EPISODE_INTENT_ENV).ok()?;
    let parent_episode_id = std::env::var(CORAL_BENCH_PARENT_EPISODE_ID_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    Some(BenchmarkEpisodeEnv {
        episode_id,
        intent,
        parent_episode_id,
    })
}
