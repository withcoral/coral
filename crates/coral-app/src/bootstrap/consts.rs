//! Central registry of environment variables read by the Coral app.

/// Application config directory override.
pub(crate) const CORAL_CONFIG_DIR: &str = "CORAL_CONFIG_DIR";
/// Set to `1` by Desktop when it launches the runtime.
pub(crate) const CORAL_DESKTOP: &str = "CORAL_DESKTOP";
/// A local launcher's web origin for human handoff.
pub(crate) const CORAL_LOCAL_WEB_URL: &str = "CORAL_LOCAL_WEB_URL";
/// Set to `1` by a launcher that stops the runtime by closing its stdin.
pub(crate) const CORAL_STOP_ON_STDIN_CLOSE: &str = "CORAL_STOP_ON_STDIN_CLOSE";
/// W3C `traceparent` of the launching trace.
pub(crate) const CORAL_TRACE_PARENT: &str = "CORAL_TRACE_PARENT";
