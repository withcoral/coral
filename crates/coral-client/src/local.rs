//! Explicit local bootstrap helpers for tests and embedding.
//!
//! This module is the opt-in escape hatch for callers that need to control
//! local server configuration or lifetime directly.

/// Re-exported local server builder for explicit local bootstrap scenarios.
pub use coral_app::ServerBuilder;

/// Re-exported local server handle for explicit local bootstrap scenarios.
pub use coral_app::RunningServer;

/// Re-exported local server startup error for explicit bootstrap surfaces.
pub use coral_app::AppError as LocalServerError;
