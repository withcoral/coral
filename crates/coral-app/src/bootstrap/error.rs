//! Defines bootstrap and application-management errors for the local app.

use coral_engine::{CoreError, QueryError, StatusCode};
use tonic::{Code, Status};

use crate::state::CredentialsError;

/// Errors surfaced by the local application layer.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// A requested source was not found in config.
    #[error("source '{0}' not found")]
    SourceNotFound(String),
    /// Caller-supplied input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The request requires additional setup before it can succeed.
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Manifest `YAML` parsing or rendering failed.
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),
    /// `config.toml` decoding failed.
    #[error(transparent)]
    TomlDecode(#[from] toml::de::Error),
    /// `config.toml` encoding failed.
    #[error(transparent)]
    TomlEncode(#[from] toml::ser::Error),
    /// `JSON` encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// `gRPC` transport setup or shutdown failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// Background server task failed to join cleanly.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// Secret-store access failed.
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    /// The Coral config directory could not be discovered from defaults.
    #[error("failed to determine Coral config directory")]
    MissingConfigDir,
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
pub(crate) fn app_status(error: AppError) -> Status {
    Status::new(app_code(&error), error.to_string())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
pub(crate) fn core_status(error: CoreError) -> Status {
    match error {
        CoreError::Structured(query_error) => status_with_query_error(&query_error),
        other => Status::new(grpc_code(other.status_code()), other.to_string()),
    }
}

/// Builds a `tonic::Status` whose `details()` payload carries a structured
/// [`QueryError`] as JSON bytes.
///
/// The gRPC `Status::message()` is set to [`QueryError::to_plain_message`] so
/// proxies or client version-skew that strip `Status::details()` still deliver
/// the summary, detail, and hint to the caller in a single string.
fn status_with_query_error(query_error: &QueryError) -> Status {
    let details = query_error.to_json_bytes();
    Status::with_details(
        grpc_code(query_error.grpc_status_code()),
        query_error.to_plain_message(),
        details.into(),
    )
}

fn grpc_code(status: StatusCode) -> Code {
    match status {
        StatusCode::InvalidArgument => Code::InvalidArgument,
        StatusCode::NotFound => Code::NotFound,
        StatusCode::FailedPrecondition => Code::FailedPrecondition,
        StatusCode::Unavailable => Code::Unavailable,
        StatusCode::Unimplemented => Code::Unimplemented,
        StatusCode::Internal => Code::Internal,
    }
}

fn app_code(error: &AppError) -> Code {
    match error {
        AppError::SourceNotFound(_) => Code::NotFound,
        AppError::InvalidInput(_) => Code::InvalidArgument,
        AppError::FailedPrecondition(_)
        | AppError::MissingConfigDir
        | AppError::Credentials(CredentialsError::Parse(_)) => Code::FailedPrecondition,
        AppError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => Code::NotFound,
        AppError::Io(_)
        | AppError::Yaml(_)
        | AppError::TomlDecode(_)
        | AppError::TomlEncode(_)
        | AppError::Json(_)
        | AppError::Transport(_)
        | AppError::TaskJoin(_)
        | AppError::Credentials(_) => Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreError, core_status};
    use coral_engine::QueryError;
    use tonic::Code;

    #[test]
    fn core_status_flat_variant_uses_plain_status_new() {
        let status = core_status(CoreError::FailedPrecondition(
            "missing config dir".to_string(),
        ));
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("missing config dir"));
        assert!(status.details().is_empty());
    }

    #[test]
    fn core_status_structured_variant_attaches_query_error_details() {
        let query_error = QueryError::missing_required_filter(
            "github",
            "issues",
            "repo",
            "missing required filter",
        );
        let status = core_status(CoreError::Structured(Box::new(query_error.clone())));
        assert_eq!(status.code(), Code::FailedPrecondition);
        // Message carries the fallback plain text (summary + detail + hint).
        let message = status.message();
        assert!(message.contains(&query_error.summary));
        assert!(message.contains("Hint: "));
        // Details carry the JSON payload — round-trip through from_json_bytes.
        let decoded = QueryError::from_json_bytes(status.details()).expect("details should decode");
        assert_eq!(decoded, query_error);
    }

    #[test]
    fn core_status_structured_provider_500_routes_to_unavailable() {
        let query_error = QueryError::provider_request(
            "github",
            "issues",
            Some(500),
            Some("GET".to_string()),
            Some("https://api.github.com/repos/coral/coral/issues".to_string()),
            "upstream boom",
        );
        let status = core_status(CoreError::Structured(Box::new(query_error)));
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("upstream boom"));
    }

    #[test]
    fn core_status_structured_provider_401_routes_to_failed_precondition() {
        let query_error =
            QueryError::provider_request("github", "issues", Some(401), None, None, "unauthorized");
        let status = core_status(CoreError::Structured(Box::new(query_error)));
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("unauthorized"));
    }
}
