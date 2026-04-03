//! Defines bootstrap and application-management errors for the local app.

use coral_engine::{CoreError, StatusCode};
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
    Status::new(grpc_code(error.status_code()), error.to_string())
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
