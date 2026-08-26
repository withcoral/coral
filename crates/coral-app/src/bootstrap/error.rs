//! Defines bootstrap and application-management errors for the local app.

use coral_api::{
    CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_DETAIL, CORAL_ERROR_METADATA_HINT,
    CORAL_ERROR_METADATA_SUMMARY, CORAL_ERROR_REASON_FUNCTION_NOT_FOUND,
    CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND, CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
    CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND,
};
use coral_engine::{CoreError, StatusCode};
use tonic::{Code, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

use crate::credentials::CredentialsError;
use crate::state::db::DbError;

/// Errors surfaced by the local application layer.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// The request did not present valid authentication.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// A requested source was not found in config.
    #[error("source '{0}' not found")]
    SourceNotFound(String),
    /// A requested function was not found in config.
    #[error("function '{0}' not found")]
    FunctionNotFound(String),
    /// A function cannot be created because the name is already installed.
    #[error("function '{0}' already exists")]
    FunctionAlreadyExists(String),
    /// A requested identity spec was not found in the selected scope.
    #[error("identity spec '{name}' not found in scope '{scope}'")]
    IdentitySpecNotFound {
        /// Requested identity-spec name.
        name: String,
        /// Canonical requested scope (`global` or `workspace:<name>`).
        scope: String,
    },
    /// A requested workspace was not found in config.
    #[error("workspace '{0}' not found")]
    WorkspaceNotFound(String),
    /// A requested workspace already exists in config.
    #[error("workspace '{0}' already exists")]
    WorkspaceAlreadyExists(String),
    /// Caller-supplied input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The request requires additional setup before it can succeed.
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// One installed source is missing required configured inputs.
    #[error("failed precondition: source '{source_name}' is missing {detail}")]
    MissingSourceInputs {
        /// Source whose required input is absent.
        source_name: String,
        /// Human-readable description of the missing input or inputs.
        detail: String,
    },
    /// This build cannot resolve a DSL v4 source's declared identities.
    #[error(
        "failed precondition: source '{source_name}' declares DSL v4 identity_requirements, but this Coral build cannot resolve source identities. Use a Coral build with identity runtime support before querying this source."
    )]
    UnsupportedV4IdentityRequirements {
        /// Source containing unsupported identity requirements.
        source_name: String,
    },
    /// A DSL v4 source has missing or stale generated runtime artifacts.
    #[error(
        "failed precondition: source '{source_name}' has missing or incompatible DSL v4 materialized artifacts: {detail}. Re-add the source or reconcile the selected artifact files."
    )]
    MissingOrIncompatibleV4Materialization {
        /// Source name whose installed artifacts failed validation.
        source_name: String,
        /// Specific materialization mismatch or missing-artifact detail.
        detail: String,
    },
    /// An installed DSL v4 manifest uses a no-longer-supported schema shape.
    #[error(
        "failed precondition: source '{source_name}' has an incompatible installed DSL v4 manifest: {detail}. Re-add the source with a current manifest."
    )]
    IncompatibleInstalledV4Manifest {
        /// Source whose installed manifest is incompatible.
        source_name: String,
        /// Specific manifest incompatibility.
        detail: String,
    },
    /// A user-maintained DSL v4 projection override is malformed or stale.
    #[error(
        "failed precondition: source '{source_name}' has invalid DSL v4 projection override '{override_path}': {detail}. Edit or remove the override file."
    )]
    InvalidV4ProjectionOverride {
        /// Source name whose override failed validation.
        source_name: String,
        /// Projection override path that failed validation.
        override_path: String,
        /// Specific override mismatch or malformed-artifact detail.
        detail: String,
    },
    /// A user-maintained DSL v4 operation metadata override is malformed or stale.
    #[error(
        "failed precondition: source '{source_name}' has invalid DSL v4 operation metadata override '{override_path}': {detail}. Edit or remove the override file."
    )]
    InvalidV4OperationMetadataOverride {
        /// Source name whose override failed validation.
        source_name: String,
        /// Operation metadata override path that failed validation.
        override_path: String,
        /// Specific override mismatch or malformed-file detail.
        detail: String,
    },
    /// Provider-managed credential refresh failed during active source use.
    #[error("credential refresh failed: {0}")]
    CredentialRefresh(String),
    /// A required remote dependency was unavailable.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// A database mutation conflicted with another transaction and may be retried.
    #[error("unavailable: database transaction conflict; retry the request")]
    RetryableTransactionConflict,
    /// The server exhausted a resource required to complete the request.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    /// An internal server operation failed.
    #[error("internal error: {0}")]
    Internal(String),
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Manifest `YAML` parsing or rendering failed.
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),
    /// `config.toml` decoding failed.
    #[error(transparent)]
    TomlDecode(#[from] toml::de::Error),
    /// `config.toml` parsing failed while preserving raw TOML structure.
    #[error(transparent)]
    TomlEditDecode(#[from] toml_edit::TomlError),
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
    /// Credential material access failed.
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    /// Durable app-state database access failed.
    #[error("database error: {0}")]
    Database(String),
    /// The Coral config directory could not be discovered from defaults.
    #[error("failed to determine Coral config directory")]
    MissingConfigDir,
}

impl From<DbError> for AppError {
    fn from(error: DbError) -> Self {
        match error {
            DbError::Config(detail) => {
                Self::FailedPrecondition(format!("database configuration is invalid: {detail}"))
            }
            error @ DbError::CorruptData(_) => Self::Database(error.to_string()),
            DbError::MissingDatabaseParent(path) => Self::FailedPrecondition(format!(
                "database file parent directory is missing for {}",
                path.display()
            )),
            DbError::Io(error) => Self::Io(error),
            DbError::TomlDecode(error) => Self::TomlDecode(error),
            DbError::RetryableTransactionConflict(_) => Self::RetryableTransactionConflict,
            DbError::Sqlx(error) => Self::Database(error.to_string()),
            DbError::Migration(error) => Self::Database(error.to_string()),
        }
    }
}

/// Upper bound on the byte length of a `tonic::Status` message (detail).
///
/// gRPC `Status` details travel in HTTP/2 trailers; peers bound the total
/// trailer set via `MAX_HEADER_LIST_SIZE` (default ~16 KiB on hyper/h2).
/// Oversized details cause the server to emit invalid trailers and the
/// client's h2 stack reports `PROTOCOL_ERROR` instead of surfacing the
/// status. 4 KiB leaves ample room for other trailer entries
/// (`grpc-status`, `grpc-status-details-bin`, `content-type`, …).
pub(crate) const MAX_STATUS_DETAIL_BYTES: usize = 4 * 1024;

/// Generic safety-net truncation for `tonic::Status` details.
///
/// Intentionally format-agnostic: no string heuristics on `DataFusion`
/// error shapes, no "did you mean?" hints (those live in the structured
/// error-conversion path where we have typed `Column` data — see
/// `coral_engine::runtime::query`). This function's only job is to keep
/// whatever string it's given under the trailer budget.
fn truncate_status_detail(detail: String) -> String {
    const MARKER: &str = "… (truncated)";
    if detail.len() <= MAX_STATUS_DETAIL_BYTES {
        return detail;
    }
    let mut cut = MAX_STATUS_DETAIL_BYTES.saturating_sub(MARKER.len());
    while cut > 0 && !detail.is_char_boundary(cut) {
        cut -= 1;
    }
    let truncated = detail
        .get(..cut)
        .expect("cut is adjusted to a UTF-8 character boundary");
    format!("{truncated}{MARKER}")
}

pub(crate) fn status_with_bounded_detail(code: Code, detail: impl Into<String>) -> Status {
    Status::new(code, truncate_status_detail(detail.into()))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
pub(crate) fn app_status(error: AppError) -> Status {
    let not_found_reason = match &error {
        AppError::SourceNotFound(_) => Some(CORAL_ERROR_REASON_SOURCE_NOT_FOUND),
        AppError::FunctionNotFound(_) => Some(CORAL_ERROR_REASON_FUNCTION_NOT_FOUND),
        AppError::IdentitySpecNotFound { .. } => Some(CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND),
        AppError::WorkspaceNotFound(_) => Some(CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND),
        _ => None,
    };
    if let Some(reason) = not_found_reason {
        // The `reason` alone discriminates typed Coral misses from other
        // `Code::NotFound` causes without echoing unbounded identifiers.
        let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
            reason,
            CORAL_ERROR_DOMAIN,
            std::collections::HashMap::new(),
        ))];
        return Status::with_error_details_vec(
            Code::NotFound,
            truncate_status_detail(error.to_string()),
            details,
        );
    }
    status_with_bounded_detail(app_code(&error), error.to_string())
}

pub(crate) fn core_status(error: CoreError) -> Status {
    match error {
        CoreError::QueryFailure(sqe) => {
            let mut metadata = sqe.metadata().clone();
            metadata.insert(
                CORAL_ERROR_METADATA_SUMMARY.to_string(),
                sqe.summary().to_string(),
            );
            if !sqe.detail().is_empty() {
                metadata.insert(
                    CORAL_ERROR_METADATA_DETAIL.to_string(),
                    truncate_status_detail(sqe.detail().to_string()),
                );
            }
            if let Some(hint) = sqe.hint() {
                metadata.insert(CORAL_ERROR_METADATA_HINT.to_string(), hint.to_string());
            }

            let mut details: Vec<ErrorDetail> = vec![ErrorDetail::ErrorInfo(
                tonic_types::ErrorInfo::new(sqe.reason(), CORAL_ERROR_DOMAIN, metadata),
            )];
            if sqe.retryable() {
                details.push(ErrorDetail::RetryInfo(tonic_types::RetryInfo::new(None)));
            }

            let plain = render_plain_message(sqe.summary(), sqe.detail(), sqe.hint());
            Status::with_error_details_vec(
                grpc_code(sqe.status()),
                truncate_status_detail(plain),
                details,
            )
        }
        other => Status::new(
            grpc_code(other.status_code()),
            truncate_status_detail(other.to_string()),
        ),
    }
}

fn render_plain_message(summary: &str, detail: &str, hint: Option<&str>) -> String {
    let mut message = summary.to_string();
    if !detail.is_empty() {
        message.push('\n');
        message.push_str(detail);
    }
    if let Some(hint) = hint {
        message.push_str("\nHint: ");
        message.push_str(hint);
    }
    message
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
        AppError::Unauthenticated(_) => Code::Unauthenticated,
        AppError::SourceNotFound(_)
        | AppError::FunctionNotFound(_)
        | AppError::IdentitySpecNotFound { .. }
        | AppError::WorkspaceNotFound(_) => Code::NotFound,
        AppError::FunctionAlreadyExists(_) | AppError::WorkspaceAlreadyExists(_) => {
            Code::AlreadyExists
        }
        AppError::InvalidInput(_) => Code::InvalidArgument,
        AppError::FailedPrecondition(_)
        | AppError::MissingSourceInputs { .. }
        | AppError::UnsupportedV4IdentityRequirements { .. }
        | AppError::MissingOrIncompatibleV4Materialization { .. }
        | AppError::IncompatibleInstalledV4Manifest { .. }
        | AppError::InvalidV4ProjectionOverride { .. }
        | AppError::InvalidV4OperationMetadataOverride { .. }
        | AppError::CredentialRefresh(_)
        | AppError::MissingConfigDir
        | AppError::Credentials(CredentialsError::Parse(_) | CredentialsError::Unavailable(_)) => {
            Code::FailedPrecondition
        }
        AppError::Unavailable(_) | AppError::RetryableTransactionConflict => Code::Unavailable,
        AppError::ResourceExhausted(_) => Code::ResourceExhausted,
        AppError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => Code::NotFound,
        AppError::Internal(_)
        | AppError::Io(_)
        | AppError::Yaml(_)
        | AppError::TomlDecode(_)
        | AppError::TomlEditDecode(_)
        | AppError::TomlEncode(_)
        | AppError::Json(_)
        | AppError::Transport(_)
        | AppError::TaskJoin(_)
        | AppError::Credentials(_)
        | AppError::Database(_) => Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_status_detail_leaves_short_detail_unchanged() {
        let detail = "short message".to_string();
        assert_eq!(truncate_status_detail(detail.clone()), detail);
    }

    #[test]
    fn truncate_status_detail_caps_long_ascii_and_marks_it() {
        let detail = "x".repeat(20 * 1024);
        let out = truncate_status_detail(detail);
        assert!(out.len() <= MAX_STATUS_DETAIL_BYTES);
        assert!(out.ends_with("… (truncated)"), "missing marker: {out:?}");
    }

    #[test]
    fn app_status_maps_unauthenticated_and_truncates_detail() {
        let status = app_status(AppError::Unauthenticated("x".repeat(20 * 1024)));

        assert_eq!(status.code(), Code::Unauthenticated);
        assert!(status.message().len() <= MAX_STATUS_DETAIL_BYTES);
        assert!(status.message().ends_with("… (truncated)"));
    }

    #[test]
    fn app_status_explains_unsupported_v4_identity_requirements_without_readd_guidance() {
        let status = app_status(AppError::UnsupportedV4IdentityRequirements {
            source_name: "demo".to_string(),
        });

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(
            status
                .message()
                .contains("source 'demo' declares DSL v4 identity_requirements")
        );
        assert!(
            status
                .message()
                .contains("cannot resolve source identities")
        );
        assert!(!status.message().contains("Re-add"));
    }

    #[test]
    fn app_status_attaches_structured_reason_for_source_not_found() {
        let status = app_status(AppError::SourceNotFound("default:hn".to_string()));
        assert_eq!(status.code(), Code::NotFound);

        let details = status.get_error_details_vec();
        let info = details
            .iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("source-not-found status must carry an ErrorInfo detail");
        assert_eq!(info.reason, CORAL_ERROR_REASON_SOURCE_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
        // The reason alone is the discriminator; we intentionally do
        // not echo unbounded identifiers into structured metadata.
        assert!(
            info.metadata.is_empty(),
            "SOURCE_NOT_FOUND must not carry unbounded identifier metadata: {:?}",
            info.metadata
        );
    }

    #[test]
    fn app_status_attaches_structured_reason_for_function_not_found() {
        let status = app_status(AppError::FunctionNotFound("review_queue".to_string()));
        assert_eq!(status.code(), Code::NotFound);

        let details = status.get_error_details_vec();
        let info = details
            .iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("function-not-found status must carry an ErrorInfo detail");
        assert_eq!(info.reason, CORAL_ERROR_REASON_FUNCTION_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
        assert!(
            info.metadata.is_empty(),
            "FUNCTION_NOT_FOUND must not carry unbounded identifier metadata: {:?}",
            info.metadata
        );
    }

    #[test]
    fn app_status_attaches_structured_reason_for_identity_spec_not_found() {
        let status = app_status(AppError::IdentitySpecNotFound {
            name: "github".to_string(),
            scope: "workspace:default".to_string(),
        });
        assert_eq!(status.code(), Code::NotFound);

        let info = status
            .get_error_details_vec()
            .into_iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("identity-spec miss must carry ErrorInfo");
        assert_eq!(info.reason, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
    }

    #[test]
    fn app_status_attaches_structured_reason_for_workspace_not_found() {
        let status = app_status(AppError::WorkspaceNotFound("work".to_string()));
        assert_eq!(status.code(), Code::NotFound);

        let details = status.get_error_details_vec();
        let info = details
            .iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("workspace-not-found status must carry an ErrorInfo detail");
        assert_eq!(info.reason, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
        assert!(
            info.metadata.is_empty(),
            "WORKSPACE_NOT_FOUND must not carry unbounded identifier metadata: {:?}",
            info.metadata
        );
    }

    #[test]
    fn app_status_does_not_attach_structured_reason_for_io_not_found() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "manifest missing");
        let status = app_status(AppError::Io(io_error));
        // Same gRPC code as SourceNotFound — but no Coral ErrorInfo, so
        // clients can't confuse a broken local manifest for a missing
        // catalog entry.
        assert_eq!(status.code(), Code::NotFound);
        assert!(
            status.get_error_details_vec().is_empty(),
            "io::NotFound must not carry SOURCE_NOT_FOUND details"
        );
    }

    #[test]
    fn app_status_maps_unavailable() {
        let status = app_status(AppError::Unavailable(
            "remote descriptor timed out".to_string(),
        ));
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn app_status_maps_retryable_database_conflicts_to_unavailable() {
        let status = app_status(AppError::RetryableTransactionConflict);

        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("retry the request"));
    }

    #[test]
    fn app_status_maps_resource_exhausted() {
        let status = app_status(AppError::ResourceExhausted(
            "local storage is full".to_string(),
        ));

        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn app_status_maps_internal() {
        let status = app_status(AppError::Internal("storage failure".to_string()));

        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn truncate_status_detail_preserves_utf8_boundaries() {
        // Fill with a 4-byte codepoint so the raw-byte cut point is
        // guaranteed to land mid-codepoint and must be walked backwards.
        let detail = "𝕏".repeat(2 * 1024); // 4 bytes per char → 8 KiB total
        let out = truncate_status_detail(detail);
        assert!(out.len() <= MAX_STATUS_DETAIL_BYTES);
        // Result must still be valid UTF-8 (guaranteed by String type) and
        // end with the truncation marker.
        assert!(out.ends_with("… (truncated)"));
    }
}
