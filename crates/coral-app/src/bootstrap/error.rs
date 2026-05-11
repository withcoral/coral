//! Defines bootstrap and application-management errors for the local app.

use coral_api::{
    CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_DETAIL, CORAL_ERROR_METADATA_HINT,
    CORAL_ERROR_METADATA_SUMMARY, CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
};
use coral_engine::{CoreError, StatusCode};
use tonic::{Code, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

use crate::credentials::CredentialsError;

struct ErrorDiagnostic {
    reason: &'static str,
    summary: String,
    detail: String,
    hint: Option<String>,
}

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
    /// The Coral config directory could not be discovered from defaults.
    #[error("failed to determine Coral config directory")]
    MissingConfigDir,
}

impl AppError {
    fn diagnostic(&self) -> ErrorDiagnostic {
        match self {
            AppError::SourceNotFound(source) => ErrorDiagnostic {
                reason: CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
                summary: format!("Source `{source}` was not found"),
                detail: format!("No source named `{source}` is installed in this workspace."),
                hint: Some("List installed sources or discover available sources, then retry with a source that exists.".to_string()),
            },
            AppError::InvalidInput(detail) => ErrorDiagnostic {
                reason: "INVALID_INPUT",
                summary: "Input is invalid".to_string(),
                detail: detail.clone(),
                hint: Some("Check the request input and retry with valid values.".to_string()),
            },
            AppError::FailedPrecondition(detail) => ErrorDiagnostic {
                reason: "SETUP_REQUIRED",
                summary: "Setup is incomplete".to_string(),
                detail: detail.clone(),
                hint: Some("Inspect configured sources, then test the source you are trying to use.".to_string()),
            },
            AppError::MissingConfigDir => ErrorDiagnostic {
                reason: "CONFIG_DIR_NOT_FOUND",
                summary: "Coral could not find a config directory".to_string(),
                detail: "The operating system did not provide a usable app config directory.".to_string(),
                hint: Some("Configure Coral with a writable config directory, then retry.".to_string()),
            },
            AppError::Io(error) => ErrorDiagnostic {
                reason: "LOCAL_FILE_ERROR",
                summary: "Coral could not read or write a local file".to_string(),
                detail: error.to_string(),
                hint: Some("Check that the path exists and that Coral can read and write its config directory.".to_string()),
            },
            AppError::Yaml(error) => ErrorDiagnostic {
                reason: "INVALID_YAML",
                summary: "Source manifest YAML is invalid".to_string(),
                detail: error.to_string(),
                hint: Some("Fix the YAML file, then rerun the command.".to_string()),
            },
            AppError::TomlDecode(error) => ErrorDiagnostic {
                reason: "INVALID_CONFIG",
                summary: "Coral config file is invalid".to_string(),
                detail: error.to_string(),
                hint: Some("Fix the Coral config file or move it aside, then retry.".to_string()),
            },
            AppError::TomlEditDecode(error) => ErrorDiagnostic {
                reason: "INVALID_CONFIG",
                summary: "Coral config file is invalid".to_string(),
                detail: error.to_string(),
                hint: Some("Fix the Coral config file or move it aside, then retry.".to_string()),
            },
            AppError::TomlEncode(error) => ErrorDiagnostic {
                reason: "CONFIG_WRITE_FAILED",
                summary: "Coral could not write its config file".to_string(),
                detail: error.to_string(),
                hint: Some("Check permissions on the Coral config directory, or use a writable config directory.".to_string()),
            },
            AppError::Json(error) => ErrorDiagnostic {
                reason: "INVALID_JSON",
                summary: "Coral could not read or write JSON data".to_string(),
                detail: error.to_string(),
                hint: Some("Retry the command. If it keeps failing, check local Coral state files for invalid JSON.".to_string()),
            },
            AppError::Transport(error) => ErrorDiagnostic {
                reason: "LOCAL_SERVER_ERROR",
                summary: "Coral could not start or use the local server".to_string(),
                detail: error.to_string(),
                hint: Some("Retry the command. If you are starting the UI, check whether the selected port is already in use.".to_string()),
            },
            AppError::TaskJoin(error) => ErrorDiagnostic {
                reason: "LOCAL_SERVER_TASK_FAILED",
                summary: "The local Coral server stopped unexpectedly".to_string(),
                detail: error.to_string(),
                hint: Some("Retry the command. If it keeps failing, restart the terminal session and try again.".to_string()),
            },
            AppError::Credentials(error) => match error {
                CredentialsError::Parse(detail) => ErrorDiagnostic {
                    reason: "INVALID_SECRETS_FILE",
                    summary: "Coral could not read saved source credentials".to_string(),
                    detail: detail.clone(),
                    hint: Some("Refresh the saved credentials for the affected source.".to_string()),
                },
                CredentialsError::Io(error) => ErrorDiagnostic {
                    reason: "SECRETS_FILE_ERROR",
                    summary: "Coral could not read or write saved source credentials".to_string(),
                    detail: error.to_string(),
                    hint: Some("Check permissions on the Coral config directory, or use a writable config directory.".to_string()),
                },
                CredentialsError::Unavailable(detail) => ErrorDiagnostic {
                    reason: "SECRETS_STORE_UNAVAILABLE",
                    summary: "Coral could not access saved source credentials".to_string(),
                    detail: detail.clone(),
                    hint: Some("Unlock or configure the system credential store, then retry.".to_string()),
                },
                CredentialsError::SnapshotStorageMismatch {
                    snapshot,
                    requested,
                } => ErrorDiagnostic {
                    reason: "SECRETS_STORAGE_MISMATCH",
                    summary: "Saved source credentials use a different storage backend".to_string(),
                    detail: format!(
                        "The credential snapshot is stored in `{snapshot}`, but `{requested}` was requested."
                    ),
                    hint: Some(
                        "Use the same credential storage backend or re-add the source credentials."
                            .to_string(),
                    ),
                },
            },
        }
    }
}

/// Upper bound on the byte length of a `tonic::Status` message or one
/// structured presentation metadata value.
///
/// gRPC `Status` details travel in HTTP/2 trailers; peers bound the total
/// trailer set via `MAX_HEADER_LIST_SIZE` (default ~16 KiB on hyper/h2).
/// Oversized details cause the server to emit invalid trailers and the
/// client's h2 stack reports `PROTOCOL_ERROR` instead of surfacing the
/// status. 4 KiB leaves ample room for other trailer entries
/// (`grpc-status`, `grpc-status-details-bin`, `content-type`, …).
pub(crate) const MAX_STATUS_VALUE_BYTES: usize = 4 * 1024;

/// Generic safety-net truncation for `tonic::Status` values.
///
/// Intentionally format-agnostic: no string heuristics on `DataFusion`
/// error shapes, no "did you mean?" hints (those live in the structured
/// error-conversion path where we have typed `Column` data — see
/// `coral_engine::runtime::query`). This function's only job is to keep
/// whatever string it's given under the trailer budget.
fn truncate_status_value(value: String) -> String {
    const MARKER: &str = "… (truncated)";
    if value.len() <= MAX_STATUS_VALUE_BYTES {
        return value;
    }
    let mut cut = MAX_STATUS_VALUE_BYTES.saturating_sub(MARKER.len());
    while cut > 0 && !value.is_char_boundary(cut) {
        cut -= 1;
    }
    let truncated = value
        .get(..cut)
        .expect("cut is adjusted to a UTF-8 character boundary");
    format!("{truncated}{MARKER}")
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
pub(crate) fn app_status(error: AppError) -> Status {
    let code = app_code(&error);
    let diagnostic = error.diagnostic();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        CORAL_ERROR_METADATA_SUMMARY.to_string(),
        truncate_status_value(diagnostic.summary.clone()),
    );

    if !diagnostic.detail.is_empty() {
        metadata.insert(
            CORAL_ERROR_METADATA_DETAIL.to_string(),
            truncate_status_value(diagnostic.detail.clone()),
        );
    }

    if let Some(hint) = &diagnostic.hint {
        metadata.insert(
            CORAL_ERROR_METADATA_HINT.to_string(),
            truncate_status_value(hint.clone()),
        );
    }

    let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
        diagnostic.reason,
        CORAL_ERROR_DOMAIN,
        metadata,
    ))];

    Status::with_error_details_vec(
        code,
        truncate_status_value(render_plain_message(
            &diagnostic.summary,
            &diagnostic.detail,
            diagnostic.hint.as_deref(),
        )),
        details,
    )
}

pub(crate) fn core_status(error: CoreError) -> Status {
    match error {
        CoreError::QueryFailure(sqe) => {
            let mut metadata = sqe.metadata().clone();
            metadata.insert(
                CORAL_ERROR_METADATA_SUMMARY.to_string(),
                truncate_status_value(sqe.summary().to_string()),
            );
            if !sqe.detail().is_empty() {
                metadata.insert(
                    CORAL_ERROR_METADATA_DETAIL.to_string(),
                    truncate_status_value(sqe.detail().to_string()),
                );
            }
            if let Some(hint) = sqe.hint() {
                metadata.insert(
                    CORAL_ERROR_METADATA_HINT.to_string(),
                    truncate_status_value(hint.to_string()),
                );
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
                truncate_status_value(plain),
                details,
            )
        }
        other => Status::new(
            grpc_code(other.status_code()),
            truncate_status_value(other.to_string()),
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
        AppError::SourceNotFound(_) => Code::NotFound,
        AppError::InvalidInput(_) => Code::InvalidArgument,
        AppError::FailedPrecondition(_)
        | AppError::MissingConfigDir
        | AppError::Credentials(CredentialsError::Parse(_) | CredentialsError::Unavailable(_)) => {
            Code::FailedPrecondition
        }
        AppError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => Code::NotFound,
        AppError::Io(_)
        | AppError::Yaml(_)
        | AppError::TomlDecode(_)
        | AppError::TomlEditDecode(_)
        | AppError::TomlEncode(_)
        | AppError::Json(_)
        | AppError::Transport(_)
        | AppError::TaskJoin(_)
        | AppError::Credentials(_) => Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_status_value_leaves_short_value_unchanged() {
        let value = "short message".to_string();
        assert_eq!(truncate_status_value(value.clone()), value);
    }

    #[test]
    fn truncate_status_value_caps_long_ascii_and_marks_it() {
        let value = "x".repeat(20 * 1024);
        let out = truncate_status_value(value);
        assert!(out.len() <= MAX_STATUS_VALUE_BYTES);
        assert!(out.ends_with("… (truncated)"), "missing marker: {out:?}");
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
        assert!(
            info.metadata
                .get(CORAL_ERROR_METADATA_SUMMARY)
                .is_some_and(|summary| summary.contains("default:hn")),
            "SOURCE_NOT_FOUND should carry bounded presentation metadata: {:?}",
            info.metadata
        );
    }

    #[test]
    fn app_status_does_not_attach_source_not_found_reason_for_io_not_found() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "manifest missing");
        let status = app_status(AppError::Io(io_error));
        assert_eq!(status.code(), Code::NotFound);

        let details = status.get_error_details_vec();
        let info = details
            .iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("io::NotFound status should carry diagnostic details");
        assert_ne!(info.reason, CORAL_ERROR_REASON_SOURCE_NOT_FOUND);
    }

    #[test]
    fn truncate_status_value_preserves_utf8_boundaries() {
        // Fill with a 4-byte codepoint so the raw-byte cut point is
        // guaranteed to land mid-codepoint and must be walked backwards.
        let value = "𝕏".repeat(2 * 1024); // 4 bytes per char → 8 KiB total
        let out = truncate_status_value(value);
        assert!(out.len() <= MAX_STATUS_VALUE_BYTES);
        // Result must still be valid UTF-8 (guaranteed by String type) and
        // end with the truncation marker.
        assert!(out.ends_with("… (truncated)"));
    }

    #[test]
    fn app_status_source_not_found_is_structured() {
        let status = app_status(AppError::SourceNotFound("github".to_string()));
        let decoded = coral_client::decode_status_error(&status);

        let coral_client::DecodedStatusError::Structured(error) = decoded else {
            panic!("expected structured error");
        };

        assert_eq!(error.reason, "SOURCE_NOT_FOUND");
        assert_eq!(error.summary, "Source `github` was not found");
        assert!(error.detail.contains("No source named `github`"));
        let hint = error.hint.as_deref().expect("hint");
        assert!(hint.contains("List installed sources"));
        assert!(!hint.contains("coral "));
    }
}
