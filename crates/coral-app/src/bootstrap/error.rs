//! Defines bootstrap and application-management errors for the local app.

use coral_api::{
    CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_DETAIL, CORAL_ERROR_METADATA_HINT,
    CORAL_ERROR_METADATA_SUMMARY, CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
};
use coral_engine::{CoreError, StatusCode};
use tonic::{Code, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

use crate::credentials::CredentialsError;

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

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
pub(crate) fn app_status(error: AppError) -> Status {
    if matches!(error, AppError::SourceNotFound(_)) {
        // The `reason` alone discriminates `SOURCE_NOT_FOUND` from other
        // `Code::NotFound` causes (e.g. `io::ErrorKind::NotFound` raised
        // when a manifest file is missing). The qualified name already
        // appears in the truncated status message; we deliberately do
        // not duplicate it into structured metadata so unbounded
        // identifiers cannot push the `grpc-status-details-bin` trailer
        // past the h2 `MAX_HEADER_LIST_SIZE` budget.
        let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
            CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
            CORAL_ERROR_DOMAIN,
            std::collections::HashMap::new(),
        ))];
        return Status::with_error_details_vec(
            Code::NotFound,
            truncate_status_detail(error.to_string()),
            details,
        );
    }
    Status::new(app_code(&error), truncate_status_detail(error.to_string()))
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
