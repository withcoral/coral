//! AIP-193 structured query error decoding for `tonic::Status`.
//!
//! The server attaches standard `google.rpc.*` error detail messages to
//! failed RPC responses following [AIP-193](https://google.aip.dev/193).
//! Coral errors are identified by an `ErrorInfo` entry with
//! `domain = "coral.withcoral.com"`. This module centralises the
//! decode-or-fallback policy so every downstream consumer — `coral-cli`,
//! `coral-mcp`, and any future SDK — sees the same shape.

use std::collections::HashMap;
use std::fmt::Write as _;

pub use coral_api::CORAL_ERROR_DOMAIN;
use coral_api::{
    CORAL_ERROR_METADATA_DETAIL, CORAL_ERROR_METADATA_HINT, CORAL_ERROR_METADATA_SUMMARY,
};
use tonic_types::{ErrorDetail, StatusExt as _};

/// Result of decoding a structured query error from a `tonic::Status`.
pub enum DecodedStatusError {
    /// The server attached a `google.rpc.ErrorInfo` with
    /// `domain = "coral.withcoral.com"`.
    Structured(Box<CoralQueryError>),
    /// No Coral error detail found. Holds the raw `Status::message()`.
    Plain(String),
}

/// Structured Coral query error extracted from AIP-193 status details.
///
/// First-class `summary`, `detail`, and `hint` fields are extracted from
/// `ErrorInfo.metadata` during decoding. The remaining metadata (source,
/// table, field, `http_status`, etc.) stays in the `metadata` map.
pub struct CoralQueryError {
    /// Machine-readable error reason (e.g. `"MISSING_REQUIRED_FILTER"`).
    pub reason: String,
    /// One-line error summary.
    pub summary: String,
    /// Longer explanation (may be empty).
    pub detail: String,
    /// Actionable recovery guidance.
    pub hint: Option<String>,
    /// Whether the error is transient and retrying may succeed.
    pub retryable: bool,
    /// Additional metadata (source, table, field, `http_status`, etc.).
    pub metadata: HashMap<String, String>,
    /// The plain-text message from `Status::message()`.
    pub message: String,
}

/// Renders labelled `Error:` / `Detail:` / `Hint:` text for structured errors.
///
/// The returned block does not include a trailing newline. Callers that write
/// line-oriented stderr can append one, while structured transports can embed
/// the text verbatim.
#[must_use]
pub fn render_error_block(summary: &str, detail: &str, hint: Option<&str>) -> String {
    let mut text = format!("Error: {summary}");
    if !detail.is_empty() {
        write!(text, "\nDetail: {detail}").expect("writing to String cannot fail");
    }
    if let Some(hint) = hint {
        write!(text, "\nHint: {hint}").expect("writing to String cannot fail");
    }
    text
}

/// Decodes a structured Coral query error from a `tonic::Status`,
/// falling back to the raw `Status::message()` when no matching detail
/// is found.
///
/// The decode looks for a `google.rpc.ErrorInfo` with
/// `domain = "coral.withcoral.com"` in the AIP-193 status details.
/// The domain string is the wire-level discriminator — unlike protobuf
/// structural matching, it cannot collide with unrelated error details
/// from other services or proxies. If no matching `ErrorInfo` is found,
/// the function returns `Plain(status.message())`.
#[must_use]
pub fn decode_status_error(status: &tonic::Status) -> DecodedStatusError {
    let details = status.get_error_details_vec();

    if details.is_empty() {
        return DecodedStatusError::Plain(status.message().to_string());
    }

    let mut error_info = None;
    let mut retryable = false;

    for detail in details {
        match detail {
            ErrorDetail::ErrorInfo(info) if info.domain == CORAL_ERROR_DOMAIN => {
                error_info = Some(info);
            }
            ErrorDetail::RetryInfo(_) => {
                retryable = true;
            }
            _ => {}
        }
    }

    match error_info {
        Some(info) => {
            let mut metadata = info.metadata;
            let summary = metadata
                .remove(CORAL_ERROR_METADATA_SUMMARY)
                .unwrap_or_else(|| status.message().to_string());
            let detail = metadata
                .remove(CORAL_ERROR_METADATA_DETAIL)
                .unwrap_or_default();
            let hint = metadata.remove(CORAL_ERROR_METADATA_HINT);
            DecodedStatusError::Structured(Box::new(CoralQueryError {
                reason: info.reason,
                summary,
                detail,
                hint,
                retryable,
                metadata,
                message: status.message().to_string(),
            }))
        }
        None => DecodedStatusError::Plain(status.message().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use coral_api::CORAL_ERROR_DOMAIN;

    use super::{CoralQueryError, DecodedStatusError, decode_status_error};

    fn build_coral_status(reason: &str, metadata: Vec<(&str, &str)>, retryable: bool) -> Status {
        let meta: std::collections::HashMap<String, String> = metadata
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut details: Vec<ErrorDetail> = vec![ErrorDetail::ErrorInfo(
            tonic_types::ErrorInfo::new(reason, CORAL_ERROR_DOMAIN, meta),
        )];
        if retryable {
            details.push(ErrorDetail::RetryInfo(tonic_types::RetryInfo::new(None)));
        }
        Status::with_error_details_vec(Code::FailedPrecondition, "test message", details)
    }

    fn assert_plain_message(status: &Status, expected: &str) {
        match decode_status_error(status) {
            DecodedStatusError::Plain(message) => assert_eq!(message, expected),
            DecodedStatusError::Structured(_) => panic!("expected Plain"),
        }
    }

    fn structured_error(status: &Status) -> Box<CoralQueryError> {
        match decode_status_error(status) {
            DecodedStatusError::Structured(err) => err,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        }
    }

    #[test]
    fn plain_status_without_details_falls_back_to_message() {
        let status = Status::new(Code::FailedPrecondition, "raw legacy message");
        assert_plain_message(&status, "raw legacy message");
    }

    #[test]
    fn unrelated_bytes_in_details_fall_back_to_message() {
        let status = Status::with_details(
            Code::Internal,
            "opaque upstream failure",
            b"not a google.rpc.Status payload".to_vec().into(),
        );
        assert_plain_message(&status, "opaque upstream failure");
    }

    #[test]
    fn error_info_with_wrong_domain_falls_back_to_message() {
        let empty_meta = std::collections::HashMap::<String, String>::default();
        let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
            "SOME_REASON",
            "other.service.dev",
            empty_meta,
        ))];
        let status = Status::with_error_details_vec(Code::Internal, "not ours", details);

        assert_plain_message(&status, "not ours");
    }

    #[test]
    fn coral_error_info_decodes_as_structured() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                ("schema", "github"),
                ("table", "issues"),
                ("column", "repo"),
                (
                    "summary",
                    "github.issues requires `WHERE repo = <constant>`",
                ),
                ("hint", "Add a constant equality filter on `repo`."),
            ],
            false,
        );
        let err = structured_error(&status);
        assert_eq!(err.reason, "MISSING_REQUIRED_FILTER");
        assert_eq!(
            err.summary,
            "github.issues requires `WHERE repo = <constant>`"
        );
        assert_eq!(
            err.hint.as_deref(),
            Some("Add a constant equality filter on `repo`.")
        );
        assert_eq!(err.metadata.get("schema").expect("schema"), "github");
        assert_eq!(err.metadata.get("table").expect("table"), "issues");
        assert_eq!(err.metadata.get("column").expect("column"), "repo");
        assert!(
            !err.metadata.contains_key("summary"),
            "summary promoted out of metadata"
        );
        assert!(
            !err.metadata.contains_key("hint"),
            "hint promoted out of metadata"
        );
        assert!(!err.retryable);
        assert_eq!(err.message, "test message");
    }

    #[test]
    fn retry_info_sets_retryable_flag() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![("http_status", "500")],
            true,
        );
        let err = structured_error(&status);
        assert!(err.retryable);
        assert_eq!(err.reason, "PROVIDER_REQUEST_FAILED");
    }

    #[test]
    fn metadata_carries_provider_fields_and_promotes_presentation() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("source", "github"),
                ("table", "issues"),
                ("http_status", "401"),
                ("http_method", "GET"),
                ("url", "https://api.github.com/repos/coral/coral/issues"),
                ("summary", "Source authentication failed (401)"),
                ("hint", "Re-install the source to refresh credentials."),
                ("detail", "bad credentials"),
            ],
            false,
        );
        let err = structured_error(&status);
        assert_eq!(err.summary, "Source authentication failed (401)");
        assert_eq!(err.detail, "bad credentials");
        assert!(err.hint.as_deref().unwrap().contains("Re-install"));
        assert_eq!(err.metadata.get("source").expect("source"), "github");
        assert_eq!(err.metadata.get("http_status").expect("http status"), "401");
        assert_eq!(err.metadata.get("http_method").expect("http method"), "GET");
        assert!(!err.metadata.contains_key("summary"));
        assert!(!err.metadata.contains_key("detail"));
        assert!(!err.metadata.contains_key("hint"));
    }

    #[test]
    fn empty_details_bytes_fall_back_even_with_non_empty_message() {
        let status = Status::new(Code::NotFound, "resource not found: github.issues");
        assert!(status.details().is_empty());
        assert_plain_message(&status, "resource not found: github.issues");
    }
}
