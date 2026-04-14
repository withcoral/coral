//! AIP-193 structured query error decoding for `tonic::Status`.
//!
//! The server attaches standard `google.rpc.*` error detail messages to
//! failed RPC responses following [AIP-193](https://google.aip.dev/193).
//! Coral errors are identified by an `ErrorInfo` entry with
//! `domain = "coral.withcoral.com"`. This module centralises the
//! decode-or-fallback policy so every downstream consumer — `coral-cli`,
//! `coral-mcp`, and any future SDK — sees the same shape.

use std::collections::HashMap;

use tonic_types::{ErrorDetail, StatusExt as _};

/// Coral error domain used in `google.rpc.ErrorInfo`.
pub const CORAL_ERROR_DOMAIN: &str = "coral.withcoral.com";

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
/// Populated by reading `ErrorInfo` (reason, domain, metadata) and
/// checking for `RetryInfo` presence from the server-attached details.
/// The `message` field comes from `Status::message()`, which the server
/// sets to the plain-text fallback rendering.
pub struct CoralQueryError {
    /// Machine-readable error reason (e.g. `"MISSING_REQUIRED_FILTER"`).
    pub reason: String,
    /// Key-value metadata from `ErrorInfo.metadata`. Contains structured
    /// fields like `schema`, `table`, `field`, `source`, `http_status`,
    /// `http_method`, `url`, `hint`, `detail`.
    pub metadata: HashMap<String, String>,
    /// Whether the error is transient and retrying may succeed.
    pub retryable: bool,
    /// The plain-text message from `Status::message()`.
    pub message: String,
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
        Some(info) => DecodedStatusError::Structured(Box::new(CoralQueryError {
            reason: info.reason,
            metadata: info.metadata,
            retryable,
            message: status.message().to_string(),
        })),
        None => DecodedStatusError::Plain(status.message().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use super::{CORAL_ERROR_DOMAIN, DecodedStatusError, decode_status_error};

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

    #[test]
    fn plain_status_without_details_falls_back_to_message() {
        let status = Status::new(Code::FailedPrecondition, "raw legacy message");
        match decode_status_error(&status) {
            DecodedStatusError::Plain(message) => assert_eq!(message, "raw legacy message"),
            DecodedStatusError::Structured(_) => panic!("expected Plain"),
        }
    }

    #[test]
    fn unrelated_bytes_in_details_fall_back_to_message() {
        let status = Status::with_details(
            Code::Internal,
            "opaque upstream failure",
            b"not a google.rpc.Status payload".to_vec().into(),
        );
        match decode_status_error(&status) {
            DecodedStatusError::Plain(message) => assert_eq!(message, "opaque upstream failure"),
            DecodedStatusError::Structured(_) => panic!("expected Plain"),
        }
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

        match decode_status_error(&status) {
            DecodedStatusError::Plain(message) => assert_eq!(message, "not ours"),
            DecodedStatusError::Structured(_) => {
                panic!("wrong domain must not produce Structured")
            }
        }
    }

    #[test]
    fn coral_error_info_decodes_as_structured() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                ("schema", "github"),
                ("table", "issues"),
                ("field", "repo"),
                ("hint", "Add a constant equality filter on `repo`."),
            ],
            false,
        );
        match decode_status_error(&status) {
            DecodedStatusError::Structured(err) => {
                assert_eq!(err.reason, "MISSING_REQUIRED_FILTER");
                assert_eq!(err.metadata.get("schema").unwrap(), "github");
                assert_eq!(err.metadata.get("table").unwrap(), "issues");
                assert_eq!(err.metadata.get("field").unwrap(), "repo");
                assert!(
                    err.metadata
                        .get("hint")
                        .unwrap()
                        .contains("equality filter")
                );
                assert!(!err.retryable);
                assert_eq!(err.message, "test message");
            }
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        }
    }

    #[test]
    fn retry_info_sets_retryable_flag() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![("http_status", "500")],
            true,
        );
        match decode_status_error(&status) {
            DecodedStatusError::Structured(err) => {
                assert!(err.retryable);
                assert_eq!(err.reason, "PROVIDER_REQUEST_FAILED");
            }
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        }
    }

    #[test]
    fn metadata_carries_all_structured_fields() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("source", "github"),
                ("table", "issues"),
                ("http_status", "401"),
                ("http_method", "GET"),
                ("url", "https://api.github.com/repos/coral/coral/issues"),
                ("hint", "Re-install the source to refresh credentials."),
                ("detail", "bad credentials"),
            ],
            false,
        );
        match decode_status_error(&status) {
            DecodedStatusError::Structured(err) => {
                assert_eq!(err.metadata.get("source").unwrap(), "github");
                assert_eq!(err.metadata.get("http_status").unwrap(), "401");
                assert_eq!(err.metadata.get("http_method").unwrap(), "GET");
                assert!(err.metadata.get("hint").unwrap().contains("Re-install"));
                assert_eq!(err.metadata.get("detail").unwrap(), "bad credentials");
            }
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        }
    }

    #[test]
    fn empty_details_bytes_fall_back_even_with_non_empty_message() {
        let status = Status::new(Code::NotFound, "resource not found: github.issues");
        assert!(status.details().is_empty());
        match decode_status_error(&status) {
            DecodedStatusError::Plain(message) => {
                assert_eq!(message, "resource not found: github.issues");
            }
            DecodedStatusError::Structured(_) => panic!("expected Plain"),
        }
    }
}
