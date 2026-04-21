//! Structured error rendering for the `coral sql` command.
//!
//! Decodes AIP-193 error details via `coral_client::decode_status_error`
//! and returns a human-readable `Error:` / `Detail:` / `Hint:` block. The
//! `Plain` variant writes the server message verbatim. This module has no
//! side effects — callers own stderr emission and process termination.

use std::fmt::Write as _;

use coral_client::{CoralQueryError, DecodedStatusError, decode_status_error};

/// Renders a `tonic::Status` as a user-facing stderr block.
///
/// Structured errors produce labelled `Error:` / `Detail:` / `Hint:` lines.
/// Plain fallback errors include the gRPC status code so distinct failure
/// modes (transport, auth, not-found) remain distinguishable. The returned
/// string always terminates with a newline.
///
/// The caller is responsible for writing the result to stderr and exiting
/// with a non-zero code — keeping this function side-effect-free so the
/// process-termination site stays in `main`.
pub(crate) fn render_query_error(status: &tonic::Status) -> String {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => render_structured(&error),
        DecodedStatusError::Plain(message) => render_plain(status.code(), &message),
    }
}

fn render_plain(code: tonic::Code, message: &str) -> String {
    let label = grpc_code_label(code);
    format!("Error ({label}): {message}\n")
}

fn grpc_code_label(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::InvalidArgument => "invalid argument",
        tonic::Code::NotFound => "not found",
        tonic::Code::FailedPrecondition => "failed precondition",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal error",
        _ => "error",
    }
}

fn render_structured(error: &CoralQueryError) -> String {
    let mut text = format!("Error: {}", error.summary);
    if !error.detail.is_empty() {
        write!(text, "\nDetail: {}", error.detail).expect("writing to String cannot fail");
    }
    if let Some(hint) = &error.hint {
        write!(text, "\nHint: {hint}").expect("writing to String cannot fail");
    }
    text.push('\n');
    text
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use coral_client::CORAL_ERROR_DOMAIN;

    use super::*;

    /// Builds a `tonic::Status` with Coral AIP-193 error details.
    fn build_coral_status(reason: &str, metadata: Vec<(&str, &str)>, retryable: bool) -> Status {
        let meta: HashMap<String, String> = metadata
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut details: Vec<ErrorDetail> = vec![ErrorDetail::ErrorInfo(
            tonic_types::ErrorInfo::new(reason, CORAL_ERROR_DOMAIN, meta),
        )];
        if retryable {
            details.push(ErrorDetail::RetryInfo(tonic_types::RetryInfo::new(None)));
        }
        Status::with_error_details_vec(Code::FailedPrecondition, "plain fallback", details)
    }

    #[test]
    fn structured_renders_summary_detail_and_hint() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                (
                    "summary",
                    "github.issues requires `WHERE repo = <constant>`",
                ),
                ("detail", "missing required filter"),
                ("hint", "Add a constant equality filter on `repo`."),
                ("schema", "github"),
                ("table", "issues"),
                ("field", "repo"),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);
        assert!(rendered.starts_with("Error: github.issues requires"));
        assert!(rendered.contains("Detail: missing required filter"));
        assert!(rendered.contains("Hint: Add a constant equality filter"));
        assert!(rendered.ends_with('\n'));
    }

    #[test]
    fn structured_omits_detail_when_absent() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source request failed"),
                ("hint", "Retry after a brief wait."),
            ],
            true,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);
        assert!(rendered.contains("Error: Source request failed"));
        assert!(!rendered.contains("Detail:"));
        assert!(rendered.contains("Hint: Retry after a brief wait."));
    }

    #[test]
    fn structured_omits_hint_when_absent() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source request failed"),
                ("detail", "connection reset"),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);
        assert!(rendered.contains("Error: Source request failed"));
        assert!(rendered.contains("Detail: connection reset"));
        assert!(!rendered.contains("Hint:"));
    }

    #[test]
    fn structured_falls_back_to_message_when_no_summary_in_metadata() {
        let status =
            build_coral_status("PROVIDER_REQUEST_FAILED", vec![("source", "github")], false);
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);
        assert!(
            rendered.contains("Error: plain fallback"),
            "should fall back to Status::message(): {rendered}"
        );
    }

    #[test]
    fn plain_status_includes_grpc_code() {
        let rendered = render_plain(Code::Internal, "legacy opaque failure");
        assert_eq!(rendered, "Error (internal error): legacy opaque failure\n");
    }

    #[test]
    fn plain_not_found_shows_code() {
        let rendered = render_plain(Code::NotFound, "resource not found: github.issues");
        assert!(rendered.starts_with("Error (not found):"));
        assert!(rendered.contains("github.issues"));
    }

    #[test]
    fn plain_unavailable_shows_code() {
        let rendered = render_plain(Code::Unavailable, "transport error");
        assert_eq!(rendered, "Error (unavailable): transport error\n");
    }

    #[test]
    fn plain_fallback_preserves_multi_line_server_message() {
        let multi_line = "Source authentication failed (401)\nbad credentials [GET] https://api.github.com/issues\nHint: Re-install the source.";
        let rendered = render_plain(Code::FailedPrecondition, multi_line);
        assert!(rendered.starts_with("Error (failed precondition):"));
        assert!(rendered.contains("Source authentication failed (401)"));
        assert!(rendered.contains("Hint: Re-install the source."));
    }
}
