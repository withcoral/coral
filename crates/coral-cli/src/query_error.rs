//! Structured error rendering for the `coral sql` command.
//!
//! Decodes AIP-193 error details via `coral_client::decode_status_error`
//! and returns a human-readable `Error:` / `Detail:` / `Hint:` block. The
//! `Plain` variant writes the server message verbatim. This module has no
//! side effects — callers own stderr emission and process termination.

use std::fmt::Write as _;

use coral_api::grpc_response_status_code;
use coral_client::{CoralQueryError, DecodedStatusError, decode_status_error};

/// Renders a `tonic::Status` as a user-facing stderr block.
///
/// Structured errors produce labelled `Error:` / `Detail:` / `Hint:` lines.
/// Plain fallback errors use the same labelled block with a generic hint so
/// legacy server statuses stay consistent with structured diagnostics. The
/// returned string always terminates with a newline.
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

pub(crate) fn telemetry_error_type(status: &tonic::Status) -> String {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => error.reason,
        DecodedStatusError::Plain(_) => grpc_response_status_code(status.code()).to_string(),
    }
}

pub(crate) fn telemetry_error_message(status: &tonic::Status) -> String {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => error.summary,
        DecodedStatusError::Plain(message) => message,
    }
}

fn render_plain(code: tonic::Code, message: &str) -> String {
    let (summary, hint) = plain_diagnostic(code);
    format!("Error: {summary}\nDetail: {message}\nHint: {hint}\n")
}

fn plain_diagnostic(code: tonic::Code) -> (&'static str, &'static str) {
    match code {
        tonic::Code::InvalidArgument => (
            "Query request is invalid",
            "Check the SQL syntax and retry. Use `SELECT * FROM coral.tables LIMIT 10` to inspect available tables.",
        ),
        tonic::Code::NotFound => (
            "Query target was not found",
            "Use `SELECT * FROM coral.tables LIMIT 10` to confirm the available source, schema, and table names.",
        ),
        tonic::Code::FailedPrecondition => (
            "Query prerequisites are not satisfied",
            "Check source setup and required filters, then retry the query.",
        ),
        tonic::Code::Unavailable => (
            "Query runtime is unavailable",
            "Retry once the local Coral server is running.",
        ),
        tonic::Code::Unimplemented => (
            "Query feature is not supported",
            "Adjust the SQL to use supported read-only query features, then retry.",
        ),
        _ => (
            "Query failed",
            "Retry the query. If it keeps failing, run `coral source test <source>` for the source you are querying.",
        ),
    }
}

fn render_structured(error: &CoralQueryError) -> String {
    let mut text = format!("Error: {}", error.summary);
    if !error.detail.is_empty() {
        write!(text, "\nDetail: {}", error.detail).expect("writing to String cannot fail");
    }
    let hint =
        crate::cli_hint_for_reason(&error.reason, &error.metadata).or_else(|| error.hint.clone());
    if let Some(hint) = hint {
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
                ("column", "repo"),
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
    fn structured_telemetry_uses_reason_and_summary() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                (
                    "summary",
                    "github.files requires `WHERE pull_number = <constant>`",
                ),
                ("detail", "missing required filter"),
            ],
            false,
        );

        assert_eq!(telemetry_error_type(&status), "MISSING_REQUIRED_FILTER");
        assert_eq!(
            telemetry_error_message(&status),
            "github.files requires `WHERE pull_number = <constant>`"
        );
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
    fn structured_app_reason_uses_cli_specific_hint() {
        let status = build_coral_status(
            "SOURCE_NOT_FOUND",
            vec![
                ("summary", "Source `github` was not found"),
                ("detail", "No source named `github` is installed."),
                ("hint", "List installed sources, then retry."),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);

        assert!(rendered.contains("Hint: Run `coral source list`"));
        assert!(!rendered.contains("List installed sources, then retry."));
    }

    #[test]
    fn structured_empty_sql_uses_cli_specific_hint() {
        let status = build_coral_status(
            "EMPTY_SQL",
            vec![
                ("summary", "SQL query is empty"),
                ("detail", "Coral cannot run an empty SQL string."),
                (
                    "hint",
                    "Try the SQL statement `SELECT * FROM coral.tables LIMIT 10`.",
                ),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);

        assert!(rendered.contains("Hint: Run `coral sql"));
        assert!(!rendered.contains("Try the SQL statement"));
    }

    #[test]
    fn structured_empty_catalog_table_not_found_uses_cli_hint() {
        let status = build_coral_status(
            "TABLE_NOT_FOUND",
            vec![
                ("summary", "Table `github.issues` not found"),
                ("detail", "No table `issues` exists in schema `github`."),
                ("catalog_empty", "true"),
                (
                    "hint",
                    "No source tables are currently queryable. Discover available sources, connect one, then retry the query.",
                ),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);

        assert!(rendered.contains("Hint: Run `coral source discover`"));
        assert!(!rendered.contains("Discover available sources, connect one"));
    }

    #[test]
    fn structured_provider_auth_failure_uses_cli_hint() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source credentials were rejected"),
                (
                    "detail",
                    "The upstream API rejected the saved credentials for `github`.",
                ),
                (
                    "hint",
                    "Refresh the saved credentials for `github`. If this is an imported source, refresh it from the manifest used to install it.",
                ),
                ("source", "github"),
                ("http_status", "401"),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);

        assert!(rendered.contains("Hint: Run `coral source add github --interactive`"));
        assert!(rendered.contains("coral source add --file <manifest-path> --interactive"));
        assert!(!rendered.contains("Refresh the saved credentials for `github`"));
    }

    #[test]
    fn structured_provider_forbidden_keeps_access_hint() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source access was denied"),
                (
                    "detail",
                    "The upstream API rejected the saved credentials for `github`.",
                ),
                (
                    "hint",
                    "Refresh the saved credentials for `github`. If the refreshed credential still fails, check that it has access to this resource.",
                ),
                ("source", "github"),
                ("http_status", "403"),
            ],
            false,
        );
        let error = match decode_status_error(&status) {
            DecodedStatusError::Structured(e) => e,
            DecodedStatusError::Plain(_) => panic!("expected Structured"),
        };
        let rendered = render_structured(&error);

        assert!(rendered.contains("Hint: Run `coral source add github --interactive`"));
        assert!(rendered.contains("check that it has access to this resource"));
        assert!(!rendered.contains("Refresh the saved credentials for `github`"));
    }

    #[test]
    fn plain_status_renders_diagnostic_block() {
        let rendered = render_plain(Code::Internal, "legacy opaque failure");
        assert!(rendered.contains("Error: Query failed"));
        assert!(rendered.contains("Detail: legacy opaque failure"));
        assert!(rendered.contains("Hint: Retry the query."));
    }

    #[test]
    fn plain_telemetry_uses_grpc_code_and_message() {
        let status = Status::new(Code::FailedPrecondition, "missing source setup");

        assert_eq!(telemetry_error_type(&status), "FAILED_PRECONDITION");
        assert_eq!(telemetry_error_message(&status), "missing source setup");
    }

    #[test]
    fn plain_not_found_has_discovery_hint() {
        let rendered = render_plain(Code::NotFound, "resource not found: github.issues");
        assert!(rendered.starts_with("Error: Query target was not found"));
        assert!(rendered.contains("github.issues"));
        assert!(rendered.contains("Hint: Use `SELECT * FROM coral.tables LIMIT 10`"));
    }

    #[test]
    fn plain_unavailable_has_runtime_hint() {
        let rendered = render_plain(Code::Unavailable, "transport error");
        assert!(rendered.contains("Error: Query runtime is unavailable"));
        assert!(rendered.contains("Detail: transport error"));
        assert!(rendered.contains("Hint: Retry once the local Coral server is running."));
    }

    #[test]
    fn plain_fallback_preserves_multi_line_server_message() {
        let multi_line = "Source authentication failed (401)\nbad credentials [GET] https://api.github.com/issues\nHint: Re-install the source.";
        let rendered = render_plain(Code::FailedPrecondition, multi_line);
        assert!(rendered.starts_with("Error: Query prerequisites are not satisfied"));
        assert!(rendered.contains("Source authentication failed (401)"));
        assert!(rendered.contains("Hint: Re-install the source."));
    }
}
