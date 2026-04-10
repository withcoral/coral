//! Error surfaces for Coral client bootstrap and query result decoding.

use coral_engine::QueryError;

/// Errors surfaced while bootstrapping a Coral client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Building the local server failed.
    #[error(transparent)]
    App(#[from] coral_app::AppError),
    /// Connecting the generated gRPC client failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Errors surfaced while decoding or rendering query results.
#[derive(Debug, thiserror::Error)]
pub enum QueryResultError {
    /// Arrow IPC decoding or rendering failed.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// `JSON` encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// `UTF-8` decoding failed.
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    /// The server returned an invalid query result payload.
    #[error("invalid query result: {0}")]
    InvalidResponse(String),
}

/// Extracts a structured [`QueryError`] from the `details` payload of a
/// `tonic::Status`, if one is present.
///
/// The engine and app attach a JSON-encoded `QueryError` to query failures via
/// `coral_app::bootstrap::status_with_query_error`. CLI and MCP consumers call
/// this helper to recover the structured error so they can render a hint
/// instead of the bare `Status::message()`.
///
/// Returns `None` when the status has no details, when the bytes don't parse
/// as JSON, or when the payload's `schema_version` doesn't match the expected
/// [`coral_engine::SCHEMA_VERSION`]. Callers should fall back to the plain
/// `Status::message()` in that case.
#[must_use]
pub fn query_error_from_status(status: &tonic::Status) -> Option<QueryError> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    QueryError::from_json_bytes(details)
}

#[cfg(test)]
mod tests {
    use super::query_error_from_status;
    use coral_engine::{QueryError, QueryErrorCode};
    use tonic::{Code, Status};

    #[test]
    fn returns_none_for_plain_status() {
        let status = Status::new(Code::FailedPrecondition, "plain message");
        assert!(query_error_from_status(&status).is_none());
    }

    #[test]
    fn returns_none_when_details_are_unrelated_bytes() {
        let status = Status::with_details(
            Code::Internal,
            "legacy",
            b"not a query error".to_vec().into(),
        );
        assert!(query_error_from_status(&status).is_none());
    }

    #[test]
    fn extracts_query_error_from_attached_details() {
        let query_error = QueryError::unknown_field("team_key", "Did you mean `team_id`?");
        let status = Status::with_details(
            Code::NotFound,
            query_error.summary.clone(),
            query_error.to_json_bytes().into(),
        );
        let extracted = query_error_from_status(&status).expect("status carries a QueryError");
        assert_eq!(extracted.code, QueryErrorCode::UnknownField);
        assert_eq!(extracted.fields.field.as_deref(), Some("team_key"));
        assert_eq!(extracted.hint.as_deref(), Some("Did you mean `team_id`?"));
    }
}
