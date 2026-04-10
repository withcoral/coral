//! Structured query error contract with user- and agent-facing hints.
//!
//! [`QueryError`] is the rich counterpart to [`super::CoreError`]: `CoreError`
//! classifies a failure into a gRPC-mappable bucket, `QueryError` carries the
//! summary, detail, actionable hint, and structured fields that downstream
//! consumers need to explain the failure to a human or agent.
//!
//! The type is transport-neutral. [`QueryError::to_json_bytes`] and
//! [`QueryError::from_json_bytes`] produce the wire format consumed by the
//! `coral-app` status encoder and the `coral-client` status decoder; the
//! [`SCHEMA_VERSION`] sentinel keeps decoders from mistaking unrelated
//! `tonic::Status` details payloads for ours.

use serde::{Deserialize, Serialize};

/// Wire-format sentinel carried as the `schema_version` field on every encoded
/// [`QueryError`]. Decoders reject payloads whose `schema_version` does not
/// match, so unrelated bytes in `tonic::Status::details()` don't get mistaken
/// for a structured query error.
pub const SCHEMA_VERSION: &str = "coral.query_error.v1";

/// Classification of a query-time failure.
///
/// Trimmed for the local-only release candidate — server, auth, and budget
/// codes from the pre-release taxonomy are intentionally omitted until the
/// corresponding features ship.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryErrorCode {
    /// Catch-all for failures that don't fit another code.
    #[default]
    Unknown,
    /// The caller submitted an empty SQL string.
    EmptyQuery,
    /// The SQL parser or logical planner rejected the statement.
    SqlError,
    /// The table was queried without a filter the upstream API requires.
    MissingRequiredFilter,
    /// The query referenced a column that does not exist on the target table.
    UnknownField,
    /// The query referenced a schema or table that does not exist.
    TableNotFound,
    /// The query shape is otherwise invalid for the target table.
    InvalidQueryShape,
    /// An upstream HTTP source returned an error response.
    ProviderRequestFailed,
}

/// Structured fields attached to a [`QueryError`] for programmatic consumers.
///
/// Every field is optional; constructors populate whichever make sense for the
/// failure. Automated callers can pattern-match on these instead of parsing
/// the human-readable summary or detail.
#[derive(Debug, Clone, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct QueryErrorFields {
    /// Schema (source) the failing table belongs to, e.g. `github`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Table the query was targeting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Column or filter field the error refers to, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Source name, echoed separately from `schema` for provider-level errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// HTTP status returned by the upstream source, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    /// HTTP method used for the failing upstream request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    /// Fully qualified URL of the failing upstream request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl QueryErrorFields {
    /// Returns `true` when every field is `None`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schema.is_none()
            && self.table.is_none()
            && self.field.is_none()
            && self.source.is_none()
            && self.http_status.is_none()
            && self.http_method.is_none()
            && self.url.is_none()
    }
}

/// A user- and agent-facing query error with a hint and structured fields.
///
/// Constructed at the failure origin (engine, backend, or app layer) and
/// serialized into `tonic::Status::details()` via [`QueryError::to_json_bytes`]
/// so consumers can render a helpful message instead of a bare status string.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct QueryError {
    /// Wire-format sentinel. See [`SCHEMA_VERSION`].
    ///
    /// Required on decode — payloads missing this field are rejected by
    /// [`QueryError::from_json_bytes`] so unrelated JSON in
    /// `tonic::Status::details()` cannot be misclassified as a structured
    /// query error.
    pub schema_version: String,
    /// Classification code for the failure.
    #[serde(default)]
    pub code: QueryErrorCode,
    /// Short, human-readable summary of what failed.
    pub summary: String,
    /// Longer-form detail for the user or agent. May be empty when the summary
    /// already says everything useful.
    pub detail: String,
    /// Optional actionable hint for recovering from the failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Whether the same query may succeed on retry (e.g. upstream 5xx, rate
    /// limits). Interactive renderers can surface this flag in their UI, and
    /// automated callers can consult it before retrying.
    #[serde(default)]
    pub retryable: bool,
    /// Structured fields for programmatic consumers.
    #[serde(default, skip_serializing_if = "QueryErrorFields::is_empty")]
    pub fields: QueryErrorFields,
}

/// Escapes a value for use inside an ANSI SQL single-quoted literal.
///
/// Replaces every single quote with two single quotes and wraps the result in
/// single quotes. Keeps copy-paste-able SQL in hints valid even when source
/// names contain apostrophes (e.g. `o'brien` → `'o''brien'`).
fn sql_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

/// Quotes a value for safe use as a POSIX shell argument.
///
/// Returns the value verbatim when it's already shell-safe (alphanumeric,
/// underscore, hyphen). Otherwise wraps it in single quotes and escapes any
/// internal single quotes using the canonical `'\''` sequence. Source names
/// currently allow spaces and quotes — this helper keeps the `coral source
/// add <name>` snippets in hints copy-pasteable.
fn shell_arg(value: &str) -> String {
    let is_safe = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if is_safe {
        value.to_string()
    } else {
        let escaped = value.replace('\'', "'\\''");
        format!("'{escaped}'")
    }
}

impl QueryError {
    /// Creates a new error with the given code, summary, and detail. Prefer
    /// the named constructors below — they populate hints and fields
    /// consistently across backends.
    #[must_use]
    pub fn new(
        code: QueryErrorCode,
        summary: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            code,
            summary: summary.into(),
            detail: detail.into(),
            hint: None,
            retryable: false,
            fields: QueryErrorFields::default(),
        }
    }

    /// Attaches a recovery hint to the error.
    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Marks the error as retryable.
    #[must_use]
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// Replaces the structured fields.
    #[must_use]
    pub fn with_fields(mut self, fields: QueryErrorFields) -> Self {
        self.fields = fields;
        self
    }

    /// Catch-all constructor for failures without a better classification.
    #[must_use]
    pub fn unknown(detail: impl Into<String>) -> Self {
        Self::new(QueryErrorCode::Unknown, "Query execution failed", detail).with_hint(
            "Inspect the query, then check `coral.columns` or `coral.tables` for table-specific guidance.",
        )
    }

    /// Constructor for an empty SQL submission.
    #[must_use]
    pub fn empty_query() -> Self {
        Self::new(
            QueryErrorCode::EmptyQuery,
            "Query execution failed",
            "SQL must not be empty.",
        )
    }

    /// Constructor for SQL parser or planner errors.
    #[must_use]
    pub fn sql_error(detail: impl Into<String>) -> Self {
        Self::new(
            QueryErrorCode::SqlError,
            "SQL parser or planner error",
            detail,
        )
        .with_hint("Check the SQL syntax near the reported position and retry.")
    }

    /// Constructor for query shapes the target table cannot fulfill (for
    /// example, an HTTP 400 returned by the upstream source).
    #[must_use]
    pub fn invalid_query_shape(detail: impl Into<String>) -> Self {
        Self::new(
            QueryErrorCode::InvalidQueryShape,
            "Query shape is not valid for this table",
            detail,
        )
        .with_hint(
            "Adjust the query filters or shape to match the target table's supported inputs.",
        )
    }

    /// Constructor for tables that require an equality filter on a specific
    /// field because the upstream API demands it.
    #[must_use]
    pub fn missing_required_filter(
        schema: impl Into<String>,
        table: impl Into<String>,
        field: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let schema = schema.into();
        let table = table.into();
        let field = field.into();
        Self::new(
            QueryErrorCode::MissingRequiredFilter,
            format!("{schema}.{table} requires `WHERE {field} = <constant>`"),
            detail,
        )
        .with_hint(format!(
            "Add a constant equality filter on `{field}` or inspect `coral.columns` / `coral.tables` first."
        ))
        .with_fields(QueryErrorFields {
            schema: Some(schema),
            table: Some(table),
            field: Some(field),
            ..QueryErrorFields::default()
        })
    }

    /// Constructor for unknown-column errors. `hint` is expected to carry a
    /// "Did you mean `X`?" suggestion derived from the available column list.
    #[must_use]
    pub fn unknown_field(field: impl Into<String>, hint: impl Into<String>) -> Self {
        let field = field.into();
        Self::new(
            QueryErrorCode::UnknownField,
            format!("Unknown field `{field}`"),
            "",
        )
        .with_hint(hint)
        .with_fields(QueryErrorFields {
            field: Some(field),
            ..QueryErrorFields::default()
        })
    }

    /// Constructor for table-not-found errors. Emits a schema-aware hint so
    /// users know whether to check `coral.tables` or install the source.
    ///
    /// Source and schema names may contain spaces, quotes, or other shell
    /// metacharacters (workspace name validation only rejects path separators
    /// and dots). Names embedded in copy-paste-able SQL literals and shell
    /// commands are routed through `sql_literal` and `shell_arg` so the
    /// rendered hint stays valid even for pathological names like `foo bar`
    /// or `foo'bar`.
    #[must_use]
    pub fn table_not_found(
        schema: impl Into<String>,
        table: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let schema = schema.into();
        let table = table.into();
        let schema_sql = sql_literal(&schema);
        let schema_shell = shell_arg(&schema);
        let hint = match schema.as_str() {
            "coral" => format!(
                "No system table named `{table}`. Run `SELECT table_name FROM coral.tables WHERE schema_name = 'coral'` to see available system tables."
            ),
            "public" => format!(
                "No table `{table}` exists. Use a schema prefix (e.g., `<source>.{table}`). Run `SELECT schema_name, table_name FROM coral.tables` to see available tables."
            ),
            _ => format!(
                "No table `{table}` in schema `{schema}`. The source may not be installed. Check with `SELECT * FROM coral.tables WHERE schema_name = {schema_sql}` or add the source with `coral source add {schema_shell}`."
            ),
        };

        Self::new(
            QueryErrorCode::TableNotFound,
            format!("Table '{schema}.{table}' not found"),
            detail,
        )
        .with_hint(hint)
        .with_fields(QueryErrorFields {
            schema: Some(schema),
            table: Some(table),
            ..QueryErrorFields::default()
        })
    }

    /// Constructor for failures returned by the upstream HTTP source. Dispatches
    /// on HTTP status: 400 → invalid query shape, 401/403 → auth, 404 → not
    /// found, 429 and 5xx → retryable server errors.
    #[must_use]
    pub fn provider_request(
        source: impl Into<String>,
        table: impl Into<String>,
        status: Option<u16>,
        method: Option<String>,
        url: Option<String>,
        detail: impl Into<String>,
    ) -> Self {
        let source = source.into();
        let table = table.into();
        let detail = detail.into();
        let source_shell = shell_arg(&source);
        let (code, summary, hint) = match status {
            Some(400) => (
                QueryErrorCode::InvalidQueryShape,
                "Source rejected the request".to_string(),
                Some(
                    "Adjust the query filters or shape to match the target table's supported inputs.".to_string(),
                ),
            ),
            Some(401) => (
                QueryErrorCode::ProviderRequestFailed,
                "Source authentication failed".to_string(),
                Some(format!(
                    "Re-run `coral source add {source_shell}` to refresh credentials."
                )),
            ),
            Some(403) => (
                QueryErrorCode::ProviderRequestFailed,
                "Source request was rejected".to_string(),
                Some(
                    "Check the configured credentials and whether they have access to this resource.".to_string(),
                ),
            ),
            Some(404) => (
                QueryErrorCode::ProviderRequestFailed,
                "Source resource was not found".to_string(),
                Some(
                    "Verify the identifier or filter values you passed; the upstream resource was not found.".to_string(),
                ),
            ),
            Some(429) => (
                QueryErrorCode::ProviderRequestFailed,
                "Source rate limit exceeded".to_string(),
                Some(
                    "The upstream API is rate-limiting requests. Wait briefly and retry.".to_string(),
                ),
            ),
            Some(s) if (500..600).contains(&s) => (
                QueryErrorCode::ProviderRequestFailed,
                "Source server error".to_string(),
                Some(
                    "The upstream API returned a server error. This may be transient — retry after a brief wait.".to_string(),
                ),
            ),
            _ => (
                QueryErrorCode::ProviderRequestFailed,
                "Source request failed".to_string(),
                None,
            ),
        };
        let summary = match status {
            Some(status) => format!("{summary} ({status})"),
            None => summary,
        };

        let is_retryable = matches!(status, Some(429 | 500..=599));
        let mut error = Self::new(code, summary, detail).with_fields(QueryErrorFields {
            source: Some(source),
            table: Some(table),
            http_status: status,
            http_method: method,
            url,
            ..QueryErrorFields::default()
        });
        if let Some(hint) = hint {
            error = error.with_hint(hint);
        }
        if is_retryable {
            error = error.retryable();
        }
        error
    }

    /// Renders a plain-text message that preserves the summary, detail, and
    /// hint in a single string.
    ///
    /// Used as the fallback `Status::message()` so proxies or client
    /// version-skew that strip `Status::details()` still deliver the actionable
    /// content to the user. Renderers may also fall back to this when
    /// structured rendering is unavailable.
    #[must_use]
    pub fn to_plain_message(&self) -> String {
        let mut message = self.summary.clone();
        if !self.detail.is_empty() {
            message.push('\n');
            message.push_str(&self.detail);
        }
        if let Some(hint) = &self.hint {
            message.push_str("\nHint: ");
            message.push_str(hint);
        }
        message
    }

    /// Serializes the error to JSON bytes for attachment to a `tonic::Status`.
    /// The output always carries a `schema_version` field so decoders can tell
    /// it apart from unrelated status details payloads.
    ///
    /// # Panics
    ///
    /// Panics only if `serde_json` fails to serialize the struct, which is
    /// impossible given the schema — all fields are plain `String`, `u16`,
    /// `bool`, or `Option` of those.
    #[must_use]
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("QueryError is always serializable")
    }

    /// Decodes a [`QueryError`] from JSON bytes. Returns `None` if the bytes
    /// don't parse as JSON, don't match the struct shape, or carry a
    /// `schema_version` other than [`SCHEMA_VERSION`] — so unrelated payloads
    /// in `tonic::Status::details()` are safely ignored.
    #[must_use]
    pub fn from_json_bytes(bytes: &[u8]) -> Option<Self> {
        let error: Self = serde_json::from_slice(bytes).ok()?;
        (error.schema_version == SCHEMA_VERSION).then_some(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryError, QueryErrorCode, QueryErrorFields, SCHEMA_VERSION};

    #[test]
    fn missing_required_filter_sets_code_and_fields() {
        let error = QueryError::missing_required_filter(
            "github",
            "issues",
            "repo",
            "missing required filter",
        );
        assert_eq!(error.code, QueryErrorCode::MissingRequiredFilter);
        assert_eq!(error.fields.schema.as_deref(), Some("github"));
        assert_eq!(error.fields.table.as_deref(), Some("issues"));
        assert_eq!(error.fields.field.as_deref(), Some("repo"));
        assert!(error.summary.contains("repo"));
        assert!(error.hint.is_some());
    }

    #[test]
    fn unknown_field_sets_code_and_hint() {
        let error = QueryError::unknown_field("team_key", "Did you mean `team_id`?");
        assert_eq!(error.code, QueryErrorCode::UnknownField);
        assert!(error.summary.contains("team_key"));
        assert_eq!(error.hint.as_deref(), Some("Did you mean `team_id`?"));
        assert_eq!(error.fields.field.as_deref(), Some("team_key"));
    }

    #[test]
    fn unknown_sets_generic_fallback_hint() {
        let error = QueryError::unknown("opaque upstream failure");
        assert_eq!(error.code, QueryErrorCode::Unknown);
        assert_eq!(error.detail, "opaque upstream failure");
        assert!(error.hint.is_some());
    }

    #[test]
    fn table_not_found_public_schema_uses_public_hint() {
        let error = QueryError::table_not_found("public", "logs", "table not found");
        assert_eq!(error.code, QueryErrorCode::TableNotFound);
        assert_eq!(error.summary, "Table 'public.logs' not found");
        let hint = error.hint.expect("should have a hint");
        assert!(hint.contains("Use a schema prefix"));
        assert!(hint.contains("coral.tables"));
    }

    #[test]
    fn table_not_found_coral_schema_uses_system_hint() {
        let error = QueryError::table_not_found("coral", "nope", "table not found");
        let hint = error.hint.expect("should have a hint");
        assert!(hint.contains("No system table named `nope`"));
        assert!(hint.contains("schema_name = 'coral'"));
    }

    #[test]
    fn table_not_found_source_schema_suggests_source_add() {
        let error = QueryError::table_not_found("datadog", "dashboards", "table not found");
        let hint = error.hint.expect("should have a hint");
        assert!(hint.contains("source may not be installed"));
        assert!(
            hint.contains("coral source add datadog"),
            "hint should guide the user to `coral source add`"
        );
    }

    #[test]
    fn provider_request_401_suggests_source_add_to_reauth() {
        let error = QueryError::provider_request(
            "github",
            "issues",
            Some(401),
            Some("GET".to_string()),
            Some("https://api.github.com/repos/coral/coral/issues".to_string()),
            "Bad credentials",
        );
        assert_eq!(error.code, QueryErrorCode::ProviderRequestFailed);
        assert_eq!(error.fields.http_status, Some(401));
        assert_eq!(error.fields.http_method.as_deref(), Some("GET"));
        assert!(error.summary.contains("Source authentication failed"));
        let hint = error.hint.expect("401 should have a hint");
        assert!(
            hint.contains("coral source add github"),
            "401 hint should guide the user to `coral source add`"
        );
        assert!(!error.retryable, "401 is not retryable");
    }

    #[test]
    fn provider_request_400_maps_to_invalid_query_shape() {
        let error = QueryError::provider_request(
            "datadog",
            "events",
            Some(400),
            Some("GET".to_string()),
            Some("https://api.datadoghq.eu/api/v1/events".to_string()),
            "invalid request",
        );
        assert_eq!(error.code, QueryErrorCode::InvalidQueryShape);
        assert!(error.summary.contains("Source rejected the request"));
    }

    #[test]
    fn provider_request_429_is_retryable() {
        let error = QueryError::provider_request(
            "datadog",
            "monitors",
            Some(429),
            Some("GET".to_string()),
            None,
            "rate limited",
        );
        assert!(error.retryable);
        assert!(error.summary.contains("rate limit"));
    }

    #[test]
    fn provider_request_500_is_retryable() {
        let error = QueryError::provider_request(
            "github",
            "issues",
            Some(500),
            None,
            None,
            "internal error",
        );
        assert!(error.retryable);
        assert!(error.summary.contains("server error"));
    }

    #[test]
    fn sql_error_is_not_retryable() {
        let error = QueryError::sql_error("syntax error at position 5");
        assert!(!error.retryable);
        assert!(error.hint.is_some());
    }

    #[test]
    fn empty_query_has_expected_message() {
        let error = QueryError::empty_query();
        assert_eq!(error.code, QueryErrorCode::EmptyQuery);
        assert!(error.detail.contains("must not be empty"));
    }

    #[test]
    fn invalid_query_shape_has_hint() {
        let error = QueryError::invalid_query_shape("column type mismatch");
        assert_eq!(error.code, QueryErrorCode::InvalidQueryShape);
        assert!(error.hint.is_some());
    }

    #[test]
    fn round_trips_through_json_bytes() {
        let error = QueryError::missing_required_filter(
            "github",
            "issues",
            "repo",
            "missing required filter",
        );
        let bytes = error.to_json_bytes();
        let decoded = QueryError::from_json_bytes(&bytes).expect("valid bytes should decode");
        assert_eq!(decoded, error);
    }

    #[test]
    fn from_json_bytes_rejects_unrelated_json() {
        let bytes = br#"{"foo": "bar"}"#;
        assert!(QueryError::from_json_bytes(bytes).is_none());
    }

    #[test]
    fn from_json_bytes_rejects_payload_without_schema_version() {
        // Regression: a plausible-looking error payload without the sentinel
        // must be rejected. Otherwise unrelated JSON in `Status::details()`
        // could be misclassified as a structured query error.
        let bytes = br#"{"summary":"x","detail":"y"}"#;
        assert!(
            QueryError::from_json_bytes(bytes).is_none(),
            "decoder must reject payloads missing schema_version"
        );
    }

    #[test]
    fn from_json_bytes_rejects_mismatched_schema_version() {
        let bytes = br#"{"schema_version": "coral.query_error.v999", "code": "unknown", "summary": "x", "detail": "y"}"#;
        assert!(
            QueryError::from_json_bytes(bytes).is_none(),
            "decoder must reject unknown schema versions"
        );
    }

    #[test]
    fn from_json_bytes_accepts_missing_optional_fields() {
        let bytes = br#"{"schema_version": "coral.query_error.v1", "summary": "x", "detail": "y"}"#;
        let error = QueryError::from_json_bytes(bytes).expect("minimal payload should decode");
        assert_eq!(error.code, QueryErrorCode::Unknown);
        assert!(!error.retryable);
        assert!(error.fields.is_empty());
    }

    #[test]
    fn query_error_round_trips_with_all_fields() {
        let error = QueryError::new(
            QueryErrorCode::ProviderRequestFailed,
            "Source request failed (500)",
            "upstream timed out",
        )
        .with_hint("Retry the request later.")
        .retryable()
        .with_fields(QueryErrorFields {
            schema: Some("github".to_string()),
            table: Some("issues".to_string()),
            field: Some("repo".to_string()),
            source: Some("github".to_string()),
            http_status: Some(500),
            http_method: Some("GET".to_string()),
            url: Some("https://api.github.com/repos/coral/coral/issues".to_string()),
        });

        let bytes = error.to_json_bytes();
        let decoded = QueryError::from_json_bytes(&bytes).expect("full payload should decode");
        assert_eq!(decoded, error);
    }

    #[test]
    fn query_error_fields_is_empty_detects_populated_state() {
        let empty = QueryErrorFields::default();
        assert!(empty.is_empty());
        let populated = QueryErrorFields {
            schema: Some("github".to_string()),
            ..QueryErrorFields::default()
        };
        assert!(!populated.is_empty());
    }

    #[test]
    fn schema_version_constant_is_stable() {
        assert_eq!(SCHEMA_VERSION, "coral.query_error.v1");
    }

    #[test]
    fn to_plain_message_includes_summary_detail_and_hint() {
        let error = QueryError::missing_required_filter(
            "github",
            "issues",
            "repo",
            "missing required filter",
        );
        let text = error.to_plain_message();
        assert!(
            text.contains(&error.summary),
            "plain message should include the summary"
        );
        assert!(
            text.contains("missing required filter"),
            "plain message should preserve the detail"
        );
        assert!(
            text.contains("Hint: "),
            "plain message should label the hint"
        );
        assert!(
            text.contains("coral.columns"),
            "plain message should include the hint content"
        );
    }

    #[test]
    fn to_plain_message_omits_empty_detail_and_missing_hint() {
        let error = QueryError::new(QueryErrorCode::Unknown, "Only summary", "");
        let text = error.to_plain_message();
        assert_eq!(text, "Only summary");
    }

    #[test]
    fn sql_literal_escapes_embedded_single_quotes() {
        assert_eq!(super::sql_literal("github"), "'github'");
        assert_eq!(super::sql_literal("o'brien"), "'o''brien'");
        assert_eq!(super::sql_literal(""), "''");
    }

    #[test]
    fn shell_arg_returns_safe_names_verbatim() {
        assert_eq!(super::shell_arg("github"), "github");
        assert_eq!(super::shell_arg("foo-bar_1"), "foo-bar_1");
    }

    #[test]
    fn shell_arg_quotes_names_with_spaces_or_quotes() {
        assert_eq!(super::shell_arg("foo bar"), "'foo bar'");
        assert_eq!(super::shell_arg("foo'bar"), "'foo'\\''bar'");
        assert_eq!(super::shell_arg(""), "''");
        // Names with dots are defensively quoted because we don't know the
        // caller's shell (zsh globs, bash completion, etc.).
        assert_eq!(super::shell_arg("foo.bar"), "'foo.bar'");
    }

    #[test]
    fn table_not_found_escapes_unsafe_schema_in_hint() {
        let error = QueryError::table_not_found("foo'bar", "things", "table not found");
        let hint = error.hint.expect("hint should be present");
        assert!(
            hint.contains("schema_name = 'foo''bar'"),
            "SQL literal should double single quotes, got: {hint}"
        );
        assert!(
            hint.contains("coral source add 'foo'\\''bar'"),
            "shell arg should be single-quoted with escaped quotes, got: {hint}"
        );
    }

    #[test]
    fn provider_request_401_quotes_unsafe_source_in_hint() {
        let error = QueryError::provider_request(
            "foo bar",
            "things",
            Some(401),
            None,
            None,
            "Bad credentials",
        );
        let hint = error.hint.expect("401 should have a hint");
        assert!(
            hint.contains("coral source add 'foo bar'"),
            "shell arg should quote names with spaces, got: {hint}"
        );
    }
}
