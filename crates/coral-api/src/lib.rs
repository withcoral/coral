//! Generated `protobuf` and `tonic` bindings for the Coral local API.
//!
//! This crate is the shared transport contract for the local Coral
//! application.
//! All request and response types are generated from the `coral.v1` protobuf
//! package, and the canonical import path is [`v1`].
//!
//! # Primary Surface
//!
//! - [`v1`] contains all generated messages, enums, and gRPC service traits.
//! - Sibling crates such as `coral-app`, `coral-engine`, and
//!   `coral-cli` and `coral-mcp` consume those generated types directly.
//!
//! # Example
//!
//! ```rust
//! use coral_api::v1::{ExecuteSqlRequest, Workspace};
//!
//! let request = ExecuteSqlRequest {
//!     workspace: Some(Workspace {
//!         name: "default".to_string(),
//!     }),
//!     sql: "select 1".to_string(),
//!     guide_read_context: None,
//!     task_attribution: None,
//! };
//! assert_eq!(request.sql, "select 1");
//! ```

#[expect(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    reason = "This module is generated from protobuf/tonic definitions."
)]
/// Generated `coral.v1` `protobuf` messages, enums, and `gRPC` services.
pub mod v1 {
    tonic::include_proto!("coral.v1");
}

/// Maximum gRPC message size for `QueryService` *responses*, in bytes.
///
/// `ExecuteSql` is a unary RPC that returns the full Arrow IPC result in
/// one message. Tonic's default of 4 MB is easily exceeded by wide
/// manifests like `github.search_issues`. Only the response direction
/// needs the bump — requests are small SQL strings.
pub const QUERY_RESPONSE_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Maximum gRPC message size for `CatalogService` *responses*, in bytes.
///
/// Catalog discovery can return full table/column metadata for large source
/// sets, and the legacy unbounded table-listing path still needs to round-trip
/// responses larger than tonic's 4 MB default.
pub const CATALOG_RESPONSE_MAX_MESSAGE_SIZE: usize = QUERY_RESPONSE_MAX_MESSAGE_SIZE;

/// Maximum gRPC message size for `SourceService` *responses*, in bytes.
///
/// Source validation returns source metadata plus all exposed tables. Generated
/// DSL v4 sources from large OpenAPI documents can exceed tonic's 4 MB default.
pub const SOURCE_RESPONSE_MAX_MESSAGE_SIZE: usize = CATALOG_RESPONSE_MAX_MESSAGE_SIZE;

/// Maximum gRPC message size for `IdentitySpecService` *responses*, in bytes.
///
/// Identity-spec listing returns an unpaginated summary aggregate. Large
/// authored descriptions or many installed specs can exceed tonic's 4 MB
/// default even though full manifest YAML is only returned by Add and Get.
pub const IDENTITY_SPEC_RESPONSE_MAX_MESSAGE_SIZE: usize = CATALOG_RESPONSE_MAX_MESSAGE_SIZE;

/// Maximum gRPC message size for `SearchService` *responses*, in bytes.
///
/// Universal Search returns bounded metadata hints, but it may include table,
/// table-function, and column descriptions for many installed sources.
pub const SEARCH_RESPONSE_MAX_MESSAGE_SIZE: usize = CATALOG_RESPONSE_MAX_MESSAGE_SIZE;

/// Maximum gRPC message size for `TraceService` *responses*, in bytes.
///
/// Trace details can include large span attributes, which may exceed tonic's
/// default 4 MB response cap.
pub const TRACE_RESPONSE_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` for the local Coral transport,
/// in bytes.
///
/// The hyper/h2 default (~16 KiB) is too small for some error trailers on
/// wide manifests even after we truncate `Status` details, and also caps
/// HPACK-encoded request headers on the way in. 128 KiB gives plenty of
/// headroom in both directions.
pub const HTTP2_MAX_HEADER_LIST_SIZE: u32 = 128 * 1024;

/// Coral error domain used in `google.rpc.ErrorInfo`.
pub const CORAL_ERROR_DOMAIN: &str = "coral.withcoral.com";

/// Canonical default workspace name used across local Coral surfaces.
pub const DEFAULT_WORKSPACE_ID: &str = "default";

/// gRPC metadata key carrying the originating task id on Coral calls after
/// `TaskService.StartTask`. This is the shared wire-contract constant for
/// clients and servers that opt into task attribution.
pub const CORAL_TASK_ID_METADATA_KEY: &str = "coral-task-id";

/// Maximum length of a Coral task id, in bytes. Validated identically server-
/// and client-side so the contract stays in one place.
pub const CORAL_TASK_ID_MAX_LEN: usize = 128;

/// Maximum length of a task `intent`, in characters. Generous for a task
/// description while bounding the per-record lifecycle log size.
pub const CORAL_TASK_INTENT_MAX_CHARS: usize = 4096;

/// Machine-readable reason for a configured source lookup miss.
pub const CORAL_ERROR_REASON_SOURCE_NOT_FOUND: &str = "SOURCE_NOT_FOUND";

/// Machine-readable reason for a configured function lookup miss.
pub const CORAL_ERROR_REASON_FUNCTION_NOT_FOUND: &str = "FUNCTION_NOT_FOUND";

/// Machine-readable reason for an installed identity-spec lookup miss.
pub const CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND: &str = "IDENTITY_SPEC_NOT_FOUND";

/// Machine-readable reason for a configured workspace lookup miss.
pub const CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND: &str = "WORKSPACE_NOT_FOUND";

/// Reserved `ErrorInfo.metadata` key for a one-line error summary.
pub const CORAL_ERROR_METADATA_SUMMARY: &str = "summary";

/// Reserved `ErrorInfo.metadata` key for a longer error explanation.
pub const CORAL_ERROR_METADATA_DETAIL: &str = "detail";

/// Reserved `ErrorInfo.metadata` key for actionable recovery guidance.
pub const CORAL_ERROR_METADATA_HINT: &str = "hint";

/// Returns the canonical OpenTelemetry `rpc.response.status_code` value.
#[must_use]
pub fn grpc_response_status_code(code: tonic::Code) -> &'static str {
    use tonic::Code;

    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}
