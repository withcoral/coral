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

/// Reserved `ErrorInfo.metadata` key for a one-line error summary.
pub const CORAL_ERROR_METADATA_SUMMARY: &str = "summary";

/// Reserved `ErrorInfo.metadata` key for a longer error explanation.
pub const CORAL_ERROR_METADATA_DETAIL: &str = "detail";

/// Reserved `ErrorInfo.metadata` key for actionable recovery guidance.
pub const CORAL_ERROR_METADATA_HINT: &str = "hint";
