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

#[allow(
    clippy::allow_attributes_without_reason,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs,
    reason = "This module is generated from protobuf/tonic definitions."
)]
/// Generated `coral.v1` `protobuf` messages, enums, and `gRPC` services.
pub mod v1 {
    tonic::include_proto!("coral.v1");
}
