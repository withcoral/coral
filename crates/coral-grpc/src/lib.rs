//! Private native gRPC adapter. [`routes`] accepts application-constructed domain
//! modules and resolved transport context, and returns the complete route set.
//!
//! Owns wire contracts, request metadata, audience reachability, resource names,
//! safe status projection, health encoding, and per-service message limits.
//! Domain modules retain authorization and lifecycle decisions. Authentication
//! belongs to `coral-auth`; W3C propagation belongs to `coral-telemetry`.
//! Application composition owns readiness calculation, endpoint publication,
//! configuration, listeners, and shutdown. No listener or storage opens here.
//!
//! For example, changing the Provider Setup decoding allowance changes only the
//! registration in this module, not application bootstrap or test listeners.
#![allow(
    unused_crate_dependencies,
    reason = "generated code uses prost and tonic-prost"
)]

mod grpc;
mod health;

pub use grpc::{Dependencies, TransportContext, routes};
pub use health::{READINESS_SERVICE_NAME, ReadinessProbe};

/// Coral private wire namespaces.
#[allow(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    reason = "tonic-generated code uses allow attributes"
)]
pub mod coral {
    /// Internal MCP support contracts.
    pub mod internal {
        /// Version one.
        #[allow(
            missing_docs,
            clippy::all,
            clippy::pedantic,
            reason = "protoc-generated wire bindings"
        )]
        pub mod v1 {
            include!("generated/coral.internal.v1.rs");
        }
    }
    /// Reef management contracts.
    pub mod management {
        /// Version one.
        #[allow(
            missing_docs,
            clippy::all,
            clippy::pedantic,
            reason = "protoc-generated wire bindings"
        )]
        pub mod v1 {
            include!("generated/coral.management.v1.rs");
        }
    }
}
