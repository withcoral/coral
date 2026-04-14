//! Integration tests for the app-level gRPC surface.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

#[path = "grpc/harness.rs"]
mod harness;
#[path = "grpc/resilience_tests.rs"]
mod resilience_tests;
#[path = "grpc/source_lifecycle_tests.rs"]
mod source_lifecycle_tests;
