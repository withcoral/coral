//! Integration tests for the app-level gRPC surface.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

#[path = "grpc/health_service_tests.rs"]
mod health_service_tests;
#[path = "grpc/oidc_login_tests.rs"]
mod oidc_login_tests;
#[path = "grpc/private_access_tests.rs"]
mod private_access_tests;
#[path = "grpc/record_locations_tests.rs"]
mod record_locations_tests;
#[path = "grpc/server_lifecycle_tests.rs"]
mod server_lifecycle_tests;
#[path = "grpc/session_auth.rs"]
mod session_auth;
