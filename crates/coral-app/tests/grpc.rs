//! Integration tests for the app-level gRPC surface.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

#[path = "grpc/catalog_discovery_tests.rs"]
mod catalog_discovery_tests;
#[path = "grpc/feature_service_tests.rs"]
mod feature_service_tests;
#[path = "grpc/function_lifecycle_tests.rs"]
mod function_lifecycle_tests;
#[path = "grpc/gui_onboarding_tests.rs"]
mod gui_onboarding_tests;
#[path = "grpc/harness.rs"]
#[expect(
    dead_code,
    reason = "The shared harness serves several integration binaries; this one does not dial the services that need a raw credential."
)]
mod harness;
#[path = "grpc/health_service_tests.rs"]
mod health_service_tests;
#[path = "grpc/oauth_refresh_tests.rs"]
mod oauth_refresh_tests;
#[path = "grpc/ownership_migration_tests.rs"]
mod ownership_migration_tests;
#[path = "grpc/resilience_tests.rs"]
mod resilience_tests;
#[path = "grpc/search_tests.rs"]
mod search_tests;
#[path = "grpc/server_lifecycle_tests.rs"]
mod server_lifecycle_tests;
#[path = "grpc/session_auth.rs"]
mod session_auth;
#[path = "grpc/source_lifecycle_tests.rs"]
mod source_lifecycle_tests;
#[path = "grpc/workspace_access_read_tests.rs"]
mod workspace_access_read_tests;
#[path = "grpc/workspace_lifecycle_tests.rs"]
mod workspace_lifecycle_tests;
