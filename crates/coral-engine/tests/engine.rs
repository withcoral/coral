//! Integration coverage for the transport-neutral `coral-engine` query API.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

#[path = "engine/catalog_tests.rs"]
mod catalog_tests;
#[path = "engine/harness.rs"]
mod harness;
#[path = "engine/http_tests.rs"]
mod http_tests;
#[path = "engine/json_tests.rs"]
mod json_tests;
#[path = "engine/jsonl_tests.rs"]
mod jsonl_tests;
#[path = "engine/parquet_tests.rs"]
mod parquet_tests;
#[path = "engine/test_source_tests.rs"]
mod test_source_tests;
