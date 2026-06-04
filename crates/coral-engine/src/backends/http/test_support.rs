//! Test helpers shared by HTTP backend unit tests.
//!
//! Actual `#[test]` cases live beside the module they exercise.

use serde_json::json;

use crate::backends::http::target::HttpFetchTarget;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::parse_source_manifest_value;

pub(super) fn parse_http_manifest(value: serde_json::Value) -> HttpSourceManifest {
    parse_source_manifest_value(value)
        .expect("manifest should deserialize")
        .as_http()
        .expect("http manifest")
        .clone()
}

pub(super) fn test_get_items_table_spec() -> HttpTableSpec {
    parse_http_manifest(json!({
        "dsl_version": 3,
        "name": "demo",
        "version": "0.1.0",
        "backend": "http",
        "base_url": "https://api.example.com",
        "tables": [{
            "name": "items",
            "description": "items",
            "request": {
                "method": "GET",
                "path": "/items"
            },
            "columns": []
        }]
    }))
    .tables
    .into_iter()
    .next()
    .expect("table should exist")
}

pub(super) fn test_http_request_target(table: &HttpTableSpec) -> HttpFetchTarget {
    HttpFetchTarget::from_resolved_table_request(table, table.request.clone())
}
