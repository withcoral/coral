//! Test helpers shared by HTTP backend unit tests.
//!
//! Actual `#[test]` cases live beside the module they exercise.

use serde_json::json;

use crate::backends::http::target::HttpFetchTarget;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::{BodySpec, RequestSpec, ValueSourceSpec, parse_source_manifest_value};

pub(super) fn parse_http_manifest(value: serde_json::Value) -> HttpSourceManifest {
    parse_source_manifest_value(value)
        .expect("manifest should deserialize")
        .as_http()
        .expect("http manifest")
        .clone()
}

pub(super) fn test_http_table_spec(
    columns: &serde_json::Value,
    request: &RequestSpec,
) -> HttpTableSpec {
    parse_http_manifest(json!({
        "dsl_version": 3,
        "name": "demo",
        "version": "0.1.0",
        "backend": "http",
        "base_url": "https://api.example.com",
        "tables": [{
            "name": "items",
            "description": "items",
            "request": request_json(request),
            "columns": columns
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

fn request_json(request: &RequestSpec) -> serde_json::Value {
    let body = match &request.body {
        BodySpec::Json { fields } => fields
            .iter()
            .map(|field| {
                json!({
                    "path": field.path,
                    "value": value_source_json(&field.value),
                })
            })
            .collect::<Vec<_>>(),
        BodySpec::Text { .. } => Vec::new(),
    };
    json!({
        "method": format!("{:?}", request.method),
        "path": request.path,
        "query": request.query.iter().map(|query| json!({
            "name": query.name,
            "value": value_source_json(&query.value),
        })).collect::<Vec<_>>(),
        "body": body,
        "headers": request.headers.iter().map(|header| json!({
            "name": header.name,
            "value": value_source_json(&header.value),
        })).collect::<Vec<_>>(),
    })
}

fn value_source_json(value: &ValueSourceSpec) -> serde_json::Value {
    match value {
        ValueSourceSpec::Literal { value } => json!({
            "from": "literal",
            "value": value,
        }),
        ValueSourceSpec::OneOf { values } => json!({
            "from": "one_of",
            "values": values.iter().map(value_source_json).collect::<Vec<_>>(),
        }),
        ValueSourceSpec::Filter { key, default } => json!({
            "from": "filter",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::FilterInt { key, default } => json!({
            "from": "filter_int",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::FilterBool { key, default } => json!({
            "from": "filter_bool",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::FilterSplit {
            key,
            separator,
            part,
        } => json!({
            "from": "filter_split",
            "key": key,
            "separator": separator,
            "part": part,
        }),
        ValueSourceSpec::FilterSplitInt {
            key,
            separator,
            part,
        } => json!({
            "from": "filter_split_int",
            "key": key,
            "separator": separator,
            "part": part,
        }),
        ValueSourceSpec::Arg { key, default } => json!({
            "from": "arg",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::ArgInt { key, default } => json!({
            "from": "arg_int",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::ArgBool { key, default } => json!({
            "from": "arg_bool",
            "key": key,
            "default": default,
        }),
        ValueSourceSpec::ArgSplit {
            key,
            separator,
            part,
        } => json!({
            "from": "arg_split",
            "key": key,
            "separator": separator,
            "part": part,
        }),
        ValueSourceSpec::ArgSplitInt {
            key,
            separator,
            part,
        } => json!({
            "from": "arg_split_int",
            "key": key,
            "separator": separator,
            "part": part,
        }),
        ValueSourceSpec::Input { key } => json!({
            "from": "input",
            "key": key,
        }),
        ValueSourceSpec::Bearer { key } => json!({
            "from": "bearer",
            "key": key,
        }),
        ValueSourceSpec::Template { template } => json!({
            "from": "template",
            "template": template,
        }),
        ValueSourceSpec::State { key } => json!({
            "from": "state",
            "key": key,
        }),
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => json!({
            "from": "now_epoch_minus_seconds",
            "seconds": seconds,
        }),
    }
}
