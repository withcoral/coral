//! End-to-end authored Universal Search contract tests for DSL v3.

#![allow(
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    unused_crate_dependencies,
    reason = "These compact test builders intentionally mutate JSON fixtures and consume owned values."
)]

use coral_spec::{
    ManifestDataType, SourceTableFunctionKind, TableFunctionArgSpec, parse_source_manifest_value,
};
use serde_json::{Value, json};

fn search_limits() -> Value {
    json!({
        "default_top_k": 10,
        "max_top_k": 50,
        "max_calls_per_query": 1
    })
}

fn query_arg(data_type: ManifestDataType, default: Option<Value>) -> Value {
    let mut arg = json!({
        "name": "query",
        "type": data_type.as_manifest_str(),
        "required": true,
        "bind": { "arg": "q" }
    });
    if let Some(default) = default {
        arg["default"] = default;
    }
    arg
}

fn secondary_arg(default: Option<Value>, values: &[&str]) -> Value {
    let mut arg = json!({
        "name": "scope",
        "type": "Utf8",
        "bind": { "arg": "scope" }
    });
    if let Some(default) = default {
        arg["default"] = default;
    }
    if !values.is_empty() {
        arg["values"] = json!(values);
    }
    arg
}

fn allow_policy(query_arg: &str) -> Value {
    json!({
        "id": "primary_search",
        "execute": true,
        "query_arg": query_arg,
        "result": {
            "entity_type": "issue",
            "identity_fields": ["id"],
            "provider_id": "id",
            "title": "title",
            "url": "url",
            "snippet": "body",
            "attributes": ["metadata"]
        }
    })
}

fn columns() -> Value {
    json!([
        { "name": "id", "type": "Utf8" },
        { "name": "title", "type": "Utf8" },
        { "name": "url", "type": "Utf8" },
        { "name": "body", "type": "Utf8" },
        { "name": "metadata", "type": "Json" }
    ])
}

fn http_function(name: &str, args: Value, policy: Value, columns: Value) -> Value {
    json!({
        "name": name,
        "kind": "search",
        "search_limits": search_limits(),
        "universal_search": policy,
        "args": args,
        "request": {
            "method": "GET",
            "path": "/search",
            "query": [
                { "name": "q", "from": "arg", "key": "q" },
                { "name": "scope", "from": "arg", "key": "scope" }
            ]
        },
        "columns": columns
    })
}

fn http_manifest(functions: Value) -> Value {
    json!({
        "name": "demo",
        "version": "1.0.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "functions": functions
    })
}

fn mcp_function(name: &str, args: Value, policy: Value) -> Value {
    json!({
        "name": name,
        "tool": "search",
        "kind": "search",
        "search_limits": search_limits(),
        "universal_search": policy,
        "args": args,
        "columns": columns()
    })
}

fn mcp_manifest(functions: Value) -> Value {
    json!({
        "name": "demo_mcp",
        "version": "1.0.0",
        "dsl_version": 3,
        "backend": "mcp",
        "server": {
            "transport": "stdio",
            "command": "demo-mcp"
        },
        "functions": functions
    })
}

fn valid_args() -> Value {
    json!([
        query_arg(ManifestDataType::Utf8, None),
        secondary_arg(Some(json!("all")), &["all", "open"])
    ])
}

#[test]
fn table_function_default_round_trip_distinguishes_missing_from_null() {
    let missing: TableFunctionArgSpec = serde_json::from_value(json!({
        "name": "scope",
        "type": "Json",
        "bind": { "arg": "scope" }
    }))
    .expect("missing default should deserialize");
    let explicit_null: TableFunctionArgSpec = serde_json::from_value(json!({
        "name": "scope",
        "type": "Json",
        "default": null,
        "bind": { "arg": "scope" }
    }))
    .expect("explicit null default should deserialize");

    let missing_json = serde_json::to_value(&missing).expect("missing default should serialize");
    let null_json =
        serde_json::to_value(&explicit_null).expect("explicit null default should serialize");
    assert!(missing_json.get("default").is_none());
    assert_eq!(null_json.get("default"), Some(&Value::Null));

    let missing_round_trip: TableFunctionArgSpec =
        serde_json::from_value(missing_json).expect("missing default should round trip");
    let null_round_trip: TableFunctionArgSpec =
        serde_json::from_value(null_json).expect("null default should round trip");
    assert!(missing_round_trip.default.is_none());
    assert!(
        null_round_trip
            .default
            .as_ref()
            .is_some_and(|default| default.value().is_null())
    );
}

#[test]
fn http_and_mcp_preserve_equivalent_authorisation_contracts() {
    let http = parse_source_manifest_value(http_manifest(json!([http_function(
        "search",
        valid_args(),
        allow_policy("query"),
        columns(),
    )])))
    .expect("HTTP Universal Search contract should parse");
    let http_function = &http.as_http().expect("HTTP manifest").functions[0];

    let mcp = parse_source_manifest_value(mcp_manifest(json!([mcp_function(
        "search",
        valid_args(),
        allow_policy("query"),
    )])))
    .expect("MCP Universal Search contract should parse");
    let mcp_function = &mcp.as_mcp().expect("MCP manifest").functions[0].common;

    for function in [http_function, mcp_function] {
        assert_eq!(function.kind, SourceTableFunctionKind::Search);
        assert_eq!(function.search_limits.as_ref().unwrap().max_top_k, 50);
        assert_eq!(
            function.universal_search.as_ref().unwrap().id,
            "primary_search"
        );
        assert_eq!(
            function.args[1]
                .default
                .as_ref()
                .expect("secondary default")
                .value(),
            &json!("all")
        );
    }
}

#[test]
fn explicit_json_null_default_remains_distinct_from_no_default() {
    let mut args = valid_args().as_array().unwrap().clone();
    args[1]["type"] = json!("Json");
    args[1]["default"] = Value::Null;
    args[1].as_object_mut().unwrap().remove("values");
    let parsed = parse_source_manifest_value(http_manifest(json!([http_function(
        "search",
        Value::Array(args),
        allow_policy("query"),
        columns(),
    )])))
    .expect("explicit JSON null is a valid typed default");
    let function = &parsed.as_http().unwrap().functions[0];

    assert!(function.args[0].default.is_none());
    assert!(function.args[1].default.as_ref().unwrap().value().is_null());
}

#[test]
fn explicit_denial_is_inert_and_does_not_require_search_metadata() {
    let denied = json!({
        "name": "lookup",
        "universal_search": { "id": "lookup", "execute": false },
        "request": { "method": "POST", "path": "/lookup" },
        "columns": [{ "name": "id", "type": "Utf8" }]
    });
    let parsed = parse_source_manifest_value(http_manifest(json!([denied])))
        .expect("explicit denial should parse without executable-route metadata");
    let function = &parsed.as_http().unwrap().functions[0];

    assert_eq!(function.kind, SourceTableFunctionKind::Table);
    assert!(!function.universal_search.as_ref().unwrap().execute);
}

#[test]
fn duplicate_route_ids_are_rejected_across_functions() {
    let denied = |name: &str| {
        json!({
            "name": name,
            "universal_search": { "id": "duplicate", "execute": false },
            "request": { "method": "GET", "path": "/lookup" },
            "columns": [{ "name": "id", "type": "Utf8" }]
        })
    };
    let error =
        parse_source_manifest_value(http_manifest(json!([denied("first"), denied("second")])))
            .expect_err("duplicate route ids must fail closed");

    assert!(error.to_string().contains("declared more than once"));
}

#[test]
fn executable_route_argument_contracts_fail_closed() {
    let cases = [
        (
            "missing query arg",
            valid_args(),
            allow_policy("missing"),
            "unknown argument 'missing'",
        ),
        (
            "wrong query type",
            json!([
                query_arg(ManifestDataType::Int64, None),
                secondary_arg(Some(json!("all")), &[])
            ]),
            allow_policy("query"),
            "must have type Utf8",
        ),
        (
            "query default",
            json!([
                query_arg(ManifestDataType::Utf8, Some(json!("anything"))),
                secondary_arg(Some(json!("all")), &[])
            ]),
            allow_policy("query"),
            "must not declare a default",
        ),
        (
            "missing secondary default",
            json!([
                query_arg(ManifestDataType::Utf8, None),
                secondary_arg(None, &[])
            ]),
            allow_policy("query"),
            "must declare a typed default",
        ),
        (
            "invalid typed default",
            json!([
                query_arg(ManifestDataType::Utf8, None),
                secondary_arg(Some(json!(42)), &[])
            ]),
            allow_policy("query"),
            "default must match type Utf8",
        ),
        (
            "enum default mismatch",
            json!([
                query_arg(ManifestDataType::Utf8, None),
                secondary_arg(Some(json!("closed")), &["all", "open"])
            ]),
            allow_policy("query"),
            "not one of its declared values",
        ),
        (
            "numeric enum default mismatch",
            json!([
                query_arg(ManifestDataType::Utf8, None),
                {
                    "name": "scope",
                    "type": "Int64",
                    "values": ["1", "2"],
                    "default": 3,
                    "bind": { "arg": "scope" }
                }
            ]),
            allow_policy("query"),
            "not one of its declared values",
        ),
    ];

    for (case, args, policy, expected) in cases {
        let error = parse_source_manifest_value(http_manifest(json!([http_function(
            "search",
            args,
            policy,
            columns(),
        )])))
        .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{case}: expected error containing {expected:?}, got {error}"
        );
    }
}

#[test]
fn float_enum_defaults_use_the_runtime_string_representation() {
    let args = json!([
        query_arg(ManifestDataType::Utf8, None),
        {
            "name": "scope",
            "type": "Float64",
            "values": ["1"],
            "default": 1.0,
            "bind": { "arg": "scope" }
        }
    ]);

    parse_source_manifest_value(http_manifest(json!([http_function(
        "search",
        args,
        allow_policy("query"),
        columns(),
    )])))
    .expect("1.0 and the runtime Float64 rendering '1' must be treated as equal");
}

#[test]
fn result_mapping_requires_known_scalar_identity_and_display_fields() {
    let mut unknown = allow_policy("query");
    unknown["result"]["title"] = json!("missing");
    let error = parse_source_manifest_value(http_manifest(json!([http_function(
        "search",
        valid_args(),
        unknown,
        columns(),
    )])))
    .expect_err("unknown result fields must fail closed");
    assert!(
        error
            .to_string()
            .contains("unknown result column 'missing'")
    );

    let mut structured = allow_policy("query");
    structured["result"]["title"] = json!("metadata");
    let error = parse_source_manifest_value(http_manifest(json!([http_function(
        "search",
        valid_args(),
        structured,
        columns(),
    )])))
    .expect_err("structured display fields must fail closed");
    assert!(error.to_string().contains("must be scalar"));
}

#[test]
fn denied_route_rejects_query_arg() {
    let denied = json!({
        "name": "lookup",
        "universal_search": {
            "id": "lookup",
            "execute": false,
            "query_arg": "query"
        },
        "request": { "method": "GET", "path": "/lookup" },
        "columns": [{ "name": "id", "type": "Utf8" }]
    });
    let error = parse_source_manifest_value(http_manifest(json!([denied])))
        .expect_err("denial query_arg must be rejected");

    let message = error.to_string();
    assert!(
        message.contains("/functions/0/universal_search") && message.contains("query_arg"),
        "unexpected denied-route schema error: {message}"
    );
}
