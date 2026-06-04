//! Registration-time HTTP source checks.
//!
//! This module verifies that source-scoped template inputs resolve against the
//! supplied inputs map and that referenced auth schemes are wired in the
//! engine's authenticator registry. Pure manifest validation lives in
//! `coral-spec`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

use crate::RequestAuthenticator;
use crate::backends::http::auth::validate_auth_inputs;
use crate::backends::shared::template::{
    validate_input_dependencies, validate_value_source_inputs,
};
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::{BodySpec, HeaderSpec, RequestRouteSpec, RequestSpec as ManifestRequestSpec};

struct HttpRequestSite<'a> {
    label: String,
    request: &'a ManifestRequestSpec,
}

pub(super) fn validate_source_scoped_http_config(
    manifest: &HttpSourceManifest,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    check_base_url_inputs(manifest, resolved_inputs)?;
    check_request_header_inputs(manifest, resolved_inputs)?;
    check_request_site_inputs(manifest, resolved_inputs)?;
    check_auth_inputs(manifest, request_authenticators, resolved_inputs)?;
    Ok(())
}

/// `base_url` may reference `{{filter.*}}` / `{{state.*}}` that only resolve
/// per-request. Check input-token deps only; runtime renders the rest.
fn check_base_url_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_input_dependencies(&manifest.base_url, resolved_inputs)
        .map_err(|error| registration_error(&manifest.common.name, "base_url", &error))
}

/// Same tolerance for filter/state tokens as `base_url`.
fn check_request_header_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_header_inputs(
        &manifest.common.name,
        "request_headers",
        &manifest.request_headers,
        resolved_inputs,
    )?;
    Ok(())
}

fn check_request_site_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    for site in http_request_sites(manifest) {
        validate_request_template_inputs(
            &manifest.common.name,
            &site.label,
            site.request,
            resolved_inputs,
        )?;
    }
    Ok(())
}

fn http_request_sites(manifest: &HttpSourceManifest) -> Vec<HttpRequestSite<'_>> {
    let table_sites = manifest.tables.iter().flat_map(|table| {
        let default = std::iter::once(HttpRequestSite {
            label: format!("table '{}' request", table.name()),
            request: &table.request,
        });
        let routes = table.requests.iter().map(move |route| HttpRequestSite {
            label: table_request_route_label(table.name(), route),
            request: &route.request,
        });
        default.chain(routes)
    });

    let function_sites = manifest.functions.iter().map(|function| HttpRequestSite {
        label: format!("function '{}' request", function.name),
        request: &function.request,
    });

    table_sites.chain(function_sites).collect()
}

fn table_request_route_label(table_name: &str, route: &RequestRouteSpec) -> String {
    if route.when_filters.is_empty() {
        format!("table '{table_name}' request route")
    } else {
        format!(
            "table '{table_name}' request route for filters [{}]",
            route.when_filters.join(", ")
        )
    }
}

/// Auth is source-scoped: all value-source input dependencies must resolve
/// before any request is issued.
fn check_auth_inputs(
    manifest: &HttpSourceManifest,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_auth_inputs(&manifest.auth, request_authenticators, resolved_inputs)
        .map_err(|error| registration_error(&manifest.common.name, "auth", &error))
}

fn registration_error(source: &str, field: &str, error: &DataFusionError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "source '{source}' {field} could not be resolved: {error}"
    ))
}

fn validate_request_template_inputs(
    source_name: &str,
    request_label: &str,
    request: &ManifestRequestSpec,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_input_dependencies(&request.path, resolved_inputs).map_err(|error| {
        registration_error(source_name, &format!("{request_label} path"), &error)
    })?;
    validate_header_inputs(
        source_name,
        &format!("{request_label} header"),
        &request.headers,
        resolved_inputs,
    )?;
    for param in &request.query {
        validate_value_source_inputs(&param.value, resolved_inputs).map_err(|error| {
            registration_error(
                source_name,
                &format!("{request_label} query param '{}'", param.name),
                &error,
            )
        })?;
    }
    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                let field_path = if field.path.is_empty() {
                    "<root>".to_string()
                } else {
                    field.path.join(".")
                };
                validate_value_source_inputs(&field.value, resolved_inputs).map_err(|error| {
                    registration_error(
                        source_name,
                        &format!("{request_label} body field '{field_path}'"),
                        &error,
                    )
                })?;
            }
        }
        BodySpec::Text { content } => {
            validate_value_source_inputs(content, resolved_inputs).map_err(|error| {
                registration_error(source_name, &format!("{request_label} body text"), &error)
            })?;
        }
    }
    Ok(())
}

fn validate_header_inputs(
    source_name: &str,
    context: &str,
    headers: &[HeaderSpec],
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    for header in headers {
        validate_value_source_inputs(&header.value, resolved_inputs).map_err(|error| {
            registration_error(source_name, &format!("{context} '{}'", header.name), &error)
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::OnceLock;

    use serde_json::{Value, json};

    use crate::backends::http::client::HttpSourceClient;
    use crate::backends::http::test_support::parse_http_manifest;
    use coral_spec::backends::http::HttpSourceManifest;

    static TEST_HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    fn test_http_client() -> reqwest::Client {
        TEST_HTTP_CLIENT.get_or_init(reqwest::Client::new).clone()
    }

    fn alpha_manifest(body: Value) -> HttpSourceManifest {
        let Value::Object(mut manifest) = body else {
            panic!("test manifest body must be an object");
        };

        for (key, value) in [
            ("dsl_version", json!(3)),
            ("name", json!("alpha")),
            ("version", json!("0.1.0")),
            ("backend", json!("http")),
            ("base_url", json!("https://api.example.com")),
        ] {
            manifest.entry(key.to_string()).or_insert(value);
        }

        parse_http_manifest(Value::Object(manifest))
    }

    fn items_table(request: &Value) -> Value {
        json!({
            "name": "items",
            "description": "items",
            "request": request,
            "columns": id_columns(),
        })
    }

    fn search_items_function(request: &Value) -> Value {
        json!({
            "name": "search_items",
            "description": "Search items",
            "request": request,
            "columns": id_columns(),
        })
    }

    fn id_columns() -> Value {
        json!([{ "name": "id", "type": "Utf8" }])
    }

    fn client_error(manifest: Value, expectation: &str) -> String {
        let manifest = alpha_manifest(manifest);
        HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
            test_http_client(),
        )
        .expect_err(expectation)
        .to_string()
    }

    struct RequestInputCase {
        name: &'static str,
        request: Value,
        expected_suffix: &'static str,
    }

    fn unresolved_account_request_cases() -> [RequestInputCase; 4] {
        [
            RequestInputCase {
                name: "path",
                request: json!({ "path": "/{{input.ACCOUNT_ID}}/items" }),
                expected_suffix: "path could not be resolved",
            },
            RequestInputCase {
                name: "header",
                request: json!({
                    "path": "/items",
                    "headers": [{
                        "name": "X-Account",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                expected_suffix: "header 'X-Account' could not be resolved",
            },
            RequestInputCase {
                name: "query",
                request: json!({
                    "path": "/items",
                    "query": [{
                        "name": "account_id",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                expected_suffix: "query param 'account_id' could not be resolved",
            },
            RequestInputCase {
                name: "body",
                request: json!({
                    "method": "POST",
                    "path": "/items",
                    "body": [{
                        "path": ["account", "id"],
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                expected_suffix: "body field 'account.id' could not be resolved",
            },
        ]
    }

    fn account_input_manifest(fields: Value) -> Value {
        let Value::Object(mut manifest) = json!({
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            }
        }) else {
            unreachable!("account input manifest fixture is an object");
        };
        let Value::Object(fields) = fields else {
            unreachable!("manifest fixture overrides must be an object");
        };
        manifest.extend(fields);
        Value::Object(manifest)
    }

    fn assert_unresolved_account_request_inputs(
        request_label: &str,
        surfaces: impl Fn(&Value) -> Value,
    ) {
        for case in unresolved_account_request_cases() {
            let expected = format!("{request_label} request {}", case.expected_suffix);
            let error = client_error(
                account_input_manifest(surfaces(&case.request)),
                &format!(
                    "missing {request_label} request {} input should fail",
                    case.name
                ),
            );

            assert!(
                error.contains(&expected),
                "unexpected error for {}: {error}",
                case.name
            );
        }
    }

    #[test]
    fn backend_client_requires_source_scoped_credentials() {
        let error = client_error(
            json!({
                "auth": {
                    "type": "HeaderAuth",
                    "headers": [{
                        "name": "Authorization",
                        "from": "template",
                        "template": "Bearer {{input.API_KEY}}"
                    }]
                },
                "inputs": {
                    "API_KEY": { "kind": "secret" }
                },
                "tables": [items_table(&json!({ "path": "/items" }))]
            }),
            "missing source-scoped credentials must fail",
        );

        assert!(error.contains("missing source input 'API_KEY' for template token"));
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_inputs() {
        assert_unresolved_account_request_inputs(
            "table 'items'",
            |request| json!({ "tables": [items_table(request)] }),
        );

        let error = client_error(
            account_input_manifest(json!({
                "tables": [json!({
                    "name": "items",
                    "description": "items",
                    "request": { "path": "/items" },
                    "requests": [{
                        "when_filters": ["account_id"],
                        "method": "GET",
                        "path": "/{{input.ACCOUNT_ID}}/items"
                    }],
                    "filters": [{
                        "name": "account_id"
                    }],
                    "columns": id_columns()
                })]
            })),
            "missing table request route input should fail",
        );
        assert!(
            error.contains(
                "table 'items' request route for filters [account_id] path could not be resolved"
            ),
            "unexpected error for route: {error}"
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_function_request_inputs() {
        assert_unresolved_account_request_inputs("function 'search_items'", |request| {
            json!({
                "tables": [items_table(&json!({ "path": "/items" }))],
                "functions": [search_items_function(request)]
            })
        });
    }
}
