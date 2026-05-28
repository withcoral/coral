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

    use serde_json::json;

    use crate::backends::http::client::HttpSourceClient;
    use crate::backends::http::test_support::parse_http_manifest;

    #[test]
    fn backend_client_requires_source_scoped_credentials() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
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
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));
        let source_secrets = BTreeMap::new();

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &source_secrets,
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing source-scoped credentials must fail");

        assert!(
            error
                .to_string()
                .contains("missing source input 'API_KEY' for template token")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_path_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "API_KEY": { "kind": "secret" },
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/{{input.ACCOUNT_ID}}/items"
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing table request path inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request path could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_header_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/items",
                    "headers": [{
                        "name": "X-Account",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing table request header inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request header 'X-Account' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_query_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/items",
                    "query": [{
                        "name": "account_id",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing table request query inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request query param 'account_id' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_body_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "method": "POST",
                    "path": "/items",
                    "body": [{
                        "path": ["account", "id"],
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing table request body inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request body field 'account.id' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_request_route_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
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
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
        )
        .expect_err("missing request route inputs must fail");

        assert!(error.to_string().contains(
            "table 'items' request route for filters [account_id] path could not be resolved"
        ));
    }

    #[test]
    fn backend_client_rejects_unresolved_function_request_inputs() {
        let cases = [
            (
                "path",
                json!({
                    "path": "/{{input.ACCOUNT_ID}}/items"
                }),
                "function 'search_items' request path could not be resolved",
            ),
            (
                "header",
                json!({
                    "path": "/items",
                    "headers": [{
                        "name": "X-Account",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                "function 'search_items' request header 'X-Account' could not be resolved",
            ),
            (
                "query",
                json!({
                    "path": "/items",
                    "query": [{
                        "name": "account_id",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                "function 'search_items' request query param 'account_id' could not be resolved",
            ),
            (
                "body",
                json!({
                    "method": "POST",
                    "path": "/items",
                    "body": [{
                        "path": ["account", "id"],
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                }),
                "function 'search_items' request body field 'account.id' could not be resolved",
            ),
        ];

        for (name, request, expected) in cases {
            let manifest = parse_http_manifest(json!({
                "dsl_version": 3,
                "name": "alpha",
                "version": "0.1.0",
                "backend": "http",
                "base_url": "https://api.example.com",
                "inputs": {
                    "ACCOUNT_ID": { "kind": "variable" }
                },
                "tables": [{
                    "name": "items",
                    "description": "items",
                    "request": { "path": "/items" },
                    "columns": [{
                        "name": "id",
                        "type": "Utf8"
                    }]
                }],
                "functions": [{
                    "name": "search_items",
                    "description": "Search items",
                    "request": request,
                    "columns": [{
                        "name": "id",
                        "type": "Utf8"
                    }]
                }]
            }));

            let error = HttpSourceClient::from_manifest(
                &manifest,
                &BTreeMap::new(),
                &BTreeMap::new(),
                &HashMap::new(),
                None,
            )
            .expect_err(&format!(
                "missing function request {name} input should fail"
            ));

            assert!(
                error.to_string().contains(expected),
                "unexpected error for {name}: {error}"
            );
        }
    }
}
