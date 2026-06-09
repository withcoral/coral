use std::collections::BTreeMap;

use coral_capabilities::ResponseTrust;
use serde_json::Value;

use crate::http::{
    apply_plan_headers, http_provider_error_detail, http_provider_error_detail_from_preview,
    limited_error_response_body, limited_response_bytes, response_headers, response_media_type,
    upstream_http_client,
};
use crate::model::bounded_provider_diagnostic_value;
use crate::{
    GraphqlRequestPlan, GraphqlUpstreamResponse, ProviderErrorKind, Result, UpstreamError,
};

impl GraphqlUpstreamResponse {
    /// Parses and classifies a GraphQL HTTP response body.
    ///
    /// # Errors
    ///
    /// Returns provider errors for invalid responses, HTTP errors, and GraphQL
    /// error arrays.
    pub fn from_http_json(
        http_status: u16,
        media_type: Option<&str>,
        headers: BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<Self> {
        let recognized_graphql = recognized_graphql_response(http_status, media_type);
        if !recognized_graphql && !(200..300).contains(&http_status) {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::HttpError,
                detail: http_provider_error_detail(
                    "GraphQL endpoint",
                    http_status,
                    media_type,
                    body,
                ),
            });
        }
        let value: Value = serde_json::from_slice(body)
            .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
        let Value::Object(mut object) = value else {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::InvalidResponse,
                detail: "GraphQL response body must be a JSON object".to_string(),
            });
        };
        let data = object.remove("data");
        let errors = object
            .remove("errors")
            .and_then(|value| match value {
                Value::Array(values) => Some(values),
                _ => None,
            })
            .unwrap_or_default();
        let extensions = object.remove("extensions");
        if !(errors.is_empty() || (200..300).contains(&http_status) && data.is_some()) {
            let errors = bounded_provider_diagnostic_value(Value::Array(errors));
            let partial_data = data.map_or(Value::Null, bounded_provider_diagnostic_value);
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::GraphqlError,
                detail: serde_json::json!({
                    "http_status": http_status,
                    "media_type": media_type,
                    "errors": errors,
                    "partial_data": partial_data,
                })
                .to_string(),
            });
        }
        let partial_data = (!errors.is_empty()).then(|| data.clone()).flatten();
        Ok(Self {
            http_status,
            headers,
            media_type: media_type.map(str::to_string),
            data,
            errors,
            extensions,
            partial_data,
            response_trust: ResponseTrust::UntrustedProviderData,
        })
    }
}

pub(crate) async fn execute_graphql_plan(
    plan: &GraphqlRequestPlan,
) -> Result<GraphqlUpstreamResponse> {
    let client = upstream_http_client()?;
    let mut request = client.post(plan.endpoint.clone());
    request = apply_plan_headers(request, &plan.headers);
    if let Some(timeout) = plan.timeout {
        request = request.timeout(timeout);
    }
    let response = request
        .json(&graphql_request_body(plan))
        .send()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?;
    let status = response.status().as_u16();
    let headers = response_headers(response.headers());
    let media_type = response_media_type(&headers);
    if !(200..300).contains(&status) {
        let body = limited_error_response_body(response).await?;
        let recognized_graphql = recognized_graphql_response(status, media_type.as_deref());
        if recognized_graphql
            && !body.body_truncated
            && let Err(UpstreamError::Provider { kind, detail }) =
                GraphqlUpstreamResponse::from_http_json(
                    status,
                    media_type.as_deref(),
                    headers,
                    &body.bytes,
                )
            && kind == ProviderErrorKind::GraphqlError
        {
            return Err(UpstreamError::Provider { kind, detail });
        }
        return Err(graphql_http_error_from_preview(
            status,
            media_type.as_deref(),
            &body,
            recognized_graphql,
        ));
    }
    let bytes = limited_response_bytes(response, "GraphQL provider").await?;
    GraphqlUpstreamResponse::from_http_json(status, media_type.as_deref(), headers, &bytes)
}

fn graphql_http_error_from_preview(
    status: u16,
    media_type: Option<&str>,
    body: &crate::http::ProviderErrorBodyPreview,
    recognized_graphql: bool,
) -> UpstreamError {
    UpstreamError::Provider {
        kind: if recognized_graphql {
            ProviderErrorKind::GraphqlError
        } else {
            ProviderErrorKind::HttpError
        },
        detail: http_provider_error_detail_from_preview(
            "GraphQL endpoint",
            status,
            media_type,
            &body.bytes,
            body.body_truncated,
            body.body_bytes,
            body.body_bytes_exact,
        ),
    }
}

fn recognized_graphql_response(http_status: u16, media_type: Option<&str>) -> bool {
    media_type.is_some_and(|value| value.starts_with("application/graphql-response+json"))
        || (http_status == 200
            && media_type.is_some_and(|value| value.starts_with("application/json")))
}

/// Renders a GraphQL request plan body.
fn graphql_request_body(plan: &GraphqlRequestPlan) -> Value {
    serde_json::json!({
        "query": plan.document,
        "variables": plan.variables,
        "operationName": plan.operation_name,
    })
}
