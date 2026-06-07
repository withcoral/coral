use std::collections::BTreeMap;

use coral_capabilities::ResponseTrust;
use serde_json::Value;

use crate::http::{
    apply_plan_headers, limited_response_bytes, response_headers, response_media_type,
    upstream_http_client,
};
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
        let recognized_graphql = media_type
            .is_some_and(|value| value.starts_with("application/graphql-response+json"))
            || (http_status == 200
                && media_type.is_some_and(|value| value.starts_with("application/json")));
        if !recognized_graphql && !(200..300).contains(&http_status) {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::HttpError,
                detail: format!("GraphQL endpoint returned HTTP {http_status}"),
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
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::GraphqlError,
                detail: serde_json::json!({
                    "errors": errors,
                    "partial_data": data,
                })
                .to_string(),
            });
        }
        let partial_data = (!errors.is_empty()).then(|| data.clone()).flatten();
        Ok(Self {
            http_status,
            headers,
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
    let bytes = limited_response_bytes(response, "GraphQL provider").await?;
    GraphqlUpstreamResponse::from_http_json(status, media_type.as_deref(), headers, &bytes)
}

/// Renders a GraphQL request plan body.
#[must_use]
pub fn graphql_request_body(plan: &GraphqlRequestPlan) -> Value {
    serde_json::json!({
        "query": plan.document,
        "variables": plan.variables,
        "operationName": plan.operation_name,
    })
}
