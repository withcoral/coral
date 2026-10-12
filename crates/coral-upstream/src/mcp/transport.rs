//! Bounded raw result capture before SDK decoding. SDK content models narrow
//! numbers (annotation priority is f32) and discard extension fields. They may
//! validate protocol envelopes, but must not become our result authority.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::{StreamExt as _, stream};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rmcp::model::{CallToolResult, ClientJsonRpcMessage, ReadResourceResult};
use rmcp::transport::common::client_side_sse::BoxedSseResponse;
use rmcp::transport::streamable_http_client::{
    StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
};
use serde_json::Value;

use crate::Error;

type TransportError = StreamableHttpError<Error>;

pub(super) enum Captured {
    Result(Value),
    InputTooLarge,
    DestinationRejected,
    ResultTooLarge,
    Unauthorized(Option<super::BearerChallenge>),
    AuthenticationUnsupported,
}

impl Captured {
    pub(super) fn into_result(self) -> Result<Value, Error> {
        match self {
            Self::Result(value) => Ok(value),
            Self::DestinationRejected => Err(Error::DestinationRejected),
            Self::InputTooLarge => Err(Error::InputTooLarge),
            Self::ResultTooLarge => Err(Error::ResultTooLarge),
            Self::Unauthorized(challenge) => Err(Error::McpUnauthorized(challenge)),
            Self::AuthenticationUnsupported => Err(Error::McpAuthenticationUnsupported),
        }
    }
}

#[derive(Clone)]
pub(super) struct McpHttpClient {
    pub(super) request_headers: std::collections::BTreeMap<String, String>,
    pub(super) client: coral_outbound::Client,
    pub(super) limit: usize,
    pub(super) result: Arc<Mutex<Option<Captured>>>,
}

impl StreamableHttpClient for McpHttpClient {
    type Error = Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, TransportError> {
        let request_json = serde_json::to_value(&message)?;
        if serde_json::to_vec(&request_json)?.len() > self.limit {
            *self
                .result
                .lock()
                .map_err(|_error| TransportError::Client(Error::McpResult))? =
                Some(Captured::InputTooLarge);
            return Err(TransportError::Client(Error::InputTooLarge));
        }
        let capture_request = result_request(&request_json);
        let mut request = self
            .client
            .post(uri.as_ref())
            .map_err(|error| self.capture_destination(error.into()))?
            .header("accept", "application/json, text/event-stream");
        for (name, value) in &self.request_headers {
            request = request.header(name, value);
        }
        for (name, value) in custom_headers {
            request = request.header(name, value);
        }
        if let Some(session_id) = &session_id {
            request = request.header("mcp-session-id", session_id.as_ref());
        }
        if let Some(token) = auth_header {
            request = request.bearer_auth(token);
        }
        let mut response = request
            .json(&message)
            .send()
            .await
            .map_err(|error| self.capture_destination(Error::send(error)))?;
        if response.status() == reqwest::StatusCode::ACCEPTED
            || response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND && session_id.is_some() {
            return Err(TransportError::SessionExpired);
        }
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            return Err(self.capture_authentication(response.headers()));
        }
        if !response.status().is_success() {
            return Err(TransportError::Client(Error::McpRequest));
        }
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let media_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(crate::media_type_essence);
        match media_type.as_deref() {
            Some("application/json") => {
                let body = crate::read_bounded_body(&mut response, self.limit, true)
                    .await
                    .map_err(|error| {
                        if matches!(error, Error::ResultTooLarge)
                            && let Ok(mut slot) = self.result.lock()
                        {
                            *slot = Some(Captured::ResultTooLarge);
                        }
                        TransportError::Client(error)
                    })?;
                let mut value = serde_json::from_slice(&body)?;
                capture(&self.result, capture_request.as_ref(), &mut value)
                    .map_err(TransportError::Client)?;
                Ok(StreamableHttpPostResponse::Json(
                    serde_json::from_value(value)?,
                    session_id,
                ))
            }
            Some("text/event-stream") => Ok(StreamableHttpPostResponse::Sse(
                self.bounded_sse(response, capture_request),
                session_id,
            )),
            _ => Err(TransportError::Client(Error::McpResult)),
        }
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), TransportError> {
        let mut request = self
            .client
            .delete(uri.as_ref())
            .map_err(|error| self.capture_destination(error.into()))?
            .header("mcp-session-id", session_id.as_ref());
        for (name, value) in &self.request_headers {
            request = request.header(name, value);
        }
        for (name, value) in custom_headers {
            request = request.header(name, value);
        }
        if let Some(token) = auth_header {
            request = request.bearer_auth(token);
        }
        request
            .send()
            .await
            .map_err(|error| self.capture_destination(Error::send(error)))?;
        Ok(())
    }

    fn get_stream(
        &self,
        _uri: Arc<str>,
        _session_id: Option<Arc<str>>,
        _last_event_id: Option<String>,
        _auth_header: Option<String>,
        _custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<Output = Result<BoxedSseResponse, TransportError>> + Send {
        // Only request-associated SSE is needed. No unsolicited stream or replay.
        std::future::ready(Err(TransportError::ServerDoesNotSupportSse))
    }
}

impl McpHttpClient {
    fn capture_authentication(&self, headers: &HeaderMap) -> TransportError {
        // The challenge is typed setup evidence; the body and realm are dropped.
        let challenges: Vec<String> = headers
            .get_all("www-authenticate")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .map(str::to_owned)
            .collect();
        if let Ok(mut slot) = self.result.lock() {
            let challenge = super::BearerChallenge::parse(&challenges);
            *slot = Some(
                if challenge.is_none() && headers.contains_key("www-authenticate") {
                    Captured::AuthenticationUnsupported
                } else {
                    Captured::Unauthorized(challenge)
                },
            );
        }
        TransportError::Client(Error::McpRequest)
    }

    fn capture_destination(&self, error: Error) -> TransportError {
        if matches!(error, Error::DestinationRejected)
            && let Ok(mut slot) = self.result.lock()
        {
            *slot = Some(Captured::DestinationRejected);
        }
        TransportError::Client(error)
    }

    fn bounded_sse(
        &self,
        response: reqwest::Response,
        capture_request: Option<(String, Value)>,
    ) -> BoxedSseResponse {
        // Bound bytes before parsing, including peers that never finish an event.
        let bytes = stream::try_unfold(
            (response, self.limit, self.result.clone()),
            |(mut response, remaining, result)| async move {
                let Some(chunk) = response.chunk().await.map_err(Error::Body)? else {
                    return Ok::<_, Error>(None);
                };
                let remaining = remaining.checked_sub(chunk.len()).ok_or_else(|| {
                    if let Ok(mut slot) = result.lock() {
                        *slot = Some(Captured::ResultTooLarge);
                    }
                    Error::ResultTooLarge
                })?;
                Ok(Some((chunk, (response, remaining, result))))
            },
        );
        let result = self.result.clone();
        sse_stream::SseStream::from_bytes_stream(bytes)
            .map(move |event| {
                let mut event = event?;
                if let Some(data) = &event.data
                    && !data.trim().is_empty()
                {
                    let mut value = serde_json::from_str(data)
                        .map_err(|error| sse_stream::Error::Body(Box::new(error)))?;
                    capture(&result, capture_request.as_ref(), &mut value)
                        .map_err(|error| sse_stream::Error::Body(Box::new(error)))?;
                    event.data = Some(
                        serde_json::to_string(&value)
                            .map_err(|error| sse_stream::Error::Body(Box::new(error)))?,
                    );
                }
                Ok(event)
            })
            .boxed()
    }
}

fn result_request(request: &Value) -> Option<(String, Value)> {
    let method = request.get("method")?.as_str()?;
    if !matches!(
        method,
        "tools/call" | "resources/read" | "initialize" | "tools/list"
    ) {
        return None;
    }
    Some((method.to_string(), request.get("id")?.clone()))
}

fn capture(
    slot: &Mutex<Option<Captured>>,
    request: Option<&(String, Value)>,
    message: &mut Value,
) -> Result<(), Error> {
    if let Some((method, id)) = request
        && message.get("id") == Some(id)
        && message.get("result").is_some()
    {
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(Error::McpResult);
        }
        let result = message.get_mut("result").ok_or(Error::McpResult)?;
        *slot.lock().map_err(|_poisoned| Error::McpResult)? =
            Some(Captured::Result(result.clone()));
        // Decode by the outstanding method. Otherwise the SDK's untagged
        // ServerResult can mistake a tool extension named `tools` for discovery.
        // Only the internal SDK envelope is narrowed; Programs get the raw copy.
        // Publication supports the 2025 protocols. Newer SDK fields such as
        // resultType and ttlMs must remain uninterpreted provider extensions.
        *result = match method.as_str() {
            "tools/call" => serde_json::from_value::<CallToolResult>(protocol_fields(
                result,
                &["content", "structuredContent", "isError", "_meta"],
            ))
            .and_then(serde_json::to_value),
            "resources/read" => serde_json::from_value::<ReadResourceResult>(protocol_fields(
                result,
                &["contents", "_meta"],
            ))
            .and_then(serde_json::to_value),
            // Discovery keeps the raw result; the SDK decodes its own copy.
            "initialize" | "tools/list" => Ok(result.clone()),
            _ => return Err(Error::McpResult),
        }
        .map_err(|_invalid_result| Error::McpResult)?;
    }
    Ok(())
}

fn protocol_fields(result: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|name| {
                result
                    .get(*name)
                    .map(|value| ((*name).to_string(), value.clone()))
            })
            .collect(),
    )
}
