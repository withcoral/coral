//! Complete bounded HTTP response capture and decoding.

use coral_catalog::{HttpExecutionBinding, HttpRedirectPolicy, SchemaGraph, SuccessSelection};
use coral_telemetry::safe;
use serde_json::Value;
use tracing::Instrument as _;
use url::Url;

use super::response_headers::{CredentialNames, safe_response_headers};
use crate::span_field::{self, Declared};
use crate::{
    Error, OutcomeClass, ReadyHttpDispatch, is_json_media_type, media_type_essence,
    read_bounded_body,
};

const MAX_REDIRECTS: usize = 10;

/// One safe redirect fact retained in an HTTP Tool result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRedirect {
    status: u16,
    url: String,
    cross_origin: bool,
}

impl HttpRedirect {
    /// Returns the redirect response status.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the followed target without user information, query, or fragment.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns whether the continuation crossed an HTTP origin.
    #[must_use]
    pub const fn cross_origin(&self) -> bool {
        self.cross_origin
    }
}

/// Safe evidence that a complete HTTP body could not be decoded as declared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpDecodeDiagnostic {
    code: &'static str,
    message: &'static str,
}

impl HttpDecodeDiagnostic {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    /// Returns the stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns a bounded content-free explanation.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.message
    }
}

/// Complete bounded response captured by the HTTP Adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpOutcome {
    status: u16,
    data: Value,
    headers: Vec<(String, String)>,
    media_type: Option<String>,
    charset: Option<String>,
    final_url: String,
    redirects: Vec<HttpRedirect>,
    decode_diagnostics: Vec<HttpDecodeDiagnostic>,
    class: OutcomeClass,
}

impl HttpOutcome {
    /// Builds a complete scripted response for a deterministic Adapter. It has
    /// no binding to select from, so a 2xx status is a success and any other an
    /// error response.
    #[must_use]
    pub const fn new(status: u16, data: Value) -> Self {
        Self {
            status,
            data,
            headers: Vec::new(),
            media_type: None,
            charset: None,
            final_url: String::new(),
            redirects: Vec::new(),
            decode_diagnostics: Vec::new(),
            class: if is_success_code(status) {
                OutcomeClass::Success
            } else {
                OutcomeClass::ErrorResponse
            },
        }
    }

    /// Returns whether this response is a declared success, or why it is not.
    #[must_use]
    pub const fn class(&self) -> OutcomeClass {
        self.class
    }

    /// Returns the complete HTTP Tool Result: its status beside its data,
    /// with mandatory metadata.
    #[must_use]
    pub fn tool_result(&self) -> Value {
        let redirects = self.redirects.iter().map(|redirect| serde_json::json!({
            "status": redirect.status(), "url": redirect.url(), "crossOrigin": redirect.cross_origin(),
        })).collect::<Vec<_>>();
        let diagnostics = self
            .decode_diagnostics
            .iter()
            .map(|diagnostic| {
                serde_json::json!({
                    "code": diagnostic.code(), "message": diagnostic.message(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "status": self.status,
            "data": self.data,
            "metadata": {
                "kind": "http", "headers": self.headers,
                "mediaType": self.media_type, "charset": self.charset, "finalUrl": self.final_url,
                "redirects": redirects, "decodeDiagnostics": diagnostics,
            }
        })
    }

    /// Returns the actual upstream HTTP status.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the decoded provider-native response value.
    #[must_use]
    pub const fn data(&self) -> &Value {
        &self.data
    }

    /// Returns every actual response-header pair, including repeated names,
    /// with credential values redacted.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// Returns the lowercase media-type essence selected from `Content-Type`.
    #[must_use]
    pub fn media_type(&self) -> Option<&str> {
        self.media_type.as_deref()
    }

    /// Returns the selected lowercase response charset when one was declared.
    #[must_use]
    pub fn charset(&self) -> Option<&str> {
        self.charset.as_deref()
    }

    /// Returns the final URL without user information, query, or fragment.
    #[must_use]
    pub fn final_url(&self) -> &str {
        &self.final_url
    }

    /// Returns each protocol continuation that Coral followed.
    #[must_use]
    pub fn redirects(&self) -> &[HttpRedirect] {
        &self.redirects
    }

    /// Returns safe body-decode diagnostics without replacing the outcome.
    #[must_use]
    pub fn decode_diagnostics(&self) -> &[HttpDecodeDiagnostic] {
        &self.decode_diagnostics
    }
}

/// Stateless HTTP sender with origin-scoped host credential injection.
#[derive(Clone)]
pub struct HttpUpstream {
    client: coral_outbound::Client,
}

impl HttpUpstream {
    /// Builds a sender with the deployment-selected outbound policy.
    /// # Errors
    /// Returns an HTTP client initialization error.
    pub fn new(policy: coral_outbound::DestinationPolicy) -> Result<Self, Error> {
        Ok(Self {
            client: crate::http_client(policy)?,
        })
    }

    /// Consumes and sends one prepared request exactly once.
    ///
    /// Declared redirects are protocol continuation. Every final complete
    /// status response becomes an [`HttpOutcome`], including non-success,
    /// undeclared, malformed, and schema-incompatible responses.
    ///
    /// # Errors
    ///
    /// Returns only when no complete response was received or the complete
    /// response crossed its finite size bound.
    pub async fn send(&self, dispatch: ReadyHttpDispatch) -> Result<HttpOutcome, Error> {
        let started = std::time::Instant::now();
        let request_bytes = dispatch.body.as_ref().map(Vec::len);
        let mut response_bytes = None;
        let sent = self.exchange(dispatch, &mut response_bytes).await;
        crate::record_exchange(
            coral_telemetry::metric::ProviderProtocol::Http,
            sent.as_ref().map(|_outcome| ()),
            started,
            request_bytes,
            response_bytes,
        );
        sent
    }

    /// [`Self::send`], noting the body size of the response it reads in
    /// `response_bytes`.
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded protocol-continuation loop owns credential stripping and complete response capture"
    )]
    async fn exchange(
        &self,
        dispatch: ReadyHttpDispatch,
        response_bytes: &mut Option<usize>,
    ) -> Result<HttpOutcome, Error> {
        let ReadyHttpDispatch {
            mut credentials,
            mut query_credentials,
            credential_contract,
            mut method,
            mut url,
            mut headers,
            caller_header_names,
            mut body,
            request_media_type: _request_media_type,
            credential_slots: _credential_slots,
            binding,
            schema_graph,
            limits,
        } = dispatch;
        if credential_contract != coral_catalog::ProviderCredentialContract::NONE
            && credentials.is_empty()
            && query_credentials.is_empty()
        {
            return Err(Error::CredentialRequired);
        }
        // Kept past a cross-origin redirect: a later service may still echo them.
        let credential_names = CredentialNames::new(&credentials, &query_credentials);
        let mut redirects = Vec::new();
        loop {
            // Query credentials join only the URL that leaves; `url` stays secret-free
            // for redirects and evidence.
            let mut sent = url.clone();
            if !query_credentials.is_empty() {
                sent.query_pairs_mut().extend_pairs(
                    query_credentials
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.expose())),
                );
            }
            let sent = zeroize::Zeroizing::new(String::from(sent));
            let mut request = self.client.request(method.clone(), sent.as_str())?;
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            for (name, secret) in &credentials {
                let mut value = reqwest::header::HeaderValue::from_str(secret.expose())
                    .map_err(|_error| Error::CredentialRequired)?;
                value.set_sensitive(true);
                request = request.header(name, value);
            }
            if let Some(body) = &body {
                request = request.body(body.clone());
            }
            // Only the first request follows the route template; a redirect
            // target has none.
            let span = client_span(
                &method,
                &url,
                redirects
                    .is_empty()
                    .then(|| Declared::path_template(&binding)),
                body.as_ref().map(Vec::len),
            );
            let request = crate::traced(request, &span)?;
            let mut response = request
                .send()
                .instrument(span.clone())
                .await
                .map_err(Error::send)?;
            let status = response.status().as_u16();
            span.record("http.response.status_code", safe::number(status));
            if status >= 400 {
                coral_telemetry::record_failure(&span, safe::http_status_code(status));
            }
            if binding.redirect_policy() == HttpRedirectPolicy::Follow
                && redirects.len() < MAX_REDIRECTS
                && let Some(next_method) = redirect_method(status, &method)
                && let Some(location) = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                // Resolved from the planned URL, which never holds query credentials.
                && let Ok(target) = url.join(location)
                && matches!(target.scheme(), "http" | "https")
            {
                let cross_origin = !same_origin(&url, &target);
                redirects.push(HttpRedirect {
                    status,
                    url: safe_url(&target),
                    cross_origin,
                });
                if cross_origin {
                    credentials.clear();
                    query_credentials.clear();
                    headers.retain(|(name, _value)| {
                        !caller_header_names
                            .iter()
                            .any(|caller| caller.eq_ignore_ascii_case(name))
                            && !matches!(
                                name.as_str(),
                                "authorization" | "cookie" | "proxy-authorization"
                            )
                    });
                }
                if status == 303
                    || (next_method == reqwest::Method::GET && method != reqwest::Method::GET)
                {
                    body = None;
                    headers.retain(|(name, _value)| !name.starts_with("content-"));
                }
                // The target carries its own query. A parameter Coral is about to add
                // would sit beside one the service already put there, and the service
                // would choose between them, so the call stops here instead.
                if query_credentials.iter().any(|(parameter, _value)| {
                    target
                        .query_pairs()
                        .any(|(name, _value)| name == parameter.as_str())
                }) {
                    return Err(Error::CredentialUnusable);
                }
                method = next_method;
                url = target;
                continue;
            }

            let headers = safe_response_headers(response.headers(), &credential_names);
            let content_type = response_content_type(response.headers());
            let content_length_describes_body =
                method != reqwest::Method::HEAD && !matches!(status, 204 | 304);
            let response_body = read_bounded_body(
                &mut response,
                limits.upstream_bytes(),
                content_length_describes_body,
            )
            .await?;
            span.record("http.response.body.size", safe::number(response_body.len()));
            *response_bytes = Some(response_body.len());
            let content_type =
                selected_decode_facts(&binding, status, !response_body.is_empty(), content_type);
            let (data, decode_diagnostics) = decode_response_body(
                status,
                &response_body,
                &content_type,
                &binding,
                &schema_graph,
                limits,
            )?;
            let class = classify(
                status,
                &binding,
                !redirects.is_empty(),
                !response_body.is_empty(),
                &decode_diagnostics,
            );
            let outcome = HttpOutcome {
                status,
                data,
                headers,
                media_type: content_type.media_type,
                charset: content_type.charset,
                final_url: safe_url(&url),
                redirects,
                decode_diagnostics,
                class,
            };
            let encoded =
                serde_json::to_string(&outcome.tool_result()).map_err(Error::EncodeJson)?;
            if encoded.len() > limits.upstream_bytes() {
                return Err(Error::ResultTooLarge);
            }
            limits
                .validate_json(&encoded)
                .map_err(Error::invalid_result)?;
            return Ok(outcome);
        }
    }
}

fn redirect_method(status: u16, method: &reqwest::Method) -> Option<reqwest::Method> {
    match status {
        301 | 302 if *method == reqwest::Method::POST => Some(reqwest::Method::GET),
        301 | 302 | 307 | 308 => Some(method.clone()),
        303 if *method == reqwest::Method::HEAD => Some(reqwest::Method::HEAD),
        303 => Some(reqwest::Method::GET),
        _ => None,
    }
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn safe_url(url: &Url) -> String {
    let mut safe = url.clone();
    let _username_removed = safe.set_username("");
    let _password_removed = safe.set_password(None);
    safe.set_query(None);
    safe.set_fragment(None);
    safe.to_string()
}

struct ResponseContentType {
    parsed: Option<mime::Mime>,
    media_type: Option<String>,
    charset: Option<String>,
    malformed: bool,
}

fn response_content_type(headers: &reqwest::header::HeaderMap) -> ResponseContentType {
    let header = headers.get(reqwest::header::CONTENT_TYPE);
    let parsed = header
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<mime::Mime>().ok())
        .filter(|value| value.type_() != mime::STAR && value.subtype() != mime::STAR);
    let media_type = parsed
        .as_ref()
        .map(|value| value.essence_str().to_ascii_lowercase());
    let charset = parsed
        .as_ref()
        .and_then(|value| value.get_param(mime::CHARSET))
        .map(|value| value.as_str().to_ascii_lowercase());
    ResponseContentType {
        malformed: header.is_some() && parsed.is_none(),
        parsed,
        media_type,
        charset,
    }
}

fn parse_content_type(value: &str) -> (Option<String>, Option<String>) {
    let parsed = value.parse::<mime::Mime>().ok();
    let media_type = parsed
        .as_ref()
        .map(|value| value.essence_str().to_ascii_lowercase());
    let charset = parsed
        .as_ref()
        .and_then(|value| value.get_param(mime::CHARSET))
        .map(|value| value.as_str().to_ascii_lowercase());
    (media_type, charset)
}

fn selected_decode_facts(
    binding: &HttpExecutionBinding,
    status: u16,
    body_present: bool,
    mut content_type: ResponseContentType,
) -> ResponseContentType {
    let declared_content = binding.response_for(status).and_then(|response| {
        response.content_for(content_type.parsed.as_ref(), !content_type.malformed)
    });
    if content_type.media_type.is_none() {
        content_type.media_type =
            declared_content.map(|content| media_type_essence(content.media_type()));
    }
    if content_type.charset.is_none() {
        content_type.charset = declared_content
            .and_then(|content| parse_content_type(content.media_type()).1)
            .or_else(|| {
                (body_present
                    && content_type
                        .media_type
                        .as_deref()
                        .is_some_and(|media_type| {
                            is_json_media_type(media_type) || media_type.starts_with("text/")
                        }))
                .then(|| "utf-8".to_string())
            });
    }
    content_type
}

fn decode_response_body(
    status: u16,
    body: &[u8],
    content_type: &ResponseContentType,
    binding: &HttpExecutionBinding,
    schema_graph: &SchemaGraph,
    limits: coral_value::ValueLimits,
) -> Result<(Value, Vec<HttpDecodeDiagnostic>), Error> {
    let mut diagnostics = Vec::new();
    let response = binding.response_for(status);
    if response.is_none() {
        push_diagnostic(
            &mut diagnostics,
            "undeclared_status",
            "the response status is not declared by this Tool",
        );
    }
    let selected_content = response.and_then(|response| {
        response.content_for(content_type.parsed.as_ref(), !content_type.malformed)
    });
    if content_type.malformed {
        push_diagnostic(
            &mut diagnostics,
            "malformed_content_type",
            "the response Content-Type header is malformed",
        );
    }
    if selected_content.is_none()
        && (!body.is_empty() || response.is_some_and(|response| !response.content().is_empty()))
    {
        push_diagnostic(
            &mut diagnostics,
            "undeclared_media_type",
            "the response media type does not select one declared alternative",
        );
    }
    let decode_media = content_type
        .media_type
        .as_deref()
        .map(str::to_string)
        .or_else(|| selected_content.map(|content| media_type_essence(content.media_type())));
    let binary_media = decode_media
        .as_deref()
        .is_some_and(|media| !is_json_media_type(media) && !media.starts_with("text/"));
    let declared_binary =
        selected_content.is_some_and(|content| schema_graph.is_binary(content.schema()));
    let (data, decoded_semantic_value) =
        if declared_binary || binary_media || (decode_media.is_none() && !body.is_empty()) {
            let media = decode_media
                .as_deref()
                .filter(|media| !media.contains('*'))
                .unwrap_or("application/octet-stream");
            (
                coral_value::binary_value(body, media, limits.binary_bytes())
                    .map_err(Error::invalid_result)?,
                false,
            )
        } else if body.is_empty() {
            (Value::Null, true)
        } else if decode_media.as_deref().is_some_and(is_json_media_type) {
            let (text, text_valid) =
                decode_text(body, content_type.charset.as_deref(), &mut diagnostics);
            match serde_json::from_str(&text) {
                Ok(value) => (value, text_valid),
                Err(_error) => {
                    push_diagnostic(
                        &mut diagnostics,
                        "malformed_body",
                        "the complete response body is not valid JSON",
                    );
                    (Value::String(text), false)
                }
            }
        } else {
            let (text, valid) =
                decode_text(body, content_type.charset.as_deref(), &mut diagnostics);
            (Value::String(text), valid)
        };
    if decoded_semantic_value
        && let Some(content) = selected_content
        && !schema_graph.accepts_output(content.schema(), &data)
    {
        push_diagnostic(
            &mut diagnostics,
            "schema_incompatible",
            "the decoded response body is incompatible with its declared schema",
        );
    }
    Ok((data, diagnostics))
}

const fn is_success_code(status: u16) -> bool {
    matches!(status, 200..=299)
}

/// Classifies one decoded response by the binding's
/// [`HttpExecutionBinding::success_for`] verdict. A success's body matters
/// only when it has one: a body that does not decode rejects the call, and so
/// does a media type that a declared success does not list. A schema mismatch
/// never changes the class.
fn classify(
    status: u16,
    binding: &HttpExecutionBinding,
    redirected: bool,
    body_present: bool,
    diagnostics: &[HttpDecodeDiagnostic],
) -> OutcomeClass {
    let reported = |codes: &[&str]| {
        body_present
            && diagnostics
                .iter()
                .any(|diagnostic| codes.contains(&diagnostic.code))
    };
    let undecodable = reported(&[
        "malformed_content_type",
        "malformed_body",
        "unsupported_charset",
    ]);
    match binding.success_for(status, redirected) {
        SuccessSelection::ErrorStatus => OutcomeClass::ErrorResponse,
        SuccessSelection::Undeclared => OutcomeClass::UndeclaredResponse,
        _ if undecodable => OutcomeClass::UndecodableResponse,
        SuccessSelection::Declared(response)
            if !response.content().is_empty() && reported(&["undeclared_media_type"]) =>
        {
            OutcomeClass::UndeclaredResponse
        }
        SuccessSelection::Declared(_) | SuccessSelection::Untyped => OutcomeClass::Success,
    }
}

fn decode_text(
    body: &[u8],
    charset: Option<&str>,
    diagnostics: &mut Vec<HttpDecodeDiagnostic>,
) -> (String, bool) {
    let label = charset.unwrap_or("utf-8");
    let Some(encoding) = encoding_rs::Encoding::for_label(label.as_bytes()) else {
        push_diagnostic(
            diagnostics,
            "unsupported_charset",
            "the response charset is not supported",
        );
        return (String::from_utf8_lossy(body).into_owned(), false);
    };
    let (decoded, _used_encoding, malformed) = encoding.decode(body);
    if malformed {
        push_diagnostic(
            diagnostics,
            "malformed_body",
            "the complete response body is malformed for its selected charset",
        );
    }
    (decoded.into_owned(), !malformed)
}

fn push_diagnostic(
    diagnostics: &mut Vec<HttpDecodeDiagnostic>,
    code: &'static str,
    message: &'static str,
) {
    if !diagnostics.iter().any(|diagnostic| diagnostic.code == code) {
        diagnostics.push(HttpDecodeDiagnostic::new(code, message));
    }
}

/// The OpenTelemetry HTTP client span of one request: method, scheme,
/// server address and port, the route `template` when the request follows
/// one, and sizes. Never the full URL, a filled path, a query or a header.
fn client_span(
    method: &reqwest::Method,
    url: &url::Url,
    template: Option<Declared<'_>>,
    request_body_size: Option<usize>,
) -> tracing::Span {
    tracing::info_span!(
        "http.client",
        otel.name = safe::http_span_name(method, template.as_ref()),
        otel.kind = "client",
        http.request.method = safe::http_method(method),
        url.scheme = safe::kind(span_field::scheme(url)),
        server.address = span_field::host(url).as_ref().map(safe::id),
        server.port = url.port_or_known_default().map(safe::number),
        url.template = template.as_ref().map(safe::id),
        http.request.body.size = request_body_size.map(safe::number),
        http.response.status_code = tracing::field::Empty,
        http.response.body.size = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}
