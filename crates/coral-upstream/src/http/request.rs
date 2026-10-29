//! HTTP request planning and OpenAPI wire serialization.

use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{
    ExecutionBinding, HttpContentBinding, HttpEncodingBinding, HttpExecutionBinding, HttpMethod,
    HttpParameterBinding, HttpParameterLocation, HttpParameterStyle, MaterializedTool, SchemaGraph,
    ToolAvailability,
};
use serde_json::{Map, Value};
use url::Url;

use crate::{Error, is_json_media_type, media_type_essence};

/// A validated one-use HTTP request with a separate, redacted host credential slot.
#[derive(Debug)]
pub struct ReadyHttpDispatch {
    pub(super) credentials: Vec<(String, coral_credentials::Secret)>,
    /// Query parameters added only to the URL a request is sent to, never to `url`.
    pub(super) query_credentials: Vec<(String, coral_credentials::Secret)>,
    pub(super) credential_contract: coral_catalog::ProviderCredentialContract,
    pub(super) method: reqwest::Method,
    pub(super) url: Url,
    pub(super) headers: Vec<(String, String)>,
    pub(super) caller_header_names: Vec<String>,
    pub(super) body: Option<Vec<u8>>,
    pub(super) request_media_type: Option<String>,
    pub(super) credential_slots: Vec<String>,
    /// The Tool's binding, which selects among its declared responses.
    pub(super) binding: HttpExecutionBinding,
    pub(super) schema_graph: SchemaGraph,
    pub(super) limits: coral_value::ValueLimits,
}

impl ReadyHttpDispatch {
    /// Plans one HTTP Tool without resolving its host credential slots.
    ///
    /// The returned request retains the selected media recipe and host-owned
    /// credential slot names without resolving credential material.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments cannot be represented by the
    /// retained HTTP serialization recipe.
    pub fn plan(
        tool: &MaterializedTool,
        schema_graph: &SchemaGraph,
        arguments: &Value,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        if tool.availability() != ToolAvailability::Available {
            return Err(Error::ToolUnavailable);
        }
        limits.validate(arguments).map_err(Error::invalid_value)?;
        let ExecutionBinding::Http(binding) = tool.execution_binding() else {
            return Err(Error::UnsupportedProtocol);
        };
        let arguments = arguments.as_object().ok_or_else(|| {
            Error::InvalidArguments("Tool arguments must be an object".to_string())
        })?;
        let server_url = binding.server_url().ok_or(Error::MissingServer)?;
        let mut url = Url::parse(server_url).map_err(Error::InvalidUrl)?;
        let SerializedParameters {
            path,
            query,
            mut headers,
        } = serialize_parameters(binding.path_template(), binding.parameters(), arguments)?;
        let base_path = url.path().trim_end_matches('/');
        let tool_path = path.trim_start_matches('/');
        // URL parsing removes dot segments, which would send the request, and
        // its credentials, to a path the Tool does not declare.
        if tool_path.split('/').any(is_dot_segment) {
            return Err(Error::InvalidArguments(
                "the request path cannot contain a `.` or `..` segment".to_string(),
            ));
        }
        let combined_path = if tool_path.is_empty() {
            format!("{base_path}/")
        } else {
            format!("{base_path}/{tool_path}")
        };
        url.set_path(&combined_path);
        // The server's query, then the path's fixed query, then the arguments.
        let query = [
            url.query().unwrap_or_default(),
            binding.query().unwrap_or_default(),
            &query,
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("&");
        if !query.is_empty() {
            url.set_query(Some(&query));
        }
        headers.extend(
            binding
                .request_headers()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        let caller_header_names = headers.iter().map(|(name, _)| name.clone()).collect();
        let (body, request_media_type) =
            serialize_request_body(binding.request_body(), arguments, schema_graph, limits)?;
        if let Some(media_type) = &request_media_type {
            headers.push(("content-type".to_string(), media_type.clone()));
        }
        headers.sort();
        validate_request_headers(&headers)?;
        let request_bytes = headers.iter().fold(
            url.as_str()
                .len()
                .saturating_add(body.as_ref().map_or(0, Vec::len)),
            |size, (name, value)| size.saturating_add(name.len()).saturating_add(value.len()),
        );
        if request_bytes > limits.upstream_bytes() {
            return Err(Error::InputTooLarge);
        }
        let credential_slots = binding
            .credential_slots()
            .iter()
            .map(|slot| slot.scheme_name().to_string())
            .collect();
        let method = method(binding.method());
        Ok(Self {
            credentials: Vec::new(),
            query_credentials: Vec::new(),
            credential_contract: binding
                .credential_slots()
                .first()
                .map_or(coral_catalog::ProviderCredentialContract::NONE, |slot| {
                    slot.contract().clone()
                }),
            method,
            url,
            headers,
            caller_header_names,
            body,
            request_media_type,
            credential_slots,
            binding: binding.clone(),
            schema_graph: schema_graph.clone(),
            limits,
        })
    }

    /// Injects an account's Credential Field values only into the catalog-selected
    /// host headers or query parameters. Logical arguments cannot override or inspect
    /// this material, and the planned URL never holds it.
    /// # Errors
    /// Rejects a credential-free contract, a missing field, a header or query
    /// parameter the request already plans, a second injection, or an oversized request.
    pub fn authorize(&mut self, values: &coral_credentials::CredentialValues) -> Result<(), Error> {
        use coral_catalog::HeaderBinding;
        use coral_credentials::{CredentialValues, Secret};
        let field = |name: &str| values.get(name).ok_or(Error::CredentialRequired);
        let secret = |value: String| Secret::new(value).map_err(|_error| Error::CredentialRequired);
        let mut query = Vec::new();
        let credentials = match self.credential_contract.authentication() {
            AuthenticationConfiguration::None => return Err(Error::CredentialRequired),
            AuthenticationConfiguration::HeaderCredentials { headers } => headers
                .iter()
                .map(|(header, binding)| {
                    let value = match binding {
                        HeaderBinding::Field {
                            field: name,
                            prefix,
                        } => {
                            format!("{prefix}{}", field(name)?.expose())
                        }
                        HeaderBinding::Literal { value } => value.clone(),
                    };
                    Ok((header.clone(), secret(value)?))
                })
                .collect::<Result<Vec<_>, Error>>()?,
            AuthenticationConfiguration::QueryCredentials { parameters } => {
                query = parameters
                    .iter()
                    .map(|(name, source)| {
                        Ok((name.clone(), secret(part(source, values)?.to_owned())?))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                Vec::new()
            }
            AuthenticationConfiguration::BasicAuth { username, password } => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                let username = part(username, values)?;
                // Basic splits the pair at its first colon, so the service would
                // read a different username.
                if username.contains(':') {
                    return Err(Error::CredentialUnusable);
                }
                let pair =
                    zeroize::Zeroizing::new(format!("{username}:{}", part(password, values)?));
                // The encoded copy is a credential too, so it is wiped with the pair.
                let encoded = zeroize::Zeroizing::new(STANDARD.encode(pair.as_bytes()));
                vec![(
                    "authorization".into(),
                    secret(format!("Basic {}", encoded.as_str()))?,
                )]
            }
            AuthenticationConfiguration::Bearer
            | AuthenticationConfiguration::OAuthAuthorizationCode { .. }
            | AuthenticationConfiguration::OAuthDeviceCode { .. } => vec![(
                "authorization".into(),
                secret(format!(
                    "Bearer {}",
                    field(CredentialValues::TOKEN)?.expose()
                ))?,
            )],
        };
        if self.sent_bytes(&credentials, &query) > self.limits.upstream_bytes() {
            return Err(Error::InputTooLarge);
        }
        if !self.credentials.is_empty()
            || !self.query_credentials.is_empty()
            || credentials.iter().any(|(header, _)| {
                self.headers
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(header))
            })
            || query.iter().any(|(parameter, _)| {
                self.url
                    .query_pairs()
                    .any(|(name, _)| name == parameter.as_str())
            })
        {
            return Err(Error::CredentialRequired);
        }
        self.credentials = credentials;
        self.query_credentials = query;
        Ok(())
    }

    /// What the request weighs once its credentials join it: the URL, the body,
    /// every header, and each query parameter as the URL will carry it.
    fn sent_bytes(
        &self,
        credentials: &[(String, coral_credentials::Secret)],
        query: &[(String, coral_credentials::Secret)],
    ) -> usize {
        let encoded = |value: &str| {
            url::form_urlencoded::byte_serialize(value.as_bytes())
                .map(str::len)
                .sum::<usize>()
        };
        self.headers
            .iter()
            .map(|(name, value)| name.len().saturating_add(value.len()))
            .chain(
                credentials
                    .iter()
                    .map(|(name, value)| name.len().saturating_add(value.expose().len())),
            )
            // A parameter is percent-encoded on its way into the URL, and joins it
            // after a separator and an equals sign.
            .chain(query.iter().map(|(name, value)| {
                encoded(name)
                    .saturating_add(encoded(value.expose()))
                    .saturating_add(2)
            }))
            .fold(
                self.url
                    .as_str()
                    .len()
                    .saturating_add(self.body.as_ref().map_or(0, Vec::len)),
                usize::saturating_add,
            )
    }

    /// Returns the planned HTTP method for deterministic Adapters.
    #[must_use]
    pub const fn method(&self) -> &reqwest::Method {
        &self.method
    }

    /// Returns the complete planned URL for deterministic Adapters.
    #[must_use]
    pub const fn url(&self) -> &Url {
        &self.url
    }

    /// Returns request headers before host credential injection.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// Returns the completely encoded request body when one was selected.
    #[must_use]
    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }

    /// Returns the selected provider-declared request media type.
    #[must_use]
    pub fn request_media_type(&self) -> Option<&str> {
        self.request_media_type.as_deref()
    }

    /// Returns unresolved host-owned credential slot names.
    #[must_use]
    pub fn credential_slots(&self) -> &[String] {
        &self.credential_slots
    }

    /// Returns the accepted complete-body bound.
    #[must_use]
    pub const fn result_max_bytes(&self) -> usize {
        self.limits.upstream_bytes()
    }
}

/// One credential part: a Credential Field's value, or a constant.
fn part<'a>(
    source: &'a coral_catalog::CredentialSource,
    values: &'a coral_credentials::CredentialValues,
) -> Result<&'a str, Error> {
    match source {
        coral_catalog::CredentialSource::Field { field } => values
            .get(field)
            .map(coral_credentials::Secret::expose)
            .ok_or(Error::CredentialRequired),
        coral_catalog::CredentialSource::Literal { value } => Ok(value),
    }
}

/// Whether URL parsing reads `segment` as `.` or `..`, including its
/// percent-encoded spellings. Java servlet containers drop a segment's `;`
/// parameters first, and a proxy in front may decode `%3B` into one, so
/// `..;x` and `..%3Bx` count too.
fn is_dot_segment(segment: &str) -> bool {
    let segment = segment.to_ascii_lowercase().replace("%3b", ";");
    let name = segment
        .split_once(';')
        .map_or(segment.as_str(), |(name, _)| name);
    matches!(name.replace("%2e", ".").as_str(), "." | "..")
}

/// Every string a path argument carries: scalars, array items, and object
/// keys and values.
fn path_texts(value: &Value) -> Box<dyn Iterator<Item = &str> + '_> {
    match value {
        Value::String(text) => Box::new(std::iter::once(text.as_str())),
        Value::Array(values) => Box::new(values.iter().flat_map(path_texts)),
        Value::Object(values) => Box::new(
            values
                .iter()
                .flat_map(|(key, value)| std::iter::once(key.as_str()).chain(path_texts(value))),
        ),
        Value::Null | Value::Bool(_) | Value::Number(_) => Box::new(std::iter::empty()),
    }
}

fn method(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Delete => reqwest::Method::DELETE,
        HttpMethod::Options => reqwest::Method::OPTIONS,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Trace => reqwest::Method::TRACE,
    }
}

struct SerializedParameters {
    path: String,
    query: String,
    headers: Vec<(String, String)>,
}

fn serialize_parameters(
    path_template: &str,
    parameters: &[HttpParameterBinding],
    arguments: &Map<String, Value>,
) -> Result<SerializedParameters, Error> {
    let mut path = path_template.to_string();
    let mut query = Vec::new();
    let mut headers = Vec::new();
    let mut cookies = Vec::new();
    for parameter in parameters {
        let value = arguments.get(parameter.logical_name());
        let Some(value) = value else {
            if parameter.required() {
                return Err(Error::InvalidArguments(format!(
                    "required Tool argument `{}` is missing",
                    parameter.logical_name()
                )));
            }
            continue;
        };
        match parameter.location() {
            HttpParameterLocation::Path => {
                // An encoded `/` keeps the value in one segment here, but an
                // upstream that decodes it before routing would read these.
                if path_texts(value).any(|text| {
                    text.contains(['/', '\\']) && text.split(['/', '\\']).any(is_dot_segment)
                }) {
                    return Err(Error::InvalidArguments(format!(
                        "Tool argument `{}` cannot contain a `.` or `..` path segment",
                        parameter.logical_name()
                    )));
                }
                let encoded = serialize_path_parameter(parameter, value)?;
                path = path.replace(&format!("{{{}}}", parameter.wire_name()), &encoded);
            }
            HttpParameterLocation::Query => {
                query.extend(serialize_query_parameter(parameter, value)?);
            }
            HttpParameterLocation::Header => {
                let value = match parameter.content_media_type() {
                    Some(media_type) => serialize_content_parameter(value, media_type)?,
                    None => serialize_simple(value, parameter.explode())?,
                };
                headers.push((parameter.wire_name().to_ascii_lowercase(), value));
            }
            HttpParameterLocation::Cookie => {
                cookies.push(serialize_cookie_parameter(parameter, value)?);
            }
        }
    }
    let has_body = arguments.contains_key("body");
    for name in arguments.keys() {
        if name == "body" && has_body {
            continue;
        }
        if !parameters
            .iter()
            .any(|parameter| parameter.logical_name() == name)
        {
            return Err(Error::InvalidArguments(format!(
                "Tool argument `{name}` is not declared"
            )));
        }
    }
    if !cookies.is_empty() {
        headers.push(("cookie".to_string(), cookies.join("; ")));
    }
    let query = query
        .into_iter()
        .map(|(name, value, allow_reserved)| {
            format!(
                "{}={}",
                percent_encode(&name, false),
                percent_encode(&value, allow_reserved)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    Ok(SerializedParameters {
        path,
        query,
        headers,
    })
}

fn serialize_path_parameter(
    parameter: &HttpParameterBinding,
    value: &Value,
) -> Result<String, Error> {
    if let Some(media_type) = parameter.content_media_type() {
        return serialize_content_parameter(value, media_type)
            .map(|value| percent_encode(&value, false));
    }
    let style = parameter.style().ok_or_else(|| {
        Error::InvalidArguments("path parameter has no serialization style".to_string())
    })?;
    let name = percent_encode(parameter.wire_name(), false);
    let encoded = match style {
        HttpParameterStyle::Simple => serialize_simple_encoded(value, parameter.explode())?,
        HttpParameterStyle::Label => match value {
            Value::Array(values) => {
                let separator = if parameter.explode() { "." } else { "," };
                format!(".{}", serialize_array(values, separator, false)?)
            }
            Value::Object(values) => {
                let separator = if parameter.explode() { "." } else { "," };
                format!(
                    ".{}",
                    serialize_object(values, separator, parameter.explode(), false)?
                )
            }
            scalar => format!(".{}", encode_scalar(scalar, false)?),
        },
        HttpParameterStyle::Matrix => match value {
            Value::Array(values) if parameter.explode() => values
                .iter()
                .map(|value| Ok(format!(";{name}={}", encode_scalar(value, false)?)))
                .collect::<Result<Vec<_>, Error>>()?
                .join(""),
            Value::Array(values) => {
                format!(";{name}={}", serialize_array(values, ",", false)?)
            }
            Value::Object(values) if parameter.explode() => sorted_object(values)
                .into_iter()
                .map(|(key, value)| {
                    Ok(format!(
                        ";{}={}",
                        percent_encode(key, false),
                        encode_scalar(value, false)?
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?
                .join(""),
            Value::Object(values) => {
                format!(";{name}={}", serialize_object(values, ",", false, false)?)
            }
            scalar => format!(";{name}={}", encode_scalar(scalar, false)?),
        },
        _ => {
            return Err(Error::InvalidArguments(
                "query serialization style cannot be used in a path".to_string(),
            ));
        }
    };
    Ok(encoded)
}

fn serialize_query_parameter(
    parameter: &HttpParameterBinding,
    value: &Value,
) -> Result<Vec<(String, String, bool)>, Error> {
    if value.as_str() == Some("") && !parameter.allow_empty_value() {
        return Err(Error::InvalidArguments(format!(
            "Tool argument `{}` does not allow an empty query value",
            parameter.logical_name()
        )));
    }
    if let Some(media_type) = parameter.content_media_type() {
        return Ok(vec![(
            parameter.wire_name().to_string(),
            serialize_content_parameter(value, media_type)?,
            parameter.allow_reserved(),
        )]);
    }
    let style = parameter.style().ok_or_else(|| {
        Error::InvalidArguments("query parameter has no serialization style".to_string())
    })?;
    let name = parameter.wire_name().to_string();
    match (style, value) {
        (HttpParameterStyle::Form, Value::Array(values)) if parameter.explode() => values
            .iter()
            .map(|value| {
                Ok((
                    name.clone(),
                    scalar_text(value)?,
                    parameter.allow_reserved(),
                ))
            })
            .collect(),
        (HttpParameterStyle::Form, Value::Object(values)) if parameter.explode() => {
            sorted_object(values)
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        key.to_string(),
                        scalar_text(value)?,
                        parameter.allow_reserved(),
                    ))
                })
                .collect()
        }
        (HttpParameterStyle::Form, Value::Array(values)) => Ok(vec![(
            name,
            serialize_array_text(values, ",")?,
            parameter.allow_reserved(),
        )]),
        (HttpParameterStyle::Form, Value::Object(values)) => Ok(vec![(
            name,
            serialize_object_text(values, ",", false)?,
            parameter.allow_reserved(),
        )]),
        (HttpParameterStyle::Form, scalar) => Ok(vec![(
            name,
            scalar_text(scalar)?,
            parameter.allow_reserved(),
        )]),
        (HttpParameterStyle::SpaceDelimited, Value::Array(values)) => Ok(vec![(
            name,
            serialize_array_text(values, " ")?,
            parameter.allow_reserved(),
        )]),
        (HttpParameterStyle::PipeDelimited, Value::Array(values)) => Ok(vec![(
            name,
            serialize_array_text(values, "|")?,
            parameter.allow_reserved(),
        )]),
        (HttpParameterStyle::DeepObject, Value::Object(values)) => sorted_object(values)
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    format!("{name}[{key}]"),
                    scalar_text(value)?,
                    parameter.allow_reserved(),
                ))
            })
            .collect(),
        _ => Err(Error::InvalidArguments(format!(
            "Tool argument `{}` does not match its query serialization",
            parameter.logical_name()
        ))),
    }
}

fn serialize_cookie_parameter(
    parameter: &HttpParameterBinding,
    value: &Value,
) -> Result<String, Error> {
    let name = parameter.wire_name();
    if let Some(media_type) = parameter.content_media_type() {
        let value = serialize_content_parameter(value, media_type)?;
        return Ok(format!("{name}={}", percent_encode(&value, false)));
    }
    if parameter.style() != Some(HttpParameterStyle::Form) {
        return Err(Error::InvalidArguments(
            "cookie parameter does not use form serialization".to_string(),
        ));
    }
    match value {
        Value::Array(values) if parameter.explode() => values
            .iter()
            .map(|value| Ok(format!("{name}={}", encode_scalar(value, false)?)))
            .collect::<Result<Vec<_>, Error>>()
            .map(|values| values.join("&")),
        Value::Object(values) if parameter.explode() => sorted_object(values)
            .into_iter()
            .map(|(key, value)| {
                Ok(format!(
                    "{}={}",
                    percent_encode(key, false),
                    encode_scalar(value, false)?
                ))
            })
            .collect::<Result<Vec<_>, Error>>()
            .map(|values| values.join("&")),
        other => Ok(format!(
            "{name}={}",
            serialize_simple_encoded(other, false)?
        )),
    }
}

fn validate_request_headers(headers: &[(String, String)]) -> Result<(), Error> {
    for (name, value) in headers {
        reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_error| {
            Error::InvalidArguments("a serialized request header name is invalid".to_string())
        })?;
        reqwest::header::HeaderValue::from_str(value).map_err(|_error| {
            Error::InvalidArguments("a serialized request header value is invalid".to_string())
        })?;
    }
    Ok(())
}

fn serialize_request_body(
    request_body: Option<&coral_catalog::HttpRequestBodyBinding>,
    arguments: &Map<String, Value>,
    schemas: &SchemaGraph,
    limits: coral_value::ValueLimits,
) -> Result<(Option<Vec<u8>>, Option<String>), Error> {
    let Some(binding) = request_body else {
        if arguments.contains_key("body") {
            return Err(Error::InvalidArguments(
                "Tool does not declare a request body".to_string(),
            ));
        }
        return Ok((None, None));
    };
    let Some(body) = arguments.get("body") else {
        if binding.required() {
            return Err(Error::InvalidArguments(
                "required Tool argument `body` is missing".to_string(),
            ));
        }
        return Ok((None, None));
    };
    let (content, body) = select_request_content(binding.content(), body).ok_or_else(|| {
        Error::InvalidArguments(
            "request body does not match a supported media alternative".to_string(),
        )
    })?;
    let essence = media_type_essence(content.media_type());
    if essence == "multipart/form-data" {
        let (body, media) = serialize_multipart(body, content, schemas, limits)?;
        return Ok((Some(body), Some(media)));
    }
    let encoded = if schemas.is_binary(content.schema()) {
        coral_value::decode_binary(body, limits.binary_bytes()).map_err(Error::invalid_value)?
    } else if is_json_media_type(&essence) {
        serde_json::to_vec(body).map_err(Error::EncodeJson)?
    } else if essence == "application/x-www-form-urlencoded" {
        serialize_form_body(body, content.encoding())?.into_bytes()
    } else if essence.starts_with("text/") {
        encode_text_request(
            body.as_str().ok_or_else(|| {
                Error::InvalidArguments("text request bodies must be strings".to_string())
            })?,
            content.media_type(),
        )?
    } else {
        coral_value::decode_binary(body, limits.binary_bytes()).map_err(Error::invalid_value)?
    };
    if encoded.len() > limits.upstream_bytes() {
        return Err(Error::InputTooLarge);
    }
    Ok((Some(encoded), Some(content.media_type().to_string())))
}

fn serialize_multipart(
    body: &Value,
    content: &HttpContentBinding,
    schemas: &SchemaGraph,
    limits: coral_value::ValueLimits,
) -> Result<(Vec<u8>, String), Error> {
    let object = body
        .as_object()
        .ok_or_else(|| Error::InvalidArguments("multipart body must be an object".to_string()))?;
    let mut parts = Vec::new();
    let mut size = 0usize;
    for (name, value) in sorted_object(object) {
        let encoding = content
            .encoding()
            .iter()
            .find(|encoding| encoding.property_name() == name);
        let values = if let Value::Array(values) = value {
            values.as_slice()
        } else {
            std::slice::from_ref(value)
        };
        for value in values {
            let part = multipart_part(name, value, encoding, schemas, limits)?;
            size = size.saturating_add(part.len());
            if size > limits.upstream_bytes() {
                return Err(Error::InputTooLarge);
            }
            parts.push(part);
        }
    }
    let boundary = loop {
        let candidate = format!("coral-{}", uuid::Uuid::new_v4().simple());
        if !parts.iter().any(|part| {
            part.windows(candidate.len())
                .any(|window| window == candidate.as_bytes())
        }) {
            break candidate;
        }
    };
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        bytes.extend_from_slice(&part);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    if bytes.len() > limits.upstream_bytes() {
        return Err(Error::InputTooLarge);
    }
    Ok((bytes, format!("multipart/form-data; boundary={boundary}")))
}

fn multipart_part(
    name: &str,
    value: &Value,
    encoding: Option<&HttpEncodingBinding>,
    schemas: &SchemaGraph,
    limits: coral_value::ValueLimits,
) -> Result<Vec<u8>, Error> {
    let binary = value.get("_tag").and_then(Value::as_str) == Some("BinaryValue");
    let media = encoding
        .and_then(HttpEncodingBinding::content_type)
        .unwrap_or(if binary {
            "application/octet-stream"
        } else if value.is_object() || value.is_array() {
            "application/json"
        } else {
            "text/plain"
        });
    let media = if media.parse::<mime::Mime>().is_ok() {
        media
    } else {
        let requested = binary
            .then(|| value.get("mimeType").and_then(Value::as_str))
            .flatten();
        media
            .split(',')
            .map(str::trim)
            .find(|candidate| {
                requested.is_some_and(|requested| {
                    candidate
                        .parse::<mime::Mime>()
                        .is_ok_and(|media| media.essence_str().eq_ignore_ascii_case(requested))
                })
            })
            .ok_or_else(|| {
                Error::InvalidArguments(
                    "Binary Value MIME type does not select a declared multipart alternative"
                        .to_string(),
                )
            })?
    };
    let body = if binary {
        coral_value::decode_binary(value, limits.binary_bytes()).map_err(Error::invalid_value)?
    } else if is_json_media_type(&media_type_essence(media)) {
        serde_json::to_vec(value).map_err(Error::EncodeJson)?
    } else {
        encode_text_request(&scalar_text(value)?, media)?
    };
    let mut disposition = format!("form-data; name=\"{}\"", multipart_quoted(name)?);
    if binary && let Some(name) = value.get("name").and_then(Value::as_str) {
        use std::fmt::Write as _;
        let _written = write!(disposition, "; filename=\"{}\"", multipart_quoted(name)?);
    }
    let mut headers = vec![
        ("Content-Disposition".to_string(), disposition),
        ("Content-Type".to_string(), media.to_string()),
    ];
    if let Some(encoding) = encoding {
        for header in encoding.headers() {
            if header.wire_name().eq_ignore_ascii_case("content-type") {
                continue;
            }
            if header
                .wire_name()
                .eq_ignore_ascii_case("content-disposition")
            {
                return Err(Error::InvalidArguments(
                    "part disposition is binding-owned".to_string(),
                ));
            }
            let schema = schemas.schema(header.schema());
            let value = schema.and_then(|s| s.get("const").or_else(|| s.get("default")));
            if let Some(value) = value {
                let encoded = if let Some(media) = header.content_media_type() {
                    serialize_content_parameter(value, media)?
                } else {
                    serialize_simple(value, header.explode())?
                };
                headers.push((header.wire_name().to_string(), encoded));
            } else if header.required() {
                return Err(Error::InvalidArguments(
                    "required part header has no binding value".to_string(),
                ));
            }
        }
    }
    validate_request_headers(&headers)?;
    let mut part = Vec::new();
    for (name, value) in headers {
        part.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    part.extend_from_slice(b"\r\n");
    part.extend_from_slice(&body);
    Ok(part)
}

fn multipart_quoted(value: &str) -> Result<String, Error> {
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidArguments(
            "invalid multipart name".to_string(),
        ));
    }
    Ok(value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn encode_text_request(value: &str, media_type: &str) -> Result<Vec<u8>, Error> {
    let parsed = media_type.parse::<mime::Mime>().map_err(|_error| {
        Error::InvalidArguments("text request body declares a malformed media type".to_string())
    })?;
    let charset = parsed
        .get_param(mime::CHARSET)
        .map_or_else(|| "utf-8".to_string(), |value| value.as_str().to_string());
    let encoding = encoding_rs::Encoding::for_label(charset.as_bytes()).ok_or_else(|| {
        Error::InvalidArguments("text request body declares an unsupported charset".to_string())
    })?;
    if !charset_label_matches_encoder(&charset, encoding) {
        return Err(Error::InvalidArguments(
            "text request body declares an ambiguous or remapped charset".to_string(),
        ));
    }
    if std::ptr::eq(encoding, encoding_rs::UTF_16LE) {
        return Ok(encode_utf16(value, u16::to_le_bytes));
    }
    if std::ptr::eq(encoding, encoding_rs::UTF_16BE) {
        return Ok(encode_utf16(value, u16::to_be_bytes));
    }
    let (encoded, used_encoding, unrepresentable) = encoding.encode(value);
    if !std::ptr::eq(used_encoding, encoding) {
        return Err(Error::InvalidArguments(
            "text request body declares a charset without an encoder".to_string(),
        ));
    }
    if unrepresentable {
        return Err(Error::InvalidArguments(
            "text request body cannot be represented by its declared charset".to_string(),
        ));
    }
    Ok(encoded.into_owned())
}

fn charset_label_matches_encoder(label: &str, encoding: &'static encoding_rs::Encoding) -> bool {
    if std::ptr::eq(encoding, encoding_rs::UTF_8) {
        return matches!(label.to_ascii_lowercase().as_str(), "utf-8" | "utf8");
    }
    if std::ptr::eq(encoding, encoding_rs::UTF_16LE) {
        return label.eq_ignore_ascii_case("utf-16le");
    }
    if std::ptr::eq(encoding, encoding_rs::UTF_16BE) {
        return label.eq_ignore_ascii_case("utf-16be");
    }
    label.eq_ignore_ascii_case(encoding.name())
}

fn encode_utf16(value: &str, byte_order: fn(u16) -> [u8; 2]) -> Vec<u8> {
    value.encode_utf16().flat_map(byte_order).collect()
}

fn select_request_content<'a, 'b>(
    alternatives: &'a [HttpContentBinding],
    body: &'b Value,
) -> Option<(&'a HttpContentBinding, &'b Value)> {
    let (content, data) = if let [content] = alternatives {
        (content, body)
    } else {
        let envelope = body.as_object()?;
        if envelope.len() != 2 {
            return None;
        }
        let media_type = envelope.get("mediaType")?.as_str()?;
        let data = envelope.get("data")?;
        let content = alternatives
            .iter()
            .find(|content| content.media_type() == media_type)?;
        (content, data)
    };
    let essence = media_type_essence(content.media_type());
    (is_json_media_type(&essence)
        || (matches!(
            essence.as_str(),
            "application/x-www-form-urlencoded" | "multipart/form-data"
        ) && data.is_object())
        || (essence.starts_with("text/") && data.is_string())
        || data.get("_tag").and_then(Value::as_str) == Some("BinaryValue"))
    .then_some((content, data))
}

fn serialize_form_body(body: &Value, encodings: &[HttpEncodingBinding]) -> Result<String, Error> {
    let object = body.as_object().ok_or_else(|| {
        Error::InvalidArguments("form request bodies must be objects".to_string())
    })?;
    let mut pairs = Vec::new();
    for (name, value) in sorted_object(object) {
        let encoding = encodings
            .iter()
            .find(|encoding| encoding.property_name() == name);
        pairs.extend(serialize_form_property(name, value, encoding)?);
    }
    Ok(pairs
        .into_iter()
        .map(|(name, value, allow_reserved)| {
            format!(
                "{}={}",
                form_percent_encode(&name, false),
                form_percent_encode(&value, allow_reserved)
            )
        })
        .collect::<Vec<_>>()
        .join("&"))
}

fn serialize_form_property(
    name: &str,
    value: &Value,
    encoding: Option<&HttpEncodingBinding>,
) -> Result<Vec<(String, String, bool)>, Error> {
    let allow_reserved = encoding.is_some_and(HttpEncodingBinding::allow_reserved);
    if let Some(content_type) = encoding.and_then(HttpEncodingBinding::content_type) {
        return Ok(vec![(
            name.to_string(),
            serialize_content_parameter(value, content_type)?,
            allow_reserved,
        )]);
    }
    let style = encoding
        .and_then(HttpEncodingBinding::style)
        .unwrap_or(HttpParameterStyle::Form);
    let explode = encoding.is_none_or(HttpEncodingBinding::explode);
    match (style, value) {
        (HttpParameterStyle::Form, Value::Array(values)) if explode => values
            .iter()
            .map(|value| Ok((name.to_string(), scalar_text(value)?, allow_reserved)))
            .collect(),
        (HttpParameterStyle::Form, Value::Object(values)) if explode => sorted_object(values)
            .into_iter()
            .map(|(key, value)| Ok((key.to_string(), scalar_text(value)?, allow_reserved)))
            .collect(),
        (HttpParameterStyle::Form, Value::Array(values)) => Ok(vec![(
            name.to_string(),
            serialize_array_text(values, ",")?,
            allow_reserved,
        )]),
        (HttpParameterStyle::Form, Value::Object(values)) => Ok(vec![(
            name.to_string(),
            serialize_object_text(values, ",", false)?,
            allow_reserved,
        )]),
        (HttpParameterStyle::Form, scalar) => Ok(vec![(
            name.to_string(),
            scalar_text(scalar)?,
            allow_reserved,
        )]),
        (HttpParameterStyle::SpaceDelimited, Value::Array(values)) => Ok(vec![(
            name.to_string(),
            serialize_array_text(values, " ")?,
            allow_reserved,
        )]),
        (HttpParameterStyle::PipeDelimited, Value::Array(values)) => Ok(vec![(
            name.to_string(),
            serialize_array_text(values, "|")?,
            allow_reserved,
        )]),
        (HttpParameterStyle::DeepObject, Value::Object(values)) => sorted_object(values)
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    format!("{name}[{key}]"),
                    scalar_text(value)?,
                    allow_reserved,
                ))
            })
            .collect(),
        _ => Err(Error::InvalidArguments(format!(
            "request body property `{name}` does not match its form encoding"
        ))),
    }
}

fn serialize_content_parameter(value: &Value, media_type: &str) -> Result<String, Error> {
    if is_json_media_type(&media_type_essence(media_type)) {
        serde_json::to_string(value).map_err(Error::EncodeJson)
    } else if media_type_essence(media_type).starts_with("text/") {
        value
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| Error::InvalidArguments("text parameters must be strings".to_string()))
    } else {
        Err(Error::UnsupportedRequestMedia(media_type.to_string()))
    }
}

fn serialize_simple(value: &Value, explode: bool) -> Result<String, Error> {
    match value {
        Value::Array(values) => serialize_array_text(values, ","),
        Value::Object(values) => serialize_object_text(values, ",", explode),
        scalar => scalar_text(scalar),
    }
}

fn serialize_simple_encoded(value: &Value, explode: bool) -> Result<String, Error> {
    match value {
        Value::Array(values) => serialize_array(values, ",", false),
        Value::Object(values) => serialize_object(values, ",", explode, false),
        scalar => encode_scalar(scalar, false),
    }
}

fn serialize_array_text(values: &[Value], separator: &str) -> Result<String, Error> {
    values
        .iter()
        .map(scalar_text)
        .collect::<Result<Vec<_>, Error>>()
        .map(|values| values.join(separator))
}

fn serialize_array(
    values: &[Value],
    separator: &str,
    allow_reserved: bool,
) -> Result<String, Error> {
    values
        .iter()
        .map(|value| encode_scalar(value, allow_reserved))
        .collect::<Result<Vec<_>, Error>>()
        .map(|values| values.join(separator))
}

fn serialize_object_text(
    values: &Map<String, Value>,
    separator: &str,
    explode: bool,
) -> Result<String, Error> {
    sorted_object(values)
        .into_iter()
        .map(|(key, value)| {
            Ok(if explode {
                format!("{key}={}", scalar_text(value)?)
            } else {
                format!("{key}{separator}{}", scalar_text(value)?)
            })
        })
        .collect::<Result<Vec<_>, Error>>()
        .map(|values| values.join(separator))
}

fn serialize_object(
    values: &Map<String, Value>,
    separator: &str,
    explode: bool,
    allow_reserved: bool,
) -> Result<String, Error> {
    sorted_object(values)
        .into_iter()
        .map(|(key, value)| {
            let key = percent_encode(key, allow_reserved);
            let value = encode_scalar(value, allow_reserved)?;
            Ok(if explode {
                format!("{key}={value}")
            } else {
                format!("{key}{separator}{value}")
            })
        })
        .collect::<Result<Vec<_>, Error>>()
        .map(|values| values.join(separator))
}

fn sorted_object(values: &Map<String, Value>) -> Vec<(&str, &Value)> {
    let mut values = values
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect::<Vec<_>>();
    values.sort_by_key(|(key, _value)| *key);
    values
}

fn encode_scalar(value: &Value, allow_reserved: bool) -> Result<String, Error> {
    scalar_text(value).map(|value| percent_encode(&value, allow_reserved))
}

fn scalar_text(value: &Value) -> Result<String, Error> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value.clone()),
        Value::Array(_) | Value::Object(_) => Err(Error::InvalidArguments(
            "nested parameter values cannot be serialized by this style".to_string(),
        )),
    }
}

fn percent_encode(value: &str, allow_reserved: bool) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        let allowed = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'.' | b'_' | b'~')
            || (allow_reserved
                && matches!(
                    byte,
                    b':' | b'/'
                        | b'?'
                        | b'#'
                        | b'['
                        | b']'
                        | b'@'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                ));
        if allowed {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _written = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn form_percent_encode(value: &str, allow_reserved: bool) -> String {
    percent_encode(value, allow_reserved).replace("%20", "+")
}
