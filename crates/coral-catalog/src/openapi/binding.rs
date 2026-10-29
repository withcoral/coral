use serde::{Deserialize, Serialize};

use crate::{Error, SchemaId};

/// HTTP method retained by the profile Execution Binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpMethod {
    /// GET.
    Get,
    /// PUT.
    Put,
    /// POST.
    Post,
    /// DELETE.
    Delete,
    /// OPTIONS.
    Options,
    /// HEAD.
    Head,
    /// PATCH.
    Patch,
    /// TRACE.
    Trace,
}

impl HttpMethod {
    /// The methods RFC 9110 defines as safe: a client does not request a state change.
    pub const SAFE: [Self; 4] = [Self::Get, Self::Head, Self::Options, Self::Trace];

    /// Returns whether RFC 9110 defines this method as safe.
    #[must_use]
    pub fn is_safe(self) -> bool {
        Self::SAFE.contains(&self)
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "get" => Some(Self::Get),
            "put" => Some(Self::Put),
            "post" => Some(Self::Post),
            "delete" => Some(Self::Delete),
            "options" => Some(Self::Options),
            "head" => Some(Self::Head),
            "patch" => Some(Self::Patch),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }

    /// Returns the lowercase OpenAPI spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Post => "post",
            Self::Delete => "delete",
            Self::Options => "options",
            Self::Head => "head",
            Self::Patch => "patch",
            Self::Trace => "trace",
        }
    }
}

/// Location of one OpenAPI parameter on the HTTP wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpParameterLocation {
    /// URI-template path parameter.
    Path,
    /// URL query parameter.
    Query,
    /// Request header parameter.
    Header,
    /// Cookie parameter.
    Cookie,
}

/// OpenAPI parameter serialization style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HttpParameterStyle {
    /// Matrix path serialization.
    Matrix,
    /// Label path serialization.
    Label,
    /// Simple path or header serialization.
    Simple,
    /// Form query or cookie serialization.
    Form,
    /// Space-delimited query array serialization.
    SpaceDelimited,
    /// Pipe-delimited query array serialization.
    PipeDelimited,
    /// Deep-object query serialization.
    DeepObject,
}

/// One typed OpenAPI parameter recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "required, explode, allowReserved, and allowEmptyValue are independent OpenAPI wire facts"
)]
pub struct HttpParameterBinding {
    logical_name: String,
    wire_name: String,
    location: HttpParameterLocation,
    required: bool,
    style: Option<HttpParameterStyle>,
    explode: bool,
    allow_reserved: bool,
    allow_empty_value: bool,
    content_media_type: Option<String>,
    schema: SchemaId,
}

impl HttpParameterBinding {
    #[expect(
        clippy::too_many_arguments,
        clippy::fn_params_excessive_bools,
        reason = "the immutable recipe retains independent OpenAPI wire facts"
    )]
    pub(crate) const fn new(
        logical_name: String,
        wire_name: String,
        location: HttpParameterLocation,
        required: bool,
        style: Option<HttpParameterStyle>,
        explode: bool,
        allow_reserved: bool,
        allow_empty_value: bool,
        content_media_type: Option<String>,
        schema: SchemaId,
    ) -> Self {
        Self {
            logical_name,
            wire_name,
            location,
            required,
            style,
            explode,
            allow_reserved,
            allow_empty_value,
            content_media_type,
            schema,
        }
    }

    /// Returns the flat logical argument name.
    #[must_use]
    pub fn logical_name(&self) -> &str {
        &self.logical_name
    }

    /// Returns the provider-authored wire name.
    #[must_use]
    pub fn wire_name(&self) -> &str {
        &self.wire_name
    }

    /// Returns the exact wire location.
    #[must_use]
    pub const fn location(&self) -> HttpParameterLocation {
        self.location
    }

    /// Returns whether the parameter is required.
    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    /// Returns the style, or `None` for content-based serialization.
    #[must_use]
    pub const fn style(&self) -> Option<HttpParameterStyle> {
        self.style
    }

    /// Returns the resolved OpenAPI explode behavior.
    #[must_use]
    pub const fn explode(&self) -> bool {
        self.explode
    }

    /// Returns whether reserved query characters remain unescaped.
    #[must_use]
    pub const fn allow_reserved(&self) -> bool {
        self.allow_reserved
    }

    /// Returns whether an empty query value is allowed.
    #[must_use]
    pub const fn allow_empty_value(&self) -> bool {
        self.allow_empty_value
    }

    /// Returns the content media type for content-based parameters.
    #[must_use]
    pub fn content_media_type(&self) -> Option<&str> {
        self.content_media_type.as_deref()
    }

    /// Returns the authoritative parameter schema root.
    #[must_use]
    pub const fn schema(&self) -> &SchemaId {
        &self.schema
    }
}

/// Declared response-header or multipart-part header recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpHeaderBinding {
    wire_name: String,
    required: bool,
    style: Option<HttpParameterStyle>,
    explode: bool,
    content_media_type: Option<String>,
    schema: SchemaId,
}

impl HttpHeaderBinding {
    pub(crate) const fn new(
        wire_name: String,
        required: bool,
        style: Option<HttpParameterStyle>,
        explode: bool,
        content_media_type: Option<String>,
        schema: SchemaId,
    ) -> Self {
        Self {
            wire_name,
            required,
            style,
            explode,
            content_media_type,
            schema,
        }
    }

    /// Returns the case-preserving provider wire name.
    #[must_use]
    pub fn wire_name(&self) -> &str {
        &self.wire_name
    }

    /// Returns whether the header is required by the declaration.
    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    /// Returns the style, or `None` for content-based headers.
    #[must_use]
    pub const fn style(&self) -> Option<HttpParameterStyle> {
        self.style
    }

    /// Returns the resolved explode behavior.
    #[must_use]
    pub const fn explode(&self) -> bool {
        self.explode
    }

    /// Returns the content media type for content-based headers.
    #[must_use]
    pub fn content_media_type(&self) -> Option<&str> {
        self.content_media_type.as_deref()
    }

    /// Returns the authoritative header schema root.
    #[must_use]
    pub const fn schema(&self) -> &SchemaId {
        &self.schema
    }
}

/// One property-specific form or multipart encoding recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpEncodingBinding {
    property_name: String,
    content_type: Option<String>,
    headers: Vec<HttpHeaderBinding>,
    style: Option<HttpParameterStyle>,
    explode: bool,
    allow_reserved: bool,
}

impl HttpEncodingBinding {
    pub(crate) const fn new(
        property_name: String,
        content_type: Option<String>,
        headers: Vec<HttpHeaderBinding>,
        style: Option<HttpParameterStyle>,
        explode: bool,
        allow_reserved: bool,
    ) -> Self {
        Self {
            property_name,
            content_type,
            headers,
            style,
            explode,
            allow_reserved,
        }
    }

    /// Returns the encoded object property.
    #[must_use]
    pub fn property_name(&self) -> &str {
        &self.property_name
    }

    /// Returns an exact declared part content type.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Returns declared part headers.
    #[must_use]
    pub fn headers(&self) -> &[HttpHeaderBinding] {
        &self.headers
    }

    /// Returns an explicitly declared form style.
    #[must_use]
    pub const fn style(&self) -> Option<HttpParameterStyle> {
        self.style
    }

    /// Returns the resolved explode behavior.
    #[must_use]
    pub const fn explode(&self) -> bool {
        self.explode
    }

    /// Returns whether reserved characters remain unescaped.
    #[must_use]
    pub const fn allow_reserved(&self) -> bool {
        self.allow_reserved
    }
}

/// One request or response media alternative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpContentBinding {
    media_type: String,
    schema: SchemaId,
    encoding: Vec<HttpEncodingBinding>,
}

impl HttpContentBinding {
    pub(crate) const fn new(
        media_type: String,
        schema: SchemaId,
        encoding: Vec<HttpEncodingBinding>,
    ) -> Self {
        Self {
            media_type,
            schema,
            encoding,
        }
    }

    /// Returns the provider-declared media type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    /// Returns the authoritative body schema root.
    #[must_use]
    pub const fn schema(&self) -> &SchemaId {
        &self.schema
    }

    /// Returns property encodings for forms or multipart content.
    #[must_use]
    pub fn encoding(&self) -> &[HttpEncodingBinding] {
        &self.encoding
    }
}

/// Complete OpenAPI request-body alternatives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestBodyBinding {
    required: bool,
    content: Vec<HttpContentBinding>,
}

impl HttpRequestBodyBinding {
    pub(crate) const fn new(required: bool, content: Vec<HttpContentBinding>) -> Self {
        Self { required, content }
    }

    /// Returns whether the request body is required.
    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    /// Returns every declared request media alternative.
    #[must_use]
    pub fn content(&self) -> &[HttpContentBinding] {
        &self.content
    }
}

/// OpenAPI response selector retained without normalizing status alternatives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HttpResponseStatus {
    /// Exact three-digit status code.
    Code(u16),
    /// One-hundred status range such as `2XX`, represented by `2`.
    Range(u16),
    /// OpenAPI default response.
    Default,
}

impl HttpResponseStatus {
    /// Returns whether this selector names a 2xx code or the `2XX` range.
    #[must_use]
    pub const fn is_success(self) -> bool {
        match self {
            Self::Code(code) => matches!(code, 200..=299),
            Self::Range(range) => range == 2,
            Self::Default => false,
        }
    }
}

/// One complete declared HTTP response alternative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponseBinding {
    status: HttpResponseStatus,
    headers: Vec<HttpHeaderBinding>,
    content: Vec<HttpContentBinding>,
}

impl HttpResponseBinding {
    pub(crate) const fn new(
        status: HttpResponseStatus,
        headers: Vec<HttpHeaderBinding>,
        content: Vec<HttpContentBinding>,
    ) -> Self {
        Self {
            status,
            headers,
            content,
        }
    }

    /// Returns the exact status selector.
    #[must_use]
    pub const fn status(&self) -> HttpResponseStatus {
        self.status
    }

    /// Returns every declared response header without case normalization.
    #[must_use]
    pub fn headers(&self) -> &[HttpHeaderBinding] {
        &self.headers
    }

    /// Returns every declared response media alternative.
    #[must_use]
    pub fn content(&self) -> &[HttpContentBinding] {
        &self.content
    }

    /// Returns the media alternative an actual `Content-Type` selects: the
    /// most specific declared match whose parameters it carries. Without a
    /// `Content-Type`, the only alternative is selected when
    /// `allow_implicit_media` holds.
    #[must_use]
    pub fn content_for(
        &self,
        content_type: Option<&mime::Mime>,
        allow_implicit_media: bool,
    ) -> Option<&HttpContentBinding> {
        match content_type {
            Some(actual) => self
                .content
                .iter()
                .filter_map(|content| {
                    media_type_match_rank(content.media_type(), actual).map(|rank| (rank, content))
                })
                .min_by_key(|(rank, _content)| *rank)
                .map(|(_rank, content)| content),
            None if allow_implicit_media && self.content.len() == 1 => self.content.first(),
            None => None,
        }
    }
}

/// How specifically a declared media type matches an actual one: exact,
/// then `type/*`, then `*/*`, then by more declared parameters. `None` when
/// it does not match or names a parameter the actual type lacks.
fn media_type_match_rank(
    declared: &str,
    actual: &mime::Mime,
) -> Option<(u8, std::cmp::Reverse<usize>)> {
    let declared = declared.parse::<mime::Mime>().ok()?;
    let specificity = if declared
        .essence_str()
        .eq_ignore_ascii_case(actual.essence_str())
    {
        0
    } else if declared.type_() == mime::STAR && declared.subtype() == mime::STAR {
        2
    } else if declared.type_() == actual.type_() && declared.subtype() == mime::STAR {
        1
    } else {
        return None;
    };
    let parameters = declared.params().collect::<Vec<_>>();
    if parameters
        .iter()
        .any(|(name, value)| actual.get_param(*name) != Some(*value))
    {
        return None;
    }
    Some((specificity, std::cmp::Reverse(parameters.len())))
}

/// How one complete HTTP response relates to a Tool's declared
/// successes. A Program receives only `Declared` and `Untyped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessSelection<'b> {
    /// A non-2xx status, whether or not the Tool declares it.
    ErrorStatus,
    /// A 2xx status that selects no declared success.
    Undeclared,
    /// The final 2xx after a followed redirect, when the Tool declares no
    /// success to describe it. Its declaration types it as unknown.
    Untyped,
    /// The declared success response the status selects.
    Declared(&'b HttpResponseBinding),
}

/// Fixed host redirect behavior selected for this binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpRedirectPolicy {
    /// Follow ordinary HTTP redirects while stripping credentials cross-origin.
    Follow,
}

/// Reference to one host-owned credential injection slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCredentialSlot {
    scheme_name: String,
    contract: crate::ProviderCredentialContract,
}

impl HttpCredentialSlot {
    pub(crate) fn new(scheme_name: String, contract: crate::ProviderCredentialContract) -> Self {
        Self {
            scheme_name,
            contract,
        }
    }

    /// Returns the exact secret-free credential method and injection contract.
    #[must_use]
    pub const fn contract(&self) -> &crate::ProviderCredentialContract {
        &self.contract
    }
    /// Returns the secret-free OpenAPI security-scheme name.
    #[must_use]
    pub fn scheme_name(&self) -> &str {
        &self.scheme_name
    }
}

/// Complete typed HTTP invocation recipe retained without the OpenAPI document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpExecutionBinding {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    request_headers: std::collections::BTreeMap<String, String>,
    method: HttpMethod,
    server_url: Option<String>,
    path_template: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    redirect_policy: HttpRedirectPolicy,
    parameters: Vec<HttpParameterBinding>,
    request_body: Option<HttpRequestBodyBinding>,
    responses: Vec<HttpResponseBinding>,
    credential_slots: Vec<HttpCredentialSlot>,
}

impl HttpExecutionBinding {
    /// Returns the responses this Tool declares as successes: each 2xx
    /// code and the `2XX` range, or `default` when no 2xx response is
    /// declared. A Program receives only these; every other response rejects
    /// its call. With none, only [`SuccessSelection::Untyped`] can succeed.
    #[must_use]
    pub fn declared_successes(&self) -> Vec<&HttpResponseBinding> {
        let successes = self
            .responses
            .iter()
            .filter(|response| response.status().is_success())
            .collect::<Vec<_>>();
        if successes.is_empty() {
            self.responses
                .iter()
                .filter(|response| response.status() == HttpResponseStatus::Default)
                .collect()
        } else {
            successes
        }
    }

    /// Returns the declared response an actual status selects: its exact code,
    /// then its range, then `default`.
    #[must_use]
    pub fn response_for(&self, status: u16) -> Option<&HttpResponseBinding> {
        self.responses
            .iter()
            .find(|response| response.status() == HttpResponseStatus::Code(status))
            .or_else(|| {
                self.responses
                    .iter()
                    .find(|response| response.status() == HttpResponseStatus::Range(status / 100))
            })
            .or_else(|| {
                self.responses
                    .iter()
                    .find(|response| response.status() == HttpResponseStatus::Default)
            })
    }

    /// Returns how a complete response with `status`, reached after a followed
    /// redirect when `redirected`, relates to this Tool's declared successes.
    #[must_use]
    pub fn success_for(&self, status: u16, redirected: bool) -> SuccessSelection<'_> {
        if !matches!(status, 200..=299) {
            return SuccessSelection::ErrorStatus;
        }
        let declares_2xx = self
            .responses
            .iter()
            .any(|response| response.status().is_success());
        match self.response_for(status) {
            // A 2xx status selects `default` only when no 2xx response matches.
            Some(response)
                if response.status().is_success()
                    || (response.status() == HttpResponseStatus::Default && !declares_2xx) =>
            {
                SuccessSelection::Declared(response)
            }
            None if redirected && self.declared_successes().is_empty() => SuccessSelection::Untyped,
            _ => SuccessSelection::Undeclared,
        }
    }

    /// Returns confirmed non-secret headers, independent of account credentials.
    #[must_use]
    pub const fn request_headers(&self) -> &std::collections::BTreeMap<String, String> {
        &self.request_headers
    }

    pub(crate) fn set_request_headers(
        &mut self,
        headers: std::collections::BTreeMap<String, String>,
    ) {
        self.request_headers = headers;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one immutable HTTP recipe is assembled once"
    )]
    pub(crate) const fn new(
        method: HttpMethod,
        server_url: Option<String>,
        path_template: String,
        query: Option<String>,
        redirect_policy: HttpRedirectPolicy,
        parameters: Vec<HttpParameterBinding>,
        request_body: Option<HttpRequestBodyBinding>,
        responses: Vec<HttpResponseBinding>,
        credential_slots: Vec<HttpCredentialSlot>,
    ) -> Self {
        Self {
            request_headers: std::collections::BTreeMap::new(),
            method,
            server_url,
            path_template,
            query,
            redirect_policy,
            parameters,
            request_body,
            responses,
            credential_slots,
        }
    }

    pub(crate) fn set_credential_slots(&mut self, slots: Vec<HttpCredentialSlot>) {
        self.credential_slots = slots;
    }

    /// Returns the exact HTTP method.
    #[must_use]
    pub const fn method(&self) -> HttpMethod {
        self.method
    }

    /// Returns the normalized selected server URL when one could be established.
    #[must_use]
    pub fn server_url(&self) -> Option<&str> {
        self.server_url.as_deref()
    }

    /// Returns the provider-authored path template.
    #[must_use]
    pub fn path_template(&self) -> &str {
        &self.path_template
    }

    /// Returns the fixed query the provider's path declares, such as `beta=true`.
    #[must_use]
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Returns the selected redirect behavior.
    #[must_use]
    pub const fn redirect_policy(&self) -> HttpRedirectPolicy {
        self.redirect_policy
    }

    /// Returns every parameter serialization recipe.
    #[must_use]
    pub fn parameters(&self) -> &[HttpParameterBinding] {
        &self.parameters
    }

    /// Returns complete request media alternatives when a body is declared.
    #[must_use]
    pub const fn request_body(&self) -> Option<&HttpRequestBodyBinding> {
        self.request_body.as_ref()
    }

    /// Returns every declared status, header, and response media alternative.
    #[must_use]
    pub fn responses(&self) -> &[HttpResponseBinding] {
        &self.responses
    }

    /// Returns secret-free host credential slots.
    #[must_use]
    pub fn credential_slots(&self) -> &[HttpCredentialSlot] {
        &self.credential_slots
    }

    pub(crate) fn to_storage_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|_error| {
            Error::CorruptData("HTTP Execution Binding cannot be encoded".to_string())
        })
    }

    pub(crate) fn from_storage_json(value: &str) -> Result<Self, Error> {
        serde_json::from_str(value).map_err(|_error| {
            Error::CorruptData("HTTP Execution Binding is not valid canonical JSON".to_string())
        })
    }

    pub(crate) fn schema_ids(&self) -> impl Iterator<Item = &SchemaId> {
        self.parameters
            .iter()
            .map(HttpParameterBinding::schema)
            .chain(
                self.request_body
                    .iter()
                    .flat_map(HttpRequestBodyBinding::content)
                    .map(HttpContentBinding::schema),
            )
            .chain(
                self.request_body
                    .iter()
                    .flat_map(HttpRequestBodyBinding::content)
                    .flat_map(HttpContentBinding::encoding)
                    .flat_map(HttpEncodingBinding::headers)
                    .map(HttpHeaderBinding::schema),
            )
            .chain(
                self.responses
                    .iter()
                    .flat_map(HttpResponseBinding::headers)
                    .map(HttpHeaderBinding::schema),
            )
            .chain(
                self.responses
                    .iter()
                    .flat_map(HttpResponseBinding::content)
                    .map(HttpContentBinding::schema),
            )
    }
}
