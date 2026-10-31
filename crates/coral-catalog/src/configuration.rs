//! Secret-free configuration projection shared by source inspection and materialization profiles.
use crate::AuthenticationConfiguration;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Non-secret authentication and destinations, independently confirmed from the source.
/// The Provider domain stores the OAuth application secret separately from this projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfiguration {
    /// One method applies to every Tool. Absence still needs an explicit answer.
    pub authentication: Option<AuthenticationConfiguration>,
    /// Non-secret service headers, sent independently of account credentials.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_headers: BTreeMap<String, String>,
    /// How Add provider presents each Credential Field the method names. Presentation
    /// never affects which accounts remain compatible.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credential_fields: BTreeMap<String, CredentialField>,
    /// Main or declaration-scoped choices; equally named variables remain independent.
    pub servers: BTreeMap<String, ServerSelection>,
    /// The Provider's name, suggested from the source's own title and confirmed by the owner.
    pub display_name: String,
}

/// How the owner asks for one Credential Field; none of it reaches the Upstream Service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialField {
    /// Short name of the box a person fills in.
    pub label: String,
    /// Help text; empty for none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Masks the value while it is typed.
    pub sensitive: bool,
}

/// The longest display name a Provider accepts, in bytes.
pub const DISPLAY_NAME_MAX_BYTES: usize = 256;

/// Trims the confirmed name and rejects one that cannot name a Provider: blank,
/// control-bearing, or over the byte bound. One rule for every source kind.
/// # Errors
/// Returns the field error for `display_name`, without echoing the rejected value.
pub fn normalize_display_name(
    configuration: &mut ProviderConfiguration,
) -> Result<(), ConfigurationFieldError> {
    let trimmed = configuration.display_name.trim();
    if trimmed.is_empty()
        || trimmed.chars().any(char::is_control)
        || trimmed.len() > DISPLAY_NAME_MAX_BYTES
    {
        return Err(ConfigurationFieldError {
            field: "display_name".into(),
            message: "Enter a name of up to 256 characters without control characters.".into(),
        });
    }
    configuration.display_name = trimmed.to_string();
    Ok(())
}

/// The confirmed name once the owner has one, otherwise the source's own title.
pub(crate) fn configured_display_name(
    configuration: Option<&ProviderConfiguration>,
    source_title: impl FnOnce() -> Result<String, crate::CatalogDiagnostic>,
) -> Result<String, crate::CatalogDiagnostic> {
    match configuration.map(|configuration| configuration.display_name.as_str()) {
        Some(name) if !name.is_empty() => Ok(name.to_string()),
        _ => source_title(),
    }
}

/// One destination choice. Declared templates, not array indices, identify saved choices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerSelection {
    /// An OpenAPI server declaration with independently scoped variable answers.
    Declared {
        /// Exact declared template; changed templates require review, not index remapping.
        url: String,
        /// Values only for declared variables.
        variables: BTreeMap<String, String>,
    },
    /// An explicit absolute base URL; MCP validation fixes this to the inspected server.
    Custom {
        /// Absolute HTTP(S) base URL without credentials, query, or fragment.
        url: String,
    },
}

/// A safe server alternative and its declared variable fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerOption {
    /// Declared URL template, or inspected MCP server URL.
    pub url: String,
    /// Declared variables; empty for MCP and never ordinary Tool arguments.
    pub variables: BTreeMap<String, ServerVariable>,
}

/// A declared server variable, scoped to one server option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerVariable {
    /// Declared default, when valid text.
    pub default: Option<String>,
    /// Exact permitted values; absence permits typed values.
    pub enumeration: Option<Vec<String>>,
}

/// Tools sharing one destination declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerGroup {
    /// OpenAPI declaration pointer, or the main server scope for MCP.
    pub declaration: String,
    /// Tools affected by this declaration, excluding explicit overrides.
    pub tools: Vec<String>,
    /// Declared alternatives; an empty list needs owner review.
    pub options: Vec<ServerOption>,
}

/// Source suggestions are never an owner's configuration confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConfigurationReview {
    /// Values inferred only when the input or metadata is unambiguous.
    pub suggested: ProviderConfiguration,
    /// Safe classification of missing, explicit, supported or ambiguous authentication facts.
    pub authentication_status: String,
    /// Main server first, then independently configured declarations.
    pub servers: Vec<ServerGroup>,
}

impl ProviderConfigurationReview {
    /// Whether the source says nothing about credentials. That is not a
    /// statement that it needs none, so Coral suggests no method for it.
    #[must_use]
    pub fn silent_on_credentials(&self) -> bool {
        self.authentication_status == "missing"
    }
}

/// Bounded diagnostic that contains a field address but no rejected value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationFieldError {
    /// Address of the field to correct.
    pub field: String,
    /// Safe instruction, never an upstream or submitted message.
    pub message: String,
}

impl ProviderConfiguration {
    /// Validates service headers without allowing credential or transport overrides.
    /// # Errors
    /// Returns bounded field diagnostics without echoing submitted values.
    pub fn normalize_request_headers(
        &mut self,
        mcp: bool,
    ) -> Result<(), Vec<ConfigurationFieldError>> {
        normalize_request_headers(&mut self.request_headers, self.authentication.as_ref(), mcp)
    }
}

/// Validates and normalizes non-secret service headers for acquisition or execution.
/// # Errors
/// Returns bounded field diagnostics without echoing submitted values.
pub fn normalize_request_headers(
    headers: &mut BTreeMap<String, String>,
    authentication: Option<&AuthenticationConfiguration>,
    mcp: bool,
) -> Result<(), Vec<ConfigurationFieldError>> {
    let problem = |field: String, message: &str| ConfigurationFieldError {
        field,
        message: message.into(),
    };
    if headers.len() > 64 {
        return Err(vec![problem(
            "request_headers".into(),
            "Send at most 64 request headers.",
        )]);
    }
    let mut normalized = BTreeMap::new();
    let mut errors = Vec::new();
    for (name, value) in &*headers {
        let header = name.to_ascii_lowercase();
        let path = format!("request_headers.{name}");
        let reserved = matches!(
            header.as_str(),
            "authorization"
                | "connection"
                | "content-length"
                | "content-type"
                | "cookie"
                | "host"
                | "keep-alive"
                | "proxy-authorization"
                | "proxy-connection"
                | "set-cookie"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "user-agent"
                | "mcp-session-id"
                | "mcp-protocol-version"
                | "last-event-id"
        ) || (mcp && header == "accept");
        let credential = matches!(authentication, Some(AuthenticationConfiguration::HeaderCredentials { headers }) if headers.keys().any(|key| key.eq_ignore_ascii_case(&header)));
        if header.is_empty()
            || header.len() > 256
            || !header
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            || reserved
            || credential
        {
            errors.push(problem(
                path,
                "Use a header that Coral does not set for credentials or transport.",
            ));
        } else if value.is_empty()
            || value.len() > 2048
            || !value.bytes().all(|b| (b' '..=b'~').contains(&b))
        {
            errors.push(problem(
                path,
                "Enter up to 2048 unaccented letters, digits, spaces and symbols.",
            ));
        } else if normalized.insert(header, value.clone()).is_some() {
            errors.push(problem(path, "Send each request header once."));
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    *headers = normalized;
    Ok(())
}
