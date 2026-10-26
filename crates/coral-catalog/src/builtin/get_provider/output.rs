//! A Provider Configuration as `getProvider` returns it: the shapes the
//! `addProvider` input takes, and the facts Coral set itself, converted
//! explicitly from Coral's configuration types. Coral's stored configuration
//! encoding never becomes the Program contract, and no output carries a fixed
//! credential value.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
    OAuthScopeFormat, ServerSelection,
};

/// What a credential part or header names.
const FIELD_DESCRIPTION: &str = "A field from credentialFields.";
/// An MCP server's authorization server, which only discovery sets.
const ISSUER_DESCRIPTION: &str = "The MCP server's authorization server, which Coral discovered. \
     addProvider does not take it.";
/// An MCP server's resource indicator, which only discovery sets.
const RESOURCE_DESCRIPTION: &str = "The MCP server's resource indicator, which Coral discovered. \
     addProvider does not take it.";

/// Where Coral last read a Provider from. An uploaded document reads without
/// its content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schemars(
    rename = "PublishedProviderSource",
    deny_unknown_fields,
    description = "Where Coral last read the Provider from."
)]
pub enum ProviderSourceOutput {
    /// An uploaded OpenAPI document.
    #[schemars(description = "An uploaded OpenAPI document. Coral does not return the document.")]
    OpenapiDocument,
    /// An OpenAPI document's web URL.
    #[schemars(description = "")]
    OpenapiUrl {
        /// The URL.
        #[schemars(
            description = "The URL of the OpenAPI document. A query parameter whose \
                                  name looks secret, such as api_key, reads [redacted]."
        )]
        url: String,
    },
    /// A Streamable HTTP MCP server's web URL.
    #[schemars(description = "")]
    McpUrl {
        /// The URL.
        #[schemars(
            description = "The URL of the Streamable HTTP MCP server. A query parameter \
                                  whose name looks secret, such as api_key, reads [redacted]."
        )]
        url: String,
    },
}

impl ProviderSourceOutput {
    /// An OpenAPI document's saved web URL, as a Program reads it, or `None`
    /// when the saved value is not a URL.
    #[must_use]
    pub fn openapi_url(saved: &str) -> Option<Self> {
        let url = shown(url::Url::parse(saved).ok()?);
        Some(Self::OpenapiUrl { url })
    }

    /// A Streamable HTTP MCP server's saved web URL, as a Program reads it,
    /// or `None` when the saved value is not a URL.
    #[must_use]
    pub fn mcp_url(saved: &str) -> Option<Self> {
        let url = shown(url::Url::parse(saved).ok()?);
        Some(Self::McpUrl { url })
    }
}

/// A source URL as Reef shows it, since Run evidence stores the result: no
/// embedded credentials, no fragment, and `[redacted]` for the value of each
/// query parameter whose name looks secret. A source URL can carry a key in
/// its query. Reef's `source-url.ts` hides the same names.
fn shown(mut url: url::Url) -> String {
    // The URL has a host, so it takes credentials.
    let _no_username = url.set_username("");
    let _no_password = url.set_password(None);
    url.set_fragment(None);
    let query = url.query().map(|query| {
        query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((name, _value)) if secret_parameter(name) => format!("{name}=[redacted]"),
                _ => pair.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("&")
    });
    url.set_query(query.as_deref());
    url.into()
}

/// Whether a query parameter's name, as the URL encodes it, looks secret.
/// Matching parts of names errs toward hiding: `author` hides too.
fn secret_parameter(encoded: &str) -> bool {
    let name = url::form_urlencoded::parse(encoded.as_bytes())
        .next()
        .map(|(name, _value)| name.to_ascii_lowercase())
        .unwrap_or_default();
    name == "code"
        || [
            "key",
            "token",
            "secret",
            "pass",
            "pwd",
            "auth",
            "sig",
            "session",
            "credential",
            "cookie",
            "jwt",
            "bearer",
        ]
        .iter()
        .any(|part| name.contains(part))
}

/// One server choice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schemars(
    rename = "ConfiguredServerSelection",
    deny_unknown_fields,
    description = ""
)]
pub(super) enum ServerOutput {
    Declared {
        #[schemars(description = "A server URL template the source declares.")]
        url: String,
        variables: BTreeMap<String, String>,
    },
    Custom {
        #[schemars(description = "An absolute HTTP or HTTPS base URL.")]
        url: String,
    },
}

impl From<ServerSelection> for ServerOutput {
    fn from(server: ServerSelection) -> Self {
        match server {
            ServerSelection::Declared { url, variables } => Self::Declared { url, variables },
            ServerSelection::Custom { url } => Self::Custom { url },
        }
    }
}

/// How a person is asked for one value when they sign in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(
    rename = "ConfiguredCredentialField",
    deny_unknown_fields,
    description = ""
)]
pub(super) struct CredentialFieldOutput {
    #[schemars(description = "The name of the box the person fills in.")]
    label: String,
    #[schemars(description = "Help text; empty for none.")]
    description: String,
    #[schemars(description = "Masks the value while it is typed.")]
    sensitive: bool,
}

impl From<CredentialField> for CredentialFieldOutput {
    fn from(field: CredentialField) -> Self {
        Self {
            label: field.label,
            description: field.description,
            sensitive: field.sensitive,
        }
    }
}

/// The authentication method and its non-secret settings. MCP issuer and
/// resource come from discovery, so a Program reads them but never writes
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
#[schemars(
    rename = "ConfiguredAuthentication",
    deny_unknown_fields,
    description = "The authentication method and its non-secret settings. A header, query \
                   parameter or Basic part names a field from credentialFields, or is a literal."
)]
pub(super) enum AuthenticationOutput {
    None,
    Bearer,
    HeaderCredentials {
        #[schemars(description = "Header name to the field it sends.")]
        headers: BTreeMap<String, HeaderOutput>,
    },
    BasicAuth {
        username: CredentialPartOutput,
        password: CredentialPartOutput,
    },
    QueryCredentials {
        #[schemars(description = "Query parameter name to the field it sends.")]
        parameters: BTreeMap<String, CredentialPartOutput>,
    },
    OauthDeviceCode {
        device_authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        scope_format: ScopeFormatOutput,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "String", description = ISSUER_DESCRIPTION)]
        issuer: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "String", description = RESOURCE_DESCRIPTION)]
        resource: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "DeviceClientOutput")]
        client: Option<DeviceClientOutput>,
    },
    OauthAuthorizationCode {
        authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        scope_format: ScopeFormatOutput,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "String", description = ISSUER_DESCRIPTION)]
        issuer: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "String", description = RESOURCE_DESCRIPTION)]
        resource: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "ClientOutput")]
        client: Option<ClientOutput>,
    },
}

impl From<AuthenticationConfiguration> for AuthenticationOutput {
    fn from(authentication: AuthenticationConfiguration) -> Self {
        match authentication {
            AuthenticationConfiguration::None => Self::None,
            AuthenticationConfiguration::Bearer => Self::Bearer,
            AuthenticationConfiguration::HeaderCredentials { headers } => Self::HeaderCredentials {
                headers: headers
                    .into_iter()
                    .map(|(name, header)| (name, header.into()))
                    .collect(),
            },
            AuthenticationConfiguration::BasicAuth { username, password } => Self::BasicAuth {
                username: username.into(),
                password: password.into(),
            },
            AuthenticationConfiguration::QueryCredentials { parameters } => {
                Self::QueryCredentials {
                    parameters: parameters
                        .into_iter()
                        .map(|(name, source)| (name, source.into()))
                        .collect(),
                }
            }
            AuthenticationConfiguration::OAuthDeviceCode {
                device_authorization_url,
                token_url,
                scopes,
                scope_format,
                resource,
                issuer,
                client,
            } => Self::OauthDeviceCode {
                device_authorization_url,
                token_url,
                scopes,
                scope_format: scope_format.into(),
                issuer,
                resource,
                client: client.map(Into::into),
            },
            AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format,
                resource,
                issuer,
                client,
            } => Self::OauthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format: scope_format.into(),
                issuer,
                resource,
                client: client.map(Into::into),
            },
        }
    }
}

/// What one credential header sends: a Credential Field after a prefix, or
/// a fixed value that Coral does not return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case")]
#[schemars(
    rename = "ConfiguredHeaderField",
    deny_unknown_fields,
    description = ""
)]
pub(super) enum HeaderOutput {
    Field {
        #[schemars(description = FIELD_DESCRIPTION)]
        field: String,
        #[schemars(description = "Non-secret text sent before the value; empty for none.")]
        prefix: String,
    },
    #[schemars(description = "A fixed value. Coral does not return it.")]
    Literal,
}

impl From<HeaderBinding> for HeaderOutput {
    fn from(header: HeaderBinding) -> Self {
        match header {
            HeaderBinding::Field { field, prefix } => Self::Field { field, prefix },
            HeaderBinding::Literal { .. } => Self::Literal,
        }
    }
}

/// Where one credential part comes from: a Credential Field, or a literal.
/// Only an empty literal reads with its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case")]
#[schemars(
    rename = "ConfiguredCredentialPart",
    deny_unknown_fields,
    description = ""
)]
pub(super) enum CredentialPartOutput {
    Field {
        #[schemars(description = FIELD_DESCRIPTION)]
        field: String,
    },
    #[schemars(
        description = "With an empty value, a part the service leaves blank, such as a Basic \
                       password. Without a value, a fixed value that Coral does not return."
    )]
    Literal {
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(with = "String", extend("const" = ""))]
        value: Option<String>,
    },
}

impl From<CredentialSource> for CredentialPartOutput {
    fn from(source: CredentialSource) -> Self {
        match source {
            CredentialSource::Field { field } => Self::Field { field },
            CredentialSource::Literal { value } => Self::Literal {
                value: value.is_empty().then_some(value),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(
    rename = "ConfiguredScopeFormat",
    description = "How scopes are joined in the request."
)]
pub(super) enum ScopeFormatOutput {
    Space,
    Comma,
}

impl From<OAuthScopeFormat> for ScopeFormatOutput {
    fn from(format: OAuthScopeFormat) -> Self {
        match format {
            OAuthScopeFormat::Space => Self::Space,
            OAuthScopeFormat::Comma => Self::Comma,
        }
    }
}

/// An OAuth application registered with the service, by a person or by
/// Coral. Its secret is stored apart and never returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(
    rename = "ConfiguredOAuthClient",
    deny_unknown_fields,
    description = "The OAuth application registered with the service. Coral never returns its \
                   secret."
)]
pub(super) struct ClientOutput {
    client_id: String,
    #[schemars(description = "How the application authenticates at the token endpoint.")]
    authentication: ClientAuthenticationOutput,
    #[schemars(description = "Authorization-code protection.")]
    pkce: PkceOutput,
    #[schemars(description = "Non-secret authorization options, such as access_type.")]
    authorization_parameters: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "RegistrationOutput",
        description = "How Coral registered the application itself. Absent when a person \
                       registered it. addProvider does not take it: omit client to let Coral \
                       register one."
    )]
    registration: Option<RegistrationOutput>,
}

impl From<OAuthClientPolicy> for ClientOutput {
    fn from(client: OAuthClientPolicy) -> Self {
        Self {
            client_id: client.client_id,
            authentication: client.authentication.into(),
            pkce: match client.pkce {
                OAuthPkcePolicy::S256 => PkceOutput::S256,
                OAuthPkcePolicy::Disabled => PkceOutput::Disabled,
            },
            authorization_parameters: client.authorization_parameters,
            registration: match client.bootstrap {
                OAuthClientBootstrap::Manual => None,
                OAuthClientBootstrap::DynamicClientRegistration => {
                    Some(RegistrationOutput::DynamicClientRegistration)
                }
                OAuthClientBootstrap::ClientIdMetadataDocument => {
                    Some(RegistrationOutput::ClientIdMetadataDocument)
                }
            },
        }
    }
}

/// How Coral registered an application itself. A person registers every
/// device-code application, so only an authorization-code one has this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub(super) enum RegistrationOutput {
    DynamicClientRegistration,
    ClientIdMetadataDocument,
}

/// A device-code application. Device authorization uses no PKCE and takes
/// no authorization parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(
    rename = "ConfiguredDeviceOAuthClient",
    deny_unknown_fields,
    description = "The OAuth application registered with the service. Coral never returns its \
                   secret."
)]
pub(super) struct DeviceClientOutput {
    client_id: String,
    #[schemars(description = "How the application authenticates at the token endpoint.")]
    authentication: ClientAuthenticationOutput,
}

impl From<OAuthClientPolicy> for DeviceClientOutput {
    fn from(client: OAuthClientPolicy) -> Self {
        Self {
            client_id: client.client_id,
            authentication: client.authentication.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub(super) enum ClientAuthenticationOutput {
    None,
    ClientSecretBasic,
    ClientSecretPost,
}

impl From<OAuthClientAuthenticationMethod> for ClientAuthenticationOutput {
    fn from(method: OAuthClientAuthenticationMethod) -> Self {
        match method {
            OAuthClientAuthenticationMethod::None => Self::None,
            OAuthClientAuthenticationMethod::ClientSecretBasic => Self::ClientSecretBasic,
            OAuthClientAuthenticationMethod::ClientSecretPost => Self::ClientSecretPost,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub(super) enum PkceOutput {
    S256,
    Disabled,
}
