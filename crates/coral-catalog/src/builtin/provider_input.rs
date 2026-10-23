//! A Provider's source and configuration as a Program writes them for
//! `addProvider` and `updateProvider`: the contract the catalog schema
//! documents, converted explicitly into Coral's configuration types. Coral's
//! stored configuration encoding never becomes the Program contract, and no
//! input carries a credential value.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;

use crate::builtin::ArgumentProblem;
use crate::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
    OAuthScopeFormat, OpenApiDocument, ProviderConfiguration, ServerSelection,
};

#[cfg(test)]
mod tests;

/// Where Coral reads a Provider from.
#[derive(Debug, Clone)]
pub enum ProviderSourceArgument {
    /// A complete OpenAPI document.
    OpenApiDocument(OpenApiDocument),
    /// An OpenAPI document's web URL.
    OpenApiUrl(String),
    /// A Streamable HTTP MCP server's web URL.
    McpUrl(String),
}

/// The configuration fields the Program chose, each over the source's
/// suggestion.
#[derive(Debug, Clone)]
pub struct ConfigurationChoices {
    /// The Provider display name.
    pub display_name: Option<String>,
    /// A server per server declaration.
    pub servers: Option<BTreeMap<String, ServerSelection>>,
    /// The authentication method and its non-secret settings.
    pub authentication: Option<AuthenticationConfiguration>,
    /// Non-secret headers sent with every request.
    pub request_headers: Option<BTreeMap<String, String>>,
    /// The values a person enters when they sign in.
    pub credential_fields: Option<BTreeMap<String, CredentialField>>,
}

impl ConfigurationChoices {
    /// Whether the Program chose no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.display_name.is_none()
            && self.servers.is_none()
            && self.authentication.is_none()
            && self.request_headers.is_none()
            && self.credential_fields.is_none()
    }

    /// The configuration to confirm: each chosen field over `suggested`.
    #[must_use]
    pub fn over(&self, suggested: ProviderConfiguration) -> ProviderConfiguration {
        ProviderConfiguration {
            display_name: self.display_name.clone().unwrap_or(suggested.display_name),
            servers: self.servers.clone().unwrap_or(suggested.servers),
            authentication: self.authentication.clone().or(suggested.authentication),
            request_headers: self
                .request_headers
                .clone()
                .unwrap_or(suggested.request_headers),
            credential_fields: self
                .credential_fields
                .clone()
                .unwrap_or(suggested.credential_fields),
        }
    }

    /// The argument a Program corrects for a configuration field that Coral
    /// reports when it reads or confirms the configuration, such as
    /// `requestHeaders.Host` for `request_headers.Host`.
    #[must_use]
    pub fn argument_for(configuration_field: &str) -> String {
        // A server declaration is a JSON pointer that keys `servers`.
        if configuration_field.starts_with("#/") {
            return format!("servers.{configuration_field}");
        }
        for (stored, argument) in [
            ("display_name", "displayName"),
            ("request_headers", "requestHeaders"),
            ("credential_fields", "credentialFields"),
            ("authentication.fields", "credentialFields"),
            ("application", "authentication.client"),
            ("server", "servers"),
        ] {
            if configuration_field == stored {
                return argument.to_owned();
            }
            if let Some(rest) = configuration_field
                .strip_prefix(stored)
                .and_then(|rest| rest.strip_prefix('.'))
            {
                return format!("{argument}.{rest}");
            }
        }
        configuration_field.to_owned()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(rename = "ProviderSource")]
pub(super) enum ProviderSourceInput {
    OpenapiDocument {
        #[schemars(description = "A complete OpenAPI document, as JSON or YAML text.")]
        document: String,
    },
    OpenapiUrl {
        #[schemars(description = "The URL of an OpenAPI document.")]
        url: String,
    },
    McpUrl {
        #[schemars(description = "The URL of a Streamable HTTP MCP server.")]
        url: String,
    },
}

impl ProviderSourceInput {
    pub(super) fn into_source(self) -> Result<ProviderSourceArgument, ArgumentProblem> {
        match self {
            Self::OpenapiDocument { document } => OpenApiDocument::parse(&document)
                .map(ProviderSourceArgument::OpenApiDocument)
                .map_err(|_error| {
                    ArgumentProblem::new(
                        "source.document",
                        "Coral cannot read this as an OpenAPI document",
                    )
                }),
            Self::OpenapiUrl { url } => web_url(url).map(ProviderSourceArgument::OpenApiUrl),
            Self::McpUrl { url } => web_url(url).map(ProviderSourceArgument::McpUrl),
        }
    }
}

/// A source URL must be an http or https URL with a host, and no credentials
/// or fragment, as the Setup requires. Whether Coral may fetch it is the
/// destination policy's to decide, when the Setup is prepared.
fn web_url(url: String) -> Result<String, ArgumentProblem> {
    let problem = || {
        ArgumentProblem::new(
            "source.url",
            "use an absolute `http` or `https` URL without credentials or a fragment",
        )
    };
    let parsed = url::Url::parse(&url).map_err(|_error| problem())?;
    if matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.fragment().is_none()
    {
        Ok(url)
    } else {
        Err(problem())
    }
}

/// What a credential part or header names.
const FIELD_DESCRIPTION: &str = "A field from credentialFields.";
/// How an OAuth application authenticates, for either method.
const CLIENT_AUTHENTICATION_DESCRIPTION: &str = "How the application authenticates at the token \
     endpoint. Omit it, or give none, for a public application.";

/// What every OAuth endpoint must be, so it cannot carry a credential.
const ENDPOINT_REASON: &str = "use an absolute `http` or `https` URL without credentials, a \
     fragment or an OAuth parameter that Coral sets, such as `client_secret`";

/// The configuration fields a Program sent, in Coral's configuration types.
/// A problem with `authentication` joins `problems`, so the Program learns it
/// beside every other.
pub(super) fn choices(
    display_name: Option<String>,
    servers: Option<BTreeMap<String, ServerInput>>,
    authentication: Option<AuthenticationInput>,
    request_headers: Option<BTreeMap<String, String>>,
    credential_fields: Option<BTreeMap<String, CredentialFieldInput>>,
    problems: &mut Vec<ArgumentProblem>,
) -> ConfigurationChoices {
    ConfigurationChoices {
        display_name,
        servers: servers.map(|servers| {
            servers
                .into_iter()
                .map(|(declaration, server)| (declaration, server.into()))
                .collect()
        }),
        authentication: authentication
            .map(AuthenticationConfiguration::try_from)
            .transpose()
            .map_err(|problem| problems.push(problem))
            .ok()
            .flatten(),
        request_headers,
        credential_fields: credential_fields.map(|fields| {
            fields
                .into_iter()
                .map(|(name, field)| (name, field.into()))
                .collect()
        }),
    }
}

/// One server choice per server declaration.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(rename = "ServerSelection", description = "")]
pub(super) enum ServerInput {
    Declared {
        #[schemars(description = "A server URL template the source declares.")]
        url: String,
        variables: BTreeMap<String, String>,
    },
    Custom {
        #[schemars(
            description = "An absolute HTTP or HTTPS base URL, without credentials, a query, or a \
                           fragment."
        )]
        url: String,
    },
}

impl From<ServerInput> for ServerSelection {
    fn from(input: ServerInput) -> Self {
        match input {
            ServerInput::Declared { url, variables } => Self::Declared { url, variables },
            ServerInput::Custom { url } => Self::Custom { url },
        }
    }
}

/// How a person is asked for one value when they sign in.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "CredentialField", description = "")]
pub(super) struct CredentialFieldInput {
    #[schemars(description = "The name of the box the person fills in.")]
    label: String,
    #[serde(default)]
    #[schemars(description = "Help text.")]
    description: String,
    #[schemars(description = "Masks the value while it is typed.")]
    sensitive: bool,
}

impl From<CredentialFieldInput> for CredentialField {
    fn from(input: CredentialFieldInput) -> Self {
        Self {
            label: input.label,
            description: input.description,
            sensitive: input.sensitive,
        }
    }
}

/// The authentication method and its non-secret settings. MCP issuer and
/// resource come from discovery, as in Reef, so a Program never sets them.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(
    rename = "Authentication",
    description = "The authentication method and its non-secret settings. Omit it to use the \
                   method the source declares. Never include a secret or a literal value: a \
                   header, query parameter or Basic part names a field from credentialFields. \
                   The one literal is empty, for a Basic or query part the service leaves blank."
)]
pub(super) enum AuthenticationInput {
    // Empty struct variants, not unit ones: serde ignores `deny_unknown_fields`
    // on a unit variant of an internally tagged enum, so `{ method: "bearer",
    // token }` would parse and its token would be stored with the arguments.
    None {},
    Bearer {},
    HeaderCredentials {
        #[schemars(description = "Header name to the field it sends.")]
        headers: BTreeMap<String, HeaderInput>,
    },
    BasicAuth {
        username: SourceInput,
        password: SourceInput,
    },
    QueryCredentials {
        #[schemars(description = "Query parameter name to the field it sends.")]
        parameters: BTreeMap<String, SourceInput>,
    },
    OauthDeviceCode {
        device_authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        #[serde(default)]
        scope_format: ScopeFormatInput,
        #[serde(default, deserialize_with = "crate::builtin::present")]
        #[schemars(with = "DeviceClientInput")]
        client: Option<DeviceClientInput>,
    },
    OauthAuthorizationCode {
        authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        #[serde(default)]
        scope_format: ScopeFormatInput,
        #[serde(default, deserialize_with = "crate::builtin::present")]
        #[schemars(with = "ClientInput")]
        client: Option<ClientInput>,
    },
}

impl TryFrom<AuthenticationInput> for AuthenticationConfiguration {
    type Error = ArgumentProblem;

    /// Converts the input, or names the first argument to correct.
    fn try_from(input: AuthenticationInput) -> Result<Self, ArgumentProblem> {
        Ok(match input {
            AuthenticationInput::None {} => Self::None,
            AuthenticationInput::Bearer {} => Self::Bearer,
            AuthenticationInput::HeaderCredentials { headers } => Self::HeaderCredentials {
                headers: headers
                    .into_iter()
                    .map(|(name, header)| (name, header.into()))
                    .collect(),
            },
            AuthenticationInput::BasicAuth { username, password } => Self::BasicAuth {
                username: credential_source("authentication.username", username)?,
                password: credential_source("authentication.password", password)?,
            },
            AuthenticationInput::QueryCredentials { parameters } => Self::QueryCredentials {
                parameters: parameters
                    .into_iter()
                    .map(|(name, source)| {
                        let field = format!("authentication.parameters.{name}");
                        Ok((name, credential_source(field, source)?))
                    })
                    .collect::<Result<_, ArgumentProblem>>()?,
            },
            // Device authorization has no redirect for PKCE to protect.
            AuthenticationInput::OauthDeviceCode {
                device_authorization_url,
                token_url,
                scopes,
                scope_format,
                client,
            } => Self::OAuthDeviceCode {
                device_authorization_url: endpoint(
                    "device_authorization_url",
                    device_authorization_url,
                )?,
                token_url: endpoint("token_url", token_url)?,
                scopes,
                scope_format: scope_format.into(),
                resource: None,
                issuer: None,
                client: client
                    .map(|client| ClientInput::from(client).policy(Some(OAuthPkcePolicy::Disabled)))
                    .transpose()?,
            },
            AuthenticationInput::OauthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format,
                client,
            } => Self::OAuthAuthorizationCode {
                authorization_url: endpoint("authorization_url", authorization_url)?,
                token_url: endpoint("token_url", token_url)?,
                scopes,
                scope_format: scope_format.into(),
                resource: None,
                issuer: None,
                client: client.map(|client| client.policy(None)).transpose()?,
            },
        })
    }
}

/// The OAuth endpoint at `authentication.{field}`, rejected before the
/// arguments are stored when it could carry a credential, such as
/// `user:secret@` or a `client_secret` parameter.
fn endpoint(field: &str, url: String) -> Result<String, ArgumentProblem> {
    if crate::oauth_endpoint_is_valid(&url) {
        Ok(url)
    } else {
        Err(ArgumentProblem::new(
            format!("authentication.{field}"),
            ENDPOINT_REASON,
        ))
    }
}

/// What one credential header sends: a Credential Field after an optional
/// prefix. A header never sends a literal value.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(rename = "HeaderField", description = "")]
pub(super) enum HeaderInput {
    Field {
        #[schemars(description = FIELD_DESCRIPTION)]
        field: String,
        #[serde(default)]
        #[schemars(
            description = "Non-secret text sent before the value, such as Bot . Never a credential."
        )]
        prefix: String,
    },
}

impl From<HeaderInput> for HeaderBinding {
    fn from(input: HeaderInput) -> Self {
        let HeaderInput::Field { field, prefix } = input;
        Self::Field { field, prefix }
    }
}

/// Where one credential part comes from: a Credential Field, or an empty
/// literal for a part the service leaves blank, such as a Basic password.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(rename = "CredentialPart", description = "")]
pub(super) enum SourceInput {
    Field {
        #[schemars(description = FIELD_DESCRIPTION)]
        field: String,
    },
    #[schemars(description = "A part the service leaves blank, such as a Basic password.")]
    Literal {
        #[schemars(extend("const" = ""))]
        value: String,
    },
}

/// The credential part at `field`. A person enters every value when they
/// sign in, so a literal must be empty.
fn credential_source(
    field: impl Into<String>,
    input: SourceInput,
) -> Result<CredentialSource, ArgumentProblem> {
    match input {
        SourceInput::Field { field } => Ok(CredentialSource::Field { field }),
        SourceInput::Literal { value } if value.is_empty() => {
            Ok(CredentialSource::Literal { value })
        }
        SourceInput::Literal { .. } => Err(ArgumentProblem::new(
            field,
            "a literal credential value must be empty; use a `field` source so a person \
             enters the value when they sign in",
        )),
    }
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(
    rename = "ScopeFormat",
    description = "How scopes are joined in the request. Omit it for space."
)]
pub(super) enum ScopeFormatInput {
    #[default]
    Space,
    Comma,
}

impl From<ScopeFormatInput> for OAuthScopeFormat {
    fn from(input: ScopeFormatInput) -> Self {
        match input {
            ScopeFormatInput::Space => Self::Space,
            ScopeFormatInput::Comma => Self::Comma,
        }
    }
}

/// An OAuth application registered with the service. A confidential
/// application's secret never comes from the Program: the person enters it
/// when they sign in.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    rename = "OAuthClient",
    description = "An OAuth application registered with the service. Omit it to let Coral \
                   register one, when the service allows it. A confidential application never \
                   takes its secret here: the person enters it when they sign in."
)]
pub(super) struct ClientInput {
    client_id: String,
    #[serde(default)]
    #[schemars(description = CLIENT_AUTHENTICATION_DESCRIPTION)]
    authentication: ClientAuthenticationInput,
    #[serde(default, deserialize_with = "crate::builtin::present")]
    #[schemars(
        with = "PkceInput",
        description = "Authorization-code protection. Omit it for s256."
    )]
    pkce: Option<PkceInput>,
    #[serde(default)]
    #[schemars(description = "Non-secret authorization options, such as access_type.")]
    authorization_parameters: BTreeMap<String, String>,
}

impl ClientInput {
    /// The application policy; `fixed_pkce` overrides the Program's choice
    /// where the method decides it. A reserved parameter, such as
    /// `client_secret`, is rejected before the arguments are stored.
    fn policy(
        self,
        fixed_pkce: Option<OAuthPkcePolicy>,
    ) -> Result<OAuthClientPolicy, ArgumentProblem> {
        if let Some(name) = self
            .authorization_parameters
            .keys()
            .find(|name| crate::oauth_parameter_is_reserved(name))
        {
            return Err(ArgumentProblem::new(
                format!("authentication.client.authorization_parameters.{name}"),
                "Coral sets this OAuth parameter, or a person enters it when they sign in; \
                 remove it",
            ));
        }
        Ok(OAuthClientPolicy {
            bootstrap: OAuthClientBootstrap::Manual,
            client_id: self.client_id,
            authentication: match self.authentication {
                ClientAuthenticationInput::None => OAuthClientAuthenticationMethod::None,
                ClientAuthenticationInput::ClientSecretBasic => {
                    OAuthClientAuthenticationMethod::ClientSecretBasic
                }
                ClientAuthenticationInput::ClientSecretPost => {
                    OAuthClientAuthenticationMethod::ClientSecretPost
                }
            },
            pkce: fixed_pkce.unwrap_or(match self.pkce {
                None => OAuthPkcePolicy::default(),
                Some(PkceInput::S256) => OAuthPkcePolicy::S256,
                Some(PkceInput::Disabled) => OAuthPkcePolicy::Disabled,
            }),
            authorization_parameters: self.authorization_parameters,
        })
    }
}

/// A device-code application. Device authorization takes no authorization
/// parameters, so the Program cannot give any.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    rename = "DeviceOAuthClient",
    description = "An OAuth application registered with the service. A confidential application \
                   never takes its secret here: the person enters it when they sign in. Device \
                   authorization takes no authorization parameters."
)]
pub(super) struct DeviceClientInput {
    client_id: String,
    #[serde(default)]
    #[schemars(description = CLIENT_AUTHENTICATION_DESCRIPTION)]
    authentication: ClientAuthenticationInput,
    #[serde(default, deserialize_with = "crate::builtin::present")]
    #[schemars(with = "PkceInput", description = "Device code never uses it.")]
    pkce: Option<PkceInput>,
}

impl From<DeviceClientInput> for ClientInput {
    fn from(input: DeviceClientInput) -> Self {
        Self {
            client_id: input.client_id,
            authentication: input.authentication,
            pkce: input.pkce,
            authorization_parameters: BTreeMap::new(),
        }
    }
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub(super) enum ClientAuthenticationInput {
    #[default]
    None,
    ClientSecretBasic,
    ClientSecretPost,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub(super) enum PkceInput {
    S256,
    Disabled,
}
