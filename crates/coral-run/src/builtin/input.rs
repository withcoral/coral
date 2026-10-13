//! The `createProvider` configuration as a Program writes it: the contract the
//! catalog schema documents, converted explicitly into Coral's configuration
//! types. Coral's stored configuration encoding never becomes the Program
//! contract, and no input carries a credential value.

use std::collections::BTreeMap;

use coral_catalog::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
    OAuthScopeFormat, ServerSelection,
};
use serde::Deserialize;

/// Input the Program could write but Coral must not accept, such as a value.
pub(super) struct Rejected;

/// One server choice per server declaration.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ServerInput {
    Declared {
        url: String,
        variables: BTreeMap<String, String>,
    },
    Custom {
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CredentialFieldInput {
    label: String,
    #[serde(default)]
    description: String,
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
#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum AuthenticationInput {
    None,
    Bearer,
    HeaderCredentials {
        headers: BTreeMap<String, HeaderInput>,
    },
    BasicAuth {
        username: SourceInput,
        password: SourceInput,
    },
    QueryCredentials {
        parameters: BTreeMap<String, SourceInput>,
    },
    OauthDeviceCode {
        device_authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        #[serde(default)]
        scope_format: ScopeFormatInput,
        client: Option<DeviceClientInput>,
    },
    OauthAuthorizationCode {
        authorization_url: String,
        token_url: String,
        scopes: Vec<String>,
        #[serde(default)]
        scope_format: ScopeFormatInput,
        client: Option<ClientInput>,
    },
}

impl TryFrom<AuthenticationInput> for AuthenticationConfiguration {
    type Error = Rejected;

    fn try_from(input: AuthenticationInput) -> Result<Self, Rejected> {
        Ok(match input {
            AuthenticationInput::None => Self::None,
            AuthenticationInput::Bearer => Self::Bearer,
            AuthenticationInput::HeaderCredentials { headers } => Self::HeaderCredentials {
                headers: headers
                    .into_iter()
                    .map(|(name, header)| (name, header.into()))
                    .collect(),
            },
            AuthenticationInput::BasicAuth { username, password } => Self::BasicAuth {
                username: username.try_into()?,
                password: password.try_into()?,
            },
            AuthenticationInput::QueryCredentials { parameters } => Self::QueryCredentials {
                parameters: parameters
                    .into_iter()
                    .map(|(name, source)| Ok((name, source.try_into()?)))
                    .collect::<Result<_, Rejected>>()?,
            },
            // Device authorization has no redirect for PKCE to protect.
            AuthenticationInput::OauthDeviceCode {
                device_authorization_url,
                token_url,
                scopes,
                scope_format,
                client,
            } => Self::OAuthDeviceCode {
                device_authorization_url,
                token_url,
                scopes,
                scope_format: scope_format.into(),
                resource: None,
                issuer: None,
                client: client.map(|client| {
                    ClientInput::from(client).policy(Some(OAuthPkcePolicy::Disabled))
                }),
            },
            AuthenticationInput::OauthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format,
                client,
            } => Self::OAuthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format: scope_format.into(),
                resource: None,
                issuer: None,
                client: client.map(|client| client.policy(None)),
            },
        })
    }
}

/// What one credential header sends: a Credential Field after an optional
/// prefix. A header never sends a literal value.
#[derive(Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum HeaderInput {
    Field {
        field: String,
        #[serde(default)]
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
#[derive(Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum SourceInput {
    Field { field: String },
    Literal { value: String },
}

impl TryFrom<SourceInput> for CredentialSource {
    type Error = Rejected;

    fn try_from(input: SourceInput) -> Result<Self, Rejected> {
        match input {
            SourceInput::Field { field } => Ok(Self::Field { field }),
            SourceInput::Literal { value } if value.is_empty() => Ok(Self::Literal { value }),
            // A person enters every value when they sign in.
            SourceInput::Literal { .. } => Err(Rejected),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClientInput {
    client_id: String,
    #[serde(default)]
    authentication: ClientAuthenticationInput,
    pkce: Option<PkceInput>,
    #[serde(default)]
    authorization_parameters: BTreeMap<String, String>,
}

impl ClientInput {
    /// The application policy; `fixed_pkce` overrides the Program's choice
    /// where the method decides it.
    fn policy(self, fixed_pkce: Option<OAuthPkcePolicy>) -> OAuthClientPolicy {
        OAuthClientPolicy {
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
        }
    }
}

/// A device-code application. Device authorization takes no authorization
/// parameters, so the Program cannot give any.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeviceClientInput {
    client_id: String,
    #[serde(default)]
    authentication: ClientAuthenticationInput,
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

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ClientAuthenticationInput {
    #[default]
    None,
    ClientSecretBasic,
    ClientSecretPost,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PkceInput {
    S256,
    Disabled,
}
