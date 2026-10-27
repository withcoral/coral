//! Thin private-wire projections of domain-owned configuration review.
use super::wire;
use coral_catalog::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
};
use coral_catalog::{ProviderConfiguration, ServerSelection};
use coral_provider::OAuthClientAuthentication;
use tonic::Status;
use wire::provider_configuration::Authentication as WireAuthentication;

pub(super) fn input(value: wire::ProviderConfiguration) -> Result<ProviderConfiguration, Status> {
    let authentication = input_authentication(value.authentication)?;
    let mut servers = std::collections::BTreeMap::new();
    for server in value.servers {
        let selection = if server.custom {
            ServerSelection::Custom { url: server.url }
        } else {
            ServerSelection::Declared {
                url: server.url,
                variables: server.variables.into_iter().collect(),
            }
        };
        if servers.insert(server.declaration, selection).is_some() {
            return Err(Status::invalid_argument("duplicate server declaration"));
        }
    }
    Ok(ProviderConfiguration {
        request_headers: value.request_headers.into_iter().collect(),
        authentication,
        credential_fields: value
            .credential_fields
            .into_iter()
            .map(|(name, field)| {
                (
                    name,
                    CredentialField {
                        label: field.label,
                        description: field.description,
                        sensitive: field.sensitive,
                    },
                )
            })
            .collect(),
        servers,
        display_name: value.display_name,
    })
}

fn input_authentication(
    value: Option<WireAuthentication>,
) -> Result<Option<AuthenticationConfiguration>, Status> {
    // A case this server does not know arrives unset, so the owner is asked to choose again.
    value
        .map(|method| -> Result<_, Status> {
            Ok(match method {
                WireAuthentication::None(()) => AuthenticationConfiguration::None,
                WireAuthentication::Bearer(()) => AuthenticationConfiguration::Bearer,
                WireAuthentication::HeaderCredentials(settings) => {
                    AuthenticationConfiguration::HeaderCredentials {
                        headers: settings
                            .headers
                            .into_iter()
                            .map(|(header, binding)| {
                                let source = binding.source.ok_or_else(|| {
                                    Status::invalid_argument("header binding source is required")
                                })?;
                                let binding = match source {
                                    wire::header_binding::Source::Field(value) => {
                                        HeaderBinding::Field {
                                            field: value.field,
                                            prefix: value.prefix,
                                        }
                                    }
                                    wire::header_binding::Source::Literal(value) => {
                                        HeaderBinding::Literal { value }
                                    }
                                };
                                Ok((header, binding))
                            })
                            .collect::<Result<_, Status>>()?,
                    }
                }
                WireAuthentication::QueryCredentials(settings) => {
                    AuthenticationConfiguration::QueryCredentials {
                        parameters: settings
                            .parameters
                            .into_iter()
                            .map(|(name, source)| {
                                let value = credential_source(Some(source), &name)?;
                                Ok((name, value))
                            })
                            .collect::<Result<_, Status>>()?,
                    }
                }
                WireAuthentication::BasicAuth(settings) => AuthenticationConfiguration::BasicAuth {
                    username: credential_source(settings.username, "username")?,
                    password: credential_source(settings.password, "password")?,
                },
                WireAuthentication::OauthDeviceCode(settings) => {
                    AuthenticationConfiguration::OAuthDeviceCode {
                        device_authorization_url: settings.device_authorization_url,
                        resource: None,
                        issuer: None,
                        token_url: settings.token_url,
                        scopes: settings.scopes,
                        scope_format: scope_format(settings.scope_format)?,
                        client: None,
                    }
                }
                WireAuthentication::OauthAuthorizationCode(settings) => {
                    AuthenticationConfiguration::OAuthAuthorizationCode {
                        authorization_url: settings.authorization_url,
                        token_url: settings.token_url,
                        scopes: settings.scopes,
                        scope_format: scope_format(settings.scope_format)?,
                        resource: None,
                        issuer: None,
                        client: None,
                    }
                }
            })
        })
        .transpose()
}

/// One credential source. `part` names it, so the caller learns which one is missing.
fn credential_source(
    value: Option<wire::CredentialSource>,
    part: &str,
) -> Result<CredentialSource, Status> {
    match value.and_then(|value| value.source) {
        Some(wire::credential_source::Source::Field(field)) => {
            Ok(CredentialSource::Field { field })
        }
        Some(wire::credential_source::Source::Literal(value)) => {
            Ok(CredentialSource::Literal { value })
        }
        None => Err(Status::invalid_argument(format!(
            "the {part} must send a credential field or a value"
        ))),
    }
}

fn wire_credential_source(value: CredentialSource) -> wire::CredentialSource {
    wire::CredentialSource {
        source: Some(match value {
            CredentialSource::Field { field } => wire::credential_source::Source::Field(field),
            CredentialSource::Literal { value } => wire::credential_source::Source::Literal(value),
        }),
    }
}

/// Every domain method has a wire case; a new method fails to compile until it has one.
fn authentication(contract: AuthenticationConfiguration) -> WireAuthentication {
    match contract {
        AuthenticationConfiguration::None => WireAuthentication::None(()),
        AuthenticationConfiguration::Bearer => WireAuthentication::Bearer(()),
        AuthenticationConfiguration::HeaderCredentials { headers } => {
            WireAuthentication::HeaderCredentials(wire::HeaderCredentialsAuthentication {
                headers: headers
                    .into_iter()
                    .map(|(header, binding)| {
                        let source = match binding {
                            HeaderBinding::Field { field, prefix } => {
                                wire::header_binding::Source::Field(wire::FieldHeaderValue {
                                    field,
                                    prefix,
                                })
                            }
                            HeaderBinding::Literal { value } => {
                                wire::header_binding::Source::Literal(value)
                            }
                        };
                        (
                            header,
                            wire::HeaderBinding {
                                source: Some(source),
                            },
                        )
                    })
                    .collect(),
            })
        }
        AuthenticationConfiguration::QueryCredentials { parameters } => {
            WireAuthentication::QueryCredentials(wire::QueryCredentialsAuthentication {
                parameters: parameters
                    .into_iter()
                    .map(|(name, source)| (name, wire_credential_source(source)))
                    .collect(),
            })
        }
        AuthenticationConfiguration::BasicAuth { username, password } => {
            WireAuthentication::BasicAuth(wire::BasicAuthentication {
                username: Some(wire_credential_source(username)),
                password: Some(wire_credential_source(password)),
            })
        }
        AuthenticationConfiguration::OAuthDeviceCode {
            device_authorization_url,
            token_url,
            scopes,
            scope_format,
            client,
            ..
        } => WireAuthentication::OauthDeviceCode(wire::OAuthDeviceCodeAuthentication {
            device_authorization_url,
            token_url,
            scopes,
            scope_format: wire_scope_format(scope_format),
            oauth_client_id: client
                .as_ref()
                .map(|policy| policy.client_id.clone())
                .unwrap_or_default(),
            application: client.as_ref().map(application_view),
        }),
        AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url,
            token_url,
            scopes,
            scope_format,
            client,
            ..
        } => {
            WireAuthentication::OauthAuthorizationCode(wire::OAuthAuthorizationCodeAuthentication {
                authorization_url,
                token_url,
                scopes,
                scope_format: wire_scope_format(scope_format),
                oauth_client_id: client
                    .as_ref()
                    .map(|policy| policy.client_id.clone())
                    .unwrap_or_default(),
                application: client.as_ref().map(application_view),
            })
        }
    }
}

pub(super) fn credential_field(field: CredentialField) -> wire::CredentialField {
    wire::CredentialField {
        label: field.label,
        description: field.description,
        sensitive: field.sensitive,
    }
}

fn configuration(value: ProviderConfiguration) -> wire::ProviderConfiguration {
    let mut result = wire::ProviderConfiguration {
        display_name: value.display_name,
        request_headers: value.request_headers.into_iter().collect(),
        authentication: value.authentication.map(authentication),
        credential_fields: super::credential_fields(value.credential_fields),
        ..Default::default()
    };
    result.servers = value
        .servers
        .into_iter()
        .map(|(declaration, selection)| {
            let (url, custom, variables) = match selection {
                ServerSelection::Custom { url } => {
                    (url, true, std::collections::HashMap::default())
                }
                ServerSelection::Declared { url, variables } => {
                    (url, false, variables.into_iter().collect())
                }
            };
            wire::ProviderServerChoice {
                declaration,
                url,
                custom,
                variables,
            }
        })
        .collect();
    result
}

pub(super) fn review(
    value: coral_provider::SetupConfigurationReview,
) -> wire::ProviderConfigurationReview {
    let mut result = wire::ProviderConfigurationReview {
        revision: value.revision,
        configuration_confirmed: value.configuration_confirmed,
        authentication_methods: value
            .authentication_methods
            .iter()
            .map(|method| super::wire_method(*method).into())
            .collect(),
        discovery_authorization_required: value.discovery_authorization_required,
        issuer: value.issuer,
        resource: value.resource,
        device_authorization_url: value.device_authorization_url.unwrap_or_default(),
        differences: value
            .differences
            .into_iter()
            .map(|difference| wire::ProviderConfigurationDifference {
                field: difference.field,
                saved: difference.saved,
                document: difference.document,
            })
            .collect(),
        interrupts_access: value.interrupts_access,
        updating: value.updating,
        confirmed: value.confirmed.map(configuration),
        destinations: value.destinations.into_iter().collect(),
        tools: value
            .tools
            .into_iter()
            .map(|tool| wire::ReviewedProviderTool {
                key: tool.key,
                relative_path: tool.relative_path,
                summary: tool.summary,
                tags: tool.tags,
            })
            .collect(),
        unavailable_tools: value
            .unavailable_tools
            .into_iter()
            .map(|(key, diagnostics)| wire::UnavailableProviderTool {
                key,
                reasons: diagnostics
                    .iter()
                    .map(|diagnostic| diagnostic.message().into())
                    .collect(),
            })
            .collect(),
        diagnostic: value.diagnostic.unwrap_or_default(),
        errors: value
            .errors
            .into_iter()
            .map(|error| (error.field, error.message))
            .collect(),
        ..Default::default()
    };
    if let Some(document) = value.document {
        result.authentication_status = document.authentication_status;
        result.suggested = Some(configuration(document.suggested));
        result.servers = document
            .servers
            .into_iter()
            .map(|group| wire::ProviderServerGroup {
                declaration: group.declaration,
                tools: group.tools,
                options: group
                    .options
                    .into_iter()
                    .map(|option| wire::ProviderServerOption {
                        url: option.url,
                        variables: option
                            .variables
                            .into_iter()
                            .map(|(name, variable)| wire::ProviderServerVariable {
                                name,
                                default_value: variable.default.unwrap_or_default(),
                                enumerated: variable.enumeration.is_some(),
                                values: variable.enumeration.unwrap_or_default(),
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect();
    }
    result
}

pub(super) fn application_view(
    policy: &coral_catalog::OAuthClientPolicy,
) -> wire::OAuthApplication {
    wire::OAuthApplication {
        mode: Some(match policy.bootstrap {
            coral_catalog::OAuthClientBootstrap::Manual => {
                wire::o_auth_application::Mode::ClientId(policy.client_id.clone())
            }
            _ => wire::o_auth_application::Mode::Automatic(()),
        }),
        authentication: Some(wire::OAuthClientAuthentication {
            method: Some(match policy.authentication {
                coral_catalog::OAuthClientAuthenticationMethod::None => {
                    wire::o_auth_client_authentication::Method::None(())
                }
                coral_catalog::OAuthClientAuthenticationMethod::ClientSecretBasic => {
                    wire::o_auth_client_authentication::Method::ClientSecretBasic(
                        wire::OAuthClientSecret::default(),
                    )
                }
                coral_catalog::OAuthClientAuthenticationMethod::ClientSecretPost => {
                    wire::o_auth_client_authentication::Method::ClientSecretPost(
                        wire::OAuthClientSecret::default(),
                    )
                }
            }),
        }),
        pkce: match policy.pkce {
            coral_catalog::OAuthPkcePolicy::S256 => wire::OAuthPkcePolicy::S256,
            coral_catalog::OAuthPkcePolicy::Disabled => wire::OAuthPkcePolicy::Disabled,
        }
        .into(),
        authorization_parameters: policy
            .authorization_parameters
            .clone()
            .into_iter()
            .collect(),
    }
}

pub(super) fn application_input(
    application: Option<wire::OAuthApplication>,
    redirect: String,
) -> Result<coral_provider::SetupOAuthClient, Status> {
    let application = application.unwrap_or_default();
    let authentication = match application.authentication.and_then(|value| value.method) {
        None | Some(wire::o_auth_client_authentication::Method::None(())) => {
            OAuthClientAuthentication::None
        }
        Some(wire::o_auth_client_authentication::Method::ClientSecretBasic(value)) => {
            OAuthClientAuthentication::ClientSecretBasic {
                secret: client_secret_input(value)?,
            }
        }
        Some(wire::o_auth_client_authentication::Method::ClientSecretPost(value)) => {
            OAuthClientAuthentication::ClientSecretPost {
                secret: client_secret_input(value)?,
            }
        }
    };
    let pkce = match wire::OAuthPkcePolicy::try_from(application.pkce) {
        Ok(wire::OAuthPkcePolicy::Unspecified) => coral_catalog::OAuthPkcePolicy::default(),
        Ok(wire::OAuthPkcePolicy::S256) => coral_catalog::OAuthPkcePolicy::S256,
        Ok(wire::OAuthPkcePolicy::Disabled) => coral_catalog::OAuthPkcePolicy::Disabled,
        Err(_) => return Err(super::invalid("unknown PKCE policy")),
    };
    match application.mode {
        Some(wire::o_auth_application::Mode::ClientId(id)) => {
            let configuration = coral_provider::OAuthClientConfiguration {
                client_id: id,
                authentication,
                pkce,
                authorization_parameters: application
                    .authorization_parameters
                    .into_iter()
                    .collect(),
            };
            coral_provider::SetupOAuthClient::application(redirect, configuration)
                .map_err(super::error)
        }
        _ if matches!(authentication, OAuthClientAuthentication::None)
            && pkce == coral_catalog::OAuthPkcePolicy::S256 =>
        {
            coral_provider::SetupOAuthClient::automatic(
                redirect,
                application.authorization_parameters.into_iter().collect(),
            )
            .map_err(super::error)
        }
        _ => Err(super::invalid(
            "automatic clients require public authentication and S256",
        )),
    }
}

fn client_secret_input(
    value: wire::OAuthClientSecret,
) -> Result<Option<coral_credentials::Secret>, Status> {
    (!value.secret.is_empty())
        .then(|| coral_credentials::Secret::new(value.secret))
        .transpose()
        .map_err(|_error| super::invalid("invalid client secret"))
}

fn scope_format(value: i32) -> Result<coral_catalog::OAuthScopeFormat, Status> {
    match wire::OAuthScopeFormat::try_from(value) {
        Ok(wire::OAuthScopeFormat::Unspecified | wire::OAuthScopeFormat::Space) => {
            Ok(coral_catalog::OAuthScopeFormat::Space)
        }
        Ok(wire::OAuthScopeFormat::Comma) => Ok(coral_catalog::OAuthScopeFormat::Comma),
        Err(_) => Err(Status::invalid_argument("unknown OAuth scope format")),
    }
}

fn wire_scope_format(value: coral_catalog::OAuthScopeFormat) -> i32 {
    match value {
        coral_catalog::OAuthScopeFormat::Space => wire::OAuthScopeFormat::Space as i32,
        coral_catalog::OAuthScopeFormat::Comma => wire::OAuthScopeFormat::Comma as i32,
    }
}
