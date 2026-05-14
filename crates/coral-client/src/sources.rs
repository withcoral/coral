//! Source-related client-side transport decoding helpers.

use coral_api::v1::{
    OAuthCredentialClientSecretTransport, OAuthCredentialFlow, OAuthCredentialFlowType,
    OAuthCredentialPkceMode, OAuthCredentialScopeDelimiter, SourceCredential,
    SourceCredentialMethod, SourceCredentialMethodType, SourceInputKind, SourceInputSpec,
};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
    ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
    ManifestOAuthScopeDelimiter, ManifestOAuthScopeSpec, ManifestOAuthScopesSpec,
};

/// Errors returned while decoding source input metadata from the gRPC API.
#[derive(Debug, thiserror::Error)]
pub enum SourceInputDecodeError {
    /// The source input kind was missing or unknown.
    #[error("unknown input kind for '{key}'")]
    UnknownInputKind {
        /// The input key whose kind could not be decoded.
        key: String,
    },
    /// The credential method type was missing or unknown.
    #[error("unknown credential method type")]
    UnknownCredentialMethodType,
    /// The OAuth credential method did not include flow settings.
    #[error("oauth credential method is missing flow")]
    MissingOAuthFlow,
    /// The OAuth flow type was missing or unknown.
    #[error("unknown oauth flow type")]
    UnknownOAuthFlowType,
    /// The OAuth PKCE mode was missing or unknown.
    #[error("unknown oauth pkce mode")]
    UnknownOAuthPkceMode,
    /// The OAuth credential method did not include provider endpoints.
    #[error("oauth credential method is missing endpoints")]
    MissingOAuthEndpoints,
    /// The OAuth credential method did not include client settings.
    #[error("oauth credential method is missing client")]
    MissingOAuthClient,
    /// The OAuth client settings did not include client ID resolution.
    #[error("oauth client is missing id")]
    MissingOAuthClientId,
    /// The OAuth client secret transport was missing or unknown.
    #[error("unknown oauth client secret transport")]
    UnknownOAuthClientSecretTransport,
    /// The OAuth scopes settings did not include a scope definition.
    #[error("oauth scopes is missing scope")]
    MissingOAuthScope,
    /// The OAuth scope delimiter was missing or unknown.
    #[error("unknown oauth scope delimiter")]
    UnknownOAuthScopeDelimiter,
}

/// Decodes one source input from the gRPC API into the manifest input model.
///
/// # Errors
///
/// Returns [`SourceInputDecodeError`] when the server response contains missing
/// or unknown enum values or incomplete nested OAuth credential metadata.
pub fn manifest_input_from_proto(
    input: &SourceInputSpec,
) -> Result<ManifestInputSpec, SourceInputDecodeError> {
    let kind = match SourceInputKind::try_from(input.kind) {
        Ok(SourceInputKind::Variable) => ManifestInputKind::Variable,
        Ok(SourceInputKind::Secret) => ManifestInputKind::Secret,
        Ok(SourceInputKind::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownInputKind {
                key: input.key.clone(),
            });
        }
    };
    Ok(ManifestInputSpec {
        key: input.key.clone(),
        kind,
        required: input.required,
        default_value: input.default_value.clone(),
        hint: (!input.hint.is_empty()).then(|| input.hint.clone()),
        credential: input
            .credential
            .as_ref()
            .map(credential_from_proto)
            .transpose()?,
    })
}

fn credential_from_proto(
    credential: &SourceCredential,
) -> Result<ManifestCredentialSpec, SourceInputDecodeError> {
    Ok(ManifestCredentialSpec {
        methods: credential
            .methods
            .iter()
            .map(credential_method_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn credential_method_from_proto(
    method: &SourceCredentialMethod,
) -> Result<ManifestCredentialMethod, SourceInputDecodeError> {
    let kind = match SourceCredentialMethodType::try_from(method.r#type) {
        Ok(SourceCredentialMethodType::SourceConfig) => ManifestCredentialMethodKind::SourceConfig,
        Ok(SourceCredentialMethodType::Oauth) => ManifestCredentialMethodKind::OAuth,
        Ok(SourceCredentialMethodType::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownCredentialMethodType);
        }
    };
    Ok(ManifestCredentialMethod {
        kind,
        label: (!method.label.is_empty()).then(|| method.label.clone()),
        description: (!method.description.is_empty()).then(|| method.description.clone()),
        oauth: method.oauth.as_ref().map(oauth_from_proto).transpose()?,
    })
}

fn oauth_from_proto(
    oauth: &coral_api::v1::OAuthCredentialMethod,
) -> Result<ManifestOAuthCredentialSpec, SourceInputDecodeError> {
    let flow = oauth
        .flow
        .as_ref()
        .ok_or(SourceInputDecodeError::MissingOAuthFlow)
        .copied()
        .and_then(oauth_flow_from_proto)?;
    let endpoints = oauth
        .endpoints
        .as_ref()
        .ok_or(SourceInputDecodeError::MissingOAuthEndpoints)?;
    let client = oauth
        .client
        .as_ref()
        .ok_or(SourceInputDecodeError::MissingOAuthClient)
        .and_then(oauth_client_from_proto)?;
    Ok(ManifestOAuthCredentialSpec {
        flow,
        redirect_uri: oauth.redirect_uri.clone(),
        authorization_url: endpoints.authorization_url.clone(),
        token_url: endpoints.token_url.clone(),
        client,
        scopes: oauth
            .scopes
            .as_ref()
            .map(oauth_scopes_from_proto)
            .transpose()?,
    })
}

fn oauth_flow_from_proto(
    flow: OAuthCredentialFlow,
) -> Result<ManifestOAuthFlowSpec, SourceInputDecodeError> {
    let kind = match OAuthCredentialFlowType::try_from(flow.r#type) {
        Ok(OAuthCredentialFlowType::AuthorizationCode) => ManifestOAuthFlowKind::AuthorizationCode,
        Ok(OAuthCredentialFlowType::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownOAuthFlowType);
        }
    };
    let pkce = match OAuthCredentialPkceMode::try_from(flow.pkce) {
        Ok(OAuthCredentialPkceMode::Required) => ManifestOAuthPkceMode::Required,
        Ok(OAuthCredentialPkceMode::Disabled) => ManifestOAuthPkceMode::Disabled,
        Ok(OAuthCredentialPkceMode::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownOAuthPkceMode);
        }
    };
    Ok(ManifestOAuthFlowSpec { kind, pkce })
}

fn oauth_client_from_proto(
    client: &coral_api::v1::OAuthCredentialClient,
) -> Result<ManifestOAuthClientSpec, SourceInputDecodeError> {
    let id = client
        .id
        .as_ref()
        .ok_or(SourceInputDecodeError::MissingOAuthClientId)?;
    Ok(ManifestOAuthClientSpec {
        id: ManifestOAuthClientIdSpec {
            default: (!id.default.is_empty()).then(|| id.default.clone()),
            input: (!id.input.is_empty()).then(|| id.input.clone()),
        },
        secret: client
            .secret
            .as_ref()
            .map(oauth_client_secret_from_proto)
            .transpose()?,
    })
}

fn oauth_client_secret_from_proto(
    secret: &coral_api::v1::OAuthCredentialClientSecret,
) -> Result<ManifestOAuthClientSecretSpec, SourceInputDecodeError> {
    let transport = match OAuthCredentialClientSecretTransport::try_from(secret.transport) {
        Ok(OAuthCredentialClientSecretTransport::BasicAuth) => {
            ManifestOAuthClientSecretTransport::BasicAuth
        }
        Ok(OAuthCredentialClientSecretTransport::RequestBody) => {
            ManifestOAuthClientSecretTransport::RequestBody
        }
        Ok(OAuthCredentialClientSecretTransport::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownOAuthClientSecretTransport);
        }
    };
    Ok(ManifestOAuthClientSecretSpec {
        input: secret.input.clone(),
        transport,
    })
}

fn oauth_scopes_from_proto(
    scopes: &coral_api::v1::OAuthCredentialScopes,
) -> Result<ManifestOAuthScopesSpec, SourceInputDecodeError> {
    let scope = scopes
        .scope
        .as_ref()
        .ok_or(SourceInputDecodeError::MissingOAuthScope)?;
    let delimiter = match OAuthCredentialScopeDelimiter::try_from(scope.delimiter) {
        Ok(OAuthCredentialScopeDelimiter::Space) => ManifestOAuthScopeDelimiter::Space,
        Ok(OAuthCredentialScopeDelimiter::Comma) => ManifestOAuthScopeDelimiter::Comma,
        Ok(OAuthCredentialScopeDelimiter::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownOAuthScopeDelimiter);
        }
    };
    Ok(ManifestOAuthScopesSpec {
        scope: ManifestOAuthScopeSpec {
            delimiter,
            values: scope.values.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "credential method order assertions intentionally fail loudly in tests"
    )]

    use coral_api::v1::{
        OAuthCredentialClient, OAuthCredentialClientId, OAuthCredentialEndpoints,
        OAuthCredentialFlowType, OAuthCredentialMethod, SourceCredentialMethodType,
    };

    use super::*;

    #[test]
    fn manifest_input_from_proto_preserves_credential_methods() {
        let input = SourceInputSpec {
            key: "API_TOKEN".to_string(),
            kind: SourceInputKind::Secret as i32,
            required: true,
            default_value: String::new(),
            hint: String::new(),
            credential: Some(SourceCredential {
                methods: vec![
                    SourceCredentialMethod {
                        r#type: SourceCredentialMethodType::Oauth as i32,
                        label: "Connect".to_string(),
                        description: String::new(),
                        oauth: Some(OAuthCredentialMethod {
                            flow: Some(OAuthCredentialFlow {
                                r#type: OAuthCredentialFlowType::AuthorizationCode as i32,
                                pkce: OAuthCredentialPkceMode::Required as i32,
                            }),
                            redirect_uri: "http://127.0.0.1:53682/oauth/callback".to_string(),
                            endpoints: Some(OAuthCredentialEndpoints {
                                authorization_url: "https://provider.example.com/oauth/authorize"
                                    .to_string(),
                                token_url: "https://provider.example.com/oauth/token".to_string(),
                            }),
                            client: Some(OAuthCredentialClient {
                                id: Some(OAuthCredentialClientId {
                                    default: "default-client".to_string(),
                                    input: String::new(),
                                }),
                                secret: None,
                            }),
                            scopes: None,
                        }),
                    },
                    SourceCredentialMethod {
                        r#type: SourceCredentialMethodType::SourceConfig as i32,
                        label: "Paste token".to_string(),
                        description: String::new(),
                        oauth: None,
                    },
                ],
            }),
        };

        let input = manifest_input_from_proto(&input).expect("manifest input");
        let credential = input.credential.expect("credential");
        assert_eq!(credential.methods.len(), 2);
        assert_eq!(
            credential.methods[0].kind,
            ManifestCredentialMethodKind::OAuth
        );
        assert_eq!(credential.methods[0].label.as_deref(), Some("Connect"));
        assert_eq!(
            credential.methods[0]
                .oauth
                .as_ref()
                .expect("oauth")
                .client
                .id
                .default
                .as_deref(),
            Some("default-client")
        );
        assert_eq!(
            credential.methods[1].kind,
            ManifestCredentialMethodKind::SourceConfig
        );
    }

    fn source_input_with_oauth_flow(flow: OAuthCredentialFlow) -> SourceInputSpec {
        SourceInputSpec {
            key: "API_TOKEN".to_string(),
            kind: SourceInputKind::Secret as i32,
            required: true,
            default_value: String::new(),
            hint: String::new(),
            credential: Some(SourceCredential {
                methods: vec![SourceCredentialMethod {
                    r#type: SourceCredentialMethodType::Oauth as i32,
                    label: "Connect".to_string(),
                    description: String::new(),
                    oauth: Some(OAuthCredentialMethod {
                        flow: Some(flow),
                        redirect_uri: "http://127.0.0.1:53682/oauth/callback".to_string(),
                        endpoints: Some(OAuthCredentialEndpoints {
                            authorization_url: "https://provider.example.com/oauth/authorize"
                                .to_string(),
                            token_url: "https://provider.example.com/oauth/token".to_string(),
                        }),
                        client: Some(OAuthCredentialClient {
                            id: Some(OAuthCredentialClientId {
                                default: "default-client".to_string(),
                                input: String::new(),
                            }),
                            secret: None,
                        }),
                        scopes: None,
                    }),
                }],
            }),
        }
    }

    #[test]
    fn manifest_input_from_proto_rejects_unspecified_oauth_flow_type() {
        let input = source_input_with_oauth_flow(OAuthCredentialFlow {
            r#type: OAuthCredentialFlowType::Unspecified as i32,
            pkce: OAuthCredentialPkceMode::Required as i32,
        });

        let error = manifest_input_from_proto(&input).expect_err("unspecified flow should fail");

        assert!(matches!(
            error,
            SourceInputDecodeError::UnknownOAuthFlowType
        ));
    }

    #[test]
    fn manifest_input_from_proto_rejects_unspecified_oauth_pkce_mode() {
        let input = source_input_with_oauth_flow(OAuthCredentialFlow {
            r#type: OAuthCredentialFlowType::AuthorizationCode as i32,
            pkce: OAuthCredentialPkceMode::Unspecified as i32,
        });

        let error = manifest_input_from_proto(&input).expect_err("unspecified pkce should fail");

        assert!(matches!(
            error,
            SourceInputDecodeError::UnknownOAuthPkceMode
        ));
    }
}
