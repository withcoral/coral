//! Source-related client-side transport decoding helpers.

use coral_api::v1::{
    OAuthAuthorizationCodeCredentialMethod, OauthCredentialClientSecretTransport,
    OauthCredentialPkceMode, OauthCredentialScopeDelimiter, SourceCredential,
    SourceCredentialMethod, SourceInputSpec,
    source_credential_method::Method as ProtoCredentialMethod,
    source_input_spec::Input as ProtoSourceInput,
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
    /// The source input did not include variant metadata.
    #[error("missing input metadata for '{key}'")]
    MissingInput {
        /// The input key whose variant metadata was missing.
        key: String,
    },
    /// The credential method did not include variant metadata.
    #[error("credential method is missing method metadata")]
    MissingCredentialMethod,
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
    let (kind, default_value, credential) = match input.input.as_ref() {
        Some(ProtoSourceInput::Variable(variable)) => (
            ManifestInputKind::Variable,
            variable.default_value.clone(),
            None,
        ),
        Some(ProtoSourceInput::Secret(secret)) => (
            ManifestInputKind::Secret,
            String::new(),
            secret
                .credential
                .as_ref()
                .map(credential_from_proto)
                .transpose()?,
        ),
        None => {
            return Err(SourceInputDecodeError::MissingInput {
                key: input.key.clone(),
            });
        }
    };
    Ok(ManifestInputSpec {
        key: input.key.clone(),
        kind,
        required: input.required,
        default_value,
        hint: (!input.hint.is_empty()).then(|| input.hint.clone()),
        credential,
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
    let (kind, oauth) = match method.method.as_ref() {
        Some(ProtoCredentialMethod::SourceConfig(_)) => {
            (ManifestCredentialMethodKind::SourceConfig, None)
        }
        Some(ProtoCredentialMethod::OauthAuthorizationCode(oauth)) => (
            ManifestCredentialMethodKind::OAuth,
            Some(oauth_authorization_code_from_proto(oauth)?),
        ),
        None => return Err(SourceInputDecodeError::MissingCredentialMethod),
    };
    Ok(ManifestCredentialMethod {
        kind,
        label: (!method.label.is_empty()).then(|| method.label.clone()),
        description: (!method.description.is_empty()).then(|| method.description.clone()),
        oauth,
    })
}

fn oauth_authorization_code_from_proto(
    oauth: &OAuthAuthorizationCodeCredentialMethod,
) -> Result<ManifestOAuthCredentialSpec, SourceInputDecodeError> {
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
        flow: ManifestOAuthFlowSpec {
            kind: ManifestOAuthFlowKind::AuthorizationCode,
            pkce: oauth_pkce_from_proto(oauth.pkce)?,
        },
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

fn oauth_pkce_from_proto(pkce: i32) -> Result<ManifestOAuthPkceMode, SourceInputDecodeError> {
    let pkce = match OauthCredentialPkceMode::try_from(pkce) {
        Ok(OauthCredentialPkceMode::Required) => ManifestOAuthPkceMode::Required,
        Ok(OauthCredentialPkceMode::Disabled) => ManifestOAuthPkceMode::Disabled,
        Ok(OauthCredentialPkceMode::Unspecified) | Err(_) => {
            return Err(SourceInputDecodeError::UnknownOAuthPkceMode);
        }
    };
    Ok(pkce)
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
            default: (!id.default_value.is_empty()).then(|| id.default_value.clone()),
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
    let transport = match OauthCredentialClientSecretTransport::try_from(secret.transport) {
        Ok(OauthCredentialClientSecretTransport::BasicAuth) => {
            ManifestOAuthClientSecretTransport::BasicAuth
        }
        Ok(OauthCredentialClientSecretTransport::RequestBody) => {
            ManifestOAuthClientSecretTransport::RequestBody
        }
        Ok(OauthCredentialClientSecretTransport::Unspecified) | Err(_) => {
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
    let delimiter = match OauthCredentialScopeDelimiter::try_from(scope.delimiter) {
        Ok(OauthCredentialScopeDelimiter::Space) => ManifestOAuthScopeDelimiter::Space,
        Ok(OauthCredentialScopeDelimiter::Comma) => ManifestOAuthScopeDelimiter::Comma,
        Ok(OauthCredentialScopeDelimiter::Unspecified) | Err(_) => {
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
        OAuthAuthorizationCodeCredentialMethod, OAuthCredentialClient, OAuthCredentialClientId,
        OAuthCredentialEndpoints, SourceConfigCredentialMethod, SourceSecretInput,
        source_credential_method::Method as ProtoCredentialMethod,
        source_input_spec::Input as ProtoSourceInput,
    };

    use super::*;

    #[test]
    fn manifest_input_from_proto_preserves_credential_methods() {
        let input = SourceInputSpec {
            key: "API_TOKEN".to_string(),
            required: true,
            hint: String::new(),
            input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                credential: Some(SourceCredential {
                    methods: vec![
                        SourceCredentialMethod {
                            label: "Connect".to_string(),
                            description: String::new(),
                            method: Some(ProtoCredentialMethod::OauthAuthorizationCode(
                                OAuthAuthorizationCodeCredentialMethod {
                                    pkce: OauthCredentialPkceMode::Required as i32,
                                    redirect_uri: "http://127.0.0.1:53682/oauth/callback"
                                        .to_string(),
                                    endpoints: Some(OAuthCredentialEndpoints {
                                        authorization_url:
                                            "https://provider.example.com/oauth/authorize"
                                                .to_string(),
                                        token_url: "https://provider.example.com/oauth/token"
                                            .to_string(),
                                    }),
                                    client: Some(OAuthCredentialClient {
                                        id: Some(OAuthCredentialClientId {
                                            default_value: "default-client".to_string(),
                                            input: String::new(),
                                        }),
                                        secret: None,
                                    }),
                                    scopes: None,
                                },
                            )),
                        },
                        SourceCredentialMethod {
                            label: "Paste token".to_string(),
                            description: String::new(),
                            method: Some(ProtoCredentialMethod::SourceConfig(
                                SourceConfigCredentialMethod {},
                            )),
                        },
                    ],
                }),
            })),
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

    fn source_input_with_oauth_pkce(pkce: OauthCredentialPkceMode) -> SourceInputSpec {
        SourceInputSpec {
            key: "API_TOKEN".to_string(),
            required: true,
            hint: String::new(),
            input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                credential: Some(SourceCredential {
                    methods: vec![SourceCredentialMethod {
                        label: "Connect".to_string(),
                        description: String::new(),
                        method: Some(ProtoCredentialMethod::OauthAuthorizationCode(
                            OAuthAuthorizationCodeCredentialMethod {
                                pkce: pkce as i32,
                                redirect_uri: "http://127.0.0.1:53682/oauth/callback".to_string(),
                                endpoints: Some(OAuthCredentialEndpoints {
                                    authorization_url:
                                        "https://provider.example.com/oauth/authorize".to_string(),
                                    token_url: "https://provider.example.com/oauth/token"
                                        .to_string(),
                                }),
                                client: Some(OAuthCredentialClient {
                                    id: Some(OAuthCredentialClientId {
                                        default_value: "default-client".to_string(),
                                        input: String::new(),
                                    }),
                                    secret: None,
                                }),
                                scopes: None,
                            },
                        )),
                    }],
                }),
            })),
        }
    }

    #[test]
    fn manifest_input_from_proto_rejects_missing_input_metadata() {
        let input = SourceInputSpec {
            key: "API_TOKEN".to_string(),
            required: true,
            hint: String::new(),
            input: None,
        };

        let error = manifest_input_from_proto(&input).expect_err("missing input should fail");

        assert!(matches!(error, SourceInputDecodeError::MissingInput { .. }));
    }

    #[test]
    fn manifest_input_from_proto_rejects_unspecified_oauth_pkce_mode() {
        let input = source_input_with_oauth_pkce(OauthCredentialPkceMode::Unspecified);

        let error = manifest_input_from_proto(&input).expect_err("unspecified pkce should fail");

        assert!(matches!(
            error,
            SourceInputDecodeError::UnknownOAuthPkceMode
        ));
    }

    #[test]
    fn manifest_input_from_proto_rejects_missing_credential_method() {
        let input = SourceInputSpec {
            key: "API_TOKEN".to_string(),
            required: true,
            hint: String::new(),
            input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                credential: Some(SourceCredential {
                    methods: vec![SourceCredentialMethod {
                        label: "Connect".to_string(),
                        description: String::new(),
                        method: None,
                    }],
                }),
            })),
        };

        let error = manifest_input_from_proto(&input).expect_err("missing method should fail");

        assert!(matches!(
            error,
            SourceInputDecodeError::MissingCredentialMethod
        ));
    }
}
