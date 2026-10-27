//! MCP has one destination and discovered OAuth facts, never metadata overrides.
use crate::{Error, repository::LoadedSetup, setup_credentials::StoredRequirement};
use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{
    ConfigurationFieldError, ProviderConfiguration, ProviderConfigurationReview,
    ProviderCredentialContract as Contract, ProviderSetupInput, ServerGroup, ServerOption,
    ServerSelection,
};
use std::collections::BTreeMap;

pub(super) fn source(loaded: &LoadedSetup) -> Result<String, Error> {
    if let Some(url) = loaded.setup.input_url() {
        return Ok(url.to_owned());
    }
    match loaded.setup_input.as_ref() {
        Some(ProviderSetupInput::Mcp(snapshot)) => snapshot
            .connection()
            .map(|(url, _)| url)
            .map_err(|_invalid| Error::ProviderSetupNotApplicable),
        _ => Err(Error::ProviderSetupNotApplicable),
    }
}

pub(super) fn requirement(loaded: &LoadedSetup) -> Result<Option<StoredRequirement>, Error> {
    loaded
        .authorization_requirement
        .as_deref()
        .map(StoredRequirement::parse)
        .transpose()
}

pub(super) fn detected(loaded: &LoadedSetup) -> Result<Contract, Error> {
    if let Some(requirement) = requirement(loaded)? {
        return Ok(requirement.contract(None));
    }
    match loaded.setup_input.as_ref() {
        Some(ProviderSetupInput::Mcp(snapshot)) => snapshot
            .connection()
            .map(|(_, contract)| contract)
            .map_err(|_invalid| Error::ProviderSetupNotApplicable),
        _ => Ok(AuthenticationConfiguration::Bearer.into()),
    }
}

/// Resolves the selected flow using only pinned discovery facts.
pub(super) fn detected_method(
    loaded: &LoadedSetup,
    kind: coral_catalog::AuthenticationKind,
) -> Result<Contract, Error> {
    if kind == coral_catalog::AuthenticationKind::OAuthDeviceCode
        && let Some(mut requirement) = requirement(loaded)?
    {
        let endpoint = requirement
            .device_authorization_endpoint
            .clone()
            .ok_or(Error::ProviderSetupNotApplicable)?;
        requirement.device_code = true;
        requirement.authorization_endpoint = endpoint;
        return Ok(requirement.contract(None));
    }
    let contract = detected(loaded)?;
    if contract.authentication().kind() != kind {
        return Err(Error::ProviderSetupNotApplicable);
    }
    Ok(contract)
}

pub(super) fn needs_discovery(loaded: &LoadedSetup) -> bool {
    loaded.setup_input.is_none()
}

pub(super) fn anonymous_catalog(loaded: &LoadedSetup) -> bool {
    matches!(loaded.setup_input.as_ref(), Some(ProviderSetupInput::Mcp(snapshot)) if snapshot.connection().is_ok_and(|(_, contract)| contract.authentication() == &AuthenticationConfiguration::None))
}

pub(super) fn inspect(loaded: &LoadedSetup) -> Result<ProviderConfigurationReview, Error> {
    let source = source(loaded)?;
    let contract = detected(loaded)?;
    let missing = loaded.authorization_requirement.is_none() && loaded.setup_input.is_none();
    let status = if missing {
        "metadata_missing"
    } else {
        match contract.authentication() {
            AuthenticationConfiguration::None => "detected_anonymous",
            AuthenticationConfiguration::OAuthAuthorizationCode { .. }
            | AuthenticationConfiguration::OAuthDeviceCode { .. } => "detected_oauth",
            _ => "detected_bearer",
        }
    };
    Ok(ProviderConfigurationReview {
        authentication_status: status.into(),
        suggested: ProviderConfiguration {
            request_headers: loaded.initial_request_headers.clone(),
            authentication: (!missing).then(|| editable_authentication(&contract)),
            credential_fields: BTreeMap::new(),
            servers: BTreeMap::from([(
                "#/servers".into(),
                ServerSelection::Custom {
                    url: source.clone(),
                },
            )]),
            // The server names itself once read; until then its host stands in.
            display_name: match loaded.setup_input.as_ref() {
                Some(ProviderSetupInput::Mcp(snapshot)) => snapshot.server_name(),
                _ => None,
            }
            .or_else(|| {
                url::Url::parse(&source)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_owned))
            })
            .unwrap_or_default(),
        },
        servers: vec![ServerGroup {
            declaration: "#/servers".into(),
            tools: Vec::new(),
            options: vec![ServerOption {
                url: source,
                variables: BTreeMap::new(),
            }],
        }],
    })
}

/// The editable transport projection excludes issuer/resource/client ownership.
/// Those facts always come from validated discovery, not form input.
pub(super) fn editable_authentication(
    contract: &Contract,
) -> coral_catalog::AuthenticationConfiguration {
    match contract.authentication() {
        AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url,
            token_url,
            scopes,
            scope_format,
            ..
        } => AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url: authorization_url.clone(),
            token_url: token_url.clone(),
            scopes: scopes.clone(),
            scope_format: *scope_format,
            resource: None,
            issuer: None,
            client: None,
        },
        AuthenticationConfiguration::OAuthDeviceCode {
            device_authorization_url,
            token_url,
            scopes,
            scope_format,
            ..
        } => AuthenticationConfiguration::OAuthDeviceCode {
            device_authorization_url: device_authorization_url.clone(),
            token_url: token_url.clone(),
            scopes: scopes.clone(),
            scope_format: *scope_format,
            resource: None,
            issuer: None,
            client: None,
        },
        method => method.clone(),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one confirmation checks editable settings against pinned MCP discovery"
)]
pub(super) fn validate(
    loaded: &LoadedSetup,
    mut values: ProviderConfiguration,
) -> Result<ProviderConfiguration, Vec<ConfigurationFieldError>> {
    let field_error = |field: &str, message: &str| {
        vec![ConfigurationFieldError {
            field: field.into(),
            message: message.into(),
        }]
    };
    values.normalize_request_headers(true)?;
    coral_catalog::normalize_display_name(&mut values).map_err(|problem| vec![problem])?;
    let display_name = values.display_name.clone();
    let Ok(source) = source(loaded) else {
        // Invalid snapshots still feed the established terminal Publication failure.
        if loaded.setup_input.is_some()
            && values.servers.is_empty()
            && values.authentication == Some(AuthenticationConfiguration::None)
        {
            return Ok(values);
        }
        return Err(field_error("server", "Choose a valid MCP server URL."));
    };
    let source = url::Url::parse(&source)
        .map_err(|_invalid| field_error("server", "Choose a valid MCP server URL."))?
        .to_string();
    let chosen = match values.servers.get("#/servers") {
        Some(ServerSelection::Custom { url }) => Some(url),
        Some(ServerSelection::Declared { url, variables }) if variables.is_empty() => Some(url),
        _ => None,
    };
    if values.servers.len() != 1
        || chosen
            .and_then(|url| url::Url::parse(url).ok())
            .is_none_or(|url| url.as_str() != source)
    {
        return Err(field_error(
            "server",
            "Change the server URL to start fresh discovery. Your previous credentials do not transfer.",
        ));
    }
    let selected = values
        .authentication
        .as_ref()
        .map(AuthenticationConfiguration::kind);
    let detected = match selected {
        Some(
            kind @ (coral_catalog::AuthenticationKind::OAuthAuthorizationCode
            | coral_catalog::AuthenticationKind::OAuthDeviceCode),
        ) => detected_method(loaded, kind),
        _ => detected(loaded),
    }
    .map_err(|_invalid| field_error("authentication", "Review the detected authentication."))?
    .with_client_policy(None);
    let policy = values
        .authentication
        .as_ref()
        .and_then(AuthenticationConfiguration::oauth_client)
        .cloned();
    if let Some(policy) = &policy
        && !valid_client(policy, detected.authentication())
    {
        return Err(field_error(
            "application",
            "Review the OAuth application settings for this flow.",
        ));
    }
    let unsupported = || {
        field_error(
            "authentication",
            "Choose a supported method. MCP OAuth endpoints are read-only.",
        )
    };
    if values
        .authentication
        .as_ref()
        .is_some_and(|method| !super::offers(loaded, method))
    {
        return Err(unsupported());
    }
    let contract = match values
        .authentication
        .map(|method| method.with_client_policy(None))
    {
        Some(AuthenticationConfiguration::None) => AuthenticationConfiguration::None,
        Some(AuthenticationConfiguration::Bearer) => AuthenticationConfiguration::Bearer,
        Some(
            method @ (AuthenticationConfiguration::OAuthAuthorizationCode { .. }
            | AuthenticationConfiguration::OAuthDeviceCode { .. }),
        ) => {
            let (scopes, format) = method.oauth_scopes().expect("OAuth has scope settings");
            let scopes = format.normalize(scopes).map_err(|_invalid| {
                field_error(
                    "authentication.scopes",
                    "Enter up to 100 valid scopes. Keep each scope separate.",
                )
            })?;
            let expected = detected
                .authentication()
                .clone()
                .with_oauth_scopes(scopes.clone(), format);
            let submitted = method.with_oauth_scopes(scopes, format);
            if submitted != editable_authentication(&expected.clone().into())
                && submitted != expected
            {
                return Err(unsupported());
            }
            expected
        }
        _ => return Err(unsupported()),
    };
    Ok(ProviderConfiguration {
        authentication: Some(contract.with_client_policy(policy)),
        request_headers: values.request_headers,
        credential_fields: BTreeMap::new(),
        servers: BTreeMap::from([("#/servers".into(), ServerSelection::Custom { url: source })]),
        display_name,
    })
}

fn valid_client(
    policy: &coral_catalog::OAuthClientPolicy,
    method: &AuthenticationConfiguration,
) -> bool {
    match method {
        AuthenticationConfiguration::OAuthDeviceCode { .. } => policy.validate_device().is_ok(),
        AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url, ..
        } => policy.validate(true, authorization_url).is_ok(),
        _ => false,
    }
}
