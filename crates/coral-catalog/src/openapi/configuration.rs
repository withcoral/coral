//! Owner-confirmed inputs remain separate from the exact accepted document.
use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::materialize::{
    api_keys, parse_document, provider_display_name, select_credentials, validate_version,
};
use super::operations::locate_operations;
use super::schema::validate_root_dialect;
use crate::{
    AuthenticationConfiguration, CatalogDiagnostic, ConfigurationFieldError, CredentialField,
    CredentialSource, HeaderBinding, OpenApiDocument, ProviderConfiguration,
    ProviderConfigurationReview, ServerGroup, ServerOption, ServerSelection, ServerVariable,
};

impl OpenApiDocument {
    /// Inspects defaults without modifying or confirming the source.
    /// # Errors
    /// Returns a safe diagnostic for structurally invalid input.
    #[expect(
        clippy::too_many_lines,
        reason = "inspection groups inherited declarations and their Tools in one traversal"
    )]
    pub fn configuration_review(&self) -> Result<ProviderConfigurationReview, CatalogDiagnostic> {
        let document = parse_document(self)?;
        let root = document.as_object().ok_or_else(invalid_document)?;
        validate_root_dialect(&document, validate_version(root)?)?;
        let located = locate_operations(root, &document)?;
        let mut groups = located
            .servers
            .iter()
            .map(|(declaration, object)| (declaration.clone(), server_group(declaration, object)))
            .collect::<BTreeMap<_, _>>();
        let mut missing = false;
        let mut explicit = false;
        let mut credentialed = false;
        let mut ambiguous = false;
        let declared_schemes = document.pointer("/components/securitySchemes");
        let mut unsupported = declared_schemes.is_some_and(|schemes| !schemes.is_object());
        let mut selected_contract: Option<AuthenticationConfiguration> = None;
        let mut selected_fields = BTreeMap::new();
        for located in located.operations {
            let operation = &located.object;
            let security = operation.get("security").or(root.get("security"));
            missing |= security.is_none();
            explicit |= security.is_some();
            credentialed |= security
                .and_then(Value::as_array)
                .is_some_and(|requirements| {
                    requirements.iter().any(|requirement| {
                        requirement
                            .as_object()
                            .is_some_and(|requirement| !requirement.is_empty())
                    })
                });
            // Schemes within one requirement are all required, so only separate
            // requirements are alternatives the owner must choose between.
            let alternatives = security
                .and_then(Value::as_array)
                .is_some_and(|requirements| requirements.len() > 1);
            ambiguous |= alternatives;
            if security.is_some() && !alternatives {
                match select_credentials(security, declared_schemes) {
                    Ok(slots) => {
                        let contract = slots
                            .first()
                            .map_or(AuthenticationConfiguration::None, |slot| {
                                slot.contract().authentication().clone()
                            });
                        ambiguous |= selected_contract
                            .as_ref()
                            .is_some_and(|selected| selected != &contract);
                        if selected_contract.is_none()
                            && matches!(
                                contract,
                                AuthenticationConfiguration::HeaderCredentials { .. }
                                    | AuthenticationConfiguration::QueryCredentials { .. }
                            )
                            && let Some(requirement) = security
                                .and_then(Value::as_array)
                                .and_then(|requirements| requirements.first())
                                .and_then(Value::as_object)
                            && let Ok(keys) = api_keys(requirement, declared_schemes)
                        {
                            selected_fields = keys
                                .keys
                                .into_iter()
                                .map(|key| {
                                    (
                                        key.field,
                                        CredentialField {
                                            label: key.scheme,
                                            description: key.description,
                                            sensitive: true,
                                        },
                                    )
                                })
                                .collect();
                        }
                        if selected_contract.is_none()
                            && matches!(contract, AuthenticationConfiguration::BasicAuth { .. })
                        {
                            // A document names neither part, so both get plain labels.
                            let field = |label: &str, sensitive| CredentialField {
                                label: label.into(),
                                description: String::new(),
                                sensitive,
                            };
                            selected_fields = BTreeMap::from([
                                ("username".into(), field("Username", false)),
                                ("password".into(), field("Password", true)),
                            ]);
                        }
                        selected_contract.get_or_insert(contract);
                    }
                    Err(_) => unsupported = true,
                }
            }
            if let Some(group) = groups.get_mut(&located.server_declaration) {
                group.tools.push(located.key.as_str().into());
            }
        }
        let has_schemes = document
            .pointer("/components/securitySchemes")
            .and_then(Value::as_object)
            .is_some_and(|schemes| !schemes.is_empty());
        let (authentication, status) = if ambiguous || (missing && credentialed) {
            (None, "ambiguous")
        } else if unsupported {
            (None, "unsupported")
        } else if missing || !explicit {
            (
                None,
                if has_schemes && !explicit {
                    "unused"
                } else {
                    "missing"
                },
            )
        } else {
            match selected_contract {
                Some(AuthenticationConfiguration::None) | None => (
                    Some(AuthenticationConfiguration::None),
                    "explicit_anonymous",
                ),
                Some(contract) => (Some(contract), "supported"),
            }
        };
        let mut servers = groups.into_values().collect::<Vec<_>>();
        servers.sort_by(|left, right| {
            (left.declaration != "#/servers", &left.declaration)
                .cmp(&(right.declaration != "#/servers", &right.declaration))
        });
        let selections = servers
            .iter()
            .filter_map(|group| {
                let [option] = group.options.as_slice() else {
                    return None;
                };
                let variables = option
                    .variables
                    .iter()
                    .filter_map(|(name, variable)| {
                        variable
                            .default
                            .as_ref()
                            .map(|value| (name.clone(), value.clone()))
                    })
                    .collect();
                let selection = ServerSelection::Declared {
                    url: option.url.clone(),
                    variables,
                };
                resolve_selection(group, Some(&selection)).ok()?;
                Some((group.declaration.clone(), selection))
            })
            .collect();
        Ok(ProviderConfigurationReview {
            suggested: ProviderConfiguration {
                request_headers: std::collections::BTreeMap::new(),
                credential_fields: if matches!(
                    authentication,
                    Some(
                        AuthenticationConfiguration::HeaderCredentials { .. }
                            | AuthenticationConfiguration::BasicAuth { .. }
                            | AuthenticationConfiguration::QueryCredentials { .. }
                    )
                ) {
                    selected_fields
                } else {
                    BTreeMap::new()
                },
                authentication,
                servers: selections,
                display_name: provider_display_name(root).unwrap_or_default(),
            },
            authentication_status: status.into(),
            servers,
        })
    }

    /// Validates explicit configuration independently of the pinned source.
    /// Unresolved Tool-specific declarations stay local catalog diagnostics.
    /// # Errors
    /// Returns safe field errors, leaving this document and accepted settings intact.
    pub fn configure(
        &self,
        configuration: ProviderConfiguration,
    ) -> Result<Self, Vec<ConfigurationFieldError>> {
        let review = self.configuration_review().ok();
        let configuration = validate_openapi_configuration(review.as_ref(), configuration)?;
        let mut configured = self.clone();
        configured.configuration = Some(Box::new(configuration));
        Ok(configured)
    }

    /// Restores secret-free confirmation evidence retained separately from the source.
    /// # Errors
    /// Rejects malformed stored configuration or invalid revision evidence.
    pub fn restore_configuration(
        mut self,
        value: &str,
        revision: i64,
    ) -> Result<Self, crate::Error> {
        if revision < 1 {
            return Err(crate::Error::CorruptData(
                "invalid configuration revision".into(),
            ));
        }
        self.configuration = Some(serde_json::from_str(value).map_err(|_invalid| {
            crate::Error::CorruptData("invalid provider configuration".into())
        })?);
        self.configuration_revision = revision;
        Ok(self)
    }

    /// Separately confirmed settings, if the owner has reviewed them.
    #[must_use]
    pub fn configuration(&self) -> Option<&ProviderConfiguration> {
        self.configuration.as_deref()
    }
}

/// Validates choices against the source analysis of the exact pinned OpenAPI input.
/// Pass `None` when source analysis returned a structural diagnostic; that failure
/// remains a Publication outcome rather than a configuration field error.
/// # Errors
/// Returns safe field errors without accepting invalid choices.
pub fn validate_openapi_configuration(
    review: Option<&ProviderConfigurationReview>,
    mut configuration: ProviderConfiguration,
) -> Result<ProviderConfiguration, Vec<ConfigurationFieldError>> {
    let mut errors = Vec::new();
    for selection in configuration.servers.values_mut() {
        if let ServerSelection::Custom { url } = selection
            && let Ok(normalized) = normalize_base_url(url)
        {
            *url = normalized;
        }
    }
    match validate_authentication(configuration.authentication.as_ref()) {
        Ok(contract) => configuration.authentication = Some(contract),
        Err(problems) => errors.extend(problems),
    }
    errors.extend(validate_credential_fields(&mut configuration));
    if let Err(problems) = configuration.normalize_request_headers(false) {
        errors.extend(problems);
    }
    if let Err(problem) = crate::configuration::normalize_display_name(&mut configuration) {
        errors.push(problem);
    }
    // Structural source failures remain terminal Publication diagnostics. They
    // do not turn configuration confirmation into a second publication lifecycle.
    if review.is_none() && !configuration.servers.is_empty() {
        errors.push(field_error(
            "servers",
            "Remove server selections that this document cannot apply.",
        ));
    }
    for declaration in configuration.servers.keys() {
        if review.as_ref().is_some_and(|review| {
            !review
                .servers
                .iter()
                .any(|group| &group.declaration == declaration)
        }) {
            errors.push(field_error(
                "servers",
                "Review removed or changed server declarations.",
            ));
        }
    }
    for group in review.iter().flat_map(|review| &review.servers) {
        let selected = configuration.servers.get(&group.declaration);
        // Missing local configuration is previewed as unavailable, not replaced by the root.
        if (selected.is_some() || (group.declaration == "#/servers" && !group.tools.is_empty()))
            && resolve_selection(group, selected).is_err()
        {
            errors.push(field_error(
                &group.declaration,
                "Select a server and complete its declared variables, or supply a valid base URL.",
            ));
        }
    }
    if !errors.is_empty() {
        errors.truncate(100);
        return Err(errors);
    }
    Ok(configuration)
}

fn validate_authentication(
    contract: Option<&AuthenticationConfiguration>,
) -> Result<AuthenticationConfiguration, Vec<ConfigurationFieldError>> {
    use super::authentication;
    use AuthenticationConfiguration as Auth;
    match contract {
        Some(contract @ (Auth::None | Auth::Bearer)) => Ok(contract.clone()),
        Some(Auth::HeaderCredentials { headers }) => header_credentials(headers),
        Some(Auth::BasicAuth { username, password }) => basic_auth(username, password),
        Some(Auth::QueryCredentials { parameters }) => query_credentials(parameters),
        Some(Auth::OAuthDeviceCode {
            device_authorization_url,
            token_url,
            scopes,
            scope_format,
            client,
            ..
        }) => device_authentication(
            device_authorization_url,
            token_url,
            scopes,
            *scope_format,
            client.as_ref(),
        ),

        Some(Auth::OAuthAuthorizationCode {
            authorization_url,
            token_url,
            scopes,
            scope_format,
            resource: None,
            issuer: None,
            client,
        }) => {
            let mut errors = Vec::new();
            let authorization_url = authentication::endpoint(authorization_url).map_err(|_invalid| {
                errors.push(field_error("authentication.authorization_url", "Enter a valid authorization URL without credentials or OAuth request parameters."));
            });
            let token_url = authentication::endpoint(token_url).map_err(|_invalid| {
                errors.push(field_error(
                    "authentication.token_url",
                    "Enter a valid token URL without credentials or OAuth request parameters.",
                ));
            });
            let scopes = scope_format.normalize(scopes).map_err(|_invalid| {
                errors.push(field_error("authentication.scopes", "Enter up to 100 valid scopes. Use at most 256 ASCII characters for each scope."));
            });
            if client.as_ref().is_some_and(|policy| {
                policy
                    .validate(false, authorization_url.as_ref().map_or("", String::as_str))
                    .is_err()
            }) {
                errors.push(field_error(
                    "application",
                    "Review the OAuth application, PKCE policy and authorization parameters.",
                ));
            }
            if !errors.is_empty() {
                return Err(errors);
            }
            match (authorization_url, token_url, scopes) {
                (Ok(authorization_url), Ok(token_url), Ok(scopes)) => {
                    Ok(Auth::OAuthAuthorizationCode {
                        authorization_url,
                        token_url,
                        scopes,
                        scope_format: *scope_format,
                        resource: None,
                        issuer: None,
                        client: client.clone(),
                    })
                }
                _ => unreachable!("invalid authentication fields have errors"),
            }
        }
        _ => Err(vec![field_error(
            "authentication",
            "Choose a supported authentication method.",
        )]),
    }
}

/// Header names in lowercase, each bound to a Credential Field or a literal.
fn header_credentials(
    headers: &BTreeMap<String, HeaderBinding>,
) -> Result<AuthenticationConfiguration, Vec<ConfigurationFieldError>> {
    use super::authentication;
    if headers.is_empty() || headers.len() > authentication::MAX_BINDINGS {
        return Err(vec![field_error(
            "authentication.headers",
            &format!(
                "Send the credentials in between 1 and {} headers.",
                authentication::MAX_BINDINGS
            ),
        )]);
    }
    // A header carries only visible ASCII and spaces; anything else would pass here
    // and then fail every call.
    let text = |value: &str, max: usize, empty: bool| {
        (empty || !value.is_empty())
            && value.len() <= max
            && value.bytes().all(|byte| (b' '..=b'~').contains(&byte))
    };
    let mut errors = Vec::new();
    let mut normalized = BTreeMap::new();
    for (name, binding) in headers {
        let path = format!("authentication.headers.{name}");
        let Ok(header) = authentication::header(name) else {
            errors.push(field_error(&path, "Enter a supported HTTP header name."));
            continue;
        };
        let valid = match binding {
            // Field names are checked with the fields they name.
            HeaderBinding::Field { prefix, .. } => text(prefix, 256, true),
            HeaderBinding::Literal { value } => text(value, 4096, false),
        };
        if !valid {
            errors.push(field_error(
                &path,
                "Bind the header to a credential field with a prefix, or to a value. Use only unaccented letters, digits, spaces and symbols.",
            ));
        } else if normalized.insert(header, binding.clone()).is_some() {
            errors.push(field_error(&path, "Send each header once."));
        }
    }
    if errors.is_empty() {
        Ok(AuthenticationConfiguration::HeaderCredentials {
            headers: normalized,
        })
    } else {
        Err(errors)
    }
}

/// Query parameter names exactly as sent, each bound to a Credential Field or a literal.
fn query_credentials(
    parameters: &BTreeMap<String, CredentialSource>,
) -> Result<AuthenticationConfiguration, Vec<ConfigurationFieldError>> {
    use super::authentication;
    if parameters.is_empty() || parameters.len() > authentication::MAX_BINDINGS {
        return Err(vec![field_error(
            "authentication.parameters",
            &format!(
                "Send the credentials in between 1 and {} query parameters.",
                authentication::MAX_BINDINGS
            ),
        )]);
    }
    let mut errors = Vec::new();
    for (name, source) in parameters {
        let path = format!("authentication.parameters.{name}");
        if authentication::query_parameter(name).is_err() {
            errors.push(field_error(&path, "Enter a plain query parameter name."));
        } else if let CredentialSource::Literal { value } = source
            && (value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control))
        {
            // An empty value says nothing and sends nothing, as for a credential header.
            errors.push(field_error(
                &path,
                "Send a credential field, or a plain value, in the parameter.",
            ));
        }
    }
    if errors.is_empty() {
        Ok(AuthenticationConfiguration::QueryCredentials {
            parameters: parameters.clone(),
        })
    } else {
        Err(errors)
    }
}

/// Plain username and password parts. Basic splits the pair at its first colon, so
/// a username cannot contain one.
fn basic_auth(
    username: &CredentialSource,
    password: &CredentialSource,
) -> Result<AuthenticationConfiguration, Vec<ConfigurationFieldError>> {
    // Field names are checked with the fields they name.
    let plain = |value: &str| value.len() <= 4096 && !value.chars().any(char::is_control);
    let mut errors = Vec::new();
    // Mailgun's username is the constant `api`, so a username may be a value.
    if let CredentialSource::Literal { value } = username
        && (!plain(value) || value.contains(':'))
    {
        errors.push(field_error(
            "authentication.username",
            "Send a credential field, or a plain value without a colon, as the username.",
        ));
    }
    // A constant password would put a secret in stored configuration, so the only
    // value a password may hold is none at all: Stripe sends a key and no password.
    if let CredentialSource::Literal { value } = password
        && !value.is_empty()
    {
        errors.push(field_error(
            "authentication.password",
            "Send a credential field as the password, or leave it empty.",
        ));
    }
    if errors.is_empty() {
        Ok(AuthenticationConfiguration::BasicAuth {
            username: username.clone(),
            password: password.clone(),
        })
    } else {
        Err(errors)
    }
}

/// Every Credential Field the method sends is presented, and only those. Methods that
/// name their own fields, or none, keep no presentation.
fn validate_credential_fields(
    configuration: &mut ProviderConfiguration,
) -> Vec<ConfigurationFieldError> {
    // Each place a field is sent, so an undeclared one is reported where it is used.
    let uses: Vec<(String, &str)> = match configuration.authentication.as_ref() {
        Some(AuthenticationConfiguration::HeaderCredentials { headers }) => headers
            .iter()
            .filter_map(|(header, binding)| match binding {
                HeaderBinding::Field { field, .. } => {
                    Some((format!("authentication.headers.{header}"), field.as_str()))
                }
                HeaderBinding::Literal { .. } => None,
            })
            .collect(),
        Some(AuthenticationConfiguration::QueryCredentials { parameters }) => parameters
            .iter()
            .filter_map(|(name, source)| match source {
                CredentialSource::Field { field } => {
                    Some((format!("authentication.parameters.{name}"), field.as_str()))
                }
                CredentialSource::Literal { .. } => None,
            })
            .collect(),
        Some(AuthenticationConfiguration::BasicAuth { username, password }) => [
            ("authentication.username", username),
            ("authentication.password", password),
        ]
        .into_iter()
        .filter_map(|(path, part)| match part {
            CredentialSource::Field { field } => Some((path.to_owned(), field.as_str())),
            CredentialSource::Literal { .. } => None,
        })
        .collect(),
        _ => {
            configuration.credential_fields.clear();
            return Vec::new();
        }
    };
    let mut errors = Vec::new();
    for (path, field) in uses {
        if !configuration.credential_fields.contains_key(field) {
            errors.push(field_error(&path, "Send a declared credential field."));
        }
    }
    let bound = configuration
        .authentication
        .as_ref()
        .map(AuthenticationConfiguration::credential_fields)
        .unwrap_or_default();
    if bound.is_empty() {
        errors.push(field_error(
            "authentication.fields",
            "Send at least one credential field.",
        ));
    }
    for (name, field) in &configuration.credential_fields {
        let presentable = !field.label.trim().is_empty()
            && field.label.len() <= super::authentication::MAX_FIELD_LABEL_BYTES
            && !field.label.chars().any(char::is_control)
            && field.description.len() <= super::authentication::MAX_FIELD_DESCRIPTION_BYTES
            && !field.description.chars().any(char::is_control);
        if !super::authentication::is_field_name(name)
            || !bound.contains(name.as_str())
            || !presentable
        {
            errors.push(field_error(
                &format!("authentication.fields.{name}"),
                "Name each field in lowercase, give it a label, and send it.",
            ));
        }
    }
    errors
}

fn server_group(declaration: &str, object: &Map<String, Value>) -> ServerGroup {
    let options = object
        .get("servers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|server| {
            let template = server
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // Invalid alternatives remain visible as unresolved choices, but URL
            // credentials and query material must never become review output.
            if template.len() > 2048
                || template.chars().any(char::is_control)
                || template.contains(['@', '?', '#'])
            {
                return ServerOption {
                    url: String::new(),
                    variables: BTreeMap::new(),
                };
            }
            let url = template.to_owned();
            let variables = server
                .get("variables")
                .and_then(Value::as_object)
                .into_iter()
                .flat_map(|variables| variables.iter())
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ServerVariable {
                            default: value
                                .get("default")
                                .and_then(Value::as_str)
                                .filter(|value| safe_variable_value(value))
                                .map(str::to_owned),
                            // Never turn a malformed enum into an unrestricted or
                            // partially accepted variable domain. Empty means unresolved.
                            enumeration: value.get("enum").map(|values| {
                                values
                                    .as_array()
                                    .and_then(|values| {
                                        values
                                            .iter()
                                            .map(|value| {
                                                value
                                                    .as_str()
                                                    .filter(|value| safe_variable_value(value))
                                                    .map(str::to_owned)
                                            })
                                            .collect::<Option<Vec<_>>>()
                                    })
                                    .unwrap_or_default()
                            }),
                        },
                    )
                })
                .collect();
            ServerOption { url, variables }
        })
        .collect();
    ServerGroup {
        declaration: declaration.into(),
        tools: Vec::new(),
        options,
    }
}

pub(super) fn configured_server(
    object: &Map<String, Value>,
    declaration: &str,
    configuration: &ProviderConfiguration,
) -> Result<Option<String>, CatalogDiagnostic> {
    resolve_selection(
        &server_group(declaration, object),
        configuration.servers.get(declaration),
    )
    .map(Some)
}

fn resolve_selection(
    group: &ServerGroup,
    selection: Option<&ServerSelection>,
) -> Result<String, CatalogDiagnostic> {
    let expanded = match selection {
        Some(ServerSelection::Custom { url }) => url.clone(),
        Some(ServerSelection::Declared { url, variables }) => {
            // A document may declare the same server template more than once, as
            // Datadog's does. Those options resolve alike, so any one the chosen
            // values complete answers for the whole group.
            let option = group
                .options
                .iter()
                .filter(|option| &option.url == url)
                .find(|option| {
                    variables.len() == option.variables.len()
                        && option.variables.iter().all(|(name, variable)| {
                            variables.get(name).is_some_and(|value| {
                                safe_variable_value(value)
                                    && variable
                                        .enumeration
                                        .as_ref()
                                        .is_none_or(|values| values.contains(value))
                            })
                        })
                })
                .ok_or_else(invalid_server)?;
            let mut expanded = url.clone();
            for name in option.variables.keys() {
                let value = variables.get(name).ok_or_else(invalid_server)?;
                expanded = expanded.replace(&format!("{{{name}}}"), value);
            }
            expanded
        }
        None => return Err(invalid_server()),
    };
    normalize_base_url(&expanded)
}

fn safe_variable_value(value: &str) -> bool {
    value.len() <= 2048
        && !value.chars().any(char::is_control)
        && !value.contains(['{', '}', '?', '#', '@', '\\'])
}

pub(super) fn normalize_base_url(value: &str) -> Result<String, CatalogDiagnostic> {
    if value.len() > 2048 || value.chars().any(char::is_control) || value.contains(['{', '}', '\\'])
    {
        return Err(invalid_server());
    }
    let mut url = url::Url::parse(value).map_err(|_invalid| invalid_server())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid_server());
    }
    let path = url.path().trim_end_matches('/').to_owned();
    if !path.is_empty() {
        url.set_path(&path);
    }
    Ok(url.to_string())
}

fn invalid_server() -> CatalogDiagnostic {
    CatalogDiagnostic::stable(
        "unresolved_server_configuration",
        "review the selected server and its declared variables",
    )
}
fn invalid_document() -> CatalogDiagnostic {
    CatalogDiagnostic::stable("invalid_openapi_document", "review the OpenAPI document")
}
fn field_error(field: &str, message: &str) -> ConfigurationFieldError {
    ConfigurationFieldError {
        field: if field.len() <= 256 && !field.chars().any(char::is_control) {
            field.into()
        } else {
            "servers".into()
        },
        message: message.into(),
    }
}

fn device_authentication(
    device_authorization_url: &str,
    token_url: &str,
    scopes: &[String],
    scope_format: crate::OAuthScopeFormat,
    client: Option<&crate::OAuthClientPolicy>,
) -> Result<AuthenticationConfiguration, Vec<ConfigurationFieldError>> {
    let mut errors = Vec::new();
    let mut endpoint = |value: &str, field| {
        super::authentication::endpoint(value).map_err(|_error| {
            errors.push(field_error(field, "Enter a valid OAuth endpoint URL."));
        })
    };
    let device_authorization_url = endpoint(
        device_authorization_url,
        "authentication.device_authorization_url",
    );
    let token_url = endpoint(token_url, "authentication.token_url");
    let scopes = scope_format.normalize(scopes).map_err(|_error| {
        errors.push(field_error("authentication.scopes", "Enter valid scopes."));
    });
    if client.is_some_and(|client| client.validate_device().is_err()) {
        errors.push(field_error(
            "application",
            "Review the device OAuth application.",
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    match (device_authorization_url, token_url, scopes) {
        (Ok(device_authorization_url), Ok(token_url), Ok(scopes)) => {
            Ok(AuthenticationConfiguration::OAuthDeviceCode {
                device_authorization_url,
                resource: None,
                issuer: None,
                token_url,
                scopes,
                scope_format,
                client: client.cloned(),
            })
        }
        _ => unreachable!("invalid authentication fields have errors"),
    }
}
