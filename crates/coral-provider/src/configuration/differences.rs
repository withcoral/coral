//! Safe field-level comparison of saved choices with replacement input defaults.
use coral_catalog::{AuthenticationConfiguration, CredentialSource, HeaderBinding};
use coral_catalog::{ProviderConfiguration, ServerSelection};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// One changed, removed, missing or ambiguous input default. No rejected input is echoed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigurationDifference {
    /// Stable form-field address, not a private identity or account address.
    pub field: String,
    /// Previous confirmed value, if any.
    pub saved: Option<String>,
    /// Replacement document suggestion; absent means no unambiguous default.
    pub document: Option<String>,
}

pub(super) fn compare(
    saved: &ProviderConfiguration,
    document: &ProviderConfiguration,
) -> Vec<ConfigurationDifference> {
    let mut differences = Vec::new();
    let mut push = |field: String, saved: Option<String>, document: Option<String>| {
        if saved != document && differences.len() < 100 {
            differences.push(ConfigurationDifference {
                field,
                saved,
                document,
            });
        }
    };
    push(
        "display_name".into(),
        Some(saved.display_name.clone()),
        Some(document.display_name.clone()),
    );
    for name in saved
        .request_headers
        .keys()
        .chain(document.request_headers.keys())
        .collect::<BTreeSet<_>>()
    {
        push(
            format!("request_headers.{name}"),
            saved.request_headers.get(name).cloned(),
            document.request_headers.get(name).cloned(),
        );
    }
    let previous = saved.authentication.as_ref();
    let next = document.authentication.as_ref();
    push(
        "authentication.method".into(),
        previous.map(method),
        next.map(method),
    );
    for field in ["authorization_url", "token_url", "scopes", "scope_format"] {
        push(
            format!("authentication.{field}"),
            oauth_field(previous, field),
            oauth_field(next, field),
        );
    }
    for (name, part) in [
        ("username", BasicPart::Username),
        ("password", BasicPart::Password),
    ] {
        push(
            format!("authentication.{name}"),
            basic_part(previous, part),
            basic_part(next, part),
        );
    }
    let (old_parameters, new_parameters) = (query_parameters(previous), query_parameters(next));
    for name in old_parameters
        .keys()
        .chain(new_parameters.keys())
        .collect::<BTreeSet<_>>()
    {
        push(
            format!("authentication.parameters.{name}"),
            old_parameters.get(name).cloned(),
            new_parameters.get(name).cloned(),
        );
    }
    let (old_headers, new_headers) = (credential_headers(previous), credential_headers(next));
    for header in old_headers
        .keys()
        .chain(new_headers.keys())
        .collect::<BTreeSet<_>>()
    {
        push(
            format!("authentication.headers.{header}"),
            old_headers.get(header).cloned(),
            new_headers.get(header).cloned(),
        );
    }
    let declarations = saved
        .servers
        .keys()
        .chain(document.servers.keys())
        .collect::<BTreeSet<_>>();
    for declaration in declarations {
        let old = saved.servers.get(declaration);
        let new = document.servers.get(declaration);
        push(
            format!("{declaration}.url"),
            old.map(server_url),
            new.map(server_url),
        );
        let variables = server_variables(old)
            .chain(server_variables(new))
            .collect::<BTreeSet<_>>();
        for name in variables {
            push(
                format!("{declaration}.variables.{name}"),
                variable(old, name),
                variable(new, name),
            );
        }
    }
    differences
}
/// Each credential header and what it sends: a field as `<name>` after its prefix,
/// or a literal as written. Field values never appear here.
fn credential_headers(contract: Option<&AuthenticationConfiguration>) -> BTreeMap<String, String> {
    let Some(AuthenticationConfiguration::HeaderCredentials { headers }) = contract else {
        return BTreeMap::new();
    };
    headers
        .iter()
        .map(|(header, binding)| {
            let sent = match binding {
                HeaderBinding::Field { field, prefix } => format!("{prefix}<{field}>"),
                HeaderBinding::Literal { value } => value.clone(),
            };
            (header.clone(), sent)
        })
        .collect()
}

/// Each credential query parameter and what it sends, as for Basic parts.
fn query_parameters(contract: Option<&AuthenticationConfiguration>) -> BTreeMap<String, String> {
    let Some(AuthenticationConfiguration::QueryCredentials { parameters }) = contract else {
        return BTreeMap::new();
    };
    parameters
        .iter()
        .map(|(name, source)| (name.clone(), sent(source)))
        .collect()
}

fn sent(source: &CredentialSource) -> String {
    match source {
        CredentialSource::Field { field } => format!("<{field}>"),
        CredentialSource::Literal { value } => value.clone(),
    }
}

/// The part of the pair a difference names.
#[derive(Clone, Copy)]
enum BasicPart {
    Username,
    Password,
}

/// One HTTP Basic part: a field as `<name>`, or a literal as written.
fn basic_part(contract: Option<&AuthenticationConfiguration>, part: BasicPart) -> Option<String> {
    let Some(AuthenticationConfiguration::BasicAuth { username, password }) = contract else {
        return None;
    };
    Some(sent(match part {
        BasicPart::Username => username,
        BasicPart::Password => password,
    }))
}

fn method(contract: &AuthenticationConfiguration) -> String {
    contract.kind().name().into()
}
fn oauth_field(contract: Option<&AuthenticationConfiguration>, field: &str) -> Option<String> {
    if let Some((scopes, format)) = contract.and_then(AuthenticationConfiguration::oauth_scopes) {
        match field {
            "scopes" => return Some(scopes.join(" ")),
            "scope_format" => {
                return Some(
                    match format {
                        coral_catalog::OAuthScopeFormat::Space => "space",
                        coral_catalog::OAuthScopeFormat::Comma => "comma",
                    }
                    .into(),
                );
            }
            _ => {}
        }
    }
    match contract {
        Some(AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url,
            token_url,
            scopes,
            ..
        }) => match field {
            "authorization_url" => Some(authorization_url.clone()),
            "token_url" => Some(token_url.clone()),
            "scopes" => Some(scopes.join(" ")),
            _ => None,
        },
        _ => None,
    }
}
fn server_url(selection: &ServerSelection) -> String {
    match selection {
        ServerSelection::Declared { url, .. } | ServerSelection::Custom { url } => url.clone(),
    }
}
fn server_variables(selection: Option<&ServerSelection>) -> impl Iterator<Item = &String> {
    match selection {
        Some(ServerSelection::Declared { variables, .. }) => Some(variables.keys()),
        _ => None,
    }
    .into_iter()
    .flatten()
}
fn variable(selection: Option<&ServerSelection>, name: &str) -> Option<String> {
    match selection {
        Some(ServerSelection::Declared { variables, .. }) => variables.get(name).cloned(),
        _ => None,
    }
}
