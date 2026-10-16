//! Shared authentication validation for document defaults and confirmed configuration.
use crate::CatalogDiagnostic;

pub(super) fn header(value: &str) -> Result<String, CatalogDiagnostic> {
    let header = value.to_ascii_lowercase();
    if header.is_empty()
        || header.len() > 128
        || !header
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || matches!(
            header.as_str(),
            "host" | "content-length" | "transfer-encoding" | "connection" | "cookie"
        )
    {
        return Err(CatalogDiagnostic::stable(
            "credentials_not_supported",
            "the Provider requires an unsupported or ambiguous credential contract",
        ));
    }
    Ok(header)
}

/// The most headers, or query parameters, one method sends credentials in.
pub(super) const MAX_BINDINGS: usize = 16;
/// The longest label, in bytes, a Credential Field can have.
pub(super) const MAX_FIELD_LABEL_BYTES: usize = 120;
/// The longest description, in bytes, a Credential Field can have.
pub(super) const MAX_FIELD_DESCRIPTION_BYTES: usize = 1000;

/// A query parameter Coral fills with a credential: plain text, sent exactly as named.
pub(super) fn query_parameter(value: &str) -> Result<String, CatalogDiagnostic> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(CatalogDiagnostic::stable(
            "credentials_not_supported",
            "the Provider requires an unsupported or ambiguous credential contract",
        ));
    }
    Ok(value.to_owned())
}

/// A Credential Field name: a lowercase letter, then lowercase letters, digits or
/// underscores, at most 64 bytes.
pub(crate) fn is_field_name(name: &str) -> bool {
    name.len() <= 64
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Names a Credential Field after a security scheme, so `apiKeyAuth` becomes `api_key_auth`.
pub(super) fn field_name_for(scheme: &str) -> Option<String> {
    let characters: Vec<char> = scheme.chars().collect();
    let mut name = String::new();
    for (index, character) in characters.iter().enumerate() {
        if character.is_ascii_uppercase() {
            // Break before a capital that starts a word, so APIKey reads api_key.
            let previous = index
                .checked_sub(1)
                .and_then(|before| characters.get(before))
                .copied();
            let next = characters.get(index + 1);
            let starts_word = previous.is_some_and(|previous| {
                previous.is_ascii_lowercase()
                    || previous.is_ascii_digit()
                    || (previous.is_ascii_uppercase() && next.is_some_and(char::is_ascii_lowercase))
            });
            if starts_word && !name.is_empty() && !name.ends_with('_') {
                name.push('_');
            }
            name.push(character.to_ascii_lowercase());
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            name.push(*character);
        } else if !name.is_empty() && !name.ends_with('_') {
            name.push('_');
        }
    }
    let name = name.trim_end_matches('_').to_owned();
    is_field_name(&name).then_some(name)
}

pub(super) fn scopes(values: &[String]) -> Result<Vec<String>, CatalogDiagnostic> {
    crate::OAuthScopeFormat::Space
        .normalize(values)
        .map_err(|_invalid| invalid())
}

pub(super) fn endpoint(value: &str) -> Result<String, CatalogDiagnostic> {
    // Shape validation only. Materialization applies the resolved outbound
    // policy to the resulting credential contract's destinations.
    if !crate::oauth_endpoint_is_valid(value) {
        return Err(invalid());
    }
    url::Url::parse(value)
        .map(|url| url.to_string())
        .map_err(|_error| invalid())
}

fn invalid() -> CatalogDiagnostic {
    CatalogDiagnostic::stable(
        "invalid_credential_contract",
        "an OpenAPI Security Scheme or reference is invalid",
    )
}
