//! Safe projection of the response headers an HTTP Tool Result retains.
//!
//! A Result reaches Programs, MCP callers, and durable Run and Invocation
//! records, so it keeps every header name and pair but no credential that an
//! upstream returned or echoed.
//!
//! `finalUrl` and redirect facts drop their whole query because they are
//! Coral's own evidence. A header URL is provider data a Program may follow,
//! such as a pagination `Link`, so it keeps every query pair except a
//! sensitive value.

use coral_credentials::Secret;

const REDACTED: &str = "[redacted]";

/// How a response header's value is made safe.
#[derive(Clone, Copy)]
enum HeaderShape {
    /// The whole value is a credential or an authentication challenge.
    Secret,
    /// The value is one URI reference.
    Reference,
    /// The value is a list of `<URI-reference>; parameters` links.
    Link,
    /// The value is `<seconds>; [url=]<URI-reference>`.
    Refresh,
}

const HEADERS: &[(&str, HeaderShape)] = &[
    ("authentication-info", HeaderShape::Secret),
    ("authorization", HeaderShape::Secret),
    ("cookie", HeaderShape::Secret),
    ("proxy-authenticate", HeaderShape::Secret),
    ("proxy-authentication-info", HeaderShape::Secret),
    ("proxy-authorization", HeaderShape::Secret),
    ("set-cookie", HeaderShape::Secret),
    ("www-authenticate", HeaderShape::Secret),
    ("x-api-key", HeaderShape::Secret),
    ("x-auth-token", HeaderShape::Secret),
    ("content-location", HeaderShape::Reference),
    ("location", HeaderShape::Reference),
    ("link", HeaderShape::Link),
    ("refresh", HeaderShape::Refresh),
];

/// Query parameters that carry a credential in common URL schemes.
const SENSITIVE_PARAMETERS: &[&str] = &[
    "access_token",
    "api_key",
    "apikey",
    "client_secret",
    "code",
    "id_token",
    "key",
    "password",
    "refresh_token",
    "secret",
    "sig",
    "signature",
    "token",
    "x-amz-credential",
    "x-amz-security-token",
    "x-amz-signature",
    "x-goog-credential",
    "x-goog-signature",
];

/// The header and query names under which Coral sent credentials, which an
/// upstream may echo back.
pub(super) struct CredentialNames {
    headers: Vec<String>,
    parameters: Vec<String>,
}

impl CredentialNames {
    pub(super) fn new(headers: &[(String, Secret)], parameters: &[(String, Secret)]) -> Self {
        let names = |pairs: &[(String, Secret)]| {
            pairs
                .iter()
                .map(|(name, _secret)| name.clone())
                .collect::<Vec<_>>()
        };
        Self {
            headers: names(headers),
            parameters: names(parameters),
        }
    }

    fn header_value(&self, name: &str) -> Option<HeaderShape> {
        if contains(&self.headers, name) {
            return Some(HeaderShape::Secret);
        }
        HEADERS
            .iter()
            .find(|(header, _value)| header.eq_ignore_ascii_case(name))
            .map(|(_header, value)| *value)
    }

    fn sensitive_parameter(&self, encoded: &str) -> bool {
        let name = url::form_urlencoded::parse(encoded.as_bytes())
            .next()
            .map_or_else(|| encoded.into(), |(name, _value)| name);
        SENSITIVE_PARAMETERS
            .iter()
            .any(|parameter| parameter.eq_ignore_ascii_case(&name))
            || contains(&self.parameters, &name)
    }
}

fn contains(names: &[String], name: &str) -> bool {
    names.iter().any(|known| known.eq_ignore_ascii_case(name))
}

/// Returns every response-header pair in order, with sensitive values redacted.
pub(super) fn safe_response_headers(
    headers: &reqwest::header::HeaderMap,
    credentials: &CredentialNames,
) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            let name = name.as_str();
            let value: String = value.as_bytes().iter().copied().map(char::from).collect();
            let value = match credentials.header_value(name) {
                Some(HeaderShape::Secret) => REDACTED.to_string(),
                Some(HeaderShape::Reference) => safe_reference(&value, credentials),
                Some(HeaderShape::Link) => safe_link(&value, credentials),
                Some(HeaderShape::Refresh) => safe_refresh(&value, credentials),
                None => value,
            };
            (name.to_string(), value)
        })
        .collect()
}

/// Drops user information and the fragment from a URI reference and redacts
/// sensitive query values. Every other byte, including the order, separators,
/// and encoding of safe query pairs such as pagination cursors, stays as the
/// upstream sent it.
fn safe_reference(reference: &str, credentials: &CredentialNames) -> String {
    let reference = reference
        .split_once('#')
        .map_or(reference, |(kept, _fragment)| kept);
    let (target, query) = match reference.split_once('?') {
        Some((target, query)) => (target, Some(query)),
        None => (reference, None),
    };
    let mut safe = without_user_information(target);
    if let Some(query) = query {
        safe.push('?');
        for pair in query.split_inclusive(['&', ';']) {
            let (pair, separator) = pair
                .strip_suffix(['&', ';'])
                .map_or((pair, ""), |kept| pair.split_at(kept.len()));
            match pair.split_once('=') {
                Some((name, _value)) if credentials.sensitive_parameter(name) => {
                    safe.push_str(name);
                    safe.push('=');
                    safe.push_str(REDACTED);
                }
                _ => safe.push_str(pair),
            }
            safe.push_str(separator);
        }
    }
    safe
}

fn without_user_information(target: &str) -> String {
    let Some((prefix, rest)) = target.split_once("//") else {
        return target.to_string();
    };
    // `//` opens an authority only at the start or right after a scheme.
    if !(prefix.is_empty() || (prefix.ends_with(':') && !prefix.contains('/'))) {
        return target.to_string();
    }
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, None), |(authority, path)| (authority, Some(path)));
    let Some((_user_information, host)) = authority.rsplit_once('@') else {
        return target.to_string();
    };
    match path {
        Some(path) => format!("{prefix}//{host}/{path}"),
        None => format!("{prefix}//{host}"),
    }
}

/// Makes each `<URI-reference>` in a `Link` value safe, and each `anchor`
/// parameter, which also names a URI.
fn safe_link(value: &str, credentials: &CredentialNames) -> String {
    let mut safe = String::with_capacity(value.len());
    let mut rest = value;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('<') {
            safe.push('<');
            let Some((reference, remainder)) = after.split_once('>') else {
                safe.push_str(&safe_reference(after, credentials));
                return safe;
            };
            safe.push_str(&safe_reference(reference, credentials));
            safe.push('>');
            rest = remainder;
        } else {
            let (parameter, remainder) = rest.split_at(link_parameter_end(rest));
            safe.push_str(&safe_link_parameter(parameter, credentials));
            rest = remainder;
        }
    }
    safe
}

/// Returns where the `Link` parameter at the start of `value` ends: after its
/// `;` or `,`, or before the next `<`. A quoted value may hold any of them.
fn link_parameter_end(value: &str) -> usize {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ';' | ',' if !quoted => return index + 1,
            '<' if !quoted && index > 0 => return index,
            _ => {}
        }
    }
    value.len()
}

fn safe_link_parameter(parameter: &str, credentials: &CredentialNames) -> String {
    match parameter.split_once('=') {
        Some((name, value)) if name.trim().eq_ignore_ascii_case("anchor") => {
            format!("{name}={}", safe_embedded_reference(value, credentials))
        }
        // An unquoted `anchor` holding `;` reads as more parameters, so one
        // with a sensitive query name is redacted here.
        Some((name, value)) if credentials.sensitive_parameter(name.trim()) => {
            let separator = value.trim_end_matches([';', ',']);
            let (_value, separator) = value.split_at(separator.len());
            format!("{name}={REDACTED}{separator}")
        }
        _ => parameter.to_string(),
    }
}

/// Makes the target of a `Refresh: <seconds>; [url=]<URI-reference>` value safe.
fn safe_refresh(value: &str, credentials: &CredentialNames) -> String {
    // Browsers also accept `,` after the delay, or a value with no delay.
    let Some((delay, target)) = value.split_once(';') else {
        return safe_embedded_reference(value, credentials);
    };
    // `url` and `=` may each have spaces around them.
    let unlabelled = target.trim_start();
    let label = unlabelled
        .get(..3)
        .filter(|label| label.eq_ignore_ascii_case("url"))
        .and_then(|_label| unlabelled.get(3..))
        .and_then(|after| after.trim_start().strip_prefix('='));
    let (prefix, reference) = match label {
        Some(reference) => target.split_at(target.len() - reference.len()),
        None => ("", target),
    };
    format!(
        "{delay};{prefix}{}",
        safe_embedded_reference(reference, credentials)
    )
}

/// Makes a URI reference safe inside surrounding whitespace, quotes, or a
/// trailing list separator, all of which stay.
fn safe_embedded_reference(value: &str, credentials: &CredentialNames) -> String {
    let unpadded = value.trim_start();
    let (padding, unpadded) = value.split_at(value.len() - unpadded.len());
    if let Some(quote) = unpadded
        .chars()
        .next()
        .filter(|first| matches!(first, '"' | '\''))
    {
        let (_quote, quoted) = unpadded.split_at(quote.len_utf8());
        return match closing_quote(quoted, quote).map(|end| quoted.split_at(end)) {
            Some((reference, after)) => format!(
                "{padding}{quote}{}{after}",
                safe_reference(reference, credentials)
            ),
            None => format!("{padding}{quote}{}", safe_reference(quoted, credentials)),
        };
    }
    let reference = unpadded.trim_end_matches([';', ',', ' ', '\t']);
    let (reference, after) = unpadded.split_at(reference.len());
    format!("{padding}{}{after}", safe_reference(reference, credentials))
}

/// Returns where `quote` closes `quoted`, skipping backslash escapes.
fn closing_quote(quoted: &str, quote: char) -> Option<usize> {
    let mut escaped = false;
    quoted.char_indices().find_map(|(index, character)| {
        match character {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            _ if character == quote => return Some(index),
            _ => {}
        }
        None
    })
}
