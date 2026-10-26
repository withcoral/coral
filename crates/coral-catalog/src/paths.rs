use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

use crate::{CatalogDiagnostic, MaterializedOperation, OperationKey};

/// One active Generation-relative path retained across regeneration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPathAssignment {
    key: OperationKey,
    relative_path: String,
}

impl OperationPathAssignment {
    pub(crate) const fn new(key: OperationKey, relative_path: String) -> Self {
        Self { key, relative_path }
    }

    pub(crate) fn for_operation(operation: &MaterializedOperation) -> Self {
        Self {
            key: operation.key().clone(),
            relative_path: operation.relative_path().to_string(),
        }
    }

    pub(crate) fn for_operations(operations: &[MaterializedOperation]) -> Vec<Self> {
        operations.iter().map(Self::for_operation).collect()
    }

    /// Returns the exact Operation Key that owns this active path.
    #[must_use]
    pub const fn key(&self) -> &OperationKey {
        &self.key
    }

    /// Returns the collision-free Operation-relative JavaScript path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

pub(super) fn assign_operation_paths<'a>(
    keys: impl Iterator<Item = &'a OperationKey>,
    previous_paths: &[OperationPathAssignment],
) -> Result<BTreeMap<OperationKey, String>, CatalogDiagnostic> {
    let keys = keys.cloned().collect::<BTreeSet<_>>();
    let mut assigned = BTreeMap::new();
    let mut used = HashSet::new();
    for previous in previous_paths {
        if keys.contains(previous.key())
            && (!valid_relative_path(previous.relative_path())
                || !used.insert(previous.relative_path().to_string())
                || assigned
                    .insert(previous.key().clone(), previous.relative_path().to_string())
                    .is_some())
        {
            return Err(diagnostic(
                "invalid_operation_path_index",
                "the previous active catalog has incoherent Operation paths",
            ));
        }
    }
    for key in keys {
        if assigned.contains_key(&key) {
            continue;
        }
        let base = operation_relative_path(&key);
        let relative_path = if used.insert(base.clone()) {
            base
        } else {
            collision_path(&base, &key, &mut used)
        };
        assigned.insert(key, relative_path);
    }
    Ok(assigned)
}

fn collision_path(base: &str, key: &OperationKey, used: &mut HashSet<String>) -> String {
    let digest = Sha256::digest(key.as_str().as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing hexadecimal to a String cannot fail");
    }
    for length in [8_usize, 12, 16, 24, 32, 64] {
        let suffix = encoded.chars().take(length).collect::<String>();
        let candidate = format!("{base}_{suffix}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("distinct Operation Keys have a collision-free SHA-256 suffix")
}

/// Projects one Operation Key onto its idiomatic JavaScript call shape.
///
/// A provider-authored key such as `actions/add-repo-access-to-runner-group`
/// or `compute.instances.list` becomes the dotted path
/// `actions.addRepoAccessToRunnerGroup` or `compute.instances.list`: `/` and
/// `.` separate namespace segments and every segment is camelCased from its
/// words. A deterministic `METHOD /path` fallback key stays one segment, such
/// as `getReposOwnerRepo`, because its URL slashes are not namespaces. Distinct
/// keys that project onto the same path receive collision suffixes.
fn operation_relative_path(key: &OperationKey) -> String {
    let key = key.as_str();
    if is_method_fallback_key(key) {
        return safe_segment(camel_case(&words(key)));
    }
    let path = key
        .split(['/', '.'])
        .map(words)
        .filter(|words| !words.is_empty())
        .map(|words| safe_segment(camel_case(&words)))
        .collect::<Vec<_>>()
        .join(".");
    if path.is_empty() {
        "_".to_string()
    } else {
        path
    }
}

/// Recognizes the `METHOD /path` key materialization assigns when an
/// OpenAPI Operation declares no `operationId`.
fn is_method_fallback_key(key: &str) -> bool {
    key.split_once(' ').is_some_and(|(method, path)| {
        !method.is_empty()
            && method.bytes().all(|byte| byte.is_ascii_uppercase())
            && path.starts_with('/')
    })
}

/// Splits one key segment into ASCII alphanumeric words.
///
/// Every other character, including `_`, `-`, whitespace, braces, and
/// non-ASCII text, separates words and never reaches the path.
fn words(segment: &str) -> Vec<&str> {
    segment
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// Joins words as lowerCamelCase.
///
/// The first word keeps its spelling apart from a lowercase initial, except
/// that a word without lowercase letters, such as `GET` or `V2`, lowercases
/// entirely. Later words keep their spelling apart from an uppercase initial,
/// so `fetch_URL` stays `fetchURL`.
fn camel_case(words: &[&str]) -> String {
    let mut path = String::new();
    for (index, word) in words.iter().enumerate() {
        let mut characters = word.chars();
        let Some(initial) = characters.next() else {
            continue;
        };
        if index == 0 {
            if word.chars().any(|character| character.is_ascii_lowercase()) {
                path.push(initial.to_ascii_lowercase());
                path.push_str(characters.as_str());
            } else {
                path.push_str(&word.to_ascii_lowercase());
            }
        } else {
            path.push(initial.to_ascii_uppercase());
            path.push_str(characters.as_str());
        }
    }
    path
}

/// Prefixes `_` when a segment would otherwise be an invalid JavaScript
/// identifier or a reserved Call Path segment.
fn safe_segment(mut segment: String) -> String {
    if !segment
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'))
        || call_path_segment_is_reserved(&segment)
    {
        segment.insert(0, '_');
    }
    segment
}

/// Whether a stored or carried-forward path has the shape every assigned path
/// has: dotted ASCII JavaScript identifiers. Naming rules, such as which
/// segments `safe_segment` escapes, apply only when a path is assigned. They
/// change over time, and a path an earlier rule assigned stays valid.
pub(super) fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|segment| {
            segment
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'))
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
        })
}

fn diagnostic(code: &'static str, message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable(code, message)
}

/// Whether `value` is a Call Path segment that the Program Runtime Surface's
/// Call Path proxy cannot route. The proxy answers `then` with `undefined`, so
/// awaiting a path does not treat it as a thenable; the rest are object-model
/// names. Operation paths, Provider namespaces and Saved Function paths all
/// reserve these. Language keywords such as `delete` stay usable: property
/// access accepts them, and an Operation's declaration does not spell its
/// path.
#[must_use]
pub fn call_path_segment_is_reserved(value: &str) -> bool {
    matches!(value, "then" | "constructor" | "prototype" | "__proto__")
}

/// Whether `value` can be a Provider Namespace: a non-reserved safe ASCII
/// JavaScript identifier of at most 128 bytes.
#[must_use]
pub fn provider_namespace_is_valid(value: &str) -> bool {
    const MAX_BYTES: usize = 128;
    let mut bytes = value.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'));
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'));
    valid_first && valid_rest && value.len() <= MAX_BYTES && !call_path_segment_is_reserved(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(key: &str) -> String {
        operation_relative_path(&OperationKey::parse(key).expect("test key"))
    }

    #[test]
    fn slashes_become_namespaces_and_segments_camel_case() {
        assert_eq!(
            path("actions/add-repo-access-to-self-hosted-runner-group-in-org"),
            "actions.addRepoAccessToSelfHostedRunnerGroupInOrg"
        );
        assert_eq!(path("compute.instances.list"), "compute.instances.list");
        assert_eq!(path("Users_GetById"), "usersGetById");
        assert_eq!(path("getForecast"), "getForecast");
        assert_eq!(path("GetCustomers"), "getCustomers");
        assert_eq!(path("fetch_URL"), "fetchURL");
        assert_eq!(path("HTML export"), "htmlExport");
        assert_eq!(path("foo bar"), "fooBar");
        assert_eq!(path("foo@bar"), "fooBar");
    }

    #[test]
    fn method_fallback_keys_stay_one_segment() {
        assert_eq!(path("GET /repos/{owner}/{repo}"), "getReposOwnerRepo");
        assert_eq!(path("DELETE /v2/items"), "deleteV2Items");
    }

    #[test]
    fn segments_keep_keywords_and_escape_only_names_the_runtime_reserves() {
        for (key, expected) in [
            ("repos/delete", "repos.delete"),
            ("pulls/get", "pulls.get"),
            ("interface", "interface"),
            ("default/new", "default.new"),
            ("then", "_then"),
            ("items/constructor", "items._constructor"),
            ("prototype", "_prototype"),
            ("2fa/enable", "_2fa.enable"),
            ("///", "_"),
            ("日本", "_"),
        ] {
            assert_eq!(path(key), expected, "{key}");
            assert!(valid_relative_path(expected), "{expected}");
        }
        for malformed in ["", "2fa", "a..b", "a.b-c"] {
            assert!(!valid_relative_path(malformed), "{malformed}");
        }
    }

    /// Naming rules change; a path an earlier rule assigned must still carry
    /// forward, even one whose segment a later rule reserves or escapes.
    #[test]
    fn paths_assigned_under_earlier_naming_rules_carry_forward() {
        for (key, earlier) in [("items/then", "items.then"), ("pulls/get", "pulls._get")] {
            let key = OperationKey::parse(key).expect("test key");
            let previous = OperationPathAssignment {
                key: key.clone(),
                relative_path: earlier.to_string(),
            };
            let assigned =
                assign_operation_paths([&key].into_iter(), &[previous]).expect("assignment");
            assert_eq!(assigned.get(&key).map(String::as_str), Some(earlier));
        }
    }

    #[test]
    fn colliding_projections_receive_deterministic_suffixes() {
        let keys = ["get-user", "getUser", "get_user"]
            .map(|key| OperationKey::parse(key).expect("test key"));
        let assigned = assign_operation_paths(keys.iter(), &[]).expect("assignment");
        let paths = assigned.values().collect::<BTreeSet<_>>();
        assert_eq!(paths.len(), 3);
        assert!(paths.iter().all(|path| path.starts_with("getUser")));
        assert!(paths.iter().all(|path| valid_relative_path(path)));
    }
}
