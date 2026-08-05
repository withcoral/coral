//! Bounded query-string scanning shared by the authorization-server handlers.
//!
//! Every handler here reads its parameters from a browser-supplied query, so
//! each one needs the same two guarantees: a request cannot grow without bound,
//! and no parameter may appear twice. Both live here rather than in each
//! handler because a handler that gets its own copy of the limits is a handler
//! whose limits can drift from the rest.
//!
//! Duplicate rejection is deliberately applied to the *decoded* name, after
//! percent-decoding, so `%73tate` is caught as a second `state` rather than
//! slipping past as an unrecognized parameter and shadowing the real one.

use std::borrow::Cow;
use std::collections::BTreeSet;

use url::form_urlencoded;

const MAX_QUERY_BYTES: usize = 8 * 1024;

/// How many parameters one request may carry, however it was encoded.
///
/// This and the two byte limits below bound a *parameter list*, not a query, so
/// they are shared with the token endpoint's form parser: that handler decodes
/// its own body rather than calling [`scan`], but a form parameter and a query
/// parameter are the same thing and must not be bounded differently. The
/// request-size limit is deliberately not shared — a request body and a URL are
/// bounded for different reasons, so each caller sets its own.
pub(super) const MAX_PARAMETERS: usize = 32;
pub(super) const MAX_PARAMETER_NAME_BYTES: usize = 64;
pub(super) const MAX_PARAMETER_VALUE_BYTES: usize = 2 * 1024;

/// Visits each decoded parameter of `raw` once, within this module's limits.
///
/// `visit` is called for every parameter, recognized or not; a handler ignores
/// the names it does not use. Unrecognized names are still counted and still
/// checked for duplication, so padding a request with junk cannot buy extra
/// room past the limits.
///
/// # Errors
///
/// Returns `Err(())` when the query exceeds [`MAX_QUERY_BYTES`], carries more
/// than [`MAX_PARAMETERS`] parameters, has a name or value over its byte limit,
/// or repeats a decoded name. The caller turns that into a fixed protocol
/// error; nothing about which limit tripped reaches the response.
pub(super) fn scan(raw: &str, mut visit: impl FnMut(&str, Cow<'_, str>)) -> Result<(), ()> {
    if raw.len() > MAX_QUERY_BYTES {
        return Err(());
    }
    let mut seen = BTreeSet::new();
    for (index, (name, value)) in form_urlencoded::parse(raw.as_bytes()).enumerate() {
        if index >= MAX_PARAMETERS
            || name.len() > MAX_PARAMETER_NAME_BYTES
            || value.len() > MAX_PARAMETER_VALUE_BYTES
        {
            return Err(());
        }
        let name = name.into_owned();
        if !seen.insert(name.clone()) {
            return Err(());
        }
        visit(&name, value);
    }
    Ok(())
}

/// Parameter lists that every handler's limit tests must reject.
///
/// Every handler that reads parameters asserts against this list rather than
/// writing its own literals, so a parameter limit added here is exercised by
/// each of them — including the token endpoint, which parses a form body with
/// its own decoder but shares these limits.
#[cfg(test)]
pub(super) fn rejected_parameter_lists() -> Vec<String> {
    vec![
        format!("{}=x", "n".repeat(MAX_PARAMETER_NAME_BYTES + 1)),
        format!("x={}", "v".repeat(MAX_PARAMETER_VALUE_BYTES + 1)),
        (0..=MAX_PARAMETERS)
            .map(|index| format!("x{index}=1"))
            .collect::<Vec<_>>()
            .join("&"),
    ]
}

/// Queries that every query-scanning handler's limit tests must reject.
///
/// This is [`rejected_parameter_lists`] plus the query-size limit, which is
/// this module's own rather than a shared parameter limit.
#[cfg(test)]
pub(super) fn rejected_queries() -> Vec<String> {
    let mut queries = vec!["x".repeat(MAX_QUERY_BYTES + 1)];
    queries.extend(rejected_parameter_lists());
    queries
}
