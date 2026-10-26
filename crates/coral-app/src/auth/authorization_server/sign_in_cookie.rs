//! The cookie that carries an upstream sign-in in the browser that started it.
//!
//! Each sign-in's session is sealed into its own cookie (see
//! [`SignInSession`](crate::auth::sealed_handshake::SignInSession)), so any
//! Coral process can finish a sign-in another one started. The cookie also
//! binds the sign-in to its browser. The OIDC `state` travels through the
//! provider, so the provider's sign-in link reaches any browser it is sent to,
//! but only the starting browser holds the cookie. A callback in any other
//! browser finds nothing to finish.
//!
//! The name carries the sign-in's start time and a tag of its `state`. The tag
//! lets the callback find its own cookie among a browser's concurrent
//! sign-ins; the start time lets a new sign-in expire the oldest ones.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use super::cookies;
use crate::auth::sealed_handshake::SignInSession;

/// As long as a sign-in may take.
pub(super) const MAX_AGE_SECONDS: u64 = 5 * 60;
/// The most sign-in cookies one browser holds.
///
/// Every cookie rides on each request to this host until it expires. With the
/// app input budget and a resource of at most 350 bytes, three sign-in cookies
/// stay under the 8 KB per-header limit of nginx's defaults; a test holds this.
/// A longer configured resource can take three past it.
const MAX_CONCURRENT_SIGN_INS: usize = 3;
/// The most bytes of `client_id`, `redirect_uri`, `state` and the client's
/// name, when it rides along, that one sign-in carries. Real apps send a few
/// hundred.
const MAX_APP_INPUT_BYTES: usize = 1536;
/// The largest cookie browsers store, counted with its attributes.
const MAX_COOKIE_BYTES: usize = 4096;
const STEM: &str = "coral_oauth_signin_";
/// Bytes of SHA-256(`state`) the name carries: enough that one browser's
/// concurrent sign-ins never share a tag.
const STATE_TAG_BYTES: usize = 8;

/// Whether the app's inputs that `session` carries fit in one sign-in cookie.
pub(super) fn app_input_fits(session: &SignInSession) -> bool {
    session.client_id.len()
        + session.redirect_uri.len()
        + session.client_state.as_deref().map_or(0, str::len)
        + session.client_name.as_deref().map_or(0, str::len)
        <= MAX_APP_INPUT_BYTES
}

/// Whether a sealed sign-in of `sealed_len` bytes fits in one cookie.
///
/// [`app_input_fits`] bounds what an app sends; this also counts what Coral
/// adds, such as the resource, so a request that cannot be carried is refused
/// before consent rather than after it.
pub(super) fn sealed_fits(sealed_len: usize, secure: bool) -> bool {
    let longest_name = name("", u64::MAX, secure);
    header_value(
        &longest_name,
        &"x".repeat(sealed_len),
        MAX_AGE_SECONDS,
        secure,
    )
    .is_some()
}

/// Host time in Unix seconds, or `None` before the epoch.
pub(super) fn now_unix_secs() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|elapsed| elapsed.as_secs())
}

/// The cookie name for the sign-in named by `oidc_state`, started at `started_at`.
pub(super) fn name(oidc_state: &str, started_at_unix_secs: u64, secure: bool) -> String {
    format!(
        "{}{STEM}{started_at_unix_secs}_{}",
        prefix(secure),
        tag(oidc_state)
    )
}

/// The `Set-Cookie` that hands the browser this sign-in's sealed session.
///
/// `None` when the cookie would exceed what browsers store.
pub(super) fn set_header(name: &str, sealed: &str, secure: bool) -> Option<HeaderValue> {
    header_value(name, sealed, MAX_AGE_SECONDS, secure)
}

/// The `Set-Cookie` that removes the sign-in cookie called `name`.
pub(super) fn clear_header(name: &str, secure: bool) -> Option<HeaderValue> {
    header_value(name, "", 0, secure)
}

/// The name and sealed value of this browser's cookie for `oidc_state`.
///
/// `None` when the browser holds none, or more than one: a shadowing cookie
/// must not choose which value is read.
pub(super) fn find(
    headers: &HeaderMap,
    oidc_state: &str,
    secure: bool,
) -> Option<(String, Zeroizing<Vec<u8>>)> {
    let expected = tag(oidc_state);
    let mut found = None;
    for (name, value) in cookies::pairs(headers) {
        let Some((_started_at, tag)) = parse(name, secure) else {
            continue;
        };
        if tag != expected.as_bytes() {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some((
            String::from_utf8(name.to_vec()).ok()?,
            Zeroizing::new(value.to_vec()),
        ));
    }
    found
}

/// The browser's sign-in cookies a new sign-in pushes out, oldest first, so
/// that with the new one it holds at most [`MAX_CONCURRENT_SIGN_INS`].
pub(super) fn evicted_by_new_sign_in(headers: &HeaderMap, secure: bool) -> Vec<String> {
    let mut held = cookies::pairs(headers)
        .filter_map(|(name, _value)| {
            let (started_at, _tag) = parse(name, secure)?;
            Some((started_at, String::from_utf8(name.to_vec()).ok()?))
        })
        .collect::<Vec<_>>();
    held.sort_unstable();
    let excess = (held.len() + 1).saturating_sub(MAX_CONCURRENT_SIGN_INS);
    held.into_iter()
        .take(excess)
        .map(|(_, name)| name)
        .collect()
}

/// Splits a sign-in cookie name into its start time and state tag.
fn parse(name: &[u8], secure: bool) -> Option<(u64, &[u8])> {
    let rest = name
        .strip_prefix(prefix(secure).as_bytes())?
        .strip_prefix(STEM.as_bytes())?;
    let separator = rest.iter().position(|byte| *byte == b'_')?;
    let (started_at, tag) = rest.split_at_checked(separator)?;
    let started_at = std::str::from_utf8(started_at).ok()?.parse().ok()?;
    let tag = tag.strip_prefix(b"_")?;
    (tag.len() == STATE_TAG_BYTES * 2).then_some((started_at, tag))
}

fn prefix(secure: bool) -> &'static str {
    if secure { "__Host-" } else { "" }
}

fn tag(oidc_state: &str) -> String {
    Sha256::digest(oidc_state.as_bytes())
        .iter()
        .take(STATE_TAG_BYTES)
        .fold(String::new(), |mut tag, byte| {
            let _infallible = write!(tag, "{byte:02x}");
            tag
        })
}

fn header_value(name: &str, value: &str, max_age: u64, secure: bool) -> Option<HeaderValue> {
    let secure_attribute = if secure { "; Secure" } else { "" };
    let cookie = format!(
        "{name}={value}; Path=/; Max-Age={max_age}; HttpOnly{secure_attribute}; SameSite=Lax"
    );
    if cookie.len() > MAX_COOKIE_BYTES {
        return None;
    }
    HeaderValue::from_str(&cookie).ok()
}

#[cfg(test)]
mod tests {
    use axum::http::header;

    use super::*;

    fn browser(names: &[String]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for name in names {
            headers.append(
                header::COOKIE,
                HeaderValue::from_str(&format!("{name}=sealed")).expect("cookie"),
            );
        }
        headers
    }

    #[test]
    fn a_new_sign_in_expires_the_oldest_beyond_three() {
        let held = [30, 10, 20]
            .map(|started_at| name(&format!("state-{started_at}"), started_at, true))
            .to_vec();
        let mut headers = browser(&held);
        headers.append(
            header::COOKIE,
            HeaderValue::from_static("__Host-coral_oauth_approval=unrelated"),
        );
        assert_eq!(
            evicted_by_new_sign_in(&headers, true),
            vec![name("state-10", 10, true)]
        );
        assert!(evicted_by_new_sign_in(&browser(held.get(..2).expect("two")), true).is_empty());
        assert!(
            evicted_by_new_sign_in(&headers, false).is_empty(),
            "a loopback server reads only unprefixed names"
        );
    }

    #[test]
    fn the_callback_finds_only_its_own_single_cookie() {
        let own = name("state", 10, true);
        let other = name("other-state", 11, true);
        let found = find(&browser(&[own.clone(), other.clone()]), "state", true);
        assert_eq!(found.map(|(name, _value)| name), Some(own.clone()));
        assert!(find(&browser(std::slice::from_ref(&other)), "state", true).is_none());
        let shadowed = browser(&[own, name("state", 12, true)]);
        assert!(find(&shadowed, "state", true).is_none());
    }

    /// A sign-in that spends `app_bytes` of the app budget on its client ID,
    /// with a 350-byte resource.
    fn full_sign_in(app_bytes: usize, client_name: Option<&str>) -> SignInSession {
        let named = client_name.map_or(0, str::len);
        SignInSession {
            client_id: "c".repeat(app_bytes - 2 - named),
            redirect_uri: "r".into(),
            client_state: Some("s".into()),
            code_challenge: "x".repeat(43),
            resource: "u".repeat(350),
            client_name: client_name.map(str::to_owned),
        }
    }

    /// The bound [`MAX_CONCURRENT_SIGN_INS`] documents: three sign-ins that
    /// each use the whole app budget, with a 350-byte resource, fit one 8 KB
    /// `Cookie` header, whether or not they carry the longest client name.
    #[test]
    fn three_full_sign_ins_fit_an_8_kb_cookie_header() {
        for client_name in [None, Some("n".repeat(255))] {
            let session = full_sign_in(MAX_APP_INPUT_BYTES, client_name.as_deref());
            assert!(app_input_fits(&session));
            let pair = name("state", u64::from(u32::MAX), true).len()
                + 1
                + session.sealed_len().expect("sealed length");
            let header = MAX_CONCURRENT_SIGN_INS * pair + 2 * (MAX_CONCURRENT_SIGN_INS - 1);
            assert!(header < 8 * 1024, "{client_name:?}: {header} bytes");
        }
    }

    #[test]
    fn cookies_browsers_would_drop_are_never_set() {
        let name = name("state", 10, true);
        let largest = MAX_COOKIE_BYTES
            - format!("{name}=; Path=/; Max-Age=300; HttpOnly; Secure; SameSite=Lax").len();
        assert!(set_header(&name, &"a".repeat(largest), true).is_some());
        assert!(set_header(&name, &"a".repeat(largest + 1), true).is_none());
        assert!(app_input_fits(&full_sign_in(MAX_APP_INPUT_BYTES, None)));
        for client_name in [None, Some("n")] {
            assert!(
                !app_input_fits(&full_sign_in(MAX_APP_INPUT_BYTES + 1, client_name)),
                "{client_name:?}"
            );
        }
    }
}
