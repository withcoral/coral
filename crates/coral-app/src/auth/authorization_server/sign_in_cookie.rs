//! The cookie that carries an upstream sign-in in the browser that started it.
//!
//! Each sign-in's session is sealed into its own cookie (see
//! [`SignInSession`](crate::auth::sealed_handshake::SignInSession)), so any
//! Coral process can finish a sign-in another one started. The cookie also
//! binds the sign-in to its browser. The sign-in's value travels through the
//! identity provider, so its sign-in link reaches any browser it is sent to,
//! but only the starting browser holds the cookie. An answer in any other
//! browser finds nothing to finish.
//!
//! The name carries the sign-in's start time and a tag of its value. The tag
//! lets the mode's answer find its own cookie among a browser's concurrent
//! sign-ins; the start time lets a new sign-in expire the oldest ones.
//!
//! The sign-in mode chooses the cookie's `SameSite`. Everything else about it
//! is the same in every mode.

use std::fmt::Write as _;

use axum::http::{HeaderMap, HeaderValue};
use coral_sign_in::SameSite;
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
/// Bytes of SHA-256 of the sign-in's value that the name carries: enough
/// that one browser's concurrent sign-ins never share a tag.
const TAG_BYTES: usize = 8;

/// The attributes of one deployment's sign-in cookies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Attributes {
    /// The `__Host-` prefix, which only an HTTPS issuer can use.
    prefixed: bool,
    secure: bool,
    same_site: SameSite,
}

impl Attributes {
    /// The cookies of a server whose issuer is `https` or not, for a mode
    /// that asks for `same_site`. A `SameSite=None` cookie is always
    /// `Secure`, since browsers drop one that is not, also on a loopback
    /// `http` issuer; its name keeps the prefix of the issuer's scheme.
    pub(super) const fn new(https: bool, same_site: SameSite) -> Self {
        Self {
            prefixed: https,
            secure: https || matches!(same_site, SameSite::None),
            same_site,
        }
    }
}

/// Whether the app's inputs that `session` carries fit in one sign-in cookie.
pub(super) fn app_input_fits(session: &SignInSession) -> bool {
    session.request.client_id.len()
        + session.request.redirect_uri.len()
        + session.request.client_state.as_deref().map_or(0, str::len)
        + session.client_name.as_deref().map_or(0, str::len)
        <= MAX_APP_INPUT_BYTES
}

/// Whether a sealed sign-in of `sealed_len` bytes fits in one cookie.
///
/// [`app_input_fits`] bounds what an app sends; this also counts what Coral
/// adds, such as the resource, so a request that cannot be carried is refused
/// before consent rather than after it.
pub(super) fn sealed_fits(sealed_len: usize, attributes: Attributes) -> bool {
    let longest_name = name("", u64::MAX, attributes);
    header_value(
        &longest_name,
        &"x".repeat(sealed_len),
        MAX_AGE_SECONDS,
        attributes,
    )
    .is_some()
}

/// The cookie name for the sign-in named by `sign_in`, started at `started_at`.
pub(super) fn name(sign_in: &str, started_at_unix_secs: u64, attributes: Attributes) -> String {
    format!(
        "{}{STEM}{started_at_unix_secs}_{}",
        prefix(attributes),
        tag(sign_in)
    )
}

/// The `Set-Cookie` that hands the browser this sign-in's sealed session.
///
/// `None` when the cookie would exceed what browsers store.
pub(super) fn set_header(name: &str, sealed: &str, attributes: Attributes) -> Option<HeaderValue> {
    header_value(name, sealed, MAX_AGE_SECONDS, attributes)
}

/// The `Set-Cookie` that removes the sign-in cookie called `name`.
pub(super) fn clear_header(name: &str, attributes: Attributes) -> Option<HeaderValue> {
    header_value(name, "", 0, attributes)
}

/// The name and sealed value of this browser's cookie for `sign_in`.
///
/// `None` when the browser holds none, or more than one: a shadowing cookie
/// must not choose which value is read.
pub(super) fn find(
    headers: &HeaderMap,
    sign_in: &str,
    attributes: Attributes,
) -> Option<(String, Zeroizing<Vec<u8>>)> {
    let expected = tag(sign_in);
    let mut found = None;
    for (name, value) in cookies::pairs(headers) {
        let Some((_started_at, tag)) = parse(name, attributes) else {
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
pub(super) fn evicted_by_new_sign_in(headers: &HeaderMap, attributes: Attributes) -> Vec<String> {
    let mut held = cookies::pairs(headers)
        .filter_map(|(name, _value)| {
            let (started_at, _tag) = parse(name, attributes)?;
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

/// Splits a sign-in cookie name into its start time and the tag of its
/// sign-in's value.
fn parse(name: &[u8], attributes: Attributes) -> Option<(u64, &[u8])> {
    let rest = name
        .strip_prefix(prefix(attributes).as_bytes())?
        .strip_prefix(STEM.as_bytes())?;
    let separator = rest.iter().position(|byte| *byte == b'_')?;
    let (started_at, tag) = rest.split_at_checked(separator)?;
    let started_at = std::str::from_utf8(started_at).ok()?.parse().ok()?;
    let tag = tag.strip_prefix(b"_")?;
    (tag.len() == TAG_BYTES * 2).then_some((started_at, tag))
}

const fn prefix(attributes: Attributes) -> &'static str {
    if attributes.prefixed { "__Host-" } else { "" }
}

fn tag(sign_in: &str) -> String {
    Sha256::digest(sign_in.as_bytes())
        .iter()
        .take(TAG_BYTES)
        .fold(String::new(), |mut tag, byte| {
            let _infallible = write!(tag, "{byte:02x}");
            tag
        })
}

fn header_value(
    name: &str,
    value: &str,
    max_age: u64,
    attributes: Attributes,
) -> Option<HeaderValue> {
    let secure_attribute = if attributes.secure { "; Secure" } else { "" };
    let cookie = format!(
        "{name}={value}; Path=/; Max-Age={max_age}; HttpOnly{secure_attribute}; SameSite={}",
        attributes.same_site.as_str()
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

    const HTTPS: Attributes = Attributes::new(true, SameSite::Lax);
    const HTTP: Attributes = Attributes::new(false, SameSite::Lax);
    use crate::auth::sealed_handshake::ClientRequest;

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

    /// The mode's `SameSite` reaches every cookie a sign-in sets or clears.
    /// A `None` cookie is `Secure` also on an `http` issuer, where its name
    /// has no `__Host-` prefix; a `Lax` cookie is as it has always been.
    #[test]
    fn each_same_site_gives_its_attributes() {
        for (https, same_site, prefix, attributes) in [
            (
                true,
                SameSite::Lax,
                "__Host-",
                "HttpOnly; Secure; SameSite=Lax",
            ),
            (false, SameSite::Lax, "", "HttpOnly; SameSite=Lax"),
            (
                true,
                SameSite::None,
                "__Host-",
                "HttpOnly; Secure; SameSite=None",
            ),
            (false, SameSite::None, "", "HttpOnly; Secure; SameSite=None"),
        ] {
            let chosen = Attributes::new(https, same_site);
            let name = name("state", 10, chosen);
            assert_eq!(
                name,
                format!("{prefix}coral_oauth_signin_10_{}", tag("state")),
                "{chosen:?}"
            );
            for (header, max_age) in [
                (set_header(&name, "sealed", chosen), MAX_AGE_SECONDS),
                (clear_header(&name, chosen), 0),
            ] {
                let value = if max_age == 0 { "" } else { "sealed" };
                assert_eq!(
                    header.expect("a cookie").to_str().expect("ASCII"),
                    format!("{name}={value}; Path=/; Max-Age={max_age}; {attributes}"),
                    "{chosen:?}"
                );
            }
            let found = find(&browser(std::slice::from_ref(&name)), "state", chosen);
            assert_eq!(found.map(|(found, _)| found), Some(name), "{chosen:?}");
        }
    }

    #[test]
    fn a_new_sign_in_expires_the_oldest_beyond_three() {
        let held = [30, 10, 20]
            .map(|started_at| name(&format!("state-{started_at}"), started_at, HTTPS))
            .to_vec();
        let mut headers = browser(&held);
        headers.append(
            header::COOKIE,
            HeaderValue::from_static("__Host-coral_oauth_approval=unrelated"),
        );
        assert_eq!(
            evicted_by_new_sign_in(&headers, HTTPS),
            vec![name("state-10", 10, HTTPS)]
        );
        assert_eq!(
            evicted_by_new_sign_in(&browser(held.get(..2).expect("two")), HTTPS),
            [] as [std::string::String; 0]
        );
        assert!(
            evicted_by_new_sign_in(&headers, HTTP).is_empty(),
            "a loopback server reads only unprefixed names"
        );
    }

    #[test]
    fn the_callback_finds_only_its_own_single_cookie() {
        let own = name("state", 10, HTTPS);
        let other = name("other-state", 11, HTTPS);
        let found = find(&browser(&[own.clone(), other.clone()]), "state", HTTPS);
        assert_eq!(found.map(|(name, _value)| name), Some(own.clone()));
        assert!(find(&browser(std::slice::from_ref(&other)), "state", HTTPS).is_none());
        let shadowed = browser(&[own, name("state", 12, HTTPS)]);
        assert!(find(&shadowed, "state", HTTPS).is_none());
    }

    /// A sign-in that spends `app_bytes` of the app budget on its client ID,
    /// with a 350-byte resource.
    fn full_sign_in(app_bytes: usize, client_name: Option<&str>) -> SignInSession {
        let named = client_name.map_or(0, str::len);
        SignInSession {
            request: ClientRequest {
                client_id: "c".repeat(app_bytes - 2 - named),
                redirect_uri: "r".into(),
                client_state: Some("s".into()),
                code_challenge: "x".repeat(43),
                resource: "u".repeat(350),
                refresh_eligible: false,
            },
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
            let pair = name("state", u64::from(u32::MAX), HTTPS).len()
                + 1
                + session.sealed_len().expect("sealed length");
            let header = MAX_CONCURRENT_SIGN_INS * pair + 2 * (MAX_CONCURRENT_SIGN_INS - 1);
            assert!(header < 8 * 1024, "{client_name:?}: {header} bytes");
        }
    }

    #[test]
    fn cookies_browsers_would_drop_are_never_set() {
        for (attributes, same_site) in [
            (HTTPS, "Lax"),
            (Attributes::new(true, SameSite::None), "None"),
        ] {
            let name = name("state", 10, attributes);
            let largest = MAX_COOKIE_BYTES
                - format!("{name}=; Path=/; Max-Age=300; HttpOnly; Secure; SameSite={same_site}")
                    .len();
            assert!(set_header(&name, &"a".repeat(largest), attributes).is_some());
            assert!(set_header(&name, &"a".repeat(largest + 1), attributes).is_none());
        }
        assert!(app_input_fits(&full_sign_in(MAX_APP_INPUT_BYTES, None)));
        for client_name in [None, Some("n")] {
            assert!(
                !app_input_fits(&full_sign_in(MAX_APP_INPUT_BYTES + 1, client_name)),
                "{client_name:?}"
            );
        }
    }
}
