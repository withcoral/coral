//! The cookie that ties an upstream sign-in to the browser that started it.
//!
//! The OIDC `state` that names a sign-in travels through the provider, so the
//! provider's sign-in link reaches any browser it is sent to. Each sign-in
//! therefore also sets its own cookie, holding a binding only the starting
//! browser has, and the callback looks the handshake up by both. A browser the
//! link was forwarded to has no such cookie: its callback finds nothing and
//! leaves the handshake for the browser that started it.
//!
//! The cookie is named for its `state`, so sign-ins running at once in one
//! browser never overwrite each other's binding.

use std::fmt::Write as _;

use axum::http::{HeaderMap, HeaderValue};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use super::super::state_store::OAuthSignInBinding;
use super::cookies;

/// As long as the handshake the cookie binds.
const MAX_AGE_SECONDS: u64 = 5 * 60;
const BINDING_LENGTH: usize = 43;
/// Bytes of SHA-256(`state`) the name carries: enough that one browser's
/// concurrent sign-ins never share a name.
const STATE_TAG_BYTES: usize = 8;

/// The `Set-Cookie` that hands `binding` to the browser starting this sign-in.
pub(super) fn set_header(
    oidc_state: &str,
    binding: &OAuthSignInBinding,
    secure: bool,
) -> Option<HeaderValue> {
    let value = Zeroizing::new(URL_SAFE_NO_PAD.encode(binding.as_bytes()));
    header_value(&name(oidc_state, secure), &value, MAX_AGE_SECONDS, secure)
}

/// The `Set-Cookie` that removes this sign-in's cookie once its callback ran.
pub(super) fn clear_header(oidc_state: &str, secure: bool) -> Option<HeaderValue> {
    header_value(&name(oidc_state, secure), "", 0, secure)
}

/// The binding this browser holds for the sign-in named by `oidc_state`.
///
/// `None` when the cookie is absent, sent more than once, or malformed.
pub(super) fn binding(
    headers: &HeaderMap,
    oidc_state: &str,
    secure: bool,
) -> Option<OAuthSignInBinding> {
    let encoded = cookies::single_value(headers, &name(oidc_state, secure))
        .filter(|value| value.len() == BINDING_LENGTH)?;
    let mut bytes = Zeroizing::new([0_u8; 32]);
    let decoded = URL_SAFE_NO_PAD.decode_slice(&**encoded, &mut *bytes).ok()?;
    (decoded == bytes.len()).then(|| OAuthSignInBinding::from_bytes(*bytes))
}

fn name(oidc_state: &str, secure: bool) -> String {
    let prefix = if secure { "__Host-" } else { "" };
    let tag = Sha256::digest(oidc_state.as_bytes())
        .iter()
        .take(STATE_TAG_BYTES)
        .fold(String::new(), |mut tag, byte| {
            let _infallible = write!(tag, "{byte:02x}");
            tag
        });
    format!("{prefix}coral_oauth_signin_{tag}")
}

fn header_value(name: &str, value: &str, max_age: u64, secure: bool) -> Option<HeaderValue> {
    let secure_attribute = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{name}={value}; Path=/; Max-Age={max_age}; HttpOnly{secure_attribute}; SameSite=Lax"
    ))
    .ok()
}
