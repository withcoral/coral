//! Sign-in state sealed into the browser instead of kept on the server.
//!
//! Coral stores nothing for a sign-in until the person has signed in at the
//! identity provider. What a later step needs travels with the browser, sealed
//! with AES-256-GCM under a key every Coral process derives from the session
//! signing key. Any process can open what another one sealed, and an anonymous
//! request costs the server no storage.
//!
//! The associated data binds each sealed value to what it belongs to: a
//! pending approval to the browser's approval cookie, and a sign-in session to
//! its OIDC `state`. A value moved to another browser or sign-in does not open.
//! The PKCE verifier and the nonce Coral sends to the identity provider are
//! derived from the OIDC `state`, so they are never carried at all.
//!
//! Rotating the session signing key changes these keys too, so sign-ins in
//! progress at that moment must start again.

use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use ring::{aead, hkdf, hmac};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

const ENVELOPE_VERSION: u8 = 1;
const SIGN_IN_SESSION_AAD: &[u8] = b"coral-oauth-sign-in-session-v1\0";
const PENDING_APPROVAL_AAD: &[u8] = b"coral-oauth-pending-approval-v1\0";
const WORKSPACE_CHOICE_AAD: &[u8] = b"coral-oauth-workspace-choice-v1\0";

/// Keys derived from the session signing key for sealing sign-in state.
pub(crate) struct HandshakeKeys {
    seal: aead::LessSafeKey,
    pkce: hmac::Key,
    nonce: hmac::Key,
}

impl fmt::Debug for HandshakeKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HandshakeKeys(<redacted>)")
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum SealError {
    #[error("sign-in state could not be sealed")]
    Seal,
    #[error("sign-in state exceeds what can be sealed")]
    TooLarge,
}

impl HandshakeKeys {
    /// Derives independent keys for sealing, PKCE and nonces from the
    /// session signing key, so every process holding that key derives the
    /// same ones.
    ///
    /// # Errors
    ///
    /// Returns [`SealError::Seal`] if HKDF refuses an expansion, which it does
    /// only for output lengths these key types never request.
    pub(crate) fn derive(signing_key: &[u8]) -> Result<Self, SealError> {
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(signing_key);
        let seal = prk
            .expand(&[b"coral-oauth-handshake-v1/seal"], &aead::AES_256_GCM)
            .map(aead::UnboundKey::from)
            .map_err(|_unspecified| SealError::Seal)?;
        let pkce = prk
            .expand(&[b"coral-oauth-handshake-v1/pkce"], hmac::HMAC_SHA256)
            .map(hmac::Key::from)
            .map_err(|_unspecified| SealError::Seal)?;
        let nonce = prk
            .expand(&[b"coral-oauth-handshake-v1/nonce"], hmac::HMAC_SHA256)
            .map(hmac::Key::from)
            .map_err(|_unspecified| SealError::Seal)?;
        Ok(Self {
            seal: aead::LessSafeKey::new(seal),
            pkce,
            nonce,
        })
    }

    /// The PKCE verifier Coral presents for the sign-in named by `oidc_state`.
    pub(crate) fn pkce_verifier(&self, oidc_state: &str) -> Zeroizing<String> {
        Zeroizing::new(URL_SAFE_NO_PAD.encode(hmac::sign(&self.pkce, oidc_state.as_bytes())))
    }

    /// The ID token nonce Coral expects for the sign-in named by `oidc_state`.
    pub(crate) fn nonce(&self, oidc_state: &str) -> String {
        URL_SAFE_NO_PAD.encode(hmac::sign(&self.nonce, oidc_state.as_bytes()))
    }

    /// Seals `plaintext` as `version ‖ nonce ‖ ciphertext ‖ tag`, base64url.
    fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<String, SealError> {
        let mut nonce = [0_u8; aead::NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_unspecified| SealError::Seal)?;
        let mut ciphertext = Zeroizing::new(plaintext.to_vec());
        self.seal
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut *ciphertext,
            )
            .map_err(|_unspecified| SealError::Seal)?;
        let mut envelope = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
        envelope.push(ENVELOPE_VERSION);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(URL_SAFE_NO_PAD.encode(envelope))
    }

    /// Opens what [`Self::seal`] produced under the same `aad`.
    fn open(&self, aad: &[u8], sealed: &[u8]) -> Option<Zeroizing<Vec<u8>>> {
        let envelope = URL_SAFE_NO_PAD.decode(sealed).ok()?;
        let (&version, rest) = envelope.split_first()?;
        if version != ENVELOPE_VERSION {
            return None;
        }
        let (nonce, ciphertext) = rest.split_at_checked(aead::NONCE_LEN)?;
        let nonce = aead::Nonce::try_assume_unique_for_key(nonce).ok()?;
        let mut buffer = Zeroizing::new(ciphertext.to_vec());
        let plaintext = self
            .seal
            .open_in_place(nonce, aead::Aad::from(aad), &mut buffer)
            .ok()?;
        Some(Zeroizing::new(plaintext.to_vec()))
    }
}

/// What the OIDC callback needs to finish a sign-in, sealed into its cookie.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SignInSession {
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) client_state: Option<String>,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
    /// The client's self-asserted name, carried only to the Workspace picker
    /// of an Organization MCP Endpoint sign-in.
    pub(crate) client_name: Option<String>,
}

impl SignInSession {
    /// Seals this session for the sign-in named by `oidc_state`, to be opened
    /// before `expires_at_unix_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when a field is too long to frame or randomness
    /// is unavailable.
    pub(crate) fn seal(
        &self,
        keys: &HandshakeKeys,
        oidc_state: &str,
        expires_at_unix_secs: u64,
    ) -> Result<String, SealError> {
        let frame = self.frame(expires_at_unix_secs)?;
        keys.seal(&sign_in_aad(oidc_state), &frame.0)
    }

    /// The length of what [`Self::seal`] returns for this session.
    ///
    /// # Errors
    ///
    /// Returns [`SealError::TooLarge`] when a field is too long to frame.
    pub(crate) fn sealed_len(&self) -> Result<usize, SealError> {
        let envelope = 1 + aead::NONCE_LEN + self.frame(0)?.0.len() + aead::AES_256_GCM.tag_len();
        Ok((4 * envelope).div_ceil(3))
    }

    fn frame(&self, expires_at_unix_secs: u64) -> Result<Frame, SealError> {
        let mut frame = Frame::default();
        frame.put_str(&self.client_id)?;
        frame.put_str(&self.redirect_uri)?;
        frame.put_optional_str(self.client_state.as_deref())?;
        frame.put_str(&self.code_challenge)?;
        frame.put_str(&self.resource)?;
        frame.put_optional_str(self.client_name.as_deref())?;
        frame.put_u64(expires_at_unix_secs);
        Ok(frame)
    }

    /// Opens the session sealed for `oidc_state`, if it is intact, belongs to
    /// that sign-in, and has not expired at `now_unix_secs`.
    pub(crate) fn open(
        keys: &HandshakeKeys,
        oidc_state: &str,
        sealed: &[u8],
        now_unix_secs: u64,
    ) -> Option<Self> {
        let plaintext = keys.open(&sign_in_aad(oidc_state), sealed)?;
        let mut reader = Reader(plaintext.as_slice());
        let session = Self {
            client_id: reader.str()?,
            redirect_uri: reader.str()?,
            client_state: reader.optional_str().ok()?,
            code_challenge: reader.str()?,
            resource: reader.str()?,
            client_name: reader.optional_str().ok()?,
        };
        let expires_at = reader.u64()?;
        (reader.0.is_empty() && now_unix_secs < expires_at).then_some(session)
    }
}

/// An authorization awaiting the person's decision, sealed into the consent form.
///
/// Nothing is stored while the page waits, so rendering consent for an
/// anonymous request costs the server no storage. The seal is bound to the
/// browser's approval cookie, so only the browser shown the page can submit it.
/// Submitting the same page twice starts the provider sign-in twice in that
/// browser, which grants nothing the first submission did not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingApproval {
    pub(crate) client_id: String,
    pub(crate) client_name: String,
    pub(crate) redirect_uri: String,
    pub(crate) client_state: Option<String>,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
}

impl PendingApproval {
    /// Seals this approval for the browser holding `browser_binding`, to be
    /// submitted before `expires_at_unix_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when a field is too long to frame or randomness
    /// is unavailable.
    pub(crate) fn seal(
        &self,
        keys: &HandshakeKeys,
        browser_binding: &[u8],
        expires_at_unix_secs: u64,
    ) -> Result<String, SealError> {
        let mut frame = Frame::default();
        frame.put_str(&self.client_id)?;
        frame.put_str(&self.client_name)?;
        frame.put_str(&self.redirect_uri)?;
        frame.put_optional_str(self.client_state.as_deref())?;
        frame.put_str(&self.code_challenge)?;
        frame.put_str(&self.resource)?;
        frame.put_u64(expires_at_unix_secs);
        keys.seal(&approval_aad(browser_binding), &frame.0)
    }

    /// Opens the approval sealed for `browser_binding`, if it is intact,
    /// belongs to that browser, and has not expired at `now_unix_secs`.
    pub(crate) fn open(
        keys: &HandshakeKeys,
        browser_binding: &[u8],
        sealed: &[u8],
        now_unix_secs: u64,
    ) -> Option<Self> {
        let plaintext = keys.open(&approval_aad(browser_binding), sealed)?;
        let mut reader = Reader(plaintext.as_slice());
        let approval = Self {
            client_id: reader.str()?,
            client_name: reader.str()?,
            redirect_uri: reader.str()?,
            client_state: reader.optional_str().ok()?,
            code_challenge: reader.str()?,
            resource: reader.str()?,
        };
        let expires_at = reader.u64()?;
        (reader.0.is_empty() && now_unix_secs < expires_at).then_some(approval)
    }

    /// The sign-in this approval starts once the person allows it. Only a
    /// sign-in that ends at the Workspace picker keeps the client's name, so
    /// every other sign-in cookie stays as small as before.
    pub(crate) fn into_sign_in(self, picks_workspace: bool) -> SignInSession {
        SignInSession {
            client_id: self.client_id,
            redirect_uri: self.redirect_uri,
            client_state: self.client_state,
            code_challenge: self.code_challenge,
            resource: self.resource,
            client_name: picks_workspace.then_some(self.client_name),
        }
    }
}

/// A finished sign-in at the Organization MCP Endpoint awaiting its User's
/// Workspace choice, sealed into the picker form.
///
/// The seal is bound to the browser's approval cookie, so only the browser
/// that signed in can submit it. It keeps the sign-in's OIDC state, so the
/// one-code-per-sign-in guard still holds when the choice issues the code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingWorkspaceChoice {
    pub(crate) user_id: String,
    pub(crate) oidc_state: String,
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) client_state: Option<String>,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
}

impl PendingWorkspaceChoice {
    /// Seals this choice for the browser holding `browser_binding`, to be
    /// submitted before `expires_at_unix_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when a field is too long to frame or randomness
    /// is unavailable.
    pub(crate) fn seal(
        &self,
        keys: &HandshakeKeys,
        browser_binding: &[u8],
        expires_at_unix_secs: u64,
    ) -> Result<String, SealError> {
        let mut frame = Frame::default();
        frame.put_str(&self.user_id)?;
        frame.put_str(&self.oidc_state)?;
        frame.put_str(&self.client_id)?;
        frame.put_str(&self.redirect_uri)?;
        frame.put_optional_str(self.client_state.as_deref())?;
        frame.put_str(&self.code_challenge)?;
        frame.put_str(&self.resource)?;
        frame.put_u64(expires_at_unix_secs);
        keys.seal(&workspace_choice_aad(browser_binding), &frame.0)
    }

    /// Opens the choice sealed for `browser_binding`, if it is intact,
    /// belongs to that browser, and has not expired at `now_unix_secs`.
    pub(crate) fn open(
        keys: &HandshakeKeys,
        browser_binding: &[u8],
        sealed: &[u8],
        now_unix_secs: u64,
    ) -> Option<Self> {
        let plaintext = keys.open(&workspace_choice_aad(browser_binding), sealed)?;
        let mut reader = Reader(plaintext.as_slice());
        let choice = Self {
            user_id: reader.str()?,
            oidc_state: reader.str()?,
            client_id: reader.str()?,
            redirect_uri: reader.str()?,
            client_state: reader.optional_str().ok()?,
            code_challenge: reader.str()?,
            resource: reader.str()?,
        };
        let expires_at = reader.u64()?;
        (reader.0.is_empty() && now_unix_secs < expires_at).then_some(choice)
    }
}

fn workspace_choice_aad(browser_binding: &[u8]) -> Vec<u8> {
    let mut aad = WORKSPACE_CHOICE_AAD.to_vec();
    aad.extend_from_slice(&Sha256::digest(browser_binding));
    aad
}

fn approval_aad(browser_binding: &[u8]) -> Vec<u8> {
    let mut aad = PENDING_APPROVAL_AAD.to_vec();
    aad.extend_from_slice(&Sha256::digest(browser_binding));
    aad
}

fn sign_in_aad(oidc_state: &str) -> Vec<u8> {
    let mut aad = SIGN_IN_SESSION_AAD.to_vec();
    aad.extend_from_slice(&Sha256::digest(oidc_state.as_bytes()));
    aad
}

/// Length-prefixed fields. Unlike a text encoding, framing never expands what
/// an app sent, so the byte budget an app is held to is what it gets sealed.
#[derive(Default)]
struct Frame(Zeroizing<Vec<u8>>);

impl Frame {
    fn put_str(&mut self, value: &str) -> Result<(), SealError> {
        let length = u16::try_from(value.len()).map_err(|_overflow| SealError::TooLarge)?;
        self.0.extend_from_slice(&length.to_be_bytes());
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn put_optional_str(&mut self, value: Option<&str>) -> Result<(), SealError> {
        self.0.push(u8::from(value.is_some()));
        value.map_or(Ok(()), |value| self.put_str(value))
    }

    fn put_u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
}

struct Reader<'a>(&'a [u8]);

/// A sealed value whose frame does not decode.
struct Malformed;

impl Reader<'_> {
    fn take(&mut self, length: usize) -> Option<&[u8]> {
        let (taken, rest) = self.0.split_at_checked(length)?;
        self.0 = rest;
        Some(taken)
    }

    fn str(&mut self) -> Option<String> {
        let length = u16::from_be_bytes(self.take(2)?.try_into().ok()?);
        String::from_utf8(self.take(usize::from(length))?.to_vec()).ok()
    }

    fn optional_str(&mut self) -> Result<Option<String>, Malformed> {
        match self.take(1).ok_or(Malformed)? {
            [0] => Ok(None),
            [1] => self.str().map(Some).ok_or(Malformed),
            _ => Err(Malformed),
        }
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATE: &str = "oidc-state";
    const NOW: u64 = 1_800_000_000;

    fn keys() -> HandshakeKeys {
        HandshakeKeys::derive(b"session signing key").expect("keys")
    }

    fn session() -> SignInSession {
        SignInSession {
            client_id: "dcr_0123456789abcdef0123456789abcdef".into(),
            redirect_uri: "http://127.0.0.1:54213/callback".into(),
            client_state: Some("client-state".into()),
            code_challenge: "client-challenge".into(),
            resource: "https://mcp.example.test/workspaces/w/mcp".into(),
            client_name: None,
        }
    }

    #[test]
    fn a_sealed_session_opens_in_any_process_holding_the_same_signing_key() {
        let sealed = session().seal(&keys(), STATE, NOW + 300).expect("sealed");
        let other_process = keys();
        assert_eq!(
            SignInSession::open(&other_process, STATE, sealed.as_bytes(), NOW),
            Some(session())
        );
        let without_state = SignInSession {
            client_state: None,
            ..session()
        };
        let sealed = without_state
            .seal(&keys(), STATE, NOW + 300)
            .expect("sealed");
        assert_eq!(
            SignInSession::open(&keys(), STATE, sealed.as_bytes(), NOW),
            Some(without_state)
        );
    }

    #[test]
    fn sealed_len_is_the_length_seal_produces() {
        for client_state in [
            None,
            Some("s"),
            Some("st"),
            Some("sta"),
            Some("a longer client state"),
        ] {
            let session = SignInSession {
                client_state: client_state.map(str::to_owned),
                ..session()
            };
            let sealed = session.seal(&keys(), STATE, NOW).expect("sealed");
            assert_eq!(session.sealed_len(), Ok(sealed.len()), "{client_state:?}");
        }
    }

    #[test]
    fn a_session_opens_only_for_its_sign_in_key_and_lifetime_and_intact() {
        let sealed = session().seal(&keys(), STATE, NOW + 300).expect("sealed");
        let mut altered = sealed.clone().into_bytes();
        let middle = altered.len() / 2;
        if let Some(byte) = altered.get_mut(middle) {
            *byte = if *byte == b'A' { b'B' } else { b'A' };
        }
        let rotated = HandshakeKeys::derive(b"rotated signing key").expect("keys");
        for (case, keys, state, sealed, now) in [
            (
                "another sign-in",
                keys(),
                "other-state",
                sealed.as_bytes().to_vec(),
                NOW,
            ),
            (
                "a rotated key",
                rotated,
                STATE,
                sealed.as_bytes().to_vec(),
                NOW,
            ),
            (
                "at expiry",
                keys(),
                STATE,
                sealed.as_bytes().to_vec(),
                NOW + 300,
            ),
            ("altered", keys(), STATE, altered, NOW),
            ("not base64url", keys(), STATE, b"!!".to_vec(), NOW),
            ("empty", keys(), STATE, Vec::new(), NOW),
        ] {
            assert_eq!(
                SignInSession::open(&keys, state, &sealed, now),
                None,
                "{case}"
            );
        }
    }

    fn approval() -> PendingApproval {
        PendingApproval {
            client_id: "https://client.example.test/oauth/client.json".into(),
            client_name: "Client <b>".into(),
            redirect_uri: "https://client.example.test/callback".into(),
            client_state: None,
            code_challenge: "client-challenge".into(),
            resource: "https://mcp.example.test/workspaces/w/mcp".into(),
        }
    }

    #[test]
    fn an_approval_opens_only_in_its_browser_within_its_lifetime() {
        const BROWSER: [u8; 32] = [0x42; 32];
        let sealed = approval()
            .seal(&keys(), &BROWSER, NOW + 300)
            .expect("sealed");
        assert_eq!(
            PendingApproval::open(&keys(), &BROWSER, sealed.as_bytes(), NOW),
            Some(approval())
        );
        for (case, browser, now) in [
            ("another browser", [0x43; 32], NOW),
            ("at expiry", BROWSER, NOW + 300),
        ] {
            assert_eq!(
                PendingApproval::open(&keys(), &browser, sealed.as_bytes(), now),
                None,
                "{case}"
            );
        }
        let session = session().seal(&keys(), STATE, NOW + 300).expect("sealed");
        assert_eq!(
            PendingApproval::open(&keys(), STATE.as_bytes(), session.as_bytes(), NOW),
            None,
            "a sealed sign-in session is not an approval"
        );
    }

    #[test]
    fn derived_secrets_are_valid_pkce_and_distinct_per_sign_in_and_purpose() {
        let keys = keys();
        let verifier = keys.pkce_verifier(STATE);
        let nonce = keys.nonce(STATE);
        assert_eq!(verifier.len(), 43);
        assert!(
            verifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        );
        assert_ne!(verifier.as_str(), nonce);
        assert_ne!(*verifier, *keys.pkce_verifier("other-state"));
        assert_eq!(
            *verifier,
            *HandshakeKeys::derive(b"session signing key")
                .expect("keys")
                .pkce_verifier(STATE)
        );
    }
}
