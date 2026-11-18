use std::time::Duration;

use coral_db::UnixNanos;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::{Map, Value};

use super::config::ResolvedOidcProvider;

const CLOCK_SKEW_SECONDS: u64 = 60;
const MAX_SUBJECT_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 255;

/// The check that refused a token.
///
/// A rejection is a fixed reason and nothing else, so it can reach the
/// operator's log as an `error.type` code: no claim, key ID or other value
/// the provider supplied travels with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TokenRejection {
    /// The provider returned no access token, and admission requires one.
    Missing,
    /// The token is not a JWT whose header and claims can be read.
    Malformed,
    /// The `alg` is one this module does not verify, or one the provider
    /// does not advertise.
    Algorithm,
    /// The header marks an extension as critical. This module knows none.
    CriticalHeader,
    /// The header names no `kid`, or the JWKS holds no usable key for it.
    KeyNotFound,
    /// More than one usable JWKS key has the token's `kid`.
    KeyAmbiguous,
    /// The signature does not verify under the selected key.
    Signature,
    /// `iss` is absent or is not the configured issuer.
    Issuer,
    /// `aud` or `azp` does not name the expected audience.
    Audience,
    /// `iat`, `exp` or `nbf` is absent, malformed or outside the clock skew
    /// this module allows.
    Times,
    /// `nonce` is not the one this sign-in sent.
    Nonce,
    /// `sub` is absent or unusable, or an access token's `sub` is not the ID
    /// token's.
    Subject,
    /// A claim the operator requires is absent or has another value.
    RequiredClaim,
    /// The display name claim is not usable text.
    DisplayName,
}

impl TokenRejection {
    /// The rejection of an ID token as a stable kind, for an event's
    /// `error.type`.
    pub(super) const fn id_token_code(self) -> &'static str {
        self.codes().0
    }

    /// The rejection of an access token as a stable kind, for an event's
    /// `error.type`.
    pub(super) const fn access_token_code(self) -> &'static str {
        self.codes().1
    }

    /// The code of this rejection for an ID token, then for an access token.
    /// Each is written out whole, so a code read in a log is found here.
    const fn codes(self) -> (&'static str, &'static str) {
        match self {
            Self::Missing => ("id_token_missing", "access_token_missing"),
            Self::Malformed => ("id_token_malformed", "access_token_malformed"),
            Self::Algorithm => ("id_token_algorithm", "access_token_algorithm"),
            Self::CriticalHeader => ("id_token_critical_header", "access_token_critical_header"),
            Self::KeyNotFound => ("id_token_key_not_found", "access_token_key_not_found"),
            Self::KeyAmbiguous => ("id_token_key_ambiguous", "access_token_key_ambiguous"),
            Self::Signature => ("id_token_signature", "access_token_signature"),
            Self::Issuer => ("id_token_issuer", "access_token_issuer"),
            Self::Audience => ("id_token_audience", "access_token_audience"),
            Self::Times => ("id_token_times", "access_token_times"),
            Self::Nonce => ("id_token_nonce", "access_token_nonce"),
            Self::Subject => ("id_token_subject", "access_token_subject"),
            Self::RequiredClaim => ("id_token_required_claim", "access_token_required_claim"),
            Self::DisplayName => ("id_token_display_name", "access_token_display_name"),
        }
    }
}

/// A fully verified identity. Intentionally does not implement `Debug`.
///
/// The upstream identity is the verified `issuer` plus the raw `subject`, and
/// that pair alone keys the user directory. A configured principal projection
/// can constrain admission but never replaces or accompanies that identity.
pub(super) struct ValidatedOidcIdentity {
    /// Issuer the ID token's signature and `iss` claim were verified against.
    pub(super) issuer: String,
    /// Raw upstream `sub` claim, unmodified and non-empty.
    pub(super) subject: String,
    pub(super) display_name: Option<String>,
    pub(super) profile_name: Option<String>,
    pub(super) email: Option<String>,
    pub(super) email_verified: Option<bool>,
    pub(super) hosted_domain: Option<String>,
    /// The token's `iat`: the instant the provider issued these profile claims.
    pub(super) profile_observed_at: UnixNanos,
}

/// Verifies the ID token of one sign-in at `now`, the caller's one reading
/// of the clock. Nothing here reads another.
pub(super) fn validate_id_token(
    provider: &ResolvedOidcProvider,
    token: &str,
    expected_nonce: &str,
    advertised_algorithms: &[String],
    jwks: &JwkSet,
    now: UnixNanos,
) -> Result<ValidatedOidcIdentity, TokenRejection> {
    let claims = verified_jwt_claims(
        &provider.issuer,
        &provider.client_id,
        token,
        advertised_algorithms,
        jwks,
    )?;
    validate_claims(provider, expected_nonce, &claims, now.whole_seconds())
}

/// Admission-only evidence from the same upstream code exchange, checked at
/// `now`, the instant that dated the ID token. No claim or
/// token is returned to callers, persisted, or forwarded to Coral clients.
pub(super) fn validate_access_token_admission(
    provider: &ResolvedOidcProvider,
    token: Option<&str>,
    identity: &ValidatedOidcIdentity,
    jwks: &JwkSet,
    now: UnixNanos,
) -> Result<(), TokenRejection> {
    let Some(policy) = &provider.access_token else {
        return Ok(());
    };
    // This is an explicit RS256 JWT profile, not an assumption that arbitrary
    // OAuth access tokens are JWTs or share ID-token signing algorithms.
    let claims = verified_jwt_claims(
        &provider.issuer,
        &policy.audience,
        token.ok_or(TokenRejection::Missing)?,
        &["RS256".to_string()],
        jwks,
    )?;
    let claims = claims.as_object().ok_or(TokenRejection::Malformed)?;
    validate_token_times(claims, now.whole_seconds())?;
    if claims.get("iss").and_then(Value::as_str) != Some(identity.issuer.as_str()) {
        return Err(TokenRejection::Issuer);
    }
    if claims.get("sub").and_then(Value::as_str) != Some(identity.subject.as_str()) {
        return Err(TokenRejection::Subject);
    }
    if claims.get("aud").and_then(Value::as_str) != Some(policy.audience.as_str()) {
        return Err(TokenRejection::Audience);
    }
    for (name, expected) in &policy.required_claims {
        if claims.get(name) != Some(expected) {
            return Err(TokenRejection::RequiredClaim);
        }
    }
    Ok(())
}

/// The claims of a token whose signature, issuer and audience are verified.
/// Its times are not: the caller checks them with [`validate_token_times`].
fn verified_jwt_claims(
    issuer: &str,
    audience: &str,
    token: &str,
    advertised_algorithms: &[String],
    jwks: &JwkSet,
) -> Result<Value, TokenRejection> {
    let header = decode_header(token).map_err(|_error| TokenRejection::Malformed)?;
    let algorithm = header.alg;
    let (key_algorithm, algorithm_name) =
        algorithm_policy(algorithm).ok_or(TokenRejection::Algorithm)?;
    if !advertised_algorithms
        .iter()
        .any(|advertised| advertised == algorithm_name)
    {
        return Err(TokenRejection::Algorithm);
    }
    if header
        .crit
        .as_ref()
        .is_some_and(|critical| !critical.is_empty())
    {
        return Err(TokenRejection::CriticalHeader);
    }
    let kid = header
        .kid
        .as_deref()
        .filter(|kid| !kid.is_empty())
        .ok_or(TokenRejection::KeyNotFound)?;
    let mut matching = jwks.keys.iter().filter(|key| {
        key.common.key_id.as_deref() == Some(kid)
            && key
                .common
                .key_algorithm
                .is_none_or(|algorithm| algorithm == key_algorithm)
            && valid_key(key, algorithm)
    });
    let key = matching.next().ok_or(TokenRejection::KeyNotFound)?;
    if matching.next().is_some() {
        return Err(TokenRejection::KeyAmbiguous);
    }
    let decoding_key = DecodingKey::from_jwk(key).map_err(|_error| TokenRejection::KeyNotFound)?;
    let mut validation = Validation::new(algorithm);
    // `decode` would read the host's clock for `exp` and `nbf`.
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    decode::<Value>(token, &decoding_key, &validation)
        .map(|verified| verified.claims)
        .map_err(|error| decode_rejection(error.kind()))
}

/// Names the check behind a failed `decode`. It reads the error's kind and
/// never its text. A claim name in a kind is one this module told `decode`
/// to check, not text from the token.
fn decode_rejection(kind: &ErrorKind) -> TokenRejection {
    match kind {
        ErrorKind::InvalidSignature => TokenRejection::Signature,
        ErrorKind::InvalidAlgorithm => TokenRejection::Algorithm,
        ErrorKind::ExpiredSignature | ErrorKind::ImmatureSignature => TokenRejection::Times,
        ErrorKind::InvalidIssuer => TokenRejection::Issuer,
        ErrorKind::InvalidAudience => TokenRejection::Audience,
        ErrorKind::InvalidSubject => TokenRejection::Subject,
        ErrorKind::MissingRequiredClaim(claim) | ErrorKind::InvalidClaimFormat(claim) => {
            match claim.as_str() {
                "exp" | "nbf" => TokenRejection::Times,
                "iss" => TokenRejection::Issuer,
                "aud" => TokenRejection::Audience,
                "sub" => TokenRejection::Subject,
                _ => TokenRejection::Malformed,
            }
        }
        _ => TokenRejection::Malformed,
    }
}

fn algorithm_policy(algorithm: Algorithm) -> Option<(KeyAlgorithm, &'static str)> {
    match algorithm {
        Algorithm::RS256 => Some((KeyAlgorithm::RS256, "RS256")),
        Algorithm::RS384 => Some((KeyAlgorithm::RS384, "RS384")),
        Algorithm::RS512 => Some((KeyAlgorithm::RS512, "RS512")),
        Algorithm::PS256 => Some((KeyAlgorithm::PS256, "PS256")),
        Algorithm::PS384 => Some((KeyAlgorithm::PS384, "PS384")),
        Algorithm::PS512 => Some((KeyAlgorithm::PS512, "PS512")),
        Algorithm::ES256 => Some((KeyAlgorithm::ES256, "ES256")),
        Algorithm::ES384 => Some((KeyAlgorithm::ES384, "ES384")),
        Algorithm::EdDSA => Some((KeyAlgorithm::EdDSA, "EdDSA")),
        _ => None,
    }
}

fn valid_key(key: &Jwk, algorithm: Algorithm) -> bool {
    let usage_ok = key
        .common
        .public_key_use
        .as_ref()
        .is_none_or(|usage| usage == &PublicKeyUse::Signature);
    let operations_ok = key.common.key_operations.as_ref().is_none_or(|operations| {
        operations
            .iter()
            .any(|operation| operation == &KeyOperations::Verify)
    });
    usage_ok
        && operations_ok
        && match (&key.algorithm, algorithm) {
            (
                AlgorithmParameters::RSA(_),
                Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::PS256
                | Algorithm::PS384
                | Algorithm::PS512,
            ) => true,
            (AlgorithmParameters::EllipticCurve(parameters), Algorithm::ES256) => {
                parameters.curve == EllipticCurve::P256
            }
            (AlgorithmParameters::EllipticCurve(parameters), Algorithm::ES384) => {
                parameters.curve == EllipticCurve::P384
            }
            (AlgorithmParameters::OctetKeyPair(parameters), Algorithm::EdDSA) => {
                parameters.curve == EllipticCurve::Ed25519
            }
            _ => false,
        }
}

/// Checks the claims of a token whose signature is verified, at `now`, the
/// caller's one reading of the clock in Unix seconds.
fn validate_claims(
    provider: &ResolvedOidcProvider,
    expected_nonce: &str,
    claims: &Value,
    now: u64,
) -> Result<ValidatedOidcIdentity, TokenRejection> {
    let claims = claims.as_object().ok_or(TokenRejection::Malformed)?;
    validate_token_times(claims, now)?;
    if claims.get("iss").and_then(Value::as_str) != Some(provider.issuer.as_str()) {
        return Err(TokenRejection::Issuer);
    }
    if claims.get("nonce").and_then(Value::as_str) != Some(expected_nonce) {
        return Err(TokenRejection::Nonce);
    }
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .ok_or(TokenRejection::Subject)?;
    if !valid_subject(subject)
        || subject
            == coral_workspace::local_organization_owner()
                .user_id()
                .as_str()
    {
        return Err(TokenRejection::Subject);
    }
    validate_audience(claims, &provider.client_id)?;
    for (claim, expected) in &provider.required_claims {
        if !claims
            .get(claim)
            .is_some_and(|actual| required_claim_matches(actual, expected))
        {
            return Err(TokenRejection::RequiredClaim);
        }
    }
    let display_name = match claims.get(&provider.display_name_claim) {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if value.trim().is_empty() => None,
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.len() > MAX_DISPLAY_NAME_BYTES || value.chars().any(char::is_control) {
                return Err(TokenRejection::DisplayName);
            }
            Some(value.to_string())
        }
        Some(_) => return Err(TokenRejection::DisplayName),
    };
    Ok(ValidatedOidcIdentity {
        issuer: provider.issuer.clone(),
        subject: subject.to_string(),
        display_name,
        profile_name: profile_text(claims, &provider.name_claim, MAX_DISPLAY_NAME_BYTES),
        email: profile_text(claims, &provider.email_claim, 320),
        email_verified: claims
            .get(&provider.email_verified_claim)
            .and_then(Value::as_bool),
        hosted_domain: profile_text(claims, "hd", 255),
        profile_observed_at: UnixNanos::new(
            i64::try_from(
                Duration::try_from_secs_f64(
                    claims
                        .get("iat")
                        .and_then(Value::as_f64)
                        .ok_or(TokenRejection::Times)?,
                )
                .map_err(|_error| TokenRejection::Times)?
                .as_nanos(),
            )
            .map_err(|_error| TokenRejection::Times)?,
        ),
    })
}

// Optional malformed profile claims do not revoke otherwise eligible login or grants.
// Preserve provider spelling; normalization belongs only to human email lookup.
fn profile_text(claims: &Map<String, Value>, claim: &str, max_bytes: usize) -> Option<String> {
    claims
        .get(claim)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.trim().is_empty()
                && value.len() <= max_bytes
                && !value.chars().any(char::is_control)
        })
        .map(ToOwned::to_owned)
}

/// Every failure here is [`TokenRejection::Times`].
fn validate_token_times(claims: &Map<String, Value>, now: u64) -> Result<(), TokenRejection> {
    let issued_at = claim_numeric_date(claims, "iat")?;
    let expires_at = claim_numeric_date(claims, "exp")?;
    if expires_at <= issued_at
        || expires_at.saturating_add(CLOCK_SKEW_SECONDS) < now
        || issued_at > now.saturating_add(CLOCK_SKEW_SECONDS)
    {
        return Err(TokenRejection::Times);
    }
    if let Some(not_before) = claims.get("nbf")
        && numeric_date(not_before)? > now.saturating_add(CLOCK_SKEW_SECONDS)
    {
        return Err(TokenRejection::Times);
    }
    Ok(())
}

fn valid_subject(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SUBJECT_BYTES && value.is_ascii()
}

fn claim_numeric_date(claims: &Map<String, Value>, name: &str) -> Result<u64, TokenRejection> {
    claims
        .get(name)
        .ok_or(TokenRejection::Times)
        .and_then(numeric_date)
}

fn numeric_date(value: &Value) -> Result<u64, TokenRejection> {
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    let value = value.as_f64().ok_or(TokenRejection::Times)?;
    if value < 0.0 {
        return Err(TokenRejection::Times);
    }
    Duration::try_from_secs_f64(value.round())
        .map(|duration| duration.as_secs())
        .map_err(|_error| TokenRejection::Times)
}

/// Every failure here is [`TokenRejection::Audience`].
fn validate_audience(claims: &Map<String, Value>, client_id: &str) -> Result<(), TokenRejection> {
    let audience = claims.get("aud").ok_or(TokenRejection::Audience)?;
    let multiple = match audience {
        Value::String(value) if value == client_id => false,
        Value::Array(values)
            if !values.is_empty()
                && values.iter().all(Value::is_string)
                && values.iter().any(|value| value.as_str() == Some(client_id)) =>
        {
            values.len() > 1
        }
        _ => return Err(TokenRejection::Audience),
    };
    let authorized_party = claims.get("azp");
    if authorized_party.is_some_and(|value| value.as_str() != Some(client_id))
        || (multiple && authorized_party.is_none())
    {
        return Err(TokenRejection::Audience);
    }
    Ok(())
}

fn required_claim_matches(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Array(expected) => actual
            .as_array()
            .is_some_and(|actual| expected.iter().all(|value| actual.contains(value))),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            actual == expected
                || actual
                    .as_array()
                    .is_some_and(|actual| actual.contains(expected))
        }
        Value::Object(_) => actual == expected,
    }
}

#[cfg(test)]
pub(in crate::auth) mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use jsonwebtoken::{EncodingKey, Header, encode};

    use serde_json::json;
    use std::path::Path;

    use super::*;
    const LOCAL_PRINCIPAL_ID: &str = "coral:local";

    const RSA_N: &str = coral_test_support::RSA_TEST_PUBLIC_MODULUS;
    const RSA_PRIVATE_DER: &str = coral_test_support::RSA_TEST_PRIVATE_KEY_DER;

    fn provider(extra: &str) -> ResolvedOidcProvider {
        let extra = extra.replace("[required_claims]", "[auth.provider.required_claims]");
        let settings = crate::bootstrap::auth_settings(&format!(
            "[auth]
             [auth.session]
             [listeners.auth]
             public_url = 'http://localhost:9080'
             [auth.provider]
             issuer = 'http://localhost/issuer'
             client_id = 'provider-client'
             client_secret = 'secret'
             {extra}"
        ))
        .expect("valid auth config")
        .expect("auth settings");
        let signing_key = STANDARD.encode(coral_test_support::p256_pkcs8());
        let (settings, _issuer) = settings
            .resolve_runtime_dependencies(Path::new("config.toml"), &|name| {
                Ok((name == "CORAL_SESSION_SIGNING_KEY").then(|| signing_key.clone()))
            })
            .expect("resolved runtime dependencies");
        settings.provider().clone()
    }

    /// The instant these tests date and validate tokens at, in 2100: nothing
    /// here reads a clock, and no real clock is near it. It is the last
    /// nanosecond of its second, and a token carries whole seconds.
    const NOW: UnixNanos = UnixNanos::new(4_102_444_800 * 1_000_000_000 + 999_999_999);

    /// The claims of an ID token that is valid at [`NOW`].
    fn claims() -> Value {
        claims_at(NOW)
    }

    /// The claims of an ID token the provider issued a second before `now`.
    pub(in crate::auth) fn claims_at(now: UnixNanos) -> Value {
        let now = now.whole_seconds();
        json!({
            "iss": "http://localhost/issuer", "aud": "provider-client", "sub": "subject",
            "iat": now - 1, "exp": now + 300, "nonce": "expected-nonce", "email": " User "
        })
    }

    pub(in crate::auth) fn rsa_key() -> Value {
        json!({"kty":"RSA", "n":RSA_N, "e":"AQAB", "kid":"key-1", "alg":"RS256", "use":"sig", "key_ops":["verify"]})
    }

    pub(in crate::auth) fn set_claim(claims: &mut Value, name: &str, value: Value) {
        let claims = claims.as_object_mut().expect("claims");
        claims.insert(name.into(), value);
    }

    fn jwks(keys: &[Value]) -> JwkSet {
        serde_json::from_value(json!({"keys": keys})).expect("JWKS")
    }

    fn signed(header: &Header, claims: &Value) -> String {
        let der = STANDARD.decode(RSA_PRIVATE_DER).expect("private key");
        encode(header, claims, &EncodingKey::from_rsa_der(&der)).expect("token")
    }

    pub(in crate::auth) fn token(claims: &Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("key-1".into());
        signed(&header, claims)
    }

    /// `token` with the first character of its signature changed.
    fn tampered(token: &str) -> String {
        let mut tampered = token.to_owned();
        let signature = tampered.rfind('.').expect("signature") + 1;
        let replacement = if tampered.as_bytes().get(signature) == Some(&b'A') {
            "B"
        } else {
            "A"
        };
        tampered.replace_range(signature..=signature, replacement);
        tampered
    }

    #[test]
    fn rejects_header_signature_and_jwk_selection_ambiguity() {
        use TokenRejection::{CriticalHeader, KeyAmbiguous, KeyNotFound, Malformed, Signature};
        let provider = provider("");
        let claims = claims();
        let good = token(&claims);
        let validate = |token: &str, algorithm: &str, keys: &[Value]| {
            let algorithms = [algorithm.to_owned()];
            validate_id_token(
                &provider,
                token,
                "expected-nonce",
                &algorithms,
                &jwks(keys),
                NOW,
            )
            .map(|_identity| ())
        };
        for (field, value) in [
            ("kid", json!("other")),
            ("kid", json!("")),
            ("alg", json!("PS256")),
            ("use", json!("enc")),
            ("key_ops", json!(["sign"])),
        ] {
            let mut key = rsa_key();
            set_claim(&mut key, field, value);
            assert_eq!(
                validate(&good, "RS256", &[key]),
                Err(KeyNotFound),
                "{field}"
            );
        }
        let mut key_without_alg = rsa_key();
        key_without_alg.as_object_mut().expect("key").remove("alg");
        let other_family = json!({
            "kty":"EC", "crv":"P-256", "x":"AA", "y":"AA",
            "kid":"key-1", "use":"sig", "key_ops":["verify"]
        });
        for (keys, expected) in [
            (vec![rsa_key(); 2], Err(KeyAmbiguous)),
            (vec![key_without_alg.clone(); 2], Err(KeyAmbiguous)),
            (vec![key_without_alg.clone()], Ok(())),
            (vec![other_family, key_without_alg], Ok(())),
        ] {
            assert_eq!(validate(&good, "RS256", &keys), expected);
        }
        let mut header = Header::new(Algorithm::RS256);
        let without_kid = signed(&header, &claims);
        header.kid = Some(String::new());
        let empty_kid = signed(&header, &claims);
        header.kid = Some("key-1".into());
        header.crit = Some(vec!["custom".into()]);
        header.extras.insert("custom", "value");
        let critical = signed(&header, &claims);
        for (case, token, expected) in [
            ("no JWT", "opaque", Malformed),
            ("no kid", &without_kid, KeyNotFound),
            ("an empty kid", &empty_kid, KeyNotFound),
            ("a critical header", &critical, CriticalHeader),
            ("a tampered signature", &tampered(&good), Signature),
        ] {
            assert_eq!(
                validate(token, "RS256", &[rsa_key()]),
                Err(expected),
                "{case}"
            );
        }
        assert_eq!(
            validate(&good, "ES256", &[rsa_key()]),
            Err(TokenRejection::Algorithm),
            "an algorithm the provider does not advertise"
        );
        let mut hmac = Header::new(Algorithm::HS256);
        hmac.kid = Some("symmetric".into());
        let hmac_token =
            encode(&hmac, &claims, &EncodingKey::from_secret(b"secret")).expect("HMAC token");
        let hmac_key = json!({"kty":"oct", "k":"c2VjcmV0", "kid":"symmetric", "alg":"HS256"});
        assert_eq!(
            validate(&hmac_token, "HS256", &[hmac_key]),
            Err(TokenRejection::Algorithm),
            "a symmetric algorithm, even when advertised"
        );
    }

    /// The signed claims `decode` checks before `validate_claims` sees them
    /// name the same reason as the check that would refuse them later.
    #[test]
    fn a_verified_signature_over_refused_claims_names_the_claim_check() {
        let provider = provider("");
        let now = NOW.whole_seconds();
        for (field, value, expected) in [
            ("iss", Some(json!("wrong")), TokenRejection::Issuer),
            ("iss", None, TokenRejection::Issuer),
            ("aud", Some(json!("other")), TokenRejection::Audience),
            ("aud", None, TokenRejection::Audience),
            ("sub", None, TokenRejection::Subject),
            ("exp", Some(json!(now - 120)), TokenRejection::Times),
            ("exp", Some(json!("not-a-date")), TokenRejection::Times),
            ("exp", None, TokenRejection::Times),
            ("nbf", Some(json!(now + 120)), TokenRejection::Times),
            ("nonce", Some(json!("wrong")), TokenRejection::Nonce),
        ] {
            let mut claims = claims();
            match value {
                Some(value) => set_claim(&mut claims, field, value),
                None => {
                    claims.as_object_mut().expect("claims").remove(field);
                }
            }
            assert_eq!(
                validate_id_token(
                    &provider,
                    &token(&claims),
                    "expected-nonce",
                    &["RS256".into()],
                    &jwks(&[rsa_key()]),
                    NOW,
                )
                .map(|_identity| ()),
                Err(expected),
                "{field}"
            );
        }
    }

    /// A code reaches the log alone, so two checks must never share one, and
    /// a code must say which token it is about.
    #[test]
    fn every_rejection_has_its_own_code_for_each_token() {
        let rejections = [
            TokenRejection::Missing,
            TokenRejection::Malformed,
            TokenRejection::Algorithm,
            TokenRejection::CriticalHeader,
            TokenRejection::KeyNotFound,
            TokenRejection::KeyAmbiguous,
            TokenRejection::Signature,
            TokenRejection::Issuer,
            TokenRejection::Audience,
            TokenRejection::Times,
            TokenRejection::Nonce,
            TokenRejection::Subject,
            TokenRejection::RequiredClaim,
            TokenRejection::DisplayName,
        ];
        let mut codes = std::collections::BTreeSet::new();
        for rejection in rejections {
            let id_token = rejection.id_token_code();
            let access_token = rejection.access_token_code();
            let reason = id_token.strip_prefix("id_token_").expect("ID token code");
            assert_eq!(access_token.strip_prefix("access_token_"), Some(reason));
            assert!(
                reason
                    .chars()
                    .all(|character| character.is_ascii_lowercase() || character == '_'),
                "{reason}"
            );
            assert!(codes.insert(id_token), "{id_token}");
            assert!(codes.insert(access_token), "{access_token}");
        }
    }

    #[test]
    fn enforces_asymmetric_key_family_curve_and_purpose() {
        let rsa: Jwk = serde_json::from_value(rsa_key()).expect("RSA key");
        for algorithm in [
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::PS384,
            Algorithm::PS512,
        ] {
            assert!(valid_key(&rsa, algorithm));
        }
        for (key, accepted, rejected) in [
            (
                json!({"kty":"EC", "crv":"P-256", "x":"AA", "y":"AA", "use":"sig", "key_ops":["verify"]}),
                Algorithm::ES256,
                Algorithm::ES384,
            ),
            (
                json!({"kty":"EC", "crv":"P-384", "x":"AA", "y":"AA", "use":"sig", "key_ops":["verify"]}),
                Algorithm::ES384,
                Algorithm::ES256,
            ),
            (
                json!({"kty":"OKP", "crv":"Ed25519", "x":"AA", "use":"sig", "key_ops":["verify"]}),
                Algorithm::EdDSA,
                Algorithm::ES256,
            ),
        ] {
            let key: Jwk = serde_json::from_value(key).expect("asymmetric key");
            assert!(valid_key(&key, accepted));
            assert!(!valid_key(&key, rejected));
        }
        let symmetric: Jwk =
            serde_json::from_value(json!({"kty":"oct", "k":"AA", "alg":"RS256", "kid":"key-1"}))
                .expect("symmetric key");
        assert!(!valid_key(&symmetric, Algorithm::RS256));
    }

    #[test]
    fn access_token_admission_binds_signature_audience_time_and_oidc_subject() {
        use TokenRejection::{Audience, Issuer, RequiredClaim, Subject, Times};
        let provider = provider(
            "[auth.provider.access_token]\n\
             audience = 'environment-audience'\n\
             [auth.provider.access_token.required_claims]\n\
             org_id = 'org_allowed'",
        );
        let identity = validate_id_token(
            &provider,
            &token(&claims()),
            "expected-nonce",
            &["RS256".into()],
            &jwks(&[rsa_key()]),
            NOW,
        )
        .expect("verified OIDC identity");
        let mut base = claims();
        set_claim(&mut base, "aud", json!("environment-audience"));
        set_claim(&mut base, "org_id", json!("org_allowed"));
        base.as_object_mut().expect("claims").remove("nonce");
        let valid = token(&base);
        let check = |token: Option<&str>, keys: &[Value]| {
            validate_access_token_admission(&provider, token, &identity, &jwks(keys), NOW)
        };
        assert_eq!(check(Some(&valid), &[rsa_key()]), Ok(()));
        assert_eq!(check(None, &[rsa_key()]), Err(TokenRejection::Missing));
        assert_eq!(
            check(Some("opaque"), &[rsa_key()]),
            Err(TokenRejection::Malformed)
        );
        assert_eq!(
            check(Some(&valid), &[rsa_key(), rsa_key()]),
            Err(TokenRejection::KeyAmbiguous)
        );
        let now = NOW.whole_seconds();
        // `None` removes the claim.
        for (field, value, expected) in [
            ("iss", Some(json!("https://another-provider.test")), Issuer),
            ("aud", Some(json!("another-environment")), Audience),
            ("aud", Some(json!("provider-client")), Audience),
            (
                "aud",
                Some(json!(["environment-audience", "another-environment"])),
                Audience,
            ),
            ("sub", Some(json!("another-user")), Subject),
            ("org_id", Some(json!("org_other")), RequiredClaim),
            (
                "org_id",
                Some(json!(["org_allowed", "org_other"])),
                RequiredClaim,
            ),
            ("org_id", Some(Value::Null), RequiredClaim),
            ("exp", Some(json!(now - 120)), Times),
            ("iat", Some(json!(now + 120)), Times),
            ("nbf", Some(json!(now + 120)), Times),
            ("exp", Some(json!("not-a-date")), Times),
            ("iss", None, Issuer),
            ("aud", None, Audience),
            ("sub", None, Subject),
            ("exp", None, Times),
            ("iat", None, Times),
            ("org_id", None, RequiredClaim),
        ] {
            let mut invalid = base.clone();
            match value {
                Some(value) => set_claim(&mut invalid, field, value),
                None => {
                    invalid.as_object_mut().expect("claims").remove(field);
                }
            }
            assert_eq!(
                check(Some(&token(&invalid)), &[rsa_key()]),
                Err(expected),
                "{field}"
            );
        }
        assert_eq!(
            check(Some(&tampered(&valid)), &[rsa_key()]),
            Err(TokenRejection::Signature)
        );
        let mut hmac = Header::new(Algorithm::HS256);
        hmac.kid = Some("key-1".into());
        let forged = encode(&hmac, &base, &EncodingKey::from_secret(b"not-a-public-key"))
            .expect("forged token");
        assert_eq!(
            check(Some(&forged), &[rsa_key()]),
            Err(TokenRejection::Algorithm)
        );
    }

    #[test]
    fn validates_mandatory_oidc_claims_and_time_types() {
        let provider = provider("");
        let base = claims();
        let now = NOW.whole_seconds();
        let mut invalid = Vec::new();
        for (field, value, expected) in [
            ("iss", json!("wrong"), TokenRejection::Issuer),
            ("aud", json!("other"), TokenRejection::Audience),
            (
                "aud",
                json!(["provider-client", 7]),
                TokenRejection::Audience,
            ),
            (
                "aud",
                json!(["provider-client", "other"]),
                TokenRejection::Audience,
            ),
            ("azp", json!("other"), TokenRejection::Audience),
            ("nonce", json!("wrong"), TokenRejection::Nonce),
            ("iat", json!("1"), TokenRejection::Times),
            ("iat", json!(-0.25), TokenRejection::Times),
            ("exp", json!("2"), TokenRejection::Times),
            ("nbf", json!("soon"), TokenRejection::Times),
            ("sub", json!(""), TokenRejection::Subject),
            ("sub", json!("nön-ascii"), TokenRejection::Subject),
            ("sub", json!(LOCAL_PRINCIPAL_ID), TokenRejection::Subject),
            (
                "sub",
                Value::String("x".repeat(MAX_SUBJECT_BYTES + 1)),
                TokenRejection::Subject,
            ),
            ("sub", json!("x".repeat(256)), TokenRejection::Subject),
        ] {
            let mut claims = base.clone();
            set_claim(&mut claims, field, value);
            invalid.push((field, claims, expected));
        }
        let mut reversed = base.clone();
        let expiration = reversed.get("exp").cloned().expect("expiration");
        set_claim(&mut reversed, "iat", expiration);
        invalid.push(("iat", reversed, TokenRejection::Times));
        for (field, claims, expected) in invalid {
            assert_eq!(
                validate_claims(&provider, "expected-nonce", &claims, now).map(|_identity| ()),
                Err(expected),
                "{field} in {claims}"
            );
        }
        assert_eq!(
            validate_claims(
                &provider,
                "expected-nonce",
                &json!(["not", "an", "object"]),
                now
            )
            .map(|_identity| ()),
            Err(TokenRejection::Malformed)
        );
        let seconds = Duration::from_secs(now).as_secs_f64();
        let mut fractional = base.clone();
        set_claim(&mut fractional, "iat", json!(seconds - 0.75));
        set_claim(&mut fractional, "exp", json!(seconds + 300.25));
        set_claim(&mut fractional, "nbf", json!(seconds - 0.25));
        validate_claims(&provider, "expected-nonce", &fractional, now)
            .expect("fractional NumericDate claims");
        let mut multiple = base;
        set_claim(&mut multiple, "aud", json!(["provider-client", "other"]));
        set_claim(&mut multiple, "azp", json!("provider-client"));
        validate_claims(&provider, "expected-nonce", &multiple, now)
            .expect("matching authorized party must allow multiple audiences");
    }

    /// A signed token's time claims are checked against the caller's one
    /// reading of the clock, so a claim one second past the allowed skew is
    /// past it however long the check takes to start. No other clock decides:
    /// neither instant here is near the host's.
    #[test]
    fn token_times_allow_the_clock_skew_and_no_more() {
        const SKEW: u64 = CLOCK_SKEW_SECONDS;
        let provider = provider(
            "[auth.provider.access_token]\n\
             audience = 'environment-audience'",
        );
        let keys = jwks(&[rsa_key()]);
        let algorithms = ["RS256".to_owned()];
        // 2001 and 2100: one long past, one far ahead of any real clock.
        for now in [1_000_000_000_u64, 4_102_444_800] {
            // A token carries whole seconds: the last nanosecond of a second
            // is still that second.
            let instant =
                UnixNanos::new(i64::try_from(now).expect("seconds") * 1_000_000_000 + 999_999_999);
            let dated = |issued_at: u64, expires_at: u64, not_before: Option<u64>| {
                let mut claims = claims();
                set_claim(&mut claims, "iat", json!(issued_at));
                set_claim(&mut claims, "exp", json!(expires_at));
                if let Some(not_before) = not_before {
                    set_claim(&mut claims, "nbf", json!(not_before));
                }
                claims
            };
            let id_token = |claims: &Value| {
                validate_id_token(
                    &provider,
                    &token(claims),
                    "expected-nonce",
                    &algorithms,
                    &keys,
                    instant,
                )
            };
            let Ok(identity) = id_token(&dated(now - 1, now + 300, None)) else {
                panic!("a token dated {now} is valid at {now}");
            };
            for (case, issued_at, expires_at, not_before, accepted) in [
                ("issued at the skew", now + SKEW, now + 300, None, true),
                (
                    "issued past the skew",
                    now + SKEW + 1,
                    now + 300,
                    None,
                    false,
                ),
                (
                    "not before the skew",
                    now - 1,
                    now + 300,
                    Some(now + SKEW),
                    true,
                ),
                (
                    "not before a time past the skew",
                    now - 1,
                    now + 300,
                    Some(now + SKEW + 1),
                    false,
                ),
                // Issued before it expired, so only the skew decides these two.
                (
                    "expired at the skew",
                    now - SKEW - 2,
                    now - SKEW,
                    None,
                    true,
                ),
                (
                    "expired past the skew",
                    now - SKEW - 2,
                    now - SKEW - 1,
                    None,
                    false,
                ),
            ] {
                let claims = dated(issued_at, expires_at, not_before);
                let expected = if accepted {
                    Ok(())
                } else {
                    Err(TokenRejection::Times)
                };
                assert_eq!(
                    id_token(&claims).map(|_identity| ()),
                    expected,
                    "an ID token {case}, at {now}"
                );
                let mut access = claims;
                set_claim(&mut access, "aud", json!("environment-audience"));
                assert_eq!(
                    validate_access_token_admission(
                        &provider,
                        Some(&token(&access)),
                        &identity,
                        &keys,
                        instant,
                    ),
                    expected,
                    "an access token {case}, at {now}"
                );
            }
        }
    }

    /// OIDC subjects remain opaque upstream identifiers, not Coral User IDs.
    #[test]
    fn keeps_verified_issuer_and_raw_subject_independent_of_other_claims() {
        let provider = provider("");
        let now = NOW.whole_seconds();
        let mut claims = claims();
        set_claim(&mut claims, "sub", json!("upstream sub/with spaces"));
        set_claim(&mut claims, "uid", json!(LOCAL_PRINCIPAL_ID));
        let identity =
            validate_claims(&provider, "expected-nonce", &claims, now).expect("verified identity");
        assert_eq!(identity.issuer, "http://localhost/issuer");
        assert_eq!(identity.subject, "upstream sub/with spaces");
    }

    #[test]
    fn profile_freshness_follows_verified_token_issuance_not_validation_order() {
        let provider = provider("");
        let mut older = claims();
        let now = NOW.whole_seconds();
        set_claim(&mut older, "iat", json!(now - 30));
        let mut newer = older.clone();
        set_claim(&mut newer, "iat", json!(now - 10));
        let newer = validate_claims(&provider, "expected-nonce", &newer, now).unwrap();
        let older = validate_claims(&provider, "expected-nonce", &older, now).unwrap();
        assert!(older.profile_observed_at < newer.profile_observed_at);
    }

    #[test]
    fn profile_claims_are_separate_and_email_verification_is_not_login_admission() {
        let provider = provider("display_name_claim = 'email'");
        let now = NOW.whole_seconds();
        for verified in [Value::Null, json!(false), json!(true)] {
            let mut claims = claims();
            set_claim(&mut claims, "name", json!("Ada"));
            set_claim(&mut claims, "email", json!("Ada@Example.test"));
            set_claim(&mut claims, "email_verified", verified.clone());
            set_claim(&mut claims, "hd", json!("example.test"));
            let identity = validate_claims(&provider, "expected-nonce", &claims, now)
                .expect("verification does not gate eligible login");
            assert_eq!(identity.profile_name.as_deref(), Some("Ada"));
            assert_eq!(identity.email.as_deref(), Some("Ada@Example.test"));
            assert_eq!(identity.email_verified, verified.as_bool());
            assert_eq!(identity.hosted_domain.as_deref(), Some("example.test"));
        }
    }

    #[test]
    fn validates_identity_projection_and_required_claim_semantics() {
        let provider = provider(
            "display_name_claim = 'name'
             [required_claims]
             tenant = 'one'
             groups = ['admin', 'dev']",
        );
        let now = NOW.whole_seconds();
        let mut claims = claims();
        set_claim(&mut claims, "name", json!("  Coral User  "));
        set_claim(&mut claims, "tenant", json!(["one", "two"]));
        set_claim(&mut claims, "groups", json!(["dev", "other", "admin"]));
        let identity =
            validate_claims(&provider, "expected-nonce", &claims, now).expect("projected identity");
        assert_eq!(identity.display_name.as_deref(), Some("Coral User"));
        // Presentation and admission claims cannot select another identity.
        assert_eq!(identity.issuer, "http://localhost/issuer");
        assert_eq!(identity.subject, "subject");
        for display in [Value::Null, json!("   ")] {
            set_claim(&mut claims, "name", display);
            assert!(
                validate_claims(&provider, "expected-nonce", &claims, now)
                    .expect("optional display")
                    .display_name
                    .is_none()
            );
        }
        set_claim(&mut claims, "name", json!("Renée Coral"));
        assert_eq!(
            validate_claims(&provider, "expected-nonce", &claims, now)
                .expect("internationalized display name")
                .display_name
                .as_deref(),
            Some("Renée Coral")
        );
        for (field, value, expected) in [
            ("name", json!(7), TokenRejection::DisplayName),
            ("name", json!("Coral\u{0}User"), TokenRejection::DisplayName),
            (
                "name",
                Value::String("x".repeat(MAX_DISPLAY_NAME_BYTES + 1)),
                TokenRejection::DisplayName,
            ),
            ("tenant", json!(["two"]), TokenRejection::RequiredClaim),
            ("groups", json!(["admin"]), TokenRejection::RequiredClaim),
        ] {
            let mut rejected = claims.clone();
            set_claim(&mut rejected, field, value);
            assert_eq!(
                validate_claims(&provider, "expected-nonce", &rejected, now).map(|_identity| ()),
                Err(expected),
                "{field}"
            );
        }
    }
}
