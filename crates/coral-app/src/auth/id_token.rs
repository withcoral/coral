use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::{Map, Value};

use super::config::ResolvedOidcProvider;

const CLOCK_SKEW_SECONDS: u64 = 60;
const MAX_SUBJECT_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 255;

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
    pub(super) profile_observed_at_unix_nanos: i64,
}

pub(super) fn validate_id_token(
    provider: &ResolvedOidcProvider,
    token: &str,
    expected_nonce: &str,
    advertised_algorithms: &[String],
    jwks: &JwkSet,
) -> Result<ValidatedOidcIdentity, ()> {
    let claims = verified_jwt_claims(
        &provider.issuer,
        &provider.client_id,
        token,
        advertised_algorithms,
        jwks,
    )?;
    validate_claims(provider, expected_nonce, &claims)
}

/// Admission-only evidence from the same upstream code exchange. No claim or
/// token is returned to callers, persisted, or forwarded to Coral clients.
pub(super) fn validate_access_token_admission(
    provider: &ResolvedOidcProvider,
    token: Option<&str>,
    identity: &ValidatedOidcIdentity,
    jwks: &JwkSet,
) -> Result<(), ()> {
    let Some(policy) = &provider.access_token else {
        return Ok(());
    };
    // This is an explicit RS256 JWT profile, not an assumption that arbitrary
    // OAuth access tokens are JWTs or share ID-token signing algorithms.
    let claims = verified_jwt_claims(
        &provider.issuer,
        &policy.audience,
        token.ok_or(())?,
        &["RS256".to_string()],
        jwks,
    )?;
    let claims = claims.as_object().ok_or(())?;
    validate_token_times(claims)?;
    if claims.get("iss").and_then(Value::as_str) != Some(identity.issuer.as_str())
        || claims.get("sub").and_then(Value::as_str) != Some(identity.subject.as_str())
        || claims.get("aud").and_then(Value::as_str) != Some(policy.audience.as_str())
    {
        return Err(());
    }
    for (name, expected) in &policy.required_claims {
        if claims.get(name) != Some(expected) {
            return Err(());
        }
    }
    Ok(())
}

fn verified_jwt_claims(
    issuer: &str,
    audience: &str,
    token: &str,
    advertised_algorithms: &[String],
    jwks: &JwkSet,
) -> Result<Value, ()> {
    let header = decode_header(token).map_err(|_error| ())?;
    let algorithm = header.alg;
    let (key_algorithm, algorithm_name) = algorithm_policy(algorithm).ok_or(())?;
    if !advertised_algorithms
        .iter()
        .any(|advertised| advertised == algorithm_name)
        || header
            .crit
            .as_ref()
            .is_some_and(|critical| !critical.is_empty())
    {
        return Err(());
    }
    let kid = header
        .kid
        .as_deref()
        .filter(|kid| !kid.is_empty())
        .ok_or(())?;
    let mut matching = jwks.keys.iter().filter(|key| {
        key.common.key_id.as_deref() == Some(kid)
            && key
                .common
                .key_algorithm
                .is_none_or(|algorithm| algorithm == key_algorithm)
            && valid_key(key, algorithm)
    });
    let key = matching.next().ok_or(())?;
    if matching.next().is_some() {
        return Err(());
    }
    let decoding_key = DecodingKey::from_jwk(key).map_err(|_error| ())?;
    let mut validation = Validation::new(algorithm);
    validation.leeway = CLOCK_SKEW_SECONDS;
    validation.validate_nbf = true;
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    decode::<Value>(token, &decoding_key, &validation)
        .map(|verified| verified.claims)
        .map_err(|_error| ())
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

fn validate_claims(
    provider: &ResolvedOidcProvider,
    expected_nonce: &str,
    claims: &Value,
) -> Result<ValidatedOidcIdentity, ()> {
    let claims = claims.as_object().ok_or(())?;
    validate_token_times(claims)?;
    if claims.get("iss").and_then(Value::as_str) != Some(provider.issuer.as_str())
        || claims.get("nonce").and_then(Value::as_str) != Some(expected_nonce)
    {
        return Err(());
    }
    let subject = claims.get("sub").and_then(Value::as_str).ok_or(())?;
    if !valid_subject(subject)
        || subject
            == coral_workspace::local_organization_owner()
                .user_id()
                .as_str()
    {
        return Err(());
    }
    validate_audience(claims, &provider.client_id)?;
    for (claim, expected) in &provider.required_claims {
        if !claims
            .get(claim)
            .is_some_and(|actual| required_claim_matches(actual, expected))
        {
            return Err(());
        }
    }
    let display_name = match claims.get(&provider.display_name_claim) {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if value.trim().is_empty() => None,
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.len() > MAX_DISPLAY_NAME_BYTES || value.chars().any(char::is_control) {
                return Err(());
            }
            Some(value.to_string())
        }
        Some(_) => return Err(()),
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
        profile_observed_at_unix_nanos: i64::try_from(
            Duration::try_from_secs_f64(claims.get("iat").and_then(Value::as_f64).ok_or(())?)
                .map_err(|_error| ())?
                .as_nanos(),
        )
        .map_err(|_error| ())?,
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

fn validate_token_times(claims: &Map<String, Value>) -> Result<(), ()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ())?
        .as_secs();
    let issued_at = claim_numeric_date(claims, "iat")?;
    let expires_at = claim_numeric_date(claims, "exp")?;
    if expires_at <= issued_at
        || expires_at.saturating_add(CLOCK_SKEW_SECONDS) < now
        || issued_at > now.saturating_add(CLOCK_SKEW_SECONDS)
    {
        return Err(());
    }
    if let Some(not_before) = claims.get("nbf")
        && numeric_date(not_before)? > now.saturating_add(CLOCK_SKEW_SECONDS)
    {
        return Err(());
    }
    Ok(())
}

fn valid_subject(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SUBJECT_BYTES && value.is_ascii()
}

fn claim_numeric_date(claims: &Map<String, Value>, name: &str) -> Result<u64, ()> {
    claims.get(name).ok_or(()).and_then(numeric_date)
}

fn numeric_date(value: &Value) -> Result<u64, ()> {
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    let value = value.as_f64().ok_or(())?;
    if value < 0.0 {
        return Err(());
    }
    Duration::try_from_secs_f64(value.round())
        .map(|duration| duration.as_secs())
        .map_err(|_error| ())
}

fn validate_audience(claims: &Map<String, Value>, client_id: &str) -> Result<(), ()> {
    let audience = claims.get("aud").ok_or(())?;
    let multiple = match audience {
        Value::String(value) if value == client_id => false,
        Value::Array(values)
            if !values.is_empty()
                && values.iter().all(Value::is_string)
                && values.iter().any(|value| value.as_str() == Some(client_id)) =>
        {
            values.len() > 1
        }
        _ => return Err(()),
    };
    let authorized_party = claims.get("azp");
    if authorized_party.is_some_and(|value| value.as_str() != Some(client_id))
        || (multiple && authorized_party.is_none())
    {
        return Err(());
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
    use jsonwebtoken::{EncodingKey, Header, encode, get_current_timestamp};

    use serde_json::json;
    use std::path::Path;

    use super::*;
    use crate::auth::config::AuthSettings;
    const LOCAL_PRINCIPAL_ID: &str = "coral:local";

    const RSA_N: &str = coral_test_support::RSA_TEST_PUBLIC_MODULUS;
    const RSA_PRIVATE_DER: &str = coral_test_support::RSA_TEST_PRIVATE_KEY_DER;

    fn provider(extra: &str) -> ResolvedOidcProvider {
        let extra = extra.replace("[required_claims]", "[auth.provider.required_claims]");
        let settings = AuthSettings::from_toml(&format!(
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

    pub(in crate::auth) fn claims() -> Value {
        let now = get_current_timestamp();
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

    #[test]
    fn rejects_header_signature_and_jwk_selection_ambiguity() {
        let provider = provider("");
        let claims = claims();
        let good_token = token(&claims);
        let validate = |token: &str, algorithms: &[String], keys: Vec<Value>| {
            validate_id_token(&provider, token, "expected-nonce", algorithms, &jwks(&keys)).is_err()
        };
        assert!(validate(&good_token, &["ES256".into()], vec![rsa_key()]));
        assert!(validate(
            &good_token,
            &["RS256".into()],
            vec![rsa_key(), rsa_key()]
        ));
        for (field, value) in [
            ("kid", Value::String("other".into())),
            ("kid", Value::String(String::new())),
            ("alg", Value::String("PS256".into())),
            ("use", Value::String("enc".into())),
            ("key_ops", json!(["sign"])),
        ] {
            let mut key = rsa_key();
            set_claim(&mut key, field, value);
            assert!(validate(&good_token, &["RS256".into()], vec![key]));
        }
        let mut key_without_alg = rsa_key();
        key_without_alg.as_object_mut().expect("key").remove("alg");
        assert!(!validate(
            &good_token,
            &["RS256".into()],
            vec![key_without_alg.clone()]
        ));
        assert!(!validate(
            &good_token,
            &["RS256".into()],
            vec![
                json!({
                    "kty":"EC", "crv":"P-256", "x":"AA", "y":"AA",
                    "kid":"key-1", "use":"sig", "key_ops":["verify"]
                }),
                key_without_alg.clone(),
            ]
        ));
        assert!(validate(
            &good_token,
            &["RS256".into()],
            vec![key_without_alg.clone(), key_without_alg]
        ));
        let mut missing_kid = Header::new(Algorithm::RS256);
        assert!(validate(
            &signed(&missing_kid, &claims),
            &["RS256".into()],
            vec![rsa_key()]
        ));
        missing_kid.kid = Some(String::new());
        assert!(validate(
            &signed(&missing_kid, &claims),
            &["RS256".into()],
            vec![rsa_key()]
        ));
        let mut critical = Header::new(Algorithm::RS256);
        critical.kid = Some("key-1".into());
        critical.crit = Some(vec!["custom".into()]);
        critical.extras.insert("custom", "value");
        assert!(validate(
            &signed(&critical, &claims),
            &["RS256".into()],
            vec![rsa_key()]
        ));
        let mut tampered = good_token;
        let signature = tampered.rfind('.').expect("signature") + 1;
        let replacement = if tampered.as_bytes().get(signature) == Some(&b'A') {
            "B"
        } else {
            "A"
        };
        tampered.replace_range(signature..=signature, replacement);
        assert!(validate(&tampered, &["RS256".into()], vec![rsa_key()]));
        let mut hmac = Header::new(Algorithm::HS256);
        hmac.kid = Some("symmetric".into());
        let hmac_token =
            encode(&hmac, &claims, &EncodingKey::from_secret(b"secret")).expect("HMAC token");
        assert!(validate(
            &hmac_token,
            &["HS256".into()],
            vec![json!({"kty":"oct", "k":"c2VjcmV0", "kid":"symmetric", "alg":"HS256"})]
        ));
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
        )
        .expect("verified OIDC identity");
        let mut base = claims();
        set_claim(&mut base, "aud", json!("environment-audience"));
        set_claim(&mut base, "org_id", json!("org_allowed"));
        base.as_object_mut().expect("claims").remove("nonce");
        let valid = token(&base);
        let check = |token: Option<&str>, keys: &[Value]| {
            validate_access_token_admission(&provider, token, &identity, &jwks(keys))
        };
        assert!(check(Some(&valid), &[rsa_key()]).is_ok());
        assert!(check(None, &[rsa_key()]).is_err());
        assert!(check(Some("opaque"), &[rsa_key()]).is_err());
        assert!(check(Some(&valid), &[rsa_key(), rsa_key()]).is_err());
        for (field, value) in [
            ("iss", json!("https://another-provider.test")),
            ("aud", json!("another-environment")),
            ("aud", json!("provider-client")),
            (
                "aud",
                json!(["environment-audience", "another-environment"]),
            ),
            ("sub", json!("another-user")),
            ("org_id", json!("org_other")),
            ("org_id", json!(["org_allowed", "org_other"])),
            ("org_id", Value::Null),
            ("exp", json!(get_current_timestamp() - 120)),
            ("iat", json!(get_current_timestamp() + 120)),
            ("nbf", json!(get_current_timestamp() + 120)),
            ("exp", json!("not-a-date")),
        ] {
            let mut invalid = base.clone();
            set_claim(&mut invalid, field, value);
            assert!(
                check(Some(&token(&invalid)), &[rsa_key()]).is_err(),
                "{field}"
            );
        }
        for field in ["iss", "aud", "sub", "exp", "iat", "org_id"] {
            let mut missing = base.clone();
            missing.as_object_mut().expect("claims").remove(field);
            assert!(
                check(Some(&token(&missing)), &[rsa_key()]).is_err(),
                "{field}"
            );
        }
        let mut tampered = valid;
        let signature = tampered.rfind('.').expect("signature") + 1;
        let replacement = if tampered.as_bytes().get(signature) == Some(&b'A') {
            "B"
        } else {
            "A"
        };
        tampered.replace_range(signature..=signature, replacement);
        assert!(check(Some(&tampered), &[rsa_key()]).is_err());
        let mut hmac = Header::new(Algorithm::HS256);
        hmac.kid = Some("key-1".into());
        let forged = encode(&hmac, &base, &EncodingKey::from_secret(b"not-a-public-key"))
            .expect("forged token");
        assert!(check(Some(&forged), &[rsa_key()]).is_err());
    }

    #[test]
    fn validates_mandatory_oidc_claims_and_time_types() {
        let provider = provider("");
        let base = claims();
        let mut invalid = Vec::new();
        for (field, value) in [
            ("iss", json!("wrong")),
            ("aud", json!("other")),
            ("aud", json!(["provider-client", 7])),
            ("aud", json!(["provider-client", "other"])),
            ("azp", json!("other")),
            ("nonce", json!("wrong")),
            ("iat", json!("1")),
            ("iat", json!(-0.25)),
            ("exp", json!("2")),
            ("nbf", json!("soon")),
            ("sub", json!("")),
            ("sub", json!("nön-ascii")),
            ("sub", json!(LOCAL_PRINCIPAL_ID)),
            ("sub", Value::String("x".repeat(MAX_SUBJECT_BYTES + 1))),
        ] {
            let mut claims = base.clone();
            set_claim(&mut claims, field, value);
            invalid.push(claims);
        }
        for (field, value) in [
            ("iat", json!(get_current_timestamp() + 61)),
            ("exp", json!(get_current_timestamp() - 61)),
            ("nbf", json!(get_current_timestamp() + 61)),
            ("sub", json!("x".repeat(256))),
        ] {
            let mut claims = base.clone();
            set_claim(&mut claims, field, value);
            invalid.push(claims);
        }
        let mut reversed = base.clone();
        let expiration = reversed.get("exp").cloned().expect("expiration");
        set_claim(&mut reversed, "iat", expiration);
        invalid.push(reversed);
        for claims in invalid {
            let Err(_error) = validate_claims(&provider, "expected-nonce", &claims) else {
                panic!("invalid OIDC claims must be rejected");
            };
        }
        let now = Duration::from_secs(get_current_timestamp()).as_secs_f64();
        let mut fractional = base.clone();
        set_claim(&mut fractional, "iat", json!(now - 0.75));
        set_claim(&mut fractional, "exp", json!(now + 300.25));
        set_claim(&mut fractional, "nbf", json!(now - 0.25));
        validate_claims(&provider, "expected-nonce", &fractional)
            .expect("fractional NumericDate claims");
        let mut multiple = base;
        set_claim(&mut multiple, "aud", json!(["provider-client", "other"]));
        set_claim(&mut multiple, "azp", json!("provider-client"));
        validate_claims(&provider, "expected-nonce", &multiple)
            .expect("matching authorized party must allow multiple audiences");
    }

    /// OIDC subjects remain opaque upstream identifiers, not Coral User IDs.
    #[test]
    fn keeps_verified_issuer_and_raw_subject_independent_of_other_claims() {
        let provider = provider("");
        let mut claims = claims();
        set_claim(&mut claims, "sub", json!("upstream sub/with spaces"));
        set_claim(&mut claims, "uid", json!(LOCAL_PRINCIPAL_ID));
        let identity =
            validate_claims(&provider, "expected-nonce", &claims).expect("verified identity");
        assert_eq!(identity.issuer, "http://localhost/issuer");
        assert_eq!(identity.subject, "upstream sub/with spaces");
    }

    #[test]
    fn profile_freshness_follows_verified_token_issuance_not_validation_order() {
        let provider = provider("");
        let mut older = claims();
        let now = get_current_timestamp();
        set_claim(&mut older, "iat", json!(now - 30));
        let mut newer = older.clone();
        set_claim(&mut newer, "iat", json!(now - 10));
        let newer = validate_claims(&provider, "expected-nonce", &newer).unwrap();
        let older = validate_claims(&provider, "expected-nonce", &older).unwrap();
        assert!(older.profile_observed_at_unix_nanos < newer.profile_observed_at_unix_nanos);
    }

    #[test]
    fn profile_claims_are_separate_and_email_verification_is_not_login_admission() {
        let provider = provider("display_name_claim = 'email'");
        for verified in [Value::Null, json!(false), json!(true)] {
            let mut claims = claims();
            set_claim(&mut claims, "name", json!("Ada"));
            set_claim(&mut claims, "email", json!("Ada@Example.test"));
            set_claim(&mut claims, "email_verified", verified.clone());
            set_claim(&mut claims, "hd", json!("example.test"));
            let identity = validate_claims(&provider, "expected-nonce", &claims)
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
        let mut claims = claims();
        set_claim(&mut claims, "name", json!("  Coral User  "));
        set_claim(&mut claims, "tenant", json!(["one", "two"]));
        set_claim(&mut claims, "groups", json!(["dev", "other", "admin"]));
        let identity =
            validate_claims(&provider, "expected-nonce", &claims).expect("projected identity");
        assert_eq!(identity.display_name.as_deref(), Some("Coral User"));
        // Presentation and admission claims cannot select another identity.
        assert_eq!(identity.issuer, "http://localhost/issuer");
        assert_eq!(identity.subject, "subject");
        for display in [Value::Null, json!("   ")] {
            set_claim(&mut claims, "name", display);
            assert!(
                validate_claims(&provider, "expected-nonce", &claims)
                    .expect("optional display")
                    .display_name
                    .is_none()
            );
        }
        set_claim(&mut claims, "name", json!("Renée Coral"));
        assert_eq!(
            validate_claims(&provider, "expected-nonce", &claims)
                .expect("internationalized display name")
                .display_name
                .as_deref(),
            Some("Renée Coral")
        );
        for (field, value) in [
            ("name", json!(7)),
            ("name", json!("Coral\u{0}User")),
            (
                "name",
                Value::String("x".repeat(MAX_DISPLAY_NAME_BYTES + 1)),
            ),
            ("tenant", json!(["two"])),
            ("groups", json!(["admin"])),
        ] {
            let mut rejected = claims.clone();
            set_claim(&mut rejected, field, value);
            let Err(_error) = validate_claims(&provider, "expected-nonce", &rejected) else {
                panic!("invalid identity projection claims must be rejected");
            };
        }
    }
}
