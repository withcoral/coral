use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::{Map, Value};

use super::config::ResolvedOidcProvider;
use crate::identity::PrincipalId;

const CLOCK_SKEW_SECONDS: u64 = 60;
const MAX_SUBJECT_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 255;

/// A fully verified identity. Intentionally does not implement `Debug`.
///
/// The upstream identity is the verified `issuer` plus the raw `subject`, and
/// that pair alone keys the user directory. `principal_claim` is a separately
/// configured projection of the same token: it is carried only so login
/// provisioning can recompute the pre-v1 task-attribution digest, and it is
/// deliberately not derived from — nor allowed to stand in for — the subject.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by OAuth callback descendants")
)]
pub(super) struct ValidatedOidcIdentity {
    /// Issuer the ID token's signature and `iss` claim were verified against.
    pub(super) issuer: String,
    /// Raw upstream `sub` claim, unmodified and non-empty.
    pub(super) subject: String,
    /// Value of the provider's configured `principal_claim`.
    pub(super) principal_claim: String,
    pub(super) display_name: Option<String>,
}

pub(super) fn validate_id_token(
    provider: &ResolvedOidcProvider,
    token: &str,
    expected_nonce: &str,
    advertised_algorithms: &[String],
    jwks: &JwkSet,
) -> Result<ValidatedOidcIdentity, ()> {
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
    validation.set_issuer(&[provider.issuer.as_str()]);
    validation.set_audience(&[provider.client_id.as_str()]);
    let claims = decode::<Value>(token, &decoding_key, &validation)
        .map_err(|_error| ())?
        .claims;
    validate_claims(provider, expected_nonce, &claims)
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
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ())?
        .as_secs();
    let issued_at = claim_numeric_date(claims, "iat")?;
    let expires_at = claim_numeric_date(claims, "exp")?;
    if expires_at <= issued_at
        || expires_at.saturating_add(CLOCK_SKEW_SECONDS) < now
        || issued_at > now.saturating_add(CLOCK_SKEW_SECONDS)
        || claims.get("iss").and_then(Value::as_str) != Some(provider.issuer.as_str())
        || claims.get("nonce").and_then(Value::as_str) != Some(expected_nonce)
    {
        return Err(());
    }
    if let Some(not_before) = claims.get("nbf") {
        let not_before = numeric_date(not_before)?;
        if not_before > now.saturating_add(CLOCK_SKEW_SECONDS) {
            return Err(());
        }
    }
    let subject = claims.get("sub").and_then(Value::as_str).ok_or(())?;
    if !valid_subject(subject) {
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
    let principal_claim = claims
        .get(&provider.principal_claim)
        .and_then(Value::as_str)
        .filter(|principal| valid_subject(principal) && PrincipalId::parse(principal).is_ok())
        .ok_or(())?
        .to_string();
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
        principal_claim,
        display_name,
    })
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
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use serde_json::json;
    use std::path::Path;

    use super::*;
    use crate::auth::config::AuthSettings;
    use crate::identity::LOCAL_PRINCIPAL_ID;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
    const RSA_PRIVATE_DER: &str = "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKdWUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZDpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7jE0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5JnLnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSSbYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBlxyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp19m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P07mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZmY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIamQOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gnPYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJHUvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==";

    fn provider(extra: &str) -> ResolvedOidcProvider {
        let extra = extra.replace("[required_claims]", "[auth.provider.required_claims]");
        let settings = AuthSettings::from_toml(&format!(
            "[auth]
             [auth.session]
             [auth.authorization_server]
             issuer = 'http://localhost:9080'
             [auth.provider]
             issuer = 'http://localhost/issuer'
             client_id = 'provider-client'
             client_secret = 'secret'
             redirect_uri = 'http://localhost:9080/auth/oidc/callback'
             {extra}"
        ))
        .expect("valid auth config")
        .expect("auth settings");
        let signing_key = STANDARD.encode(
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key"),
        );
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
        critical.extras.insert("custom".into(), "value".into());
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

    /// The directory key is the verified issuer plus the raw `sub`. It survives
    /// whether the configured principal claim happens to be `sub` or names a
    /// different claim entirely, and the subject keeps a shape the principal
    /// claim would be rejected for.
    #[test]
    fn keeps_verified_issuer_and_raw_subject_independent_of_the_principal_claim() {
        let default_claim = provider("");
        let identity = validate_claims(&default_claim, "expected-nonce", &claims())
            .expect("default principal claim");
        assert_eq!(identity.issuer, "http://localhost/issuer");
        assert_eq!(identity.subject, "subject");
        assert_eq!(identity.principal_claim, "subject");

        let projected = provider("principal_claim = 'uid'");
        let mut claims = claims();
        set_claim(&mut claims, "sub", json!("upstream sub/with spaces"));
        set_claim(&mut claims, "uid", json!("projected-claim"));
        let identity =
            validate_claims(&projected, "expected-nonce", &claims).expect("projected identity");
        assert_eq!(identity.subject, "upstream sub/with spaces");
        assert_eq!(identity.principal_claim, "projected-claim");
    }

    #[test]
    fn validates_identity_projection_and_required_claim_semantics() {
        let provider = provider(
            "principal_claim = 'uid'
             display_name_claim = 'name'
             [required_claims]
             tenant = 'one'
             groups = ['admin', 'dev']",
        );
        let mut claims = claims();
        set_claim(&mut claims, "uid", json!("Case-Sensitive"));
        set_claim(&mut claims, "name", json!("  Coral User  "));
        set_claim(&mut claims, "tenant", json!(["one", "two"]));
        set_claim(&mut claims, "groups", json!(["dev", "other", "admin"]));
        let identity =
            validate_claims(&provider, "expected-nonce", &claims).expect("projected identity");
        assert_eq!(identity.principal_claim, "Case-Sensitive");
        assert_eq!(identity.display_name.as_deref(), Some("Coral User"));
        // The directory key is the verified issuer plus the raw `sub`, neither of
        // which the configured principal claim may displace.
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
            ("uid", json!(" surrounded ")),
            ("uid", json!("interior space")),
            ("uid", json!("interior\u{1}control")),
            ("uid", json!(LOCAL_PRINCIPAL_ID)),
            ("uid", json!("é")),
            ("uid", Value::String("x".repeat(MAX_SUBJECT_BYTES + 1))),
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
