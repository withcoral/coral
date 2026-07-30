use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, PublicKeyUse, ThumbprintHash,
};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const DEFAULT_ISSUER: &str = "coral";
const CLOCK_SKEW: Duration = Duration::from_mins(1);
const SESSION_TOKEN_ALGORITHM: Algorithm = Algorithm::ES256;

pub(crate) type SessionTokenError = String;

#[derive(Clone)]
pub(crate) struct SessionTokenIssuer {
    pub(super) issuer: String,
    signing_key: Arc<EncodingKey>,
    signing_key_id: String,
    verifier: SessionTokenVerifier,
    pub(super) access_token_ttl: Duration,
}

impl fmt::Debug for SessionTokenIssuer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTokenIssuer")
            .field("issuer", &self.issuer)
            .field("signing_key", &"<redacted>")
            .field("signing_key_id", &self.signing_key_id)
            .field(
                "verification_key_count",
                &self.verifier.verification_keys.len(),
            )
            .field("access_token_ttl", &self.access_token_ttl)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct SessionTokenVerifier {
    issuer: String,
    verification_keys: Arc<HashMap<String, DecodingKey>>,
    verification_jwks: Arc<JwkSet>,
    access_token_ttl: Duration,
}

impl fmt::Debug for SessionTokenVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTokenVerifier")
            .field("issuer", &self.issuer)
            .field("verification_key_count", &self.verification_keys.len())
            .field("access_token_ttl", &self.access_token_ttl)
            .finish_non_exhaustive()
    }
}

impl SessionTokenIssuer {
    pub(crate) fn new(
        issuer: Option<&str>,
        signing_key: impl AsRef<[u8]>,
        access_token_ttl: Duration,
    ) -> Result<Self, SessionTokenError> {
        let signing_key = EncodingKey::from_ec_der(signing_key.as_ref());
        let mut signing_jwk = Jwk::from_encoding_key(&signing_key, SESSION_TOKEN_ALGORITHM)
            .map_err(|_error| {
                config_error("signing key must be a PKCS#8 P-256 private key encoded as DER bytes")
            })?;
        signing_jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        let signing_key_id = signing_jwk.thumbprint(ThumbprintHash::SHA256);
        signing_jwk.common.key_id = Some(signing_key_id.clone());

        let verifier = SessionTokenVerifier::new(
            issuer,
            JwkSet {
                keys: vec![signing_jwk],
            },
            access_token_ttl,
        )?;

        Ok(Self {
            issuer: verifier.issuer.clone(),
            signing_key: Arc::new(signing_key),
            signing_key_id,
            verifier,
            access_token_ttl,
        })
    }

    pub(crate) fn issue_access_token(
        &self,
        subject: &str,
        client_id: &str,
        audience: &str,
    ) -> Result<IssuedAccessToken, SessionTokenError> {
        if subject.trim().is_empty() {
            return Err(config_error("subject must not be empty"));
        }
        if client_id.trim().is_empty()
            || client_id.trim() != client_id
            || audience.trim().is_empty()
            || audience.trim() != audience
        {
            return Err(config_error(
                "client_id and audience must be non-empty and have no surrounding whitespace",
            ));
        }
        let issued_at = unix_timestamp()?;
        let expires_at = issued_at
            .checked_add(self.access_token_ttl.as_secs())
            .ok_or_else(|| config_error("access token expiry overflowed"))?;
        let claims = SessionTokenClaims {
            iss: self.issuer.clone(),
            aud: audience.to_string(),
            sub: subject.to_string(),
            jti: Uuid::new_v4().to_string(),
            client_id: client_id.to_string(),
            exp: expires_at,
            iat: issued_at,
            nbf: issued_at,
        };
        let mut header = Header::new(SESSION_TOKEN_ALGORITHM);
        header.kid = Some(self.signing_key_id.clone());
        header.typ = Some("at+jwt".to_string());
        let access_token = encode(&header, &claims, &self.signing_key)
            .map_err(|error| format!("failed to sign Coral access token: {error}"))?;
        Ok(IssuedAccessToken {
            access_token,
            expires_at,
        })
    }

    pub(crate) fn verifier(&self) -> SessionTokenVerifier {
        self.verifier.clone()
    }

    pub(crate) fn verification_jwks(&self) -> &JwkSet {
        &self.verifier.verification_jwks
    }
}

impl SessionTokenVerifier {
    pub(crate) fn new(
        issuer: Option<&str>,
        verification_jwks: JwkSet,
        access_token_ttl: Duration,
    ) -> Result<Self, SessionTokenError> {
        let issuer = normalized_or_default(issuer, DEFAULT_ISSUER)
            .trim_end_matches('/')
            .to_string();
        if issuer.is_empty() {
            return Err(config_error("issuer must not be empty"));
        }
        if access_token_ttl.is_zero() {
            return Err(config_error(
                "access_token_ttl_seconds must be greater than 0",
            ));
        }
        if verification_jwks.keys.is_empty() {
            return Err(config_error(
                "at least one session-token verification key is required",
            ));
        }

        let mut verification_keys = HashMap::with_capacity(verification_jwks.keys.len());
        for jwk in &verification_jwks.keys {
            let (key_id, key) = verification_key(jwk)?;
            if verification_keys.insert(key_id.clone(), key).is_some() {
                return Err(config_error(format!(
                    "duplicate session-token verification key id `{key_id}`"
                )));
            }
        }

        Ok(Self {
            issuer,
            verification_keys: Arc::new(verification_keys),
            verification_jwks: Arc::new(verification_jwks),
            access_token_ttl,
        })
    }

    #[cfg(test)]
    pub(crate) fn verification_jwks(&self) -> &JwkSet {
        &self.verification_jwks
    }

    pub(crate) fn validate_access_token(
        &self,
        token: &str,
        accepted_audiences: &[&str],
    ) -> Result<ValidatedSession, SessionTokenError> {
        if accepted_audiences.is_empty()
            || accepted_audiences
                .iter()
                .any(|audience| audience.is_empty() || audience.trim() != *audience)
        {
            return Err(config_error(
                "accepted audiences must be non-empty and have no surrounding whitespace",
            ));
        }
        let header =
            decode_header(token).map_err(|error| format!("invalid Coral access token: {error}"))?;
        if header.alg != SESSION_TOKEN_ALGORITHM {
            return Err(invalid_token("unsupported signing algorithm"));
        }
        if header.typ.as_deref() != Some("at+jwt") {
            return Err(invalid_token("access token type must be `at+jwt`"));
        }
        let key_id = header
            .kid
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_token("missing signing key id"))?;
        let verification_key = self
            .verification_keys
            .get(key_id)
            .ok_or_else(|| invalid_token("unknown signing key id"))?;

        let mut validation = Validation::new(SESSION_TOKEN_ALGORITHM);
        validation.set_audience(accepted_audiences);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_required_spec_claims(&[
            "aud",
            "client_id",
            "exp",
            "iat",
            "iss",
            "jti",
            "nbf",
            "sub",
        ]);
        validation.validate_nbf = true;
        validation.leeway = CLOCK_SKEW.as_secs();
        let claims = decode::<SessionTokenClaims>(token, verification_key, &validation)
            .map_err(|error| format!("invalid Coral access token: {error}"))?
            .claims;
        self.validate_claims(&claims)?;
        Ok(ValidatedSession {
            token_id: claims.jti,
            audience: claims.aud,
            client_id: claims.client_id,
            subject: claims.sub,
        })
    }

    fn validate_claims(&self, claims: &SessionTokenClaims) -> Result<(), SessionTokenError> {
        let invalid = |message: &str| format!("invalid Coral access token: {message}");
        if claims.sub.trim().is_empty()
            || claims.client_id.trim().is_empty()
            || claims.client_id.trim() != claims.client_id
            || claims.jti.trim().is_empty()
            || claims.jti.trim() != claims.jti
        {
            return Err(invalid("subject, client_id, and jti must be valid"));
        }
        let now = unix_timestamp()?;
        if claims.iat > now.saturating_add(CLOCK_SKEW.as_secs()) {
            return Err(invalid("issued-at timestamp is in the future"));
        }
        let latest_exp = now
            .checked_add(self.access_token_ttl.as_secs())
            .and_then(|value| value.checked_add(CLOCK_SKEW.as_secs()))
            .ok_or_else(|| config_error("configured access token TTL is too large"))?;
        if claims.exp > latest_exp {
            return Err(invalid("expiration exceeds configured access token TTL"));
        }
        let lifetime = claims
            .exp
            .checked_sub(claims.iat)
            .ok_or_else(|| invalid("expiration is earlier than issued-at timestamp"))?;
        if lifetime > self.access_token_ttl.as_secs() {
            return Err(invalid(
                "token lifetime exceeds configured access token TTL",
            ));
        }
        Ok(())
    }
}

fn verification_key(jwk: &Jwk) -> Result<(String, DecodingKey), SessionTokenError> {
    let key_id = jwk
        .common
        .key_id
        .as_deref()
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .ok_or_else(|| config_error("session-token verification keys must have a non-empty `kid`"))?
        .to_string();
    if jwk.common.key_algorithm != Some(KeyAlgorithm::ES256) {
        return Err(config_error(format!(
            "session-token verification key `{key_id}` must use ES256"
        )));
    }
    if jwk.common.public_key_use != Some(PublicKeyUse::Signature) {
        return Err(config_error(format!(
            "session-token verification key `{key_id}` must declare `use` as `sig`"
        )));
    }
    if jwk.common.key_operations.is_some() {
        return Err(config_error(format!(
            "session-token verification key `{key_id}` must not combine `use` with `key_ops`"
        )));
    }
    let AlgorithmParameters::EllipticCurve(parameters) = &jwk.algorithm else {
        return Err(config_error(format!(
            "session-token verification key `{key_id}` must be a P-256 EC key"
        )));
    };
    if parameters.curve != EllipticCurve::P256 {
        return Err(config_error(format!(
            "session-token verification key `{key_id}` must use the P-256 curve"
        )));
    }
    for coordinate in [&parameters.x, &parameters.y] {
        let coordinate = URL_SAFE_NO_PAD.decode(coordinate).map_err(|_error| {
            config_error(format!(
                "session-token verification key `{key_id}` has an invalid coordinate"
            ))
        })?;
        if coordinate.len() != 32 {
            return Err(config_error(format!(
                "session-token verification key `{key_id}` has an invalid coordinate length"
            )));
        }
    }
    let key = DecodingKey::from_jwk(jwk).map_err(|_error| {
        config_error(format!(
            "session-token verification key `{key_id}` is invalid"
        ))
    })?;
    Ok((key_id, key))
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IssuedAccessToken {
    pub(crate) access_token: String,
    pub(crate) expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedSession {
    pub(crate) token_id: String,
    pub(crate) audience: String,
    pub(crate) client_id: String,
    pub(crate) subject: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SessionTokenClaims {
    iss: String,
    aud: String,
    sub: String,
    jti: String,
    client_id: String,
    exp: u64,
    iat: u64,
    nbf: u64,
}

fn normalized_or_default<'a>(value: Option<&'a str>, default: &'a str) -> &'a str {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
}

fn config_error(message: impl Into<String>) -> SessionTokenError {
    format!("invalid auth.session configuration: {}", message.into())
}

fn invalid_token(message: impl Into<String>) -> SessionTokenError {
    format!("invalid Coral access token: {}", message.into())
}

fn unix_timestamp() -> Result<u64, SessionTokenError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};

    use super::*;

    const ISSUER: &str = "https://coral.example.test/";
    const MCP_AUDIENCE: &str = "https://coral.example.test/mcp";
    const BFF_AUDIENCE: &str = "https://app.example.test";
    const CLIENT_ID: &str = "https://client.example.test/client.json";

    fn signing_key() -> Vec<u8> {
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key")
            .as_ref()
            .to_vec()
    }

    fn test_issuer_with_key(key: &[u8]) -> SessionTokenIssuer {
        SessionTokenIssuer::new(Some(ISSUER), key, Duration::from_hours(1)).unwrap()
    }

    fn test_issuer() -> SessionTokenIssuer {
        test_issuer_with_key(&signing_key())
    }

    fn claims(issuer: &SessionTokenIssuer) -> SessionTokenClaims {
        let now = unix_timestamp().expect("timestamp");
        SessionTokenClaims {
            iss: issuer.issuer.clone(),
            aud: MCP_AUDIENCE.to_string(),
            sub: "user-123".to_string(),
            jti: Uuid::new_v4().to_string(),
            client_id: CLIENT_ID.to_string(),
            exp: now + issuer.access_token_ttl.as_secs(),
            iat: now,
            nbf: now,
        }
    }

    fn changed(
        mut claims: SessionTokenClaims,
        change: impl FnOnce(&mut SessionTokenClaims),
    ) -> SessionTokenClaims {
        change(&mut claims);
        claims
    }

    fn sign(
        issuer: &SessionTokenIssuer,
        claims: &SessionTokenClaims,
        key_id: Option<&str>,
    ) -> String {
        let mut header = Header::new(SESSION_TOKEN_ALGORITHM);
        header.kid = key_id.map(str::to_string);
        header.typ = Some("at+jwt".to_string());
        encode(&header, claims, &issuer.signing_key).expect("signed token")
    }

    fn assert_invalid(issuer: &SessionTokenIssuer, claims: &SessionTokenClaims) {
        let token = sign(issuer, claims, Some(&issuer.signing_key_id));
        issuer
            .verifier()
            .validate_access_token(&token, &[MCP_AUDIENCE])
            .expect_err("invalid token");
    }

    fn decode_unverified_claims(token: &str) -> serde_json::Value {
        jsonwebtoken::dangerous::insecure_decode::<serde_json::Value>(token)
            .expect("test-issued access token should decode")
            .claims
    }

    fn string_claim<'a>(claims: &'a serde_json::Value, name: &str) -> &'a str {
        claims
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("`{name}` claim should be a string"))
    }
    fn only_jwk(jwks: &JwkSet) -> &Jwk {
        jwks.keys.first().expect("one verification key")
    }

    fn only_jwk_mut(jwks: &mut JwkSet) -> &mut Jwk {
        jwks.keys.first_mut().expect("one verification key")
    }

    #[test]
    fn token_configuration_rejects_invalid_inputs_and_redacts_secrets() {
        let issuer = test_issuer();
        let debug = format!("{issuer:?}");
        assert!(debug.contains("<redacted>"));
        SessionTokenIssuer::new(None, b"not a P-256 key", Duration::from_hours(1))
            .expect_err("invalid key");
        SessionTokenIssuer::new(None, signing_key(), Duration::ZERO).expect_err("zero TTL");
    }

    #[test]
    fn access_token_wire_format_and_validated_claims_match() {
        let issuer = test_issuer();
        let verifier = issuer.verifier();
        let access = issuer
            .issue_access_token("issuer.example|opaque:subject/123", CLIENT_ID, MCP_AUDIENCE)
            .unwrap();
        let header = decode_header(&access.access_token).unwrap();
        assert_eq!(header.alg, Algorithm::ES256);
        assert_eq!(header.kid.as_deref(), Some(issuer.signing_key_id.as_str()));
        assert_eq!(header.typ.as_deref(), Some("at+jwt"));
        let claims = decode_unverified_claims(&access.access_token);
        assert_eq!(string_claim(&claims, "aud"), MCP_AUDIENCE);
        assert_eq!(string_claim(&claims, "client_id"), CLIENT_ID);
        let token_id = string_claim(&claims, "jti");
        assert!(!token_id.is_empty());
        assert!(claims.get("scope").is_none());
        let session = verifier
            .validate_access_token(&access.access_token, &[MCP_AUDIENCE])
            .unwrap();
        assert_eq!(session.token_id, token_id);
        assert_eq!(session.audience, MCP_AUDIENCE);
        assert_eq!(session.client_id, CLIENT_ID);
        assert_eq!(session.subject, "issuer.example|opaque:subject/123");
    }

    #[test]
    fn access_token_rejects_invalid_claims_headers_and_signatures() {
        let issuer = test_issuer();
        let verifier = issuer.verifier();
        let original = claims(&issuer);
        let invalid = [
            changed(original.clone(), |c| c.iss = "other".into()),
            changed(original.clone(), |c| c.aud = "other".into()),
            changed(original.clone(), |c| c.sub = " ".into()),
            changed(original.clone(), |c| c.client_id = " ".into()),
            changed(original.clone(), |c| c.jti = " ".into()),
        ];
        for claims in &invalid {
            assert_invalid(&issuer, claims);
        }
        let mut missing_client_id = serde_json::to_value(&original).unwrap();
        missing_client_id
            .as_object_mut()
            .unwrap()
            .remove("client_id");
        let mut access_token_header = Header::new(SESSION_TOKEN_ALGORITHM);
        access_token_header.kid = Some(issuer.signing_key_id.clone());
        access_token_header.typ = Some("at+jwt".to_string());
        let missing_client_id = encode(
            &access_token_header,
            &missing_client_id,
            &issuer.signing_key,
        )
        .unwrap();
        verifier
            .validate_access_token(&missing_client_id, &[MCP_AUDIENCE])
            .expect_err("missing client_id");
        let mut wrong_algorithm_header = Header::new(Algorithm::HS256);
        wrong_algorithm_header.kid = Some(issuer.signing_key_id.clone());
        let wrong_algorithm = encode(
            &wrong_algorithm_header,
            &original,
            &EncodingKey::from_secret(b"test-only-HMAC-key-that-is-long-enough"),
        )
        .unwrap();
        verifier
            .validate_access_token(&wrong_algorithm, &[MCP_AUDIENCE])
            .expect_err("wrong algorithm");
        let mut wrong_type_header = Header::new(SESSION_TOKEN_ALGORITHM);
        wrong_type_header.kid = Some(issuer.signing_key_id.clone());
        let wrong_type = encode(&wrong_type_header, &original, &issuer.signing_key).unwrap();
        verifier
            .validate_access_token(&wrong_type, &[MCP_AUDIENCE])
            .expect_err("wrong token type");
        let other = test_issuer();
        let signed_by_other = sign(&other, &original, Some(&issuer.signing_key_id));
        verifier
            .validate_access_token(&signed_by_other, &[MCP_AUDIENCE])
            .expect_err("wrong key");
        let missing_key_id = sign(&issuer, &original, None);
        verifier
            .validate_access_token(&missing_key_id, &[MCP_AUDIENCE])
            .expect_err("missing key id");
        let wrong_key_id = sign(&issuer, &original, Some("unknown"));
        verifier
            .validate_access_token(&wrong_key_id, &[MCP_AUDIENCE])
            .expect_err("unknown key id");
    }

    #[test]
    fn access_token_rejects_invalid_temporal_claims() {
        let issuer = test_issuer();
        let original = claims(&issuer);
        let now = unix_timestamp().expect("timestamp");
        // How far past the accepted skew the future-dated cases sit.
        //
        // The verifier reads the clock again after this test does, and
        // `unix_timestamp` truncates to whole seconds, so a one-second offset
        // lands exactly on the tolerance boundary — which `iat > now + skew`
        // accepts — whenever the second boundary falls between the two reads.
        // That raced at roughly 2% of runs. Pinning the boundary itself would
        // need an injectable clock on the verifier; short of that the offset
        // only has to dwarf the time these cases spend signing and validating.
        let past_skew = CLOCK_SKEW.as_secs() + 30;
        let time_invalid = [
            changed(original.clone(), |c| {
                c.iat = now + past_skew;
                c.exp = c.iat + issuer.access_token_ttl.as_secs();
            }),
            changed(original.clone(), |c| c.nbf = now + past_skew),
            changed(original.clone(), |c| c.exp += 1),
            changed(original.clone(), |c| {
                c.iat = now + CLOCK_SKEW.as_secs();
                c.exp = c.iat + issuer.access_token_ttl.as_secs() + 2;
            }),
            changed(original.clone(), |c| c.exp = c.iat - 1),
            changed(original, |c| {
                c.iat = now - issuer.access_token_ttl.as_secs() - CLOCK_SKEW.as_secs() - 1;
                c.nbf = c.iat;
                c.exp = now - CLOCK_SKEW.as_secs() - 1;
            }),
        ];
        for claims in time_invalid {
            assert_invalid(&issuer, &claims);
        }
    }

    #[test]
    fn audience_policies_are_caller_owned_and_fail_closed() {
        let issuer = test_issuer();
        let verifier = issuer.verifier();
        let mcp = issuer
            .issue_access_token("user-123", "mcp-client", MCP_AUDIENCE)
            .unwrap();
        let bff = issuer
            .issue_access_token("user-123", "bff-client", BFF_AUDIENCE)
            .unwrap();

        verifier
            .validate_access_token(&mcp.access_token, &[MCP_AUDIENCE])
            .expect("MCP token should validate for the MCP audience");
        verifier
            .validate_access_token(&bff.access_token, &[BFF_AUDIENCE])
            .expect("BFF token should validate for the BFF audience");
        verifier
            .validate_access_token(&mcp.access_token, &[BFF_AUDIENCE])
            .expect_err("MCP token should not validate for the BFF audience");
        for token in [&mcp.access_token, &bff.access_token] {
            verifier
                .validate_access_token(token, &[MCP_AUDIENCE, BFF_AUDIENCE])
                .expect("token should validate for the combined audience policy");
            verifier
                .validate_access_token(token, &[])
                .expect_err("an empty audience policy must fail closed");
            verifier
                .validate_access_token(token, &[" "])
                .expect_err("an invalid audience policy must fail closed");
        }

        for (client_id, audience) in [
            ("", MCP_AUDIENCE),
            (" client", MCP_AUDIENCE),
            (CLIENT_ID, ""),
            (CLIENT_ID, "audience "),
        ] {
            let Err(_error) = issuer.issue_access_token("user-123", client_id, audience) else {
                panic!(
                    "token issuance should reject client_id={client_id:?}, audience={audience:?}"
                );
            };
        }
    }

    #[test]
    fn public_jwks_support_detached_validation() {
        let issuer = test_issuer();
        let token = issuer
            .issue_access_token("user-123", CLIENT_ID, MCP_AUDIENCE)
            .unwrap();
        let expected = issuer
            .verifier()
            .validate_access_token(&token.access_token, &[MCP_AUDIENCE])
            .expect("issuer verifier should accept the token");
        assert_eq!(issuer.verification_jwks().keys.len(), 1);
        let detached = SessionTokenVerifier::new(
            Some(ISSUER),
            issuer.verification_jwks().clone(),
            Duration::from_hours(1),
        )
        .unwrap();
        assert_eq!(
            detached
                .validate_access_token(&token.access_token, &[MCP_AUDIENCE])
                .unwrap(),
            expected
        );
        assert_eq!(detached.verification_jwks().keys.len(), 1);

        let public_jwk = only_jwk(issuer.verification_jwks());
        assert_eq!(
            public_jwk.common.key_id.as_deref(),
            Some(issuer.signing_key_id.as_str())
        );
        assert_eq!(
            public_jwk.thumbprint(ThumbprintHash::SHA256),
            issuer.signing_key_id
        );
        let serialized = serde_json::to_value(public_jwk).unwrap();
        assert_eq!(
            serialized.get("alg").and_then(serde_json::Value::as_str),
            Some("ES256")
        );
        assert_eq!(
            serialized.get("use").and_then(serde_json::Value::as_str),
            Some("sig")
        );
        assert_eq!(
            serialized.get("kty").and_then(serde_json::Value::as_str),
            Some("EC")
        );
        assert_eq!(
            serialized.get("crv").and_then(serde_json::Value::as_str),
            Some("P-256")
        );
        assert!(serialized.get("x").is_some());
        assert!(serialized.get("y").is_some());
        assert!(serialized.get("d").is_none());

        SessionTokenVerifier::new(
            Some(ISSUER),
            JwkSet {
                keys: vec![public_jwk.clone(), public_jwk.clone()],
            },
            Duration::from_hours(1),
        )
        .expect_err("duplicate key id");
    }

    #[test]
    fn verifier_rejects_unsafe_jwks_metadata() {
        let issuer = test_issuer();
        let original = issuer.verification_jwks().clone();

        let mut missing_key_id = original.clone();
        only_jwk_mut(&mut missing_key_id).common.key_id = None;
        SessionTokenVerifier::new(Some(ISSUER), missing_key_id, Duration::from_hours(1))
            .expect_err("missing key id");

        let mut wrong_algorithm = original.clone();
        only_jwk_mut(&mut wrong_algorithm).common.key_algorithm = Some(KeyAlgorithm::HS256);
        SessionTokenVerifier::new(Some(ISSUER), wrong_algorithm, Duration::from_hours(1))
            .expect_err("wrong algorithm");

        let mut wrong_use = original.clone();
        only_jwk_mut(&mut wrong_use).common.public_key_use = Some(PublicKeyUse::Encryption);
        SessionTokenVerifier::new(Some(ISSUER), wrong_use, Duration::from_hours(1))
            .expect_err("wrong key use");

        let mut conflicting_key_operations = original.clone();
        only_jwk_mut(&mut conflicting_key_operations)
            .common
            .key_operations = Some(Vec::new());
        SessionTokenVerifier::new(
            Some(ISSUER),
            conflicting_key_operations,
            Duration::from_hours(1),
        )
        .expect_err("conflicting key operations");

        let mut wrong_curve = original;
        let AlgorithmParameters::EllipticCurve(parameters) =
            &mut only_jwk_mut(&mut wrong_curve).algorithm
        else {
            panic!("expected EC key");
        };
        parameters.curve = EllipticCurve::P384;
        SessionTokenVerifier::new(Some(ISSUER), wrong_curve, Duration::from_hours(1))
            .expect_err("wrong curve");
    }
}
