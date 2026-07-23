use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, PublicKeyUse, ThumbprintHash,
};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::state::AppStateLayout;

const DEFAULT_ISSUER: &str = "coral";
const DEFAULT_AUDIENCE: &str = "coral";
const DEFAULT_SIGNING_KEY_ENV: &str = "CORAL_SESSION_SIGNING_KEY";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_hours(720);
const CLOCK_SKEW: Duration = Duration::from_mins(1);
const SESSION_TOKEN_ALGORITHM: Algorithm = Algorithm::ES256;

pub(crate) type SessionTokenError = String;

#[derive(Clone)]
pub(crate) struct SessionTokenIssuer {
    pub(super) issuer: String,
    pub(super) audience: String,
    signing_key: Arc<EncodingKey>,
    signing_key_id: String,
    verifier: SessionTokenVerifier,
    pub(super) access_token_ttl: Duration,
}

impl fmt::Debug for SessionTokenIssuer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTokenIssuer")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
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
    audience: String,
    verification_keys: Arc<HashMap<String, DecodingKey>>,
    verification_jwks: Arc<JwkSet>,
    access_token_ttl: Duration,
}

impl fmt::Debug for SessionTokenVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTokenVerifier")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("verification_key_count", &self.verification_keys.len())
            .field("access_token_ttl", &self.access_token_ttl)
            .finish_non_exhaustive()
    }
}

impl SessionTokenIssuer {
    pub(crate) fn new(
        issuer: Option<&str>,
        audience: Option<&str>,
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
            audience,
            JwkSet {
                keys: vec![signing_jwk],
            },
            access_token_ttl,
        )?;

        Ok(Self {
            issuer: verifier.issuer.clone(),
            audience: verifier.audience.clone(),
            signing_key: Arc::new(signing_key),
            signing_key_id,
            verifier,
            access_token_ttl,
        })
    }

    pub(crate) fn load(layout: &AppStateLayout) -> Result<Option<Self>, SessionTokenError> {
        Self::load_with(layout, &|name| {
            crate::bootstrap::env_var(name).map_err(|error| signing_key_env_error(&error))
        })
    }

    fn load_with(
        layout: &AppStateLayout,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<Option<Self>, SessionTokenError> {
        let config_path = layout.config_file();
        match std::fs::symlink_metadata(config_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(file_error("inspect config file", config_path, &error)),
        }
        let raw = std::fs::read_to_string(config_path)
            .map_err(|error| file_error("read config file", config_path, &error))?;
        let file: ConfigFile =
            toml::from_str(&raw).map_err(|error| config_error(error.message()))?;
        let Some(session) = file.auth.and_then(|auth| auth.session) else {
            return Ok(None);
        };
        let key = session.load_signing_key(config_path, get_var)?;
        let ttl = session
            .access_token_ttl_seconds
            .unwrap_or(DEFAULT_TOKEN_TTL.as_secs());
        Self::new(
            session.issuer.as_deref(),
            session.audience.as_deref(),
            key.as_slice(),
            Duration::from_secs(ttl),
        )
        .map(Some)
    }

    pub(crate) fn issue_access_token(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<IssuedAccessToken, SessionTokenError> {
        if provider.trim().is_empty() || subject.trim().is_empty() {
            return Err(config_error("provider and subject must not be empty"));
        }
        let issued_at = unix_timestamp()?;
        let expires_at = issued_at
            .checked_add(self.access_token_ttl.as_secs())
            .ok_or_else(|| config_error("access token expiry overflowed"))?;
        let claims = SessionTokenClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: subject.to_string(),
            exp: expires_at,
            iat: issued_at,
            nbf: issued_at,
            provider: provider.to_string(),
        };
        let mut header = Header::new(SESSION_TOKEN_ALGORITHM);
        header.kid = Some(self.signing_key_id.clone());
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
        audience: Option<&str>,
        verification_jwks: JwkSet,
        access_token_ttl: Duration,
    ) -> Result<Self, SessionTokenError> {
        let issuer = normalized_or_default(issuer, DEFAULT_ISSUER)
            .trim_end_matches('/')
            .to_string();
        let audience = normalized_or_default(audience, DEFAULT_AUDIENCE).to_string();
        if issuer.is_empty() || audience.is_empty() {
            return Err(config_error("issuer and audience must not be empty"));
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
            audience,
            verification_keys: Arc::new(verification_keys),
            verification_jwks: Arc::new(verification_jwks),
            access_token_ttl,
        })
    }

    pub(crate) fn verification_jwks(&self) -> &JwkSet {
        &self.verification_jwks
    }

    pub(crate) fn validate_access_token(
        &self,
        token: &str,
    ) -> Result<ValidatedSession, SessionTokenError> {
        let header =
            decode_header(token).map_err(|error| format!("invalid Coral access token: {error}"))?;
        if header.alg != SESSION_TOKEN_ALGORITHM {
            return Err(invalid_token("unsupported signing algorithm"));
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
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation
            .set_required_spec_claims(&["aud", "exp", "iat", "iss", "nbf", "provider", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = CLOCK_SKEW.as_secs();
        let claims = decode::<SessionTokenClaims>(token, verification_key, &validation)
            .map_err(|error| format!("invalid Coral access token: {error}"))?
            .claims;
        self.validate_claims(&claims)?;
        Ok(ValidatedSession {
            provider: claims.provider,
            subject: claims.sub,
        })
    }

    fn validate_claims(&self, claims: &SessionTokenClaims) -> Result<(), SessionTokenError> {
        let invalid = |message: &str| format!("invalid Coral access token: {message}");
        if claims.provider.trim().is_empty() || claims.sub.trim().is_empty() {
            return Err(invalid("provider and subject must not be empty"));
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
    pub(crate) provider: String,
    pub(crate) subject: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SessionTokenClaims {
    iss: String,
    aud: String,
    sub: String,
    exp: u64,
    iat: u64,
    nbf: u64,
    provider: String,
}

#[derive(Deserialize)]
struct ConfigFile {
    auth: Option<AuthConfigFile>,
}

#[derive(Deserialize)]
struct AuthConfigFile {
    session: Option<SessionConfigFile>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SessionConfigFile {
    issuer: Option<String>,
    audience: Option<String>,
    signing_key_env: Option<String>,
    signing_key_file: Option<PathBuf>,
    access_token_ttl_seconds: Option<u64>,
}

impl SessionConfigFile {
    fn load_signing_key(
        &self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<Zeroizing<Vec<u8>>, SessionTokenError> {
        match (&self.signing_key_env, &self.signing_key_file) {
            (Some(_), Some(_)) => Err(config_error(
                "configure only one of signing_key_env or signing_key_file",
            )),
            (None, Some(path)) => {
                let path = config_path.parent().unwrap_or(Path::new(".")).join(path);
                std::fs::read(&path)
                    .map(Zeroizing::new)
                    .map_err(|error| file_error("read signing_key_file", &path, &error))
            }
            (env_name, None) => {
                let env_name = env_name
                    .as_deref()
                    .unwrap_or(DEFAULT_SIGNING_KEY_ENV)
                    .trim();
                if env_name.is_empty() {
                    return Err(config_error("signing_key_env must not be empty"));
                }
                let value = get_var(env_name)
                    .map_err(|error| config_error(format!("failed to read `{env_name}`: {error}")))?
                    .ok_or_else(|| config_error(format!("env var `{env_name}` is not set")))?;
                let value = Zeroizing::new(value);
                BASE64_STANDARD
                    .decode(value.trim())
                    .map(Zeroizing::new)
                    .map_err(|_error| {
                        config_error(format!(
                            "env var `{env_name}` must contain a base64-encoded PKCS#8 P-256 private key"
                        ))
                    })
            }
        }
    }
}

pub(crate) fn bearer_authorization_value(token: &str) -> String {
    let token = token.trim();
    match token.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => token.to_string(),
        _ => format!("Bearer {token}"),
    }
}

pub(crate) fn bearer_token_from_authorization_header(
    authorization: Option<&str>,
) -> Result<&str, SessionTokenError> {
    let value = authorization.ok_or_else(|| invalid_token("missing authorization"))?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| invalid_token("authorization must use Bearer scheme"))?;
    let token = token.trim();
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(invalid_token("authorization must contain one Bearer token"));
    }
    Ok(token)
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

fn file_error(action: &str, path: &Path, error: &std::io::Error) -> SessionTokenError {
    config_error(format!("failed to {action} {}: {error}", path.display()))
}

fn signing_key_env_error(error: &std::env::VarError) -> String {
    match error {
        std::env::VarError::NotPresent => "environment variable is not present".to_string(),
        std::env::VarError::NotUnicode(_) => "environment value is not valid UTF-8".to_string(),
    }
}

fn unix_timestamp() -> Result<u64, SessionTokenError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use tempfile::TempDir;

    use super::*;

    const ISSUER: &str = "https://coral.example.test/";
    const AUDIENCE: &str = "https://coral.example.test/mcp";

    fn signing_key() -> Vec<u8> {
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key")
            .as_ref()
            .to_vec()
    }

    fn test_issuer_with_key(key: &[u8]) -> SessionTokenIssuer {
        SessionTokenIssuer::new(Some(ISSUER), Some(AUDIENCE), key, Duration::from_hours(1)).unwrap()
    }

    fn test_issuer() -> SessionTokenIssuer {
        test_issuer_with_key(&signing_key())
    }

    fn claims(issuer: &SessionTokenIssuer) -> SessionTokenClaims {
        let now = unix_timestamp().expect("timestamp");
        SessionTokenClaims {
            iss: issuer.issuer.clone(),
            aud: issuer.audience.clone(),
            sub: "user-123".to_string(),
            exp: now + issuer.access_token_ttl.as_secs(),
            iat: now,
            nbf: now,
            provider: "oidc".to_string(),
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
        encode(&header, claims, &issuer.signing_key).expect("signed token")
    }

    fn assert_invalid(issuer: &SessionTokenIssuer, claims: &SessionTokenClaims) {
        let token = sign(issuer, claims, Some(&issuer.signing_key_id));
        issuer
            .verifier()
            .validate_access_token(&token)
            .expect_err("invalid token");
    }

    fn test_layout() -> (TempDir, AppStateLayout) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        (temp, layout)
    }

    fn write_config(layout: &AppStateLayout, contents: &str) {
        fs::write(layout.config_file(), contents).expect("config file");
    }

    fn load(layout: &AppStateLayout) -> Result<Option<SessionTokenIssuer>, SessionTokenError> {
        SessionTokenIssuer::load_with(layout, &|_| Ok(None))
    }

    fn assert_path(error: &str, path: &Path) {
        assert!(error.contains(&path.display().to_string()));
    }

    fn only_jwk(jwks: &JwkSet) -> &Jwk {
        jwks.keys.first().expect("one verification key")
    }

    fn only_jwk_mut(jwks: &mut JwkSet) -> &mut Jwk {
        jwks.keys.first_mut().expect("one verification key")
    }

    #[test]
    fn token_security_and_bearer_semantics() {
        let issuer = test_issuer();
        let verifier = issuer.verifier();
        let debug = format!("{issuer:?}");
        assert!(debug.contains("<redacted>"));
        SessionTokenIssuer::new(None, None, b"not a P-256 key", DEFAULT_TOKEN_TTL)
            .expect_err("invalid key");
        SessionTokenIssuer::new(None, None, signing_key(), Duration::ZERO).expect_err("zero TTL");
        let access = issuer
            .issue_access_token("oidc", "issuer.example|opaque:subject/123")
            .unwrap();
        let header = decode_header(&access.access_token).unwrap();
        assert_eq!(header.alg, Algorithm::ES256);
        assert_eq!(header.kid.as_deref(), Some(issuer.signing_key_id.as_str()));
        let payload =
            jsonwebtoken::dangerous::insecure_decode::<serde_json::Value>(&access.access_token)
                .unwrap();
        assert!(payload.claims.get("scope").is_none());
        let session = verifier
            .validate_access_token(&access.access_token)
            .unwrap();
        assert_eq!(session.provider, "oidc");
        assert_eq!(session.subject, "issuer.example|opaque:subject/123");
        let original = claims(&issuer);
        let invalid = [
            changed(original.clone(), |c| c.iss = "other".into()),
            changed(original.clone(), |c| c.aud = "other".into()),
            changed(original.clone(), |c| c.provider = " ".into()),
            changed(original.clone(), |c| c.sub = " ".into()),
        ];
        for claims in &invalid {
            assert_invalid(&issuer, claims);
        }
        let mut wrong_algorithm_header = Header::new(Algorithm::HS256);
        wrong_algorithm_header.kid = Some(issuer.signing_key_id.clone());
        let wrong_algorithm = encode(
            &wrong_algorithm_header,
            &original,
            &EncodingKey::from_secret(b"test-only-HMAC-key-that-is-long-enough"),
        )
        .unwrap();
        verifier
            .validate_access_token(&wrong_algorithm)
            .expect_err("wrong algorithm");
        let other = test_issuer();
        let signed_by_other = sign(&other, &original, Some(&issuer.signing_key_id));
        verifier
            .validate_access_token(&signed_by_other)
            .expect_err("wrong key");
        let missing_key_id = sign(&issuer, &original, None);
        verifier
            .validate_access_token(&missing_key_id)
            .expect_err("missing key id");
        let wrong_key_id = sign(&issuer, &original, Some("unknown"));
        verifier
            .validate_access_token(&wrong_key_id)
            .expect_err("unknown key id");
        let now = unix_timestamp().expect("timestamp");
        let time_invalid = [
            changed(original.clone(), |c| {
                c.iat = now + CLOCK_SKEW.as_secs() + 1;
                c.exp = c.iat + issuer.access_token_ttl.as_secs();
            }),
            changed(original.clone(), |c| c.nbf = now + CLOCK_SKEW.as_secs() + 1),
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
        assert_eq!(bearer_authorization_value(" token "), "Bearer token");
        assert_eq!(bearer_authorization_value("bearer token"), "bearer token");
        let parsed = bearer_token_from_authorization_header(Some("Bearer token")).unwrap();
        assert_eq!(parsed, "token");
        let invalid = [
            None,
            Some("Basic token"),
            Some("Bearer"),
            Some("Bearer two tokens"),
        ];
        for value in invalid {
            bearer_token_from_authorization_header(value).expect_err("invalid bearer");
        }
    }

    #[test]
    fn public_jwks_support_detached_validation() {
        let issuer = test_issuer();
        let token = issuer.issue_access_token("oidc", "user-123").unwrap();
        assert_eq!(issuer.verification_jwks().keys.len(), 1);
        let detached = SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
            issuer.verification_jwks().clone(),
            Duration::from_hours(1),
        )
        .unwrap();
        assert_eq!(
            detached.validate_access_token(&token.access_token).unwrap(),
            ValidatedSession {
                provider: "oidc".to_string(),
                subject: "user-123".to_string(),
            }
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
            Some(AUDIENCE),
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
        SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
            missing_key_id,
            Duration::from_hours(1),
        )
        .expect_err("missing key id");

        let mut wrong_algorithm = original.clone();
        only_jwk_mut(&mut wrong_algorithm).common.key_algorithm = Some(KeyAlgorithm::HS256);
        SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
            wrong_algorithm,
            Duration::from_hours(1),
        )
        .expect_err("wrong algorithm");

        let mut wrong_use = original.clone();
        only_jwk_mut(&mut wrong_use).common.public_key_use = Some(PublicKeyUse::Encryption);
        SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
            wrong_use,
            Duration::from_hours(1),
        )
        .expect_err("wrong key use");

        let mut conflicting_key_operations = original.clone();
        only_jwk_mut(&mut conflicting_key_operations)
            .common
            .key_operations = Some(Vec::new());
        SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
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
        SessionTokenVerifier::new(
            Some(ISSUER),
            Some(AUDIENCE),
            wrong_curve,
            Duration::from_hours(1),
        )
        .expect_err("wrong curve");
    }

    #[test]
    fn key_sources_fail_closed_without_leaking_secrets() {
        let (temp, layout) = test_layout();
        let key = signing_key();
        let encoded_key = BASE64_STANDARD.encode(&key);
        assert!(load(&layout).unwrap().is_none());
        write_config(
            &layout,
            "[credentials]\nencryption_key_env = 'IGNORED_KEK'\n[auth.session]\nissuer = 'issuer'\n",
        );
        let loaded = SessionTokenIssuer::load_with(&layout, &|name| {
            Ok((name == DEFAULT_SIGNING_KEY_ENV).then(|| encoded_key.clone()))
        });
        assert!(loaded.expect("env config").is_some());
        fs::write(temp.path().join("config/session.key"), &key).expect("key file");
        write_config(
            &layout,
            "[auth.session]\nsigning_key_file = 'session.key'\n",
        );
        assert!(load(&layout).unwrap().is_some());
        for config in [
            "[auth.session]\nsigning_key_env = 'MISSING'\nsigning_key_file = 'key'\n",
            "[auth.session]\nsigning_key_env = 'MISSING'\n",
            "[auth.session]\nsigning_key_file = 'missing-key'\n",
        ] {
            write_config(&layout, config);
            load(&layout).expect_err("invalid key source");
        }
        assert_path(
            &load(&layout).expect_err("missing key"),
            &layout.config_file().parent().unwrap().join("missing-key"),
        );
        write_config(&layout, "[auth.session]\nsigning_key = 'visible-secret'\n");
        let error = load(&layout).expect_err("inline key is rejected");
        assert!(!error.contains("visible-secret"));

        write_config(
            &layout,
            "[auth.session]\nsigning_key_env = 'INVALID_BASE64'\n",
        );
        let error =
            SessionTokenIssuer::load_with(&layout, &|_| Ok(Some("visible-secret".to_string())))
                .expect_err("invalid base64");
        assert!(!error.contains("visible-secret"));

        write_config(
            &layout,
            "[auth.session]\nsigning_key_file = 'session.key'\nverification_jwks_file = 'unsupported.jwks'\n",
        );
        load(&layout).expect_err("unsupported verification JWKS config");

        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt as _;

            let (_temp, dangling) = test_layout();
            std::os::unix::fs::symlink("missing-config-mount", dangling.config_file())
                .expect("symlink");
            let error = load(&dangling).expect_err("dangling config mount");
            assert_path(&error, dangling.config_file());
            let secret = b"visible-secret-\xff-tail".to_vec();
            let os = OsString::from_vec(secret.clone());
            let error = signing_key_env_error(&std::env::VarError::NotUnicode(os));
            let bytes = error.as_bytes();
            let leaked = bytes.windows(secret.len()).any(|bytes| bytes == secret);
            assert!(!leaked);
            assert!(!error.contains("visible-secret"));
            assert_eq!(error, "environment value is not valid UTF-8");
        }
    }
}
