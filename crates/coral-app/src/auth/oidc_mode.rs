//! The OIDC sign-in mode's settings: what `[auth.provider]` holds when its
//! `mode` is `oidc` or absent, their checks, and the provider they resolve to.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use url::Url;
use zeroize::Zeroizing;

use super::OIDC_CALLBACK_PATH;
use super::config::{config_error, invalid_provider};
use super::error::AuthServerError;
use coral_outbound::{Configured, EndpointUrl};

mod sign_in;

pub(super) use sign_in::OidcMode;

const DEFAULT_PROVIDER_SCOPES: &[&str] = &["openid", "email", "profile"];

/// Query parameters reserved to the authorization request.
///
/// No outside input may pre-set one. Operator-supplied `auth_params` are
/// checked against this list here, and the provider client checks the
/// `authorization_endpoint` it reads out of the discovery document against the
/// same list, so the two paths cannot drift apart and leave the less trusted
/// source — the discovery document — the more permissive one.
pub(super) const RESERVED_PROVIDER_AUTH_PARAMS: &[&str] = &[
    "response_type",
    // `fragment` and `form_post` both stop the authorization code from ever
    // reaching the GET callback route, so a login started with either simply
    // never completes and leaves nothing in this server's logs.
    "response_mode",
    "client_id",
    "redirect_uri",
    "scope",
    "state",
    "nonce",
    "code_challenge",
    "code_challenge_method",
];

/// Deserialize-and-validate target for the `[auth.provider]` section.
///
/// This shape is the one place a client secret and a name pointing at one
/// coexist. Validated settings keep it until startup resolves them;
/// [`resolve`](Self::resolve) settles which of the two the provider uses, and
/// the resolved provider carries neither name.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct OidcProviderSettings {
    issuer: String,
    client_id: String,
    client_secret: Option<ProviderSecret>,
    client_secret_env: Option<String>,
    scopes: Vec<String>,
    display_name_claim: String,
    name_claim: String,
    email_claim: String,
    email_verified_claim: String,
    bootstrap_owner_subject: Option<String>,
    auth_params: BTreeMap<String, String>,
    required_claims: BTreeMap<String, Value>,
    access_token: Option<AccessTokenAdmission>,
}

/// Optional login admission evidence from an upstream RS256 JWT access token.
/// It supplements, never replaces, the verified OIDC identity. The audience is
/// explicit because a provider's access-token audience need not be its OIDC
/// client ID. Keys come only from that provider's validated discovery JWKS.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AccessTokenAdmission {
    pub(super) audience: String,
    #[serde(default)]
    pub(super) required_claims: BTreeMap<String, Value>,
}

impl OidcProviderSettings {
    pub(super) fn validate(&mut self) -> Result<(), AuthServerError> {
        if self.client_secret.is_some() == self.client_secret_env.is_some() {
            return Err(config_error(
                "auth.provider must configure exactly one of client_secret or client_secret_env",
            ));
        }
        self.issuer = provider_required("issuer", &self.issuer)?;
        let issuer = provider_literal_endpoint("issuer", &self.issuer)?;
        if issuer.as_url().query().is_some() {
            return Err(invalid_provider("issuer must not include a query"));
        }
        validate_canonical_issuer(&self.issuer, &issuer)?;
        self.client_id = provider_required("client_id", &self.client_id)?;

        if let Some(secret) = &mut self.client_secret {
            *secret = ProviderSecret::from_trimmed("client_secret", secret.as_str())?;
        }
        if let Some(env_name) = &mut self.client_secret_env {
            *env_name = env_name.trim().to_string();
            if env_name.is_empty() {
                return Err(invalid_provider(
                    "client_secret_env must be a nonempty string",
                ));
            }
            if env_name.bytes().any(|byte| matches!(byte, b'=' | b'\0')) {
                return Err(invalid_provider("client_secret_env is invalid"));
            }
        }

        // An omitted `scopes` and an explicit `scopes = []` both land here, and
        // both take the defaults. Separating them would only let an empty list
        // report `scopes must include openid` instead, which is the same
        // startup failure for a request that could never authenticate.
        if self.scopes.is_empty() {
            self.scopes = DEFAULT_PROVIDER_SCOPES
                .iter()
                .map(ToString::to_string)
                .collect();
        }
        if self.scopes.iter().any(|scope| !valid_scope_token(scope)) {
            return Err(invalid_provider(
                "scopes must contain valid OAuth scope tokens",
            ));
        }
        if !self.scopes.iter().any(|scope| scope == "openid") {
            return Err(invalid_provider("scopes must include `openid`"));
        }

        self.validate_identity_policy()?;
        for key in self.auth_params.keys() {
            validate_provider_key("auth_params", key)?;
            if RESERVED_PROVIDER_AUTH_PARAMS
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
            {
                return Err(invalid_provider(format!(
                    "auth_params must not include reserved parameter `{key}`"
                )));
            }
        }
        Ok(())
    }

    fn validate_identity_policy(&mut self) -> Result<(), AuthServerError> {
        self.display_name_claim =
            provider_claim("display_name_claim", &self.display_name_claim, "email")?;
        self.name_claim = provider_claim("name_claim", &self.name_claim, "name")?;
        self.email_claim = provider_claim("email_claim", &self.email_claim, "email")?;
        self.email_verified_claim = provider_claim(
            "email_verified_claim",
            &self.email_verified_claim,
            "email_verified",
        )?;
        if self
            .bootstrap_owner_subject
            .as_deref()
            .is_some_and(|subject| {
                subject.len() > 255
                    || !subject.is_ascii()
                    || coral_workspace::UserId::parse(subject).is_err()
                    || subject
                        == coral_workspace::local_organization_owner()
                            .user_id()
                            .as_str()
            })
        {
            return Err(invalid_provider(
                "bootstrap_owner_subject must be a valid non-local OIDC subject",
            ));
        }
        for (key, value) in &self.required_claims {
            validate_provider_key("required_claims", key)?;
            validate_required_claim_value(key, value)?;
        }
        if let Some(admission) = &mut self.access_token {
            admission.audience = provider_required("access_token.audience", &admission.audience)?;
            for (key, value) in &admission.required_claims {
                validate_provider_key("access_token.required_claims", key)?;
                validate_required_claim_value(key, value)?;
            }
        }
        Ok(())
    }

    /// Reads the client secret from whichever source the config names and
    /// hands the validated fields to the provider that will serve logins.
    ///
    /// The fields move rather than copy, so the two ways of naming a secret do
    /// not survive into the resolved value alongside the secret itself.
    pub(super) fn resolve(
        self,
        authorization_server_issuer: &str,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<ResolvedOidcProvider, AuthServerError> {
        let client_secret = match (self.client_secret, self.client_secret_env) {
            (Some(secret), _) => secret,
            (None, Some(env_name)) => {
                let value = Zeroizing::new(
                    get_var(&env_name)
                        .map_err(|_error| invalid_provider("client_secret_env could not be read"))?
                        .ok_or_else(|| {
                            invalid_provider(format!(
                                "client_secret_env names `{env_name}`, which is not set"
                            ))
                        })?,
                );
                ProviderSecret::from_trimmed("client_secret_env", &value)?
            }
            // Defense in depth: `validate` requires exactly one source before
            // any call reaches here, so this arm only guards a future caller
            // that skips it.
            (None, None) => {
                return Err(invalid_provider(
                    "client_secret or client_secret_env is required",
                ));
            }
        };
        Ok(ResolvedOidcProvider {
            issuer: self.issuer,
            client_id: self.client_id,
            // The validated issuer is a canonical origin with no trailing slash.
            redirect_uri: format!("{authorization_server_issuer}{OIDC_CALLBACK_PATH}"),
            scopes: self.scopes,
            bootstrap_owner_subject: self.bootstrap_owner_subject,
            display_name_claim: self.display_name_claim,
            name_claim: self.name_claim,
            email_claim: self.email_claim,
            email_verified_claim: self.email_verified_claim,
            auth_params: self.auth_params,
            required_claims: self.required_claims,
            access_token: self.access_token,
            client_secret,
        })
    }
}

/// One upstream OIDC provider, validated and ready to serve logins.
///
/// [`OidcProviderSettings::resolve`] is the only way to build one, and it moves
/// the validated fields out of the configured shape, so this type carries no
/// `client_secret_env` still pointing at a secret and no empty `client_secret`
/// to disagree with the one it holds. Holding one is therefore proof the secret
/// was fetched, which is why reading it cannot fail.
///
/// The derived `Debug` is safe to print: the only secret-bearing field is a
/// [`ProviderSecret`], which redacts itself.
#[derive(Clone, Debug)]
pub(super) struct ResolvedOidcProvider {
    pub(super) issuer: String,
    pub(super) client_id: String,
    pub(super) redirect_uri: String,
    pub(super) scopes: Vec<String>,
    pub(super) bootstrap_owner_subject: Option<String>,
    pub(super) display_name_claim: String,
    pub(super) name_claim: String,
    pub(super) email_claim: String,
    pub(super) email_verified_claim: String,
    pub(super) auth_params: BTreeMap<String, String>,
    pub(super) required_claims: BTreeMap<String, Value>,
    pub(super) access_token: Option<AccessTokenAdmission>,
    client_secret: ProviderSecret,
}

impl ResolvedOidcProvider {
    pub(super) fn client_secret(&self) -> &str {
        self.client_secret.as_str()
    }
}

#[derive(Clone)]
struct ProviderSecret(Arc<Zeroizing<String>>);

impl ProviderSecret {
    /// Trims a secret read from `field`, which names it in any error.
    fn from_trimmed(field: &str, value: &str) -> Result<Self, AuthServerError> {
        let value = value.trim();
        if value.is_empty() {
            Err(invalid_provider(format!("{field} must not be empty")))
        } else {
            Ok(Self(Arc::new(Zeroizing::new(value.to_string()))))
        }
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for ProviderSecret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || D::Error::custom("provider secret must be a nonempty string");
        let value = String::deserialize(deserializer)
            .map(Zeroizing::new)
            .map_err(|_error| invalid())?;
        if value.trim().is_empty() {
            return Err(invalid());
        }
        Ok(Self(Arc::new(value)))
    }
}

impl fmt::Debug for ProviderSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn provider_required(field: &str, value: &str) -> Result<String, AuthServerError> {
    let value = value.trim();
    if value.is_empty() {
        Err(invalid_provider(format!("{field} is required")))
    } else {
        Ok(value.to_string())
    }
}

/// Parses the provider issuer without silently repairing its configured spelling.
/// Discovery compares it byte for byte; syntax violations must fail at startup.
fn provider_literal_endpoint(
    field: &str,
    value: &str,
) -> Result<EndpointUrl<Configured>, AuthServerError> {
    let syntax_violation = Cell::new(None);
    let record_violation = |violation| syntax_violation.set(Some(violation));
    let url = Url::options()
        .syntax_violation_callback(Some(&record_violation))
        .parse(value)
        .map_err(|error| invalid_provider(format!("{field} is invalid: {error}")))?;
    if let Some(violation) = syntax_violation.get() {
        return Err(invalid_provider(format!("{field} is invalid: {violation}")));
    }
    EndpointUrl::<Configured>::from_parsed(url)
        .map_err(|error| invalid_provider(format!("{field} is invalid: {error}")))
}

/// Rejects a provider issuer the URL parser would rewrite.
///
/// The issuer is retained exactly as configured, because an `OpenID` Connect
/// discovery document's `issuer` must equal the configured one byte for byte.
/// `OpenID` Connect Discovery 1.0 §3 and RFC 8414 §2 both require a published
/// issuer to be a normalized URL, so a spelling the parser rewrites is one no
/// compliant provider can publish. Accepting it would not make it work: it
/// would trade a startup error for a discovery mismatch on every login.
///
/// Providers publish issuers both with a trailing slash and without one, so
/// both are accepted — the parser appends one only to a root-path URL.
fn validate_canonical_issuer(
    configured: &str,
    parsed: &EndpointUrl<Configured>,
) -> Result<(), AuthServerError> {
    let canonical = parsed.as_url().as_str();
    if configured == canonical || format!("{configured}/") == canonical {
        return Ok(());
    }
    Err(invalid_provider(format!(
        "issuer must be copied exactly as the provider publishes it; `{configured}` is not a canonical URL (its canonical form is `{canonical}`)"
    )))
}

fn valid_scope_token(scope: &str) -> bool {
    !scope.is_empty()
        && scope
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn provider_claim(field: &str, value: &str, default: &str) -> Result<String, AuthServerError> {
    let value = if value.trim().is_empty() {
        default
    } else {
        value.trim()
    };
    validate_provider_key(field, value)?;
    Ok(value.to_string())
}

/// Rejects a `required_claims` value no ID token could carry.
///
/// These values are held as JSON so the check against a decoded ID token is a
/// plain equality, but they are written in TOML, and the two languages do not
/// describe the same values. TOML has a datetime that JSON does not, and `toml`
/// hands one over as an object carrying a private marker key — accepted in
/// silence today, then unmatchable against every ID token a provider can send.
/// A date belongs in quotes, where it compares as the string the provider
/// actually sends.
///
/// Numbers are accepted as written and compared as written: `serde_json` holds
/// integers and floats apart, so `3` does not equal `3.0`. That distinction is
/// the provider's to make, not something this can settle at startup — the value
/// has to be spelled the way the ID token spells it.
fn validate_required_claim_value(key: &str, value: &Value) -> Result<(), AuthServerError> {
    let is_scalar =
        |value: &Value| matches!(value, Value::String(_) | Value::Bool(_) | Value::Number(_));
    let supported = match value {
        Value::Array(values) => values.iter().all(is_scalar),
        value => is_scalar(value),
    };
    if supported {
        Ok(())
    } else {
        Err(invalid_provider(format!(
            "required_claims `{key}` must be a string, boolean, number, or an array of those; quote a date or time so it is compared as the string the provider sends"
        )))
    }
}

/// Rejects a key that cannot name a claim or an authorization query parameter.
///
/// Interior whitespace matters as much as surrounding whitespace: a claim name
/// carrying it can never be found in an ID token, and validating it here is
/// what makes `provider_claim`'s startup guarantee real rather than reporting
/// it as a missing claim on every login.
fn validate_provider_key(field: &str, key: &str) -> Result<(), AuthServerError> {
    if key.is_empty() || key.chars().any(char::is_whitespace) {
        return Err(invalid_provider(format!(
            "{field} keys must be nonempty and contain no whitespace"
        )));
    }
    Ok(())
}
