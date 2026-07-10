//! Validated configuration for upstream OIDC providers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::outbound_url_policy::ConfiguredEndpointUrl;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use zeroize::Zeroizing;
const DEFAULT_SCOPES: &[&str] = &["openid", "email", "profile"];
const RESERVED_AUTH_PARAMS: &[&str] = &[
    "response_type",
    "client_id",
    "redirect_uri",
    "scope",
    "state",
    "nonce",
    "code_challenge",
    "code_challenge_method",
];

#[derive(Clone)]
pub(super) struct OidcProviderConfig {
    pub(super) issuer: String,
    pub(super) client_id: String,
    client_secret: Arc<Zeroizing<String>>,
    pub(super) redirect_uri: String,
    pub(super) scopes: Vec<String>,
    pub(super) principal_claim: String,
    pub(super) display_name_claim: String,
    pub(super) auth_params: BTreeMap<String, String>,
    pub(super) required_claims: BTreeMap<String, Value>,
}
impl OidcProviderConfig {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the OIDC federation descendant")
    )]
    pub(super) fn client_secret(&self) -> &str {
        self.client_secret.as_str()
    }
}

impl fmt::Debug for OidcProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcProviderConfig")
            .field("issuer", &self.issuer)
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("redirect_uri", &self.redirect_uri)
            .field("scopes", &self.scopes)
            .field("principal_claim", &self.principal_claim)
            .field("display_name_claim", &self.display_name_claim)
            .field("auth_params", &self.auth_params)
            .field("required_claims", &self.required_claims)
            .finish()
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ProviderConfigFile {
    #[serde(rename = "type")]
    provider_type: Option<String>,
    issuer: Option<String>,
    client_id: Option<String>,
    client_secret: Option<InlineSecret>,
    client_secret_env: Option<String>,
    redirect_uri: Option<String>,
    scopes: Vec<String>,
    principal_claim: Option<String>,
    display_name_claim: Option<String>,
    auth_params: BTreeMap<String, String>,
    required_claims: BTreeMap<String, Value>,
}

impl ProviderConfigFile {
    pub(super) fn build(
        self,
        name: &str,
        get_var: &impl Fn(&str) -> Result<Option<String>, std::env::VarError>,
    ) -> Result<OidcProviderConfig, String> {
        if self
            .provider_type
            .as_deref()
            .is_some_and(|kind| kind != "oidc")
        {
            return Err(invalid(name, "type must be `oidc`"));
        }
        let issuer = required(name, "issuer", self.issuer.as_deref())?;
        let issuer_url = endpoint(name, "issuer", &issuer)?;
        if issuer_url.as_url().query().is_some() {
            return Err(invalid(name, "issuer must not include a query"));
        }
        let client_id = required(name, "client_id", self.client_id.as_deref())?;
        let client_secret = resolve_secret(
            name,
            self.client_secret,
            self.client_secret_env.as_deref(),
            get_var,
        )?;
        let redirect_uri = required(name, "redirect_uri", self.redirect_uri.as_deref())?;
        endpoint(name, "redirect_uri", &redirect_uri)?;
        let scopes = if self.scopes.is_empty() {
            DEFAULT_SCOPES.iter().map(ToString::to_string).collect()
        } else {
            self.scopes
        };
        for scope in &scopes {
            if !valid_scope_token(scope) {
                return Err(invalid(
                    name,
                    "scopes must contain valid OAuth scope tokens",
                ));
            }
        }
        if !scopes.iter().any(|scope| scope == "openid") {
            return Err(invalid(name, "scopes must include `openid`"));
        }
        let principal_claim = claim(
            name,
            "principal_claim",
            self.principal_claim.as_deref(),
            "sub",
        )?;
        let display_name_claim = claim(
            name,
            "display_name_claim",
            self.display_name_claim.as_deref(),
            "email",
        )?;
        for key in self.required_claims.keys() {
            validate_key(name, "required_claims", key)?;
        }
        for key in self.auth_params.keys() {
            validate_key(name, "auth_params", key)?;
            if RESERVED_AUTH_PARAMS
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
            {
                return Err(invalid(
                    name,
                    format!("auth_params must not include reserved parameter `{key}`"),
                ));
            }
        }
        Ok(OidcProviderConfig {
            issuer,
            client_id,
            client_secret: Arc::new(client_secret),
            redirect_uri,
            scopes,
            principal_claim,
            display_name_claim,
            auth_params: self.auth_params,
            required_claims: self.required_claims,
        })
    }
}

struct InlineSecret(Zeroizing<String>);
impl<'de> Deserialize<'de> for InlineSecret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)
            .map(|value| Self(Zeroizing::new(value)))
            .map_err(|_error| D::Error::custom("provider secret must be a string"))
    }
}

fn resolve_secret(
    name: &str,
    inline: Option<InlineSecret>,
    env_name: Option<&str>,
    get_var: &impl Fn(&str) -> Result<Option<String>, std::env::VarError>,
) -> Result<Zeroizing<String>, String> {
    match (inline, env_name) {
        (Some(_), Some(_)) => Err(invalid(
            name,
            "configure exactly one of client_secret or client_secret_env",
        )),
        (None, None) => Err(invalid(
            name,
            "exactly one of client_secret or client_secret_env is required",
        )),
        (Some(secret), None) => trimmed_secret(name, &secret.0),
        (None, Some(env_name)) => {
            let env_name = env_name.trim();
            if env_name.is_empty() || env_name.bytes().any(|byte| matches!(byte, b'=' | b'\0')) {
                return Err(invalid(name, "client_secret_env is invalid"));
            }
            let value = get_var(env_name)
                .map_err(|_error| invalid(name, "client_secret_env could not be read"))?
                .ok_or_else(|| invalid(name, "client_secret_env is unset or empty"))?;
            trimmed_secret(name, &Zeroizing::new(value))
        }
    }
}

fn trimmed_secret(name: &str, value: &Zeroizing<String>) -> Result<Zeroizing<String>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(invalid(name, "client secret must not be empty"));
    }
    Ok(Zeroizing::new(trimmed.to_string()))
}
fn required(name: &str, field: &str, value: Option<&str>) -> Result<String, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid(name, format!("{field} is required")))
}
fn endpoint(name: &str, field: &str, value: &str) -> Result<ConfiguredEndpointUrl, String> {
    ConfiguredEndpointUrl::parse(value)
        .map_err(|error| invalid(name, format!("{field} is invalid: {error}")))
}

fn valid_scope_token(scope: &str) -> bool {
    !scope.is_empty()
        && scope
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn claim(
    provider: &str,
    field: &str,
    value: Option<&str>,
    default: &str,
) -> Result<String, String> {
    let value = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string();
    validate_key(provider, field, &value)?;
    Ok(value)
}

fn validate_key(provider: &str, field: &str, key: &str) -> Result<(), String> {
    if key.is_empty() || key.trim() != key {
        return Err(invalid(
            provider,
            format!("{field} keys must be nonempty and have no surrounding whitespace"),
        ));
    }
    Ok(())
}

fn invalid(name: &str, message: impl fmt::Display) -> String {
    format!("invalid auth configuration: auth.providers.{name}.{message}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::ProviderConfigFile;

    const BASE: &str = "issuer = ' https://id.test/tenant/ '
        client_id = ' client-id '
        client_secret = ' inline-secret '
        redirect_uri = ' http://localhost/callback '
        ";

    fn provider(extra: &str) -> ProviderConfigFile {
        toml::from_str(&format!("{BASE}{extra}")).expect("provider config")
    }
    fn reject(file: ProviderConfigFile) -> String {
        file.build("test", &|_| Ok(None)).expect_err("rejected")
    }

    #[test]
    fn retains_validated_values_and_secure_defaults() {
        let config = provider(
            "principal_claim = ' '
            display_name_claim = ' email '",
        )
        .build("test", &|_| Ok(None))
        .expect("provider");
        assert_eq!(config.issuer, "https://id.test/tenant/");
        assert_eq!(config.client_id, "client-id");
        assert_eq!(config.client_secret(), "inline-secret");
        assert_eq!(config.redirect_uri, "http://localhost/callback");
        assert_eq!(config.scopes, ["openid", "email", "profile"]);
        assert_eq!(config.principal_claim, "sub");
        assert_eq!(config.display_name_claim, "email");
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>") && !debug.contains("inline-secret"));
    }

    #[test]
    fn resolves_env_secret_and_retains_provider_options() {
        let mut file = provider(
            "scopes = ['openid', 'groups']
             principal_claim = 'uid'
             display_name_claim = 'name'
             [auth_params]
             prompt = 'select_account'
             [required_claims]
             hd = 'example.test'",
        );
        file.client_secret = None;
        file.client_secret_env = Some(" PROVIDER_SECRET ".into());
        let config = file
            .build("test", &|name| {
                assert_eq!(name, "PROVIDER_SECRET");
                Ok(Some(" env-secret ".into()))
            })
            .expect("provider");
        assert_eq!(config.client_secret(), "env-secret");
        assert_eq!(config.scopes, ["openid", "groups"]);
        assert_eq!(config.principal_claim, "uid");
        assert_eq!(config.display_name_claim, "name");
        let prompt = config.auth_params.get("prompt").map(String::as_str);
        assert_eq!(prompt, Some("select_account"));
        let claim = config.required_claims.get("hd");
        assert_eq!(claim, Some(&json!("example.test")));
    }

    #[test]
    fn rejects_ambiguous_or_unavailable_secrets_without_leaking() {
        let mut both = provider("");
        both.client_secret_env = Some("PROVIDER_SECRET".into());
        reject(both);
        let mut neither = provider("");
        neither.client_secret = None;
        reject(neither);
        let parse_error = toml::from_str::<ProviderConfigFile>("client_secret = 42")
            .err()
            .expect("non-string secret");
        assert!(!parse_error.message().contains("42"));
        for env_name in ["", "BAD=NAME", "BAD\0NAME", "MISSING"] {
            let mut missing = provider("");
            missing.client_secret = None;
            missing.client_secret_env = Some(env_name.into());
            let error = reject(missing);
            assert!(!error.contains("inline-secret"));
        }
        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt as _;
            let mut unreadable = provider("");
            unreadable.client_secret = None;
            unreadable.client_secret_env = Some("PROVIDER_SECRET".into());
            let secret = b"visible-secret-\xff-tail".to_vec();
            let error = unreadable
                .build("test", &|_| {
                    Err(std::env::VarError::NotUnicode(OsString::from_vec(
                        secret.clone(),
                    )))
                })
                .expect_err("nonunicode env secret");
            assert!(!error.contains("visible-secret"));
        }
    }

    #[test]
    fn rejects_invalid_provider_fields() {
        let invalid = [
            format!("{BASE}type = 'oauth'"),
            BASE.replace("https://id.test/tenant/", "http://remote.test"),
            BASE.replace("https://id.test/tenant/", "https://id.test?q=1"),
            BASE.replace("http://localhost", "http://remote.test"),
            format!("{BASE}scopes = ['email']"),
            format!("{BASE}scopes = ['openid', 'two scopes']"),
        ];
        for raw in invalid {
            reject(toml::from_str(&raw).expect("fixture"));
        }
        let mut bad_claim = provider("");
        bad_claim.required_claims = BTreeMap::from([(" spaced ".into(), json!(true))]);
        reject(bad_claim);
        for reserved in ["state", "CLIENT_ID", "code_challenge_method"] {
            let mut bad_param = provider("");
            bad_param.auth_params = BTreeMap::from([(reserved.into(), "value".into())]);
            let error = reject(bad_param);
            assert!(error.contains(reserved));
        }
    }
}
