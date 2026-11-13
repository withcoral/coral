//! One store's settings, as a store kind receives them from deployment config.
//!
//! Operators can put a secret under the wrong key, so nothing here repeats a
//! rejected value: errors name the field and the reason only.

use std::borrow::Cow;
use std::env::VarError;
use std::fmt;

use serde::de::DeserializeOwned;

use crate::Secret;

/// Serde messages that quote the value they reject.
const VALUE_BEARING_MESSAGES: [&str; 3] = ["invalid type: ", "invalid value: ", "unknown variant "];

/// Why one store setting is unusable: the field below the store's entry, and a
/// reason that never holds the value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingError {
    field: String,
    reason: Cow<'static, str>,
}

impl SettingError {
    /// The setting at `field` (empty for the entry itself) is unusable.
    #[must_use]
    pub fn new(field: impl Into<String>, reason: impl Into<Cow<'static, str>>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// The field below the store's entry, such as `auth.mount`. Empty for the
    /// entry itself.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Why the setting is unusable.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for SettingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.field.is_empty() {
            f.write_str(&self.reason)
        } else {
            write!(f, "{}: {}", self.field, self.reason)
        }
    }
}

impl std::error::Error for SettingError {}

/// One store's entry in `[[secret_stores]]`, without the `name` and `kind`
/// that core reads.
#[derive(Clone, Debug, Default)]
pub struct StoreSettings(toml::Table);

impl StoreSettings {
    /// The settings in `table`.
    #[must_use]
    pub const fn new(table: toml::Table) -> Self {
        Self(table)
    }

    /// Deserializes the settings.
    ///
    /// # Errors
    /// Returns the field and reason a value is rejected, never the value.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, SettingError> {
        serde_path_to_error::deserialize(toml::Value::Table(self.0.clone())).map_err(
            |error: serde_path_to_error::Error<toml::de::Error>| {
                let message = error.inner().message();
                let reason = if VALUE_BEARING_MESSAGES
                    .iter()
                    .any(|prefix| message.starts_with(prefix))
                {
                    "unsupported value".to_owned()
                } else {
                    message.to_owned()
                };
                let field = if error.path().iter().next().is_none() {
                    String::new()
                } else {
                    error.path().to_string()
                };
                SettingError::new(field, reason)
            },
        )
    }
}

/// Checks a setting that names an environment variable, and returns the name
/// trimmed.
///
/// # Errors
/// Returns why the name is unusable, without repeating it.
pub fn env_var_name(field: &str, configured: &str) -> Result<String, SettingError> {
    let name = configured.trim();
    if name.is_empty() {
        return Err(SettingError::new(field, "must not be empty"));
    }
    if name.contains(['=', '\0']) {
        return Err(SettingError::new(field, "must not contain '=' or NUL"));
    }
    Ok(name.to_owned())
}

/// The environment variables the rest of deployment config names, each with
/// the setting that names it, such as `credentials.key_env`.
#[derive(Clone, Debug, Default)]
pub struct NamedVariables(Vec<(String, String)>);

impl NamedVariables {
    /// The `(setting, variable)` pairs config holds.
    #[must_use]
    pub fn new(named: impl IntoIterator<Item = (String, String)>) -> Self {
        Self(named.into_iter().collect())
    }

    /// Each setting and the variable it names.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(setting, variable)| (setting.as_str(), variable.as_str()))
    }
}

/// What bootstrap gives a store kind to build a store.
pub struct BuildEnvironment<'a> {
    /// The destination policy every request to the store goes through.
    pub policy: coral_outbound::DestinationPolicy,
    /// Reads one process environment variable. Bootstrap owns the environment,
    /// so a store kind reads it only through this.
    pub env: &'a dyn Fn(&str) -> Result<Option<String>, VarError>,
}

impl BuildEnvironment<'_> {
    /// Reads the secret held in the variable `name`, which the setting at
    /// `field` names.
    ///
    /// # Errors
    /// Returns why no secret was read. Never names the variable: an operator
    /// may have pasted the secret itself into the setting.
    pub fn secret(&self, field: &str, name: &str) -> Result<Secret, SettingError> {
        let value = (self.env)(name)
            .map_err(|_not_unicode| {
                SettingError::new(field, "environment variable must contain valid UTF-8")
            })?
            .ok_or_else(|| SettingError::new(field, "environment variable is not set"))?;
        Secret::new(value).map_err(|_error| {
            SettingError::new(field, "environment variable is not a usable secret")
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    const SECRET: &str = "sk-live-0123456789abcdef";

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[expect(dead_code, reason = "only decoding is under test")]
    struct Settings {
        mount: String,
        auth: Auth,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[expect(dead_code, reason = "only decoding is under test")]
    struct Auth {
        method: Method,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Method {
        Approle,
    }

    fn settings(raw: &str) -> StoreSettings {
        StoreSettings::new(toml::from_str(raw).unwrap())
    }

    #[test]
    fn a_rejected_setting_names_its_field_and_never_its_value() {
        for (raw, expected) in [
            (
                format!("mount = 'secret'\nauth = {{ method = '{SECRET}' }}"),
                "auth.method: unsupported value",
            ),
            (
                "mount = 7\nauth = { method = 'approle' }".to_owned(),
                "mount: unsupported value",
            ),
            (
                format!("mount = 'a'\nauth = {{ method = 'approle' }}\n{SECRET} = 1"),
                "unknown field",
            ),
            (
                "auth = { method = 'approle' }".to_owned(),
                "missing field `mount`",
            ),
        ] {
            let error = settings(&raw).decode::<Settings>().unwrap_err().to_string();
            assert!(error.contains(expected), "{raw}: {error}");
            assert!(
                !error.contains(SECRET) || expected == "unknown field",
                "{raw}: {error}"
            );
        }
    }

    #[test]
    fn an_env_var_name_is_trimmed_and_fails_closed() {
        assert_eq!(
            env_var_name("auth.secret_id_env", " NAME \n").unwrap(),
            "NAME"
        );
        for (configured, reason) in [
            (" \t", "must not be empty"),
            ("A=B", "must not contain '=' or NUL"),
            ("A\0B", "must not contain '=' or NUL"),
        ] {
            let error = env_var_name("auth.secret_id_env", configured).unwrap_err();
            assert_eq!(error.to_string(), format!("auth.secret_id_env: {reason}"));
        }
    }

    #[test]
    fn a_secret_from_the_environment_is_never_named_in_its_error() {
        let env = |name: &str| -> Result<Option<String>, VarError> {
            match name {
                "SET" => Ok(Some("value".to_owned())),
                "BINARY" => Err(VarError::NotUnicode("x".into())),
                "EMPTY" => Ok(Some(String::new())),
                _ => Ok(None),
            }
        };
        let environment = BuildEnvironment {
            policy: coral_outbound::DestinationPolicy::default(),
            env: &env,
        };
        assert_eq!(environment.secret("f", "SET").unwrap().expose(), "value");
        for (name, reason) in [
            (SECRET, "environment variable is not set"),
            ("BINARY", "environment variable must contain valid UTF-8"),
            ("EMPTY", "environment variable is not a usable secret"),
        ] {
            let error = environment.secret("f", name).unwrap_err().to_string();
            assert_eq!(error, format!("f: {reason}"));
        }
    }
}
