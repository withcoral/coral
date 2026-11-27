// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The settings of `mode = "saml"` in `[auth.provider]`, and their checks.

use coral_sign_in::{ModeSettings, SettingError};
use serde::Deserialize;
use url::Url;

use crate::certificate::Certificate;

/// The settings as the file holds them. Every key the mode does not read is
/// refused, so a setting of another mode never passes in silence.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSettings {
    entity_id: String,
    sign_in_url: String,
    certificates: Vec<String>,
    #[serde(default = "default_email_attribute")]
    email_attribute: String,
    #[serde(default = "default_name_attribute")]
    name_attribute: String,
    #[serde(default)]
    bootstrap_owner_name_id: Option<String>,
}

fn default_email_attribute() -> String {
    "email".to_owned()
}

fn default_name_attribute() -> String {
    "name".to_owned()
}

/// The checked settings.
pub(crate) struct Settings {
    /// The Identity Provider's entity ID.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the check of a response reads it")
    )]
    pub(crate) entity_id: String,
    /// Where Coral sends the request: `https` only.
    pub(crate) sign_in_url: Url,
    /// One or two certificates that sign the Identity Provider's responses.
    #[cfg_attr(not(test), expect(dead_code, reason = "the startup warnings read it"))]
    pub(crate) certificates: Vec<Certificate>,
    /// The attribute that holds the person's email.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the check of a response reads it")
    )]
    pub(crate) email_attribute: String,
    /// The attribute that holds the person's name.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the check of a response reads it")
    )]
    pub(crate) name_attribute: String,
    /// The `NameID` of the person who signs in as the first Owner.
    pub(crate) bootstrap_owner_name_id: Option<String>,
}

impl Settings {
    /// Checks `settings`.
    ///
    /// # Errors
    /// Returns the setting that is unusable and why, never its value.
    pub(crate) fn check(settings: &ModeSettings) -> Result<Self, SettingError> {
        let raw: RawSettings = settings.decode()?;
        let entity_id = filled("entity_id", &raw.entity_id)?;
        let sign_in_url = filled("sign_in_url", &raw.sign_in_url)?;
        let email_attribute = filled("email_attribute", &raw.email_attribute)?;
        let name_attribute = filled("name_attribute", &raw.name_attribute)?;
        let bootstrap_owner_name_id = raw
            .bootstrap_owner_name_id
            .as_deref()
            .map(|value| filled("bootstrap_owner_name_id", value))
            .transpose()?;
        let sign_in_url = Url::parse(&sign_in_url)
            .map_err(|_error| SettingError::new("sign_in_url", "must be a URL"))?;
        if sign_in_url.scheme() != "https" {
            return Err(SettingError::new("sign_in_url", "SAML sign-in needs https"));
        }
        // The Identity Provider compares the request's `Destination` with
        // its own URL, and a browser never sends a fragment.
        if sign_in_url.fragment().is_some() {
            return Err(SettingError::new("sign_in_url", "must have no fragment"));
        }
        if !sign_in_url.username().is_empty() || sign_in_url.password().is_some() {
            return Err(SettingError::new(
                "sign_in_url",
                "must hold no user name or password",
            ));
        }
        if !(1..=2).contains(&raw.certificates.len()) {
            return Err(SettingError::new(
                "certificates",
                "needs one or two certificates",
            ));
        }
        let certificates = raw
            .certificates
            .iter()
            .enumerate()
            .map(|(index, pem)| {
                Certificate::parse(pem).ok_or_else(|| {
                    SettingError::new(
                        format!("certificates[{index}]"),
                        "must be one PEM certificate",
                    )
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            entity_id,
            sign_in_url,
            certificates,
            email_attribute,
            name_attribute,
            bootstrap_owner_name_id,
        })
    }
}

/// `value` without the whitespace around it, refused under `field` when
/// nothing is left.
fn filled(field: &'static str, value: &str) -> Result<String, SettingError> {
    let value = value.trim();
    if value.is_empty() {
        Err(SettingError::new(field, "must not be empty"))
    } else {
        Ok(value.to_owned())
    }
}
