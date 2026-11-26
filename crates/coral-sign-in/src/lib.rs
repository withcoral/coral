//! The sign-in mode: how a build adds a way to sign people in.
//!
//! # Interface
//!
//! A launching binary registers each mode it has as a [`SignInModeFactory`]
//! in [`SignInModes`], under a [`ModeName`]: the value of `mode` in
//! `[auth.provider]`. Bootstrap hands the rest of that table
//! ([`ModeSettings`]) and Coral's URL ([`ModeEnvironment`]) to the factory of
//! that name, which checks the settings and builds the [`SignInMode`].
//!
//! A mode does the part of a sign-in that its protocol owns: the URL that
//! begins it, which sign-in a request at the mode's URL names, and the check
//! of that request, which ends in a [`ProvenPerson`] or a [`Refusal`]. Core
//! does the rest: the sign-in cookie, the User, the code and the tokens.
//!
//! No error here repeats a configured value, since an operator can paste a
//! secret under the wrong key.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use coral_db::UnixNanos;
use coral_edition::EnterpriseFeature;
use serde::de::DeserializeOwned;
use url::Url;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;

/// The time of a refusal: RFC 3339 in UTC, to the second.
const TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

/// Serde messages that quote the value they reject.
const VALUE_BEARING_MESSAGES: [&str; 3] = ["invalid type: ", "invalid value: ", "unknown variant "];

/// Serde's message for a key that a struct does not read.
const UNKNOWN_FIELD_MESSAGE: &str = "unknown field ";

/// The name a build registers a mode under: the value of `mode`, and the
/// segment of the mode's URLs under `/auth/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModeName(&'static str);

impl ModeName {
    /// The name `name`.
    ///
    /// # Errors
    /// Returns [`InvalidName`] unless `name` has the form of a mode's name
    /// ([`Self::is_name`]).
    pub fn new(name: &'static str) -> Result<Self, InvalidName> {
        if Self::is_name(name) {
            Ok(Self(name))
        } else {
            Err(InvalidName)
        }
    }

    /// Whether `value` has the form of a mode's name: at most 32 lowercase
    /// letters, digits, `-` or `_`, starting with a letter. A message may
    /// repeat only such a value.
    #[must_use]
    pub fn is_name(value: &str) -> bool {
        is_token(value, 32, |c| c == '-' || c == '_')
    }

    /// The name as config writes it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// A mode's name that does not have the form of one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "a sign-in mode's name must be at most 32 lowercase letters, digits, `-` or `_`, starting with a letter"
)]
pub struct InvalidName;

fn is_token(value: &str, max_len: usize, also: impl Fn(char) -> bool) -> bool {
    value.len() <= max_len
        && value.starts_with(|c: char| c.is_ascii_lowercase())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || also(c))
}

/// Why one setting of a mode is unusable: the field below `[auth.provider]`,
/// and a reason that never holds the value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingError {
    field: String,
    reason: Cow<'static, str>,
    unknown: bool,
}

impl SettingError {
    /// The setting at `field` (empty for the table itself) is unusable for
    /// `reason`. The reason is static, so it cannot hold the value.
    #[must_use]
    pub fn new(field: impl Into<String>, reason: &'static str) -> Self {
        Self {
            field: field.into(),
            reason: Cow::Borrowed(reason),
            unknown: false,
        }
    }

    /// Whether [`ModeSettings::decode`] found a nonempty key that the mode's
    /// settings do not read. The reason then names no mode, so that the
    /// caller can name it.
    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        self.unknown
    }

    /// The field below `[auth.provider]`. Empty for the table itself, and for
    /// a key that is the empty string.
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

/// `[auth.provider]` without the `mode` that core reads.
#[derive(Clone, Default)]
pub struct ModeSettings(toml::Table);

/// Prints no value: an operator can paste a secret under any key.
impl fmt::Debug for ModeSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModeSettings").finish_non_exhaustive()
    }
}

impl ModeSettings {
    /// The settings in `table`.
    #[must_use]
    pub const fn new(table: toml::Table) -> Self {
        Self(table)
    }

    /// Deserializes the settings. They go back through TOML text, so each
    /// value decodes as it does in the file: deserializing from a
    /// `toml::Value` hands a datetime over as a string.
    ///
    /// A message that serde writes with the value it rejects becomes
    /// "unsupported value". When `T` has `#[serde(deny_unknown_fields)]`, a
    /// nonempty key that it does not read gives an error for which
    /// [`SettingError::is_unknown`] holds. Without that attribute, or with a
    /// `#[serde(flatten)]` field, serde accepts such a key in silence, so a
    /// mode's settings deny unknown fields. Any other message is kept, so a
    /// mode's own `Deserialize` must never quote what it rejects, as for
    /// every setting of `config.toml`, nor start a message with "unknown
    /// field".
    ///
    /// # Errors
    /// Returns the field and reason a value is rejected, never the value.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, SettingError> {
        let raw = toml::to_string(&self.0)
            .map_err(|_error| SettingError::new("", "unsupported value"))?;
        let deserializer = toml::Deserializer::parse(&raw)
            .map_err(|_error| SettingError::new("", "unsupported value"))?;
        serde_path_to_error::deserialize(deserializer).map_err(
            |error: serde_path_to_error::Error<toml::de::Error>| {
                let field = if error.path().iter().next().is_none() {
                    String::new()
                } else {
                    error.path().to_string()
                };
                let message = error.inner().message();
                // An empty key names no field, so serde's text stays.
                let unknown = !field.is_empty() && message.starts_with(UNKNOWN_FIELD_MESSAGE);
                let reason = if unknown {
                    Cow::Borrowed("not a setting of this sign-in mode")
                } else if VALUE_BEARING_MESSAGES
                    .iter()
                    .any(|prefix| message.starts_with(prefix))
                {
                    Cow::Borrowed("unsupported value")
                } else {
                    Cow::Owned(message.to_owned())
                };
                SettingError {
                    field,
                    reason,
                    unknown,
                }
            },
        )
    }
}

/// What core gives a mode beside its settings.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ModeEnvironment {
    /// Coral's URL, `listeners.auth.public_url`. The mode's own URLs are
    /// below `/auth/{name}/` of it.
    pub coral_url: Url,
}

impl ModeEnvironment {
    /// The environment of a Coral at `coral_url`.
    #[must_use]
    pub const fn new(coral_url: Url) -> Self {
        Self { coral_url }
    }
}

/// Builds the one mode of a deployment from its settings.
pub trait SignInModeFactory: Send + Sync {
    /// The name this factory builds a mode for.
    fn name(&self) -> ModeName;

    /// The Enterprise Feature that the mode is, or `None` for a mode of
    /// every Edition. A mode that is one checks the Edition at each use
    /// itself, and refuses while it is Community.
    fn feature(&self) -> Option<EnterpriseFeature>;

    /// Checks `settings` and builds the mode. Sends nothing.
    ///
    /// # Errors
    /// Returns the setting that is unusable and why, never its value.
    fn build(
        &self,
        settings: &ModeSettings,
        environment: &ModeEnvironment,
    ) -> Result<Arc<dyn SignInMode>, SettingError>;
}

/// The sign-in modes a build has, by name. The default has none.
#[derive(Clone, Default)]
pub struct SignInModes(Vec<Arc<dyn SignInModeFactory>>);

impl SignInModes {
    /// Adds a mode. A later factory for the same name replaces the earlier one.
    #[must_use]
    pub fn with(mut self, factory: Arc<dyn SignInModeFactory>) -> Self {
        self.0.retain(|other| other.name() != factory.name());
        self.0.push(factory);
        self
    }

    /// The factory of the mode that config names `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SignInModeFactory>> {
        self.0
            .iter()
            .find(|factory| factory.name().as_str() == name)
    }

    /// The name of each mode, once.
    pub fn names(&self) -> impl Iterator<Item = &'static str> {
        self.0.iter().map(|factory| factory.name().as_str())
    }
}

/// One mode of signing people in, built from a deployment's settings.
#[async_trait::async_trait]
pub trait SignInMode: Send + Sync {
    /// Begins the sign-in whose random value is `sign_in`, at `now`, and
    /// returns the URL to send the browser to.
    ///
    /// # Errors
    /// Returns the refusal that ends the sign-in before the browser leaves
    /// Coral.
    async fn begin(&self, sign_in: &SignInValue, now: UnixNanos) -> Result<Url, Refusal>;

    /// Says which sign-in `request`, at one of the mode's URLs, names. Core
    /// then looks for that sign-in's cookie in the browser.
    fn names(&self, request: &ModeRequest<'_>, now: UnixNanos) -> Named;

    /// Checks `request` against the sign-in `sign_in`, whose cookie core
    /// found and opened, and returns the person it proves.
    ///
    /// # Errors
    /// Returns the refusal that ends the sign-in.
    async fn check(
        &self,
        request: &ModeRequest<'_>,
        sign_in: &SignInValue,
        now: UnixNanos,
    ) -> Result<ProvenPerson, Refusal>;
}

/// A sign-in's random value. Core makes it when the sign-in begins, and finds
/// the sign-in's cookie by it.
#[derive(Clone, PartialEq, Eq)]
pub struct SignInValue(String);

impl fmt::Debug for SignInValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignInValue(<redacted>)")
    }
}

impl SignInValue {
    /// The value `value`.
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self(value)
    }

    /// The value as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A request at one of a mode's URLs, as core hands it to the mode.
#[derive(Clone, Copy)]
pub struct ModeRequest<'a> {
    /// The query, without its `?`.
    pub query: &'a str,
    /// The body.
    pub body: &'a [u8],
}

/// Prints no field: a request can carry a code or a signed answer.
impl fmt::Debug for ModeRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModeRequest").finish_non_exhaustive()
    }
}

/// Which sign-in a request at a mode's URL names.
#[derive(Debug)]
pub enum Named {
    /// The sign-in with this random value.
    SignIn(SignInValue),
    /// No sign-in: the request is not an answer to one that Coral began.
    NoSignIn,
    /// The request is refused before any sign-in is known.
    Refused(Refusal),
}

/// Why a mode ends a sign-in, as the description that the client receives.
///
/// The mode writes the description. Core sends it only in the form
/// `<code> <time>`, which [`Self::checked`] checks: one space, a code of at
/// most 64 lowercase letters, digits or `_` that starts with a letter, and
/// the time in RFC 3339 UTC to the second.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal(String);

impl Refusal {
    /// The refusal with `code` at `now`, in the form that core sends. The
    /// code must be at most 64 lowercase letters, digits or `_`, starting
    /// with a letter: for any other, [`Self::checked`] refuses the refusal,
    /// and core sends none of its text.
    #[must_use]
    pub fn new(code: &str, now: UnixNanos) -> Self {
        let time = chrono::DateTime::from_timestamp_nanos(now.get());
        Self(format!("{code} {}", time.format(TIME_FORMAT)))
    }

    /// A refusal whose description the mode wrote itself.
    #[must_use]
    pub const fn from_description(description: String) -> Self {
        Self(description)
    }

    /// The description, when it has the form that core sends. Its code,
    /// which the log records, is the text before the space.
    #[must_use]
    pub fn checked(&self) -> Option<&str> {
        let (code, time) = self.0.split_once(' ')?;
        (is_token(code, 64, |c| c == '_') && is_time(time)).then_some(&self.0)
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ`, and a time that exists. A leap second (`:60`) is
/// refused, as Reef refuses one.
fn is_time(value: &str) -> bool {
    value.len() == 20
        && value.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            10 => byte == b'T',
            13 | 16 => byte == b':',
            17 => (b'0'..=b'5').contains(&byte),
            19 => byte == b'Z',
            _ => byte.is_ascii_digit(),
        })
        && chrono::NaiveDateTime::parse_from_str(value, TIME_FORMAT).is_ok()
}

/// The person a mode proved.
///
/// Each kind of identifier has its own constructor, so a Subject always comes
/// with the issuer that verified it.
#[derive(Clone, PartialEq, Eq)]
pub struct ProvenPerson {
    identifier: Identifier,
    name: Option<String>,
    email: Option<String>,
    email_verified: bool,
}

/// Prints no field: a person's identifier, name and email are personal data.
impl fmt::Debug for ProvenPerson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProvenPerson").finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
enum Identifier {
    Subject { issuer: String, subject: String },
    NameId(String),
}

/// How a proven person is named: the kind and the value of a User
/// Identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersonIdentifier<'a> {
    /// A Subject, and the issuer that verified it.
    Subject {
        /// The issuer that verified the Subject.
        issuer: &'a str,
        /// The Subject.
        subject: &'a str,
    },
    /// The value of a `NameID`.
    NameId(&'a str),
}

impl ProvenPerson {
    /// The person whose Subject is `subject`, verified under `issuer`.
    #[must_use]
    pub const fn subject(issuer: String, subject: String) -> Self {
        Self::of(Identifier::Subject { issuer, subject })
    }

    /// The person named by the value of a `NameID`.
    #[must_use]
    pub const fn name_id(value: String) -> Self {
        Self::of(Identifier::NameId(value))
    }

    const fn of(identifier: Identifier) -> Self {
        Self {
            identifier,
            name: None,
            email: None,
            email_verified: false,
        }
    }

    /// Adds the person's name and email, and whether the mode verified the
    /// email.
    #[must_use]
    pub fn with_profile(
        mut self,
        name: Option<String>,
        email: Option<String>,
        email_verified: bool,
    ) -> Self {
        self.name = name;
        self.email = email;
        self.email_verified = email_verified;
        self
    }

    /// How the person is named.
    #[must_use]
    pub fn identifier(&self) -> PersonIdentifier<'_> {
        match &self.identifier {
            Identifier::Subject { issuer, subject } => {
                PersonIdentifier::Subject { issuer, subject }
            }
            Identifier::NameId(value) => PersonIdentifier::NameId(value),
        }
    }

    /// The person's name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The person's email.
    #[must_use]
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    /// Whether the mode verified the email.
    #[must_use]
    pub const fn email_verified(&self) -> bool {
        self.email_verified
    }
}

#[cfg(test)]
mod tests;
