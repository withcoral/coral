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
//! begins it, the URLs where its answer arrives ([`ModeUrl`]), the
//! `SameSite` of the sign-in cookie that answer needs ([`SameSite`]), which
//! sign-in a request at those URLs names, and the check of that request,
//! which ends in a [`ProvenPerson`] or a [`Refusal`]. Core does the rest:
//! the sign-in cookie, the User, the code and the tokens.
//!
//! No error here repeats a configured value, since an operator can paste a
//! secret under the wrong key.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use coral_db::{Clock, UnixNanos};
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
    pub const fn new(name: &'static str) -> Result<Self, InvalidName> {
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
    pub const fn is_name(value: &str) -> bool {
        is_token(value, 32, true)
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

/// At most `max_len` lowercase letters, digits, `_`, and `-` when `dash`,
/// starting with a letter.
const fn is_token(value: &str, max_len: usize, dash: bool) -> bool {
    let mut rest = value.as_bytes();
    if rest.len() > max_len || !matches!(rest.first(), Some(b'a'..=b'z')) {
        return false;
    }
    while let Some((byte, tail)) = rest.split_first() {
        if !(byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || *byte == b'_'
            || (dash && *byte == b'-'))
        {
            return false;
        }
        rest = tail;
    }
    true
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

/// The `SameSite` value of a mode's sign-in cookie.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SameSite {
    /// `Lax`: the browser sends the cookie when another site sends it to
    /// Coral with a GET, but not with a POST.
    #[default]
    Lax,
    /// `None`: the browser sends the cookie on every request to Coral, also
    /// a POST from another site. Core then always marks the cookie
    /// `Secure`, as browsers require.
    None,
}

impl SameSite {
    /// The value as a `Set-Cookie` writes it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lax => "Lax",
            Self::None => "None",
        }
    }
}

/// A URL that a mode answers at: `/auth/{name}/{segment}` of Coral's URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModeUrl {
    segment: &'static str,
    method: ModeMethod,
}

/// How a browser reaches a mode's URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModeMethod {
    /// A GET. Core hands the mode no body.
    Get,
    /// A POST. Core hands the mode a body of at most `body_limit` bytes, and
    /// refuses a longer one itself.
    Post {
        /// The most bytes of body the mode reads.
        body_limit: usize,
    },
}

impl ModeUrl {
    /// A GET at `segment`.
    #[must_use]
    pub const fn get(segment: &'static str) -> Self {
        Self {
            segment,
            method: ModeMethod::Get,
        }
    }

    /// A POST at `segment`, whose body is at most `body_limit` bytes.
    #[must_use]
    pub const fn post(segment: &'static str, body_limit: usize) -> Self {
        Self {
            segment,
            method: ModeMethod::Post { body_limit },
        }
    }

    /// How a browser reaches the URL.
    #[must_use]
    pub const fn method(self) -> ModeMethod {
        self.method
    }

    /// The URL's path below Coral's URL, for the mode `name`.
    #[must_use]
    pub fn path(self, name: ModeName) -> String {
        format!("/auth/{}/{}", name.as_str(), self.segment)
    }

    /// Checks that each of `urls` stays below `/auth/{name}/`: its segment
    /// has the form of a mode's name ([`ModeName::is_name`]), and no segment
    /// is declared twice with the same method.
    ///
    /// # Errors
    /// Returns the first rule that `urls` breaks.
    pub fn check_all(urls: &[Self]) -> Result<(), InvalidUrl> {
        for (index, url) in urls.iter().enumerate() {
            if !ModeName::is_name(url.segment) {
                return Err(InvalidUrl::Segment);
            }
            let same = |other: &Self| {
                other.segment == url.segment
                    && std::mem::discriminant(&other.method) == std::mem::discriminant(&url.method)
            };
            if urls.iter().skip(index + 1).any(same) {
                return Err(InvalidUrl::Repeated);
            }
        }
        Ok(())
    }
}

/// URLs that a mode cannot serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidUrl {
    /// A segment that does not have the form of a mode's name.
    #[error(
        "a sign-in mode's URL segment must be at most 32 lowercase letters, digits, `-` or `_`, starting with a letter"
    )]
    Segment,
    /// One segment declared twice with the same method.
    #[error("a sign-in mode declares one URL segment twice with the same method")]
    Repeated,
}

/// One mode of signing people in, built from a deployment's settings.
#[async_trait::async_trait]
pub trait SignInMode: Send + Sync {
    /// The URLs the mode answers at, which core serves below
    /// `/auth/{name}/`. Core reads them once, when the server starts, and
    /// refuses to start when [`ModeUrl::check_all`] refuses them.
    fn urls(&self) -> Vec<ModeUrl>;

    /// The `SameSite` value of the mode's sign-in cookie. A mode whose
    /// answer arrives as a POST from another site needs [`SameSite::None`].
    fn same_site(&self) -> SameSite {
        SameSite::Lax
    }

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

    /// The refusal of a request that names a sign-in, at `now`, when the
    /// browser holds no cookie for it, more than one, or one that does not
    /// open: the sign-in expired, or another browser began it. Core answers
    /// it directly, since no client is known to send the browser back to.
    fn no_sign_in_cookie(&self, now: UnixNanos) -> Refusal;

    /// Checks `request` against the sign-in `sign_in`, whose cookie core
    /// found and opened, and returns the person it proves.
    ///
    /// The mode reads `clock`, the deployment's clock, once, after its last
    /// answer from another server: an instant read before that answer can
    /// date a proof the server has only just issued in the future.
    ///
    /// # Errors
    /// Returns the refusal that ends the sign-in.
    async fn check(
        &self,
        request: &ModeRequest<'_>,
        sign_in: &SignInValue,
        clock: &dyn Clock,
    ) -> Result<ProvenPerson, Refusal>;

    /// The person who signs in as the Organization's first Owner, as the
    /// mode's settings name them, or `None` when they name no one. Core
    /// reads it once, when the server starts.
    fn first_owner(&self) -> Option<PersonIdentifier<'_>> {
        None
    }
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
    /// Core sends the browser to the web interface to begin one; in a
    /// deployment that has none, it answers with this refusal directly.
    NoSignIn(Refusal),
    /// The request is refused before any sign-in is known.
    Refused(Refusal),
}

/// The text core sends for a refusal whose description does not have the
/// form `<code> <time>`.
pub const DENIED: &str = "authorization was denied";

/// The text of [`Refusal::failed`].
const FAILED: &str = "authorization failed";

/// The OAuth error that a refusal sends to the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalError {
    /// `access_denied`: the person, their provider or the mode refused.
    AccessDenied,
    /// `invalid_request`: the answer at the mode's URL is not one it reads.
    InvalidRequest,
    /// `server_error`: the sign-in failed, and the person can try again.
    ServerError,
}

impl RefusalError {
    /// The error as OAuth writes it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccessDenied => "access_denied",
            Self::InvalidRequest => "invalid_request",
            Self::ServerError => "server_error",
        }
    }
}

/// Why a mode ends a sign-in: the OAuth error and the description that the
/// client receives, and a code.
///
/// A refusal that [`Self::new`] makes is described in the form
/// `<code> <time>`, which Reef shows, and core logs its code. The others
/// send a fixed text, and their code is for the mode's own log, written
/// where the mode knows what failed. Each code is text that the mode's code
/// fixes, never a request's or a setting's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    error: RefusalError,
    code: &'static str,
    description: Description,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Description {
    /// `<code> <time>`, sent only when the code has the form of one.
    Form(String),
    Fixed(&'static str),
}

impl Refusal {
    /// `access_denied`, described as `<code> <time>` at `now`. The code must
    /// be at most 64 lowercase letters, digits or `_`, starting with a
    /// letter: for any other, core sends [`DENIED`] and none of the code.
    #[must_use]
    pub fn new(code: &'static str, now: UnixNanos) -> Self {
        let time = chrono::DateTime::from_timestamp_nanos(now.get());
        Self {
            error: RefusalError::AccessDenied,
            code,
            description: Description::Form(format!("{code} {}", time.format(TIME_FORMAT))),
        }
    }

    /// `access_denied` with [`DENIED`]: the person or their provider
    /// refused.
    #[must_use]
    pub const fn denied(code: &'static str) -> Self {
        Self::fixed(RefusalError::AccessDenied, code, DENIED)
    }

    /// `server_error` with "authorization failed": the sign-in failed on
    /// Coral's side or the provider's.
    #[must_use]
    pub const fn failed(code: &'static str) -> Self {
        Self::fixed(RefusalError::ServerError, code, FAILED)
    }

    /// `invalid_request` with `text`.
    #[must_use]
    pub const fn invalid(code: &'static str, text: &'static str) -> Self {
        Self::fixed(RefusalError::InvalidRequest, code, text)
    }

    const fn fixed(error: RefusalError, code: &'static str, text: &'static str) -> Self {
        Self {
            error,
            code,
            description: Description::Fixed(text),
        }
    }

    /// The OAuth error to send.
    #[must_use]
    pub const fn error(&self) -> RefusalError {
        self.error
    }

    /// The description in the form `<code> <time>`, when [`Self::new`] made
    /// the refusal and its code has the form of one.
    #[must_use]
    pub fn checked(&self) -> Option<&str> {
        match &self.description {
            Description::Form(description) => is_code(self.code).then_some(description.as_str()),
            Description::Fixed(_) => None,
        }
    }

    /// The description to send: the checked form, [`DENIED`] in its place
    /// when the form is wrong, or the fixed text.
    #[must_use]
    pub fn description(&self) -> &str {
        match &self.description {
            Description::Form(_) => self.checked().unwrap_or(DENIED),
            Description::Fixed(text) => text,
        }
    }

    /// The code, when it has the form of one.
    #[must_use]
    pub fn code(&self) -> Option<&'static str> {
        is_code(self.code).then_some(self.code)
    }
}

/// At most 64 lowercase letters, digits or `_`, starting with a letter.
const fn is_code(value: &str) -> bool {
    is_token(value, 64, false)
}

/// The person a mode proved.
///
/// Each kind of identifier has its own constructor, so a Subject always comes
/// with the issuer that verified it.
#[derive(Clone, PartialEq, Eq)]
pub struct ProvenPerson {
    identifier: Identifier,
    display_name: Option<String>,
    name: Option<String>,
    email: Option<String>,
    email_verified: Option<bool>,
    hosted_domain: Option<String>,
    observed_at: Option<UnixNanos>,
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
            display_name: None,
            name: None,
            email: None,
            email_verified: None,
            hosted_domain: None,
            observed_at: None,
        }
    }

    /// Adds the name Coral shows for the person.
    #[must_use]
    pub fn with_display_name(mut self, display_name: Option<String>) -> Self {
        self.display_name = display_name;
        self
    }

    /// Adds the person's profile: their name and email, whether the
    /// provider says it verified the email (`None` when it says nothing),
    /// and the domain the provider hosts them under.
    #[must_use]
    pub fn with_profile(
        mut self,
        name: Option<String>,
        email: Option<String>,
        email_verified: Option<bool>,
        hosted_domain: Option<String>,
    ) -> Self {
        self.name = name;
        self.email = email;
        self.email_verified = email_verified;
        self.hosted_domain = hosted_domain;
        self
    }

    /// Adds the instant the provider issued the profile at. A sign-in
    /// whose profile is older than the one a User holds keeps that one.
    #[must_use]
    pub const fn with_observed_at(mut self, observed_at: UnixNanos) -> Self {
        self.observed_at = Some(observed_at);
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

    /// The name Coral shows for the person.
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
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

    /// Whether the provider says it verified the email, or `None` when it
    /// says nothing.
    #[must_use]
    pub const fn email_verified(&self) -> Option<bool> {
        self.email_verified
    }

    /// The domain the provider hosts the person under.
    #[must_use]
    pub fn hosted_domain(&self) -> Option<&str> {
        self.hosted_domain.as_deref()
    }

    /// The instant the provider issued the profile at, when the mode knows
    /// it.
    #[must_use]
    pub const fn observed_at(&self) -> Option<UnixNanos> {
        self.observed_at
    }
}

#[cfg(test)]
mod tests;
