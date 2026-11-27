//! A sign-in mode that is no real protocol, for tests of the contract and of
//! core.
//!
//! Its settings are `sign_in_url`, which must be `https`; `refuse`, which
//! refuses every check with `test_refused`; `first_owner`, the `NameID` of
//! the first Owner; and `same_site`, `"lax"` (the default) or `"none"`. Its
//! begin URL is `sign_in_url` with the sign-in's value as `sign_in` and
//! Coral's URL as `return_to`. It answers a GET and a POST at `answer`, the
//! POST's body at most [`BODY_LIMIT`] bytes. A request names its sign-in by
//! `sign_in` and its person by `person`, in the query or a form body. A
//! request that names no sign-in is refused, where core answers it, with
//! `test_not_requested`, and one whose cookie core lacks with
//! `test_no_sign_in`. It serves the document `about`, which names Coral's
//! URL, and logs one line when the server starts.

use std::borrow::Cow;
use std::sync::Arc;

use coral_db::{Clock, UnixNanos};
use coral_edition::{EnterpriseFeature, EnterpriseFeatureId};
use serde::Deserialize;
use url::Url;

use crate::{
    ModeDocument, ModeEnvironment, ModeName, ModeRequest, ModeSettings, ModeUrl, Named,
    PersonIdentifier, ProvenPerson, Refusal, SameSite, SettingError, SignInMode, SignInModeFactory,
    SignInValue,
};

/// The most bytes of body the test mode's POST reads.
pub const BODY_LIMIT: usize = 1024;

/// The media type of the test mode's document `about`.
pub const ABOUT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

/// The Enterprise Feature that [`EnterpriseTestModeFactory`]'s mode is.
pub const TEST_FEATURE: EnterpriseFeature =
    EnterpriseFeature::new(EnterpriseFeatureId::new("test-sign-in"), "Test sign-in");

/// Builds the test mode, named `test`, a mode of every Edition.
pub struct TestModeFactory;

/// Builds the test mode, named `test`, as the Enterprise Feature
/// [`TEST_FEATURE`]. Its mode does not check the Edition.
pub struct EnterpriseTestModeFactory;

impl SignInModeFactory for EnterpriseTestModeFactory {
    fn name(&self) -> ModeName {
        TestModeFactory.name()
    }

    fn feature(&self) -> Option<EnterpriseFeature> {
        Some(TEST_FEATURE)
    }

    fn build(
        &self,
        settings: &ModeSettings,
        environment: &ModeEnvironment,
    ) -> Result<Arc<dyn SignInMode>, SettingError> {
        TestModeFactory.build(settings, environment)
    }
}

/// The test mode of `settings`, a TOML table, for a Coral at
/// `https://coral.example.test/`.
///
/// # Panics
/// When `settings` is not TOML, or the test mode refuses it.
#[must_use]
pub fn mode(settings: &str) -> Arc<dyn SignInMode> {
    let settings = ModeSettings::new(toml::from_str(settings).expect("TOML settings"));
    let environment =
        ModeEnvironment::new(Url::parse("https://coral.example.test/").expect("Coral's URL"));
    TestModeFactory
        .build(&settings, &environment)
        .unwrap_or_else(|error| panic!("{error}"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    sign_in_url: String,
    #[serde(default)]
    refuse: bool,
    #[serde(default)]
    first_owner: Option<String>,
    #[serde(default)]
    same_site: TestSameSite,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TestSameSite {
    #[default]
    Lax,
    None,
}

struct TestMode {
    sign_in_url: Url,
    refuse: bool,
    first_owner: Option<String>,
    same_site: SameSite,
    coral_url: Url,
}

impl SignInModeFactory for TestModeFactory {
    fn name(&self) -> ModeName {
        ModeName("test")
    }

    fn feature(&self) -> Option<EnterpriseFeature> {
        None
    }

    fn build(
        &self,
        settings: &ModeSettings,
        environment: &ModeEnvironment,
    ) -> Result<Arc<dyn SignInMode>, SettingError> {
        let settings: Settings = settings.decode()?;
        let sign_in_url = Url::parse(&settings.sign_in_url)
            .map_err(|_error| SettingError::new("sign_in_url", "must be a URL"))?;
        if sign_in_url.scheme() != "https" {
            return Err(SettingError::new("sign_in_url", "must be https"));
        }
        Ok(Arc::new(TestMode {
            sign_in_url,
            refuse: settings.refuse,
            first_owner: settings.first_owner,
            same_site: match settings.same_site {
                TestSameSite::Lax => SameSite::Lax,
                TestSameSite::None => SameSite::None,
            },
            coral_url: environment.coral_url.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl SignInMode for TestMode {
    fn urls(&self) -> Vec<ModeUrl> {
        vec![ModeUrl::get("answer"), ModeUrl::post("answer", BODY_LIMIT)]
    }

    fn documents(&self) -> Vec<ModeDocument> {
        vec![ModeDocument::new(
            "about",
            ABOUT_MEDIA_TYPE,
            format!("the test sign-in mode of {}", self.coral_url),
        )]
    }

    fn started(&self, _now: UnixNanos) {
        tracing::info!("the test sign-in mode started");
    }

    fn same_site(&self) -> SameSite {
        self.same_site
    }

    async fn begin(&self, sign_in: &SignInValue, _now: UnixNanos) -> Result<Url, Refusal> {
        let mut url = self.sign_in_url.clone();
        url.query_pairs_mut()
            .append_pair("sign_in", sign_in.as_str())
            .append_pair("return_to", self.coral_url.as_str());
        Ok(url)
    }

    fn names(&self, request: &ModeRequest<'_>, now: UnixNanos) -> Named {
        let mut values = values(request, "sign_in");
        match (values.next(), values.next()) {
            (None, _) => Named::NoSignIn(Refusal::new("test_not_requested", now)),
            (Some(value), None) => Named::SignIn(SignInValue::new(value.into_owned())),
            (Some(_), Some(_)) => Named::Refused(Refusal::new("test_malformed", now)),
        }
    }

    fn no_sign_in_cookie(&self, now: UnixNanos) -> Refusal {
        Refusal::new("test_no_sign_in", now)
    }

    async fn check(
        &self,
        request: &ModeRequest<'_>,
        sign_in: &SignInValue,
        clock: &dyn Clock,
    ) -> Result<ProvenPerson, Refusal> {
        let now = clock.now();
        if values(request, "sign_in").next().as_deref() != Some(sign_in.as_str()) {
            return Err(Refusal::new("test_mismatch", now));
        }
        if self.refuse {
            return Err(Refusal::new("test_refused", now));
        }
        values(request, "person")
            .next()
            .map(|person| {
                ProvenPerson::name_id(person.clone().into_owned())
                    .with_display_name(Some(person.into_owned()))
                    .with_observed_at(now)
            })
            .ok_or_else(|| Refusal::new("test_malformed", now))
    }

    fn first_owner(&self) -> Option<PersonIdentifier<'_>> {
        self.first_owner.as_deref().map(PersonIdentifier::NameId)
    }
}

/// Each value of `name` in the request's query, then in its form body.
fn values<'a>(request: &ModeRequest<'a>, name: &'a str) -> impl Iterator<Item = Cow<'a, str>> {
    url::form_urlencoded::parse(request.query.as_bytes())
        .chain(url::form_urlencoded::parse(request.body))
        .filter(move |(key, _)| key == name)
        .map(|(_, value)| value)
}
