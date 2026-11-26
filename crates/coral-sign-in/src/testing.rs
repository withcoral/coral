//! A sign-in mode that is no real protocol, for tests of the contract and of
//! core.
//!
//! Its settings are `sign_in_url`, which must be `https`, and `refuse`, a
//! code that refuses every check. Its begin URL is `sign_in_url` with the
//! sign-in's value as `sign_in` and Coral's URL as `return_to`. A request
//! names its sign-in by `sign_in` in the query, and its person by `person`.

use std::borrow::Cow;
use std::sync::Arc;

use coral_db::UnixNanos;
use coral_edition::EnterpriseFeature;
use serde::Deserialize;
use url::Url;

use crate::{
    ModeEnvironment, ModeName, ModeRequest, ModeSettings, Named, ProvenPerson, Refusal,
    SettingError, SignInMode, SignInModeFactory, SignInValue,
};

/// Builds the test mode, named `test`.
pub struct TestModeFactory;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    sign_in_url: String,
    #[serde(default)]
    refuse: Option<String>,
}

struct TestMode {
    sign_in_url: Url,
    refuse: Option<String>,
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
            coral_url: environment.coral_url.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl SignInMode for TestMode {
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
            (None, _) => Named::NoSignIn,
            (Some(value), None) => Named::SignIn(SignInValue::new(value.into_owned())),
            (Some(_), Some(_)) => Named::Refused(Refusal::new("test_malformed", now)),
        }
    }

    async fn check(
        &self,
        request: &ModeRequest<'_>,
        sign_in: &SignInValue,
        now: UnixNanos,
    ) -> Result<ProvenPerson, Refusal> {
        if values(request, "sign_in").next().as_deref() != Some(sign_in.as_str()) {
            return Err(Refusal::new("test_mismatch", now));
        }
        if let Some(code) = &self.refuse {
            return Err(Refusal::new(code, now));
        }
        values(request, "person")
            .next()
            .map(|person| ProvenPerson::name_id(person.into_owned()))
            .ok_or_else(|| Refusal::new("test_malformed", now))
    }
}

/// Each value of `name` in the request's query.
fn values<'a>(request: &ModeRequest<'a>, name: &'a str) -> impl Iterator<Item = Cow<'a, str>> {
    url::form_urlencoded::parse(request.query.as_bytes())
        .filter(move |(key, _)| key == name)
        .map(|(_, value)| value)
}
