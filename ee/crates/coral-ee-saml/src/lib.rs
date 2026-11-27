// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! SAML Sign-in: the sign-in mode `saml`, in place of OIDC. It begins a
//! sign-in by SAML 2.0 and serves Coral's metadata. It reads no response
//! yet, so it signs no one in.
//!
//! [`SamlModeFactory`] builds the mode from `[auth.provider]` with
//! `mode = "saml"`. The mode is the Enterprise Feature [`SAML_SIGN_IN`]: it
//! asks the Edition at each sign-in, and while the Edition is Community it
//! ends the sign-in before the browser leaves Coral, with
//! `saml_needs_enterprise`. Nothing is deleted, and with a License the
//! sign-in begins.
//!
//! A sign-in begins with an unsigned `AuthnRequest` sent by the
//! HTTP-Redirect binding, with no `RelayState`, whose ID is `_` and the
//! sign-in's random value. Coral's metadata document answers a GET at
//! `/auth/saml/metadata`, its entity ID, with no sign-in. At startup the
//! mode warns of a certificate past its last day or ending within 30 days.

use std::sync::Arc;

use coral_db::{Clock, UnixNanos};
use coral_edition::{EditionCheck, EnterpriseFeature, EnterpriseFeatureId};
use coral_sign_in::{
    ModeDocument, ModeEnvironment, ModeName, ModeRequest, ModeSettings, ModeUrl, Named,
    PersonIdentifier, ProvenPerson, Refusal, SameSite, SettingError, SignInMode, SignInModeFactory,
    SignInValue,
};
use url::Url;

mod certificate;
mod metadata;
mod request;
mod settings;
mod xml;

use request::RequestTarget;
use settings::Settings;

/// The Enterprise Feature that signs people in by SAML 2.0.
pub const SAML_SIGN_IN: EnterpriseFeature =
    EnterpriseFeature::new(EnterpriseFeatureId::new("saml-sign-in"), "SAML sign-in");

/// The mode's name: `mode = "saml"`, and its URLs below `/auth/saml/`.
const NAME: ModeName = match ModeName::new("saml") {
    Ok(name) => name,
    Err(_) => panic!("`saml` has the form of a mode's name"),
};

/// The segment of Coral's metadata document, whose URL is Coral's entity
/// ID.
const METADATA: &str = "metadata";

/// The URL that receives the response: a form of at most 256 KiB.
const ACS: ModeUrl = ModeUrl::post("acs", 256 * 1024);

/// The code of a sign-in that the Community Edition refuses.
const NEEDS_ENTERPRISE: &str = "saml_needs_enterprise";

/// The code of a response whose sign-in cookie the browser lacks.
const NO_SIGN_IN: &str = "saml_no_sign_in";

/// The code of a response that Coral cannot read.
const MALFORMED: &str = "saml_malformed";

/// The code of a request that Coral could not write.
const REQUEST_FAILED: &str = "saml_request";

/// Builds the SAML mode, which works only while `edition` says Enterprise.
pub struct SamlModeFactory {
    edition: EditionCheck,
}

impl SamlModeFactory {
    /// The factory of a deployment whose Edition `edition` decides.
    #[must_use]
    pub const fn new(edition: EditionCheck) -> Self {
        Self { edition }
    }
}

impl SignInModeFactory for SamlModeFactory {
    fn name(&self) -> ModeName {
        NAME
    }

    fn feature(&self) -> Option<EnterpriseFeature> {
        Some(SAML_SIGN_IN)
    }

    fn build(
        &self,
        settings: &ModeSettings,
        environment: &ModeEnvironment,
    ) -> Result<Arc<dyn SignInMode>, SettingError> {
        let settings = Settings::check(settings)?;
        let coral_entity_id = coral_url_at(&environment.coral_url, ModeUrl::get(METADATA));
        let acs_url = coral_url_at(&environment.coral_url, ACS);
        let metadata = ModeDocument::new(
            METADATA,
            metadata::MEDIA_TYPE,
            metadata::document(&coral_entity_id, &acs_url),
        );
        Ok(Arc::new(SamlMode {
            edition: self.edition.clone(),
            coral_entity_id,
            acs_url,
            metadata,
            settings,
        }))
    }
}

/// The mode's URL `url` of Coral at `coral_url`, which core mounts at the
/// root.
fn coral_url_at(coral_url: &Url, url: ModeUrl) -> String {
    format!(
        "{}{}",
        coral_url.as_str().trim_end_matches('/'),
        url.path(NAME)
    )
}

struct SamlMode {
    edition: EditionCheck,
    settings: Settings,
    /// Coral's entity ID: its metadata URL.
    coral_entity_id: String,
    /// Coral's URL that receives the response.
    acs_url: String,
    /// Coral's metadata document.
    metadata: ModeDocument,
}

#[async_trait::async_trait]
impl SignInMode for SamlMode {
    /// None: the mode serves no URL that receives a response, so it accepts
    /// none unchecked.
    fn urls(&self) -> Vec<ModeUrl> {
        Vec::new()
    }

    /// The metadata document, at Coral's entity ID. It answers with no
    /// sign-in, in every Edition.
    fn documents(&self) -> Vec<ModeDocument> {
        vec![self.metadata.clone()]
    }

    fn started(&self, now: UnixNanos) {
        certificate::warn_at_start(&self.settings.certificates, now);
    }

    /// The Identity Provider's response arrives as a POST from its own site.
    fn same_site(&self) -> SameSite {
        SameSite::None
    }

    async fn begin(&self, sign_in: &SignInValue, now: UnixNanos) -> Result<Url, Refusal> {
        if !self.edition.is_enterprise() {
            return Err(Refusal::new(NEEDS_ENTERPRISE, now));
        }
        RequestTarget {
            sign_in_url: &self.settings.sign_in_url,
            acs_url: &self.acs_url,
            coral_entity_id: &self.coral_entity_id,
        }
        .redirect(sign_in, now)
        .map_err(|_error| Refusal::failed(REQUEST_FAILED))
    }

    /// No URL of this mode receives a request yet (see [`Self::urls`]).
    fn names(&self, _request: &ModeRequest<'_>, now: UnixNanos) -> Named {
        Named::Refused(Refusal::new(MALFORMED, now))
    }

    fn no_sign_in_cookie(&self, now: UnixNanos) -> Refusal {
        Refusal::new(NO_SIGN_IN, now)
    }

    /// No URL of this mode receives a request yet (see [`Self::urls`]), so
    /// every request is refused.
    async fn check(
        &self,
        _request: &ModeRequest<'_>,
        _sign_in: &SignInValue,
        clock: &dyn Clock,
    ) -> Result<ProvenPerson, Refusal> {
        let now = clock.now();
        if !self.edition.is_enterprise() {
            return Err(Refusal::new(NEEDS_ENTERPRISE, now));
        }
        Err(Refusal::new(MALFORMED, now))
    }

    fn first_owner(&self) -> Option<PersonIdentifier<'_>> {
        self.settings
            .bootstrap_owner_name_id
            .as_deref()
            .map(PersonIdentifier::NameId)
    }
}

#[cfg(test)]
mod tests;
