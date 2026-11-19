//! The Edition a Coral deployment runs as, and the check an Enterprise
//! Feature makes before it works.
//!
//! This crate owns the Edition's type and its check. What sets the Edition,
//! such as a License, lives elsewhere and plugs in as an [`EditionSource`].
//! Startup hands the source the License the deployment configures, once,
//! through [`EditionCheck::apply_license`], and a periodic pass asks it,
//! through [`EditionCheck::recheck_license`], whether Coral may keep
//! running: an Evaluation License stops Coral when it expires. An Enterprise
//! Feature still asks [`EditionCheck::is_enterprise`] each time it is used,
//! not once at startup.
//!
//! A launching binary registers the Enterprise Features it contains as
//! [`EnterpriseFeatures`], and [`EditionInspection::report`] says what state each
//! is in, for Reef to show.

use std::fmt;
use std::sync::Arc;

use coral_credentials::Secret;

/// Whether a deployment runs as the Community Edition or the Enterprise
/// Edition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edition {
    /// No Enterprise Feature works. Every build starts here.
    Community,
    /// The Enterprise Features the build contains work.
    Enterprise,
}

impl Edition {
    /// The Edition's stable lowercase name, for logs and telemetry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Community => "community",
            Self::Enterprise => "enterprise",
        }
    }
}

impl fmt::Display for Edition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decides the deployment's Edition each time it is asked.
pub trait EditionSource: Send + Sync {
    /// The Edition right now.
    fn edition(&self) -> Edition;

    /// Takes the License the deployment configures, once at startup and
    /// before anything asks for the Edition, and returns what startup should
    /// log about it.
    ///
    /// `today` is the UTC date startup read from its clock. Only an
    /// Evaluation License is decided with it (ADR 0038).
    ///
    /// The default is a source that reads no License: it keeps its Edition
    /// and warns that it ignores a configured License.
    ///
    /// # Errors
    ///
    /// Returns [`LicenseRefusal`] when Coral must not start with `license`.
    fn apply_license(
        &self,
        license: Option<&Secret>,
        today: chrono::NaiveDate,
    ) -> Result<Vec<LicenseNotice>, LicenseRefusal> {
        let _ = today;
        Ok(license
            .map(|_| {
                LicenseNotice::warning(
                    "This build reads no License, so it ignores the configured License.",
                )
            })
            .into_iter()
            .collect())
    }

    /// Whether Coral may keep running with the License it started with, on
    /// `today`, the UTC date a periodic pass read from the clock.
    ///
    /// The default is a build that reads no License: it always may.
    ///
    /// # Errors
    ///
    /// Returns [`LicenseRefusal`] when Coral must stop, such as when an
    /// Evaluation License has expired.
    fn recheck_license(&self, today: chrono::NaiveDate) -> Result<(), LicenseRefusal> {
        let _ = today;
        Ok(())
    }
}

/// How loudly startup logs a [`LicenseNotice`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoticeLevel {
    /// What the License set.
    Info,
    /// Something the operator should act on.
    Warning,
}

/// One line startup logs about the configured License.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LicenseNotice {
    /// How loudly to log it.
    pub level: NoticeLevel,
    /// The line, for people to read.
    pub message: String,
}

impl LicenseNotice {
    /// A line that says what the License set.
    #[must_use]
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            level: NoticeLevel::Info,
            message: message.into(),
        }
    }

    /// A line the operator should act on.
    #[must_use]
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            level: NoticeLevel::Warning,
            message: message.into(),
        }
    }
}

/// Why Coral must not start with the configured License. It says what is
/// wrong and what the operator can do about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LicenseRefusal(String);

impl LicenseRefusal {
    /// A refusal that says `message`.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for LicenseRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LicenseRefusal {}

/// A fixed Edition is its own source.
impl EditionSource for Edition {
    fn edition(&self) -> Edition {
        *self
    }
}

/// A shared handle on the deployment's [`EditionSource`].
///
/// The default always answers [`Edition::Community`].
#[derive(Clone)]
pub struct EditionCheck(Arc<dyn EditionSource>);

impl EditionCheck {
    /// Asks `source` for the Edition on every check.
    #[must_use]
    pub fn new(source: Arc<dyn EditionSource>) -> Self {
        Self(source)
    }

    /// The Edition right now.
    #[must_use]
    pub fn current(&self) -> Edition {
        self.0.edition()
    }

    /// Hands the source the License the deployment configures, once at
    /// startup. See [`EditionSource::apply_license`].
    ///
    /// # Errors
    ///
    /// Returns [`LicenseRefusal`] when Coral must not start with `license`.
    pub fn apply_license(
        &self,
        license: Option<&Secret>,
        today: chrono::NaiveDate,
    ) -> Result<Vec<LicenseNotice>, LicenseRefusal> {
        self.0.apply_license(license, today)
    }

    /// Whether Coral may keep running with its License on `today`. See
    /// [`EditionSource::recheck_license`].
    ///
    /// # Errors
    ///
    /// Returns [`LicenseRefusal`] when Coral must stop.
    pub fn recheck_license(&self, today: chrono::NaiveDate) -> Result<(), LicenseRefusal> {
        self.0.recheck_license(today)
    }

    /// Whether Enterprise Features work right now.
    #[must_use]
    pub fn is_enterprise(&self) -> bool {
        self.current() == Edition::Enterprise
    }
}

/// The deployment's Edition beside the Enterprise Features the launching
/// binary registers, for the private API to report.
#[derive(Clone, Debug, Default)]
pub struct EditionInspection {
    check: EditionCheck,
    features: EnterpriseFeatures,
}

impl EditionInspection {
    /// Reports `features` in the Edition `check` decides.
    #[must_use]
    pub const fn new(check: EditionCheck, features: EnterpriseFeatures) -> Self {
        Self { check, features }
    }

    /// The Edition right now, and the state of each feature in it. It asks
    /// the source once, so every feature agrees with the Edition.
    #[must_use]
    pub fn report(&self) -> EditionReport {
        let edition = self.check.current();
        let state = match edition {
            Edition::Community => FeatureState::NeedsEnterprise,
            Edition::Enterprise => FeatureState::Enabled,
        };
        EditionReport {
            edition,
            features: self
                .features
                .0
                .iter()
                .map(|feature| (feature.clone(), state))
                .collect(),
        }
    }
}

/// The stable name code gives an Enterprise Feature. Reef finds a feature
/// by it, so it never changes once a release reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EnterpriseFeatureId(&'static str);

impl EnterpriseFeatureId {
    /// The id `id`, in lowercase words joined by hyphens.
    #[must_use]
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    /// The id as the private API reports it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for EnterpriseFeatureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// An Enterprise Feature a build contains, as people see it named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnterpriseFeature {
    id: EnterpriseFeatureId,
    display_name: &'static str,
}

impl EnterpriseFeature {
    /// A feature that `id` names for code and `display_name` names for
    /// people.
    #[must_use]
    pub const fn new(id: EnterpriseFeatureId, display_name: &'static str) -> Self {
        Self { id, display_name }
    }

    /// The feature's stable id.
    #[must_use]
    pub const fn id(&self) -> EnterpriseFeatureId {
        self.id
    }

    /// The name people read.
    #[must_use]
    pub const fn display_name(&self) -> &'static str {
        self.display_name
    }
}

/// The Enterprise Features a launching binary registers. Core registers
/// none, so a source build does not know they exist (ADR 0033).
#[derive(Clone, Debug, Default)]
pub struct EnterpriseFeatures(Vec<EnterpriseFeature>);

impl EnterpriseFeatures {
    /// Adds a feature. A later feature with the same id replaces the
    /// earlier one.
    #[must_use]
    pub fn with(mut self, feature: EnterpriseFeature) -> Self {
        self.0.retain(|other| other.id != feature.id);
        self.0.push(feature);
        self
    }
}

/// Whether an Enterprise Feature works right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureState {
    /// The Edition is Enterprise, so the feature works.
    Enabled,
    /// The Edition is Community, so the feature fails closed.
    NeedsEnterprise,
}

/// The Edition at one moment, and each registered feature's state in it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditionReport {
    /// The Edition when asked.
    pub edition: Edition,
    /// Each registered feature, in the order registered, with its state.
    pub features: Vec<(EnterpriseFeature, FeatureState)>,
}

impl Default for EditionCheck {
    fn default() -> Self {
        Self::new(Arc::new(Edition::Community))
    }
}

impl fmt::Debug for EditionCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EditionCheck")
            .field(&self.current())
            .finish()
    }
}

/// Sources a test drives.
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{Edition, EditionSource};

    /// An Edition a test changes while Coral runs, as a License can.
    #[derive(Debug, Default)]
    pub struct SwitchedEdition(AtomicBool);

    impl SwitchedEdition {
        /// Makes the Edition `edition` from the next read on.
        pub fn set(&self, edition: Edition) {
            self.0
                .store(edition == Edition::Enterprise, Ordering::SeqCst);
        }
    }

    impl EditionSource for SwitchedEdition {
        fn edition(&self) -> Edition {
            if self.0.load(Ordering::SeqCst) {
                Edition::Enterprise
            } else {
                Edition::Community
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_fixtures::SwitchedEdition;
    use super::*;

    fn id(id: &'static str) -> EnterpriseFeatureId {
        EnterpriseFeatureId::new(id)
    }

    #[test]
    fn check_answers_what_its_source_says() {
        for (check, expected) in [
            (EditionCheck::default(), Edition::Community),
            (
                EditionCheck::new(Arc::new(Edition::Community)),
                Edition::Community,
            ),
            (
                EditionCheck::new(Arc::new(Edition::Enterprise)),
                Edition::Enterprise,
            ),
        ] {
            assert_eq!(check.current(), expected);
            assert_eq!(check.is_enterprise(), expected == Edition::Enterprise);
        }
    }

    #[test]
    fn a_source_that_reads_no_license_keeps_its_edition() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 11, 18).expect("date");
        for (license, warns) in [(None, false), (Some("v4.public.anything"), true)] {
            let license = license.map(|text: &str| Secret::new(text.to_owned()).expect("secret"));
            let check = EditionCheck::default();
            let notices = check
                .apply_license(license.as_ref(), today)
                .expect("never refused");
            check.recheck_license(today).expect("never stops Coral");
            assert_eq!(check.current(), Edition::Community);
            assert_eq!(
                notices
                    .iter()
                    .any(|notice| notice.level == NoticeLevel::Warning),
                warns,
                "{license:?}"
            );
        }
    }

    #[test]
    fn each_feature_follows_the_edition_when_asked() {
        let switch = Arc::new(SwitchedEdition::default());
        let check = EditionCheck::new(Arc::clone(&switch) as Arc<dyn EditionSource>);
        let inspection = EditionInspection::new(
            check,
            EnterpriseFeatures::default()
                .with(EnterpriseFeature::new(id("one"), "One"))
                .with(EnterpriseFeature::new(id("two"), "Two"))
                // A later registration under the same id replaces the earlier one.
                .with(EnterpriseFeature::new(id("one"), "One again")),
        );

        for (edition, state) in [
            (Edition::Community, FeatureState::NeedsEnterprise),
            (Edition::Enterprise, FeatureState::Enabled),
            (Edition::Community, FeatureState::NeedsEnterprise),
        ] {
            switch.set(edition);
            let report = inspection.report();
            assert_eq!(report.edition, edition);
            assert_eq!(
                report
                    .features
                    .iter()
                    .map(|(feature, state)| (feature.id(), feature.display_name(), *state))
                    .collect::<Vec<_>>(),
                [(id("two"), "Two", state), (id("one"), "One again", state)]
            );
        }
    }

    #[test]
    fn a_build_that_registers_nothing_reports_community_alone() {
        let report = EditionInspection::default().report();
        assert_eq!(report.edition, Edition::Community);
        assert_eq!(report.features, []);
    }

    #[test]
    fn check_follows_a_source_that_changes() {
        let switch = Arc::new(SwitchedEdition::default());
        let check = EditionCheck::new(Arc::clone(&switch) as Arc<dyn EditionSource>);
        assert!(!check.is_enterprise());

        switch.set(Edition::Enterprise);
        assert!(check.is_enterprise());

        switch.set(Edition::Community);
        assert!(!check.is_enterprise());
    }
}
