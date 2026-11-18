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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// A source whose answer changes.
    struct Switch(AtomicBool);

    impl EditionSource for Switch {
        fn edition(&self) -> Edition {
            if self.0.load(Ordering::SeqCst) {
                Edition::Enterprise
            } else {
                Edition::Community
            }
        }
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
    fn check_follows_a_source_that_changes() {
        let switch = Arc::new(Switch(AtomicBool::new(false)));
        let check = EditionCheck::new(Arc::clone(&switch) as Arc<dyn EditionSource>);
        assert!(!check.is_enterprise());

        switch.0.store(true, Ordering::SeqCst);
        assert!(check.is_enterprise());

        switch.0.store(false, Ordering::SeqCst);
        assert!(!check.is_enterprise());
    }
}
