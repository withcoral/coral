//! The Edition a Coral deployment runs as, and the check an Enterprise
//! Feature makes before it works.
//!
//! This crate owns the Edition's type and its check. What sets the Edition,
//! such as a licence, lives elsewhere and plugs in as an [`EditionSource`].
//! A licence can change while Coral runs, so an Enterprise Feature asks
//! [`EditionCheck::is_enterprise`] each time it is used, not once at startup.

use std::fmt;
use std::sync::Arc;

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
}

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

    /// A source whose answer changes, like a licence renewed while Coral runs.
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
