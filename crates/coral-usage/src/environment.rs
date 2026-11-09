//! What stops one process from sending, whatever the setting says.

/// Why this process never sends a Usage Report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendBlock {
    /// An environment variable turned sending off for this process.
    Environment(BlockingVariable),
    /// A source or development build. Only release builds send.
    SourceBuild,
}

/// The environment variables that turn sending off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockingVariable {
    /// `DO_NOT_TRACK` with any value but empty, `0` or `false`.
    DoNotTrack,
    /// `CORAL_TELEMETRY` set to `0` or `false`.
    CoralTelemetry,
    /// `CI` with any non-empty value.
    Ci,
}

impl BlockingVariable {
    /// The variable's name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::DoNotTrack => "DO_NOT_TRACK",
            Self::CoralTelemetry => "CORAL_TELEMETRY",
            Self::Ci => "CI",
        }
    }
}

/// How this binary was built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Build {
    /// Built by the release workflow. Only a release build sends.
    Release,
    /// Built from a checkout.
    Source,
}

impl Build {
    /// The value of the compile-time `CORAL_RELEASE_BUILD` marker: `1` is a
    /// release build, anything else a source build.
    #[must_use]
    pub fn from_marker(marker: Option<&str>) -> Self {
        if marker == Some("1") {
            Self::Release
        } else {
            Self::Source
        }
    }

    pub(crate) const fn wire(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Source => "source",
        }
    }
}

/// The values bootstrap read for the variables that turn sending off.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendEnvironment {
    /// `DO_NOT_TRACK`, when set.
    pub do_not_track: Option<String>,
    /// `CORAL_TELEMETRY`, when set.
    pub coral_telemetry: Option<String>,
    /// `CI`, when set.
    pub ci: Option<String>,
}

impl SendEnvironment {
    /// The first rule that stops this process from sending, if any. Values
    /// are trimmed and compared without case.
    #[must_use]
    pub fn block(&self, build: Build) -> Option<SendBlock> {
        let value = |value: &Option<String>| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
        };
        let off = |value: &str| value == "0" || value == "false";
        if value(&self.do_not_track).is_some_and(|value| !off(&value)) {
            return Some(SendBlock::Environment(BlockingVariable::DoNotTrack));
        }
        if value(&self.coral_telemetry).is_some_and(|value| off(&value)) {
            return Some(SendBlock::Environment(BlockingVariable::CoralTelemetry));
        }
        if value(&self.ci).is_some() {
            return Some(SendBlock::Environment(BlockingVariable::Ci));
        }
        (build == Build::Source).then_some(SendBlock::SourceBuild)
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockingVariable, Build, SendBlock, SendEnvironment};

    #[test]
    fn each_rule_stops_sending_only_for_its_values() {
        let env = |dnt: Option<&str>, telemetry: Option<&str>, ci: Option<&str>| SendEnvironment {
            do_not_track: dnt.map(str::to_owned),
            coral_telemetry: telemetry.map(str::to_owned),
            ci: ci.map(str::to_owned),
        };
        let blocked = |variable| Some(SendBlock::Environment(variable));
        for (environment, build, expected) in [
            (env(None, None, None), Build::Release, None),
            (
                env(None, None, None),
                Build::Source,
                Some(SendBlock::SourceBuild),
            ),
            (
                env(Some("1"), None, None),
                Build::Release,
                blocked(BlockingVariable::DoNotTrack),
            ),
            (
                env(Some(" TRUE "), None, None),
                Build::Release,
                blocked(BlockingVariable::DoNotTrack),
            ),
            (
                env(Some("yes"), None, None),
                Build::Release,
                blocked(BlockingVariable::DoNotTrack),
            ),
            (env(Some("0"), None, None), Build::Release, None),
            (env(Some("False"), None, None), Build::Release, None),
            (env(Some(""), None, None), Build::Release, None),
            (
                env(None, Some("0"), None),
                Build::Release,
                blocked(BlockingVariable::CoralTelemetry),
            ),
            (
                env(None, Some("FALSE"), None),
                Build::Release,
                blocked(BlockingVariable::CoralTelemetry),
            ),
            (env(None, Some("1"), None), Build::Release, None),
            (env(None, Some(""), None), Build::Release, None),
            (
                env(None, None, Some("true")),
                Build::Release,
                blocked(BlockingVariable::Ci),
            ),
            (
                env(None, None, Some("0")),
                Build::Release,
                blocked(BlockingVariable::Ci),
            ),
            (env(None, None, Some(" ")), Build::Release, None),
            (
                env(None, Some("0"), Some("1")),
                Build::Source,
                blocked(BlockingVariable::CoralTelemetry),
            ),
        ] {
            assert_eq!(
                environment.block(build),
                expected,
                "{environment:?} {build:?}"
            );
        }
    }

    #[test]
    fn only_the_release_marker_makes_a_release_build() {
        assert_eq!(Build::from_marker(Some("1")), Build::Release);
        for marker in [None, Some(""), Some("0"), Some("true")] {
            assert_eq!(Build::from_marker(marker), Build::Source, "{marker:?}");
        }
    }
}
