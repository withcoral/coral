//! The version a row keeps of itself.
//!
//! A row that a change can conflict on counts its accepted changes in an
//! integer column: the statement that changes the row raises the count by one.
//! A caller who read the row sends the count back with its change, and the
//! change is refused when the row has moved on.
//!
//! A repository binds a [`Version`] with [`Version::get`]. It builds one in
//! its row decoder with [`Version::new`], and reports a count that is not
//! positive as corrupt data.

use std::fmt;
use std::str::FromStr;

/// What a row stores as its version: a positive count of accepted changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(i64);

impl Version {
    /// The version of a row that no change has touched since it was created.
    pub const FIRST: Self = Self(1);

    /// The version a row holds as `value`, or `None` unless `value` is
    /// positive.
    #[must_use]
    pub const fn new(value: i64) -> Option<Self> {
        if value > 0 { Some(Self(value)) } else { None }
    }

    /// Returns the count, as a row holds it.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// The version after one more accepted change, or `None` past the last
    /// count a row can hold.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// The canonical text of a version: its count in decimal.
impl fmt::Display for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Text that is not the canonical text of a [`Version`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidVersion;

impl fmt::Display for InvalidVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("a version is a positive decimal integer with no sign and no leading zero")
    }
}

impl std::error::Error for InvalidVersion {}

/// Reads only the text that [`Version`] writes, so one version has one
/// spelling: ASCII digits, no sign, no leading zero, and a positive count
/// that a row can hold.
impl FromStr for Version {
    type Err = InvalidVersion;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.starts_with('0') || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidVersion);
        }
        // An empty text, and a count past the last one a row can hold, fail
        // here.
        text.parse::<i64>()
            .ok()
            .and_then(Self::new)
            .ok_or(InvalidVersion)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_reads_only_the_text_it_writes() {
        for (text, count) in [("1", 1), ("42", 42), ("9223372036854775807", i64::MAX)] {
            let version = text.parse::<Version>().expect(text);
            assert_eq!(version.get(), count, "{text}");
            assert_eq!(version.to_string(), text);
        }
        for text in [
            "",
            "0",
            "-1",
            "+1",
            "01",
            "00",
            " 1",
            "1 ",
            "1.0",
            "1e3",
            "0x1",
            "١",
            "stale",
            // One past the last count a row can hold.
            "9223372036854775808",
        ] {
            assert_eq!(text.parse::<Version>(), Err(InvalidVersion), "{text:?}");
        }
    }

    #[test]
    fn a_version_is_positive_and_counts_up_to_the_last_count_a_row_holds() {
        assert_eq!(Version::new(1), Some(Version::FIRST));
        for count in [0, -1, i64::MIN] {
            assert_eq!(Version::new(count), None, "{count}");
        }
        assert_eq!(Version::FIRST.next(), Version::new(2));
        let last = Version::new(i64::MAX).expect("the last count");
        assert_eq!(last.next(), None);
        assert!(Version::FIRST < last);
    }
}
