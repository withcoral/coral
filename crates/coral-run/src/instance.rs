//! The Coral Instance (ADR 0025): one running Coral server process, named by
//! a random id it generates each time it starts.

use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

/// The id of one Coral Instance. A Run row records the id of the Coral
/// Instance that holds its claim. The id is new on every start, so no two
/// live Coral Instances share one; it is not a host or pod name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoralInstanceId(Arc<str>);

impl CoralInstanceId {
    /// A new id for a Coral Instance that is starting.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().hyphenated().to_string()))
    }

    /// The id a Run row recorded.
    pub(crate) fn from_stored(value: String) -> Self {
        Self(Arc::from(value))
    }

    /// The id as the Run row records it.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CoralInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
