//! Validated, client-minted episode identifier.

use std::fmt;

use coral_api::CORAL_EPISODE_ID_MAX_LEN;

use crate::bootstrap::AppError;

/// App-owned identity for one validated, client-minted episode id.
///
/// The id round-trips as the `coral-episode-id` gRPC metadata value on every
/// subsequent Coral call, so it is constrained to graphic ASCII (no whitespace,
/// control bytes, or non-ASCII) with a bounded length — a UUID or ULID fits.
/// Like [`WorkspaceName`](crate::workspaces::WorkspaceName), the validated id is
/// carried as a narrow type so the store and the service boundary never handle
/// an unvalidated string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct EpisodeId(String);

impl EpisodeId {
    /// Parse and validate a client-minted episode id. Rejects an empty id, one
    /// longer than [`CORAL_EPISODE_ID_MAX_LEN`] bytes, or one containing any byte
    /// outside graphic ASCII (`0x21..=0x7E`) — see the `OpenEpisode` proto.
    pub(crate) fn parse(id: &str) -> Result<Self, AppError> {
        if id.is_empty() {
            return Err(AppError::InvalidInput("missing episode id".to_string()));
        }
        if id.len() > CORAL_EPISODE_ID_MAX_LEN {
            return Err(AppError::InvalidInput(format!(
                "episode id must be at most {CORAL_EPISODE_ID_MAX_LEN} bytes"
            )));
        }
        if !id.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(AppError::InvalidInput(
                "episode id must be graphic ASCII (no spaces, control, or non-ASCII bytes)"
                    .to_string(),
            ));
        }
        Ok(Self(id.to_string()))
    }

    /// Borrow the validated id for metadata and persistence boundaries that
    /// operate on strings.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EpisodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use coral_api::CORAL_EPISODE_ID_MAX_LEN;

    use super::EpisodeId;

    #[test]
    fn accepts_uuid_and_ulid() {
        EpisodeId::parse("550e8400-e29b-41d4-a716-446655440000").expect("uuid is valid");
        EpisodeId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("ulid is valid");
    }

    #[test]
    fn rejects_empty_whitespace_and_non_ascii() {
        EpisodeId::parse("").expect_err("empty id must be rejected");
        EpisodeId::parse("   ").expect_err("whitespace id must be rejected");
        EpisodeId::parse("has space").expect_err("embedded space must be rejected");
        EpisodeId::parse("tab\tid").expect_err("control byte must be rejected");
        EpisodeId::parse("épisode").expect_err("non-ASCII must be rejected");
    }

    #[test]
    fn enforces_max_length() {
        EpisodeId::parse(&"a".repeat(CORAL_EPISODE_ID_MAX_LEN)).expect("max length is valid");
        EpisodeId::parse(&"a".repeat(CORAL_EPISODE_ID_MAX_LEN + 1))
            .expect_err("over-long id must be rejected");
    }
}
