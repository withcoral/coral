use base64::Engine as _;
use coral_workspace::WorkspaceId;
use coral_workspace::{AuthenticatedPrincipal, PrincipalKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{Error, ProviderCallRoot, SearchCursor};

/// The ranking format a cursor pages. It changes whenever a ranking change
/// can reorder a result universe, so a cursor issued before fails as
/// incompatible instead of paging a mixed one. 2: the phrase test, the
/// Postgres word score and near-spelling changed, and a Provider filter keeps
/// Saved Functions.
const SEARCH_CURSOR_FORMAT: u8 = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPayload {
    version: u8,
    position: u64,
    digest: String,
}

pub(super) fn decode_cursor_position(
    cursor: Option<&SearchCursor>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    normalized_query: Option<&str>,
    provider: Option<&ProviderCallRoot>,
) -> Result<usize, Error> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.as_str())
        .map_err(|_error| Error::InvalidCursor)?;
    let payload: CursorPayload =
        serde_json::from_slice(&bytes).map_err(|_error| Error::InvalidCursor)?;
    if payload.version != SEARCH_CURSOR_FORMAT
        || payload.digest
            != cursor_digest(
                actor,
                workspace_id,
                normalized_query,
                provider,
                payload.position,
            )
    {
        return Err(Error::InvalidCursor);
    }
    usize::try_from(payload.position).map_err(|_error| Error::InvalidCursor)
}

pub(super) fn encode_cursor(
    position: usize,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    normalized_query: Option<&str>,
    provider: Option<&ProviderCallRoot>,
) -> Result<SearchCursor, Error> {
    let position = u64::try_from(position).map_err(|_error| Error::ServiceUnavailable)?;
    let payload = CursorPayload {
        version: SEARCH_CURSOR_FORMAT,
        position,
        digest: cursor_digest(actor, workspace_id, normalized_query, provider, position),
    };
    let bytes = serde_json::to_vec(&payload).map_err(|_error| Error::ServiceUnavailable)?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    SearchCursor::encoded(&encoded)
}

fn cursor_digest(
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    normalized_query: Option<&str>,
    provider: Option<&ProviderCallRoot>,
    position: u64,
) -> String {
    let mut hasher = Sha256::new();
    let workspace = workspace_id.to_string();
    let provider = provider.map(ProviderCallRoot::as_str).unwrap_or_default();
    let position = position.to_string();
    for value in [
        match actor.principal().kind() {
            PrincipalKind::User => "user",
            PrincipalKind::Agent => "agent",
        },
        actor.principal().id().as_str(),
        workspace.as_str(),
        normalized_query.unwrap_or_default(),
        provider,
        position.as_str(),
    ] {
        hasher.update(value.len().to_be_bytes());
        hasher.update(value.as_bytes());
    }
    encode_digest(hasher.finalize())
}

fn encode_digest(digest: impl IntoIterator<Item = u8>) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        if let Some(high) = char::from_digit(u32::from(byte >> 4), 16) {
            encoded.push(high);
        }
        if let Some(low) = char::from_digit(u32::from(byte & 0x0f), 16) {
            encoded.push(low);
        }
    }
    encoded
}
