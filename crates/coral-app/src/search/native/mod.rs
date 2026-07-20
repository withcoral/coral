//! Pure native-row security, identity, deduplication, and rank-input boundary.
mod dedupe;
mod diagnostics;
mod identity;
mod normalize;
pub(crate) mod provider;
mod rank;
mod redaction;

use serde::Serialize;

use crate::search::result::{NativeSearchAttribute, NativeSearchResult};

pub(super) const MAX_RESULTS_PER_FUNCTION: usize = 5;
pub(super) const MAX_RESULTS_PER_REQUEST: usize = 20;
pub(super) const MAX_RESULT_BYTES: usize = 8 * 1_024;
pub(super) const MAX_REQUEST_BYTES: usize = 256 * 1_024;
pub(super) const MAX_ATTRIBUTES: usize = 8;

#[derive(Debug, Clone)]
pub(super) struct NativeCandidate {
    pub(super) result: NativeSearchResult,
    pub(super) identity: Option<identity::NativeIdentity>,
    pub(super) sort_key: identity::NativeSortKey,
    pub(super) rank_input: rank::NativeRankInput,
}

/// Conservative accounting for one public native `SearchResult` JSON envelope.
///
/// Serializing the locked public shape counts JSON escaping, field names, and
/// collection/object framing. Serialization failure returns the largest value
/// so budget enforcement fails closed.
pub(super) fn result_payload_bytes(result: &NativeSearchResult) -> usize {
    serde_json::to_vec(&PublicSearchResultEnvelope::from(result))
        .map_or(usize::MAX, |payload| payload.len())
}

#[derive(Serialize)]
struct PublicSearchResultEnvelope<'a> {
    provider: &'static str,
    kind: &'static str,
    native_result: PublicNativeSearchResult<'a>,
}

impl<'a> From<&'a NativeSearchResult> for PublicSearchResultEnvelope<'a> {
    fn from(result: &'a NativeSearchResult) -> Self {
        Self {
            provider: "native_fanout",
            kind: "native_result",
            native_result: PublicNativeSearchResult::from(result),
        }
    }
}

#[derive(Serialize)]
struct PublicNativeSearchResult<'a> {
    schema_name: &'a str,
    function_name: &'a str,
    row_ordinal: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snippet: Option<&'a str>,
    attributes: Vec<PublicNativeSearchAttribute<'a>>,
    omitted_attribute_count: u32,
    content_truncated: bool,
}

impl<'a> From<&'a NativeSearchResult> for PublicNativeSearchResult<'a> {
    fn from(result: &'a NativeSearchResult) -> Self {
        Self {
            schema_name: &result.schema_name,
            function_name: &result.function_name,
            row_ordinal: result.row_ordinal,
            entity_type: result.entity_type.as_deref(),
            provider_id: result.provider_id.as_deref(),
            title: result.title.as_deref(),
            url: result.url.as_deref(),
            snippet: result.snippet.as_deref(),
            attributes: result
                .attributes
                .iter()
                .map(PublicNativeSearchAttribute::from)
                .collect(),
            omitted_attribute_count: result.omitted_attribute_count,
            content_truncated: result.content_truncated,
        }
    }
}

#[derive(Serialize)]
struct PublicNativeSearchAttribute<'a> {
    name: &'a str,
    display_value: &'a str,
}

impl<'a> From<&'a NativeSearchAttribute> for PublicNativeSearchAttribute<'a> {
    fn from(attribute: &'a NativeSearchAttribute) -> Self {
        Self {
            name: &attribute.name,
            display_value: &attribute.display_value,
        }
    }
}

#[cfg(test)]
mod tests;
