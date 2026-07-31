//! Stable, route-scoped native candidate deduplication.

use std::collections::HashMap;

use crate::search::result::{NativeSearchAttribute, NativeSearchResult};

use super::identity::NativeIdentity;
use super::{
    MAX_ATTRIBUTES, MAX_REQUEST_BYTES, MAX_RESULT_BYTES, MAX_RESULTS_PER_REQUEST, NativeCandidate,
    result_payload_bytes,
};

pub(super) fn deduplicate(mut candidates: Vec<NativeCandidate>) -> Vec<NativeCandidate> {
    candidates.sort_by_key(|candidate| (candidate.result.row_ordinal, candidate.identity));

    let mut retained = Vec::<NativeCandidate>::with_capacity(candidates.len());
    let mut identities = HashMap::<NativeIdentity, usize>::new();
    for candidate in candidates {
        let Some(identity) = candidate.identity else {
            retained.push(candidate);
            continue;
        };
        if let Some(index) = identities.get(&identity).copied() {
            if let Some(existing) = retained.get_mut(index)
                && candidate.result.row_ordinal > existing.result.row_ordinal
            {
                fill_missing_display(existing, candidate);
            }
        } else {
            identities.insert(identity, retained.len());
            retained.push(candidate);
        }
    }
    retained.sort_by_key(|candidate| (candidate.result.row_ordinal, candidate.identity));
    retained
}

pub(super) fn cap_request(candidates: Vec<NativeCandidate>) -> Vec<NativeCandidate> {
    let mut retained = Vec::with_capacity(candidates.len().min(MAX_RESULTS_PER_REQUEST));
    // Account for the enclosing public `results` array even when it is empty.
    let mut payload_bytes = 2_usize;
    for candidate in candidates {
        if retained.len() == MAX_RESULTS_PER_REQUEST {
            break;
        }
        let candidate_bytes = result_payload_bytes(&candidate.result);
        let separator_bytes = usize::from(!retained.is_empty());
        let next_payload_bytes = payload_bytes
            .saturating_add(separator_bytes)
            .saturating_add(candidate_bytes);
        if next_payload_bytes > MAX_REQUEST_BYTES {
            break;
        }
        payload_bytes = next_payload_bytes;
        retained.push(candidate);
    }
    retained
}

fn fill_missing_display(existing: &mut NativeCandidate, later: NativeCandidate) {
    let later_content_truncated = later.result.content_truncated;
    let later_omitted_attribute_count = later.result.omitted_attribute_count;
    let mut filled_from_later = false;
    filled_from_later |=
        fill_optional_field(&mut existing.result, later.result.entity_type, |result| {
            &mut result.entity_type
        });
    filled_from_later |=
        fill_optional_field(&mut existing.result, later.result.provider_id, |result| {
            &mut result.provider_id
        });
    filled_from_later |= fill_optional_field(&mut existing.result, later.result.title, |result| {
        &mut result.title
    });
    filled_from_later |= fill_optional_field(&mut existing.result, later.result.url, |result| {
        &mut result.url
    });
    filled_from_later |=
        fill_optional_field(&mut existing.result, later.result.snippet, |result| {
            &mut result.snippet
        });
    for attribute in later.result.attributes {
        if existing
            .result
            .attributes
            .iter()
            .any(|retained| retained.name == attribute.name)
        {
            continue;
        }
        if existing.result.attributes.len() == MAX_ATTRIBUTES
            || !attribute_fits(&existing.result, &attribute)
        {
            existing.result.omitted_attribute_count =
                existing.result.omitted_attribute_count.saturating_add(1);
            existing.result.content_truncated = true;
            continue;
        }
        existing.result.attributes.push(attribute);
        filled_from_later = true;
    }
    if filled_from_later {
        existing.result.content_truncated |= later_content_truncated;
        existing.result.omitted_attribute_count = existing
            .result
            .omitted_attribute_count
            .saturating_add(later_omitted_attribute_count);
    }
}

fn fill_optional_field(
    result: &mut NativeSearchResult,
    value: Option<String>,
    field: impl Fn(&mut NativeSearchResult) -> &mut Option<String>,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    if field(result).is_some() {
        return false;
    }
    *field(result) = Some(value);
    if result_payload_bytes(result) > MAX_RESULT_BYTES {
        *field(result) = None;
        result.content_truncated = true;
        false
    } else {
        true
    }
}

fn attribute_fits(result: &NativeSearchResult, attribute: &NativeSearchAttribute) -> bool {
    let mut trial = result.clone();
    trial.attributes.push(attribute.clone());
    result_payload_bytes(&trial) <= MAX_RESULT_BYTES
}
