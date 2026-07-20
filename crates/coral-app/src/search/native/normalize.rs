//! Typed Arrow row normalization into the compact native result contract.

use std::collections::HashSet;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use coral_spec::ManifestDataType;

use crate::search::result::{NativeSearchAttribute, NativeSearchResult};
use crate::sources::universal_search::{
    ResolvedUniversalSearchResultField, ResolvedUniversalSearchRoute,
};
use crate::workspaces::WorkspaceName;

use super::identity::{identity_for_row, sort_key_for_row};
use super::rank::NativeRankInput;
use super::redaction::{
    ATTRIBUTE_VALUE_BYTES, OverflowPolicy, PROVIDER_ID_BYTES, SNIPPET_BYTES, Sanitized,
    TITLE_BYTES, sanitize_attribute_name, sanitize_canonical_json, sanitize_text, sanitize_url,
};
use super::{
    MAX_ATTRIBUTES, MAX_RESULT_BYTES, MAX_RESULTS_PER_FUNCTION, NativeCandidate,
    result_payload_bytes,
};

const PROVIDER_ID_NAMES: &[&str] = &["id", "node_id", "global_id"];
const TITLE_NAMES: &[&str] = &["title", "name"];
const URL_NAMES: &[&str] = &["url", "html_url", "web_url"];
const SNIPPET_NAMES: &[&str] = &["snippet", "summary", "description"];

pub(super) fn normalize_batches(
    workspace: &WorkspaceName,
    route: &ResolvedUniversalSearchRoute,
    batches: &[RecordBatch],
) -> Vec<NativeCandidate> {
    let mut candidates = Vec::new();
    let mut row_ordinal = 0_usize;
    for batch in batches {
        for row in 0..batch.num_rows() {
            if row_ordinal == MAX_RESULTS_PER_FUNCTION {
                return candidates;
            }
            let ordinal = u32::try_from(row_ordinal).unwrap_or(u32::MAX);
            if let Some(candidate) = normalize_row(workspace, route, batch, row, ordinal) {
                candidates.push(candidate);
            }
            row_ordinal = row_ordinal.saturating_add(1);
        }
    }
    candidates
}

fn normalize_row(
    workspace: &WorkspaceName,
    route: &ResolvedUniversalSearchRoute,
    batch: &RecordBatch,
    row: usize,
    row_ordinal: u32,
) -> Option<NativeCandidate> {
    let mapped = route.result.authored_mapping;
    let provider = mapped_value(
        batch,
        row,
        mapped
            .then_some(route.result.provider_id.as_ref())
            .flatten(),
        (!mapped).then_some(PROVIDER_ID_NAMES),
    );
    let title = mapped_value(
        batch,
        row,
        mapped.then_some(route.result.title.as_ref()).flatten(),
        (!mapped).then_some(TITLE_NAMES),
    );
    let url = mapped_value(
        batch,
        row,
        mapped.then_some(route.result.url.as_ref()).flatten(),
        (!mapped).then_some(URL_NAMES),
    );
    let snippet = mapped_value(
        batch,
        row,
        mapped.then_some(route.result.snippet.as_ref()).flatten(),
        (!mapped).then_some(SNIPPET_NAMES),
    );

    let (provider_id, provider_limited) =
        sanitize_optional_text(provider.as_ref(), PROVIDER_ID_BYTES, OverflowPolicy::Omit);
    let (title, title_limited) =
        sanitize_optional_text(title.as_ref(), TITLE_BYTES, OverflowPolicy::Truncate);
    let (url, url_limited) = sanitize_optional_url(url.as_ref());
    let (snippet, snippet_limited) =
        sanitize_optional_text(snippet.as_ref(), SNIPPET_BYTES, OverflowPolicy::Truncate);
    let content_truncated = provider_limited || title_limited || url_limited || snippet_limited;

    let identity = identity_for_row(
        workspace,
        route,
        batch,
        row,
        provider_id.as_deref(),
        url.as_deref(),
    );
    let entity_type = route.result.entity_type.as_deref().and_then(|value| {
        match sanitize_text("entity_type", value, usize::MAX, OverflowPolicy::Omit) {
            Sanitized::Safe(value) => Some(value),
            Sanitized::SizeLimited(_) | Sanitized::Rejected => None,
        }
    });
    let mut result = NativeSearchResult {
        schema_name: route.locator.schema_name.clone(),
        function_name: route.locator.function_name.clone(),
        row_ordinal,
        entity_type,
        provider_id,
        title,
        url,
        snippet,
        attributes: Vec::new(),
        omitted_attribute_count: 0,
        content_truncated,
    };

    if mapped {
        append_attributes(&mut result, batch, row, &route.result.attributes);
    }
    enforce_result_budget(&mut result)?;

    let sort_key = sort_key_for_row(workspace, route, row_ordinal, identity);
    has_row_display_content(&result).then_some(NativeCandidate {
        result,
        identity,
        sort_key,
        rank_input: NativeRankInput::from_provider_ordinal(row_ordinal),
    })
}

fn mapped_value(
    batch: &RecordBatch,
    row: usize,
    explicit: Option<&ResolvedUniversalSearchResultField>,
    inference_names: Option<&[&str]>,
) -> Option<MappedValue> {
    if let Some(field) = explicit {
        let column = unique_column(batch, &field.column_name)?;
        if !display_type_matches(column.data_type(), field.data_type) {
            return None;
        }
        return raw_scalar(column.as_ref(), row).map(|value| MappedValue {
            field_name: field.column_name.clone(),
            value,
        });
    }
    inference_names?.iter().find_map(|name| {
        let column = unique_column(batch, name)?;
        raw_scalar(column.as_ref(), row).map(|value| MappedValue {
            field_name: (*name).to_string(),
            value,
        })
    })
}

fn append_attributes(
    result: &mut NativeSearchResult,
    batch: &RecordBatch,
    row: usize,
    fields: &[ResolvedUniversalSearchResultField],
) {
    let mut names = HashSet::new();
    for field in fields {
        let name = match sanitize_attribute_name(&field.column_name) {
            Sanitized::Safe(name) => name,
            Sanitized::SizeLimited(_) => {
                omit_safe_attribute(result);
                continue;
            }
            Sanitized::Rejected => continue,
        };
        let Some(column) = unique_column(batch, &field.column_name) else {
            continue;
        };
        if !display_type_matches(column.data_type(), field.data_type) || column.is_null(row) {
            continue;
        }
        let sanitized = if field.data_type == ManifestDataType::Json {
            raw_string(column.as_ref(), row).map_or(Sanitized::Rejected, |value| {
                sanitize_canonical_json(&field.column_name, value)
            })
        } else {
            raw_scalar(column.as_ref(), row).map_or(Sanitized::Rejected, |value| {
                sanitize_text(
                    &field.column_name,
                    &value,
                    ATTRIBUTE_VALUE_BYTES,
                    OverflowPolicy::Truncate,
                )
            })
        };
        let display_value = match sanitized {
            Sanitized::Safe(value) => value,
            Sanitized::SizeLimited(Some(value)) => {
                result.content_truncated = true;
                value
            }
            Sanitized::SizeLimited(None) => {
                omit_safe_attribute(result);
                continue;
            }
            Sanitized::Rejected => continue,
        };
        if !names.insert(name.clone()) || result.attributes.len() == MAX_ATTRIBUTES {
            omit_safe_attribute(result);
            continue;
        }
        let attribute = NativeSearchAttribute {
            name,
            display_value,
        };
        let mut trial = result.clone();
        trial.attributes.push(attribute.clone());
        if result_payload_bytes(&trial) > MAX_RESULT_BYTES {
            omit_safe_attribute(result);
        } else {
            result.attributes.push(attribute);
        }
    }
}

fn enforce_result_budget(result: &mut NativeSearchResult) -> Option<()> {
    if result_payload_bytes(result) <= MAX_RESULT_BYTES {
        return Some(());
    }
    for field_index in 0..5 {
        let removed = match field_index {
            0 => result.entity_type.take(),
            1 => result.snippet.take(),
            2 => result.url.take(),
            3 => result.title.take(),
            4 => result.provider_id.take(),
            _ => None,
        };
        if removed.is_some() {
            result.content_truncated = true;
        }
        if result_payload_bytes(result) <= MAX_RESULT_BYTES {
            return Some(());
        }
    }
    None
}

fn has_row_display_content(result: &NativeSearchResult) -> bool {
    result.provider_id.is_some()
        || result.title.is_some()
        || result.url.is_some()
        || result.snippet.is_some()
        || !result.attributes.is_empty()
}

fn sanitize_optional_text(
    value: Option<&MappedValue>,
    max_bytes: usize,
    overflow: OverflowPolicy,
) -> (Option<String>, bool) {
    let Some(value) = value else {
        return (None, false);
    };
    match sanitize_text(&value.field_name, &value.value, max_bytes, overflow) {
        Sanitized::Safe(value) => (Some(value), false),
        Sanitized::SizeLimited(value) => (value, true),
        Sanitized::Rejected => (None, false),
    }
}

fn sanitize_optional_url(value: Option<&MappedValue>) -> (Option<String>, bool) {
    let Some(value) = value else {
        return (None, false);
    };
    match sanitize_url(&value.field_name, &value.value) {
        Sanitized::Safe(value) => (Some(value), false),
        Sanitized::SizeLimited(value) => (value, true),
        Sanitized::Rejected => (None, false),
    }
}

fn omit_safe_attribute(result: &mut NativeSearchResult) {
    result.omitted_attribute_count = result.omitted_attribute_count.saturating_add(1);
    result.content_truncated = true;
}

fn unique_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a arrow::array::ArrayRef> {
    let schema = batch.schema();
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == name)
        .map(|(index, _)| index);
    let index = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    batch.columns().get(index)
}

fn display_type_matches(data_type: &DataType, expected: ManifestDataType) -> bool {
    match expected {
        ManifestDataType::Utf8 | ManifestDataType::Json => matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        ManifestDataType::Int64 => matches!(data_type, DataType::Int64),
        ManifestDataType::Boolean => matches!(data_type, DataType::Boolean),
        ManifestDataType::Float64 => matches!(data_type, DataType::Float64),
        ManifestDataType::Timestamp => matches!(data_type, DataType::Timestamp(_, _)),
    }
}

fn raw_scalar(column: &dyn Array, row: usize) -> Option<String> {
    if row >= column.len() || column.is_null(row) || is_unsupported_display_type(column.data_type())
    {
        return None;
    }
    raw_string(column, row)
        .map(str::to_string)
        .or_else(|| array_value_to_string(column, row).ok())
}

fn raw_string(column: &dyn Array, row: usize) -> Option<&str> {
    match column.data_type() {
        DataType::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(row)),
        DataType::LargeUtf8 => column
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| array.value(row)),
        DataType::Utf8View => column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|array| array.value(row)),
        _ => None,
    }
}

fn is_unsupported_display_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

struct MappedValue {
    field_name: String,
    value: String,
}
