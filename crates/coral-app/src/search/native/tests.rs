#![expect(
    clippy::indexing_slicing,
    reason = "focused native fixtures assert result shape immediately before indexed access"
)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_spec::{DO_NOT_INDEX_COLUMN_METADATA_KEY, ManifestDataType, SearchLimitsSpec};
use uuid::Uuid;

use super::dedupe::{cap_request, deduplicate};
use super::normalize::normalize_batches;
use super::rank::NativeRankInput;
use super::{
    MAX_REQUEST_BYTES, MAX_RESULT_BYTES, MAX_RESULTS_PER_FUNCTION, MAX_RESULTS_PER_REQUEST,
    NativeCandidate, PublicSearchResultEnvelope, result_payload_bytes,
};
use crate::search::result::{NativeSearchAttribute, NativeSearchResult};
use crate::sources::runtime_package::RuntimeContractFingerprint;
use crate::sources::universal_search::{
    ResolvedUniversalSearchArgument, ResolvedUniversalSearchResultField,
    ResolvedUniversalSearchResultMapping, ResolvedUniversalSearchRoute,
    ResolvedUniversalSearchTarget, UniversalSearchFunctionLocator, UniversalSearchResolutionOrigin,
};
use crate::workspaces::WorkspaceName;

fn resolved_field(name: &str, data_type: ManifestDataType) -> ResolvedUniversalSearchResultField {
    ResolvedUniversalSearchResultField {
        column_name: name.to_string(),
        data_type,
    }
}

fn route(mapping: ResolvedUniversalSearchResultMapping) -> ResolvedUniversalSearchRoute {
    ResolvedUniversalSearchRoute {
        owner_source_name: "github".to_string(),
        installation_revision: Uuid::from_u128(1),
        authored_route_id: Some("issues".to_string()),
        target: ResolvedUniversalSearchTarget {
            operation_id: "search_issues".to_string(),
        },
        locator: UniversalSearchFunctionLocator {
            schema_name: "github".to_string(),
            function_name: "search_issues".to_string(),
        },
        query_argument: ResolvedUniversalSearchArgument {
            name: "query".to_string(),
            data_type: ManifestDataType::Utf8,
        },
        default_arguments: Vec::new(),
        search_limits: SearchLimitsSpec {
            default_top_k: 5,
            max_top_k: 5,
            max_calls_per_query: 1,
        },
        result: mapping,
        origin: UniversalSearchResolutionOrigin::Explicit,
        runtime_contract_fingerprint: RuntimeContractFingerprint::for_test("v1:test"),
    }
}

fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("valid native test batch")
}

fn workspace() -> WorkspaceName {
    WorkspaceName::default()
}

#[test]
fn explicit_mapping_wins_and_attributes_are_never_inferred() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        entity_type: Some("issue".to_string()),
        identity_fields: vec![resolved_field("node", ManifestDataType::Utf8)],
        provider_id: Some(resolved_field("node", ManifestDataType::Utf8)),
        title: Some(resolved_field("headline", ManifestDataType::Utf8)),
        url: Some(resolved_field("link", ManifestDataType::Utf8)),
        snippet: None,
        attributes: vec![resolved_field("state", ManifestDataType::Utf8)],
    };
    let batch = batch(vec![
        ("node", string_column(vec![Some("N1")])),
        ("headline", string_column(vec![Some("Mapped title")])),
        ("title", string_column(vec![Some("Inferred title")])),
        (
            "link",
            string_column(vec![Some("https://example.test/issues/1")]),
        ),
        ("state", string_column(vec![Some("open")])),
        ("arbitrary", string_column(vec![Some("must not appear")])),
    ]);

    let candidates = normalize_batches(&workspace(), &route(mapping), &[batch]);
    assert_eq!(candidates.len(), 1);
    let result = &candidates[0].result;
    assert_eq!(result.provider_id.as_deref(), Some("N1"));
    assert_eq!(result.title.as_deref(), Some("Mapped title"));
    assert_eq!(result.entity_type.as_deref(), Some("issue"));
    assert_eq!(
        result.attributes,
        vec![NativeSearchAttribute {
            name: "state".to_string(),
            display_value: "open".to_string(),
        }]
    );
}

#[test]
fn conservative_inference_uses_only_locked_names_in_priority_order() {
    let batch = batch(vec![
        ("node_id", string_column(vec![Some("node-1")])),
        ("global_id", string_column(vec![Some("global-1")])),
        ("name", string_column(vec![Some("Ada")])),
        (
            "html_url",
            string_column(vec![Some("https://example.test/ada")]),
        ),
        ("description", string_column(vec![Some("Researcher")])),
        ("status", string_column(vec![Some("active")])),
    ]);
    let candidates = normalize_batches(
        &workspace(),
        &route(ResolvedUniversalSearchResultMapping::default()),
        &[batch],
    );
    let result = &candidates[0].result;
    assert_eq!(result.provider_id.as_deref(), Some("node-1"));
    assert_eq!(result.title.as_deref(), Some("Ada"));
    assert_eq!(result.url.as_deref(), Some("https://example.test/ada"));
    assert_eq!(result.snippet.as_deref(), Some("Researcher"));
    assert!(result.attributes.is_empty());
}

#[test]
fn direct_display_does_not_overload_do_not_index_metadata() {
    let field = Field::new("title", DataType::Utf8, false).with_metadata(HashMap::from([(
        DO_NOT_INDEX_COLUMN_METADATA_KEY.to_string(),
        "true".to_string(),
    )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![string_column(vec![Some("Still displayable")])],
    )
    .expect("metadata batch");

    let candidates = normalize_batches(
        &workspace(),
        &route(ResolvedUniversalSearchResultMapping::default()),
        &[batch],
    );
    assert_eq!(
        candidates[0].result.title.as_deref(),
        Some("Still displayable")
    );
}

#[test]
fn source_authored_entity_type_uses_the_display_safety_boundary() {
    let mapping = ResolvedUniversalSearchResultMapping {
        entity_type: Some("sk-12345678901234567890".to_string()),
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let batch = batch(vec![("title", string_column(vec![Some("Safe title")]))]);
    let candidates = normalize_batches(&workspace(), &route(mapping), &[batch]);

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].result.entity_type, None);
}

#[test]
fn secrets_binary_and_invalid_urls_are_dropped_while_explicit_json_is_canonical() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        title: Some(resolved_field("api_token", ManifestDataType::Utf8)),
        url: Some(resolved_field("link", ManifestDataType::Utf8)),
        attributes: vec![
            resolved_field("binary", ManifestDataType::Utf8),
            resolved_field("metadata", ManifestDataType::Json),
        ],
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let batch = batch(vec![
        ("api_token", string_column(vec![Some("visible-looking")])),
        ("link", string_column(vec![Some("javascript:alert(1)")])),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![b"bytes".as_slice()])) as ArrayRef,
        ),
        (
            "metadata",
            string_column(vec![Some(
                r#"{"z":2,"password":"hidden","a":{"y":2,"x":1}}"#,
            )]),
        ),
    ]);
    let candidates = normalize_batches(&workspace(), &route(mapping), &[batch]);
    assert_eq!(candidates.len(), 1);
    let result = &candidates[0].result;
    assert_eq!(result.title, None);
    assert_eq!(result.url, None);
    assert_eq!(
        result.attributes,
        vec![NativeSearchAttribute {
            name: "metadata".to_string(),
            display_value: r#"{"a":{"x":1,"y":2},"z":2}"#.to_string(),
        }]
    );
    assert_eq!(result.omitted_attribute_count, 0);
    assert!(!result.content_truncated);
}

#[test]
fn redaction_does_not_count_but_safe_attribute_omissions_and_truncation_do() {
    let oversized_name = "n".repeat(129);
    let mut attributes = vec![
        resolved_field("api_key", ManifestDataType::Utf8),
        resolved_field("\0", ManifestDataType::Utf8),
        resolved_field("a\0b", ManifestDataType::Utf8),
        resolved_field("ab", ManifestDataType::Utf8),
        resolved_field(&oversized_name, ManifestDataType::Utf8),
        resolved_field("long", ManifestDataType::Utf8),
    ];
    attributes.extend(
        (0..9).map(|index| resolved_field(&format!("safe_{index}"), ManifestDataType::Utf8)),
    );
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        attributes,
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let mut columns = vec![
        ("api_key", string_column(vec![Some("hidden")])),
        ("\0", string_column(vec![Some("safe but nameless")])),
        ("a\0b", string_column(vec![Some("first")])),
        ("ab", string_column(vec![Some("second")])),
        (oversized_name.as_str(), string_column(vec![Some("safe")])),
        ("long", string_column(vec![Some(&"é".repeat(513))])),
    ];
    let safe_names = (0..9)
        .map(|index| format!("safe_{index}"))
        .collect::<Vec<_>>();
    for name in &safe_names {
        columns.push((name.as_str(), string_column(vec![Some("value")])));
    }
    let batch = batch(columns);

    let candidates = normalize_batches(&workspace(), &route(mapping), &[batch]);
    let result = &candidates[0].result;
    assert_eq!(result.attributes[0].name, "ab");
    assert_eq!(result.attributes[0].display_value, "first");
    assert!(
        result
            .attributes
            .iter()
            .all(|attribute| attribute.name != "api_key")
    );
    assert_eq!(result.attributes.len(), 8);
    assert_eq!(result.omitted_attribute_count, 5);
    assert!(result.content_truncated);
    assert!(
        result
            .attributes
            .iter()
            .find(|attribute| attribute.name == "long")
            .is_some_and(|attribute| attribute.display_value.len() == 1_024)
    );
}

#[test]
fn utf8_field_limits_and_total_result_budget_are_exact_and_safe() {
    let mut attributes = Vec::new();
    let mut columns = vec![
        ("id", string_column(vec![Some(&"i".repeat(513))])),
        ("title", string_column(vec![Some(&"é".repeat(257))])),
        ("snippet", string_column(vec![Some(&"s".repeat(1_025))])),
    ];
    let attribute_names = (0..8)
        .map(|index| format!("attr_{index}"))
        .collect::<Vec<_>>();
    for name in &attribute_names {
        attributes.push(resolved_field(name, ManifestDataType::Utf8));
        columns.push((name.as_str(), string_column(vec![Some(&"x".repeat(1_024))])));
    }
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        provider_id: Some(resolved_field("id", ManifestDataType::Utf8)),
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        snippet: Some(resolved_field("snippet", ManifestDataType::Utf8)),
        attributes,
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let candidates = normalize_batches(&workspace(), &route(mapping), &[batch(columns)]);
    let result = &candidates[0].result;
    assert_eq!(result.provider_id, None);
    assert_eq!(result.title.as_ref().map(String::len), Some(512));
    assert_eq!(result.snippet.as_ref().map(String::len), Some(1_024));
    assert!(
        result
            .title
            .as_deref()
            .is_some_and(|title| title.is_char_boundary(title.len()))
    );
    assert!(result_payload_bytes(result) <= MAX_RESULT_BYTES);
    assert!(result.omitted_attribute_count > 0);
    assert!(result.content_truncated);
}

#[test]
fn payload_accounting_matches_public_json_with_escaping_and_utf8() {
    let result = NativeSearchResult {
        schema_name: "sch\"\\\u{0001}éma".to_string(),
        function_name: "fun\n\\\"雪".to_string(),
        row_ordinal: u32::MAX,
        entity_type: Some("entity\t😀".to_string()),
        provider_id: Some("id\"\\\u{0008}é".to_string()),
        title: Some("title\r\n\"\\東京".to_string()),
        url: Some("https://example.test/a?quoted=\"&slash=\\".to_string()),
        snippet: Some("snippet\u{000c}\"\\🪸".to_string()),
        attributes: vec![NativeSearchAttribute {
            name: "na\"\\\u{0002}me".to_string(),
            display_value: "va\"\\\u{0003}lueé".to_string(),
        }],
        omitted_attribute_count: u32::MAX,
        content_truncated: true,
    };
    let public_json = serde_json::json!({
        "provider": "native_fanout",
        "kind": "native_result",
        "native_result": {
            "schema_name": &result.schema_name,
            "function_name": &result.function_name,
            "row_ordinal": result.row_ordinal,
            "entity_type": result.entity_type.as_deref(),
            "provider_id": result.provider_id.as_deref(),
            "title": result.title.as_deref(),
            "url": result.url.as_deref(),
            "snippet": result.snippet.as_deref(),
            "attributes": [{
                "name": &result.attributes[0].name,
                "display_value": &result.attributes[0].display_value,
            }],
            "omitted_attribute_count": result.omitted_attribute_count,
            "content_truncated": result.content_truncated,
        },
    });
    let serialized = serde_json::to_vec(&public_json).expect("public JSON serializes");

    assert_eq!(result_payload_bytes(&result), serialized.len());
    let serialized = String::from_utf8(serialized).expect("JSON is UTF-8");
    assert!(serialized.contains(r#"\""#));
    assert!(serialized.contains(r"\\"));
    assert!(serialized.contains(r"\u0001"));
    assert!(serialized.contains('é'));
    assert!(serialized.contains('😀'));
}

#[test]
fn escaped_attribute_values_stay_within_the_public_json_result_budget() {
    let attribute_names = (0..8)
        .map(|index| format!("attr_{index}"))
        .collect::<Vec<_>>();
    let mut mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        ..ResolvedUniversalSearchResultMapping::default()
    };
    mapping.attributes.extend(
        attribute_names
            .iter()
            .map(|name| resolved_field(name, ManifestDataType::Utf8)),
    );
    let escaped = "\"\\".repeat(512);
    let mut columns = vec![("title", string_column(vec![Some(&"é".repeat(256))]))];
    for name in &attribute_names {
        columns.push((name.as_str(), string_column(vec![Some(&escaped)])));
    }

    let normalized = normalize_batches(&workspace(), &route(mapping), &[batch(columns)]);
    let result = &normalized[0].result;
    assert!(result_payload_bytes(result) <= MAX_RESULT_BYTES);
    assert!(result.omitted_attribute_count > 0);
    assert!(result.content_truncated);
}

#[test]
fn per_function_and_request_result_caps_preserve_provider_order() {
    let ids = (0..6)
        .map(|index| Some(format!("id-{index}")))
        .collect::<Vec<_>>();
    let id_refs = ids.iter().map(|value| value.as_deref()).collect::<Vec<_>>();
    let batch = batch(vec![("id", string_column(id_refs))]);
    let route = route(ResolvedUniversalSearchResultMapping::default());
    let normalized = normalize_batches(&workspace(), &route, &[batch]);
    assert_eq!(normalized.len(), MAX_RESULTS_PER_FUNCTION);
    assert_eq!(normalized[4].result.row_ordinal, 4);

    let many = (0..25)
        .map(|ordinal| candidate(ordinal, Some(format!("id-{ordinal}")), None))
        .collect::<Vec<_>>();
    let capped = cap_request(many);
    assert_eq!(capped.len(), MAX_RESULTS_PER_REQUEST);
    assert_eq!(capped[19].result.row_ordinal, 19);
    let accounted_bytes = 2_usize
        .saturating_add(capped.len().saturating_sub(1))
        .saturating_add(
            capped
                .iter()
                .map(|candidate| result_payload_bytes(&candidate.result))
                .sum::<usize>(),
        );
    let public_results = capped
        .iter()
        .map(|candidate| PublicSearchResultEnvelope::from(&candidate.result))
        .collect::<Vec<_>>();
    let serialized_bytes =
        serde_json::to_vec(&public_results).expect("public native result array serializes");
    assert_eq!(accounted_bytes, serialized_bytes.len());
    assert!(serialized_bytes.len() <= MAX_REQUEST_BYTES);
}

#[test]
fn identity_is_stable_across_display_changes_and_absent_for_title_only_rows() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        identity_fields: vec![resolved_field("id", ManifestDataType::Int64)],
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let identity_route = route(mapping);
    let first = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        ("title", string_column(vec![Some("First title")])),
    ]);
    let second = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        ("title", string_column(vec![Some("Changed title")])),
    ]);
    let first = normalize_batches(&workspace(), &identity_route, &[first]);
    let second = normalize_batches(&workspace(), &identity_route, &[second]);
    assert_eq!(first[0].identity, second[0].identity);

    let title_only = batch(vec![("title", string_column(vec![Some("Only title")]))]);
    let title_only = normalize_batches(
        &workspace(),
        &route(ResolvedUniversalSearchResultMapping::default()),
        &[title_only],
    );
    assert_eq!(title_only.len(), 1);
    assert_eq!(title_only[0].identity, None);
}

#[test]
fn invalid_authored_identity_falls_back_to_safe_provider_id_then_url() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        identity_fields: vec![resolved_field("score", ManifestDataType::Float64)],
        provider_id: Some(resolved_field("id", ManifestDataType::Utf8)),
        url: Some(resolved_field("url", ManifestDataType::Utf8)),
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let route = route(mapping);
    let with_fallback = batch(vec![
        ("score", Arc::new(Float64Array::from(vec![1.0])) as ArrayRef),
        ("id", string_column(vec![Some("provider-1")])),
        (
            "url",
            string_column(vec![Some("https://example.test/1?token=hidden&safe=1")]),
        ),
        ("title", string_column(vec![Some("Title")])),
    ]);
    let normalized = normalize_batches(&workspace(), &route, &[with_fallback]);
    assert!(normalized[0].identity.is_some());
    assert_eq!(
        normalized[0].result.url.as_deref(),
        Some("https://example.test/1?safe=1")
    );

    let no_fallback = batch(vec![
        ("score", Arc::new(Float64Array::from(vec![1.0])) as ArrayRef),
        ("id", string_column(vec![None])),
        ("url", string_column(vec![None])),
        ("title", string_column(vec![Some("Title")])),
    ]);
    let normalized = normalize_batches(&workspace(), &route, &[no_fallback]);
    assert_eq!(normalized[0].identity, None);
}

#[test]
fn dedupe_keeps_lowest_ordinal_fills_missing_fields_and_never_crosses_scope() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        identity_fields: vec![resolved_field("id", ManifestDataType::Utf8)],
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        snippet: Some(resolved_field("snippet", ManifestDataType::Utf8)),
        attributes: vec![resolved_field("state", ManifestDataType::Utf8)],
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let primary_route = route(mapping.clone());
    let duplicate_rows = batch(vec![
        ("id", string_column(vec![Some("same"), Some("same")])),
        ("title", string_column(vec![Some("First"), Some("Later")])),
        (
            "snippet",
            string_column(vec![None, Some(&"s".repeat(1_025))]),
        ),
        ("state", string_column(vec![None, Some("open")])),
    ]);
    let normalized = normalize_batches(&workspace(), &primary_route, &[duplicate_rows]);
    let deduped = deduplicate(normalized.into_iter().rev().collect());
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].result.row_ordinal, 0);
    assert_eq!(deduped[0].result.title.as_deref(), Some("First"));
    assert_eq!(
        deduped[0].result.snippet.as_ref().map(String::len),
        Some(1_024)
    );
    assert!(deduped[0].result.content_truncated);
    assert_eq!(
        deduped[0].result.attributes,
        vec![NativeSearchAttribute {
            name: "state".to_string(),
            display_value: "open".to_string(),
        }]
    );

    let one_row = batch(vec![
        ("id", string_column(vec![Some("same")])),
        ("title", string_column(vec![Some("Title")])),
        ("snippet", string_column(vec![None])),
        ("state", string_column(vec![None])),
    ]);
    let mut other_route = route(mapping.clone());
    other_route.authored_route_id = Some("pull_requests".to_string());
    let mut other_source = route(mapping);
    other_source.owner_source_name = "gitlab".to_string();
    let mut other_installation = primary_route.clone();
    other_installation.installation_revision = Uuid::from_u128(2);
    let candidates =
        normalize_batches(&workspace(), &primary_route, std::slice::from_ref(&one_row))
            .into_iter()
            .chain(normalize_batches(
                &workspace(),
                &other_route,
                std::slice::from_ref(&one_row),
            ))
            .chain(normalize_batches(
                &workspace(),
                &other_source,
                std::slice::from_ref(&one_row),
            ))
            .chain(normalize_batches(
                &workspace(),
                &other_installation,
                &[one_row],
            ))
            .collect();
    assert_eq!(deduplicate(candidates).len(), 4);
}

#[test]
fn dedupe_orders_unsorted_identities_and_fills_from_the_nearest_higher_row() {
    let mapping = ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        identity_fields: vec![resolved_field("id", ManifestDataType::Utf8)],
        title: Some(resolved_field("title", ManifestDataType::Utf8)),
        snippet: Some(resolved_field("snippet", ManifestDataType::Utf8)),
        ..ResolvedUniversalSearchResultMapping::default()
    };
    let rows = batch(vec![
        (
            "id",
            string_column(vec![Some("a"), Some("b"), Some("a"), Some("c")]),
        ),
        (
            "title",
            string_column(vec![Some("A first"), Some("B"), Some("A later"), Some("C")]),
        ),
        (
            "snippet",
            string_column(vec![None, None, Some("filled from row two"), None]),
        ),
    ]);
    let mut normalized = normalize_batches(&workspace(), &route(mapping), &[rows]);
    normalized.reverse();

    let deduped = deduplicate(normalized);
    assert_eq!(
        deduped
            .iter()
            .map(|candidate| candidate.result.row_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 3]
    );
    assert_eq!(deduped[0].result.title.as_deref(), Some("A first"));
    assert_eq!(
        deduped[0].result.snippet.as_deref(),
        Some("filled from row two")
    );
}

#[test]
fn rank_input_is_exact_provider_ordinal_and_upstream_score_is_ignored() {
    let batch = batch(vec![
        ("title", string_column(vec![Some("First"), Some("Second")])),
        (
            "score",
            Arc::new(Float64Array::from(vec![0.01, 999.0])) as ArrayRef,
        ),
    ]);
    let candidates = normalize_batches(
        &workspace(),
        &route(ResolvedUniversalSearchResultMapping::default()),
        &[batch],
    );
    assert_eq!(candidates[0].rank_input.provider_ordinal(), 0);
    assert_eq!(candidates[1].rank_input.provider_ordinal(), 1);
    assert_eq!(candidates[0].result.title.as_deref(), Some("First"));
    assert_eq!(candidates[1].result.title.as_deref(), Some("Second"));
}

fn candidate(
    row_ordinal: u32,
    provider_id: Option<String>,
    title: Option<String>,
) -> NativeCandidate {
    NativeCandidate {
        result: NativeSearchResult {
            schema_name: "github".to_string(),
            function_name: "search".to_string(),
            row_ordinal,
            entity_type: None,
            provider_id,
            title,
            url: None,
            snippet: None,
            attributes: Vec::new(),
            omitted_attribute_count: 0,
            content_truncated: false,
        },
        identity: None,
        rank_input: NativeRankInput::from_provider_ordinal(row_ordinal),
    }
}
