//! Public Search-response credential scrubbing.
//!
//! Search results can expose observed values, so the response boundary applies
//! the same obvious-credential suppression used while collecting those values.
//! This is defense in depth, not PII redaction or a data-loss-prevention boundary.

use crate::search::observed::{
    DEFAULT_OBSERVED_VALUE_BYTES_LIMIT, is_sensitive_column, sanitize_observed_value,
};
use crate::search::result::{FieldValues, SearchResponse};

/// Scrubs credential-like matching-value evidence before it crosses the public boundary.
///
/// The caller must apply this once before mapping the domain response to its
/// public representation so the live and retained responses stay identical.
pub(crate) fn sanitize_search_response(response: &mut SearchResponse) {
    for result in &mut response.results {
        sanitize_matching_values(&mut result.matching_values);
    }
}

fn sanitize_matching_values(matching_values: &mut Vec<FieldValues>) {
    matching_values.retain_mut(|field_values| {
        if is_sensitive_column(&field_values.field) {
            return false;
        }

        let mut sanitized_values = Vec::with_capacity(field_values.values.len());
        for value in std::mem::take(&mut field_values.values) {
            let Some(value) = sanitize_observed_value(value, DEFAULT_OBSERVED_VALUE_BYTES_LIMIT)
            else {
                continue;
            };
            sanitized_values.push(value);
        }
        sanitized_values.sort();
        sanitized_values.dedup();
        field_values.values = sanitized_values;
        !field_values.values.is_empty()
    });
}

#[cfg(test)]
mod tests {
    use crate::search::response_safety::{sanitize_matching_values, sanitize_search_response};
    use crate::search::result::{
        CatalogSurface, Field, FieldValues, ProviderCoverage, ProviderStatus, SearchProviderKind,
        SearchProviderState, SearchResponse, SearchResult, SearchSurfaceId, SearchSurfaceKind,
        SearchTruncation, SurfaceShape,
    };

    #[test]
    fn removes_credential_fields_and_values() {
        let mut matching_values = vec![
            FieldValues {
                field: "api_token".to_string(),
                values: vec!["must-not-escape".to_string()],
            },
            FieldValues {
                field: "details".to_string(),
                values: vec![
                    r#"{"name":"Ada","password":"hidden"}"#.to_string(),
                    "plain-safe-value".to_string(),
                    "sk-12345678901234567890".to_string(),
                    "x".repeat(4 * 1024 + 1),
                ],
            },
        ];

        sanitize_matching_values(&mut matching_values);

        assert_eq!(
            matching_values,
            [FieldValues {
                field: "details".to_string(),
                values: vec![
                    "plain-safe-value".to_string(),
                    r#"{"name":"Ada"}"#.to_string()
                ],
            }]
        );
    }

    #[test]
    fn drops_empty_groups_and_deduplicates_values_created_by_scrubbing() {
        let mut matching_values = vec![
            FieldValues {
                field: "empty".to_string(),
                values: vec!["github_pat_1234567890".to_string()],
            },
            FieldValues {
                field: "callback".to_string(),
                values: vec![
                    "https://example.test/?z=last&token=first".to_string(),
                    "https://example.test/?name=Ada&token=first".to_string(),
                    "https://example.test/?name=Ada&token=second".to_string(),
                ],
            },
        ];

        sanitize_matching_values(&mut matching_values);

        assert_eq!(
            matching_values,
            [FieldValues {
                field: "callback".to_string(),
                values: vec![
                    "https://example.test/?name=Ada".to_string(),
                    "https://example.test/?z=last".to_string()
                ],
            }]
        );
    }

    #[test]
    fn public_response_scrubbing_preserves_non_value_debugging_context() {
        let mut response = SearchResponse {
            results: vec![SearchResult {
                surface: CatalogSurface {
                    id: SearchSurfaceId {
                        catalog_name: Some("catalog".to_string()),
                        schema_name: "schema".to_string(),
                        name: "search_function".to_string(),
                        kind: SearchSurfaceKind::TableFunction,
                    },
                    description: "may mention token handling".to_string(),
                    guide: "pass api_key as configured".to_string(),
                    shape: SurfaceShape::Function {
                        arguments: vec![Field {
                            name: "api_token".to_string(),
                            data_type: "TEXT".to_string(),
                            required: true,
                        }],
                        returns: vec![Field {
                            name: "session_count".to_string(),
                            data_type: "BIGINT".to_string(),
                            required: false,
                        }],
                    },
                },
                providers: vec![SearchProviderKind::ObservedValues],
                matching_values: vec![FieldValues {
                    field: "details".to_string(),
                    values: vec![r#"{"name":"Ada","password":"hidden"}"#.to_string()],
                }],
                omitted_matching_field_count: 7,
            }],
            provider_statuses: vec![ProviderStatus {
                provider: SearchProviderKind::ObservedValues,
                state: SearchProviderState::ResultsFound,
                note: "provider token note".to_string(),
                coverage: Some(ProviderCoverage::default()),
            }],
            truncation: SearchTruncation {
                truncated: true,
                returned_count: 1,
                max_results: 10,
                note: "result token note".to_string(),
            },
        };

        sanitize_search_response(&mut response);

        let result = response.results.first().expect("one result");
        assert_eq!(
            result.matching_values,
            [FieldValues {
                field: "details".to_string(),
                values: vec![r#"{"name":"Ada"}"#.to_string()],
            }]
        );
        assert_eq!(result.surface.description, "may mention token handling");
        assert_eq!(result.surface.guide, "pass api_key as configured");
        assert_eq!(result.omitted_matching_field_count, 7);
        assert_eq!(
            response
                .provider_statuses
                .first()
                .expect("one provider status")
                .note,
            "provider token note"
        );
        assert_eq!(response.truncation.note, "result token note");
        let SurfaceShape::Function { arguments, returns } = &result.surface.shape else {
            panic!("function shape is preserved");
        };
        assert_eq!(arguments.first().expect("one argument").name, "api_token");
        assert_eq!(returns.first().expect("one return").name, "session_count");
    }
}
