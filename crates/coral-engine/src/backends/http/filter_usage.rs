//! Helpers for proving which SQL filters an HTTP request template consumes.

use std::collections::HashSet;

use coral_spec::{BodySpec, ParsedTemplate, RequestSpec, TemplateNamespace, ValueSourceSpec};

pub(crate) fn request_filter_names(request: &RequestSpec) -> HashSet<String> {
    let mut filters = HashSet::new();

    collect_template_filters(&request.path, &mut filters);
    for param in &request.query {
        collect_value_source_filters(&param.value, &mut filters);
    }
    for header in &request.headers {
        collect_value_source_filters(&header.value, &mut filters);
    }
    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                collect_value_source_filters(&field.value, &mut filters);
            }
        }
        BodySpec::Text { content } => collect_value_source_filters(content, &mut filters),
    }

    filters
}

fn collect_template_filters(template: &ParsedTemplate, filters: &mut HashSet<String>) {
    for token in template.tokens() {
        if matches!(token.namespace(), TemplateNamespace::Filter) {
            filters.insert(token.key().to_string());
        }
    }
}

fn collect_value_source_filters(source: &ValueSourceSpec, filters: &mut HashSet<String>) {
    match source {
        ValueSourceSpec::Template { template } => collect_template_filters(template, filters),
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => {
            filters.insert(key.clone());
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Arg { .. }
        | ValueSourceSpec::ArgInt { .. }
        | ValueSourceSpec::ArgBool { .. }
        | ValueSourceSpec::Input { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use coral_spec::{
        BodyFieldSpec, BodySpec, ParsedTemplate, QueryParamSpec, RequestSpec, ValueSourceSpec,
    };
    use serde_json::json;

    use super::request_filter_names;

    #[test]
    fn finds_filters_consumed_by_request_templates_and_fields() {
        let request = RequestSpec {
            path: ParsedTemplate::parse("/repos/{{filter.owner}}/{{filter.repo}}")
                .expect("path template"),
            query: vec![QueryParamSpec {
                name: "state".to_string(),
                value: ValueSourceSpec::Filter {
                    key: "state".to_string(),
                    default: Some(json!("all")),
                },
            }],
            body: BodySpec::Json {
                fields: vec![BodyFieldSpec {
                    path: vec!["labels".to_string()],
                    value: ValueSourceSpec::FilterSplit {
                        key: "labels".to_string(),
                        separator: ",".to_string(),
                        part: 0,
                    },
                }],
            },
            ..RequestSpec::default()
        };

        let filters = request_filter_names(&request);

        assert!(filters.contains("owner"));
        assert!(filters.contains("repo"));
        assert!(filters.contains("state"));
        assert!(filters.contains("labels"));
        assert_eq!(filters.len(), 4);
    }
}
