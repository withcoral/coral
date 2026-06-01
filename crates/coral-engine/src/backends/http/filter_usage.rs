//! Helpers for proving which SQL filters an HTTP request template consumes.

use std::collections::HashSet;

use coral_spec::{
    BodySpec, HeaderSpec, ParsedTemplate, RequestSpec, TemplateNamespace, ValueSourceSpec,
};

#[derive(Clone)]
pub(crate) struct HttpRequestFilterUsage {
    base_url: ParsedTemplate,
    source_headers: Vec<HeaderSpec>,
}

impl HttpRequestFilterUsage {
    pub(crate) fn new(base_url: ParsedTemplate, source_headers: Vec<HeaderSpec>) -> Self {
        Self {
            base_url,
            source_headers,
        }
    }

    pub(crate) fn request_filter_names(&self, request: &RequestSpec) -> HashSet<String> {
        http_request_filter_names(&self.base_url, &self.source_headers, request)
    }
}

pub(crate) fn http_request_filter_names(
    base_url: &ParsedTemplate,
    source_headers: &[HeaderSpec],
    request: &RequestSpec,
) -> HashSet<String> {
    let mut filters = HashSet::new();

    collect_source_filters(base_url, source_headers, &mut filters);
    collect_request_filters(request, &mut filters);

    filters
}

fn collect_source_filters(
    base_url: &ParsedTemplate,
    source_headers: &[HeaderSpec],
    filters: &mut HashSet<String>,
) {
    collect_template_filters(base_url, filters);
    for header in source_headers {
        collect_value_source_filters(&header.value, filters);
    }
}

fn collect_request_filters(request: &RequestSpec, filters: &mut HashSet<String>) {
    collect_template_filters(&request.path, filters);
    for param in &request.query {
        collect_value_source_filters(&param.value, filters);
    }
    for header in &request.headers {
        collect_value_source_filters(&header.value, filters);
    }
    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                collect_value_source_filters(&field.value, filters);
            }
        }
        BodySpec::Text { content } => collect_value_source_filters(content, filters),
    }
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
        ValueSourceSpec::OneOf { values } => {
            for value in values {
                collect_value_source_filters(value, filters);
            }
        }
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
        | ValueSourceSpec::ArgSplit { .. }
        | ValueSourceSpec::ArgSplitInt { .. }
        | ValueSourceSpec::Input { .. }
        | ValueSourceSpec::Bearer { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use coral_spec::{
        BodyFieldSpec, BodySpec, HeaderSpec, ParsedTemplate, QueryParamSpec, RequestSpec,
        ValueSourceSpec,
    };
    use serde_json::json;

    use super::http_request_filter_names;

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
                    when_arg: None,
                    value: ValueSourceSpec::FilterSplit {
                        key: "labels".to_string(),
                        separator: ",".to_string(),
                        part: 0,
                    },
                }],
            },
            ..RequestSpec::default()
        };

        let base_url = ParsedTemplate::parse("https://example.com").expect("base url");
        let filters = http_request_filter_names(&base_url, &[], &request);

        assert!(filters.contains("owner"));
        assert!(filters.contains("repo"));
        assert!(filters.contains("state"));
        assert!(filters.contains("labels"));
        assert_eq!(filters.len(), 4);
    }

    #[test]
    fn finds_filters_consumed_by_source_level_render_sites() {
        let base_url =
            ParsedTemplate::parse("https://{{filter.region}}.example.com").expect("base url");
        let source_headers = vec![
            HeaderSpec {
                name: "X-Tenant".to_string(),
                value: ValueSourceSpec::Filter {
                    key: "tenant".to_string(),
                    default: None,
                },
            },
            HeaderSpec {
                name: "X-Route".to_string(),
                value: ValueSourceSpec::Template {
                    template: ParsedTemplate::parse("route-{{filter.route}}")
                        .expect("header template"),
                },
            },
        ];
        let request = RequestSpec {
            path: ParsedTemplate::parse("/items/{{filter.item}}").expect("path template"),
            ..RequestSpec::default()
        };

        let filters = http_request_filter_names(&base_url, &source_headers, &request);

        assert!(filters.contains("region"));
        assert!(filters.contains("tenant"));
        assert!(filters.contains("route"));
        assert!(filters.contains("item"));
        assert_eq!(filters.len(), 4);
    }
}
