#![allow(
    missing_docs,
    reason = "This module defines many field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for HTTP sources.
//!
//! HTTP manifests describe request templating, response-row extraction, filter
//! binding, and pagination. These types are normalized and validated here, but
//! they are still engine-neutral; no runtime HTTP client or execution concerns
//! live in this crate.

use std::collections::{BTreeSet, HashSet};

use serde::Deserialize;
use serde_json::Value;

use crate::common::build_source_manifest_common;
use crate::{
    AuthSpec, ColumnSpec, FilterSpec, ManifestError, PaginationSpec, ParsedTemplate,
    RequestRouteSpec, RequestSpec, ResponseSpec, Result, SourceBackend, SourceManifestCommon,
    TableCommon, TemplateNamespace, ValueSourceSpec, validate_http_table,
    validate_manifest_top_level,
};

/// Validated top-level manifest for an HTTP-backed source.
#[derive(Debug, Clone)]
pub struct HttpSourceManifest {
    pub common: SourceManifestCommon,
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub tables: Vec<HttpTableSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: Option<String>,
    backend: SourceBackend,
    #[serde(default)]
    base_url: ParsedTemplate,
    #[serde(default)]
    auth: AuthSpec,
    tables: Vec<RawHttpTableSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpTableSpec {
    name: String,
    description: String,
    #[serde(default)]
    guide: String,
    #[serde(default)]
    filters: Vec<FilterSpec>,
    #[serde(default)]
    fetch_limit_default: Option<usize>,
    #[serde(default)]
    request: RequestSpec,
    #[serde(default)]
    requests: Vec<RequestRouteSpec>,
    #[serde(default)]
    response: ResponseSpec,
    #[serde(default)]
    pagination: PaginationSpec,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
}

/// One validated HTTP table declaration.
#[derive(Debug, Clone)]
pub struct HttpTableSpec {
    pub common: TableCommon,
    pub request: RequestSpec,
    pub requests: Vec<RequestRouteSpec>,
    pub response: ResponseSpec,
    pub pagination: PaginationSpec,
}

impl HttpTableSpec {
    #[must_use]
    /// Returns the stable table name.
    pub fn name(&self) -> &str {
        &self.common.name
    }

    #[must_use]
    /// Returns the declared SQL filters that may influence request selection.
    pub fn filters(&self) -> &[FilterSpec] {
        &self.common.filters
    }

    #[must_use]
    /// Returns the declared output columns for this table.
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.common.columns
    }

    #[must_use]
    /// Returns the default fetch limit declared by the manifest, if any.
    pub fn fetch_limit_default(&self) -> Option<usize> {
        self.common.fetch_limit_default
    }

    #[must_use]
    /// Selects the most specific request route that matches the provided
    /// filter set, or falls back to the default request.
    pub fn resolve_request(&self, provided_filters: &HashSet<String>) -> &RequestSpec {
        let mut best_match: Option<&RequestRouteSpec> = None;
        let mut best_specificity = 0usize;

        for route in &self.requests {
            if route
                .when_filters
                .iter()
                .all(|f| provided_filters.contains(f))
            {
                let specificity = route.when_filters.len();
                if best_match.is_none() || specificity > best_specificity {
                    best_match = Some(route);
                    best_specificity = specificity;
                }
            }
        }

        best_match.map_or(&self.request, |route| &route.request)
    }
}

impl HttpSourceManifest {
    /// Returns the source secrets required by auth headers and request
    /// templates after defaults are taken into account.
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        let mut secret_names = self
            .auth
            .required_secrets
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for header in &self.auth.headers {
            collect_value_source_secret_names(&header.value, &mut secret_names);
        }
        for table in &self.tables {
            collect_request_secret_names(&table.request, &mut secret_names);
            for route in &table.requests {
                collect_route_secret_names(route, &mut secret_names);
            }
        }
        secret_names
    }
}

impl RawHttpTableSpec {
    fn into_validated(self, schema: &str) -> Result<HttpTableSpec> {
        validate_http_table(
            schema,
            &self.name,
            &self.filters,
            &self.columns,
            &self.request,
            &self.requests,
            &self.pagination,
        )?;

        Ok(HttpTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                self.filters,
                self.fetch_limit_default,
                self.columns,
            ),
            request: self.request,
            requests: self.requests,
            response: self.response,
            pagination: self.pagination,
        })
    }
}

impl HttpSourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw: RawHttpSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawHttpSourceManifest {
            dsl_version,
            name,
            version,
            description: _description,
            backend: _backend,
            base_url,
            auth,
            tables,
        } = raw;
        let common = build_source_manifest_common(dsl_version, name, version);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;
        validate_manifest_top_level(
            &common.name,
            &common.name,
            SourceBackend::Http,
            &base_url,
        )?;
        Ok(Self {
            common,
            base_url,
            auth,
            tables,
        })
    }
}

#[cfg(test)]
pub(crate) fn test_http_table_spec(
    name: &str,
    columns: Vec<ColumnSpec>,
    filters: Vec<FilterSpec>,
    request: RequestSpec,
) -> HttpTableSpec {
    HttpTableSpec {
        common: TableCommon::new(
            name.to_string(),
            "test".to_string(),
            String::new(),
            filters,
            None,
            columns,
        ),
        request,
        requests: vec![],
        response: ResponseSpec::default(),
        pagination: PaginationSpec::default(),
    }
}

fn collect_route_secret_names(route: &RequestRouteSpec, secret_names: &mut BTreeSet<String>) {
    collect_request_secret_names(&route.request, secret_names);
}

fn collect_request_secret_names(request: &RequestSpec, secret_names: &mut BTreeSet<String>) {
    for query in &request.query {
        collect_value_source_secret_names(&query.value, secret_names);
    }
    for body in &request.body {
        collect_value_source_secret_names(&body.value, secret_names);
    }
    for header in &request.headers {
        collect_value_source_secret_names(&header.value, secret_names);
    }
}

fn collect_value_source_secret_names(
    value_source: &ValueSourceSpec,
    secret_names: &mut BTreeSet<String>,
) {
    match value_source {
        ValueSourceSpec::Secret { key, default } => {
            if default.is_none() {
                secret_names.insert(key.clone());
            }
        }
        ValueSourceSpec::Template { template } => {
            collect_template_secret_names(template, secret_names);
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Filter { .. }
        | ValueSourceSpec::Variable { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => {}
    }
}

fn collect_template_secret_names(template: &ParsedTemplate, secret_names: &mut BTreeSet<String>) {
    for token in template.tokens() {
        if token.namespace() == &TemplateNamespace::Secret && token.default_value().is_none() {
            secret_names.insert(token.key().to_string());
        }
    }
}
