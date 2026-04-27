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
use serde_json::{Map, Value};

use crate::{
    ColumnSpec, FilterSpec, HeaderSpec, ManifestError, ManifestInputKind, ManifestInputSpec,
    PaginationSpec, ParsedTemplate, RequestRouteSpec, RequestSpec, ResponseSpec, Result,
    SourceBackend, SourceManifestCommon, TableCommon, inputs::collect_source_inputs_value,
    validate::validate_template, validate_http_table, validate_test_queries,
};

/// Source-level authentication requirements for HTTP-backed source specs.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum AuthSpec {
    /// HTTP Basic authentication; runtime base64-encodes `username:password`.
    #[serde(rename = "BasicAuth")]
    BasicAuth(BasicAuthSpec),
    /// Declarative list of auth headers to attach to the request.
    #[serde(rename = "HeaderAuth")]
    HeaderAuth(HeaderAuthSpec),
    /// Dispatches auth header resolution to a runtime-registered authenticator.
    #[serde(rename = "CustomAuth")]
    CustomAuth(CustomAuthSpec),
}

impl Default for AuthSpec {
    fn default() -> Self {
        Self::HeaderAuth(HeaderAuthSpec::default())
    }
}

/// HTTP Basic authenticator with separate username and password templates.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuthSpec {
    pub username: ParsedTemplate,
    pub password: ParsedTemplate,
}

/// Declarative authenticator that injects one or more headers.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HeaderAuthSpec {
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
}

/// Dispatches to a runtime-registered request authenticator by name.
#[derive(Debug, Clone, Deserialize)]
pub struct CustomAuthSpec {
    pub authenticator: String,
    #[serde(flatten)]
    pub config: Map<String, Value>,
}

/// Provider-specific response hints for classifying and delaying rate-limit retries.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RateLimitSpec {
    #[serde(default)]
    pub extra_statuses: Vec<u16>,
    #[serde(default)]
    pub retry_after_header: Option<String>,
    #[serde(default)]
    pub remaining_header: Option<String>,
    #[serde(default)]
    pub reset_header: Option<String>,
}

/// Validated top-level manifest for an HTTP-backed source.
#[derive(Debug, Clone)]
pub struct HttpSourceManifest {
    pub common: SourceManifestCommon,
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub request_headers: Vec<HeaderSpec>,
    pub rate_limit: RateLimitSpec,
    pub tables: Vec<HttpTableSpec>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    backend: SourceBackend,
    #[serde(default)]
    base_url: ParsedTemplate,
    #[serde(default)]
    auth: AuthSpec,
    #[serde(default)]
    request_headers: Vec<HeaderSpec>,
    #[serde(default)]
    rate_limit: RateLimitSpec,
    #[serde(default)]
    inputs: Option<Value>,
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
    /// Returns the source secrets required by this manifest.
    ///
    /// In the new input model, every declared input with `kind: secret` is
    /// required because secrets cannot carry defaults.
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        self.declared_inputs
            .iter()
            .filter(|input| input.kind == ManifestInputKind::Secret)
            .map(|input| input.key.clone())
            .collect()
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
        let declared_inputs = collect_source_inputs_value(&value)?;
        let raw: RawHttpSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawHttpSourceManifest {
            dsl_version,
            name,
            version,
            description,
            test_queries,
            backend: _backend,
            base_url,
            auth,
            request_headers,
            rate_limit,
            inputs: _inputs,
            tables,
        } = raw;
        validate_test_queries(&name, &test_queries)?;
        let common =
            SourceManifestCommon::new(dsl_version, name, version, description, test_queries);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;
        if base_url.raw().trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{}' must define a non-empty base_url",
                common.name
            )));
        }
        validate_template(
            &base_url,
            &HashSet::new(),
            &format!("source '{}'", common.name),
        )?;

        Ok(Self {
            common,
            base_url,
            auth,
            request_headers,
            rate_limit,
            tables,
            declared_inputs,
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
