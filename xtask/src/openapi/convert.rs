//! Conversion from a validated v3 HTTP source manifest to OpenAPI 3.0.
//!
//! [`convert_http_manifest`] turns one manifest into a self-contained
//! OpenAPI 3.0.3 document: every table, request route, and table function
//! becomes one operation, filters and function arguments become typed
//! parameters, declared pagination becomes the request parameters and
//! response fields the API actually exposes, and declared columns are
//! folded back into response schemas (see `schema_tree`).
//!
//! The emitted document is aimed at the DSL v4 OpenAPI importer: pagination
//! knowledge is encoded as ordinary OpenAPI facts (parameters, response
//! headers, cursor properties) that the importer's detection already
//! understands. Anything the OpenAPI vocabulary cannot express — SQL filter
//! bindings, runtime-computed values, row strategies — is preserved under
//! `x-coral*` extensions instead of being dropped, and every lossy or
//! ambiguous mapping is reported as a warning.

use std::collections::{HashMap, HashSet};

use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec, RateLimitSpec};
use coral_spec::{
    AuthSpec, BodySpec, ColumnSpec, DetailHintSpec, FilterMode, FilterSpec, HttpMethod,
    ManifestDataType, PaginationMode, PaginationSpec, RequestSpec, ResponseBodyFormat,
    ResponseSpec, RowStrategy, SearchLimitsSpec, SourceTableFunctionKind, SourceTableFunctionSpec,
    TableFunctionArgSpec, TemplateNamespace, TemplatePart, TemplateToken, ValueSourceSpec,
};
use serde_json::{Map, Value, json};

use super::schema_tree::{SchemaTree, row_schema_tree, scalar_schema};

/// One converted manifest: the OpenAPI document plus human-readable
/// warnings for every construct that could not be mapped faithfully.
pub(crate) struct ConvertedDocument {
    pub(crate) document: Value,
    pub(crate) warnings: Vec<String>,
}

/// Convert one validated v3 HTTP source manifest into an OpenAPI 3.0.3
/// document. Infallible by design: unmappable constructs degrade to
/// warnings plus `x-coral*` extensions rather than errors.
pub(crate) fn convert_http_manifest(manifest: &HttpSourceManifest) -> ConvertedDocument {
    Converter {
        manifest,
        warnings: Vec::new(),
    }
    .convert()
}

/// Returns true when every request in the manifest targets a GraphQL
/// endpoint (`/graphql`, `/api/graphql`, `/graphql.json`, ...). OpenAPI
/// keys operations by path and method, so a GraphQL source would collapse
/// into a single meaningless operation; callers should skip these instead
/// of converting them.
pub(crate) fn is_graphql_source(manifest: &HttpSourceManifest) -> bool {
    let mut paths = manifest
        .tables
        .iter()
        .flat_map(|table| {
            std::iter::once(&table.request)
                .chain(table.requests.iter().map(|route| &route.request))
        })
        .chain(manifest.functions.iter().map(|function| &function.request))
        .map(|request| request.path.raw())
        .filter(|path| !path.is_empty())
        .peekable();
    paths.peek().is_some() && paths.all(|path| path.to_ascii_lowercase().contains("graphql"))
}

struct Converter<'a> {
    manifest: &'a HttpSourceManifest,
    warnings: Vec<String>,
}

/// One table or function viewed uniformly by the operation builder.
struct Surface<'a> {
    kind: &'static str,
    name: &'a str,
    description: &'a str,
    guide: &'a str,
    filters: &'a [FilterSpec],
    args: &'a [TableFunctionArgSpec],
    response: &'a ResponseSpec,
    pagination: &'a PaginationSpec,
    columns: &'a [ColumnSpec],
    function_kind: Option<SourceTableFunctionKind>,
    search_limits: Option<&'a SearchLimitsSpec>,
    detail_hints: &'a [DetailHintSpec],
    fetch_limit_default: Option<usize>,
}

impl<'a> Surface<'a> {
    fn from_table(table: &'a HttpTableSpec) -> Self {
        Self {
            kind: "table",
            name: &table.common.name,
            description: &table.common.description,
            guide: &table.common.guide,
            filters: &table.common.filters,
            args: &[],
            response: &table.response,
            pagination: &table.pagination,
            columns: &table.common.columns,
            function_kind: None,
            search_limits: table.common.search_limits.as_ref(),
            detail_hints: &table.common.detail_hints,
            fetch_limit_default: table.common.fetch_limit_default,
        }
    }

    fn from_function(function: &'a SourceTableFunctionSpec) -> Self {
        Self {
            kind: "function",
            name: &function.name,
            description: &function.description,
            guide: "",
            filters: &[],
            args: &function.args,
            response: &function.response,
            pagination: &function.pagination,
            columns: &function.columns,
            function_kind: Some(function.kind),
            search_limits: function.search_limits.as_ref(),
            detail_hints: &function.detail_hints,
            fetch_limit_default: function.fetch_limit_default,
        }
    }

    fn label(&self) -> String {
        format!("{} '{}'", self.kind, self.name)
    }

    fn filter(&self, key: &str) -> Option<&'a FilterSpec> {
        self.filters.iter().find(|filter| filter.name == key)
    }

    /// Resolve a request-side arg key (`from: arg, key: X`) to the declared
    /// SQL-facing argument bound to it.
    fn arg(&self, key: &str) -> Option<&'a TableFunctionArgSpec> {
        self.args.iter().find(|arg| arg.bind.arg == key)
    }
}

/// A parameter or body-field description derived from one v3 value source.
struct ValueSourceDoc {
    schema: Value,
    required: bool,
    description: String,
    extensions: Vec<(&'static str, Value)>,
}

/// Everything the declared pagination contributes to one operation.
#[derive(Default)]
struct PaginationArtifacts {
    parameters: Vec<Value>,
    response_headers: Vec<(String, Value)>,
    response_cursor_path: Vec<String>,
    body_properties: Vec<(Vec<String>, Value)>,
}

/// Document-level accumulators threaded through operation building.
#[derive(Default)]
struct DocumentParts {
    paths: Map<String, Value>,
    /// Which operation claimed each `(path, method)` slot, for collision
    /// warnings.
    slot_owners: HashMap<(String, &'static str), String>,
    schemas: Map<String, Value>,
    operation_ids: HashSet<String>,
}

impl Converter<'_> {
    fn convert(mut self) -> ConvertedDocument {
        let mut parts = DocumentParts::default();

        for table in &self.manifest.tables {
            let surface = Surface::from_table(table);
            let row_ref = register_row_schema(&surface, &mut parts.schemas);
            self.add_operation(
                &mut parts,
                &surface,
                surface.name,
                &table.request,
                &[],
                row_ref.as_ref(),
            );
            for route in &table.requests {
                let operation_id = format!("{}_by_{}", surface.name, route.when_filters.join("_"));
                self.add_operation(
                    &mut parts,
                    &surface,
                    &operation_id,
                    &route.request,
                    &route.when_filters,
                    row_ref.as_ref(),
                );
            }
        }
        for function in &self.manifest.functions {
            let surface = Surface::from_function(function);
            let row_ref = register_row_schema(&surface, &mut parts.schemas);
            self.add_operation(
                &mut parts,
                &surface,
                surface.name,
                &function.request,
                &[],
                row_ref.as_ref(),
            );
        }

        let servers = self.servers_value();
        let (security_schemes, security) = self.security_values();

        let mut document = Map::new();
        document.insert("openapi".to_string(), json!("3.0.3"));
        document.insert(
            "info".to_string(),
            json!({
                "title": self.manifest.common.name,
                "version": self.manifest.common.version,
                "description": self.manifest.common.description,
            }),
        );
        document.insert("servers".to_string(), servers);
        if !security.is_empty() {
            document.insert("security".to_string(), Value::Array(security));
        }
        document.insert("paths".to_string(), Value::Object(parts.paths));
        let mut components = Map::new();
        if !parts.schemas.is_empty() {
            components.insert("schemas".to_string(), Value::Object(parts.schemas));
        }
        if !security_schemes.is_empty() {
            components.insert(
                "securitySchemes".to_string(),
                Value::Object(security_schemes),
            );
        }
        if !components.is_empty() {
            document.insert("components".to_string(), Value::Object(components));
        }
        document.insert("x-coral".to_string(), self.document_extension());

        ConvertedDocument {
            document: Value::Object(document),
            warnings: self.warnings,
        }
    }

    fn warn(&mut self, surface: &Surface<'_>, message: &str) {
        self.warnings
            .push(format!("{}: {message}", surface.label()));
    }

    // ------------------------------------------------------------------
    // Operations
    // ------------------------------------------------------------------

    fn add_operation(
        &mut self,
        parts: &mut DocumentParts,
        surface: &Surface<'_>,
        operation_id: &str,
        request: &RequestSpec,
        when_filters: &[String],
        row_ref: Option<&Value>,
    ) {
        let method = method_key(request.method);
        let (path, mut parameters, mut seen) = self.convert_path(surface, request);

        let slot = (path.clone(), method);
        if let Some(owner) = parts.slot_owners.get(&slot) {
            self.warn(
                surface,
                &format!(
                    "operation '{operation_id}' ({} {path}) skipped: slot already \
                     claimed by operation '{owner}'",
                    method.to_uppercase()
                ),
            );
            return;
        }

        for query in &request.query {
            self.push_parameter(
                &mut parameters,
                &mut seen,
                surface,
                &query.name,
                "query",
                &query.value,
                when_filters,
            );
        }
        for header in &request.headers {
            self.push_parameter(
                &mut parameters,
                &mut seen,
                surface,
                &header.name,
                "header",
                &header.value,
                when_filters,
            );
        }

        let artifacts = self.pagination_artifacts(surface, &seen);
        parameters.extend(artifacts.parameters.iter().cloned());

        let request_body = self.request_body_value(surface, request, when_filters, &artifacts);
        if request_body.is_some() && request.method == HttpMethod::GET {
            self.warn(
                surface,
                &format!(
                    "operation '{operation_id}' sends a request body with GET; \
                     OpenAPI consumers may ignore it"
                ),
            );
        }
        let responses = self.responses_value(surface, row_ref, &artifacts);

        let mut unique_id = operation_id.to_string();
        let mut suffix = 2;
        while !parts.operation_ids.insert(unique_id.clone()) {
            unique_id = format!("{operation_id}_{suffix}");
            suffix += 1;
        }
        if unique_id != operation_id {
            self.warn(
                surface,
                &format!("operation id '{operation_id}' already taken; renamed to '{unique_id}'"),
            );
        }

        let mut operation = Map::new();
        operation.insert("operationId".to_string(), json!(unique_id));
        let description = if surface.guide.is_empty() {
            surface.description.to_string()
        } else {
            format!("{}\n\n{}", surface.description, surface.guide)
        };
        if !description.is_empty() {
            operation.insert("description".to_string(), json!(description));
        }
        if !parameters.is_empty() {
            operation.insert("parameters".to_string(), Value::Array(parameters));
        }
        if let Some(body) = request_body {
            operation.insert("requestBody".to_string(), body);
        }
        operation.insert("responses".to_string(), responses);
        operation.insert(
            "x-coral".to_string(),
            operation_extension(surface, when_filters),
        );

        parts.slot_owners.insert(slot, unique_id);
        let path_item = parts
            .paths
            .entry(path)
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(path_item) = path_item.as_object_mut() {
            path_item.insert(method.to_string(), Value::Object(operation));
        }
    }

    /// Convert the v3 path template into an OpenAPI path string plus the
    /// path parameters implied by its tokens. Returns the parameter list and
    /// the `(location, name)` pairs already used, so later query/header
    /// parameters can be deduplicated against it.
    fn convert_path(
        &mut self,
        surface: &Surface<'_>,
        request: &RequestSpec,
    ) -> (String, Vec<Value>, HashSet<(String, String)>) {
        let mut path = String::new();
        let mut parameters = Vec::new();
        let mut seen = HashSet::new();
        for part in request.path.parts() {
            match part {
                TemplatePart::Literal(literal) => path.push_str(literal),
                TemplatePart::Token(token) => {
                    let name = sanitize_token(token.key());
                    path.push('{');
                    path.push_str(&name);
                    path.push('}');
                    if !seen.insert(("path".to_string(), name.clone())) {
                        continue;
                    }
                    let doc = self.path_token_doc(surface, token);
                    parameters.push(parameter_value(&name, "path", true, doc));
                }
            }
        }
        (path, parameters, seen)
    }

    fn path_token_doc(&mut self, surface: &Surface<'_>, token: &TemplateToken) -> ValueSourceDoc {
        let default = token.default_value().map(|value| json!(value));
        match token.namespace() {
            TemplateNamespace::Filter => self.filter_doc(FilterDocRequest {
                surface,
                key: token.key(),
                when_filters: &[],
                schema_override: None,
                default,
                note: None,
            }),
            TemplateNamespace::Arg => self.arg_doc(surface, token.key(), None, default, None),
            TemplateNamespace::Input => self.input_doc(token.key()),
            TemplateNamespace::State => state_doc(token.key()),
            TemplateNamespace::Expr | TemplateNamespace::Other(_) => {
                self.warn(
                    surface,
                    &format!(
                        "path template token '{{{{{}}}}}' has an unsupported namespace; \
                         emitted as an untyped path parameter",
                        token.raw()
                    ),
                );
                ValueSourceDoc {
                    schema: json!({"type": "string"}),
                    required: true,
                    description: format!("Rendered from the v3 template token `{}`.", token.raw()),
                    extensions: Vec::new(),
                }
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "One call site per parameter location; grouping would only rename the arguments."
    )]
    fn push_parameter(
        &mut self,
        parameters: &mut Vec<Value>,
        seen: &mut HashSet<(String, String)>,
        surface: &Surface<'_>,
        wire_name: &str,
        location: &str,
        source: &ValueSourceSpec,
        when_filters: &[String],
    ) {
        if !seen.insert((location.to_string(), wire_name.to_string())) {
            self.warn(
                surface,
                &format!("duplicate {location} parameter '{wire_name}' dropped"),
            );
            return;
        }
        let doc = self.value_source_doc(surface, source, when_filters);
        let required = doc.required;
        parameters.push(parameter_value(wire_name, location, required, doc));
    }

    // ------------------------------------------------------------------
    // Value sources
    // ------------------------------------------------------------------

    #[expect(
        clippy::too_many_lines,
        reason = "One exhaustive match mapping every value-source variant; splitting it \
                  would smear the variant table across helpers."
    )]
    fn value_source_doc(
        &mut self,
        surface: &Surface<'_>,
        source: &ValueSourceSpec,
        when_filters: &[String],
    ) -> ValueSourceDoc {
        match source {
            ValueSourceSpec::Filter { key, default } => self.filter_doc(FilterDocRequest {
                surface,
                key,
                when_filters,
                schema_override: None,
                default: default.clone(),
                note: None,
            }),
            ValueSourceSpec::FilterInt { key, default } => self.filter_doc(FilterDocRequest {
                surface,
                key,
                when_filters,
                schema_override: Some(json!({"type": "integer"})),
                default: default.map(|value| json!(value)),
                note: None,
            }),
            ValueSourceSpec::FilterBool { key, default } => self.filter_doc(FilterDocRequest {
                surface,
                key,
                when_filters,
                schema_override: Some(json!({"type": "boolean"})),
                default: default.map(|value| json!(value)),
                note: None,
            }),
            ValueSourceSpec::FilterStringArray { key, default } => {
                self.filter_doc(FilterDocRequest {
                    surface,
                    key,
                    when_filters,
                    schema_override: Some(json!({"type": "array", "items": {"type": "string"}})),
                    default: default.clone().map(|values| json!(values)),
                    note: None,
                })
            }
            ValueSourceSpec::FilterSplit {
                key,
                separator,
                part,
            } => self.filter_doc(FilterDocRequest {
                surface,
                key,
                when_filters,
                schema_override: Some(json!({"type": "string"})),
                default: None,
                note: Some(split_note(key, separator, *part)),
            }),
            ValueSourceSpec::FilterSplitInt {
                key,
                separator,
                part,
            } => self.filter_doc(FilterDocRequest {
                surface,
                key,
                when_filters,
                schema_override: Some(json!({"type": "integer"})),
                default: None,
                note: Some(split_note(key, separator, *part)),
            }),
            ValueSourceSpec::Arg { key, default } => {
                self.arg_doc(surface, key, None, default.clone(), None)
            }
            ValueSourceSpec::ArgInt { key, default } => self.arg_doc(
                surface,
                key,
                Some(json!({"type": "integer"})),
                default.map(|value| json!(value)),
                None,
            ),
            ValueSourceSpec::ArgBool { key, default } => self.arg_doc(
                surface,
                key,
                Some(json!({"type": "boolean"})),
                default.map(|value| json!(value)),
                None,
            ),
            ValueSourceSpec::ArgSplit {
                key,
                separator,
                part,
            } => self.arg_doc(
                surface,
                key,
                Some(json!({"type": "string"})),
                None,
                Some(split_note(key, separator, *part)),
            ),
            ValueSourceSpec::ArgSplitInt {
                key,
                separator,
                part,
            } => self.arg_doc(
                surface,
                key,
                Some(json!({"type": "integer"})),
                None,
                Some(split_note(key, separator, *part)),
            ),
            ValueSourceSpec::Literal { value } => ValueSourceDoc {
                schema: literal_schema(value),
                required: false,
                description: "Fixed value always sent with the request.".to_string(),
                extensions: vec![("x-coral-constant", json!(true))],
            },
            ValueSourceSpec::Template { template } => ValueSourceDoc {
                schema: json!({"type": "string"}),
                required: false,
                description: format!("Rendered from the v3 template `{}`.", template.raw()),
                extensions: vec![("x-coral-template", json!(template.raw()))],
            },
            ValueSourceSpec::OneOf { .. } => ValueSourceDoc {
                schema: json!({}),
                required: false,
                description: "Resolved from the first available of several value sources."
                    .to_string(),
                extensions: vec![(
                    "x-coral-value-source",
                    serde_json::to_value(source).unwrap_or(Value::Null),
                )],
            },
            ValueSourceSpec::Input { key } | ValueSourceSpec::Bearer { key } => self.input_doc(key),
            ValueSourceSpec::State { key } => state_doc(key),
            ValueSourceSpec::NowEpochMinusSeconds { seconds } => ValueSourceDoc {
                schema: json!({"type": "integer"}),
                required: false,
                description: format!(
                    "Computed at request time as the Unix epoch timestamp {seconds} \
                     seconds before now."
                ),
                extensions: vec![("x-coral-computed", json!("now_epoch_minus_seconds"))],
            },
        }
    }

    fn filter_doc(&mut self, request: FilterDocRequest<'_, '_>) -> ValueSourceDoc {
        let FilterDocRequest {
            surface,
            key,
            when_filters,
            schema_override,
            default,
            note,
        } = request;
        let spec = surface.filter(key);
        if spec.is_none() {
            self.warn(
                surface,
                &format!("request binds undeclared filter '{key}'; emitted as a string parameter"),
            );
        }
        let mut schema = schema_override.unwrap_or_else(|| {
            scalar_schema(spec.map_or(ManifestDataType::Utf8, |spec| spec.data_type))
        });
        if let (Some(default), Some(object)) = (default, schema.as_object_mut()) {
            object.insert("default".to_string(), default);
        }
        let mut description = spec.map_or(String::new(), |spec| spec.description.clone());
        match spec.map(|spec| spec.mode) {
            Some(FilterMode::Contains) => {
                append_sentence(&mut description, "Matched as a substring.");
            }
            Some(FilterMode::Search) => {
                append_sentence(&mut description, "Free-text search query.");
            }
            Some(FilterMode::Equality) | None => {}
        }
        if let Some(note) = note {
            append_sentence(&mut description, &note);
        }
        ValueSourceDoc {
            schema,
            required: spec.is_some_and(|spec| spec.required)
                || when_filters.iter().any(|filter| filter == key),
            description,
            extensions: vec![("x-coral-filter", json!(key))],
        }
    }

    fn arg_doc(
        &mut self,
        surface: &Surface<'_>,
        key: &str,
        schema_override: Option<Value>,
        default: Option<Value>,
        note: Option<String>,
    ) -> ValueSourceDoc {
        let spec = surface.arg(key);
        if spec.is_none() {
            self.warn(
                surface,
                &format!(
                    "request binds undeclared argument '{key}'; emitted as a string parameter"
                ),
            );
        }
        let mut schema = schema_override.unwrap_or_else(|| {
            scalar_schema(spec.map_or(ManifestDataType::Utf8, |spec| spec.data_type))
        });
        if let Some(object) = schema.as_object_mut() {
            if let Some(values) = spec
                .map(|spec| &spec.values)
                .filter(|values| !values.is_empty())
            {
                object.insert("enum".to_string(), json!(values));
            }
            if let Some(default) = default {
                object.insert("default".to_string(), default);
            }
        }
        let mut description = String::new();
        if let Some(note) = note {
            append_sentence(&mut description, &note);
        }
        ValueSourceDoc {
            schema,
            required: spec.is_some_and(|spec| spec.required),
            description,
            extensions: vec![(
                "x-coral-arg",
                json!(spec.map_or(key, |spec| spec.name.as_str())),
            )],
        }
    }

    fn input_doc(&mut self, key: &str) -> ValueSourceDoc {
        let mut schema = json!({"type": "string"});
        if let (Some(default), Some(object)) = (self.input_default(key), schema.as_object_mut()) {
            object.insert("default".to_string(), json!(default));
        }
        ValueSourceDoc {
            schema,
            required: false,
            description: format!("Resolved from the source input '{key}'."),
            extensions: vec![("x-coral-input", json!(key))],
        }
    }

    fn input_default(&self, key: &str) -> Option<String> {
        self.manifest
            .declared_inputs
            .iter()
            .find(|input| input.key == key)
            .map(|input| input.default_value.clone())
            .filter(|default| !default.is_empty())
    }

    // ------------------------------------------------------------------
    // Pagination
    // ------------------------------------------------------------------

    /// Turn the declared pagination into the request parameters, response
    /// headers, cursor properties, and body fields the API actually
    /// exposes, so downstream OpenAPI consumers (including the v4 importer)
    /// can rediscover the pagination without Coral-specific knowledge.
    fn pagination_artifacts(
        &mut self,
        surface: &Surface<'_>,
        seen: &HashSet<(String, String)>,
    ) -> PaginationArtifacts {
        let pagination = surface.pagination;
        let mut artifacts = PaginationArtifacts {
            response_cursor_path: pagination.response_cursor_path.clone(),
            ..PaginationArtifacts::default()
        };
        let mut add_query = |name: &str, schema: Value, description: &str| {
            if seen.contains(&("query".to_string(), name.to_string())) {
                return;
            }
            artifacts.parameters.push(json!({
                "name": name,
                "in": "query",
                "required": false,
                "description": description,
                "schema": schema,
                "x-coral-pagination": true,
            }));
        };

        if let Some(page_size) = &pagination.page_size {
            let schema = json!({
                "type": "integer",
                "default": page_size.default,
                "maximum": page_size.max,
            });
            if let Some(query_param) = &page_size.query_param {
                add_query(query_param, schema, "Number of results per page.");
            } else if !page_size.body_path.is_empty() {
                artifacts.body_properties.push((
                    page_size.body_path.clone(),
                    pagination_body_schema(schema, "Number of results per page."),
                ));
            }
        }
        // Every declared request-side knob becomes a parameter regardless of
        // the mode: v3 mixes them freely (for example GitHub declares a page
        // parameter alongside link_header pagination). The mode itself only
        // shapes the response side and is preserved in `x-coral`.
        if let Some(page_param) = &pagination.page_param {
            add_query(
                page_param,
                json!({"type": "integer", "default": pagination.page_start}),
                "Page number.",
            );
        }
        if let Some(offset_param) = &pagination.offset_param {
            add_query(
                offset_param,
                json!({"type": "integer", "default": pagination.offset_start}),
                "Offset of the first result.",
            );
        }
        if let Some(cursor_param) = &pagination.cursor_param {
            add_query(
                cursor_param,
                json!({"type": "string"}),
                "Opaque cursor for the next page; omit on the first request.",
            );
        }
        if !pagination.cursor_body_path.is_empty() {
            artifacts.body_properties.push((
                pagination.cursor_body_path.clone(),
                pagination_body_schema(
                    json!({"type": "string"}),
                    "Opaque cursor for the next page; omit on the first request.",
                ),
            ));
        }
        artifacts.response_headers = pagination_response_headers(pagination);
        if surface.response.row_strategy != RowStrategy::Direct
            && !artifacts.response_cursor_path.is_empty()
        {
            self.warn(
                surface,
                "response cursor path on a non-direct row strategy is not represented \
                 in the response schema",
            );
            artifacts.response_cursor_path.clear();
        }
        artifacts
    }

    // ------------------------------------------------------------------
    // Request bodies
    // ------------------------------------------------------------------

    fn request_body_value(
        &mut self,
        surface: &Surface<'_>,
        request: &RequestSpec,
        when_filters: &[String],
        artifacts: &PaginationArtifacts,
    ) -> Option<Value> {
        match &request.body {
            BodySpec::Json { fields } => {
                if fields.is_empty() && artifacts.body_properties.is_empty() {
                    return None;
                }
                let mut tree = SchemaTree::new();
                for field in fields {
                    let doc = self.value_source_doc(surface, &field.value, when_filters);
                    let mut description = doc.description.clone();
                    if let Some(when_arg) = &field.when_arg {
                        append_sentence(
                            &mut description,
                            &format!("Only sent when the '{when_arg}' argument is provided."),
                        );
                    }
                    tree.insert(
                        &field.path,
                        SchemaTree::leaf(body_field_schema(doc, &description)),
                    );
                }
                for (path, schema) in &artifacts.body_properties {
                    tree.insert(path, SchemaTree::leaf(schema.clone()));
                }
                Some(json!({
                    "required": true,
                    "content": {"application/json": {"schema": tree.into_schema()}},
                }))
            }
            BodySpec::Text { content } => {
                if !artifacts.body_properties.is_empty() {
                    self.warn(
                        surface,
                        "pagination body fields cannot be represented in a text request body",
                    );
                }
                let doc = self.value_source_doc(surface, content, when_filters);
                let description = doc.description.clone();
                let schema = body_field_schema(doc, &description);
                Some(json!({
                    "required": true,
                    "content": {"text/plain": {"schema": schema}},
                }))
            }
        }
    }

    // ------------------------------------------------------------------
    // Responses
    // ------------------------------------------------------------------

    fn responses_value(
        &mut self,
        surface: &Surface<'_>,
        row_ref: Option<&Value>,
        artifacts: &PaginationArtifacts,
    ) -> Value {
        let row_schema = row_ref.cloned().unwrap_or_else(|| json!({}));
        let (media_type, schema) = match surface.response.row_strategy {
            RowStrategy::Direct => self.direct_response_schema(surface, &row_schema, artifacts),
            RowStrategy::DictEntries => ("application/json", self.dict_response_schema(surface)),
            RowStrategy::SeriesPointList => {
                self.warn(
                    surface,
                    "series_point_list responses have no faithful OpenAPI schema; \
                     emitted as an unconstrained object",
                );
                ("application/json", json!({}))
            }
        };

        let mut ok_response = Map::new();
        ok_response.insert("description".to_string(), json!("Successful response."));
        if !artifacts.response_headers.is_empty() {
            let headers: Map<String, Value> = artifacts
                .response_headers
                .iter()
                .map(|(name, header)| (name.clone(), header.clone()))
                .collect();
            ok_response.insert("headers".to_string(), Value::Object(headers));
        }
        ok_response.insert(
            "content".to_string(),
            json!({media_type: {"schema": schema}}),
        );

        let mut responses = Map::new();
        responses.insert("200".to_string(), Value::Object(ok_response));
        if surface.response.allow_404_empty {
            responses.insert(
                "404".to_string(),
                json!({"description": "Not found; treated as an empty result set."}),
            );
        }
        Value::Object(responses)
    }

    fn direct_response_schema(
        &mut self,
        surface: &Surface<'_>,
        row_schema: &Value,
        artifacts: &PaginationArtifacts,
    ) -> (&'static str, Value) {
        if surface.response.format == ResponseBodyFormat::JsonEachRow {
            self.warn(
                surface,
                "newline-delimited JSON responses are approximated as an array schema \
                 under application/x-ndjson",
            );
            return (
                "application/x-ndjson",
                json!({
                    "type": "array",
                    "items": row_schema,
                    "description": "Newline-delimited JSON; each line is one row.",
                }),
            );
        }
        let rows = json!({"type": "array", "items": row_schema});
        if !surface.response.rows_path.is_empty() {
            let mut wrapper = SchemaTree::new();
            wrapper.insert(&surface.response.rows_path, SchemaTree::leaf(rows));
            if !artifacts.response_cursor_path.is_empty() {
                wrapper.insert(
                    &artifacts.response_cursor_path,
                    SchemaTree::leaf(json!({
                        "type": "string",
                        "nullable": true,
                        "description": "Cursor for the next page; absent on the last page.",
                    })),
                );
            }
            return ("application/json", wrapper.into_schema());
        }
        if !artifacts.response_cursor_path.is_empty() {
            self.warn(
                surface,
                "response cursor path declared without rows_path; emitting an object \
                 schema with the cursor property only",
            );
            let mut wrapper = SchemaTree::new();
            wrapper.insert(
                &artifacts.response_cursor_path,
                SchemaTree::leaf(json!({"type": "string", "nullable": true})),
            );
            return ("application/json", wrapper.into_schema());
        }
        if surface.pagination.mode == PaginationMode::None {
            self.warn(
                surface,
                "response cardinality is ambiguous (no rows_path, no pagination); \
                 emitted as an array of rows — change to the row object if this \
                 endpoint returns a single object",
            );
        }
        ("application/json", rows)
    }

    /// `dict_entries` rows are synthesized from one JSON object: keys become
    /// the `_key` column and values the remaining columns. The honest
    /// OpenAPI shape is therefore an object with `additionalProperties`.
    fn dict_response_schema(&mut self, surface: &Surface<'_>) -> Value {
        let mut tree = row_schema_tree(surface.columns);
        drop(tree.remove_child("_key"));
        let value_tree = tree.remove_child("_value");
        let additional = match value_tree {
            Some(value_tree) if tree.is_empty() => value_tree.into_schema(),
            Some(_) => {
                self.warn(
                    surface,
                    "dict_entries columns mix `_value` with object fields; using the \
                     object fields for the value schema",
                );
                tree.into_schema()
            }
            None => tree.into_schema(),
        };
        let dict = json!({"type": "object", "additionalProperties": additional});
        if surface.response.rows_path.is_empty() {
            return dict;
        }
        let mut wrapper = SchemaTree::new();
        wrapper.insert(&surface.response.rows_path, SchemaTree::leaf(dict));
        wrapper.into_schema()
    }

    // ------------------------------------------------------------------
    // Servers, security, document extension
    // ------------------------------------------------------------------

    fn servers_value(&mut self) -> Value {
        let template = &self.manifest.base_url;
        let mut url = String::new();
        let mut variables = Map::new();
        for part in template.parts() {
            match part {
                TemplatePart::Literal(literal) => url.push_str(literal),
                TemplatePart::Token(token) => {
                    let name = sanitize_token(token.key());
                    url.push('{');
                    url.push_str(&name);
                    url.push('}');
                    let default = token
                        .default_value()
                        .map(str::to_string)
                        .or_else(|| self.input_default(token.key()))
                        .unwrap_or_else(|| {
                            self.warnings
                                .push(format!("base_url variable '{name}' has no default value"));
                            String::new()
                        });
                    let mut variable = Map::new();
                    variable.insert("default".to_string(), json!(default));
                    if let Some(hint) = self
                        .manifest
                        .declared_inputs
                        .iter()
                        .find(|input| input.key == token.key())
                        .and_then(|input| input.hint.clone())
                    {
                        variable.insert("description".to_string(), json!(hint.trim()));
                    }
                    variables.insert(name, Value::Object(variable));
                }
            }
        }
        let mut server = Map::new();
        server.insert("url".to_string(), json!(url));
        if !variables.is_empty() {
            server.insert("variables".to_string(), Value::Object(variables));
        }
        json!([Value::Object(server)])
    }

    fn security_values(&mut self) -> (Map<String, Value>, Vec<Value>) {
        let mut schemes = Map::new();
        let mut security = Vec::new();
        match &self.manifest.auth {
            AuthSpec::BasicAuth(_) => {
                schemes.insert(
                    "basic_auth".to_string(),
                    json!({"type": "http", "scheme": "basic"}),
                );
                security.push(json!({"basic_auth": []}));
            }
            AuthSpec::CustomAuth(spec) => {
                self.warnings.push(format!(
                    "custom auth '{}' has no OpenAPI equivalent; auth is only recorded \
                     under the document's x-coral extension",
                    spec.authenticator
                ));
            }
            AuthSpec::HeaderAuth(spec) => {
                let mut per_header_keys: Vec<Vec<String>> = Vec::new();
                for header in &spec.headers {
                    let alternatives: Vec<&ValueSourceSpec> = match &header.value {
                        ValueSourceSpec::OneOf { values } => values.iter().collect(),
                        other => vec![other],
                    };
                    let mut keys = Vec::new();
                    for alternative in alternatives {
                        let (key, scheme) = auth_header_scheme(&header.name, alternative);
                        schemes.entry(key.clone()).or_insert(scheme);
                        if !keys.contains(&key) {
                            keys.push(key);
                        }
                    }
                    per_header_keys.push(keys);
                }
                if let [keys] = per_header_keys.as_slice() {
                    // A single auth header with alternatives: each is a valid
                    // way to authenticate on its own.
                    security.extend(keys.iter().map(|key| json!({key.as_str(): []})));
                } else if !per_header_keys.is_empty() {
                    if per_header_keys.iter().any(|keys| keys.len() > 1) {
                        self.warnings.push(
                            "multiple auth headers with alternative sources; only the \
                             first alternative of each is listed as a security requirement"
                                .to_string(),
                        );
                    }
                    let requirement: Map<String, Value> = per_header_keys
                        .iter()
                        .filter_map(|keys| keys.first())
                        .map(|key| (key.clone(), json!([])))
                        .collect();
                    security.push(Value::Object(requirement));
                }
            }
        }
        (schemes, security)
    }

    fn document_extension(&self) -> Value {
        let mut extension = Map::new();
        extension.insert("generator".to_string(), json!("coral xtask export-openapi"));
        extension.insert("source".to_string(), json!(self.manifest.common.name));
        extension.insert(
            "source_version".to_string(),
            json!(self.manifest.common.version),
        );
        extension.insert(
            "dsl_version".to_string(),
            json!(self.manifest.common.dsl_version),
        );
        extension.insert("base_url".to_string(), json!(self.manifest.base_url.raw()));
        extension.insert(
            "auth".to_string(),
            serde_json::to_value(&self.manifest.auth).unwrap_or(Value::Null),
        );
        if !self.manifest.request_headers.is_empty() {
            extension.insert(
                "request_headers".to_string(),
                serde_json::to_value(&self.manifest.request_headers).unwrap_or(Value::Null),
            );
        }
        if rate_limit_is_declared(&self.manifest.rate_limit) {
            extension.insert(
                "rate_limit".to_string(),
                serde_json::to_value(&self.manifest.rate_limit).unwrap_or(Value::Null),
            );
        }
        Value::Object(extension)
    }
}

/// Arguments for [`Converter::filter_doc`], grouped because filter-bound
/// value sources vary along several independent axes.
struct FilterDocRequest<'a, 'b> {
    surface: &'a Surface<'b>,
    key: &'a str,
    when_filters: &'a [String],
    schema_override: Option<Value>,
    default: Option<Value>,
    note: Option<String>,
}

/// Register the reconstructed row schema for one surface under
/// `components/schemas` and return a `$ref` to it. Surfaces whose rows are
/// synthesized rather than read from the payload (`dict_entries`,
/// `series_point_list`) or whose columns imply no shape get no component.
///
/// The component is named exactly after the v3 surface: the v4 importer
/// takes its entity name from the `$ref` leaf and derives list-projection
/// names by pluralizing it (a no-op for v3's conventionally plural table
/// names), so this is what keeps converted catalog names aligned with the
/// v3 names. A `_row`-style suffix here would leak into every derived name.
fn register_row_schema(surface: &Surface<'_>, schemas: &mut Map<String, Value>) -> Option<Value> {
    if surface.response.row_strategy != RowStrategy::Direct {
        return None;
    }
    let tree = row_schema_tree(surface.columns);
    if tree.is_empty() {
        return None;
    }
    let mut name = sanitize_token(surface.name);
    let mut suffix = 2;
    while schemas.contains_key(&name) {
        name = format!("{}_{suffix}", sanitize_token(surface.name));
        suffix += 1;
    }
    schemas.insert(name.clone(), tree.into_schema());
    Some(json!({"$ref": format!("#/components/schemas/{name}")}))
}

fn operation_extension(surface: &Surface<'_>, when_filters: &[String]) -> Value {
    let mut extension = Map::new();
    extension.insert("surface".to_string(), json!(surface.kind));
    extension.insert("name".to_string(), json!(surface.name));
    if !when_filters.is_empty() {
        extension.insert("when_filters".to_string(), json!(when_filters));
    }
    if surface.function_kind == Some(SourceTableFunctionKind::Search) {
        extension.insert("function_kind".to_string(), json!("search"));
    }
    if let Some(fetch_limit) = surface.fetch_limit_default {
        extension.insert("fetch_limit_default".to_string(), json!(fetch_limit));
    }
    if let Some(search_limits) = surface.search_limits {
        extension.insert(
            "search_limits".to_string(),
            serde_json::to_value(search_limits).unwrap_or(Value::Null),
        );
    }
    if !surface.detail_hints.is_empty() {
        extension.insert(
            "detail_hints".to_string(),
            serde_json::to_value(surface.detail_hints).unwrap_or(Value::Null),
        );
    }
    if response_is_declared(surface.response) {
        extension.insert(
            "response".to_string(),
            serde_json::to_value(surface.response).unwrap_or(Value::Null),
        );
    }
    if pagination_is_declared(surface.pagination) {
        extension.insert(
            "pagination".to_string(),
            serde_json::to_value(surface.pagination).unwrap_or(Value::Null),
        );
    }
    Value::Object(extension)
}

fn response_is_declared(response: &ResponseSpec) -> bool {
    !response.rows_path.is_empty()
        || !response.ok_path.is_empty()
        || !response.error_path.is_empty()
        || response.allow_404_empty
        || response.format != ResponseBodyFormat::Json
        || response.row_strategy != RowStrategy::Direct
}

fn pagination_is_declared(pagination: &PaginationSpec) -> bool {
    pagination.mode != PaginationMode::None
        || pagination.page_size.is_some()
        || pagination.next_url_header.is_some()
        || pagination.response_cursor_header.is_some()
}

fn rate_limit_is_declared(rate_limit: &RateLimitSpec) -> bool {
    !rate_limit.extra_statuses.is_empty()
        || rate_limit.retry_after_header.is_some()
        || rate_limit.remaining_header.is_some()
        || rate_limit.reset_header.is_some()
}

fn parameter_value(name: &str, location: &str, required: bool, doc: ValueSourceDoc) -> Value {
    let mut parameter = Map::new();
    parameter.insert("name".to_string(), json!(name));
    parameter.insert("in".to_string(), json!(location));
    parameter.insert("required".to_string(), json!(required));
    if !doc.description.is_empty() {
        parameter.insert("description".to_string(), json!(doc.description));
    }
    parameter.insert("schema".to_string(), doc.schema);
    for (key, value) in doc.extensions {
        parameter.insert(key.to_string(), value);
    }
    Value::Object(parameter)
}

/// Fold a value-source doc into a standalone schema object for use inside a
/// request-body schema, where description and extensions have no parameter
/// object to live on.
fn body_field_schema(doc: ValueSourceDoc, description: &str) -> Value {
    let mut schema = doc.schema;
    if let Some(object) = schema.as_object_mut() {
        if !description.is_empty() {
            object.insert("description".to_string(), json!(description));
        }
        for (key, value) in doc.extensions {
            object.insert(key.to_string(), value);
        }
    }
    schema
}

/// The response headers the declared pagination promises: RFC 5988 `Link`
/// headers, cursor headers, and next-page-URL headers.
fn pagination_response_headers(pagination: &PaginationSpec) -> Vec<(String, Value)> {
    let mut headers = Vec::new();
    if pagination.mode == PaginationMode::LinkHeader {
        headers.push((
            "Link".to_string(),
            json!({
                "description": "RFC 5988 pagination links; rel=\"next\" points at the next page.",
                "schema": {"type": "string"},
            }),
        ));
    }
    if let Some(header) = &pagination.response_cursor_header {
        headers.push((
            header.clone(),
            json!({
                "description": "Cursor for the next page; absent on the last page.",
                "schema": {"type": "string"},
            }),
        ));
    }
    if let Some(header) = &pagination.next_url_header {
        headers.push((
            header.clone(),
            json!({
                "description": "URL of the next page; absent on the last page.",
                "schema": {"type": "string"},
            }),
        ));
    }
    headers
}

fn pagination_body_schema(mut schema: Value, description: &str) -> Value {
    if let Some(object) = schema.as_object_mut() {
        object.insert("description".to_string(), json!(description));
        object.insert("x-coral-pagination".to_string(), json!(true));
    }
    schema
}

fn state_doc(key: &str) -> ValueSourceDoc {
    ValueSourceDoc {
        schema: json!({"type": "string"}),
        required: false,
        description: "Managed by the Coral runtime (request or pagination state).".to_string(),
        extensions: vec![("x-coral-state", json!(key))],
    }
}

fn literal_schema(value: &Value) -> Value {
    let mut schema = Map::new();
    let type_name = match value {
        Value::String(_) => Some("string"),
        Value::Bool(_) => Some("boolean"),
        Value::Number(number) => Some(if number.is_f64() { "number" } else { "integer" }),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    };
    if let Some(type_name) = type_name {
        schema.insert("type".to_string(), json!(type_name));
    }
    schema.insert("enum".to_string(), json!([value]));
    schema.insert("default".to_string(), value.clone());
    Value::Object(schema)
}

fn split_note(key: &str, separator: &str, part: usize) -> String {
    format!("Part {part} (0-based) of the '{key}' value split on '{separator}'.")
}

fn append_sentence(description: &mut String, sentence: &str) {
    if !description.is_empty() && !description.ends_with(' ') {
        description.push(' ');
    }
    description.push_str(sentence);
}

fn method_key(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "get",
        HttpMethod::POST => "post",
    }
}

/// Restrict a template-token or component key to the characters OpenAPI
/// accepts for path parameter names and component keys.
fn sanitize_token(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "_".to_string()
    } else {
        sanitized
    }
}

/// Map one auth header value source to an OpenAPI security scheme.
fn auth_header_scheme(header_name: &str, source: &ValueSourceSpec) -> (String, Value) {
    let input_keys = auth_source_input_keys(source);
    let description = if input_keys.is_empty() {
        String::new()
    } else {
        format!("Provided via source input(s): {}.", input_keys.join(", "))
    };
    let is_authorization = header_name.eq_ignore_ascii_case("authorization");
    let bearer = matches!(source, ValueSourceSpec::Bearer { .. })
        || matches!(source, ValueSourceSpec::Template { template } if template.raw().starts_with("Bearer "));
    if is_authorization && bearer {
        let mut scheme = Map::new();
        scheme.insert("type".to_string(), json!("http"));
        scheme.insert("scheme".to_string(), json!("bearer"));
        if !description.is_empty() {
            scheme.insert("description".to_string(), json!(description));
        }
        return ("bearer_auth".to_string(), Value::Object(scheme));
    }
    if is_authorization
        && matches!(source, ValueSourceSpec::Template { template } if template.raw().starts_with("Basic "))
    {
        let mut scheme = Map::new();
        scheme.insert("type".to_string(), json!("http"));
        scheme.insert("scheme".to_string(), json!("basic"));
        if !description.is_empty() {
            scheme.insert("description".to_string(), json!(description));
        }
        return ("basic_auth".to_string(), Value::Object(scheme));
    }
    let mut scheme = Map::new();
    scheme.insert("type".to_string(), json!("apiKey"));
    scheme.insert("in".to_string(), json!("header"));
    scheme.insert("name".to_string(), json!(header_name));
    if !description.is_empty() {
        scheme.insert("description".to_string(), json!(description));
    }
    (
        sanitize_token(&header_name.to_ascii_lowercase()),
        Value::Object(scheme),
    )
}

fn auth_source_input_keys(source: &ValueSourceSpec) -> Vec<String> {
    match source {
        ValueSourceSpec::Template { template } => template
            .tokens()
            .flat_map(TemplateToken::input_keys)
            .map(str::to_string)
            .collect(),
        ValueSourceSpec::Input { key } | ValueSourceSpec::Bearer { key } => vec![key.clone()],
        _ => Vec::new(),
    }
}
