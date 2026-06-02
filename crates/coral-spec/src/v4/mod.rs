#![allow(
    missing_docs,
    reason = "DSL v4 contracts are field-heavy artifact models documented in the PRD."
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

mod schema;

use crate::backends::http::{AuthSpec, RateLimitSpec};
use crate::inputs::{collect_declared_inputs, validate_input_references};
use crate::{
    ColumnSpec, DetailHintSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding, HeaderSpec,
    ManifestDataType, ManifestError, ManifestInputSpec, PageSizeSpec, PaginationMode,
    PaginationSpec, ParsedTemplate, RequestSpec, ResponseSpec, Result, SearchLimitsSpec,
    SourceTableFunctionKind, TableFunctionArgSpec, validate_test_queries,
};

pub const V4_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const OPENAPI_IMPORTER_VERSION: &str = "openapi-v2";
pub const PROJECTION_GENERATOR_VERSION: &str = "derive-read-v3";

pub use schema::generated_v4_source_manifest_schema;

#[derive(Debug, Clone)]
pub struct V4SourceManifest {
    pub common: V4SourceCommon,
    pub surfaces: Vec<V4Surface>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone)]
pub struct V4SourceCommon {
    pub dsl_version: u32,
    pub name: String,
    pub description: String,
    pub test_queries: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct V4Surface {
    pub id: String,
    pub surface_type: SurfaceType,
    pub descriptor: SurfaceDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
    pub openapi_runtime: OpenApiRuntimeConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceType {
    OpenApi,
}

#[derive(Debug, Clone)]
pub enum SurfaceDescriptor {
    Url { url: String },
    File { file: PathBuf },
}

impl SurfaceDescriptor {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Url { .. } => "url",
            Self::File { .. } => "file",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::Url { url, .. } => url.clone(),
            Self::File { file, .. } => file.display().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenApiRuntimeConfig {
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub request_headers: Vec<HeaderSpec>,
    pub rate_limit: RateLimitSpec,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4SourceManifest {
    dsl_version: u32,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    surfaces: Vec<RawV4Surface>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4Surface {
    id: String,
    #[serde(rename = "type")]
    _surface_type: RawSurfaceType,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    file: Option<PathBuf>,
    #[serde(default, rename = "inputs")]
    _inputs: Option<Value>,
    #[serde(default)]
    base_url: Option<ParsedTemplate>,
    #[serde(default)]
    auth: AuthSpec,
    #[serde(default)]
    request_headers: Vec<HeaderSpec>,
    #[serde(default)]
    rate_limit: RateLimitSpec,
}

#[derive(Debug, Deserialize)]
enum RawSurfaceType {
    #[serde(rename = "openapi")]
    OpenApi,
}

impl V4SourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw_value = value.clone();
        let raw: RawV4SourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawV4SourceManifest {
            dsl_version,
            name,
            description,
            test_queries,
            surfaces,
        } = raw;
        if dsl_version != 4 {
            return Err(ManifestError::validation(format!(
                "source '{name}' declares dsl_version {dsl_version}; expected 4"
            )));
        }
        if surfaces.is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{name}' must declare at least one surface"
            )));
        }
        validate_test_queries(&name, &test_queries)?;
        let common = V4SourceCommon {
            dsl_version,
            name: name.clone(),
            description,
            test_queries,
        };
        let surface_values = raw_value
            .get("surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| ManifestError::validation("v4 manifest surfaces must be a list"))?;
        let mut seen_surface_ids = HashSet::new();
        let mut validated_surfaces = Vec::with_capacity(surfaces.len());
        let mut declared_inputs = Vec::new();
        let mut input_by_key: BTreeMap<String, (String, ManifestInputSpec)> = BTreeMap::new();

        for (index, raw_surface) in surfaces.into_iter().enumerate() {
            let surface_value = surface_values.get(index).ok_or_else(|| {
                ManifestError::validation(format!("source '{name}' surface[{index}] is missing"))
            })?;
            validate_surface_id(&name, &raw_surface.id)?;
            if !seen_surface_ids.insert(raw_surface.id.clone()) {
                return Err(ManifestError::validation(format!(
                    "source '{name}' has duplicate surface id '{}'",
                    raw_surface.id
                )));
            }
            let inputs = collect_declared_inputs(surface_value)?;
            validate_input_references(surface_value, &inputs)?;
            merge_surface_inputs(
                &name,
                &raw_surface.id,
                &inputs,
                &mut input_by_key,
                &mut declared_inputs,
            )?;
            let descriptor = parse_descriptor(&name, &raw_surface)?;
            validated_surfaces.push(V4Surface {
                id: raw_surface.id,
                surface_type: SurfaceType::OpenApi,
                descriptor,
                inputs,
                openapi_runtime: OpenApiRuntimeConfig {
                    base_url: raw_surface
                        .base_url
                        .unwrap_or_else(|| ParsedTemplate::parse("").expect("empty template")),
                    auth: raw_surface.auth,
                    request_headers: raw_surface.request_headers,
                    rate_limit: raw_surface.rate_limit,
                },
            });
        }

        Ok(Self {
            common,
            surfaces: validated_surfaces,
            declared_inputs,
        })
    }

    pub fn surface(&self, surface_id: &str) -> Option<&V4Surface> {
        self.surfaces
            .iter()
            .find(|surface| surface.id == surface_id)
    }
}

fn validate_surface_id(source_name: &str, id: &str) -> Result<()> {
    let mut chars = id.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface id '{id}' must match [a-z][a-z0-9_]*"
        )))
    }
}

fn parse_descriptor(source_name: &str, surface: &RawV4Surface) -> Result<SurfaceDescriptor> {
    match (&surface.url, &surface.file) {
        (Some(url), None) => {
            if !url.starts_with("https://") {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{}' url descriptors must use https",
                    surface.id
                )));
            }
            Ok(SurfaceDescriptor::Url { url: url.clone() })
        }
        (None, Some(file)) => Ok(SurfaceDescriptor::File { file: file.clone() }),
        (Some(_), Some(_)) | (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{}' must declare exactly one of url or file",
            surface.id
        ))),
    }
}

fn merge_surface_inputs(
    source_name: &str,
    surface_id: &str,
    inputs: &[ManifestInputSpec],
    input_by_key: &mut BTreeMap<String, (String, ManifestInputSpec)>,
    declared_inputs: &mut Vec<ManifestInputSpec>,
) -> Result<()> {
    for input in inputs {
        if let Some((existing_surface, existing)) = input_by_key.get(&input.key) {
            if existing != input {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surfaces '{existing_surface}' and '{surface_id}' declare incompatible input '{}'",
                    input.key
                )));
            }
            continue;
        }
        input_by_key.insert(input.key.clone(), (surface_id.to_string(), input.clone()));
        declared_inputs.push(input.clone());
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticIr {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub importer_version: String,
    pub operations: Vec<IrOperation>,
    pub types: Vec<IrType>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperation {
    pub id: String,
    pub method_name: String,
    pub description: String,
    pub deprecated: bool,
    pub read_only: bool,
    pub inputs: Vec<IrOperationInput>,
    pub output: IrOperationOutput,
    pub entity: Option<IrEntityCandidate>,
    pub execution: IrExecutionAttachment,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationInput {
    pub name: String,
    pub location: OpenApiParameterLocation,
    pub required: bool,
    pub data_type: IrScalarType,
    pub default_value: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationOutput {
    pub cardinality: OutputCardinality,
    pub type_ref: String,
    pub row_path: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrEntityCandidate {
    pub name: String,
    pub type_ref: String,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputCardinality {
    None,
    Singleton,
    List,
    WrappedList,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrType {
    pub id: String,
    pub shape: IrTypeShape,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IrTypeShape {
    Scalar(IrScalarType),
    Object { fields: Vec<IrField> },
    List { item_type_ref: String },
    Map { value_type_ref: String },
    Enum { values: Vec<String> },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrField {
    pub name: String,
    pub type_ref: String,
    pub required: bool,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IrScalarType {
    String,
    Integer,
    Number,
    Boolean,
    Id,
    Timestamp,
    Json,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OpenApiParameterLocation {
    Path,
    Query,
    Header,
    Cookie,
    Body,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HttpMethod {
    Get,
    Head,
    Options,
    Post,
    Put,
    Patch,
    Delete,
    Trace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IrExecutionAttachment {
    Rest(RestExecutionAttachment),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestExecutionAttachment {
    pub method: HttpMethod,
    pub path_template: String,
    pub parameters: Vec<RestParameterBinding>,
    pub request_body: Option<RestRequestBody>,
    pub response: RestResponseAttachment,
    pub pagination: PaginationSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestRequestBody {
    pub required: bool,
    pub media_type: String,
    pub type_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestParameterBinding {
    pub input_name: String,
    pub location: OpenApiParameterLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: IrScalarType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponseAttachment {
    pub status_code: u16,
    pub media_type: String,
    pub response: ResponseSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalog {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub generator_version: String,
    pub projections: Vec<Projection>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub name: String,
    pub kind: ProjectionKind,
    pub description: String,
    pub guide: String,
    pub surface_id: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInput>,
    pub columns: Vec<ProjectionColumn>,
    pub pagination: PaginationSpec,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionKind {
    Table,
    TableFunction {
        function_kind: SourceTableFunctionKind,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionVisibility {
    Published,
    Hidden,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionInput {
    pub name: String,
    pub sql_exposure: SqlInputExposure,
    pub source_location: OpenApiParameterLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: ManifestDataType,
    pub default_value: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SqlInputExposure {
    Filter,
    FunctionArg,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionColumn {
    pub name: String,
    pub data_type: ManifestDataType,
    pub source_path: Vec<String>,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub surface_id: Option<String>,
    pub operation_id: Option<String>,
    pub projection_name: Option<String>,
}

impl Diagnostic {
    fn warning(
        code: &str,
        message: impl Into<String>,
        surface_id: impl Into<String>,
        operation_id: Option<String>,
    ) -> Self {
        Self {
            code: code.to_string(),
            severity: DiagnosticSeverity::Warning,
            message: message.into(),
            surface_id: Some(surface_id.into()),
            operation_id,
            projection_name: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4MaterializedSource {
    pub fingerprint: Fingerprint,
    pub surfaces: Vec<MaterializedSurface>,
    pub projections: ProjectionCatalog,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedSurface {
    pub surface_id: String,
    pub semantic_ir: SemanticIr,
    pub source_document_sha256: String,
    pub normalized_source_document_path: PathBuf,
    pub raw_source_document_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fingerprint {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub manifest_sha256: String,
    pub surfaces: Vec<FingerprintSurface>,
    pub importer_version: String,
    pub projection_generator_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintSurface {
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub descriptor_kind: String,
    pub descriptor_location: String,
    pub descriptor_sha256: String,
    pub input_declarations_sha256: String,
}

pub fn normalize_source_document(bytes: &[u8]) -> Result<String> {
    let value: Value = serde_yaml::from_slice(bytes).map_err(ManifestError::parse_yaml)?;
    serde_yaml::to_string(&value).map_err(ManifestError::parse_yaml)
}

#[derive(Debug, Clone, Default)]
pub struct OpenApiDocumentMetadata {
    pub description: Option<String>,
    pub server_url: Option<String>,
}

pub fn openapi_document_metadata(document_bytes: &[u8]) -> Result<OpenApiDocumentMetadata> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !openapi.starts_with("3.0.") {
        return Err(ManifestError::validation(format!(
            "OpenAPI document uses unsupported version '{openapi}'"
        )));
    }
    Ok(OpenApiDocumentMetadata {
        description: trimmed_string_at(&document, &["info", "description"]),
        server_url: document
            .get("servers")
            .and_then(Value::as_array)
            .and_then(|servers| servers.iter().find_map(openapi_server_url)),
    })
}

fn openapi_server_url(server: &Value) -> Option<String> {
    let url = server
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())?;
    let variables = server.get("variables").and_then(Value::as_object);
    resolve_openapi_server_url(url, variables)
}

fn resolve_openapi_server_url(url: &str, variables: Option<&Map<String, Value>>) -> Option<String> {
    let mut resolved = String::with_capacity(url.len());
    let mut rest = url;
    while let Some((literal, after_open)) = rest.split_once('{') {
        resolved.push_str(literal);
        let (name, after_close) = after_open.split_once('}')?;
        let default = variables?.get(name)?.get("default")?.as_str()?.trim();
        if default.is_empty() {
            return None;
        }
        resolved.push_str(default);
        rest = after_close;
    }
    resolved.push_str(rest);
    Some(resolved)
}

fn trimmed_string_at(document: &Value, path: &[&str]) -> Option<String> {
    let value = path
        .iter()
        .try_fold(document, |value, key| value.get(*key))?;
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<SemanticIr> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !openapi.starts_with("3.0.") {
        return Err(ManifestError::validation(format!(
            "OpenAPI document for surface '{}' uses unsupported version '{openapi}'",
            surface.id
        )));
    }

    let mut importer = OpenApiImporter::new(manifest, surface, &document);
    importer.import()
}

struct OpenApiImporter<'a> {
    manifest: &'a V4SourceManifest,
    surface: &'a V4Surface,
    document: &'a Value,
    types: BTreeMap<String, IrType>,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> OpenApiImporter<'a> {
    fn new(manifest: &'a V4SourceManifest, surface: &'a V4Surface, document: &'a Value) -> Self {
        Self {
            manifest,
            surface,
            document,
            types: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn import(&mut self) -> Result<SemanticIr> {
        let paths = self
            .document
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| ManifestError::validation("OpenAPI document is missing paths"))?;
        let mut operations = Vec::new();
        let mut operation_ids = HashSet::new();
        for (path, path_item) in paths {
            let Some(path_item) = path_item.as_object() else {
                continue;
            };
            for method_name in [
                "get", "head", "options", "post", "put", "patch", "delete", "trace",
            ] {
                let Some(operation_value) = path_item.get(method_name) else {
                    continue;
                };
                let operation =
                    self.import_operation(path, path_item, method_name, operation_value)?;
                if !operation_ids.insert(operation.id.clone()) {
                    return Err(ManifestError::validation(format!(
                        "source '{}' surface '{}' imports duplicate operation id '{}'",
                        self.manifest.common.name, self.surface.id, operation.id
                    )));
                }
                operations.push(operation);
            }
        }
        Ok(SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            surface_id: self.surface.id.clone(),
            surface_type: self.surface.surface_type,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        })
    }

    fn import_operation(
        &mut self,
        path: &str,
        path_item: &Map<String, Value>,
        method_name: &str,
        operation: &Value,
    ) -> Result<IrOperation> {
        let op_obj = operation.as_object().ok_or_else(|| {
            ManifestError::validation(format!(
                "OpenAPI operation {method_name} {path} must be a mapping"
            ))
        })?;
        let operation_id = op_obj
            .get("operationId")
            .and_then(Value::as_str)
            .map_or_else(
                || fallback_operation_id(method_name, path),
                |raw| normalize_identifier(raw, "operation"),
            );
        let method = parse_http_method(method_name);
        let mut diagnostics = Vec::new();
        let parameters = self.import_parameters(path_item, op_obj, &operation_id, &mut diagnostics);
        let request_body = self.import_request_body(op_obj, &operation_id, &mut diagnostics);
        let (output, response, entity) =
            self.import_response(path, op_obj, &operation_id, &mut diagnostics);
        let pagination = detect_pagination(&parameters);
        let rest_parameters = parameters
            .iter()
            .map(|input| RestParameterBinding {
                input_name: input.name.clone(),
                location: input.location,
                wire_name: input.name.clone(),
                required: input.required,
                data_type: input.data_type,
            })
            .collect();
        Ok(IrOperation {
            id: operation_id.clone(),
            method_name: op_obj
                .get("operationId")
                .and_then(Value::as_str)
                .unwrap_or(method_name)
                .to_string(),
            description: op_obj
                .get("description")
                .or_else(|| op_obj.get("summary"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            deprecated: op_obj
                .get("deprecated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            read_only: method == HttpMethod::Get,
            inputs: parameters,
            output,
            entity,
            execution: IrExecutionAttachment::Rest(RestExecutionAttachment {
                method,
                path_template: path.to_string(),
                parameters: rest_parameters,
                request_body,
                response,
                pagination,
            }),
            diagnostics,
        })
    }

    fn import_parameters(
        &mut self,
        path_item: &Map<String, Value>,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrOperationInput> {
        let mut merged: BTreeMap<(OpenApiParameterLocation, String), Value> = BTreeMap::new();
        for parameter in path_item
            .get("parameters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(
                operation
                    .get("parameters")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten(),
            )
        {
            let Some(resolved) = self.resolve_ref(parameter, operation_id, diagnostics) else {
                continue;
            };
            let Some(parameter_obj) = resolved.as_object() else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_INVALID",
                    format!("operation '{operation_id}' has a parameter that is not an object"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(name) = parameter_obj.get("name").and_then(Value::as_str) else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_INVALID",
                    format!("operation '{operation_id}' has a parameter without a string name"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(location) = parameter_obj
                .get("in")
                .and_then(Value::as_str)
                .and_then(parse_parameter_location)
            else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_SERIALIZATION_UNSUPPORTED",
                    format!("operation '{operation_id}' has unsupported parameter location"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            merged.insert((location, name.to_string()), resolved.clone());
        }

        merged
            .into_values()
            .filter_map(|parameter| {
                let parameter_obj = parameter.as_object()?;
                let name = parameter_obj.get("name")?.as_str()?.to_string();
                let location = parameter_obj
                    .get("in")
                    .and_then(Value::as_str)
                    .and_then(parse_parameter_location)?;
                let schema = parameter_obj.get("schema").unwrap_or(&Value::Null);
                let scalar =
                    self.import_parameter_scalar(schema, &name, operation_id, diagnostics)?;
                Some(IrOperationInput {
                    name,
                    location,
                    required: parameter_obj
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    data_type: scalar,
                    default_value: schema.get("default").map(openapi_default_to_string),
                    description: parameter_obj
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
            })
            .collect()
    }

    fn import_parameter_scalar(
        &mut self,
        schema: &Value,
        name: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<IrScalarType> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let schema_type = resolved
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("string");
        let scalar = match schema_type {
            "string" => {
                if resolved.get("format").and_then(Value::as_str) == Some("date-time") {
                    IrScalarType::Timestamp
                } else {
                    IrScalarType::String
                }
            }
            "integer" => IrScalarType::Integer,
            "number" => IrScalarType::Number,
            "boolean" => IrScalarType::Boolean,
            other => {
                diagnostics.push(Diagnostic::warning(
                    "PROJECTION_INPUT_UNSUPPORTED",
                    format!("parameter '{name}' has unsupported schema type '{other}'"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                return None;
            }
        };
        Some(scalar)
    }

    fn import_request_body(
        &mut self,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<RestRequestBody> {
        let body = operation.get("requestBody")?;
        let body = self.resolve_ref(body, operation_id, diagnostics)?;
        let body_obj = body.as_object()?;
        let content = body_obj.get("content")?.as_object()?;
        let json = content.get("application/json")?;
        let schema = json.get("schema").unwrap_or(&Value::Null);
        let type_ref = self
            .import_schema(
                schema,
                &format!("{operation_id}_request_body"),
                operation_id,
                diagnostics,
            )
            .unwrap_or_else(|| "json".to_string());
        diagnostics.push(Diagnostic::warning(
            "OPENAPI_REQUEST_BODY_UNPUBLISHED",
            format!("operation '{operation_id}' has a request body and will not be published"),
            self.surface.id.clone(),
            Some(operation_id.to_string()),
        ));
        Some(RestRequestBody {
            required: body_obj
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            media_type: "application/json".to_string(),
            type_ref,
        })
    }

    fn import_response(
        &mut self,
        path: &str,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> (
        IrOperationOutput,
        RestResponseAttachment,
        Option<IrEntityCandidate>,
    ) {
        let Some((status_code, media_type, schema)) =
            select_json_response(operation.get("responses").and_then(Value::as_object))
        else {
            let response = ResponseSpec::default();
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::None,
                    type_ref: "none".to_string(),
                    row_path: Vec::new(),
                },
                RestResponseAttachment {
                    status_code: 204,
                    media_type: "application/json".to_string(),
                    response,
                },
                None,
            );
        };

        let Some(resolved) = self.resolve_ref(schema, operation_id, diagnostics) else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_RESPONSE_SCHEMA_UNRESOLVED",
                format!("operation '{operation_id}' response schema could not be resolved"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::Unknown,
                    type_ref: "json".to_string(),
                    row_path: Vec::new(),
                },
                RestResponseAttachment {
                    status_code,
                    media_type,
                    response: ResponseSpec::default(),
                },
                None,
            );
        };
        let (cardinality, row_path, row_schema, entity_name) =
            classify_response_schema(path, &resolved);
        let type_ref = self
            .import_schema(
                &row_schema,
                &format!("{operation_id}_row"),
                operation_id,
                diagnostics,
            )
            .unwrap_or_else(|| "json".to_string());
        let response = ResponseSpec {
            rows_path: row_path.clone(),
            ..ResponseSpec::default()
        };
        let entity = (cardinality != OutputCardinality::None
            && cardinality != OutputCardinality::Unknown)
            .then(|| IrEntityCandidate {
                name: entity_name.unwrap_or_else(|| entity_name_from_path(path)),
                type_ref: type_ref.clone(),
                identity_fields: vec!["id".to_string()],
            });
        (
            IrOperationOutput {
                cardinality,
                type_ref,
                row_path,
            },
            RestResponseAttachment {
                status_code,
                media_type,
                response,
            },
            entity,
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "OpenAPI schema import is deliberately kept in one local recursive routine for the first v4 slice."
    )]
    fn import_schema(
        &mut self,
        schema: &Value,
        suggested_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<String> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let type_id = schema.get("$ref").and_then(Value::as_str).map_or_else(
            || normalize_identifier(suggested_id, "type"),
            type_id_from_ref,
        );
        if self.types.contains_key(&type_id) {
            return Some(type_id);
        }
        let description = resolved
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let nullable = resolved
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape: IrTypeShape::Json,
                nullable,
                description: description.clone(),
            },
        );
        let shape = if let Some(all_of) = resolved.get("allOf").and_then(Value::as_array) {
            let mut merged = Map::new();
            for item in all_of {
                let item = self.resolve_ref(item, operation_id, diagnostics)?;
                if let Some(properties) = item.get("properties").and_then(Value::as_object) {
                    for (name, property) in properties {
                        if let Some(existing) = merged.get(name)
                            && existing != property
                        {
                            diagnostics.push(Diagnostic::warning(
                                "OPENAPI_ALLOF_CONFLICT",
                                format!("allOf property '{name}' conflicts in operation '{operation_id}'"),
                                self.surface.id.clone(),
                                Some(operation_id.to_string()),
                            ));
                            return None;
                        }
                        merged.insert(name.clone(), property.clone());
                    }
                }
            }
            IrTypeShape::Object {
                fields: self.import_object_fields(
                    &merged,
                    &BTreeSet::new(),
                    &type_id,
                    operation_id,
                    diagnostics,
                ),
            }
        } else if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
            IrTypeShape::Enum {
                values: values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            }
        } else {
            match resolved
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("object")
            {
                "object" => {
                    if let Some(properties) = resolved.get("properties").and_then(Value::as_object)
                    {
                        let required = required_fields(&resolved);
                        IrTypeShape::Object {
                            fields: self.import_object_fields(
                                properties,
                                &required,
                                &type_id,
                                operation_id,
                                diagnostics,
                            ),
                        }
                    } else if let Some(additional) = resolved.get("additionalProperties") {
                        let value_type_ref = self
                            .import_schema(
                                additional,
                                &format!("{type_id}_value"),
                                operation_id,
                                diagnostics,
                            )
                            .unwrap_or_else(|| "json".to_string());
                        IrTypeShape::Map { value_type_ref }
                    } else {
                        IrTypeShape::Json
                    }
                }
                "array" => {
                    let item = resolved.get("items").unwrap_or(&Value::Null);
                    let item_type_ref = self
                        .import_schema(item, &format!("{type_id}_item"), operation_id, diagnostics)
                        .unwrap_or_else(|| "json".to_string());
                    IrTypeShape::List { item_type_ref }
                }
                "string" => {
                    let scalar =
                        if resolved.get("format").and_then(Value::as_str) == Some("date-time") {
                            IrScalarType::Timestamp
                        } else {
                            IrScalarType::String
                        };
                    IrTypeShape::Scalar(scalar)
                }
                "integer" => IrTypeShape::Scalar(IrScalarType::Integer),
                "number" => IrTypeShape::Scalar(IrScalarType::Number),
                "boolean" => IrTypeShape::Scalar(IrScalarType::Boolean),
                _ => IrTypeShape::Json,
            }
        };
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape,
                nullable,
                description,
            },
        );
        Some(type_id)
    }

    fn import_object_fields(
        &mut self,
        properties: &Map<String, Value>,
        required: &BTreeSet<String>,
        parent_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrField> {
        properties
            .iter()
            .map(|(name, schema)| {
                let type_ref = self
                    .import_schema(
                        schema,
                        &format!("{parent_id}_{name}"),
                        operation_id,
                        diagnostics,
                    )
                    .unwrap_or_else(|| "json".to_string());
                IrField {
                    name: name.clone(),
                    type_ref,
                    required: required.contains(name),
                    nullable: true,
                    description: schema
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }
            })
            .collect()
    }

    fn resolve_ref(
        &self,
        value: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        let Some(reference) = value.get("$ref").and_then(Value::as_str) else {
            return Some(value.clone());
        };
        if !reference.starts_with("#/") {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_EXTERNAL_REF_UNSUPPORTED",
                format!("external reference '{reference}' is unsupported"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return None;
        }
        let pointer = reference.strip_prefix('#').unwrap_or(reference);
        if let Some(target) = self.document.pointer(pointer) {
            Some(target.clone())
        } else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_REF_NOT_FOUND",
                format!("reference '{reference}' was not found"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            None
        }
    }
}

fn parse_http_method(method: &str) -> HttpMethod {
    match method {
        "get" => HttpMethod::Get,
        "head" => HttpMethod::Head,
        "options" => HttpMethod::Options,
        "post" => HttpMethod::Post,
        "put" => HttpMethod::Put,
        "patch" => HttpMethod::Patch,
        "delete" => HttpMethod::Delete,
        "trace" => HttpMethod::Trace,
        other => unreachable!("unsupported method passed to OpenAPI importer: {other}"),
    }
}

fn parse_parameter_location(location: &str) -> Option<OpenApiParameterLocation> {
    match location {
        "path" => Some(OpenApiParameterLocation::Path),
        "query" => Some(OpenApiParameterLocation::Query),
        "header" => Some(OpenApiParameterLocation::Header),
        "cookie" => Some(OpenApiParameterLocation::Cookie),
        _ => None,
    }
}

fn openapi_default_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null | Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn select_json_response(responses: Option<&Map<String, Value>>) -> Option<(u16, String, &Value)> {
    let responses = responses?;
    let mut candidates = Vec::new();
    for (status, response) in responses {
        let Ok(status_code) = status.parse::<u16>() else {
            continue;
        };
        if !(200..300).contains(&status_code) {
            continue;
        }
        let Some(content) = response.get("content").and_then(Value::as_object) else {
            continue;
        };
        let Some(json) = content.get("application/json") else {
            continue;
        };
        let schema = json.get("schema").unwrap_or(&Value::Null);
        candidates.push((status_code, "application/json".to_string(), schema));
    }
    candidates
        .iter()
        .position(|(status, _, _)| *status == 200)
        .and_then(|index| candidates.get(index).cloned())
        .or_else(|| candidates.into_iter().min_by_key(|(status, _, _)| *status))
}

fn classify_response_schema(
    path: &str,
    schema: &Value,
) -> (OutputCardinality, Vec<String>, Value, Option<String>) {
    if schema == &Value::Null {
        return (OutputCardinality::None, Vec::new(), Value::Null, None);
    }
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        let item = schema.get("items").cloned().unwrap_or(Value::Null);
        return (
            OutputCardinality::List,
            Vec::new(),
            item.clone(),
            item.get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref),
        );
    }
    if schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        == "object"
    {
        if let Some((property_name, items)) = schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(wrapped_list_property)
        {
            let item = items.get("items").cloned().unwrap_or(Value::Null);
            return (
                OutputCardinality::WrappedList,
                vec![property_name.to_string()],
                item.clone(),
                item.get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref),
            );
        }
        return (
            OutputCardinality::Singleton,
            Vec::new(),
            schema.clone(),
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref)
                .or_else(|| Some(entity_name_from_path(path))),
        );
    }
    (OutputCardinality::Unknown, Vec::new(), schema.clone(), None)
}

fn wrapped_list_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    ["items", "data", "results", "rows"]
        .iter()
        .find_map(|name| {
            properties
                .get(*name)
                .filter(|property| property.get("type").and_then(Value::as_str) == Some("array"))
                .map(|property| (*name, property))
        })
        .or_else(|| single_array_payload_property(properties))
}

fn single_array_payload_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    let array_properties = properties
        .iter()
        .filter(|(_, property)| property.get("type").and_then(Value::as_str) == Some("array"))
        .filter(|(name, _)| !is_wrapper_metadata_property(name))
        .collect::<Vec<_>>();
    match array_properties.as_slice() {
        [(name, property)] => Some((name.as_str(), *property)),
        [] | [_, _, ..] => None,
    }
}

fn is_wrapper_metadata_property(name: &str) -> bool {
    matches!(
        name,
        "total_count" | "incomplete_results" | "has_more" | "next" | "previous"
    )
}

fn required_fields(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn detect_pagination(inputs: &[IrOperationInput]) -> PaginationSpec {
    let has_page = inputs
        .iter()
        .any(|input| input.location == OpenApiParameterLocation::Query && input.name == "page");
    let has_per_page = inputs
        .iter()
        .any(|input| input.location == OpenApiParameterLocation::Query && input.name == "per_page");
    if has_page && has_per_page {
        PaginationSpec {
            mode: PaginationMode::Page,
            page_size: Some(PageSizeSpec {
                default: 30,
                max: 100,
                query_param: Some("per_page".to_string()),
                body_path: Vec::new(),
            }),
            page_param: Some("page".to_string()),
            page_start: 1,
            page_step: 1,
            ..PaginationSpec::default()
        }
    } else {
        PaginationSpec::default()
    }
}

pub fn generate_projection_catalog(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
) -> Result<ProjectionCatalog> {
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    for ir in surfaces {
        for operation in &ir.operations {
            let projection = generate_projection(ir, operation, &mut diagnostics);
            projections.push(projection);
        }
        diagnostics.extend(ir.diagnostics.clone());
    }
    resolve_projection_name_collisions(manifest, surfaces, &mut projections);
    Ok(ProjectionCatalog {
        artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
        source_name: manifest.common.name.clone(),
        generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
        projections,
        diagnostics,
    })
}

fn generate_projection(
    ir: &SemanticIr,
    operation: &IrOperation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Projection {
    let is_search = is_search_operation(operation);
    let mut visibility = ProjectionVisibility::Published;
    let mut projection_diagnostics = operation.diagnostics.clone();
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    if !operation.read_only
        || rest.method != HttpMethod::Get
        || rest.request_body.is_some()
        || matches!(
            operation.output.cardinality,
            OutputCardinality::None | OutputCardinality::Unknown
        )
    {
        visibility = ProjectionVisibility::Hidden;
    }

    let function_kind = if is_search {
        Some(SourceTableFunctionKind::Search)
    } else if operation.output.cardinality == OutputCardinality::Singleton
        && operation.inputs.iter().any(|input| input.required)
    {
        Some(SourceTableFunctionKind::Table)
    } else {
        None
    };
    let kind = function_kind.map_or(ProjectionKind::Table, |function_kind| {
        ProjectionKind::TableFunction { function_kind }
    });
    let sql_exposure = if matches!(kind, ProjectionKind::Table) {
        SqlInputExposure::Filter
    } else {
        SqlInputExposure::FunctionArg
    };
    let pagination_query_params = pagination_query_param_names(&rest.pagination);
    let inputs = operation
        .inputs
        .iter()
        .map(|input| {
            let (exposure, pagination_owned_query_input) =
                projection_input_sql_exposure(input, sql_exposure, &pagination_query_params);
            if exposure == SqlInputExposure::Internal
                && input.required
                && !pagination_owned_query_input
            {
                visibility = ProjectionVisibility::Hidden;
                projection_diagnostics.push(Diagnostic::warning(
                    "PROJECTION_INPUT_UNSUPPORTED",
                    format!(
                        "required {:?} input '{}' cannot be exposed in SQL",
                        input.location, input.name
                    ),
                    ir.surface_id.clone(),
                    Some(operation.id.clone()),
                ));
            }
            ProjectionInput {
                name: normalize_identifier(&input.name, "input"),
                sql_exposure: exposure,
                source_location: input.location,
                wire_name: input.name.clone(),
                required: input.required && input.default_value.is_none(),
                data_type: manifest_type(input.data_type),
                default_value: input.default_value.clone(),
                description: input.description.clone(),
            }
        })
        .collect::<Vec<_>>();
    let columns = projection_columns(ir, operation);
    let mut name = projection_name(operation, is_search);
    if name.is_empty() {
        name = normalize_identifier(&operation.id, "projection");
    }
    let guide = projection_guide(&kind, &inputs, &rest.pagination, is_search);
    let projection = Projection {
        name,
        kind,
        description: operation.description.clone(),
        guide,
        surface_id: ir.surface_id.clone(),
        operation_id: operation.id.clone(),
        visibility,
        inputs,
        columns,
        pagination: rest.pagination.clone(),
        search_limits: is_search.then_some(SearchLimitsSpec {
            default_top_k: 30,
            max_top_k: 100,
            max_calls_per_query: 100,
        }),
        detail_hints: Vec::new(),
        diagnostics: projection_diagnostics.clone(),
    };
    diagnostics.extend(projection_diagnostics);
    projection
}

fn projection_input_sql_exposure(
    input: &IrOperationInput,
    default_exposure: SqlInputExposure,
    pagination_query_params: &HashSet<&str>,
) -> (SqlInputExposure, bool) {
    let pagination_owned_query_input = input.location == OpenApiParameterLocation::Query
        && pagination_query_params.contains(input.name.as_str());
    let exposure = match input.location {
        OpenApiParameterLocation::Query if pagination_owned_query_input => {
            SqlInputExposure::Internal
        }
        OpenApiParameterLocation::Path | OpenApiParameterLocation::Query => default_exposure,
        OpenApiParameterLocation::Header
        | OpenApiParameterLocation::Cookie
        | OpenApiParameterLocation::Body => SqlInputExposure::Internal,
    };
    (exposure, pagination_owned_query_input)
}

fn pagination_query_param_names(pagination: &PaginationSpec) -> HashSet<&str> {
    let mut names = HashSet::new();
    if let Some(name) = pagination.page_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.offset_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.cursor_param.as_deref() {
        names.insert(name);
    }
    if let Some(page_size) = &pagination.page_size
        && let Some(name) = page_size.query_param.as_deref()
    {
        names.insert(name);
    }
    names
}

fn pagination_owns_input(input: &ProjectionInput, pagination_query_params: &HashSet<&str>) -> bool {
    input.source_location == OpenApiParameterLocation::Query
        && pagination_query_params.contains(input.wire_name.as_str())
}

fn resolve_projection_name_collisions(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
    projections: &mut [Projection],
) {
    let operations = surfaces
        .iter()
        .flat_map(|ir| {
            ir.operations
                .iter()
                .map(move |operation| ((ir.surface_id.as_str(), operation.id.as_str()), operation))
        })
        .collect::<HashMap<_, _>>();
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, projection) in projections.iter().enumerate() {
        groups
            .entry(projection.name.clone())
            .or_default()
            .push(index);
    }

    let mut keep_base_name = HashSet::new();
    for indexes in groups.values() {
        let keep = indexes
            .iter()
            .copied()
            .min_by_key(|index| {
                let projection = projections
                    .get(*index)
                    .expect("projection index came from projections");
                let operation = operations
                    .get(&(
                        projection.surface_id.as_str(),
                        projection.operation_id.as_str(),
                    ))
                    .copied();
                projection_name_priority(projection, operation, *index)
            })
            .expect("group has at least one projection");
        keep_base_name.insert(keep);
    }

    let mut used_names = HashSet::new();
    for index in keep_base_name.iter().copied() {
        if let Some(projection) = projections.get(index) {
            used_names.insert(projection.name.clone());
        }
    }

    for indexes in groups.values().filter(|indexes| indexes.len() > 1) {
        for index in indexes {
            if keep_base_name.contains(index) {
                continue;
            }
            let projection = projections
                .get(*index)
                .expect("projection index came from projections");
            let operation = operations.get(&(
                projection.surface_id.as_str(),
                projection.operation_id.as_str(),
            ));
            let base_name = projection.name.clone();
            let mut name = operation.map_or_else(
                || normalize_identifier(&projection.operation_id, "projection"),
                |operation| contextual_projection_name(&base_name, operation),
            );
            if name == base_name || used_names.contains(&name) {
                let suffix = stable_suffix(&format!(
                    "{}/{}/{}",
                    manifest.common.name, projection.surface_id, projection.operation_id
                ));
                name = format!("{name}__{suffix}");
            }
            used_names.insert(name.clone());
            let projection = projections
                .get_mut(*index)
                .expect("projection index came from projections");
            projection.name.clone_from(&name);
            projection.diagnostics.push(Diagnostic {
                code: "PROJECTION_NAME_COLLISION_RESOLVED".to_string(),
                severity: DiagnosticSeverity::Warning,
                message: format!("projection name collision resolved as '{name}'"),
                surface_id: Some(projection.surface_id.clone()),
                operation_id: Some(projection.operation_id.clone()),
                projection_name: Some(name),
            });
        }
    }
}

fn projection_name_priority(
    projection: &Projection,
    operation: Option<&IrOperation>,
    index: usize,
) -> (bool, bool, usize, usize, usize) {
    (
        projection.visibility != ProjectionVisibility::Published,
        !matches!(projection.kind, ProjectionKind::Table),
        operation.map_or(usize::MAX, required_input_count),
        operation.map_or(usize::MAX, rest_literal_path_depth),
        index,
    )
}

fn required_input_count(operation: &IrOperation) -> usize {
    operation
        .inputs
        .iter()
        .filter(|input| input.required && input.default_value.is_none())
        .count()
}

fn rest_literal_path_depth(operation: &IrOperation) -> usize {
    rest_literal_path_segments(operation).len()
}

fn contextual_projection_name(base_name: &str, operation: &IrOperation) -> String {
    let Some(context) = projection_path_context(operation) else {
        return normalize_identifier(&operation.id, base_name);
    };
    if base_name == context || base_name.starts_with(&format!("{context}_")) {
        base_name.to_string()
    } else {
        format!("{context}_{base_name}")
    }
}

fn projection_path_context(operation: &IrOperation) -> Option<String> {
    let mut segments = rest_literal_path_segments(operation);
    segments.pop();
    (!segments.is_empty()).then(|| segments.join("_"))
}

fn rest_literal_path_segments(operation: &IrOperation) -> Vec<String> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    rest.path_template
        .split('/')
        .filter_map(normalized_path_literal_segment)
        .collect()
}

fn normalized_path_literal_segment(segment: &str) -> Option<String> {
    if segment.is_empty() || segment.starts_with('{') {
        return None;
    }
    let normalized = normalize_identifier(segment, "path");
    (!normalized.is_empty()).then_some(normalized)
}

fn projection_guide(
    kind: &ProjectionKind,
    inputs: &[ProjectionInput],
    pagination: &PaginationSpec,
    is_search: bool,
) -> String {
    let exposed_inputs = inputs
        .iter()
        .filter(|input| input.sql_exposure != SqlInputExposure::Internal)
        .collect::<Vec<_>>();
    let required = exposed_inputs
        .iter()
        .filter(|input| input.required)
        .map(|input| input.name.as_str())
        .collect::<Vec<_>>();
    let optional = exposed_inputs
        .iter()
        .filter(|input| !input.required)
        .filter(|input| !matches!(input.name.as_str(), "page" | "per_page"))
        .map(|input| input.name.as_str())
        .take(3)
        .collect::<Vec<_>>();

    let mut sentences = Vec::new();
    if required.is_empty() {
        sentences.push(match kind {
            ProjectionKind::Table => "Works without WHERE filters.".to_string(),
            ProjectionKind::TableFunction { .. } => "Takes no required arguments.".to_string(),
        });
    } else {
        let required = human_join(&required);
        sentences.push(match kind {
            ProjectionKind::Table => format!("Requires {required}."),
            ProjectionKind::TableFunction { .. } => format!("Requires {required} arguments."),
        });
    }

    if !optional.is_empty() {
        sentences.push(format!(
            "Most useful optional filters: {}.",
            optional.join(", ")
        ));
    }

    if is_search {
        sentences.push(
            "Use LIMIT to control result size; search endpoints can be rate-limited.".to_string(),
        );
    } else if pagination.mode != PaginationMode::None {
        sentences
            .push("Use LIMIT for spot checks; large result sets paginate quickly.".to_string());
    }

    sentences.join(" ")
}

fn human_join(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [one] => (*one).to_string(),
        [first, second] => format!("{first} and {second}"),
        [prefix @ .., last] => format!("{}, and {last}", prefix.join(", ")),
    }
}

fn projection_columns(ir: &SemanticIr, operation: &IrOperation) -> Vec<ProjectionColumn> {
    let type_by_id = ir
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
        .collect::<HashMap<_, _>>();
    let Some(row_type) = type_by_id.get(operation.output.type_ref.as_str()) else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: ManifestDataType::Json,
            source_path: Vec::new(),
            nullable: true,
            description: String::new(),
        }];
    };
    let IrTypeShape::Object { fields } = &row_type.shape else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: ManifestDataType::Json,
            source_path: Vec::new(),
            nullable: true,
            description: row_type.description.clone(),
        }];
    };
    let mut columns = Vec::new();
    let mut names = HashSet::new();
    for field in fields {
        let mut name = normalize_identifier(&field.name, "column");
        if !names.insert(name.clone()) {
            let suffix = stable_suffix(&field.name);
            name = format!("{name}__{suffix}");
        }
        let data_type =
            type_by_id
                .get(field.type_ref.as_str())
                .map_or(ManifestDataType::Json, |ty| match &ty.shape {
                    IrTypeShape::Scalar(scalar) => manifest_type(*scalar),
                    IrTypeShape::Enum { .. } => ManifestDataType::Utf8,
                    IrTypeShape::Json
                    | IrTypeShape::Object { .. }
                    | IrTypeShape::List { .. }
                    | IrTypeShape::Map { .. } => ManifestDataType::Json,
                });
        columns.push(ProjectionColumn {
            name,
            data_type,
            source_path: vec![field.name.clone()],
            nullable: true,
            description: field.description.clone(),
        });
    }
    columns
}

fn projection_name(operation: &IrOperation, is_search: bool) -> String {
    let entity = projection_entity_name(operation, is_search);
    if is_search {
        return format!("search_{}", pluralize(&entity));
    }
    match operation.output.cardinality {
        OutputCardinality::List | OutputCardinality::WrappedList => pluralize(&entity),
        OutputCardinality::Singleton if operation.inputs.iter().any(|input| input.required) => {
            format!("get_{}", singularize(&entity))
        }
        OutputCardinality::Singleton => singularize(&entity),
        OutputCardinality::None | OutputCardinality::Unknown => {
            normalize_identifier(&operation.id, "projection")
        }
    }
}

fn projection_entity_name(operation: &IrOperation, is_search: bool) -> String {
    if is_search && let Some(search_entity) = search_entity_from_path(operation) {
        return search_entity;
    }
    operation.entity.as_ref().map_or_else(
        || normalize_identifier(&operation.id, "projection"),
        |entity| normalize_entity_identifier(&entity.name),
    )
}

fn search_entity_from_path(operation: &IrOperation) -> Option<String> {
    rest_literal_path_segments(operation)
        .into_iter()
        .next_back()
        .map(|segment| singularize(&segment))
}

fn normalize_entity_identifier(raw: &str) -> String {
    let normalized = normalize_identifier(&entity_identifier_seed(raw), "projection");
    let mut tokens = normalized.split('_').collect::<Vec<_>>();
    tokens.retain(|token| !matches!(*token, "minimal" | "simple" | "base" | "short"));
    if tokens.is_empty() {
        normalized
    } else {
        tokens.join("_")
    }
}

fn entity_identifier_seed(raw: &str) -> String {
    let mut seed = String::new();
    let mut previous_was_lowercase_or_digit = false;
    for ch in raw.chars() {
        if ch.is_ascii_uppercase() && previous_was_lowercase_or_digit {
            seed.push('_');
        }
        if ch == '-' || ch == ' ' {
            seed.push('_');
            previous_was_lowercase_or_digit = false;
        } else {
            seed.push(ch.to_ascii_lowercase());
            previous_was_lowercase_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    seed
}

fn is_search_operation(operation: &IrOperation) -> bool {
    let id_tokens = operation.id.split('_').collect::<Vec<_>>();
    let path_has_search = match &operation.execution {
        IrExecutionAttachment::Rest(rest) => rest
            .path_template
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token.eq_ignore_ascii_case("search")),
    };
    path_has_search
        || id_tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("search"))
}

fn manifest_type(scalar: IrScalarType) -> ManifestDataType {
    match scalar {
        IrScalarType::String | IrScalarType::Id => ManifestDataType::Utf8,
        IrScalarType::Integer => ManifestDataType::Int64,
        IrScalarType::Number => ManifestDataType::Float64,
        IrScalarType::Boolean => ManifestDataType::Boolean,
        IrScalarType::Timestamp => ManifestDataType::Timestamp,
        IrScalarType::Json => ManifestDataType::Json,
    }
}

pub fn projection_filter_specs(projection: &Projection) -> Vec<FilterSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| FilterSpec {
            name: input.name.clone(),
            data_type: manifest_data_type_name(input.data_type).to_string(),
            required: input.required,
            mode: FilterMode::Equality,
            description: input.description.clone(),
        })
        .collect()
}

pub fn projection_arg_specs(projection: &Projection) -> Vec<TableFunctionArgSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| TableFunctionArgSpec {
            name: input.name.clone(),
            required: input.required,
            values: Vec::new(),
            bind: FunctionArgBinding {
                arg: input.name.clone(),
            },
        })
        .collect()
}

pub fn projection_column_specs(projection: &Projection) -> Vec<ColumnSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut columns = projection
        .columns
        .iter()
        .map(|column| ColumnSpec {
            name: column.name.clone(),
            data_type: manifest_data_type_name(column.data_type).to_string(),
            nullable: column.nullable,
            r#virtual: false,
            description: column.description.clone(),
            expr: Some(ExprSpec::Path {
                path: column.source_path.clone(),
            }),
        })
        .collect::<Vec<_>>();
    let existing = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    columns.extend(
        projection
            .inputs
            .iter()
            .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
            .filter(|input| !pagination_owns_input(input, &pagination_query_params))
            .filter(|input| !existing.contains(&input.name))
            .map(|input| ColumnSpec {
                name: input.name.clone(),
                data_type: manifest_data_type_name(input.data_type).to_string(),
                nullable: !input.required,
                r#virtual: true,
                description: input.description.clone(),
                expr: Some(ExprSpec::FromFilter {
                    key: input.name.clone(),
                }),
            }),
    );
    columns
}

pub fn manifest_data_type_name(data_type: ManifestDataType) -> &'static str {
    match data_type {
        ManifestDataType::Utf8 => "Utf8",
        ManifestDataType::Int64 => "Int64",
        ManifestDataType::Boolean => "Boolean",
        ManifestDataType::Float64 => "Float64",
        ManifestDataType::Timestamp => "Timestamp",
        ManifestDataType::Json => "Json",
    }
}

pub fn request_spec_for_projection(
    projection: &Projection,
    operation: &IrOperation,
) -> Result<RequestSpec> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut path = rest.path_template.clone();
    for input in &projection.inputs {
        if input.source_location == OpenApiParameterLocation::Path {
            let replacement = match input.sql_exposure {
                SqlInputExposure::Filter => format!("{{{{filter.{}}}}}", input.name),
                SqlInputExposure::FunctionArg => format!("{{{{arg.{}}}}}", input.name),
                SqlInputExposure::Internal => continue,
            };
            path = path.replace(&format!("{{{}}}", input.wire_name), &replacement);
        }
    }
    let query = projection
        .inputs
        .iter()
        .filter(|input| input.source_location == OpenApiParameterLocation::Query)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .filter_map(|input| {
            let value = match input.sql_exposure {
                SqlInputExposure::Filter => crate::ValueSourceSpec::Filter {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::FunctionArg => crate::ValueSourceSpec::Arg {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::Internal => return None,
            };
            Some(crate::QueryParamSpec {
                name: input.wire_name.clone(),
                value,
            })
        })
        .collect();
    Ok(RequestSpec {
        method: crate::HttpMethod::GET,
        path: ParsedTemplate::parse(&path)?,
        query,
        body: crate::BodySpec::default(),
        headers: Vec::new(),
    })
}

pub fn validate_materialized_source(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<()> {
    if materialized.fingerprint.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(ManifestError::validation(
            "DSL v4 materialized artifact schema version mismatch",
        ));
    }
    if materialized.fingerprint.source_name != manifest.common.name {
        return Err(ManifestError::validation(format!(
            "DSL v4 materialized source identity mismatch for '{}'",
            manifest.common.name
        )));
    }
    if materialized.fingerprint.importer_version != OPENAPI_IMPORTER_VERSION
        || materialized.fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(ManifestError::validation(
            "DSL v4 materialized importer or generator version mismatch",
        ));
    }
    for surface in &manifest.surfaces {
        if !materialized
            .surfaces
            .iter()
            .any(|materialized_surface| materialized_surface.surface_id == surface.id)
        {
            return Err(ManifestError::validation(format!(
                "DSL v4 materialized surface '{}' is missing",
                surface.id
            )));
        }
    }
    Ok(())
}

fn fallback_operation_id(method: &str, path: &str) -> String {
    normalize_identifier(
        &format!("{method}_{}", path.replace(['{', '}'], "")),
        "operation",
    )
}

pub fn normalize_identifier(value: &str, prefix: &str) -> String {
    let mut output = String::new();
    let mut last_underscore = false;
    for c in value.chars() {
        if c.is_ascii_alphanumeric() {
            output.push(c.to_ascii_lowercase());
            last_underscore = false;
        } else if !last_underscore {
            output.push('_');
            last_underscore = true;
        }
    }
    let output = output.trim_matches('_').to_string();
    if output.is_empty() || output.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("{prefix}_{output}")
    } else {
        output
    }
}

fn entity_name_from_ref(reference: &str) -> String {
    reference
        .rsplit('/')
        .next()
        .map_or_else(|| "entity".to_string(), |raw| raw.replace(" Response", ""))
}

fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}

fn entity_name_from_path(path: &str) -> String {
    path.split('/')
        .rfind(|segment| !segment.is_empty() && !segment.starts_with('{'))
        .unwrap_or("entity")
        .to_string()
}

fn singularize(value: &str) -> String {
    if let Some(stem) = value.strip_suffix("ies")
        && !stem.is_empty()
    {
        return format!("{stem}y");
    }
    for suffix in ["ches", "shes", "xes", "ses"] {
        if let Some(stem) = value.strip_suffix(suffix)
            && !stem.is_empty()
        {
            return format!("{stem}{}", suffix.trim_end_matches("es"));
        }
    }
    if value.ends_with('s')
        && !value.ends_with("ss")
        && !value.ends_with("us")
        && !value.ends_with("ics")
        && value != "news"
    {
        return value.trim_end_matches('s').to_string();
    }
    value.to_string()
}

fn pluralize(value: &str) -> String {
    if value.ends_with('s') {
        value.to_string()
    } else if let Some(stem) = value.strip_suffix('y') {
        if stem
            .chars()
            .next_back()
            .is_some_and(|c| !"aeiou".contains(c))
        {
            format!("{stem}ies")
        } else {
            format!("{value}s")
        }
    } else if value.ends_with('x') || value.ends_with("ch") || value.ends_with("sh") {
        format!("{value}es")
    } else {
        format!("{value}s")
    }
}

fn stable_suffix(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}").chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_source_manifest_yaml;

    #[test]
    fn parses_v4_manifest_and_unions_surface_inputs() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ZZZ_TOKEN:
        kind: secret
      AAA_BASE:
        kind: variable
        default: https://api.example.com
    base_url: "{{input.AAA_BASE}}"
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: template
          template: Bearer {{input.ZZZ_TOKEN}}
"#,
        )
        .expect("v4 manifest");
        assert_eq!(manifest.dsl_version(), 4);
        assert!(manifest.as_v4().is_some());
        assert_eq!(manifest.declared_inputs().len(), 2);
        let keys = manifest
            .declared_inputs()
            .iter()
            .map(|input| input.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["ZZZ_TOKEN", "AAA_BASE"]);
    }

    #[test]
    fn parses_v4_openapi_surface_without_base_url() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
        )
        .expect("v4 manifest");
        let v4 = manifest.as_v4().expect("v4");
        assert_eq!(
            v4.surfaces
                .first()
                .expect("surface")
                .openapi_runtime
                .base_url
                .raw(),
            ""
        );
    }

    #[test]
    fn extracts_openapi_document_metadata() {
        let metadata = openapi_document_metadata(
            r"
openapi: 3.0.3
info:
  title: Demo
  description: Query demo data.
servers:
  - url: https://api.example.com/v1
paths: {}
"
            .as_bytes(),
        )
        .expect("metadata");
        assert_eq!(metadata.description.as_deref(), Some("Query demo data."));
        assert_eq!(
            metadata.server_url.as_deref(),
            Some("https://api.example.com/v1")
        );
    }

    #[test]
    fn extracts_openapi_server_url_with_variable_defaults() {
        let metadata = openapi_document_metadata(
            r"
openapi: 3.0.1
info:
  title: StatusGator
  version: v3
servers:
  - url: https://{defaultHost}/api/v3
    variables:
      defaultHost:
        default: statusgator.com
paths: {}
"
            .as_bytes(),
        )
        .expect("metadata");
        assert_eq!(
            metadata.server_url.as_deref(),
            Some("https://statusgator.com/api/v3")
        );
    }

    #[test]
    fn imports_and_generates_github_issue_slice() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      GITHUB_API_BASE:
        kind: variable
        default: https://api.github.com
    base_url: "{{input.GITHUB_API_BASE}}"
"#,
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
        let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
        let published = catalog
            .projections
            .iter()
            .filter(|projection| projection.visibility == ProjectionVisibility::Published)
            .map(|projection| projection.name.as_str())
            .collect::<Vec<_>>();
        assert!(published.contains(&"issues"), "{published:?}");
        assert!(published.contains(&"search_issues"), "{published:?}");
        assert!(published.contains(&"get_issue"), "{published:?}");
    }

    #[test]
    fn projection_generation_keeps_pagination_inputs_internal() {
        let manifest = parse_source_manifest_yaml(
            r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
        let catalog = generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("catalog");
        let projection = catalog
            .projections
            .iter()
            .find(|projection| projection.operation_id == "issues_list_for_repo")
            .expect("repo issues projection");
        let operation = ir
            .operations
            .iter()
            .find(|operation| operation.id == projection.operation_id)
            .expect("repo issues operation");

        assert_eq!(projection.pagination.mode, PaginationMode::Page);
        assert_eq!(projection.pagination.page_param.as_deref(), Some("page"));
        assert_eq!(
            projection
                .pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            Some("per_page")
        );

        let exposures = projection
            .inputs
            .iter()
            .map(|input| (input.wire_name.as_str(), input.sql_exposure))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(exposures.get("owner"), Some(&SqlInputExposure::Filter));
        assert_eq!(exposures.get("repo"), Some(&SqlInputExposure::Filter));
        assert_eq!(exposures.get("state"), Some(&SqlInputExposure::Filter));
        assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Internal));
        assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

        let filter_names = projection_filter_specs(projection)
            .into_iter()
            .map(|filter| filter.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            filter_names,
            BTreeSet::from(["owner".to_string(), "repo".to_string(), "state".to_string()])
        );

        let column_names = projection_column_specs(projection)
            .into_iter()
            .map(|column| column.name)
            .collect::<BTreeSet<_>>();
        assert!(column_names.contains("owner"));
        assert!(column_names.contains("repo"));
        assert!(column_names.contains("state"));
        assert!(!column_names.contains("page"));
        assert!(!column_names.contains("per_page"));

        let request = request_spec_for_projection(projection, operation).expect("request");
        let query_names = request
            .query
            .iter()
            .map(|param| param.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(query_names, BTreeSet::from(["state"]));

        let mut stale_projection = projection.clone();
        for input in &mut stale_projection.inputs {
            if matches!(input.wire_name.as_str(), "page" | "per_page") {
                input.sql_exposure = SqlInputExposure::Filter;
            }
        }
        let stale_filter_names = projection_filter_specs(&stale_projection)
            .into_iter()
            .map(|filter| filter.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(stale_filter_names, filter_names);

        let stale_request =
            request_spec_for_projection(&stale_projection, operation).expect("stale request");
        let stale_query_names = stale_request
            .query
            .iter()
            .map(|param| param.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(stale_query_names, query_names);

        for input in &mut stale_projection.inputs {
            if matches!(input.wire_name.as_str(), "page" | "per_page") {
                input.sql_exposure = SqlInputExposure::FunctionArg;
            }
        }
        let stale_arg_names = projection_arg_specs(&stale_projection)
            .into_iter()
            .map(|arg| arg.name)
            .collect::<BTreeSet<_>>();
        assert!(!stale_arg_names.contains("page"));
        assert!(!stale_arg_names.contains("per_page"));
    }

    #[test]
    fn importer_recognizes_common_wrapped_list_response_fields() {
        let manifest = parse_source_manifest_yaml(
            r"
name: statusgator
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /boards/{board_id}/incidents:
    get:
      operationId: listIncidents
      parameters:
        - {name: board_id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  success: {type: boolean}
                  data:
                    type: array
                    items: {$ref: '#/components/schemas/Incident'}
                  pagination:
                    type: object
components:
  schemas:
    Incident:
      type: object
      properties:
        id: {type: string}
        name: {type: string}
"
            .as_bytes(),
        )
        .expect("import");
        let operation = ir.operations.first().expect("operation");
        assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
        assert_eq!(operation.output.row_path, vec!["data".to_string()]);

        let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
        let projection = catalog
            .projections
            .iter()
            .find(|projection| projection.operation_id == "listincidents")
            .expect("projection");
        assert_eq!(projection.name, "incidents");
        assert!(matches!(projection.kind, ProjectionKind::Table));
    }

    #[test]
    fn importer_recognizes_single_array_payload_wrappers() {
        let manifest = parse_source_manifest_yaml(
            r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /orgs/{org}/actions/permissions/repositories:
    get:
      operationId: actions/list-selected-repositories-enabled-github-actions-organization
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  total_count: {type: integer}
                  repositories:
                    type: array
                    items: {$ref: '#/components/schemas/Repository'}
components:
  schemas:
    Repository:
      type: object
      properties:
        id: {type: integer}
        name: {type: string}
"
            .as_bytes(),
        )
        .expect("import");
        let operation = ir.operations.first().expect("operation");
        assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
        assert_eq!(operation.output.row_path, vec!["repositories".to_string()]);

        let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
        let projection = catalog.projections.first().expect("projection");
        assert_eq!(projection.name, "repositories");
        assert!(matches!(projection.kind, ProjectionKind::Table));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "The OpenAPI fixture keeps related collision cases together."
    )]
    fn projection_names_use_path_context_for_collisions() {
        let manifest = parse_source_manifest_yaml(
            r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /orgs/{org}/issues:
    get:
      operationId: issues/list-for-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/pulls:
    get:
      operationId: pulls/list
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/PullRequestSimple'}}
  /repos/{owner}/{repo}/commits:
    get:
      operationId: repos/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
  /repos/{owner}/{repo}/pulls/{pull_number}/commits:
    get:
      operationId: pulls/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: pull_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
components:
  schemas:
    Issue:
      type: object
      properties:
        id: {type: integer}
    PullRequestSimple:
      type: object
      properties:
        id: {type: integer}
    Commit:
      type: object
      properties:
        sha: {type: string}
"
            .as_bytes(),
        )
        .expect("import");
        let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
        let names_by_operation = catalog
            .projections
            .iter()
            .map(|projection| {
                (
                    projection.operation_id.as_str(),
                    (projection.name.as_str(), &projection.kind),
                )
            })
            .collect::<HashMap<_, _>>();

        let issues_list = names_by_operation
            .get("issues_list")
            .expect("issues_list projection");
        assert_eq!(issues_list.0, "issues");
        let org_issues = names_by_operation
            .get("issues_list_for_org")
            .expect("issues_list_for_org projection");
        assert_eq!(org_issues.0, "orgs_issues");
        let repo_issues = names_by_operation
            .get("issues_list_for_repo")
            .expect("issues_list_for_repo projection");
        assert_eq!(repo_issues.0, "repos_issues");
        let pulls = names_by_operation
            .get("pulls_list")
            .expect("pulls_list projection");
        assert_eq!(pulls.0, "pull_requests");
        assert!(matches!(pulls.1, ProjectionKind::Table));
        let commits = names_by_operation
            .get("repos_list_commits")
            .expect("repos_list_commits projection");
        assert_eq!(commits.0, "commits");
        let pull_commits = names_by_operation
            .get("pulls_list_commits")
            .expect("pulls_list_commits projection");
        assert_eq!(pull_commits.0, "repos_pulls_commits");
    }

    #[test]
    fn importer_handles_recursive_schema_refs() {
        let manifest = parse_source_manifest_yaml(
            r"
name: trees
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /trees/{id}:
    get:
      operationId: trees/get
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/tree'}
components:
  schemas:
    tree:
      type: object
      properties:
        id: {type: string}
        children:
          type: array
          items: {$ref: '#/components/schemas/tree'}
"
            .as_bytes(),
        )
        .expect("recursive schema imports");
        assert!(ir.types.iter().any(|ty| ty.id == "tree"));
    }

    #[test]
    fn importer_preserves_non_string_parameter_defaults() {
        let manifest = parse_source_manifest_yaml(
            r"
name: defaults
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
        - {name: archived, in: query, schema: {type: boolean, default: false}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
"
            .as_bytes(),
        )
        .expect("defaults import");
        let operation = ir.operations.first().expect("operation");
        let defaults = operation
            .inputs
            .iter()
            .map(|input| (input.name.as_str(), input.default_value.as_deref()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(defaults.get("per_page"), Some(&Some("30")));
        assert_eq!(defaults.get("archived"), Some(&Some("false")));
    }

    #[test]
    fn importer_warns_for_invalid_parameters_and_unresolved_responses() {
        let manifest = parse_source_manifest_yaml(
            r"
name: broken
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("one surface");
        let ir = import_openapi_surface(
            v4,
            surface,
            r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/missing'}
"
            .as_bytes(),
        )
        .expect("broken schema imports with diagnostics");
        let operation = ir.operations.first().expect("operation");
        let codes = operation
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect::<Vec<_>>();
        assert!(codes.contains(&"OPENAPI_PARAMETER_INVALID"), "{codes:?}");
        assert!(
            codes.contains(&"OPENAPI_RESPONSE_SCHEMA_UNRESOLVED"),
            "{codes:?}"
        );
        assert_eq!(operation.output.cardinality, OutputCardinality::Unknown);
    }

    #[test]
    fn projection_names_avoid_obvious_bad_singulars() {
        assert_eq!(singularize("status"), "status");
        assert_eq!(singularize("news"), "news");
        assert_eq!(singularize("analytics"), "analytics");
        assert_eq!(singularize("addresses"), "address");
        assert_eq!(pluralize("box"), "boxes");
    }

    fn github_openapi() -> &'static str {
        r"
openapi: 3.0.3
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
        - {name: page, in: query, schema: {type: integer}}
        - {name: per_page, in: query, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/issue'}
  /search/issues:
    get:
      operationId: search/issues-and-pull-requests
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
        - {name: sort, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  items:
                    type: array
                    items: {$ref: '#/components/schemas/issue'}
  /repos/{owner}/{repo}/issues/{issue_number}:
    get:
      operationId: issues/get
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: issue_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
    patch:
      operationId: issues/update
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                title: {type: string}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
components:
  schemas:
    issue:
      type: object
      properties:
        id: {type: integer}
        number: {type: integer}
        title: {type: string}
        state: {type: string}
        html_url: {type: string}
        created_at: {type: string, format: date-time}
        updated_at: {type: string, format: date-time}
        body: {type: string}
        user: {type: object}
"
    }
}
