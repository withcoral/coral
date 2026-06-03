use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    DetailHintSpec, ManifestDataType, PaginationSpec, ResponseSpec, SearchLimitsSpec,
    SourceTableFunctionKind,
};

use super::manifest::SurfaceType;

pub const V4_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const OPENAPI_IMPORTER_VERSION: &str = "openapi-v2";
pub const PROJECTION_GENERATOR_VERSION: &str = "derive-read-v3";

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
    pub(super) fn warning(
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
