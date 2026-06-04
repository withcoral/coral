use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::rest::RestExecutionAttachment;
use crate::v4::manifest::SurfaceType;

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
