//! Stable on-disk representations for DSL v4 semantic IR and projections.
//!
//! These types deliberately duplicate the runtime models. Persisted YAML is a
//! compatibility contract; runtime models may evolve only through the explicit
//! mappings in this module.

use serde::{Deserialize, Serialize};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::{
    Diagnostic, HttpMethod, IrEntityCandidate, IrExecutionAttachment, IrField, IrInputLocation,
    IrOperation, IrOperationInput, IrOperationNaming, IrOperationOutput, IrScalarType, IrType,
    IrTypeShape, McpExecutionAttachment, OutputCardinality, Projection, ProjectionCatalog,
    ProjectionColumn, ProjectionInput, ProjectionKind, ProjectionVisibility,
    RestExecutionAttachment, RestParameterBinding, RestRequestBody, RestResponseAttachment,
    SemanticIr, SqlInputExposure, SurfaceType,
};
use crate::{
    DetailHintSpec, ManifestDataType, PaginationSpec, ResponseSpec, SearchLimitsSpec,
    SourceTableFunctionKind,
};

#[derive(Debug, thiserror::Error)]
#[error("failed to migrate {artifact} at {path}: {detail}")]
pub struct ArtifactMigrationError {
    pub artifact: &'static str,
    pub path: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticIrFile {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub importer_version: String,
    pub operations: Vec<IrOperationFile>,
    pub types: Vec<IrTypeFile>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationFile {
    pub id: String,
    pub method_name: String,
    pub description: String,
    pub deprecated: bool,
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub naming: Option<IrOperationNamingFile>,
    pub inputs: Vec<IrOperationInputFile>,
    pub output: IrOperationOutputFile,
    pub entity: Option<IrEntityCandidateFile>,
    pub execution: IrExecutionAttachmentFile,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationNamingFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationInputFile {
    pub name: String,
    pub location: IrInputLocation,
    pub required: bool,
    pub data_type: IrScalarType,
    pub default_value: Option<String>,
    pub description: String,
    #[serde(default)]
    pub exclude_from_lookup_keys: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationOutputFile {
    pub cardinality: OutputCardinality,
    pub type_ref: String,
    pub row_path: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrEntityCandidateFile {
    pub name: String,
    pub type_ref: String,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrTypeFile {
    pub id: String,
    pub shape: IrTypeShapeFile,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrTypeShapeFile {
    Scalar(IrScalarType),
    Object { fields: Vec<IrFieldFile> },
    List { item_type_ref: String },
    Map { value_type_ref: String },
    Enum { values: Vec<String> },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrFieldFile {
    pub name: String,
    pub type_ref: String,
    pub required: bool,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrExecutionAttachmentFile {
    Rest(Box<RestExecutionAttachmentFile>),
    Mcp(McpExecutionAttachmentFile),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestExecutionAttachmentFile {
    pub method: HttpMethod,
    pub path_template: String,
    pub parameters: Vec<RestParameterBindingFile>,
    pub request_body: Option<RestRequestBodyFile>,
    pub response: RestResponseAttachmentFile,
    pub pagination: PaginationSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestRequestBodyFile {
    pub required: bool,
    pub media_type: String,
    pub type_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestParameterBindingFile {
    pub input_name: String,
    pub location: IrInputLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: IrScalarType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponseAttachmentFile {
    pub status_code: u16,
    pub media_type: String,
    pub response: ResponseSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachmentFile {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<McpPaginationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_pagination: Option<McpOffsetPaginationSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalogFile {
    pub artifact_schema_version: u32,
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator_version: Option<String>,
    pub projections: Vec<ProjectionFile>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionFile {
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    pub kind: ProjectionKindFile,
    pub description: String,
    pub guide: String,
    pub surface_id: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInputFile>,
    pub columns: Vec<ProjectionColumnFile>,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ProjectionKindFile {
    Table,
    TableFunction {
        function_kind: SourceTableFunctionKind,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionInputFile {
    pub name: String,
    pub sql_exposure: SqlInputExposure,
    pub source_location: IrInputLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: ManifestDataType,
    pub default_value: Option<String>,
    pub description: String,
    #[serde(default)]
    pub lookup_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionColumnFile {
    pub name: String,
    pub data_type: ManifestDataType,
    pub source_path: Vec<String>,
    pub nullable: bool,
    pub description: String,
}

impl From<&SemanticIr> for SemanticIrFile {
    fn from(value: &SemanticIr) -> Self {
        Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name.clone(),
            surface_id: value.surface_id.clone(),
            surface_type: value.surface_type,
            importer_version: value.importer_version.clone(),
            operations: value.operations.iter().map(IrOperationFile::from).collect(),
            types: value.types.iter().map(IrTypeFile::from).collect(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl TryFrom<SemanticIrFile> for SemanticIr {
    type Error = ArtifactMigrationError;

    fn try_from(value: SemanticIrFile) -> Result<Self, Self::Error> {
        Ok(Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name,
            surface_id: value.surface_id,
            surface_type: value.surface_type,
            importer_version: value.importer_version,
            operations: value
                .operations
                .into_iter()
                .map(IrOperation::from)
                .collect(),
            types: value.types.into_iter().map(IrType::from).collect(),
            diagnostics: value.diagnostics,
        })
    }
}

impl From<&ProjectionCatalog> for ProjectionCatalogFile {
    fn from(value: &ProjectionCatalog) -> Self {
        Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name.clone(),
            generator_version: value.generator_version.clone(),
            projections: value.projections.iter().map(ProjectionFile::from).collect(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl TryFrom<ProjectionCatalogFile> for ProjectionCatalog {
    type Error = ArtifactMigrationError;

    fn try_from(value: ProjectionCatalogFile) -> Result<Self, Self::Error> {
        Ok(Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name,
            generator_version: value.generator_version,
            projections: value
                .projections
                .into_iter()
                .map(Projection::from)
                .collect(),
            diagnostics: value.diagnostics,
        })
    }
}

impl From<&IrOperation> for IrOperationFile {
    fn from(value: &IrOperation) -> Self {
        Self {
            id: value.id.clone(),
            method_name: value.method_name.clone(),
            description: value.description.clone(),
            deprecated: value.deprecated,
            read_only: value.read_only,
            naming: value.naming.as_ref().map(IrOperationNamingFile::from),
            inputs: value
                .inputs
                .iter()
                .map(IrOperationInputFile::from)
                .collect(),
            output: IrOperationOutputFile::from(&value.output),
            entity: value.entity.as_ref().map(IrEntityCandidateFile::from),
            execution: IrExecutionAttachmentFile::from(&value.execution),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl From<IrOperationFile> for IrOperation {
    fn from(value: IrOperationFile) -> Self {
        Self {
            id: value.id,
            method_name: value.method_name,
            description: value.description,
            deprecated: value.deprecated,
            read_only: value.read_only,
            naming: value.naming.map(IrOperationNaming::from),
            inputs: value
                .inputs
                .into_iter()
                .map(IrOperationInput::from)
                .collect(),
            output: value.output.into(),
            entity: value.entity.map(IrEntityCandidate::from),
            execution: value.execution.into(),
            diagnostics: value.diagnostics,
        }
    }
}

macro_rules! copy_struct {
    ($file:ty => $runtime:ty { $($field:ident),+ $(,)? }) => {
        impl From<&$runtime> for $file {
            fn from(value: &$runtime) -> Self {
                Self { $($field: value.$field.clone()),+ }
            }
        }
        impl From<$file> for $runtime {
            fn from(value: $file) -> Self {
                Self { $($field: value.$field),+ }
            }
        }
    };
}

copy_struct!(IrOperationNamingFile => IrOperationNaming { group, operation });
copy_struct!(IrOperationInputFile => IrOperationInput {
    name, location, required, data_type, default_value, description, exclude_from_lookup_keys
});
copy_struct!(IrOperationOutputFile => IrOperationOutput { cardinality, type_ref, row_path });
copy_struct!(IrEntityCandidateFile => IrEntityCandidate { name, type_ref, identity_fields });
copy_struct!(IrFieldFile => IrField { name, type_ref, required, nullable, description });
copy_struct!(RestRequestBodyFile => RestRequestBody { required, media_type, type_ref });
copy_struct!(RestParameterBindingFile => RestParameterBinding {
    input_name, location, wire_name, required, data_type
});
copy_struct!(RestResponseAttachmentFile => RestResponseAttachment {
    status_code, media_type, response
});
copy_struct!(McpExecutionAttachmentFile => McpExecutionAttachment {
    tool_name, pagination, offset_pagination
});
copy_struct!(ProjectionInputFile => ProjectionInput {
    name, sql_exposure, source_location, wire_name, required, data_type, default_value,
    description, lookup_key
});
copy_struct!(ProjectionColumnFile => ProjectionColumn {
    name, data_type, source_path, nullable, description
});

impl From<&IrType> for IrTypeFile {
    fn from(value: &IrType) -> Self {
        Self {
            id: value.id.clone(),
            shape: IrTypeShapeFile::from(&value.shape),
            nullable: value.nullable,
            description: value.description.clone(),
        }
    }
}

impl From<IrTypeFile> for IrType {
    fn from(value: IrTypeFile) -> Self {
        Self {
            id: value.id,
            shape: value.shape.into(),
            nullable: value.nullable,
            description: value.description,
        }
    }
}

impl From<&IrTypeShape> for IrTypeShapeFile {
    fn from(value: &IrTypeShape) -> Self {
        match value {
            IrTypeShape::Scalar(value) => Self::Scalar(*value),
            IrTypeShape::Object { fields } => Self::Object {
                fields: fields.iter().map(IrFieldFile::from).collect(),
            },
            IrTypeShape::List { item_type_ref } => Self::List {
                item_type_ref: item_type_ref.clone(),
            },
            IrTypeShape::Map { value_type_ref } => Self::Map {
                value_type_ref: value_type_ref.clone(),
            },
            IrTypeShape::Enum { values } => Self::Enum {
                values: values.clone(),
            },
            IrTypeShape::Json => Self::Json,
        }
    }
}

impl From<IrTypeShapeFile> for IrTypeShape {
    fn from(value: IrTypeShapeFile) -> Self {
        match value {
            IrTypeShapeFile::Scalar(value) => Self::Scalar(value),
            IrTypeShapeFile::Object { fields } => Self::Object {
                fields: fields.into_iter().map(IrField::from).collect(),
            },
            IrTypeShapeFile::List { item_type_ref } => Self::List { item_type_ref },
            IrTypeShapeFile::Map { value_type_ref } => Self::Map { value_type_ref },
            IrTypeShapeFile::Enum { values } => Self::Enum { values },
            IrTypeShapeFile::Json => Self::Json,
        }
    }
}

impl From<&IrExecutionAttachment> for IrExecutionAttachmentFile {
    fn from(value: &IrExecutionAttachment) -> Self {
        match value {
            IrExecutionAttachment::Rest(value) => {
                Self::Rest(Box::new(RestExecutionAttachmentFile::from(value.as_ref())))
            }
            IrExecutionAttachment::Mcp(value) => Self::Mcp(McpExecutionAttachmentFile::from(value)),
        }
    }
}

impl From<IrExecutionAttachmentFile> for IrExecutionAttachment {
    fn from(value: IrExecutionAttachmentFile) -> Self {
        match value {
            IrExecutionAttachmentFile::Rest(value) => Self::Rest(Box::new((*value).into())),
            IrExecutionAttachmentFile::Mcp(value) => Self::Mcp(value.into()),
        }
    }
}

impl From<&RestExecutionAttachment> for RestExecutionAttachmentFile {
    fn from(value: &RestExecutionAttachment) -> Self {
        Self {
            method: value.method,
            path_template: value.path_template.clone(),
            parameters: value
                .parameters
                .iter()
                .map(RestParameterBindingFile::from)
                .collect(),
            request_body: value.request_body.as_ref().map(RestRequestBodyFile::from),
            response: RestResponseAttachmentFile::from(&value.response),
            pagination: value.pagination.clone(),
        }
    }
}

impl From<RestExecutionAttachmentFile> for RestExecutionAttachment {
    fn from(value: RestExecutionAttachmentFile) -> Self {
        Self {
            method: value.method,
            path_template: value.path_template,
            parameters: value
                .parameters
                .into_iter()
                .map(RestParameterBinding::from)
                .collect(),
            request_body: value.request_body.map(RestRequestBody::from),
            response: value.response.into(),
            pagination: value.pagination,
        }
    }
}

impl From<&Projection> for ProjectionFile {
    fn from(value: &Projection) -> Self {
        Self {
            name: value.name.clone(),
            namespace: value.namespace.clone(),
            kind: ProjectionKindFile::from(&value.kind),
            description: value.description.clone(),
            guide: value.guide.clone(),
            surface_id: value.surface_id.clone(),
            operation_id: value.operation_id.clone(),
            visibility: value.visibility,
            inputs: value.inputs.iter().map(ProjectionInputFile::from).collect(),
            columns: value
                .columns
                .iter()
                .map(ProjectionColumnFile::from)
                .collect(),
            search_limits: value.search_limits.clone(),
            detail_hints: value.detail_hints.clone(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl From<ProjectionFile> for Projection {
    fn from(value: ProjectionFile) -> Self {
        Self {
            name: value.name,
            namespace: value.namespace,
            kind: value.kind.into(),
            description: value.description,
            guide: value.guide,
            surface_id: value.surface_id,
            operation_id: value.operation_id,
            visibility: value.visibility,
            inputs: value
                .inputs
                .into_iter()
                .map(ProjectionInput::from)
                .collect(),
            columns: value
                .columns
                .into_iter()
                .map(ProjectionColumn::from)
                .collect(),
            search_limits: value.search_limits,
            detail_hints: value.detail_hints,
            diagnostics: value.diagnostics,
        }
    }
}

impl From<&ProjectionKind> for ProjectionKindFile {
    fn from(value: &ProjectionKind) -> Self {
        match value {
            ProjectionKind::Table => Self::Table,
            ProjectionKind::TableFunction { function_kind } => Self::TableFunction {
                function_kind: *function_kind,
            },
        }
    }
}

impl From<ProjectionKindFile> for ProjectionKind {
    fn from(value: ProjectionKindFile) -> Self {
        match value {
            ProjectionKindFile::Table => Self::Table,
            ProjectionKindFile::TableFunction { function_kind } => {
                Self::TableFunction { function_kind }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectionCatalogFile, SemanticIrFile};
    use crate::v4::{ProjectionCatalog, SemanticIr};

    #[test]
    fn schema_v3_semantic_ir_fixture_migrates() {
        let file: SemanticIrFile =
            serde_yaml::from_str(include_str!("fixtures/v3/semantic-ir.yaml"))
                .expect("decode schema-v3 semantic IR fixture");
        let runtime = SemanticIr::try_from(file).expect("migrate schema-v3 semantic IR");

        assert_eq!(runtime.source_name, "compatibility_fixture");
        assert_eq!(runtime.surface_id, "rest");
    }

    #[test]
    fn schema_v3_projection_fixture_migrates_legacy_defaults() {
        let file: ProjectionCatalogFile =
            serde_yaml::from_str(include_str!("fixtures/v3/projections.yaml"))
                .expect("decode schema-v3 projection fixture");
        let runtime = ProjectionCatalog::try_from(file).expect("migrate schema-v3 projections");

        assert_eq!(runtime.source_name, "compatibility_fixture");
        assert_eq!(runtime.projections.len(), 1);
        assert_eq!(
            runtime
                .projections
                .first()
                .expect("fixture projection")
                .namespace,
            ""
        );
    }
}
