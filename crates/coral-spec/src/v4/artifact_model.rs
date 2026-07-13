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
pub struct SemanticIrDto {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub importer_version: String,
    pub operations: Vec<IrOperationDto>,
    pub types: Vec<IrTypeDto>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationDto {
    pub id: String,
    pub method_name: String,
    pub description: String,
    pub deprecated: bool,
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub naming: Option<IrOperationNamingDto>,
    pub inputs: Vec<IrOperationInputDto>,
    pub output: IrOperationOutputDto,
    pub entity: Option<IrEntityCandidateDto>,
    pub execution: IrExecutionAttachmentDto,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationNamingDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationInputDto {
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
pub struct IrOperationOutputDto {
    pub cardinality: OutputCardinality,
    pub type_ref: String,
    pub row_path: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrEntityCandidateDto {
    pub name: String,
    pub type_ref: String,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrTypeDto {
    pub id: String,
    pub shape: IrTypeShapeDto,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrTypeShapeDto {
    Scalar(IrScalarType),
    Object { fields: Vec<IrFieldDto> },
    List { item_type_ref: String },
    Map { value_type_ref: String },
    Enum { values: Vec<String> },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrFieldDto {
    pub name: String,
    pub type_ref: String,
    pub required: bool,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrExecutionAttachmentDto {
    Rest(Box<RestExecutionAttachmentDto>),
    Mcp(McpExecutionAttachmentDto),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestExecutionAttachmentDto {
    pub method: HttpMethod,
    pub path_template: String,
    pub parameters: Vec<RestParameterBindingDto>,
    pub request_body: Option<RestRequestBodyDto>,
    pub response: RestResponseAttachmentDto,
    pub pagination: PaginationSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestRequestBodyDto {
    pub required: bool,
    pub media_type: String,
    pub type_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestParameterBindingDto {
    pub input_name: String,
    pub location: IrInputLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: IrScalarType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponseAttachmentDto {
    pub status_code: u16,
    pub media_type: String,
    pub response: ResponseSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpExecutionAttachmentDto {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<McpPaginationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_pagination: Option<McpOffsetPaginationSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalogDto {
    pub artifact_schema_version: u32,
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator_version: Option<String>,
    pub projections: Vec<ProjectionDto>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionDto {
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    pub kind: ProjectionKindDto,
    pub description: String,
    pub guide: String,
    pub surface_id: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInputDto>,
    pub columns: Vec<ProjectionColumnDto>,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ProjectionKindDto {
    Table,
    TableFunction {
        function_kind: SourceTableFunctionKind,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionInputDto {
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
pub struct ProjectionColumnDto {
    pub name: String,
    pub data_type: ManifestDataType,
    pub source_path: Vec<String>,
    pub nullable: bool,
    pub description: String,
}

impl From<&SemanticIr> for SemanticIrDto {
    fn from(value: &SemanticIr) -> Self {
        Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name.clone(),
            surface_id: value.surface_id.clone(),
            surface_type: value.surface_type,
            importer_version: value.importer_version.clone(),
            operations: value.operations.iter().map(IrOperationDto::from).collect(),
            types: value.types.iter().map(IrTypeDto::from).collect(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl TryFrom<SemanticIrDto> for SemanticIr {
    type Error = ArtifactMigrationError;

    fn try_from(value: SemanticIrDto) -> Result<Self, Self::Error> {
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

impl From<&ProjectionCatalog> for ProjectionCatalogDto {
    fn from(value: &ProjectionCatalog) -> Self {
        Self {
            artifact_schema_version: value.artifact_schema_version,
            source_name: value.source_name.clone(),
            generator_version: value.generator_version.clone(),
            projections: value.projections.iter().map(ProjectionDto::from).collect(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl TryFrom<ProjectionCatalogDto> for ProjectionCatalog {
    type Error = ArtifactMigrationError;

    fn try_from(value: ProjectionCatalogDto) -> Result<Self, Self::Error> {
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

impl From<&IrOperation> for IrOperationDto {
    fn from(value: &IrOperation) -> Self {
        Self {
            id: value.id.clone(),
            method_name: value.method_name.clone(),
            description: value.description.clone(),
            deprecated: value.deprecated,
            read_only: value.read_only,
            naming: value.naming.as_ref().map(IrOperationNamingDto::from),
            inputs: value.inputs.iter().map(IrOperationInputDto::from).collect(),
            output: IrOperationOutputDto::from(&value.output),
            entity: value.entity.as_ref().map(IrEntityCandidateDto::from),
            execution: IrExecutionAttachmentDto::from(&value.execution),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl From<IrOperationDto> for IrOperation {
    fn from(value: IrOperationDto) -> Self {
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

copy_struct!(IrOperationNamingDto => IrOperationNaming { group, operation });
copy_struct!(IrOperationInputDto => IrOperationInput {
    name, location, required, data_type, default_value, description, exclude_from_lookup_keys
});
copy_struct!(IrOperationOutputDto => IrOperationOutput { cardinality, type_ref, row_path });
copy_struct!(IrEntityCandidateDto => IrEntityCandidate { name, type_ref, identity_fields });
copy_struct!(IrFieldDto => IrField { name, type_ref, required, nullable, description });
copy_struct!(RestRequestBodyDto => RestRequestBody { required, media_type, type_ref });
copy_struct!(RestParameterBindingDto => RestParameterBinding {
    input_name, location, wire_name, required, data_type
});
copy_struct!(RestResponseAttachmentDto => RestResponseAttachment {
    status_code, media_type, response
});
copy_struct!(McpExecutionAttachmentDto => McpExecutionAttachment {
    tool_name, pagination, offset_pagination
});
copy_struct!(ProjectionInputDto => ProjectionInput {
    name, sql_exposure, source_location, wire_name, required, data_type, default_value,
    description, lookup_key
});
copy_struct!(ProjectionColumnDto => ProjectionColumn {
    name, data_type, source_path, nullable, description
});

impl From<&IrType> for IrTypeDto {
    fn from(value: &IrType) -> Self {
        Self {
            id: value.id.clone(),
            shape: IrTypeShapeDto::from(&value.shape),
            nullable: value.nullable,
            description: value.description.clone(),
        }
    }
}

impl From<IrTypeDto> for IrType {
    fn from(value: IrTypeDto) -> Self {
        Self {
            id: value.id,
            shape: value.shape.into(),
            nullable: value.nullable,
            description: value.description,
        }
    }
}

impl From<&IrTypeShape> for IrTypeShapeDto {
    fn from(value: &IrTypeShape) -> Self {
        match value {
            IrTypeShape::Scalar(value) => Self::Scalar(*value),
            IrTypeShape::Object { fields } => Self::Object {
                fields: fields.iter().map(IrFieldDto::from).collect(),
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

impl From<IrTypeShapeDto> for IrTypeShape {
    fn from(value: IrTypeShapeDto) -> Self {
        match value {
            IrTypeShapeDto::Scalar(value) => Self::Scalar(value),
            IrTypeShapeDto::Object { fields } => Self::Object {
                fields: fields.into_iter().map(IrField::from).collect(),
            },
            IrTypeShapeDto::List { item_type_ref } => Self::List { item_type_ref },
            IrTypeShapeDto::Map { value_type_ref } => Self::Map { value_type_ref },
            IrTypeShapeDto::Enum { values } => Self::Enum { values },
            IrTypeShapeDto::Json => Self::Json,
        }
    }
}

impl From<&IrExecutionAttachment> for IrExecutionAttachmentDto {
    fn from(value: &IrExecutionAttachment) -> Self {
        match value {
            IrExecutionAttachment::Rest(value) => {
                Self::Rest(Box::new(RestExecutionAttachmentDto::from(value.as_ref())))
            }
            IrExecutionAttachment::Mcp(value) => Self::Mcp(McpExecutionAttachmentDto::from(value)),
        }
    }
}

impl From<IrExecutionAttachmentDto> for IrExecutionAttachment {
    fn from(value: IrExecutionAttachmentDto) -> Self {
        match value {
            IrExecutionAttachmentDto::Rest(value) => Self::Rest(Box::new((*value).into())),
            IrExecutionAttachmentDto::Mcp(value) => Self::Mcp(value.into()),
        }
    }
}

impl From<&RestExecutionAttachment> for RestExecutionAttachmentDto {
    fn from(value: &RestExecutionAttachment) -> Self {
        Self {
            method: value.method,
            path_template: value.path_template.clone(),
            parameters: value
                .parameters
                .iter()
                .map(RestParameterBindingDto::from)
                .collect(),
            request_body: value.request_body.as_ref().map(RestRequestBodyDto::from),
            response: RestResponseAttachmentDto::from(&value.response),
            pagination: value.pagination.clone(),
        }
    }
}

impl From<RestExecutionAttachmentDto> for RestExecutionAttachment {
    fn from(value: RestExecutionAttachmentDto) -> Self {
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

impl From<&Projection> for ProjectionDto {
    fn from(value: &Projection) -> Self {
        Self {
            name: value.name.clone(),
            namespace: value.namespace.clone(),
            kind: ProjectionKindDto::from(&value.kind),
            description: value.description.clone(),
            guide: value.guide.clone(),
            surface_id: value.surface_id.clone(),
            operation_id: value.operation_id.clone(),
            visibility: value.visibility,
            inputs: value.inputs.iter().map(ProjectionInputDto::from).collect(),
            columns: value
                .columns
                .iter()
                .map(ProjectionColumnDto::from)
                .collect(),
            search_limits: value.search_limits.clone(),
            detail_hints: value.detail_hints.clone(),
            diagnostics: value.diagnostics.clone(),
        }
    }
}

impl From<ProjectionDto> for Projection {
    fn from(value: ProjectionDto) -> Self {
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

impl From<&ProjectionKind> for ProjectionKindDto {
    fn from(value: &ProjectionKind) -> Self {
        match value {
            ProjectionKind::Table => Self::Table,
            ProjectionKind::TableFunction { function_kind } => Self::TableFunction {
                function_kind: *function_kind,
            },
        }
    }
}

impl From<ProjectionKindDto> for ProjectionKind {
    fn from(value: ProjectionKindDto) -> Self {
        match value {
            ProjectionKindDto::Table => Self::Table,
            ProjectionKindDto::TableFunction { function_kind } => {
                Self::TableFunction { function_kind }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectionCatalogDto, SemanticIrDto};
    use crate::v4::{
        HttpMethod, IrExecutionAttachment, IrInputLocation, IrScalarType, IrTypeShape,
        OutputCardinality, ProjectionCatalog, ProjectionKind, ProjectionVisibility, SemanticIr,
        SqlInputExposure,
    };
    use crate::{ManifestDataType, PaginationMode, SourceTableFunctionKind};

    fn decode_semantic_ir(raw: &str) -> SemanticIr {
        let dto: SemanticIrDto =
            serde_yaml::from_str(raw).expect("decode schema-v3 semantic IR fixture");
        SemanticIr::try_from(dto).expect("migrate schema-v3 semantic IR")
    }

    fn decode_projections() -> ProjectionCatalog {
        let dto: ProjectionCatalogDto =
            serde_yaml::from_str(include_str!("fixtures/v3/projections.yaml"))
                .expect("decode schema-v3 projection fixture");
        ProjectionCatalog::try_from(dto).expect("migrate schema-v3 projections")
    }

    #[test]
    fn schema_v3_rest_semantic_ir_fixture_migrates_nested_models() {
        let runtime = decode_semantic_ir(include_str!("fixtures/v3/semantic-ir.yaml"));

        assert_eq!(runtime.source_name, "compatibility_fixture");
        assert_eq!(runtime.surface_id, "rest");
        assert_eq!(runtime.operations.len(), 1);
        assert_eq!(runtime.types.len(), 6);
        assert_eq!(runtime.diagnostics.len(), 1);

        let operation = runtime.operations.first().expect("REST operation");
        assert_eq!(operation.id, "list_items");
        assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
        assert_eq!(operation.output.row_path, ["data", "items"]);
        assert_eq!(operation.inputs.len(), 2);
        let owner = operation.inputs.first().expect("owner input");
        let cursor = operation.inputs.get(1).expect("cursor input");
        assert_eq!(owner.location, IrInputLocation::Path);
        assert!(cursor.exclude_from_lookup_keys);
        assert_eq!(
            operation.entity.as_ref().expect("entity").identity_fields,
            ["id"]
        );

        let IrExecutionAttachment::Rest(execution) = &operation.execution else {
            panic!("expected REST execution attachment");
        };
        assert_eq!(execution.method, HttpMethod::Get);
        assert_eq!(execution.parameters.len(), 2);
        assert_eq!(
            execution
                .request_body
                .as_ref()
                .expect("request body")
                .type_ref,
            "ItemQuery"
        );
        assert!(execution.response.response.allow_404_empty);
        assert_eq!(execution.pagination.mode, PaginationMode::CursorQuery);
        assert_eq!(execution.pagination.cursor_param.as_deref(), Some("cursor"));
        assert_eq!(execution.pagination.max_pages, Some(10));

        let shapes = runtime
            .types
            .iter()
            .map(|ir_type| (&ir_type.id, &ir_type.shape))
            .collect::<Vec<_>>();
        assert!(shapes.iter().any(|(id, shape)| {
            id.as_str() == "Item"
                && matches!(shape, IrTypeShape::Object { fields } if fields.len() == 2)
        }));
        assert!(shapes.iter().any(|(id, shape)| {
            id.as_str() == "ItemId" && matches!(shape, IrTypeShape::Scalar(IrScalarType::Id))
        }));
        assert!(shapes.iter().any(|(id, shape)| {
            id.as_str() == "ItemList"
                && matches!(shape, IrTypeShape::List { item_type_ref } if item_type_ref == "Item")
        }));
        assert!(shapes.iter().any(|(id, shape)| {
            id.as_str() == "ItemMap"
                && matches!(shape, IrTypeShape::Map { value_type_ref } if value_type_ref == "Item")
        }));
        assert!(shapes.iter().any(|(id, shape)| {
            id.as_str() == "ItemState"
                && matches!(shape, IrTypeShape::Enum { values } if values == &["open", "closed"])
        }));
        assert!(shapes
            .iter()
            .any(|(id, shape)| id.as_str() == "ItemQuery" && matches!(shape, IrTypeShape::Json)));
    }

    #[test]
    fn schema_v3_mcp_semantic_ir_fixture_migrates_pagination() {
        let runtime = decode_semantic_ir(include_str!("fixtures/v3/mcp-semantic-ir.yaml"));
        let operation = runtime.operations.first().expect("MCP operation");

        assert_eq!(runtime.surface_id, "mcp");
        assert_eq!(operation.output.cardinality, OutputCardinality::List);
        let query = operation.inputs.first().expect("query input");
        let limit = operation.inputs.get(1).expect("limit input");
        assert_eq!(query.location, IrInputLocation::ToolArg);
        assert_eq!(limit.default_value.as_deref(), Some("20"));

        let IrExecutionAttachment::Mcp(execution) = &operation.execution else {
            panic!("expected MCP execution attachment");
        };
        assert_eq!(execution.tool_name, "search_items");
        let cursor = execution.pagination.as_ref().expect("cursor pagination");
        assert_eq!(cursor.cursor_arg, "cursor");
        assert_eq!(cursor.response_cursor_path, ["next_cursor"]);
        assert_eq!(cursor.max_pages, Some(5));
        let offset = execution
            .offset_pagination
            .as_ref()
            .expect("offset pagination");
        assert_eq!(offset.limit_arg, "limit");
        assert_eq!(offset.default_limit, 20);
        assert_eq!(offset.max_limit, 100);
        assert_eq!(offset.offset_arg, "offset");
        assert_eq!(offset.max_pages, Some(8));
    }

    #[test]
    fn schema_v3_projection_fixture_migrates_legacy_defaults() {
        let runtime = decode_projections();

        assert_eq!(runtime.source_name, "compatibility_fixture");
        assert_eq!(runtime.projections.len(), 2);
        assert_eq!(runtime.diagnostics.len(), 1);

        let table = runtime.projections.first().expect("table projection");
        assert_eq!(table.namespace, "");
        assert!(matches!(table.kind, ProjectionKind::Table));
        assert_eq!(table.visibility, ProjectionVisibility::Published);
        assert_eq!(table.inputs.len(), 2);
        let owner = table.inputs.first().expect("owner filter");
        let cursor = table.inputs.get(1).expect("cursor input");
        assert_eq!(owner.sql_exposure, SqlInputExposure::Filter);
        assert!(owner.lookup_key);
        assert_eq!(cursor.sql_exposure, SqlInputExposure::Internal);
        assert_eq!(table.columns.len(), 2);
        let id = table.columns.first().expect("id column");
        assert_eq!(id.data_type, ManifestDataType::Utf8);
        assert!(!id.nullable);
        assert_eq!(table.diagnostics.len(), 1);
    }

    #[test]
    fn schema_v3_projection_fixture_migrates_function_metadata() {
        let runtime = decode_projections();
        let function = runtime
            .projections
            .get(1)
            .expect("table-function projection");

        assert_eq!(function.namespace, "compatibility_fixture_mcp");
        assert!(matches!(
            function.kind,
            ProjectionKind::TableFunction {
                function_kind: SourceTableFunctionKind::Search
            }
        ));
        assert_eq!(function.visibility, ProjectionVisibility::Hidden);
        let query = function.inputs.first().expect("query argument");
        let limit = function.inputs.get(1).expect("limit input");
        assert_eq!(query.sql_exposure, SqlInputExposure::FunctionArg);
        assert_eq!(limit.data_type, ManifestDataType::Int64);
        assert_eq!(limit.default_value.as_deref(), Some("20"));
        let score = function.columns.get(1).expect("score column");
        assert_eq!(score.data_type, ManifestDataType::Float64);
        let limits = function.search_limits.as_ref().expect("search limits");
        assert_eq!(limits.default_top_k, 20);
        assert_eq!(limits.max_top_k, 100);
        assert_eq!(limits.max_calls_per_query, 10);
        let hint = function.detail_hints.first().expect("detail hint");
        assert_eq!(hint.table, "items");
        assert_eq!(hint.search_result_column, "id");
        assert_eq!(hint.detail_filter, "id");
    }
}
