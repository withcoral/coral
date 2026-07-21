#![allow(
    missing_docs,
    reason = "DSL v4 contracts are field-heavy artifact models documented in the PRD."
)]

pub const V4_ARTIFACT_SCHEMA_VERSION: u32 = 4;
pub const SURFACE_IMPORTER_VERSION: &str = "surface-import-v2";
pub const OPENAPI_IMPORTER_VERSION: &str = "openapi-v6";
pub const MCP_IMPORTER_VERSION: &str = "mcp-tools-v2";
pub const PROJECTION_GENERATOR_VERSION: &str = "derive-read-v9";

mod artifacts;
mod diagnostics;
mod ir;
mod lookup_keys;
mod manifest;
mod naming;
mod parameter_metadata;
mod projections;
mod schema;
mod surfaces;

#[cfg(test)]
mod manifest_tests;
#[cfg(test)]
mod openapi_tests;
#[cfg(test)]
mod projection_tests;
#[cfg(test)]
mod test_support;

pub use artifacts::{
    Fingerprint, FingerprintSurface, MaterializedSurface, V4MaterializedSource,
    validate_materialized_source, validate_materialized_source_structure,
};
pub use diagnostics::{Diagnostic, DiagnosticSeverity};
pub use ir::{
    HttpMethod, IrEntityCandidate, IrExecutionAttachment, IrField, IrInputLocation, IrOperation,
    IrOperationInput, IrOperationNaming, IrOperationOutput, IrScalarType, IrType, IrTypeShape,
    McpExecutionAttachment, OutputCardinality, RestExecutionAttachment, RestParameterBinding,
    RestRequestBody, RestResponseAttachment, SemanticIr,
};
pub use manifest::{
    AcceptedIdentityRequirement, DatabaseRuntimeConfig, IdentityRequirements, McpRuntimeConfig,
    OpenApiRuntimeConfig, SurfaceDescriptor, SurfaceRuntimeConfig, SurfaceType, V4SourceCommon,
    V4SourceManifest, V4Surface, validate_openapi_base_url_template,
};
pub use naming::normalize_identifier;
pub use parameter_metadata::{
    LookupKeysMetadata, ParameterMetadataOverrides, ProjectionPaginationInputSyncMode,
    apply_parameter_metadata_overrides, parse_parameter_metadata_overrides_yaml,
    sync_projection_pagination_inputs, validate_lookup_keys_for_surface,
};
pub use projections::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure, generate_projection_catalog, mcp_projection_arg_specs,
    projection_arg_specs, projection_column_specs, projection_filter_specs,
    request_spec_for_projection,
};
pub use schema::generated_v4_source_manifest_schema;
pub use surfaces::{
    McpToolCatalog, McpToolDescriptor, OpenApiDocumentMetadata, import_mcp_surface,
    import_openapi_surface, normalize_mcp_tool_catalog, normalize_source_document,
    openapi_document_metadata,
};
