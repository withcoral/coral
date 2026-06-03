#![allow(
    missing_docs,
    reason = "DSL v4 contracts are field-heavy artifact models documented in the PRD."
)]

mod artifacts;
mod document;
mod identifiers;
mod importer;
mod manifest;
mod materialized;
mod projection;
mod schema;

#[cfg(test)]
mod tests;

pub use artifacts::{
    Diagnostic, DiagnosticSeverity, Fingerprint, FingerprintSurface, HttpMethod, IrEntityCandidate,
    IrExecutionAttachment, IrField, IrOperation, IrOperationInput, IrOperationOutput, IrScalarType,
    IrType, IrTypeShape, MaterializedSurface, OPENAPI_IMPORTER_VERSION, OpenApiParameterLocation,
    OutputCardinality, PROJECTION_GENERATOR_VERSION, Projection, ProjectionCatalog,
    ProjectionColumn, ProjectionInput, ProjectionKind, ProjectionVisibility,
    RestExecutionAttachment, RestParameterBinding, RestRequestBody, RestResponseAttachment,
    SemanticIr, SqlInputExposure, V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource,
};
pub use document::{OpenApiDocumentMetadata, normalize_source_document, openapi_document_metadata};
pub use identifiers::normalize_identifier;
pub use importer::import_openapi_surface;
pub use manifest::{
    OpenApiRuntimeConfig, SurfaceDescriptor, SurfaceType, V4SourceCommon, V4SourceManifest,
    V4Surface,
};
pub use materialized::validate_materialized_source;
pub use projection::{
    generate_projection_catalog, manifest_data_type_name, projection_arg_specs,
    projection_column_specs, projection_filter_specs, request_spec_for_projection,
};
pub use schema::generated_v4_source_manifest_schema;
