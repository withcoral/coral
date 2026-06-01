#![allow(
    missing_docs,
    reason = "DSL v4 contracts are field-heavy artifact models documented in the PRD."
)]

pub const V4_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const OPENAPI_IMPORTER_VERSION: &str = "openapi-v2";
pub const PROJECTION_GENERATOR_VERSION: &str = "derive-read-v2";

mod artifact;
mod diagnostic;
mod identifiers;
mod ir;
mod manifest;
mod openapi;
mod projection;

pub use artifact::{
    Fingerprint, FingerprintSurface, MaterializedSurface, V4MaterializedSource,
    validate_materialized_source,
};
pub use diagnostic::{Diagnostic, DiagnosticSeverity};
pub use identifiers::normalize_identifier;
pub use ir::{
    HttpMethod, IrEntityCandidate, IrExecutionAttachment, IrField, IrOperation, IrOperationInput,
    IrOperationOutput, IrScalarType, IrType, IrTypeShape, OpenApiParameterLocation,
    OutputCardinality, RestExecutionAttachment, RestParameterBinding, RestRequestBody,
    RestResponseAttachment, SemanticIr,
};
pub use manifest::{
    OpenApiRuntimeConfig, ProjectionPolicy, ProjectionPolicyDefault, SurfaceDescriptor,
    SurfaceType, V4SourceManifest, V4Surface,
};
pub use openapi::{
    OpenApiDocumentMetadata, import_openapi_surface, normalize_source_document,
    openapi_document_metadata,
};
pub use projection::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure, generate_projection_catalog, manifest_data_type_name,
    projection_arg_specs, projection_column_specs, projection_filter_specs,
    request_spec_for_projection,
};

#[cfg(test)]
mod tests;
