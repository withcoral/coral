#![allow(
    missing_docs,
    reason = "DSL v4 contracts are field-heavy artifact models documented in the PRD."
)]

pub const V4_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const OPENAPI_IMPORTER_VERSION: &str = "openapi-v3";
pub const PROJECTION_GENERATOR_VERSION: &str = "derive-read-v4";

mod artifacts;
mod diagnostics;
mod ir;
mod manifest;
mod naming;
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
    validate_materialized_source,
};
pub use diagnostics::{Diagnostic, DiagnosticSeverity};
pub use ir::{
    HttpMethod, IrEntityCandidate, IrExecutionAttachment, IrField, IrOperation, IrOperationInput,
    IrOperationOutput, IrScalarType, IrType, IrTypeShape, OpenApiParameterLocation,
    OutputCardinality, RestExecutionAttachment, RestParameterBinding, RestRequestBody,
    RestResponseAttachment, SemanticIr,
};
pub use manifest::{
    OpenApiRuntimeConfig, SurfaceDescriptor, SurfaceType, V4SourceCommon, V4SourceManifest,
    V4Surface, validate_openapi_base_url_template,
};
pub use naming::normalize_identifier;
pub use projections::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure, generate_projection_catalog, manifest_data_type_name,
    projection_arg_specs, projection_column_specs, projection_filter_specs,
    request_spec_for_projection,
};
pub use schema::generated_v4_source_manifest_schema;
pub use surfaces::{
    OpenApiDocumentMetadata, import_openapi_surface, normalize_source_document,
    openapi_document_metadata,
};
