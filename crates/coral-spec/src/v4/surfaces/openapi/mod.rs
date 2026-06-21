mod document;
mod import;
mod operations;
mod responses;
mod schemas;

pub use document::{OpenApiDocumentMetadata, normalize_source_document, openapi_document_metadata};
pub use import::import_openapi_surface;

pub(super) fn is_supported_openapi_version(version: &str) -> bool {
    version.starts_with("3.0.") || version.starts_with("3.1.")
}
