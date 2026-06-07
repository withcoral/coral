//! Export artifact models and builders.

pub mod derive;
pub mod model;
pub mod validate;

pub use derive::{build_source_exports, compose_workspace_exports};
pub use model::{
    Binding, BindingBuildContext, BindingContribution, BindingContributor, CapabilityExport,
    EffectProfileSnapshot, ExportKind, ExportRef, FileScanProjection, PaginationProfile,
    ResponseSelection, SOURCE_EXPORTS_GENERATOR_VERSION, SourceExports, SqlBinding, SqlBindingKind,
    SqlColumn, SqlInput, SqlProjectionV1, SqlRowShape, TypescriptBinding, WorkspaceExportSource,
    WorkspaceExports,
};
pub use validate::{ExportError, Result};
