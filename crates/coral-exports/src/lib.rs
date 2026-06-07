//! Source and workspace export contracts.
//!
//! `coral-exports` owns generated binding metadata and discovery items. It does
//! not import provider documents, execute SQL, call upstream providers, or know
//! app state.

#![allow(
    missing_docs,
    reason = "Serializable export contract fields are documented in the capability projection plan and covered by focused tests."
)]
#![allow(
    clippy::module_name_repetitions,
    reason = "Contract names intentionally include the export/product surface."
)]

pub mod contributors;
pub mod discovery;
pub mod exports;
pub mod package;
pub mod paths;

pub use contributors::{TypescriptBindingContributor, typescript_type_name};
pub use discovery::{
    DescribeResolution, SearchFilter, SearchResult, SearchResultsPage, describe_export,
    search_exports, search_exports_page,
};
pub use exports::{
    Binding, BindingBuildContext, BindingContribution, BindingContributor, CapabilityExport,
    EffectProfileSnapshot, ExportError, ExportKind, ExportRef, FileScanProjection,
    PaginationProfile, ResponseSelection, Result, SOURCE_EXPORTS_GENERATOR_VERSION, SourceExports,
    SqlBinding, SqlBindingKind, SqlColumn, SqlInput, SqlProjectionV1, SqlRowShape,
    TypescriptBinding, WorkspaceExportSource, WorkspaceExports, build_source_exports,
    compose_workspace_exports,
};
pub use package::SourceKey;
