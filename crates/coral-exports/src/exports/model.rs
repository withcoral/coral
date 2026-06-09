use coral_capabilities::{
    Capability, CapabilityId, CapabilityKind, Diagnostic, EffectKind, EffectProfile,
    IdempotencyKind, SourceCapabilitySet, SourceId, SupportStatus,
};
use serde::{Deserialize, Serialize};

use crate::package::SourceKey;

/// Generator version for source export artifacts produced by this binary.
pub const SOURCE_EXPORTS_GENERATOR_VERSION: &str = "source-exports-v11";

/// Build context for one installed source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingBuildContext {
    pub source_id: SourceId,
    pub display_name: String,
    pub source_key: SourceKey,
}

/// Contribution from one binding generator for one capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingContribution {
    pub bindings: Vec<Binding>,
    pub search_text: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
    pub binding_diagnostics: Vec<BindingDiagnostic>,
}

impl BindingContribution {
    /// Empty contribution.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            bindings: Vec::new(),
            search_text: Vec::new(),
            diagnostics: Vec::new(),
            binding_diagnostics: Vec::new(),
        }
    }
}

/// Binding contribution port. SQL implements this trait outside this crate.
pub trait BindingContributor {
    /// Stable contributor name.
    fn name(&self) -> &'static str;

    /// Contribute bindings for one capability.
    ///
    /// # Errors
    ///
    /// Returns an export error when contributor-specific validation fails.
    fn contribute(
        &self,
        capability: &Capability,
        ctx: &BindingBuildContext,
    ) -> super::Result<BindingContribution>;
}

/// Source-scoped export artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceExports {
    pub artifact_schema_version: u32,
    pub source_id: SourceId,
    pub display_name: String,
    pub source_key: SourceKey,
    pub generator_version: String,
    pub entries: Vec<CapabilityExport>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SourceExports {
    /// Creates an empty source export artifact.
    #[must_use]
    pub fn empty(ctx: &BindingBuildContext) -> Self {
        Self {
            artifact_schema_version: 1,
            source_id: ctx.source_id.clone(),
            display_name: ctx.display_name.clone(),
            source_key: ctx.source_key.clone(),
            generator_version: SOURCE_EXPORTS_GENERATOR_VERSION.to_string(),
            entries: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
}

/// App-composed workspace export view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceExports {
    pub artifact_schema_version: u32,
    pub workspace_id: String,
    pub sources: Vec<WorkspaceExportSource>,
    pub entries: Vec<CapabilityExport>,
    pub diagnostics: Vec<Diagnostic>,
}

/// One source included in a workspace export view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceExportSource {
    pub source_id: SourceId,
    pub display_name: String,
    pub source_key: SourceKey,
    pub source_exports_generator_version: String,
}

/// One discoverable capability export row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityExport {
    pub capability_id: CapabilityId,
    pub source_id: SourceId,
    pub display_name: String,
    pub source_key: SourceKey,
    pub interface_id: String,
    pub operation_id: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub deprecated: bool,
    #[serde(default = "default_support_status")]
    pub support_status: SupportStatus,
    pub bindings: Vec<Binding>,
    pub search_text: Vec<String>,
    pub effect_profile: EffectProfileSnapshot,
    pub diagnostics: Vec<Diagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding_diagnostics: Vec<BindingDiagnostic>,
}

fn default_support_status() -> SupportStatus {
    SupportStatus::Generated
}

impl CapabilityExport {
    /// Build base export metadata from a capability.
    #[must_use]
    pub fn from_capability(capability: &Capability, ctx: &BindingBuildContext) -> Self {
        Self {
            capability_id: capability.capability_id.clone(),
            source_id: capability.source_id.clone(),
            display_name: ctx.display_name.clone(),
            source_key: ctx.source_key.clone(),
            interface_id: capability.interface_id.clone(),
            operation_id: capability.operation_id.clone(),
            title: capability.display.title.clone(),
            description: capability.display.description.clone(),
            deprecated: capability.display.deprecated,
            support_status: capability.display.support_status,
            bindings: Vec::new(),
            search_text: base_search_text(capability, ctx),
            effect_profile: EffectProfileSnapshot::from(&capability.effect_profile),
            diagnostics: capability.diagnostics.clone(),
            binding_diagnostics: Vec::new(),
        }
    }
}

fn base_search_text(capability: &Capability, ctx: &BindingBuildContext) -> Vec<String> {
    let mut values = [
        ctx.source_key.as_str(),
        ctx.display_name.as_str(),
        capability.interface_id.as_str(),
        capability.operation_id.as_str(),
        capability.provider_origin.provider_name.as_str(),
        capability.display.title.as_str(),
        capability.display.description.as_str(),
    ]
    .into_iter()
    .filter(|value| !value.trim().is_empty())
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();
    values.extend(capability.provider_origin.tags.iter().cloned());
    values
}

/// Snapshot of capability effect metadata in exports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectProfileSnapshot {
    pub capability_kind: CapabilityKind,
    pub effects: Vec<EffectKind>,
    pub idempotency: IdempotencyKind,
}

/// Diagnostic produced by a product binding contributor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingDiagnostic {
    pub applies_to: Vec<ExportKind>,
    pub diagnostic: Diagnostic,
}

impl BindingDiagnostic {
    /// Creates a binding-scoped diagnostic.
    #[must_use]
    pub fn new(applies_to: Vec<ExportKind>, diagnostic: Diagnostic) -> Self {
        Self {
            applies_to,
            diagnostic,
        }
    }
}

impl From<&EffectProfile> for EffectProfileSnapshot {
    fn from(value: &EffectProfile) -> Self {
        Self {
            capability_kind: value.capability_kind,
            effects: value.effects.clone(),
            idempotency: value.idempotency,
        }
    }
}

/// Product binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "binding_type", rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "Export binding variants stay unboxed so serialized artifacts remain plain binding objects."
)]
pub enum Binding {
    Typescript(TypescriptBinding),
    Sql(SqlBinding),
}

impl Binding {
    /// Returns the typed binding ref.
    #[must_use]
    pub fn ref_(&self) -> &ExportRef {
        match self {
            Self::Typescript(binding) => &binding.ref_,
            Self::Sql(binding) => &binding.ref_,
        }
    }

    /// Returns the untyped binding alias agents may use for lookup.
    #[must_use]
    pub fn alias(&self) -> String {
        match self {
            Self::Typescript(binding) => binding.path.join("."),
            Self::Sql(binding) => binding.sql_reference.clone(),
        }
    }

    /// Returns the generated Code Mode call path, when the binding is invokable.
    #[must_use]
    pub fn full_path(&self) -> Option<String> {
        match self {
            Self::Typescript(binding) if !binding.path.is_empty() => {
                Some(format!("tools.{}", binding.path.join(".")))
            }
            Self::Typescript(_) | Self::Sql(_) => None,
        }
    }
}

/// TypeScript binding metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypescriptBinding {
    #[serde(rename = "ref")]
    pub ref_: ExportRef,
    pub path: Vec<String>,
    pub args_type_name: String,
    pub result_type_name: String,
}

/// SQL binding metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlBinding {
    pub kind: SqlBindingKind,
    #[serde(rename = "ref")]
    pub ref_: ExportRef,
    pub sql_reference: String,
    pub projection: SqlProjectionV1,
}

/// SQL binding kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlBindingKind {
    Table,
    Function,
}

impl SqlBindingKind {
    /// Build the typed export ref for this SQL binding kind.
    #[must_use]
    pub fn export_ref(self, sql_reference: impl Into<String>) -> ExportRef {
        match self {
            Self::Table => ExportRef::sql_table(sql_reference),
            Self::Function => ExportRef::sql_function(sql_reference),
        }
    }
}

/// Serializable SQL projection metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlProjectionV1 {
    pub row_shape: SqlRowShape,
    pub columns: Vec<SqlColumn>,
    pub inputs: Vec<SqlInput>,
    pub response_selection: Option<ResponseSelection>,
    pub pagination: Option<PaginationProfile>,
    pub file_scan: Option<FileScanProjection>,
    pub diagnostics: Vec<Diagnostic>,
}

/// SQL row shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlRowShape {
    Collection,
    Singleton,
}

/// SQL column metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: String,
}

/// SQL input metadata for table functions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlInput {
    pub name: String,
    pub required: bool,
    pub data_type: String,
}

/// Response variant selected for SQL projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseSelection {
    pub status: String,
    pub media_type: String,
    pub path: Vec<String>,
}

/// SQL pagination metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaginationProfile {
    pub kind: String,
    pub cursor_input: Option<String>,
    pub page_size_input: Option<String>,
    pub cursor_path: Option<Vec<String>>,
}

/// File scan projection metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileScanProjection {
    pub file_refs: Vec<String>,
    pub format: String,
    pub schema_ref: Option<String>,
}

/// Typed export ref.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExportRef {
    pub kind: ExportKind,
    pub value: String,
}

impl ExportRef {
    /// Build a TypeScript ref.
    #[must_use]
    pub fn typescript(path: &[String]) -> Self {
        Self {
            kind: ExportKind::Typescript,
            value: format!("typescript:{}", path.join(".")),
        }
    }

    /// Build a SQL table ref.
    #[must_use]
    pub fn sql_table(sql_reference: impl Into<String>) -> Self {
        Self {
            kind: ExportKind::SqlTable,
            value: format!("sql_table:{}", sql_reference.into()),
        }
    }

    /// Build a SQL function ref.
    #[must_use]
    pub fn sql_function(sql_reference: impl Into<String>) -> Self {
        Self {
            kind: ExportKind::SqlFunction,
            value: format!("sql_function:{}", sql_reference.into()),
        }
    }
}

/// Export ref kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    Typescript,
    SqlTable,
    SqlFunction,
}

/// Capability set reference used only by builders.
pub(crate) fn capability_by_id(
    capabilities: &SourceCapabilitySet,
) -> std::collections::BTreeSet<CapabilityId> {
    capabilities
        .capabilities
        .iter()
        .map(|capability| capability.capability_id.clone())
        .collect()
}
