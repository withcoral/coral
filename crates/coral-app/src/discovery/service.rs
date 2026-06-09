//! Implements the gRPC `DiscoveryService`.

use coral_api::v1::discovery_service_server::DiscoveryService as DiscoveryServiceApi;
use coral_api::v1::{
    DescribeExportCandidate, DescribeExportRequest, DescribeExportResponse, ExportBindingKind,
    ExportDescription, ExportDiagnosticDescription, PaginationRequest, SearchExportItem,
    SearchExportsRequest, SearchExportsResponse, SqlBindingDescription, SqlColumnDescription,
    SqlInputDescription, TypeScriptBindingDescription,
};
use coral_capabilities::{
    CapabilityKind, Diagnostic, DiagnosticSeverity, DiagnosticStage, EffectKind, SupportStatus,
};
use coral_exports::{
    Binding, CapabilityExport, ExportKind, SqlBinding, SqlBindingKind, SqlColumn, SqlInput,
    SqlRowShape, TypescriptBinding,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::discovery::manager::{
    DiscoveryDescribeResult, DiscoveryManager, DiscoveryPagination, DiscoverySearchFilter,
    binding_alias, binding_kinds, binding_refs,
};
use crate::transport::{
    grpc_span, instrument_grpc, json_value_to_proto, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct DiscoveryService {
    discovery: DiscoveryManager,
}

impl DiscoveryService {
    pub(crate) fn new(discovery: DiscoveryManager) -> Self {
        Self { discovery }
    }
}

#[tonic::async_trait]
impl DiscoveryServiceApi for DiscoveryService {
    async fn search(
        &self,
        request: Request<SearchExportsRequest>,
    ) -> Result<Response<SearchExportsResponse>, Status> {
        let span = grpc_span(&request);
        let discovery = self.discovery.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let filter = search_filter_from_proto(&request)?;
            let pagination = pagination_from_proto(request.pagination.unwrap_or_default());
            let page = discovery
                .search(&workspace_name, &request.query, &filter, pagination)
                .map_err(app_status)?;
            let items = page
                .items
                .into_iter()
                .map(search_item_to_proto)
                .collect::<Vec<_>>();
            Ok(Response::new(SearchExportsResponse {
                items,
                total: u32::try_from(page.total).unwrap_or(u32::MAX),
                has_more: page.has_more,
                next_offset: page
                    .next_offset
                    .map_or(0, |offset| u32::try_from(offset).unwrap_or(u32::MAX)),
                limit: u32::try_from(page.limit).unwrap_or(u32::MAX),
                offset: u32::try_from(page.offset).unwrap_or(u32::MAX),
                diagnostics: page.diagnostics.iter().map(diagnostic_to_proto).collect(),
            }))
        })
        .await
    }

    async fn describe(
        &self,
        request: Request<DescribeExportRequest>,
    ) -> Result<Response<DescribeExportResponse>, Status> {
        let span = grpc_span(&request);
        let discovery = self.discovery.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            if request.reference.trim().is_empty() {
                return Err(app_status(AppError::InvalidInput(
                    "reference is required".to_string(),
                )));
            }
            let response = match discovery
                .describe(&workspace_name, &request.reference)
                .map_err(app_status)?
            {
                DiscoveryDescribeResult::Found(description) => DescribeExportResponse {
                    found: true,
                    ambiguous: false,
                    entry: Some(description_to_proto(*description).map_err(app_status)?),
                    candidates: Vec::new(),
                    diagnostics: Vec::new(),
                },
                DiscoveryDescribeResult::Ambiguous(candidates) => DescribeExportResponse {
                    found: false,
                    ambiguous: true,
                    entry: None,
                    candidates: candidates.iter().map(candidate_to_proto).collect(),
                    diagnostics: Vec::new(),
                },
                DiscoveryDescribeResult::NotFound { diagnostics } => DescribeExportResponse {
                    found: false,
                    ambiguous: false,
                    entry: None,
                    candidates: Vec::new(),
                    diagnostics: diagnostics.iter().map(diagnostic_to_proto).collect(),
                },
            };
            Ok(Response::new(response))
        })
        .await
    }
}

fn search_filter_from_proto(
    request: &SearchExportsRequest,
) -> Result<DiscoverySearchFilter, Status> {
    Ok(DiscoverySearchFilter {
        source_id: optional_trimmed(&request.source_id),
        source_key: optional_trimmed(&request.source_key),
        display_name: optional_trimmed(&request.display_name),
        kind: export_kind_from_proto(request.kind()),
        allowed_kinds: export_kinds_from_proto(&request.allowed_kinds)?,
        capability_kind: optional_capability_kind(&request.capability_kind)?,
        effect: optional_effect(&request.effect)?,
    })
}

fn pagination_from_proto(pagination: PaginationRequest) -> DiscoveryPagination {
    DiscoveryPagination::new(pagination.limit, pagination.offset)
}

fn search_item_to_proto(item: coral_exports::SearchResult) -> SearchExportItem {
    SearchExportItem {
        alias: item.alias.unwrap_or_default(),
        full_path: item.full_path.unwrap_or_default(),
        capability_id: item.capability_id.to_string(),
        refs: item.refs,
        source_id: item.source_id.to_string(),
        display_name: item.display_name,
        source_key: item.source_key,
        capability_kind: capability_kind_to_text(item.capability_kind).to_string(),
        effects: item
            .effects
            .into_iter()
            .map(|effect| effect_to_text(effect).to_string())
            .collect(),
        title: item.title,
        description: item.description,
        available_bindings: item
            .available_bindings
            .into_iter()
            .map(export_kind_to_proto)
            .map(|kind| kind as i32)
            .collect(),
        diagnostic_count: u32::try_from(item.diagnostic_count).unwrap_or(u32::MAX),
        score: item.score,
        matched_fields: item.matched_fields,
        rank_reason: item.rank_reason,
        deprecated: item.deprecated,
        support_status: support_status_to_text(item.support_status).to_string(),
    }
}

fn description_to_proto(
    description: crate::discovery::manager::DiscoveryDescription,
) -> Result<ExportDescription, AppError> {
    let entry = description.entry;
    let capability = description
        .capability
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?
        .map(json_value_to_proto);
    let alias = entry
        .bindings
        .first()
        .map(binding_alias)
        .unwrap_or_default();
    let refs = binding_refs(&entry);
    let full_path = entry
        .bindings
        .iter()
        .find_map(Binding::full_path)
        .unwrap_or_default();
    let typescript_path = entry
        .bindings
        .iter()
        .find_map(|binding| match binding {
            Binding::Typescript(binding) => Some(binding.path.clone()),
            Binding::Sql(_) => None,
        })
        .unwrap_or_default();
    let typescript_binding = entry.bindings.iter().find_map(|binding| match binding {
        Binding::Typescript(binding) => Some(typescript_binding_to_proto(binding)),
        Binding::Sql(_) => None,
    });
    let sql_bindings = entry
        .bindings
        .iter()
        .filter_map(|binding| match binding {
            Binding::Sql(binding) => Some(sql_binding_to_proto(binding)),
            Binding::Typescript(_) => None,
        })
        .collect();
    let diagnostics = entry.diagnostics.iter().map(diagnostic_to_proto).collect();
    Ok(ExportDescription {
        capability_id: entry.capability_id.to_string(),
        alias,
        refs,
        source_id: entry.source_id.to_string(),
        display_name: entry.display_name,
        source_key: entry.source_key.as_str().to_string(),
        interface_id: entry.interface_id,
        operation_id: entry.operation_id,
        title: entry.title,
        description: entry.description,
        capability_kind: capability_kind_to_text(entry.effect_profile.capability_kind).to_string(),
        effects: entry
            .effect_profile
            .effects
            .into_iter()
            .map(|effect| effect_to_text(effect).to_string())
            .collect(),
        typescript_path,
        capability,
        typescript_binding,
        sql_bindings,
        diagnostics,
        full_path,
        deprecated: entry.deprecated,
        support_status: support_status_to_text(entry.support_status).to_string(),
    })
}

fn typescript_binding_to_proto(binding: &TypescriptBinding) -> TypeScriptBindingDescription {
    TypeScriptBindingDescription {
        r#ref: binding.ref_.value.clone(),
        path: binding.path.clone(),
        args_type_name: binding.args_type_name.clone(),
        result_type_name: binding.result_type_name.clone(),
        full_path: generated_tool_path(&binding.path),
    }
}

fn sql_binding_to_proto(binding: &SqlBinding) -> SqlBindingDescription {
    SqlBindingDescription {
        kind: sql_binding_kind_to_proto(binding.kind) as i32,
        r#ref: binding.ref_.value.clone(),
        sql_reference: binding.sql_reference.clone(),
        row_shape: sql_row_shape_to_text(binding.projection.row_shape).to_string(),
        columns: binding
            .projection
            .columns
            .iter()
            .map(sql_column_to_proto)
            .collect(),
        inputs: binding
            .projection
            .inputs
            .iter()
            .map(sql_input_to_proto)
            .collect(),
    }
}

fn sql_column_to_proto(column: &SqlColumn) -> SqlColumnDescription {
    SqlColumnDescription {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        description: column.description.clone(),
    }
}

fn sql_input_to_proto(input: &SqlInput) -> SqlInputDescription {
    SqlInputDescription {
        name: input.name.clone(),
        required: input.required,
        data_type: input.data_type.clone(),
    }
}

fn diagnostic_to_proto(diagnostic: &Diagnostic) -> ExportDiagnosticDescription {
    ExportDiagnosticDescription {
        code: diagnostic.code.clone(),
        severity: diagnostic_severity_to_text(diagnostic.severity).to_string(),
        stage: diagnostic_stage_to_text(diagnostic.stage).to_string(),
        message: diagnostic.message.clone(),
        source_ref: diagnostic.source_ref.clone().unwrap_or_default(),
        details: Some(json_value_to_proto(diagnostic.details.clone())),
    }
}

fn candidate_to_proto(entry: &CapabilityExport) -> DescribeExportCandidate {
    DescribeExportCandidate {
        alias: entry
            .bindings
            .first()
            .map(binding_alias)
            .unwrap_or_default(),
        capability_id: entry.capability_id.to_string(),
        refs: binding_refs(entry),
        binding_kinds: binding_kinds(entry)
            .into_iter()
            .map(export_kind_to_proto)
            .map(|kind| kind as i32)
            .collect(),
        full_path: entry
            .bindings
            .iter()
            .find_map(Binding::full_path)
            .unwrap_or_default(),
        deprecated: entry.deprecated,
        support_status: support_status_to_text(entry.support_status).to_string(),
    }
}

fn generated_tool_path(path: &[String]) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("tools.{}", path.join("."))
    }
}

fn optional_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn export_kind_from_proto(kind: ExportBindingKind) -> Option<ExportKind> {
    match kind {
        ExportBindingKind::Unspecified => None,
        ExportBindingKind::Typescript => Some(ExportKind::Typescript),
        ExportBindingKind::SqlTable => Some(ExportKind::SqlTable),
        ExportBindingKind::SqlFunction => Some(ExportKind::SqlFunction),
    }
}

fn export_kinds_from_proto(kinds: &[i32]) -> Result<Vec<ExportKind>, Status> {
    kinds
        .iter()
        .map(|kind| {
            ExportBindingKind::try_from(*kind)
                .map_err(|error| {
                    Status::invalid_argument(format!("unknown export binding kind {kind}: {error}"))
                })
                .map(export_kind_from_proto)
        })
        .filter_map(Result::transpose)
        .collect()
}

fn export_kind_to_proto(kind: ExportKind) -> ExportBindingKind {
    match kind {
        ExportKind::Typescript => ExportBindingKind::Typescript,
        ExportKind::SqlTable => ExportBindingKind::SqlTable,
        ExportKind::SqlFunction => ExportBindingKind::SqlFunction,
    }
}

fn sql_binding_kind_to_proto(kind: SqlBindingKind) -> ExportBindingKind {
    match kind {
        SqlBindingKind::Table => ExportBindingKind::SqlTable,
        SqlBindingKind::Function => ExportBindingKind::SqlFunction,
    }
}

fn sql_row_shape_to_text(shape: SqlRowShape) -> &'static str {
    match shape {
        SqlRowShape::Collection => "collection",
        SqlRowShape::Singleton => "singleton",
    }
}

fn diagnostic_severity_to_text(severity: DiagnosticSeverity) -> &'static str {
    match severity {
        DiagnosticSeverity::Info => "info",
        DiagnosticSeverity::Warning => "warning",
        DiagnosticSeverity::Error => "error",
    }
}

fn diagnostic_stage_to_text(stage: DiagnosticStage) -> &'static str {
    match stage {
        DiagnosticStage::SourceSpec => "source_spec",
        DiagnosticStage::ProviderImport => "provider_import",
        DiagnosticStage::CapabilityGeneration => "capability_generation",
        DiagnosticStage::ExportGeneration => "export_generation",
        DiagnosticStage::SqlProjection => "sql_projection",
        DiagnosticStage::Materialization => "materialization",
        DiagnosticStage::Runtime => "runtime",
    }
}

fn optional_capability_kind(value: &str) -> Result<Option<CapabilityKind>, Status> {
    match value.trim() {
        "" => Ok(None),
        "query" => Ok(Some(CapabilityKind::Query)),
        "mutation" => Ok(Some(CapabilityKind::Mutation)),
        "action" => Ok(Some(CapabilityKind::Action)),
        other => Err(app_status(AppError::InvalidInput(format!(
            "unsupported capability_kind '{other}'"
        )))),
    }
}

fn optional_effect(value: &str) -> Result<Option<EffectKind>, Status> {
    match value.trim() {
        "" => Ok(None),
        "read" => Ok(Some(EffectKind::Read)),
        "write" => Ok(Some(EffectKind::Write)),
        "delete" => Ok(Some(EffectKind::Delete)),
        "unknown" => Ok(Some(EffectKind::Unknown)),
        other => Err(app_status(AppError::InvalidInput(format!(
            "unsupported effect '{other}'"
        )))),
    }
}

fn capability_kind_to_text(kind: CapabilityKind) -> &'static str {
    match kind {
        CapabilityKind::Query => "query",
        CapabilityKind::Mutation => "mutation",
        CapabilityKind::Action => "action",
    }
}

fn effect_to_text(effect: EffectKind) -> &'static str {
    match effect {
        EffectKind::Read => "read",
        EffectKind::Write => "write",
        EffectKind::Delete => "delete",
        EffectKind::Unknown => "unknown",
    }
}

fn support_status_to_text(status: SupportStatus) -> &'static str {
    match status {
        SupportStatus::Generated => "generated",
        SupportStatus::GeneratedPartial => "generated_partial",
        SupportStatus::PartiallySupported => "partially_supported",
        SupportStatus::Unsupported => "unsupported",
        SupportStatus::Deprecated => "deprecated",
    }
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{CapabilityId, IdempotencyKind, SourceId};
    use coral_exports::{CapabilityExport, EffectProfileSnapshot, SearchResult, SourceKey};

    use super::*;

    fn deprecated_export() -> CapabilityExport {
        CapabilityExport {
            capability_id: CapabilityId(
                "source/src_demo/interface/graph/operation/query_old".to_string(),
            ),
            source_id: SourceId("src_demo".to_string()),
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
            interface_id: "graph".to_string(),
            operation_id: "query_old".to_string(),
            title: "Query old".to_string(),
            description: "Deprecated provider query.".to_string(),
            deprecated: true,
            support_status: SupportStatus::Deprecated,
            bindings: Vec::new(),
            search_text: Vec::new(),
            effect_profile: EffectProfileSnapshot {
                capability_kind: CapabilityKind::Query,
                effects: vec![EffectKind::Read],
                idempotency: IdempotencyKind::Idempotent,
            },
            diagnostics: Vec::new(),
            binding_diagnostics: Vec::new(),
        }
    }

    #[test]
    fn public_discovery_proto_preserves_deprecated_status() {
        let entry = deprecated_export();
        let search = search_item_to_proto(SearchResult {
            alias: None,
            full_path: None,
            capability_id: entry.capability_id.clone(),
            refs: Vec::new(),
            source_id: entry.source_id.clone(),
            display_name: entry.display_name.clone(),
            source_key: entry.source_key.as_str().to_string(),
            capability_kind: entry.effect_profile.capability_kind,
            effects: entry.effect_profile.effects.clone(),
            title: entry.title.clone(),
            description: entry.description.clone(),
            deprecated: entry.deprecated,
            support_status: entry.support_status,
            available_bindings: Vec::new(),
            diagnostic_count: 0,
            score: 1,
            matched_fields: Vec::new(),
            rank_reason: "test".to_string(),
        });
        assert!(search.deprecated);
        assert_eq!(search.support_status, "deprecated");

        let candidate = candidate_to_proto(&entry);
        assert!(candidate.deprecated);
        assert_eq!(candidate.support_status, "deprecated");

        let description = description_to_proto(crate::discovery::manager::DiscoveryDescription {
            entry,
            capability: None,
        })
        .expect("description");
        assert!(description.deprecated);
        assert_eq!(description.support_status, "deprecated");
    }
}
