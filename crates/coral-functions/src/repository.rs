use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, PrincipalKind};
use coral_workspace::{TraceId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use crate::Error;
use crate::types::{
    AdministrativeLifecycleEvent, ExactSavedFunctionAuthoringMaterial,
    ExactSavedFunctionDescriptionMaterial, LifecycleAction, LifecycleOutcome,
    SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT, SAVED_FUNCTION_DIAGNOSTIC_MESSAGE_MAX_BYTES,
    SavedFunction, SavedFunctionAuthoringMaterial, SavedFunctionDeclaration,
    SavedFunctionDeclarationFieldProjection, SavedFunctionDeclarationSearchMaterial,
    SavedFunctionDescription, SavedFunctionDiagnostic, SavedFunctionEntry, SavedFunctionId,
    SavedFunctionPage, SavedFunctionPath, SavedFunctionSearchMaterial, SavedFunctionSelection,
    SavedFunctionSource, SavedFunctionSummary,
};

#[derive(Iden)]
enum SavedFunctionRecords {
    Table,
    Id,
    WorkspaceId,
    PrincipalId,
    PrincipalKind,
    Path,
    Description,
    Source,
    Declaration,
    DeclarationFieldsFormat,
    DeclarationFieldsStatus,
    DeclarationFieldsJson,
    DeclarationFieldsFailure,
    DiagnosticsJson,
    DiagnosticsTruncated,
    SavedAtUnixNanos,
    DefinitionRevision,
}

#[derive(Iden)]
enum SavedFunctionLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorPrincipalId,
    ActorPrincipalKind,
    PrivateUserId,
    Action,
    TargetSavedFunctionId,
    TargetPath,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

#[derive(Debug, FromRow)]
struct SavedFunctionRow {
    id: String,
    workspace_id: String,
    principal_id: String,
    principal_kind: String,
    path: String,
    description: String,
    source: String,
    declaration: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
    diagnostics_json: String,
    diagnostics_truncated: bool,
    saved_at_unix_nanos: i64,
    definition_revision: i64,
}

impl SavedFunctionRow {
    fn decode(self) -> Result<SavedFunction, Error> {
        let id = SavedFunctionId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid identity".to_string())
        })?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid Workspace identity".to_string())
        })?;
        let principal_id = PrincipalId::parse(&self.principal_id).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid Principal identity".to_string())
        })?;
        let principal = Principal::from_parts(
            principal_id,
            principal_kind_from_storage(&self.principal_kind)?,
        );
        let path = SavedFunctionPath::parse(&self.path)
            .map_err(|_error| Error::CorruptData("Saved Function has invalid path".to_string()))?;
        let description = SavedFunctionDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid description".to_string())
        })?;
        let source = SavedFunctionSource::parse(&self.source).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid source".to_string())
        })?;
        let declaration = SavedFunctionDeclaration::from_persisted(&self.declaration)?;
        let declaration_field_projection = SavedFunctionDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )?;
        let diagnostics = decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?;
        let revision = u64::try_from(self.definition_revision)
            .ok()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| {
                Error::CorruptData("Saved Function has invalid definition revision".to_string())
            })?;
        Ok(SavedFunction::new(
            id,
            workspace_id,
            principal,
            path,
            description,
            source,
            declaration,
            declaration_field_projection,
            diagnostics,
            self.diagnostics_truncated,
            self.saved_at_unix_nanos,
            revision,
        ))
    }
}

#[derive(Debug, FromRow)]
struct SavedFunctionSearchRow {
    id: String,
    path: String,
    description: String,
}

impl SavedFunctionSearchRow {
    fn decode(self) -> Result<SavedFunctionSearchMaterial, Error> {
        let id = SavedFunctionId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Saved Function search target has invalid identity".to_string())
        })?;
        let path = SavedFunctionPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Saved Function search target has invalid path".to_string())
        })?;
        let description = SavedFunctionDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Saved Function search target has invalid description".to_string())
        })?;
        Ok(SavedFunctionSearchMaterial::new(id, path, description))
    }
}

#[derive(Debug, FromRow)]
struct SavedFunctionSummaryRow {
    path: String,
    description: String,
    saved_at_unix_nanos: i64,
}

impl SavedFunctionSummaryRow {
    fn decode(self) -> Result<SavedFunctionSummary, Error> {
        let path = SavedFunctionPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Saved Function summary has invalid path".to_string())
        })?;
        let description = SavedFunctionDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Saved Function summary has invalid description".to_string())
        })?;
        Ok(SavedFunctionSummary::new(
            path,
            description,
            self.saved_at_unix_nanos,
        ))
    }
}

#[derive(Debug, FromRow)]
struct SavedFunctionDeclarationSearchRow {
    path: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
}

impl SavedFunctionDeclarationSearchRow {
    fn decode(self) -> Result<SavedFunctionDeclarationSearchMaterial, Error> {
        let path = SavedFunctionPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Saved Function search lane has invalid path".to_string())
        })?;
        let declaration_fields = SavedFunctionDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )
        .ok()
        .and_then(|projection| projection.fields().map(<[_]>::to_vec));
        Ok(SavedFunctionDeclarationSearchMaterial::new(
            path,
            declaration_fields,
        ))
    }
}

#[derive(Debug, FromRow)]
struct SavedFunctionEntryRow {
    id: String,
    workspace_id: String,
    principal_id: String,
    principal_kind: String,
    path: String,
    description: String,
    declaration: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
    definition_revision: i64,
}

impl SavedFunctionEntryRow {
    fn decode(self) -> Result<SavedFunctionEntry, Error> {
        let id = SavedFunctionId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Saved Function entry has invalid identity".to_string())
        })?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Saved Function entry has invalid Workspace identity".to_string())
        })?;
        let principal_id = PrincipalId::parse(&self.principal_id).map_err(|_error| {
            Error::CorruptData("Saved Function entry has invalid Principal identity".to_string())
        })?;
        let principal = Principal::from_parts(
            principal_id,
            principal_kind_from_storage(&self.principal_kind)?,
        );
        let path = SavedFunctionPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Saved Function entry has invalid path".to_string())
        })?;
        let description = SavedFunctionDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Saved Function entry has invalid description".to_string())
        })?;
        let declaration = SavedFunctionDeclaration::from_persisted(&self.declaration)?;
        // As in the relevance lane, an unreadable projection stays isolated to
        // its one function, which remains searchable by path and description.
        let declaration_fields = SavedFunctionDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )
        .ok()
        .and_then(|projection| projection.fields().map(<[_]>::to_vec));
        let revision = u64::try_from(self.definition_revision)
            .ok()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| {
                Error::CorruptData(
                    "Saved Function entry has invalid definition revision".to_string(),
                )
            })?;
        Ok(SavedFunctionEntry::new(
            id,
            workspace_id,
            principal,
            path,
            description,
            declaration,
            declaration_fields,
            revision,
        ))
    }
}

#[derive(Debug, FromRow)]
struct SavedFunctionAuthoringRow {
    path: String,
    description: String,
    declaration: String,
}

impl SavedFunctionAuthoringRow {
    fn decode(self) -> Result<SavedFunctionAuthoringMaterial, Error> {
        let path = SavedFunctionPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Saved Function authoring material has invalid path".to_string())
        })?;
        let description = SavedFunctionDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData(
                "Saved Function authoring material has invalid description".to_string(),
            )
        })?;
        let declaration = SavedFunctionDeclaration::from_persisted(&self.declaration)?;
        Ok(SavedFunctionAuthoringMaterial::new(
            path,
            description,
            declaration,
        ))
    }
}

#[derive(Debug, FromRow)]
struct ExactSavedFunctionDescriptionRow {
    path: String,
    description: String,
    declaration: String,
    source: String,
    diagnostics_json: String,
    diagnostics_truncated: bool,
}

impl ExactSavedFunctionDescriptionRow {
    fn decode(self) -> Result<ExactSavedFunctionDescriptionMaterial, Error> {
        let authoring = SavedFunctionAuthoringRow {
            path: self.path,
            description: self.description,
            declaration: self.declaration,
        }
        .decode()?;
        let source = SavedFunctionSource::parse(&self.source).map_err(|_error| {
            Error::CorruptData("Saved Function has invalid source".to_string())
        })?;
        let diagnostics = decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?;
        Ok(ExactSavedFunctionDescriptionMaterial::new(
            authoring,
            ExactSavedFunctionAuthoringMaterial::new(
                source,
                diagnostics,
                self.diagnostics_truncated,
            ),
        ))
    }
}

#[derive(Debug, FromRow)]
struct LifecycleEventRow {
    workspace_id: String,
    actor_principal_id: String,
    actor_principal_kind: String,
    action: String,
    target_saved_function_id: String,
    target_path: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Saved Function lifecycle Event has invalid Workspace".to_string())
        })?;
        let actor_id = PrincipalId::parse(&self.actor_principal_id).map_err(|_error| {
            Error::CorruptData("Saved Function lifecycle Event has invalid actor".to_string())
        })?;
        let actor = Principal::from_parts(
            actor_id,
            principal_kind_from_storage(&self.actor_principal_kind)?,
        );
        let target_id =
            SavedFunctionId::parse(&self.target_saved_function_id).map_err(|_error| {
                Error::CorruptData("Saved Function lifecycle Event has invalid target".to_string())
            })?;
        let target_path = SavedFunctionPath::parse(&self.target_path).map_err(|_error| {
            Error::CorruptData("Saved Function lifecycle Event has invalid target path".to_string())
        })?;
        let changed_field_paths =
            serde_json::from_str(&self.changed_field_paths).map_err(|error| {
                Error::CorruptData(format!(
                    "Saved Function lifecycle Event has invalid changed fields: {error}"
                ))
            })?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|message| {
            Error::CorruptData(format!("Saved Function lifecycle Event {message}"))
        })?;
        Ok(AdministrativeLifecycleEvent::new(
            workspace_id,
            actor,
            LifecycleAction::from_storage_str(&self.action)?,
            LifecycleOutcome::from_storage_str(&self.outcome)?,
            target_id,
            target_path,
            changed_field_paths,
            self.occurred_at_unix_nanos,
            trace_id,
        ))
    }
}

pub(crate) async fn find_current_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<SavedFunction>, Error> {
    let row: Option<SavedFunctionRow> = transaction
        .fetch_optional(current_statement(workspace_id, principal, path))
        .await?;
    row.map(SavedFunctionRow::decode).transpose()
}

pub(crate) async fn hold_current_path_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(SavedFunctionRecords::Table)
        .value(
            SavedFunctionRecords::DefinitionRevision,
            Expr::col(SavedFunctionRecords::DefinitionRevision),
        )
        .and_where(Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Path).eq(path.as_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) async fn list_current_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<SavedFunction>, Error> {
    let rows: Vec<SavedFunctionRow> = transaction
        .fetch_all(
            Query::select()
                .columns(saved_function_columns())
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SavedFunctionRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .order_by(SavedFunctionRecords::Path, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(SavedFunctionRow::decode).collect()
}

/// Reads one page of source-free summaries after `after` in path order.
pub(crate) async fn list_summary_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    after: Option<&SavedFunctionPath>,
    page_size: u32,
) -> Result<SavedFunctionPage, Error> {
    // Match Rust string ordering regardless of the Postgres locale, so the
    // exclusive cursor predicate agrees with the page order. SQLite's TEXT
    // columns use BINARY by default.
    let path = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => Expr::col(SavedFunctionRecords::Path),
        coral_db::DatabaseSystem::Postgres => Expr::cust("\"path\" COLLATE \"C\""),
    };
    let mut statement = Query::select();
    statement
        .columns([
            SavedFunctionRecords::Path,
            SavedFunctionRecords::Description,
            SavedFunctionRecords::SavedAtUnixNanos,
        ])
        .from(SavedFunctionRecords::Table)
        .and_where(Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .order_by_expr(path.clone(), Order::Asc)
        // One extra row shows whether another page follows.
        .limit(u64::from(page_size) + 1);
    if let Some(after) = after {
        statement.and_where(path.gt(after.as_str()));
    }
    let rows: Vec<SavedFunctionSummaryRow> = transaction.fetch_all(statement).await?;
    let mut summaries = rows
        .into_iter()
        .map(SavedFunctionSummaryRow::decode)
        .collect::<Result<Vec<_>, _>>()?;
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    let next_after = if summaries.len() > page_size {
        summaries.truncate(page_size);
        summaries.last().map(|summary| summary.path().clone())
    } else {
        None
    };
    Ok(SavedFunctionPage::new(summaries, next_after))
}

pub(crate) async fn list_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<SavedFunctionSearchMaterial>, Error> {
    let rows: Vec<SavedFunctionSearchRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    SavedFunctionRecords::Id,
                    SavedFunctionRecords::Path,
                    SavedFunctionRecords::Description,
                ])
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SavedFunctionRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .order_by(SavedFunctionRecords::Path, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(SavedFunctionSearchRow::decode)
        .collect()
}

pub(crate) async fn declaration_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<SavedFunctionDeclarationSearchMaterial>, Error> {
    let row: Option<SavedFunctionDeclarationSearchRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    SavedFunctionRecords::Path,
                    SavedFunctionRecords::DeclarationFieldsFormat,
                    SavedFunctionRecords::DeclarationFieldsStatus,
                    SavedFunctionRecords::DeclarationFieldsJson,
                    SavedFunctionRecords::DeclarationFieldsFailure,
                ])
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SavedFunctionRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(SavedFunctionRecords::Path).eq(path.as_str()))
                .to_owned(),
        )
        .await?;
    row.map(SavedFunctionDeclarationSearchRow::decode)
        .transpose()
}

pub(crate) async fn list_workspace_entries_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<SavedFunctionEntry>, Error> {
    let rows: Vec<SavedFunctionEntryRow> = transaction
        .fetch_all(
            entries_statement()
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(SavedFunctionEntryRow::decode)
        .collect()
}

pub(crate) async fn list_every_entry_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<SavedFunctionEntry>, Error> {
    let rows: Vec<SavedFunctionEntryRow> = transaction.fetch_all(entries_statement()).await?;
    rows.into_iter()
        .map(SavedFunctionEntryRow::decode)
        .collect()
}

fn entries_statement() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            SavedFunctionRecords::Id,
            SavedFunctionRecords::WorkspaceId,
            SavedFunctionRecords::PrincipalId,
            SavedFunctionRecords::PrincipalKind,
            SavedFunctionRecords::Path,
            SavedFunctionRecords::Description,
            SavedFunctionRecords::Declaration,
            SavedFunctionRecords::DeclarationFieldsFormat,
            SavedFunctionRecords::DeclarationFieldsStatus,
            SavedFunctionRecords::DeclarationFieldsJson,
            SavedFunctionRecords::DeclarationFieldsFailure,
            SavedFunctionRecords::DefinitionRevision,
        ])
        .from(SavedFunctionRecords::Table)
        .order_by(SavedFunctionRecords::WorkspaceId, Order::Asc)
        .order_by(SavedFunctionRecords::PrincipalKind, Order::Asc)
        .order_by(SavedFunctionRecords::PrincipalId, Order::Asc)
        .order_by(SavedFunctionRecords::Path, Order::Asc)
        .to_owned()
}

pub(crate) async fn hydrate_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &SavedFunctionSearchMaterial,
) -> Result<Option<SavedFunctionAuthoringMaterial>, Error> {
    let row: Option<SavedFunctionAuthoringRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    SavedFunctionRecords::Path,
                    SavedFunctionRecords::Description,
                    SavedFunctionRecords::Declaration,
                ])
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SavedFunctionRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(SavedFunctionRecords::Id).eq(target.id().to_string()))
                .and_where(Expr::col(SavedFunctionRecords::Path).eq(target.path().as_str()))
                .to_owned(),
        )
        .await?;
    let material = row.map(SavedFunctionAuthoringRow::decode).transpose()?;
    if material
        .as_ref()
        .is_some_and(|material| material.description() != target.description())
    {
        return Err(Error::CorruptData(
            "Saved Function changed inside one search snapshot".to_string(),
        ));
    }
    Ok(material)
}

pub(crate) async fn exact_description_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<ExactSavedFunctionDescriptionMaterial>, Error> {
    let row: Option<ExactSavedFunctionDescriptionRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    SavedFunctionRecords::Path,
                    SavedFunctionRecords::Description,
                    SavedFunctionRecords::Declaration,
                    SavedFunctionRecords::Source,
                    SavedFunctionRecords::DiagnosticsJson,
                    SavedFunctionRecords::DiagnosticsTruncated,
                ])
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SavedFunctionRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(SavedFunctionRecords::Path).eq(path.as_str()))
                .to_owned(),
        )
        .await?;
    row.map(ExactSavedFunctionDescriptionRow::decode)
        .transpose()
}

pub(crate) async fn insert_current(
    transaction: &mut Transaction<'_>,
    function: &SavedFunction,
) -> Result<(), Error> {
    let diagnostics_json = serde_json::to_string(function.diagnostics())?;
    let revision = stored_revision(function.revision)?;
    let statement = Query::insert()
        .into_table(SavedFunctionRecords::Table)
        .columns([
            SavedFunctionRecords::Id,
            SavedFunctionRecords::WorkspaceId,
            SavedFunctionRecords::PrincipalId,
            SavedFunctionRecords::PrincipalKind,
            SavedFunctionRecords::Path,
            SavedFunctionRecords::Description,
            SavedFunctionRecords::Source,
            SavedFunctionRecords::Declaration,
            SavedFunctionRecords::DeclarationFieldsFormat,
            SavedFunctionRecords::DeclarationFieldsStatus,
            SavedFunctionRecords::DeclarationFieldsJson,
            SavedFunctionRecords::DeclarationFieldsFailure,
            SavedFunctionRecords::DiagnosticsJson,
            SavedFunctionRecords::DiagnosticsTruncated,
            SavedFunctionRecords::SavedAtUnixNanos,
            SavedFunctionRecords::DefinitionRevision,
        ])
        .values_panic([
            Expr::val(function.id().to_string()),
            Expr::val(function.workspace_id().to_string()),
            Expr::val(function.principal().id().as_str()),
            Expr::val(function.principal().kind().as_storage_str()),
            Expr::val(function.path().as_str()),
            Expr::val(function.description().as_str()),
            Expr::val(function.source().as_str()),
            Expr::val(function.declaration().as_str()),
            Expr::val(SavedFunctionDeclarationFieldProjection::format()),
            Expr::val(function.declaration_field_projection().status()),
            Expr::val(
                function
                    .declaration_field_projection()
                    .fields()
                    .map(serde_json::to_string)
                    .transpose()?,
            ),
            Expr::val(function.declaration_field_projection().failure_reason()),
            Expr::val(diagnostics_json),
            Expr::val(function.diagnostics_truncated()),
            Expr::val(function.saved_at_unix_nanos()),
            Expr::val(revision),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(crate) async fn replace_current(
    transaction: &mut Transaction<'_>,
    previous: &SavedFunction,
    replacement: &SavedFunction,
) -> Result<(), Error> {
    let diagnostics_json = serde_json::to_string(replacement.diagnostics())?;
    let previous_revision = stored_revision(previous.revision)?;
    let replacement_revision = stored_revision(replacement.revision)?;
    let statement = Query::update()
        .table(SavedFunctionRecords::Table)
        .values([
            (
                SavedFunctionRecords::Description,
                Expr::val(replacement.description().as_str()),
            ),
            (
                SavedFunctionRecords::Source,
                Expr::val(replacement.source().as_str()),
            ),
            (
                SavedFunctionRecords::Declaration,
                Expr::val(replacement.declaration().as_str()),
            ),
            (
                SavedFunctionRecords::DeclarationFieldsFormat,
                Expr::val(SavedFunctionDeclarationFieldProjection::format()),
            ),
            (
                SavedFunctionRecords::DeclarationFieldsStatus,
                Expr::val(replacement.declaration_field_projection().status()),
            ),
            (
                SavedFunctionRecords::DeclarationFieldsJson,
                Expr::val(
                    replacement
                        .declaration_field_projection()
                        .fields()
                        .map(serde_json::to_string)
                        .transpose()?,
                ),
            ),
            (
                SavedFunctionRecords::DeclarationFieldsFailure,
                Expr::val(replacement.declaration_field_projection().failure_reason()),
            ),
            (
                SavedFunctionRecords::DiagnosticsJson,
                Expr::val(diagnostics_json),
            ),
            (
                SavedFunctionRecords::DiagnosticsTruncated,
                Expr::val(replacement.diagnostics_truncated()),
            ),
            (
                SavedFunctionRecords::SavedAtUnixNanos,
                Expr::val(replacement.saved_at_unix_nanos()),
            ),
            (
                SavedFunctionRecords::DefinitionRevision,
                Expr::val(replacement_revision),
            ),
        ])
        .and_where(
            Expr::col(SavedFunctionRecords::WorkspaceId).eq(previous.workspace_id().to_string()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Id).eq(previous.id().to_string()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalId).eq(previous.principal().id().as_str()),
        )
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind)
                .eq(previous.principal().kind().as_storage_str()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Path).eq(previous.path().as_str()))
        .and_where(Expr::col(SavedFunctionRecords::DefinitionRevision).eq(previous_revision))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "Saved Function replacement lost its held current definition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn delete_current(
    transaction: &mut Transaction<'_>,
    function: &SavedFunction,
) -> Result<(), Error> {
    let statement = Query::delete()
        .from_table(SavedFunctionRecords::Table)
        .and_where(
            Expr::col(SavedFunctionRecords::WorkspaceId).eq(function.workspace_id().to_string()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Id).eq(function.id().to_string()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalId).eq(function.principal().id().as_str()),
        )
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind)
                .eq(function.principal().kind().as_storage_str()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Path).eq(function.path().as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::DefinitionRevision)
                .eq(stored_revision(function.revision)?),
        )
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "Saved Function deletion lost its held current definition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) struct NewLifecycleEvent<'a> {
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) actor: &'a Principal,
    pub(crate) private_user_id: Option<&'a PrincipalId>,
    pub(crate) action: LifecycleAction,
    pub(crate) target_id: &'a SavedFunctionId,
    pub(crate) target_path: &'a SavedFunctionPath,
    pub(crate) changed_field_paths: &'a [&'a str],
    pub(crate) occurred_at_unix_nanos: i64,
    pub(crate) trace_id: &'a TraceId,
}

pub(crate) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    let changed_field_paths = serde_json::to_string(event.changed_field_paths)?;
    let statement = Query::insert()
        .into_table(SavedFunctionLifecycleEvents::Table)
        .columns([
            SavedFunctionLifecycleEvents::Id,
            SavedFunctionLifecycleEvents::WorkspaceId,
            SavedFunctionLifecycleEvents::ActorPrincipalId,
            SavedFunctionLifecycleEvents::ActorPrincipalKind,
            SavedFunctionLifecycleEvents::PrivateUserId,
            SavedFunctionLifecycleEvents::Action,
            SavedFunctionLifecycleEvents::TargetSavedFunctionId,
            SavedFunctionLifecycleEvents::TargetPath,
            SavedFunctionLifecycleEvents::Outcome,
            SavedFunctionLifecycleEvents::ChangedFieldPaths,
            SavedFunctionLifecycleEvents::OccurredAtUnixNanos,
            SavedFunctionLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(event.workspace_id.to_string()),
            Expr::val(event.actor.id().as_str()),
            Expr::val(event.actor.kind().as_storage_str()),
            Expr::val(event.private_user_id.map(PrincipalId::as_str)),
            Expr::val(event.action.as_storage_str()),
            Expr::val(event.target_id.to_string()),
            Expr::val(event.target_path.as_str()),
            Expr::val(LifecycleOutcome::Accepted.as_storage_str()),
            Expr::val(changed_field_paths),
            Expr::val(event.occurred_at_unix_nanos),
            Expr::val(event.trace_id.as_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(crate) async fn list_lifecycle_events_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let statement = Query::select()
        .columns([
            SavedFunctionLifecycleEvents::WorkspaceId,
            SavedFunctionLifecycleEvents::ActorPrincipalId,
            SavedFunctionLifecycleEvents::ActorPrincipalKind,
            SavedFunctionLifecycleEvents::Action,
            SavedFunctionLifecycleEvents::TargetSavedFunctionId,
            SavedFunctionLifecycleEvents::TargetPath,
            SavedFunctionLifecycleEvents::Outcome,
            SavedFunctionLifecycleEvents::ChangedFieldPaths,
            SavedFunctionLifecycleEvents::OccurredAtUnixNanos,
            SavedFunctionLifecycleEvents::TraceId,
        ])
        .from(SavedFunctionLifecycleEvents::Table)
        .and_where(
            Expr::col(SavedFunctionLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()),
        )
        .and_where(
            Expr::col(SavedFunctionLifecycleEvents::PrivateUserId)
                .eq(principal.id().as_str())
                .or(Expr::col(SavedFunctionLifecycleEvents::PrivateUserId)
                    .is_null()
                    .and(
                        Expr::col(SavedFunctionLifecycleEvents::ActorPrincipalId)
                            .eq(principal.id().as_str()),
                    )
                    .and(
                        Expr::col(SavedFunctionLifecycleEvents::ActorPrincipalKind)
                            .eq(principal.kind().as_storage_str()),
                    )),
        )
        .order_by(
            SavedFunctionLifecycleEvents::OccurredAtUnixNanos,
            Order::Asc,
        )
        .order_by(SavedFunctionLifecycleEvents::Id, Order::Asc)
        .to_owned();
    let rows: Vec<LifecycleEventRow> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}

/// Selects exact current source inside the admission owner's read snapshot.
///
/// The caller must first authorize the Principal in this Workspace, use the same
/// snapshot for the complete closure, and recheck every selection at acceptance.
/// This facet neither commits nor creates retained Run history.
///
/// # Errors
///
/// Returns persistence or corrupt-definition failures.
pub async fn select_source_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<SavedFunctionSelection>, Error> {
    find_current_in_transaction(transaction, workspace_id, principal, path)
        .await
        .map(|function| function.as_ref().map(SavedFunctionSelection::from_current))
}

/// Holds one Saved Function shared through the caller's write transaction;
/// `false` means it is gone.
pub(crate) async fn share_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &SavedFunctionId,
) -> Result<bool, Error> {
    let held: Option<(String,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(SavedFunctionRecords::Id)
                .from(SavedFunctionRecords::Table)
                .and_where(
                    Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(SavedFunctionRecords::Id).eq(id.to_string()))
                .to_owned(),
            coral_db::RowLock::Shared,
        )
        .await?;
    Ok(held.is_some())
}

/// Holds a selected definition unchanged through the caller's acceptance commit.
///
/// The caller must hold current Workspace authority in this write transaction and
/// pass the same Workspace and Principal used for selection. `false` means the
/// definition was replaced or deleted; admission must reject without a Run.
/// The revision and locking mechanics remain private to Functions.
///
/// # Errors
///
/// Returns persistence or corrupt-selection failures.
pub async fn hold_source_selection_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    snapshot: &SavedFunctionSelection,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(SavedFunctionRecords::Table)
        .value(
            SavedFunctionRecords::DefinitionRevision,
            Expr::col(SavedFunctionRecords::DefinitionRevision),
        )
        .and_where(Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SavedFunctionRecords::Id).eq(snapshot.id.to_string()))
        .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Path).eq(snapshot.path.as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::DefinitionRevision)
                .eq(stored_revision(snapshot.revision)?),
        )
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

fn decode_diagnostics(json: &str, truncated: bool) -> Result<Vec<SavedFunctionDiagnostic>, Error> {
    let diagnostics: Vec<SavedFunctionDiagnostic> =
        serde_json::from_str(json).map_err(|error| {
            Error::CorruptData(format!("Saved Function has invalid diagnostics: {error}"))
        })?;
    if diagnostics.len() > SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT
        || diagnostics.iter().any(|diagnostic| {
            diagnostic.message().len() > SAVED_FUNCTION_DIAGNOSTIC_MESSAGE_MAX_BYTES
        })
        || (truncated && diagnostics.len() != SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT)
    {
        return Err(Error::CorruptData(
            "Saved Function diagnostics exceed their stored bounds".to_string(),
        ));
    }
    Ok(diagnostics)
}

fn current_statement(
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> sea_query::SelectStatement {
    Query::select()
        .columns(saved_function_columns())
        .from(SavedFunctionRecords::Table)
        .and_where(Expr::col(SavedFunctionRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(SavedFunctionRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(SavedFunctionRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(SavedFunctionRecords::Path).eq(path.as_str()))
        .to_owned()
}

fn saved_function_columns() -> [SavedFunctionRecords; 16] {
    [
        SavedFunctionRecords::Id,
        SavedFunctionRecords::WorkspaceId,
        SavedFunctionRecords::PrincipalId,
        SavedFunctionRecords::PrincipalKind,
        SavedFunctionRecords::Path,
        SavedFunctionRecords::Description,
        SavedFunctionRecords::Source,
        SavedFunctionRecords::Declaration,
        SavedFunctionRecords::DeclarationFieldsFormat,
        SavedFunctionRecords::DeclarationFieldsStatus,
        SavedFunctionRecords::DeclarationFieldsJson,
        SavedFunctionRecords::DeclarationFieldsFailure,
        SavedFunctionRecords::DiagnosticsJson,
        SavedFunctionRecords::DiagnosticsTruncated,
        SavedFunctionRecords::SavedAtUnixNanos,
        SavedFunctionRecords::DefinitionRevision,
    ]
}

fn principal_kind_from_storage(value: &str) -> Result<PrincipalKind, Error> {
    PrincipalKind::from_storage_str(value).map_err(|_error| {
        Error::CorruptData(format!(
            "Saved Function has unrecognized Principal kind '{value}'"
        ))
    })
}

fn stored_revision(revision: u64) -> Result<i64, Error> {
    i64::try_from(revision)
        .map_err(|_error| Error::CorruptData("Saved Function revision overflowed".to_string()))
}
