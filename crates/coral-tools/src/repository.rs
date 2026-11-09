use coral_catalog::ToolAccessState;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, PrincipalKind};
use coral_workspace::{TraceId, WorkspaceId};
use sea_query::{Cond, Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use crate::Error;
use crate::types::{
    AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT, AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES,
    AdministrativeLifecycleEvent, AuthoredToolDeclaration, AuthoredToolDeclarationFieldProjection,
    AuthoredToolDescription, AuthoredToolDiagnostic, AuthoredToolSource,
    ExactPrivateToolAuthoringMaterial, ExactPrivateToolDescriptionMaterial, LifecycleAction,
    LifecycleOutcome, PrivateTool, PrivateToolAuthoringMaterial,
    PrivateToolDeclarationSearchMaterial, PrivateToolEntry, PrivateToolId, PrivateToolPage,
    PrivateToolPath, PrivateToolSearchMaterial, PrivateToolSelection, PrivateToolSummary,
};

#[derive(Iden)]
enum PrivateToolRecords {
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
    DefaultAccessState,
}

#[derive(Iden)]
enum PrivateToolLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorPrincipalId,
    ActorPrincipalKind,
    PrivateUserId,
    Action,
    TargetPrivateToolId,
    TargetPath,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

#[derive(Debug, FromRow)]
struct PrivateToolRow {
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
    default_access_state: String,
}

impl PrivateToolRow {
    fn decode(self) -> Result<PrivateTool, Error> {
        let id = PrivateToolId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Private Tool has invalid identity".to_string())
        })?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Private Tool has invalid Workspace identity".to_string())
        })?;
        let principal_id = PrincipalId::parse(&self.principal_id).map_err(|_error| {
            Error::CorruptData("Private Tool has invalid Principal identity".to_string())
        })?;
        let principal = Principal::from_parts(
            principal_id,
            principal_kind_from_storage(&self.principal_kind)?,
        );
        let path = PrivateToolPath::parse(&self.path)
            .map_err(|_error| Error::CorruptData("Private Tool has invalid path".to_string()))?;
        let description = AuthoredToolDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Private Tool has invalid description".to_string())
        })?;
        let source = AuthoredToolSource::parse(&self.source)
            .map_err(|_error| Error::CorruptData("Private Tool has invalid source".to_string()))?;
        let declaration = AuthoredToolDeclaration::from_persisted(&self.declaration)?;
        let declaration_field_projection = AuthoredToolDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )?;
        let diagnostics = decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?;
        let revision = decode_revision(self.definition_revision)?;
        Ok(PrivateTool::new(
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
            access_state_from_storage(&self.default_access_state)?,
        ))
    }
}

#[derive(Debug, FromRow)]
struct PrivateToolSearchRow {
    id: String,
    path: String,
    description: String,
    default_access_state: String,
}

impl PrivateToolSearchRow {
    fn decode(self) -> Result<PrivateToolSearchMaterial, Error> {
        let id = PrivateToolId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Private Tool search target has invalid identity".to_string())
        })?;
        let path = PrivateToolPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Private Tool search target has invalid path".to_string())
        })?;
        let description = AuthoredToolDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Private Tool search target has invalid description".to_string())
        })?;
        Ok(PrivateToolSearchMaterial::new(
            id,
            path,
            description,
            access_state_from_storage(&self.default_access_state)?,
        ))
    }
}

#[derive(Debug, FromRow)]
struct PrivateToolSummaryRow {
    path: String,
    description: String,
    saved_at_unix_nanos: i64,
}

impl PrivateToolSummaryRow {
    fn decode(self) -> Result<PrivateToolSummary, Error> {
        let path = PrivateToolPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Private Tool summary has invalid path".to_string())
        })?;
        let description = AuthoredToolDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Private Tool summary has invalid description".to_string())
        })?;
        Ok(PrivateToolSummary::new(
            path,
            description,
            self.saved_at_unix_nanos,
        ))
    }
}

#[derive(Debug, FromRow)]
struct PrivateToolDeclarationSearchRow {
    path: String,
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
}

impl PrivateToolDeclarationSearchRow {
    fn decode(self) -> Result<PrivateToolDeclarationSearchMaterial, Error> {
        let path = PrivateToolPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Private Tool search lane has invalid path".to_string())
        })?;
        let declaration_fields = AuthoredToolDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )
        .ok()
        .and_then(|projection| projection.fields().map(<[_]>::to_vec));
        Ok(PrivateToolDeclarationSearchMaterial::new(
            path,
            declaration_fields,
        ))
    }
}

#[derive(Debug, FromRow)]
struct PrivateToolEntryRow {
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

impl PrivateToolEntryRow {
    fn decode(self) -> Result<PrivateToolEntry, Error> {
        let id = PrivateToolId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Private Tool entry has invalid identity".to_string())
        })?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Private Tool entry has invalid Workspace identity".to_string())
        })?;
        let principal_id = PrincipalId::parse(&self.principal_id).map_err(|_error| {
            Error::CorruptData("Private Tool entry has invalid Principal identity".to_string())
        })?;
        let principal = Principal::from_parts(
            principal_id,
            principal_kind_from_storage(&self.principal_kind)?,
        );
        let path = PrivateToolPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Private Tool entry has invalid path".to_string())
        })?;
        let description = AuthoredToolDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData("Private Tool entry has invalid description".to_string())
        })?;
        let declaration = AuthoredToolDeclaration::from_persisted(&self.declaration)?;
        // As in the relevance lane, an unreadable projection stays isolated to
        // its one Private Tool, which remains searchable by path and description.
        let declaration_fields = AuthoredToolDeclarationFieldProjection::from_storage(
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
                Error::CorruptData("Private Tool entry has invalid definition revision".to_string())
            })?;
        Ok(PrivateToolEntry::new(
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
struct PrivateToolAuthoringRow {
    path: String,
    description: String,
    declaration: String,
}

impl PrivateToolAuthoringRow {
    fn decode(self) -> Result<PrivateToolAuthoringMaterial, Error> {
        let path = PrivateToolPath::parse(&self.path).map_err(|_error| {
            Error::CorruptData("Private Tool authoring material has invalid path".to_string())
        })?;
        let description = AuthoredToolDescription::parse(&self.description).map_err(|_error| {
            Error::CorruptData(
                "Private Tool authoring material has invalid description".to_string(),
            )
        })?;
        let declaration = AuthoredToolDeclaration::from_persisted(&self.declaration)?;
        Ok(PrivateToolAuthoringMaterial::new(
            path,
            description,
            declaration,
        ))
    }
}

#[derive(Debug, FromRow)]
struct ExactPrivateToolDescriptionRow {
    id: String,
    path: String,
    description: String,
    declaration: String,
    source: String,
    diagnostics_json: String,
    diagnostics_truncated: bool,
    definition_revision: i64,
    default_access_state: String,
}

impl ExactPrivateToolDescriptionRow {
    fn decode(self) -> Result<ExactPrivateToolDescriptionMaterial, Error> {
        let authoring = PrivateToolAuthoringRow {
            path: self.path,
            description: self.description,
            declaration: self.declaration,
        }
        .decode()?;
        let source = AuthoredToolSource::parse(&self.source)
            .map_err(|_error| Error::CorruptData("Private Tool has invalid source".to_string()))?;
        let diagnostics = decode_diagnostics(&self.diagnostics_json, self.diagnostics_truncated)?;
        let id = PrivateToolId::parse(&self.id)
            .map_err(|_error| Error::CorruptData("Private Tool has an invalid id".to_string()))?;
        Ok(ExactPrivateToolDescriptionMaterial::new(
            id,
            authoring,
            ExactPrivateToolAuthoringMaterial::new(source, diagnostics, self.diagnostics_truncated),
            decode_revision(self.definition_revision)?,
            access_state_from_storage(&self.default_access_state)?,
        ))
    }
}

#[derive(Debug, FromRow)]
struct LifecycleEventRow {
    workspace_id: String,
    actor_principal_id: String,
    actor_principal_kind: String,
    action: String,
    target_private_tool_id: String,
    target_path: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Private Tool lifecycle Event has invalid Workspace".to_string())
        })?;
        let actor_id = PrincipalId::parse(&self.actor_principal_id).map_err(|_error| {
            Error::CorruptData("Private Tool lifecycle Event has invalid actor".to_string())
        })?;
        let actor = Principal::from_parts(
            actor_id,
            principal_kind_from_storage(&self.actor_principal_kind)?,
        );
        let target_id = PrivateToolId::parse(&self.target_private_tool_id).map_err(|_error| {
            Error::CorruptData("Private Tool lifecycle Event has invalid target".to_string())
        })?;
        let target_path = PrivateToolPath::parse(&self.target_path).map_err(|_error| {
            Error::CorruptData("Private Tool lifecycle Event has invalid target path".to_string())
        })?;
        let changed_field_paths =
            serde_json::from_str(&self.changed_field_paths).map_err(|error| {
                Error::CorruptData(format!(
                    "Private Tool lifecycle Event has invalid changed fields: {error}"
                ))
            })?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|message| {
            Error::CorruptData(format!("Private Tool lifecycle Event {message}"))
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
    path: &PrivateToolPath,
) -> Result<Option<PrivateTool>, Error> {
    let row: Option<PrivateToolRow> = transaction
        .fetch_optional(current_statement(workspace_id, principal, path))
        .await?;
    row.map(PrivateToolRow::decode).transpose()
}

pub(crate) async fn hold_current_path_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(PrivateToolRecords::Table)
        .value(
            PrivateToolRecords::DefinitionRevision,
            Expr::col(PrivateToolRecords::DefinitionRevision),
        )
        .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(PrivateToolRecords::Path).eq(path.as_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

/// The rows of the Private Tools within `scope` of `reader`: its own and,
/// for [`crate::PrivateToolScope::OwnAndAgents`], every Agent's.
fn readable_owners(reader: &Principal, scope: crate::PrivateToolScope) -> Cond {
    let owners = Cond::any().add(
        Cond::all()
            .add(Expr::col(PrivateToolRecords::PrincipalId).eq(reader.id().as_str()))
            .add(Expr::col(PrivateToolRecords::PrincipalKind).eq(reader.kind().as_storage_str())),
    );
    if scope == crate::PrivateToolScope::OwnAndAgents {
        owners.add(
            Expr::col(PrivateToolRecords::PrincipalKind).eq(PrincipalKind::Agent.as_storage_str()),
        )
    } else {
        owners
    }
}

/// One page of the Private Tools within `scope` of `reader`, in Tool Key
/// order after `after`, with one row more than `page_size` when another
/// page follows.
pub(crate) async fn readable_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    reader: &Principal,
    scope: crate::PrivateToolScope,
    after: Option<&coral_catalog::ToolKey>,
    page_size: u32,
) -> Result<Vec<crate::ReadablePrivateTool>, Error> {
    // Match Rust string ordering regardless of the Postgres locale, so the
    // exclusive cursor predicate agrees with the page order.
    let id = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => Expr::col(PrivateToolRecords::Id),
        coral_db::DatabaseSystem::Postgres => Expr::cust("\"id\" COLLATE \"C\""),
    };
    let mut condition = Cond::all()
        .add(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .add(readable_owners(reader, scope));
    if let Some(after) = after {
        condition = condition.add(id.clone().gt(after.as_str()));
    }
    let rows: Vec<(String, String, String, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    PrivateToolRecords::Id,
                    PrivateToolRecords::Path,
                    PrivateToolRecords::PrincipalKind,
                    PrivateToolRecords::PrincipalId,
                    PrivateToolRecords::DefaultAccessState,
                ])
                .from(PrivateToolRecords::Table)
                .cond_where(condition)
                .order_by_expr(id, Order::Asc)
                .limit(u64::from(page_size) + 1)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, path, kind, principal, state)| {
            let corrupt = || Error::CorruptData("Private Tool has an invalid row".to_string());
            let id = PrivateToolId::parse(&id).map_err(|_error| corrupt())?;
            let path = PrivateToolPath::parse(&path).map_err(|_error| corrupt())?;
            let agent = match PrincipalKind::from_storage_str(&kind).map_err(|_error| corrupt())? {
                PrincipalKind::Agent => {
                    Some(PrincipalId::parse(&principal).map_err(|_error| corrupt())?)
                }
                PrincipalKind::User => None,
            };
            Ok(crate::ReadablePrivateTool::new(
                id.tool_key(),
                path,
                agent,
                access_state_from_storage(&state)?,
            ))
        })
        .collect()
}

/// The default Tool Access State of each of `tools` that names a current
/// Private Tool of the Workspace within `scope` of `reader`.
pub(crate) async fn readable_defaults_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    reader: &Principal,
    scope: crate::PrivateToolScope,
    tools: &[coral_catalog::ToolKey],
) -> Result<std::collections::BTreeMap<coral_catalog::ToolKey, ToolAccessState>, Error> {
    let owners = readable_owners(reader, scope);
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    PrivateToolRecords::Id,
                    PrivateToolRecords::DefaultAccessState,
                ])
                .from(PrivateToolRecords::Table)
                .cond_where(
                    Cond::all()
                        .add(
                            Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()),
                        )
                        .add(
                            Expr::col(PrivateToolRecords::Id)
                                .is_in(tools.iter().map(|key| key.as_str().to_owned())),
                        )
                        .add(owners),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, state)| {
            let id = PrivateToolId::parse(&id).map_err(|_error| {
                Error::CorruptData("Private Tool has an invalid id".to_string())
            })?;
            Ok((id.tool_key(), access_state_from_storage(&state)?))
        })
        .collect()
}

pub(crate) async fn list_current_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<PrivateTool>, Error> {
    let rows: Vec<PrivateToolRow> = transaction
        .fetch_all(
            Query::select()
                .columns(private_tool_columns())
                .from(PrivateToolRecords::Table)
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(PrivateToolRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .order_by(PrivateToolRecords::Path, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(PrivateToolRow::decode).collect()
}

/// Reads one page of source-free summaries after `after` in path order.
pub(crate) async fn list_summary_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    after: Option<&PrivateToolPath>,
    page_size: u32,
) -> Result<PrivateToolPage, Error> {
    // Match Rust string ordering regardless of the Postgres locale, so the
    // exclusive cursor predicate agrees with the page order. SQLite's TEXT
    // columns use BINARY by default.
    let path = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => Expr::col(PrivateToolRecords::Path),
        coral_db::DatabaseSystem::Postgres => Expr::cust("\"path\" COLLATE \"C\""),
    };
    let mut statement = Query::select();
    statement
        .columns([
            PrivateToolRecords::Path,
            PrivateToolRecords::Description,
            PrivateToolRecords::SavedAtUnixNanos,
        ])
        .from(PrivateToolRecords::Table)
        .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .order_by_expr(path.clone(), Order::Asc)
        // One extra row shows whether another page follows.
        .limit(u64::from(page_size) + 1);
    if let Some(after) = after {
        statement.and_where(path.gt(after.as_str()));
    }
    let rows: Vec<PrivateToolSummaryRow> = transaction.fetch_all(statement).await?;
    let mut summaries = rows
        .into_iter()
        .map(PrivateToolSummaryRow::decode)
        .collect::<Result<Vec<_>, _>>()?;
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    let next_after = if summaries.len() > page_size {
        summaries.truncate(page_size);
        summaries.last().map(|summary| summary.path().clone())
    } else {
        None
    };
    Ok(PrivateToolPage::new(summaries, next_after))
}

pub(crate) async fn list_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<PrivateToolSearchMaterial>, Error> {
    let rows: Vec<PrivateToolSearchRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    PrivateToolRecords::Id,
                    PrivateToolRecords::Path,
                    PrivateToolRecords::Description,
                    PrivateToolRecords::DefaultAccessState,
                ])
                .from(PrivateToolRecords::Table)
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(PrivateToolRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .order_by(PrivateToolRecords::Path, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(PrivateToolSearchRow::decode).collect()
}

pub(crate) async fn declaration_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> Result<Option<PrivateToolDeclarationSearchMaterial>, Error> {
    let row: Option<PrivateToolDeclarationSearchRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    PrivateToolRecords::Path,
                    PrivateToolRecords::DeclarationFieldsFormat,
                    PrivateToolRecords::DeclarationFieldsStatus,
                    PrivateToolRecords::DeclarationFieldsJson,
                    PrivateToolRecords::DeclarationFieldsFailure,
                ])
                .from(PrivateToolRecords::Table)
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(PrivateToolRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(PrivateToolRecords::Path).eq(path.as_str()))
                .to_owned(),
        )
        .await?;
    row.map(PrivateToolDeclarationSearchRow::decode).transpose()
}

pub(crate) async fn list_workspace_entries_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<PrivateToolEntry>, Error> {
    let rows: Vec<PrivateToolEntryRow> = transaction
        .fetch_all(
            entries_statement()
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(PrivateToolEntryRow::decode).collect()
}

pub(crate) async fn list_every_entry_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<PrivateToolEntry>, Error> {
    let rows: Vec<PrivateToolEntryRow> = transaction.fetch_all(entries_statement()).await?;
    rows.into_iter().map(PrivateToolEntryRow::decode).collect()
}

pub(crate) async fn entry_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &PrivateToolId,
) -> Result<Option<PrivateToolEntry>, Error> {
    let row: Option<PrivateToolEntryRow> = transaction
        .fetch_optional(
            entries_statement()
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(PrivateToolEntryRow::decode).transpose()
}

pub(crate) async fn index_revisions(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<crate::PrivateToolIndexRevision>, Error> {
    let rows: Vec<(String, String, i64)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    PrivateToolRecords::Id,
                    PrivateToolRecords::WorkspaceId,
                    PrivateToolRecords::DefinitionRevision,
                ])
                .from(PrivateToolRecords::Table)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id, workspace, revision)| {
            Ok(crate::PrivateToolIndexRevision {
                id: PrivateToolId::parse(&id).map_err(|_error| {
                    Error::CorruptData("invalid Private Tool index identity".to_string())
                })?,
                workspace_id: WorkspaceId::parse(&workspace).map_err(|_error| {
                    Error::CorruptData("invalid Private Tool index Workspace".to_string())
                })?,
                revision: u64::try_from(revision).map_err(|_error| {
                    Error::CorruptData("invalid Private Tool index revision".to_string())
                })?,
            })
        })
        .collect()
}

fn entries_statement() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            PrivateToolRecords::Id,
            PrivateToolRecords::WorkspaceId,
            PrivateToolRecords::PrincipalId,
            PrivateToolRecords::PrincipalKind,
            PrivateToolRecords::Path,
            PrivateToolRecords::Description,
            PrivateToolRecords::Declaration,
            PrivateToolRecords::DeclarationFieldsFormat,
            PrivateToolRecords::DeclarationFieldsStatus,
            PrivateToolRecords::DeclarationFieldsJson,
            PrivateToolRecords::DeclarationFieldsFailure,
            PrivateToolRecords::DefinitionRevision,
        ])
        .from(PrivateToolRecords::Table)
        .order_by(PrivateToolRecords::WorkspaceId, Order::Asc)
        .order_by(PrivateToolRecords::PrincipalKind, Order::Asc)
        .order_by(PrivateToolRecords::PrincipalId, Order::Asc)
        .order_by(PrivateToolRecords::Path, Order::Asc)
        .to_owned()
}

pub(crate) async fn hydrate_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &PrivateToolSearchMaterial,
) -> Result<Option<PrivateToolAuthoringMaterial>, Error> {
    let row: Option<PrivateToolAuthoringRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    PrivateToolRecords::Path,
                    PrivateToolRecords::Description,
                    PrivateToolRecords::Declaration,
                ])
                .from(PrivateToolRecords::Table)
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(PrivateToolRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(PrivateToolRecords::Id).eq(target.id().to_string()))
                .and_where(Expr::col(PrivateToolRecords::Path).eq(target.path().as_str()))
                .to_owned(),
        )
        .await?;
    let material = row.map(PrivateToolAuthoringRow::decode).transpose()?;
    if material
        .as_ref()
        .is_some_and(|material| material.description() != target.description())
    {
        return Err(Error::CorruptData(
            "Private Tool changed inside one search snapshot".to_string(),
        ));
    }
    Ok(material)
}

pub(crate) async fn exact_description_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> Result<Option<ExactPrivateToolDescriptionMaterial>, Error> {
    let row: Option<ExactPrivateToolDescriptionRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    PrivateToolRecords::Id,
                    PrivateToolRecords::Path,
                    PrivateToolRecords::Description,
                    PrivateToolRecords::Declaration,
                    PrivateToolRecords::Source,
                    PrivateToolRecords::DiagnosticsJson,
                    PrivateToolRecords::DiagnosticsTruncated,
                    PrivateToolRecords::DefinitionRevision,
                    PrivateToolRecords::DefaultAccessState,
                ])
                .from(PrivateToolRecords::Table)
                .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(PrivateToolRecords::PrincipalKind)
                        .eq(principal.kind().as_storage_str()),
                )
                .and_where(Expr::col(PrivateToolRecords::Path).eq(path.as_str()))
                .to_owned(),
        )
        .await?;
    row.map(ExactPrivateToolDescriptionRow::decode).transpose()
}

pub(crate) async fn insert_current(
    transaction: &mut Transaction<'_>,
    tool: &PrivateTool,
) -> Result<(), Error> {
    let diagnostics_json = serde_json::to_string(tool.diagnostics())?;
    let revision = stored_revision(tool.revision)?;
    let statement = Query::insert()
        .into_table(PrivateToolRecords::Table)
        .columns([
            PrivateToolRecords::Id,
            PrivateToolRecords::WorkspaceId,
            PrivateToolRecords::PrincipalId,
            PrivateToolRecords::PrincipalKind,
            PrivateToolRecords::Path,
            PrivateToolRecords::Description,
            PrivateToolRecords::Source,
            PrivateToolRecords::Declaration,
            PrivateToolRecords::DeclarationFieldsFormat,
            PrivateToolRecords::DeclarationFieldsStatus,
            PrivateToolRecords::DeclarationFieldsJson,
            PrivateToolRecords::DeclarationFieldsFailure,
            PrivateToolRecords::DiagnosticsJson,
            PrivateToolRecords::DiagnosticsTruncated,
            PrivateToolRecords::SavedAtUnixNanos,
            PrivateToolRecords::DefinitionRevision,
            PrivateToolRecords::DefaultAccessState,
        ])
        .values_panic([
            Expr::val(tool.id().to_string()),
            Expr::val(tool.workspace_id().to_string()),
            Expr::val(tool.principal().id().as_str()),
            Expr::val(tool.principal().kind().as_storage_str()),
            Expr::val(tool.path().as_str()),
            Expr::val(tool.description().as_str()),
            Expr::val(tool.source().as_str()),
            Expr::val(tool.declaration().as_str()),
            Expr::val(AuthoredToolDeclarationFieldProjection::format()),
            Expr::val(tool.declaration_field_projection().status()),
            Expr::val(
                tool.declaration_field_projection()
                    .fields()
                    .map(serde_json::to_string)
                    .transpose()?,
            ),
            Expr::val(tool.declaration_field_projection().failure_reason()),
            Expr::val(diagnostics_json),
            Expr::val(tool.diagnostics_truncated()),
            Expr::val(tool.saved_at_unix_nanos()),
            Expr::val(revision),
            Expr::val(tool.default_access_state().as_storage_str()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(crate) async fn replace_current(
    transaction: &mut Transaction<'_>,
    previous: &PrivateTool,
    replacement: &PrivateTool,
) -> Result<(), Error> {
    let diagnostics_json = serde_json::to_string(replacement.diagnostics())?;
    let previous_revision = stored_revision(previous.revision)?;
    let replacement_revision = stored_revision(replacement.revision)?;
    let statement = Query::update()
        .table(PrivateToolRecords::Table)
        .values([
            (
                PrivateToolRecords::Description,
                Expr::val(replacement.description().as_str()),
            ),
            (
                PrivateToolRecords::Source,
                Expr::val(replacement.source().as_str()),
            ),
            (
                PrivateToolRecords::Declaration,
                Expr::val(replacement.declaration().as_str()),
            ),
            (
                PrivateToolRecords::DeclarationFieldsFormat,
                Expr::val(AuthoredToolDeclarationFieldProjection::format()),
            ),
            (
                PrivateToolRecords::DeclarationFieldsStatus,
                Expr::val(replacement.declaration_field_projection().status()),
            ),
            (
                PrivateToolRecords::DeclarationFieldsJson,
                Expr::val(
                    replacement
                        .declaration_field_projection()
                        .fields()
                        .map(serde_json::to_string)
                        .transpose()?,
                ),
            ),
            (
                PrivateToolRecords::DeclarationFieldsFailure,
                Expr::val(replacement.declaration_field_projection().failure_reason()),
            ),
            (
                PrivateToolRecords::DiagnosticsJson,
                Expr::val(diagnostics_json),
            ),
            (
                PrivateToolRecords::DiagnosticsTruncated,
                Expr::val(replacement.diagnostics_truncated()),
            ),
            (
                PrivateToolRecords::SavedAtUnixNanos,
                Expr::val(replacement.saved_at_unix_nanos()),
            ),
            (
                PrivateToolRecords::DefaultAccessState,
                Expr::val(replacement.default_access_state().as_storage_str()),
            ),
            (
                PrivateToolRecords::DefinitionRevision,
                Expr::val(replacement_revision),
            ),
        ])
        .and_where(
            Expr::col(PrivateToolRecords::WorkspaceId).eq(previous.workspace_id().to_string()),
        )
        .and_where(Expr::col(PrivateToolRecords::Id).eq(previous.id().to_string()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalId).eq(previous.principal().id().as_str()),
        )
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind)
                .eq(previous.principal().kind().as_storage_str()),
        )
        .and_where(Expr::col(PrivateToolRecords::Path).eq(previous.path().as_str()))
        .and_where(Expr::col(PrivateToolRecords::DefinitionRevision).eq(previous_revision))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "Private Tool replacement lost its held current definition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn delete_current(
    transaction: &mut Transaction<'_>,
    tool: &PrivateTool,
) -> Result<(), Error> {
    let statement = Query::delete()
        .from_table(PrivateToolRecords::Table)
        .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(tool.workspace_id().to_string()))
        .and_where(Expr::col(PrivateToolRecords::Id).eq(tool.id().to_string()))
        .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(tool.principal().id().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind)
                .eq(tool.principal().kind().as_storage_str()),
        )
        .and_where(Expr::col(PrivateToolRecords::Path).eq(tool.path().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::DefinitionRevision).eq(stored_revision(tool.revision)?),
        )
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::CorruptData(
            "Private Tool deletion lost its held current definition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) struct NewLifecycleEvent<'a> {
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) actor: &'a Principal,
    pub(crate) private_user_id: Option<&'a PrincipalId>,
    pub(crate) action: LifecycleAction,
    pub(crate) target_id: &'a PrivateToolId,
    pub(crate) target_path: &'a PrivateToolPath,
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
        .into_table(PrivateToolLifecycleEvents::Table)
        .columns([
            PrivateToolLifecycleEvents::Id,
            PrivateToolLifecycleEvents::WorkspaceId,
            PrivateToolLifecycleEvents::ActorPrincipalId,
            PrivateToolLifecycleEvents::ActorPrincipalKind,
            PrivateToolLifecycleEvents::PrivateUserId,
            PrivateToolLifecycleEvents::Action,
            PrivateToolLifecycleEvents::TargetPrivateToolId,
            PrivateToolLifecycleEvents::TargetPath,
            PrivateToolLifecycleEvents::Outcome,
            PrivateToolLifecycleEvents::ChangedFieldPaths,
            PrivateToolLifecycleEvents::OccurredAtUnixNanos,
            PrivateToolLifecycleEvents::TraceId,
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
            PrivateToolLifecycleEvents::WorkspaceId,
            PrivateToolLifecycleEvents::ActorPrincipalId,
            PrivateToolLifecycleEvents::ActorPrincipalKind,
            PrivateToolLifecycleEvents::Action,
            PrivateToolLifecycleEvents::TargetPrivateToolId,
            PrivateToolLifecycleEvents::TargetPath,
            PrivateToolLifecycleEvents::Outcome,
            PrivateToolLifecycleEvents::ChangedFieldPaths,
            PrivateToolLifecycleEvents::OccurredAtUnixNanos,
            PrivateToolLifecycleEvents::TraceId,
        ])
        .from(PrivateToolLifecycleEvents::Table)
        .and_where(Expr::col(PrivateToolLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(
            Expr::col(PrivateToolLifecycleEvents::PrivateUserId)
                .eq(principal.id().as_str())
                .or(Expr::col(PrivateToolLifecycleEvents::PrivateUserId)
                    .is_null()
                    .and(
                        Expr::col(PrivateToolLifecycleEvents::ActorPrincipalId)
                            .eq(principal.id().as_str()),
                    )
                    .and(
                        Expr::col(PrivateToolLifecycleEvents::ActorPrincipalKind)
                            .eq(principal.kind().as_storage_str()),
                    )),
        )
        .order_by(PrivateToolLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
        .order_by(PrivateToolLifecycleEvents::Id, Order::Asc)
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
    path: &PrivateToolPath,
) -> Result<Option<PrivateToolSelection>, Error> {
    find_current_in_transaction(transaction, workspace_id, principal, path)
        .await
        .map(|tool| tool.as_ref().map(PrivateToolSelection::from_current))
}

/// Holds a selected definition unchanged through the caller's acceptance commit.
///
/// The caller must hold current Workspace authority in this write transaction and
/// pass the same Workspace and Principal used for selection. `false` means the
/// definition was replaced or deleted; admission must reject without a Run.
/// The revision and locking mechanics remain private to Tools.
///
/// # Errors
///
/// Returns persistence or corrupt-selection failures.
pub async fn hold_source_selection_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    snapshot: &PrivateToolSelection,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(PrivateToolRecords::Table)
        .value(
            PrivateToolRecords::DefinitionRevision,
            Expr::col(PrivateToolRecords::DefinitionRevision),
        )
        .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(PrivateToolRecords::Id).eq(snapshot.id.to_string()))
        .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(PrivateToolRecords::Path).eq(snapshot.path.as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::DefinitionRevision)
                .eq(stored_revision(snapshot.revision)?),
        )
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(crate) fn decode_diagnostics(
    json: &str,
    truncated: bool,
) -> Result<Vec<AuthoredToolDiagnostic>, Error> {
    let diagnostics: Vec<AuthoredToolDiagnostic> = serde_json::from_str(json).map_err(|error| {
        Error::CorruptData(format!("authored Tool has invalid diagnostics: {error}"))
    })?;
    if diagnostics.len() > AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT
        || diagnostics.iter().any(|diagnostic| {
            diagnostic.message().len() > AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES
        })
        || (truncated && diagnostics.len() != AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT)
    {
        return Err(Error::CorruptData(
            "authored Tool diagnostics exceed their stored bounds".to_string(),
        ));
    }
    Ok(diagnostics)
}

fn current_statement(
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> sea_query::SelectStatement {
    Query::select()
        .columns(private_tool_columns())
        .from(PrivateToolRecords::Table)
        .and_where(Expr::col(PrivateToolRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(PrivateToolRecords::PrincipalId).eq(principal.id().as_str()))
        .and_where(
            Expr::col(PrivateToolRecords::PrincipalKind).eq(principal.kind().as_storage_str()),
        )
        .and_where(Expr::col(PrivateToolRecords::Path).eq(path.as_str()))
        .to_owned()
}

fn private_tool_columns() -> [PrivateToolRecords; 17] {
    [
        PrivateToolRecords::Id,
        PrivateToolRecords::WorkspaceId,
        PrivateToolRecords::PrincipalId,
        PrivateToolRecords::PrincipalKind,
        PrivateToolRecords::Path,
        PrivateToolRecords::Description,
        PrivateToolRecords::Source,
        PrivateToolRecords::Declaration,
        PrivateToolRecords::DeclarationFieldsFormat,
        PrivateToolRecords::DeclarationFieldsStatus,
        PrivateToolRecords::DeclarationFieldsJson,
        PrivateToolRecords::DeclarationFieldsFailure,
        PrivateToolRecords::DiagnosticsJson,
        PrivateToolRecords::DiagnosticsTruncated,
        PrivateToolRecords::SavedAtUnixNanos,
        PrivateToolRecords::DefinitionRevision,
        PrivateToolRecords::DefaultAccessState,
    ]
}

fn principal_kind_from_storage(value: &str) -> Result<PrincipalKind, Error> {
    PrincipalKind::from_storage_str(value).map_err(|_error| {
        Error::CorruptData(format!(
            "Private Tool has unrecognized Principal kind '{value}'"
        ))
    })
}

pub(crate) fn stored_revision(revision: u64) -> Result<i64, Error> {
    i64::try_from(revision)
        .map_err(|_error| Error::CorruptData("authored Tool revision overflowed".to_string()))
}

pub(crate) fn access_state_from_storage(value: &str) -> Result<ToolAccessState, Error> {
    ToolAccessState::from_storage_str(value).ok_or_else(|| {
        Error::CorruptData("authored Tool has an invalid default Tool Access State".to_string())
    })
}

fn decode_revision(stored: i64) -> Result<u64, Error> {
    u64::try_from(stored)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::CorruptData("Private Tool has invalid definition revision".to_string())
        })
}
