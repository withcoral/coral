use coral_db::Transaction;
use coral_workspace::{AuthoritySource, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use crate::{
    AdministrativeLifecycleEvent, Error, LifecycleAction, Middleware, MiddlewareDescription,
    MiddlewareDiagnostic, MiddlewareId, MiddlewareName, MiddlewareScopes, MiddlewareSource,
};

#[derive(Iden)]
enum MiddlewareRecords {
    Table,
    Id,
    WorkspaceId,
    Name,
    Description,
    Source,
    DiagnosticsJson,
    RunScope,
    FunctionScope,
    OperationScope,
    Enabled,
    Position,
    Revision,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum MiddlewareLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetMiddlewareId,
    TargetName,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

#[derive(FromRow)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the row mirrors the persisted scope and activation columns"
)]
struct MiddlewareRow {
    id: String,
    workspace_id: String,
    name: String,
    description: String,
    source: String,
    diagnostics_json: String,
    run_scope: bool,
    function_scope: bool,
    operation_scope: bool,
    enabled: bool,
    position: i64,
    revision: i64,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl MiddlewareRow {
    fn decode(self) -> Result<Middleware, Error> {
        let corrupt = |field: &str| Error::CorruptData(format!("Middleware has invalid {field}"));
        Ok(Middleware {
            id: MiddlewareId::parse(&self.id).map_err(|_error| corrupt("identity"))?,
            workspace_id: WorkspaceId::parse(&self.workspace_id)
                .map_err(|_error| corrupt("Workspace"))?,
            name: MiddlewareName::parse(&self.name).map_err(|_error| corrupt("name"))?,
            description: MiddlewareDescription::parse(&self.description)
                .map_err(|_error| corrupt("description"))?,
            source: MiddlewareSource::parse(&self.source).map_err(|_error| corrupt("source"))?,
            diagnostics: decode_diagnostics(&self.diagnostics_json)?,
            scopes: MiddlewareScopes {
                run: self.run_scope,
                function: self.function_scope,
                operation: self.operation_scope,
            },
            enabled: self.enabled,
            position: u32::try_from(self.position).map_err(|_error| corrupt("position"))?,
            revision: self.revision,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredDiagnostic {
    category: coral_v8::DiagnosticCategory,
    code: u64,
    message: String,
    line: Option<u64>,
    column: Option<u64>,
}

fn encode_diagnostics(diagnostics: &[MiddlewareDiagnostic]) -> Result<String, Error> {
    let stored: Vec<_> = diagnostics
        .iter()
        .map(|diagnostic| StoredDiagnostic {
            category: diagnostic.category,
            code: diagnostic.code,
            message: diagnostic.message.clone(),
            line: diagnostic.line,
            column: diagnostic.column,
        })
        .collect();
    Ok(serde_json::to_string(&stored)?)
}

fn decode_diagnostics(value: &str) -> Result<Vec<MiddlewareDiagnostic>, Error> {
    let stored: Vec<StoredDiagnostic> = serde_json::from_str(value)
        .map_err(|_error| Error::CorruptData("Middleware has invalid diagnostics".to_string()))?;
    Ok(stored
        .into_iter()
        .map(|diagnostic| MiddlewareDiagnostic {
            category: diagnostic.category,
            code: diagnostic.code,
            message: diagnostic.message,
            line: diagnostic.line,
            column: diagnostic.column,
        })
        .collect())
}

fn columns() -> [MiddlewareRecords; 14] {
    [
        MiddlewareRecords::Id,
        MiddlewareRecords::WorkspaceId,
        MiddlewareRecords::Name,
        MiddlewareRecords::Description,
        MiddlewareRecords::Source,
        MiddlewareRecords::DiagnosticsJson,
        MiddlewareRecords::RunScope,
        MiddlewareRecords::FunctionScope,
        MiddlewareRecords::OperationScope,
        MiddlewareRecords::Enabled,
        MiddlewareRecords::Position,
        MiddlewareRecords::Revision,
        MiddlewareRecords::CreatedAtUnixNanos,
        MiddlewareRecords::UpdatedAtUnixNanos,
    ]
}

pub(crate) async fn list_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<Middleware>, Error> {
    let rows: Vec<MiddlewareRow> = transaction
        .fetch_all(
            Query::select()
                .columns(columns())
                .from(MiddlewareRecords::Table)
                .and_where(Expr::col(MiddlewareRecords::WorkspaceId).eq(workspace_id.to_string()))
                .order_by(MiddlewareRecords::Position, Order::Asc)
                .order_by(MiddlewareRecords::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(MiddlewareRow::decode).collect()
}

pub(crate) async fn find_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &MiddlewareId,
) -> Result<Option<Middleware>, Error> {
    let row: Option<MiddlewareRow> = transaction
        .fetch_optional(
            Query::select()
                .columns(columns())
                .from(MiddlewareRecords::Table)
                .and_where(Expr::col(MiddlewareRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(MiddlewareRecords::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(MiddlewareRow::decode).transpose()
}

pub(crate) async fn insert(
    transaction: &mut Transaction<'_>,
    middleware: &Middleware,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::insert()
                .into_table(MiddlewareRecords::Table)
                .columns(columns())
                .values_panic([
                    Expr::val(middleware.id.to_string()),
                    Expr::val(middleware.workspace_id.to_string()),
                    Expr::val(middleware.name.as_str()),
                    Expr::val(middleware.description.as_str()),
                    Expr::val(middleware.source.as_str()),
                    Expr::val(encode_diagnostics(&middleware.diagnostics)?),
                    Expr::val(middleware.scopes.run),
                    Expr::val(middleware.scopes.function),
                    Expr::val(middleware.scopes.operation),
                    Expr::val(middleware.enabled),
                    Expr::val(i64::from(middleware.position)),
                    Expr::val(middleware.revision),
                    Expr::val(middleware.created_at_unix_nanos),
                    Expr::val(middleware.updated_at_unix_nanos),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(crate) async fn replace(
    transaction: &mut Transaction<'_>,
    middleware: &Middleware,
) -> Result<(), Error> {
    let changed = transaction
        .execute_change(
            Query::update()
                .table(MiddlewareRecords::Table)
                .values([
                    (
                        MiddlewareRecords::Description,
                        Expr::val(middleware.description.as_str()),
                    ),
                    (
                        MiddlewareRecords::Source,
                        Expr::val(middleware.source.as_str()),
                    ),
                    (
                        MiddlewareRecords::DiagnosticsJson,
                        Expr::val(encode_diagnostics(&middleware.diagnostics)?),
                    ),
                    (
                        MiddlewareRecords::RunScope,
                        Expr::val(middleware.scopes.run),
                    ),
                    (
                        MiddlewareRecords::FunctionScope,
                        Expr::val(middleware.scopes.function),
                    ),
                    (
                        MiddlewareRecords::OperationScope,
                        Expr::val(middleware.scopes.operation),
                    ),
                    (MiddlewareRecords::Enabled, Expr::val(middleware.enabled)),
                    (
                        MiddlewareRecords::Position,
                        Expr::val(i64::from(middleware.position)),
                    ),
                    (MiddlewareRecords::Revision, Expr::val(middleware.revision)),
                    (
                        MiddlewareRecords::UpdatedAtUnixNanos,
                        Expr::val(middleware.updated_at_unix_nanos),
                    ),
                ])
                .and_where(
                    Expr::col(MiddlewareRecords::WorkspaceId)
                        .eq(middleware.workspace_id.to_string()),
                )
                .and_where(Expr::col(MiddlewareRecords::Id).eq(middleware.id.to_string()))
                .to_owned(),
        )
        .await?;
    if changed == 1 {
        Ok(())
    } else {
        Err(Error::MiddlewareNotFound)
    }
}

pub(crate) async fn delete(
    transaction: &mut Transaction<'_>,
    middleware: &Middleware,
) -> Result<(), Error> {
    let changed = transaction
        .execute_change(
            Query::delete()
                .from_table(MiddlewareRecords::Table)
                .and_where(
                    Expr::col(MiddlewareRecords::WorkspaceId)
                        .eq(middleware.workspace_id.to_string()),
                )
                .and_where(Expr::col(MiddlewareRecords::Id).eq(middleware.id.to_string()))
                .to_owned(),
        )
        .await?;
    if changed == 1 {
        Ok(())
    } else {
        Err(Error::MiddlewareNotFound)
    }
}

pub(crate) struct NewLifecycleEvent<'a> {
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) actor_user_id: &'a str,
    pub(crate) authority_source: AuthoritySource,
    pub(crate) action: LifecycleAction,
    pub(crate) target: &'a Middleware,
    pub(crate) changed_field_paths: &'a [&'a str],
    pub(crate) occurred_at_unix_nanos: i64,
    pub(crate) trace_id: &'a str,
}

pub(crate) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::insert()
                .into_table(MiddlewareLifecycleEvents::Table)
                .columns([
                    MiddlewareLifecycleEvents::Id,
                    MiddlewareLifecycleEvents::WorkspaceId,
                    MiddlewareLifecycleEvents::ActorUserId,
                    MiddlewareLifecycleEvents::AuthoritySource,
                    MiddlewareLifecycleEvents::Action,
                    MiddlewareLifecycleEvents::TargetMiddlewareId,
                    MiddlewareLifecycleEvents::TargetName,
                    MiddlewareLifecycleEvents::Outcome,
                    MiddlewareLifecycleEvents::ChangedFieldPaths,
                    MiddlewareLifecycleEvents::OccurredAtUnixNanos,
                    MiddlewareLifecycleEvents::TraceId,
                ])
                .values_panic([
                    Expr::val(uuid::Uuid::new_v4().to_string()),
                    Expr::val(event.workspace_id.to_string()),
                    Expr::val(event.actor_user_id),
                    Expr::val(event.authority_source.as_storage_str()),
                    Expr::val(event.action.as_storage_str()),
                    Expr::val(event.target.id.to_string()),
                    Expr::val(event.target.name.as_str()),
                    Expr::val("accepted"),
                    Expr::val(serde_json::to_string(event.changed_field_paths)?),
                    Expr::val(event.occurred_at_unix_nanos),
                    Expr::val(event.trace_id),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(crate) async fn list_lifecycle_events_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let rows: Vec<(String, String, String, String, String, i64)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    MiddlewareLifecycleEvents::Action,
                    MiddlewareLifecycleEvents::TargetMiddlewareId,
                    MiddlewareLifecycleEvents::TargetName,
                    MiddlewareLifecycleEvents::ActorUserId,
                    MiddlewareLifecycleEvents::ChangedFieldPaths,
                    MiddlewareLifecycleEvents::OccurredAtUnixNanos,
                ])
                .from(MiddlewareLifecycleEvents::Table)
                .and_where(
                    Expr::col(MiddlewareLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()),
                )
                .order_by(MiddlewareLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
                .order_by(MiddlewareLifecycleEvents::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(
            |(action, target_id, target_name, actor_user_id, fields, occurred_at)| {
                Ok(AdministrativeLifecycleEvent {
                    action: LifecycleAction::from_storage_str(&action)?,
                    target_id: MiddlewareId::parse(&target_id).map_err(|_error| {
                        Error::CorruptData("Middleware Event has invalid target".to_string())
                    })?,
                    target_name: MiddlewareName::parse(&target_name).map_err(|_error| {
                        Error::CorruptData("Middleware Event has invalid name".to_string())
                    })?,
                    actor_user_id,
                    changed_field_paths: serde_json::from_str(&fields)?,
                    occurred_at_unix_nanos: occurred_at,
                })
            },
        )
        .collect()
}
