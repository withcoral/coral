use std::collections::BTreeMap;

use coral_catalog::ToolKey;
use coral_db::Transaction;
use coral_provider::ProviderId;
use coral_telemetry::TraceId;
use coral_workspace::{AuthoritySource, UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use crate::{
    AdministrativeLifecycleEvent, Error, ExactToolRules, LifecycleAction, LifecycleOutcome,
    ToolAccessState, ToolPolicyTarget,
};

#[derive(Iden)]
enum ToolAccessRules {
    Table,
    WorkspaceId,
    ProviderId,
    ToolKey,
    AccessState,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum ToolPolicyLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetProviderId,
    TargetToolKey,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

#[derive(Debug, FromRow)]
struct LifecycleEventRow {
    workspace_id: String,
    actor_user_id: String,
    authority_source: String,
    action: String,
    target_provider_id: String,
    target_tool_key: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData(
                "Tool-policy lifecycle Event has invalid Workspace identity".to_string(),
            )
        })?;
        let provider_id = ProviderId::parse(&self.target_provider_id).map_err(|_error| {
            Error::CorruptData(
                "Tool-policy lifecycle Event has invalid Provider identity".to_string(),
            )
        })?;
        let tool_key = ToolKey::parse(&self.target_tool_key).map_err(|_error| {
            Error::CorruptData("Tool-policy lifecycle Event has invalid Tool Key".to_string())
        })?;
        let actor_user_id = UserId::parse(&self.actor_user_id).map_err(|_error| {
            Error::CorruptData("Tool-policy lifecycle Event has invalid actor identity".to_string())
        })?;
        let authority_source = AuthoritySource::from_storage_str(&self.authority_source)?;
        let action = LifecycleAction::from_storage_str(&self.action)?;
        let outcome = LifecycleOutcome::from_storage_str(&self.outcome)?;
        let changed_field_paths = serde_json::from_str(&self.changed_field_paths)?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|_error| {
            Error::CorruptData("Tool-policy lifecycle Event has invalid trace identity".to_string())
        })?;
        Ok(AdministrativeLifecycleEvent::new(
            ToolPolicyTarget::new(workspace_id, provider_id, tool_key),
            actor_user_id,
            authority_source,
            action,
            outcome,
            changed_field_paths,
            self.occurred_at_unix_nanos,
            trace_id,
        ))
    }
}

/// Reads the exact explicit rule holding its row as `lock` says. An absent
/// rule holds nothing; the first explicit rule's own race is out of scope.
pub(super) async fn access_state_in_transaction(
    transaction: &mut Transaction<'_>,
    target: &ToolPolicyTarget,
    lock: coral_db::RowLock,
) -> Result<Option<ToolAccessState>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(ToolAccessRules::AccessState)
                .from(ToolAccessRules::Table)
                .and_where(
                    Expr::col(ToolAccessRules::WorkspaceId).eq(target.workspace_id().to_string()),
                )
                .and_where(
                    Expr::col(ToolAccessRules::ProviderId).eq(target.provider_id().to_string()),
                )
                .and_where(Expr::col(ToolAccessRules::ToolKey).eq(target.tool_key().as_str()))
                .to_owned(),
            lock,
        )
        .await?;
    row.map(|(value,)| ToolAccessState::from_storage_str(&value))
        .transpose()
}

pub(super) async fn access_states_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    keys: &[ToolKey],
) -> Result<BTreeMap<ToolKey, ToolAccessState>, Error> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([ToolAccessRules::ToolKey, ToolAccessRules::AccessState])
                .from(ToolAccessRules::Table)
                .and_where(Expr::col(ToolAccessRules::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolAccessRules::ProviderId).eq(provider.to_string()))
                .and_where(
                    Expr::col(ToolAccessRules::ToolKey).is_in(keys.iter().map(ToolKey::as_str)),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(key, state)| {
            let key = ToolKey::parse(&key)
                .map_err(|_error| Error::CorruptData("Tool rule has invalid Tool Key".into()))?;
            Ok((key, ToolAccessState::from_storage_str(&state)?))
        })
        .collect()
}

pub(super) async fn provider_rules_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<BTreeMap<ToolKey, ToolAccessState>, Error> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([ToolAccessRules::ToolKey, ToolAccessRules::AccessState])
                .from(ToolAccessRules::Table)
                .and_where(Expr::col(ToolAccessRules::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolAccessRules::ProviderId).eq(provider.to_string()))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(key, state)| {
            let key = ToolKey::parse(&key)
                .map_err(|_error| Error::CorruptData("Tool rule has invalid Tool Key".into()))?;
            Ok((key, ToolAccessState::from_storage_str(&state)?))
        })
        .collect()
}

pub(super) async fn exact_rules_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<ExactToolRules, Error> {
    let rows: Vec<(String, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    ToolAccessRules::ProviderId,
                    ToolAccessRules::ToolKey,
                    ToolAccessRules::AccessState,
                ])
                .from(ToolAccessRules::Table)
                .and_where(Expr::col(ToolAccessRules::WorkspaceId).eq(workspace_id.to_string()))
                .to_owned(),
        )
        .await?;
    let mut rules = BTreeMap::<ProviderId, BTreeMap<ToolKey, ToolAccessState>>::new();
    for (provider_id, tool_key, access_state) in rows {
        let provider_id = ProviderId::parse(&provider_id).map_err(|_error| {
            Error::CorruptData("Tool rule has invalid Provider identity".to_string())
        })?;
        let tool_key = ToolKey::parse(&tool_key)
            .map_err(|_error| Error::CorruptData("Tool rule has invalid Tool Key".to_string()))?;
        rules
            .entry(provider_id)
            .or_default()
            .insert(tool_key, ToolAccessState::from_storage_str(&access_state)?);
    }
    Ok(ExactToolRules::new(rules))
}

pub(super) async fn upsert_access_state(
    transaction: &mut Transaction<'_>,
    target: &ToolPolicyTarget,
    access_state: ToolAccessState,
    occurred_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ToolAccessRules::Table)
        .columns([
            ToolAccessRules::WorkspaceId,
            ToolAccessRules::ProviderId,
            ToolAccessRules::ToolKey,
            ToolAccessRules::AccessState,
            ToolAccessRules::CreatedAtUnixNanos,
            ToolAccessRules::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(target.workspace_id().to_string()),
            Expr::val(target.provider_id().to_string()),
            Expr::val(target.tool_key().as_str()),
            Expr::val(access_state.as_storage_str()),
            Expr::val(occurred_at_unix_nanos),
            Expr::val(occurred_at_unix_nanos),
        ])
        .on_conflict(
            OnConflict::columns([
                ToolAccessRules::WorkspaceId,
                ToolAccessRules::ProviderId,
                ToolAccessRules::ToolKey,
            ])
            .update_columns([
                ToolAccessRules::AccessState,
                ToolAccessRules::UpdatedAtUnixNanos,
            ])
            .to_owned(),
        )
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) struct NewLifecycleEvent<'a> {
    pub(super) target: &'a ToolPolicyTarget,
    pub(super) actor_user_id: &'a UserId,
    pub(super) authority_source: AuthoritySource,
    pub(super) changed_field_paths: &'a [&'a str],
    pub(super) occurred_at_unix_nanos: i64,
    pub(super) trace_id: &'a TraceId,
}

pub(super) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    let changed_field_paths = serde_json::to_string(event.changed_field_paths)?;
    let statement = Query::insert()
        .into_table(ToolPolicyLifecycleEvents::Table)
        .columns([
            ToolPolicyLifecycleEvents::Id,
            ToolPolicyLifecycleEvents::WorkspaceId,
            ToolPolicyLifecycleEvents::ActorUserId,
            ToolPolicyLifecycleEvents::AuthoritySource,
            ToolPolicyLifecycleEvents::Action,
            ToolPolicyLifecycleEvents::TargetProviderId,
            ToolPolicyLifecycleEvents::TargetToolKey,
            ToolPolicyLifecycleEvents::Outcome,
            ToolPolicyLifecycleEvents::ChangedFieldPaths,
            ToolPolicyLifecycleEvents::OccurredAtUnixNanos,
            ToolPolicyLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(event.target.workspace_id().to_string()),
            Expr::val(event.actor_user_id.as_str()),
            Expr::val(event.authority_source.as_storage_str()),
            Expr::val(LifecycleAction::AccessStateUpdated.as_storage_str()),
            Expr::val(event.target.provider_id().to_string()),
            Expr::val(event.target.tool_key().as_str()),
            Expr::val(LifecycleOutcome::Accepted.as_storage_str()),
            Expr::val(changed_field_paths),
            Expr::val(event.occurred_at_unix_nanos),
            Expr::val(event.trace_id.as_str()),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

pub(super) async fn list_lifecycle_events_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let rows: Vec<LifecycleEventRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    ToolPolicyLifecycleEvents::WorkspaceId,
                    ToolPolicyLifecycleEvents::ActorUserId,
                    ToolPolicyLifecycleEvents::AuthoritySource,
                    ToolPolicyLifecycleEvents::Action,
                    ToolPolicyLifecycleEvents::TargetProviderId,
                    ToolPolicyLifecycleEvents::TargetToolKey,
                    ToolPolicyLifecycleEvents::Outcome,
                    ToolPolicyLifecycleEvents::ChangedFieldPaths,
                    ToolPolicyLifecycleEvents::OccurredAtUnixNanos,
                    ToolPolicyLifecycleEvents::TraceId,
                ])
                .from(ToolPolicyLifecycleEvents::Table)
                .and_where(
                    Expr::col(ToolPolicyLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()),
                )
                .order_by(ToolPolicyLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
                .order_by(ToolPolicyLifecycleEvents::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}
