use std::collections::BTreeMap;

use coral_catalog::OperationKey;
use coral_db::Transaction;
use coral_provider::ProviderId;
use coral_telemetry::TraceId;
use coral_workspace::{AuthoritySource, UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use crate::{
    AdministrativeLifecycleEvent, Error, ExactOperationRules, LifecycleAction, LifecycleOutcome,
    OperationAccessState, OperationPolicyTarget,
};

#[derive(Iden)]
enum OperationAccessRules {
    Table,
    WorkspaceId,
    ProviderId,
    OperationKey,
    AccessState,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum OperationPolicyLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetProviderId,
    TargetOperationKey,
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
    target_operation_key: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData(
                "Operation-policy lifecycle Event has invalid Workspace identity".to_string(),
            )
        })?;
        let provider_id = ProviderId::parse(&self.target_provider_id).map_err(|_error| {
            Error::CorruptData(
                "Operation-policy lifecycle Event has invalid Provider identity".to_string(),
            )
        })?;
        let operation_key = OperationKey::parse(&self.target_operation_key).map_err(|_error| {
            Error::CorruptData(
                "Operation-policy lifecycle Event has invalid Operation Key".to_string(),
            )
        })?;
        let actor_user_id = UserId::parse(&self.actor_user_id).map_err(|_error| {
            Error::CorruptData(
                "Operation-policy lifecycle Event has invalid actor identity".to_string(),
            )
        })?;
        let authority_source = AuthoritySource::from_storage_str(&self.authority_source)?;
        let action = LifecycleAction::from_storage_str(&self.action)?;
        let outcome = LifecycleOutcome::from_storage_str(&self.outcome)?;
        let changed_field_paths = serde_json::from_str(&self.changed_field_paths)?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|_error| {
            Error::CorruptData(
                "Operation-policy lifecycle Event has invalid trace identity".to_string(),
            )
        })?;
        Ok(AdministrativeLifecycleEvent::new(
            OperationPolicyTarget::new(workspace_id, provider_id, operation_key),
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
    target: &OperationPolicyTarget,
    lock: coral_db::RowLock,
) -> Result<Option<OperationAccessState>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(OperationAccessRules::AccessState)
                .from(OperationAccessRules::Table)
                .and_where(
                    Expr::col(OperationAccessRules::WorkspaceId)
                        .eq(target.workspace_id().to_string()),
                )
                .and_where(
                    Expr::col(OperationAccessRules::ProviderId)
                        .eq(target.provider_id().to_string()),
                )
                .and_where(
                    Expr::col(OperationAccessRules::OperationKey)
                        .eq(target.operation_key().as_str()),
                )
                .to_owned(),
            lock,
        )
        .await?;
    row.map(|(value,)| OperationAccessState::from_storage_str(&value))
        .transpose()
}

pub(super) async fn access_states_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    keys: &[OperationKey],
) -> Result<BTreeMap<OperationKey, OperationAccessState>, Error> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    OperationAccessRules::OperationKey,
                    OperationAccessRules::AccessState,
                ])
                .from(OperationAccessRules::Table)
                .and_where(Expr::col(OperationAccessRules::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationAccessRules::ProviderId).eq(provider.to_string()))
                .and_where(
                    Expr::col(OperationAccessRules::OperationKey)
                        .is_in(keys.iter().map(OperationKey::as_str)),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(key, state)| {
            let key = OperationKey::parse(&key).map_err(|_error| {
                Error::CorruptData("Operation rule has invalid Operation Key".into())
            })?;
            Ok((key, OperationAccessState::from_storage_str(&state)?))
        })
        .collect()
}

pub(super) async fn provider_rules_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<BTreeMap<OperationKey, OperationAccessState>, Error> {
    let rows: Vec<(String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    OperationAccessRules::OperationKey,
                    OperationAccessRules::AccessState,
                ])
                .from(OperationAccessRules::Table)
                .and_where(Expr::col(OperationAccessRules::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationAccessRules::ProviderId).eq(provider.to_string()))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(key, state)| {
            let key = OperationKey::parse(&key).map_err(|_error| {
                Error::CorruptData("Operation rule has invalid Operation Key".into())
            })?;
            Ok((key, OperationAccessState::from_storage_str(&state)?))
        })
        .collect()
}

pub(super) async fn exact_rules_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<ExactOperationRules, Error> {
    let rows: Vec<(String, String, String)> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    OperationAccessRules::ProviderId,
                    OperationAccessRules::OperationKey,
                    OperationAccessRules::AccessState,
                ])
                .from(OperationAccessRules::Table)
                .and_where(
                    Expr::col(OperationAccessRules::WorkspaceId).eq(workspace_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    let mut rules = BTreeMap::<ProviderId, BTreeMap<OperationKey, OperationAccessState>>::new();
    for (provider_id, operation_key, access_state) in rows {
        let provider_id = ProviderId::parse(&provider_id).map_err(|_error| {
            Error::CorruptData("Operation rule has invalid Provider identity".to_string())
        })?;
        let operation_key = OperationKey::parse(&operation_key).map_err(|_error| {
            Error::CorruptData("Operation rule has invalid Operation Key".to_string())
        })?;
        rules.entry(provider_id).or_default().insert(
            operation_key,
            OperationAccessState::from_storage_str(&access_state)?,
        );
    }
    Ok(ExactOperationRules::new(rules))
}

pub(super) async fn upsert_access_state(
    transaction: &mut Transaction<'_>,
    target: &OperationPolicyTarget,
    access_state: OperationAccessState,
    occurred_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(OperationAccessRules::Table)
        .columns([
            OperationAccessRules::WorkspaceId,
            OperationAccessRules::ProviderId,
            OperationAccessRules::OperationKey,
            OperationAccessRules::AccessState,
            OperationAccessRules::CreatedAtUnixNanos,
            OperationAccessRules::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(target.workspace_id().to_string()),
            Expr::val(target.provider_id().to_string()),
            Expr::val(target.operation_key().as_str()),
            Expr::val(access_state.as_storage_str()),
            Expr::val(occurred_at_unix_nanos),
            Expr::val(occurred_at_unix_nanos),
        ])
        .on_conflict(
            OnConflict::columns([
                OperationAccessRules::WorkspaceId,
                OperationAccessRules::ProviderId,
                OperationAccessRules::OperationKey,
            ])
            .update_columns([
                OperationAccessRules::AccessState,
                OperationAccessRules::UpdatedAtUnixNanos,
            ])
            .to_owned(),
        )
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) struct NewLifecycleEvent<'a> {
    pub(super) target: &'a OperationPolicyTarget,
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
        .into_table(OperationPolicyLifecycleEvents::Table)
        .columns([
            OperationPolicyLifecycleEvents::Id,
            OperationPolicyLifecycleEvents::WorkspaceId,
            OperationPolicyLifecycleEvents::ActorUserId,
            OperationPolicyLifecycleEvents::AuthoritySource,
            OperationPolicyLifecycleEvents::Action,
            OperationPolicyLifecycleEvents::TargetProviderId,
            OperationPolicyLifecycleEvents::TargetOperationKey,
            OperationPolicyLifecycleEvents::Outcome,
            OperationPolicyLifecycleEvents::ChangedFieldPaths,
            OperationPolicyLifecycleEvents::OccurredAtUnixNanos,
            OperationPolicyLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(event.target.workspace_id().to_string()),
            Expr::val(event.actor_user_id.as_str()),
            Expr::val(event.authority_source.as_storage_str()),
            Expr::val(LifecycleAction::AccessStateUpdated.as_storage_str()),
            Expr::val(event.target.provider_id().to_string()),
            Expr::val(event.target.operation_key().as_str()),
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
                    OperationPolicyLifecycleEvents::WorkspaceId,
                    OperationPolicyLifecycleEvents::ActorUserId,
                    OperationPolicyLifecycleEvents::AuthoritySource,
                    OperationPolicyLifecycleEvents::Action,
                    OperationPolicyLifecycleEvents::TargetProviderId,
                    OperationPolicyLifecycleEvents::TargetOperationKey,
                    OperationPolicyLifecycleEvents::Outcome,
                    OperationPolicyLifecycleEvents::ChangedFieldPaths,
                    OperationPolicyLifecycleEvents::OccurredAtUnixNanos,
                    OperationPolicyLifecycleEvents::TraceId,
                ])
                .from(OperationPolicyLifecycleEvents::Table)
                .and_where(
                    Expr::col(OperationPolicyLifecycleEvents::WorkspaceId)
                        .eq(workspace_id.to_string()),
                )
                .order_by(
                    OperationPolicyLifecycleEvents::OccurredAtUnixNanos,
                    Order::Asc,
                )
                .order_by(OperationPolicyLifecycleEvents::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}
