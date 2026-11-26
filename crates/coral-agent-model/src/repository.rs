//! The model's row.

use coral_db::{RowLock, Transaction, UnixNanos, Version};
use coral_workspace::UserId;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query, SelectStatement, SimpleExpr};
use uuid::Uuid;

use crate::agent_models::AgentModel;
use crate::error::Error;
use crate::settings::{ModelProtocol, ModelSettings};

#[derive(Iden)]
enum SubAgentModels {
    Table,
    Id,
    Singleton,
    Version,
    Protocol,
    BaseUrl,
    Model,
    SealedApiKey,
    ContextWindow,
    RequestArgsJson,
    ChangedByUserId,
    ChangedAtUnixNanos,
}

/// The row as it is stored: the model, and its sealed key.
#[derive(Clone)]
pub(crate) struct StoredModel {
    pub(crate) model: AgentModel,
    pub(crate) sealed_api_key: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ModelRow {
    id: String,
    version: i64,
    protocol: String,
    base_url: String,
    model: String,
    sealed_api_key: Option<String>,
    context_window: Option<i64>,
    request_args_json: String,
    changed_by_user_id: String,
    changed_at_unix_nanos: UnixNanos,
}

impl TryFrom<ModelRow> for StoredModel {
    type Error = Error;

    fn try_from(row: ModelRow) -> Result<Self, Error> {
        let corrupt = |what| move || Error::CorruptData(what);
        let context_window = row
            .context_window
            .map(u32::try_from)
            .transpose()
            .map_err(|_out_of_range| Error::CorruptData("a model's context window"))?;
        Ok(Self {
            model: AgentModel {
                id: Uuid::parse_str(&row.id)
                    .map_err(|_not_a_uuid| Error::CorruptData("a model's id"))?,
                version: Version::new(row.version).ok_or_else(corrupt("a model's version"))?,
                settings: ModelSettings {
                    protocol: ModelProtocol::from_storage_str(&row.protocol)?,
                    base_url: row.base_url,
                    model: row.model,
                    context_window,
                    request_args: serde_json::from_str(&row.request_args_json)
                        .map_err(|_not_an_object| Error::CorruptData("a model's args"))?,
                },
                has_api_key: row.sealed_api_key.is_some(),
                changed_by: UserId::parse(&row.changed_by_user_id)
                    .map_err(|_invalid| Error::CorruptData("who changed a model"))?,
                changed_at: row.changed_at_unix_nanos,
            },
            sealed_api_key: row.sealed_api_key,
        })
    }
}

fn select() -> SelectStatement {
    Query::select()
        .columns([
            SubAgentModels::Id,
            SubAgentModels::Version,
            SubAgentModels::Protocol,
            SubAgentModels::BaseUrl,
            SubAgentModels::Model,
            SubAgentModels::SealedApiKey,
            SubAgentModels::ContextWindow,
            SubAgentModels::RequestArgsJson,
            SubAgentModels::ChangedByUserId,
            SubAgentModels::ChangedAtUnixNanos,
        ])
        .from(SubAgentModels::Table)
        .to_owned()
}

/// The stored model, holding its row as `lock` says.
pub(crate) async fn find(
    tx: &mut Transaction<'_>,
    lock: RowLock,
) -> Result<Option<StoredModel>, Error> {
    let row: Option<ModelRow> = tx.fetch_optional_locked(select(), lock).await?;
    row.map(StoredModel::try_from).transpose()
}

/// The id of the stored model, holding its row as `lock` says. A row whose id
/// is not a UUID reads as none, which turns the Sub-agent off.
pub(crate) async fn find_id(
    tx: &mut Transaction<'_>,
    lock: RowLock,
) -> Result<Option<Uuid>, coral_db::Error> {
    let query = Query::select()
        .column(SubAgentModels::Id)
        .from(SubAgentModels::Table)
        .to_owned();
    let row: Option<(String,)> = tx.fetch_optional_locked(query, lock).await?;
    Ok(row.and_then(|(id,)| Uuid::parse_str(&id).ok()))
}

/// What both writes store of the model, beside the singleton mark.
fn stored_values(stored: &StoredModel) -> Result<[(SubAgentModels, SimpleExpr); 10], Error> {
    let StoredModel {
        model,
        sealed_api_key,
    } = stored;
    let settings = &model.settings;
    Ok([
        (SubAgentModels::Id, model.id.to_string().into()),
        (SubAgentModels::Version, model.version.get().into()),
        (
            SubAgentModels::Protocol,
            settings.protocol.as_storage_str().into(),
        ),
        (SubAgentModels::BaseUrl, settings.base_url.as_str().into()),
        (SubAgentModels::Model, settings.model.as_str().into()),
        (SubAgentModels::SealedApiKey, sealed_api_key.clone().into()),
        (
            SubAgentModels::ContextWindow,
            settings.context_window.map(i64::from).into(),
        ),
        (
            SubAgentModels::RequestArgsJson,
            request_args_json(settings)?.into(),
        ),
        (
            SubAgentModels::ChangedByUserId,
            model.changed_by.as_str().into(),
        ),
        (SubAgentModels::ChangedAtUnixNanos, model.changed_at.into()),
    ])
}

/// Inserts the model unless there is one. Of two concurrent inserts, the
/// first one stands. Returns whether this one did.
pub(crate) async fn insert(tx: &mut Transaction<'_>, stored: &StoredModel) -> Result<bool, Error> {
    let singleton = (SubAgentModels::Singleton, SimpleExpr::from(1_i64));
    let (columns, values): (Vec<_>, Vec<_>) = stored_values(stored)?
        .into_iter()
        .chain([singleton])
        .unzip();
    let inserted = tx
        .execute_change(
            Query::insert()
                .into_table(SubAgentModels::Table)
                .columns(columns)
                .values_panic(values)
                .on_conflict(
                    OnConflict::column(SubAgentModels::Singleton)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(inserted == 1)
}

/// Writes `next` over the model that the caller holds as `current`, under a
/// new id when the two differ.
pub(crate) async fn replace(
    tx: &mut Transaction<'_>,
    current: &AgentModel,
    next: &StoredModel,
) -> Result<(), Error> {
    let replaced = tx
        .execute_change(
            Query::update()
                .table(SubAgentModels::Table)
                .values(stored_values(next)?)
                .and_where(Expr::col(SubAgentModels::Id).eq(current.id.to_string()))
                .and_where(Expr::col(SubAgentModels::Version).eq(current.version.get()))
                .to_owned(),
        )
        .await?;
    if replaced == 1 {
        Ok(())
    } else {
        Err(Error::EtagMismatch)
    }
}

/// Deletes the model that the caller holds as `current`.
pub(crate) async fn delete(tx: &mut Transaction<'_>, current: &AgentModel) -> Result<(), Error> {
    let deleted = tx
        .execute_change(
            Query::delete()
                .from_table(SubAgentModels::Table)
                .and_where(Expr::col(SubAgentModels::Id).eq(current.id.to_string()))
                .and_where(Expr::col(SubAgentModels::Version).eq(current.version.get()))
                .to_owned(),
        )
        .await?;
    if deleted == 1 {
        Ok(())
    } else {
        Err(Error::EtagMismatch)
    }
}

fn request_args_json(settings: &ModelSettings) -> Result<String, Error> {
    serde_json::to_string(&settings.request_args)
        .map_err(|_unwritable| Error::CorruptData("a model's args"))
}
