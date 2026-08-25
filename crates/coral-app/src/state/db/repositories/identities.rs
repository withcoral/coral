use sea_query::{Alias, Expr, ExprTrait, OnConflict, Order, Query};

use crate::bootstrap::AppError;
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::state::db::schema::Identities;
use crate::state::db::{CoralTx, DbError, DbSession, IdentitySpecKey};

const IDENTITY_COUNT: &str = "identity_count";

/// Safe persisted fields for one owner-scoped identity instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityRecord {
    pub(crate) owner: IdentityOwner,
    pub(crate) name: IdentityName,
    pub(crate) spec_reference: IdentitySpecReference,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) updated_at_unix_nanos: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct IdentityRow {
    owner_kind: String,
    owner_key: String,
    workspace_id: Option<String>,
    name: String,
    identity_spec_workspace_id: Option<String>,
    identity_spec_name: String,
    identity_spec_fingerprint: String,
    issuer: String,
    identity_type: String,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentityRow {
    fn validate(self) -> Result<IdentityRecord, DbError> {
        if self.created_at_unix_nanos < 0 || self.updated_at_unix_nanos < self.created_at_unix_nanos
        {
            return Err(DbError::CorruptData(
                "identity row has invalid timestamps".to_string(),
            ));
        }
        let owner = IdentityOwner::from_storage_parts(
            &self.owner_kind,
            &self.owner_key,
            self.workspace_id.as_deref(),
        )?;
        let name = IdentityName::from_storage(&self.name)?;
        let spec_reference = IdentitySpecReference::from_storage_parts(
            &owner,
            self.identity_spec_workspace_id.as_deref(),
            &self.identity_spec_name,
            self.identity_spec_fingerprint,
            self.issuer,
            self.identity_type,
        )?;
        Ok(IdentityRecord {
            owner,
            name,
            spec_reference,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

#[derive(sqlx::FromRow)]
struct IdentityCountRow {
    identity_count: i64,
}

/// Repository for durable owner-scoped identity metadata.
pub(crate) struct IdentitiesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitiesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Load one identity by its complete owner and name key.
    pub(crate) async fn get(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
    ) -> Result<Option<IdentityRecord>, DbError> {
        let row: Option<IdentityRow> = self
            .session
            .fetch_optional(
                identity_select()
                    .and_where(identity_key_where(owner, name))
                    .to_owned(),
            )
            .await?;
        row.map(IdentityRow::validate).transpose()
    }

    /// List identities owned by one exact owner in name order.
    pub(crate) async fn list(
        &mut self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, DbError> {
        let rows: Vec<IdentityRow> = self
            .session
            .fetch_all(
                identity_select()
                    .and_where(identity_owner_where(owner))
                    .order_by(Identities::Name, Order::Asc)
                    .to_owned(),
            )
            .await?;
        rows.into_iter().map(IdentityRow::validate).collect()
    }

    /// Count identities pinned to one exact spec scope and name.
    pub(crate) async fn count_dependents(&mut self, key: &IdentitySpecKey) -> Result<u64, DbError> {
        self.count_where(identity_spec_where(key)).await
    }

    /// Count identities pinned to one exact spec scope, name, and fingerprint.
    pub(crate) async fn count_exact_dependents(
        &mut self,
        key: &IdentitySpecKey,
        fingerprint: &str,
    ) -> Result<u64, DbError> {
        self.count_where(
            identity_spec_where(key)
                .and(Expr::col(Identities::IdentitySpecFingerprint).eq(fingerprint)),
        )
        .await
    }

    async fn count_where(&mut self, predicate: sea_query::SimpleExpr) -> Result<u64, DbError> {
        let row: IdentityCountRow = self
            .session
            .fetch_optional(
                Query::select()
                    .expr_as(
                        Expr::col(Identities::Name).count(),
                        Alias::new(IDENTITY_COUNT),
                    )
                    .from(Identities::Table)
                    .and_where(predicate)
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| {
                DbError::CorruptData("identity count query returned no row".to_string())
            })?;
        u64::try_from(row.identity_count).map_err(|error| {
            DbError::CorruptData(format!(
                "identity count query returned invalid count {}: {error}",
                row.identity_count,
            ))
        })
    }
}

impl IdentitiesRepo<'_, CoralTx<'_>> {
    /// Insert or replace one identity while preserving its creation time.
    pub(crate) async fn upsert(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
        spec_reference: &IdentitySpecReference,
        now_unix_nanos: i64,
    ) -> Result<IdentityRecord, AppError> {
        validate_write_timestamp(now_unix_nanos)?;
        spec_reference.validate_for_owner(owner)?;
        let current_updated_at = Expr::col((Identities::Table, Identities::UpdatedAtUnixNanos));
        let key = spec_reference.key();
        let statement = Query::insert()
            .into_table(Identities::Table)
            .columns(identity_columns())
            .values_panic([
                Expr::val(owner.kind()),
                Expr::val(owner.key()),
                Expr::val(
                    owner
                        .workspace_name()
                        .map(|workspace| workspace.as_str().to_owned()),
                ),
                Expr::val(name.as_str()),
                Expr::val(key.scope().workspace_id().map(ToOwned::to_owned)),
                Expr::val(key.name()),
                Expr::val(spec_reference.fingerprint()),
                Expr::val(spec_reference.issuer()),
                Expr::val(spec_reference.identity_type()),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    Identities::OwnerKind,
                    Identities::OwnerKey,
                    Identities::Name,
                ])
                .update_columns([
                    Identities::IdentitySpecWorkspaceId,
                    Identities::IdentitySpecName,
                    Identities::IdentitySpecFingerprint,
                    Identities::Issuer,
                    Identities::IdentityType,
                ])
                .value(
                    Identities::UpdatedAtUnixNanos,
                    Expr::case(
                        current_updated_at.clone().gt(now_unix_nanos),
                        current_updated_at,
                    )
                    .finally(now_unix_nanos),
                )
                .to_owned(),
            )
            .to_owned();
        let rows_affected = self.session.execute_rows_affected(statement).await?;
        if rows_affected != 1 {
            return Err(AppError::Database(format!(
                "identity upsert affected {rows_affected} rows"
            )));
        }
        self.get(owner, name)
            .await?
            .ok_or_else(|| AppError::Database("identity disappeared after upsert".to_string()))
    }

    /// Delete one exact identity row.
    pub(crate) async fn delete(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
    ) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_rows_affected(
                Query::delete()
                    .from_table(Identities::Table)
                    .and_where(identity_key_where(owner, name))
                    .to_owned(),
            )
            .await?;
        zero_or_one_affected(rows_affected, "identity delete")
    }
}

fn validate_write_timestamp(now_unix_nanos: i64) -> Result<(), AppError> {
    match now_unix_nanos {
        0.. => Ok(()),
        _ => Err(AppError::InvalidInput(
            "identity timestamp is negative".to_string(),
        )),
    }
}

fn zero_or_one_affected(rows_affected: u64, operation: &str) -> Result<bool, DbError> {
    match rows_affected {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DbError::CorruptData(format!(
            "{operation} affected {rows_affected} rows"
        ))),
    }
}

fn identity_select() -> sea_query::SelectStatement {
    Query::select()
        .columns(identity_columns())
        .from(Identities::Table)
        .to_owned()
}

fn identity_columns() -> [Identities; 11] {
    [
        Identities::OwnerKind,
        Identities::OwnerKey,
        Identities::WorkspaceId,
        Identities::Name,
        Identities::IdentitySpecWorkspaceId,
        Identities::IdentitySpecName,
        Identities::IdentitySpecFingerprint,
        Identities::Issuer,
        Identities::IdentityType,
        Identities::CreatedAtUnixNanos,
        Identities::UpdatedAtUnixNanos,
    ]
}

fn identity_owner_where(owner: &IdentityOwner) -> sea_query::SimpleExpr {
    Expr::col(Identities::OwnerKind)
        .eq(owner.kind())
        .and(Expr::col(Identities::OwnerKey).eq(owner.key()))
}

fn identity_key_where(owner: &IdentityOwner, name: &IdentityName) -> sea_query::SimpleExpr {
    identity_owner_where(owner).and(Expr::col(Identities::Name).eq(name.as_str()))
}

fn identity_spec_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    let scope = match key.scope().workspace_id() {
        None => Expr::col(Identities::IdentitySpecWorkspaceId).is_null(),
        Some(workspace_id) => Expr::col(Identities::IdentitySpecWorkspaceId).eq(workspace_id),
    };
    scope.and(Expr::col(Identities::IdentitySpecName).eq(key.name()))
}
