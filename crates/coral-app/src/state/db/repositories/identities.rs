#![cfg_attr(not(test), expect(dead_code, reason = "Used by B4f."))]

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
    identity_spec_scope_kind: String,
    identity_spec_scope_id: String,
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
            &self.identity_spec_scope_kind,
            &self.identity_spec_scope_id,
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

/// Repository shell for durable owner-scoped identity rows.
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
    pub(crate) async fn load_optional(
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
    pub(crate) async fn list_for_owner(
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

    /// Count all identities pinned to one exact spec scope and name.
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
                Expr::val(key.scope().kind()),
                Expr::val(key.scope().scope_id()),
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
                    Identities::IdentitySpecScopeKind,
                    Identities::IdentitySpecScopeId,
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
        let rows_affected = self.session.execute_affected(statement).await?;
        if rows_affected != 1 {
            return Err(AppError::Database(format!(
                "identity upsert affected {rows_affected} rows"
            )));
        }
        self.load_optional(owner, name)
            .await?
            .ok_or_else(|| AppError::Database("identity disappeared after upsert".to_string()))
    }

    /// Delete one exact identity row and cascade any encrypted document.
    pub(crate) async fn delete(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
    ) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_affected(
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

fn identity_columns() -> [Identities; 12] {
    [
        Identities::OwnerKind,
        Identities::OwnerKey,
        Identities::WorkspaceId,
        Identities::Name,
        Identities::IdentitySpecScopeKind,
        Identities::IdentitySpecScopeId,
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
    Expr::col(Identities::IdentitySpecScopeKind)
        .eq(key.scope().kind())
        .and(Expr::col(Identities::IdentitySpecScopeId).eq(key.scope().scope_id()))
        .and(Expr::col(Identities::IdentitySpecName).eq(key.name()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::IdentityRow;
    use crate::bootstrap::AppError;
    use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
    use crate::identity::UserPrincipal;
    use crate::state::db::{
        CoralDb, DbError, DbRepos, IdentityRecord, IdentitySpecKey, ResolvedDatabaseConfig,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn persisted_identity_key_columns_fail_closed() {
        let row = |owner_key: &str, scope_kind: &str, scope_id: &str| IdentityRow {
            owner_kind: "user".to_string(),
            owner_key: owner_key.to_string(),
            workspace_id: None,
            name: "github".to_string(),
            identity_spec_scope_kind: scope_kind.to_string(),
            identity_spec_scope_id: scope_id.to_string(),
            identity_spec_name: "github".to_string(),
            identity_spec_fingerprint: "fingerprint".to_string(),
            issuer: "issuer".to_string(),
            identity_type: "fixed_token".to_string(),
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        };
        for corrupt in [
            row(" member ", "global", "__global__"),
            row("member", "workspace", "other"),
        ] {
            assert!(matches!(corrupt.validate(), Err(DbError::CorruptData(_))));
        }
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "Repository contract.")]
    async fn identity_rows_round_trip_and_count_exact_dependencies() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        let user = IdentityOwner::for_user(UserPrincipal::local());
        let workspace = WorkspaceName::parse("local").expect("workspace");
        let workspace_owner = IdentityOwner::workspace(workspace.clone());
        let other_owner =
            IdentityOwner::workspace(WorkspaceName::parse("other").expect("other workspace"));
        let alpha = IdentityName::parse("alpha").expect("alpha");
        let bravo = IdentityName::parse("bravo").expect("bravo");
        let charlie = IdentityName::parse("charlie").expect("charlie");
        let global_key = IdentitySpecKey::global("github").expect("global key");
        let workspace_key =
            IdentitySpecKey::workspace(workspace.clone(), "github").expect("workspace key");
        let user_f1 = reference(&user, global_key.clone(), "f1");
        let user_f2 = reference(&user, global_key.clone(), "f2");
        let workspace_global = reference(&workspace_owner, global_key.clone(), "f1");
        let workspace_local = reference(&workspace_owner, workspace_key.clone(), "f1");
        let replacement_key = IdentitySpecKey::global("gitlab").expect("replacement key");
        let replacement = IdentitySpecReference::new(
            &user,
            replacement_key.clone(),
            "f3",
            "replacement-issuer",
            "oauth",
        )
        .expect("replacement reference");

        let mut tx = db.begin().await.expect("begin seed tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("seed workspace");
        tx.workspaces()
            .ensure(other_owner.key(), 2)
            .await
            .expect("seed other workspace");
        for (owner, name, spec, now) in [
            (&user, &alpha, &user_f1, 10),
            (&user, &bravo, &user_f2, 11),
            (&workspace_owner, &alpha, &workspace_global, 12),
            (&workspace_owner, &charlie, &workspace_local, 13),
        ] {
            tx.identities()
                .upsert(owner, name, spec, now)
                .await
                .expect("upsert identity");
        }
        tx.commit().await.expect("commit seed tx");

        let mut session = &db;
        let user_rows = session
            .identities()
            .list_for_owner(&user)
            .await
            .expect("list user rows");
        assert_eq!(record_names(&user_rows), ["alpha", "bravo"]);
        let workspace_rows = session
            .identities()
            .list_for_owner(&workspace_owner)
            .await
            .expect("list workspace rows");
        assert_eq!(record_names(&workspace_rows), ["alpha", "charlie"]);
        assert_eq!(dependent_counts(&db, &global_key, "f1").await, (3, 2));
        assert_eq!(dependent_counts(&db, &workspace_key, "f1").await, (1, 1));

        let mut tx = db.begin().await.expect("begin replacement tx");
        let replaced = tx
            .identities()
            .upsert(&user, &alpha, &replacement, 20)
            .await
            .expect("replace all mutable fields");
        assert_eq!(
            (
                replaced.created_at_unix_nanos,
                replaced.updated_at_unix_nanos
            ),
            (10, 20)
        );
        assert_eq!(replaced.spec_reference, replacement);
        let regressed = tx
            .identities()
            .upsert(&user, &alpha, &replacement, 5)
            .await
            .expect("preserve update time under regressed clock");
        assert_eq!(regressed.updated_at_unix_nanos, 20);
        let deleted = tx
            .identities()
            .delete(&workspace_owner, &alpha)
            .await
            .unwrap();
        let missing = tx
            .identities()
            .delete(&workspace_owner, &alpha)
            .await
            .unwrap();
        assert!(deleted && !missing);
        let wrong_owner = tx
            .identities()
            .upsert(&other_owner, &alpha, &workspace_local, 20)
            .await
            .expect_err("cross-workspace reference must fail");
        assert!(matches!(wrong_owner, AppError::InvalidInput(_)));
        let negative_time = tx
            .identities()
            .upsert(&user, &alpha, &user_f2, -1)
            .await
            .expect_err("negative timestamp must fail");
        assert!(matches!(negative_time, AppError::InvalidInput(_)));
        tx.commit().await.expect("commit replacement tx");

        let user_row = session
            .identities()
            .load_optional(&user, &alpha)
            .await
            .unwrap()
            .expect("reloaded replacement");
        assert_eq!(user_row, regressed);
        assert_eq!(dependent_counts(&db, &global_key, "f2").await, (1, 1));
        assert_eq!(dependent_counts(&db, &replacement_key, "f3").await, (1, 1));
    }

    fn reference(
        owner: &IdentityOwner,
        key: IdentitySpecKey,
        fingerprint: &str,
    ) -> IdentitySpecReference {
        IdentitySpecReference::new(owner, key, fingerprint, "issuer", "fixed_token")
            .expect("valid reference")
    }

    fn record_names(records: &[IdentityRecord]) -> Vec<&str> {
        records.iter().map(|record| record.name.as_str()).collect()
    }

    async fn dependent_counts(
        db: &CoralDb,
        key: &IdentitySpecKey,
        fingerprint: &str,
    ) -> (u64, u64) {
        let mut session = db;
        let total = session.identities().count_dependents(key).await.unwrap();
        let exact = session
            .identities()
            .count_exact_dependents(key, fingerprint)
            .await
            .unwrap();
        (total, exact)
    }
}
