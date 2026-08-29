//! Installed-source catalog persistence.
//!
//! One [`InstalledSource`] spans three tables — `sources` plus the
//! `source_variables` and `source_secret_keys` child sets — so every read here
//! reassembles it and every write replaces the child sets wholesale inside the
//! caller's transaction.
//!
//! Deletions additionally write a row to `source_tombstones`. The tombstone is
//! what makes a deletion stick when several hosts share one database: the
//! deleting binary records the removal, and a peer's boot import consults the
//! record instead of re-adding the entry from its own stale config mirror.

use std::collections::{BTreeMap, BTreeSet};

use sea_query::{Expr, ExprTrait, OnConflict, Query, SelectStatement};
use uuid::Uuid;

use crate::credentials::CredentialStorageKind;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::db::DbError;
use crate::state::db::schema::{SourceSecretKeys, SourceTombstones, SourceVariables, Sources};
use crate::state::db::session::DbSession;
use crate::workspaces::WorkspaceName;

/// One `sources` row, before its child sets are folded back in.
#[derive(sqlx::FromRow)]
struct SourceRow {
    name: String,
    version: Option<String>,
    origin_kind: String,
    credential_storage: Option<String>,
    credential_revision: String,
}

pub(crate) struct SourcesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> SourcesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Lists one workspace's installed sources, ordered by source name.
    ///
    /// The ordering is applied in Rust rather than in SQL because a source name
    /// is a name its installer chose: ordering it in the database would order it
    /// under the backend's collation, and `SQLite`'s binary comparison and
    /// Postgres's locale-aware default disagree on names that differ only by
    /// case or punctuation. One listing must not depend on which backend a
    /// deployment happens to run.
    pub(crate) async fn list_workspace_sources(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, DbError> {
        self.load(workspace_name, None).await
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the source manager reads through this next")
    )]
    pub(crate) async fn get_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<InstalledSource>, DbError> {
        Ok(self.load(workspace_name, Some(source_name)).await?.pop())
    }

    /// Writes one source and replaces its child sets.
    ///
    /// The child sets are deleted and reinserted rather than merged: they are
    /// sets, so a stale key left behind by a merge would be indistinguishable
    /// from a configured one. Everything runs on the caller's session, so a
    /// caller in a transaction gets the whole replacement atomically.
    ///
    /// Any tombstone for this `(workspace, name)` is cleared first — re-adding a
    /// source is exactly the operator action that revokes an earlier deletion,
    /// and leaving the record standing would have a peer's boot import skip the
    /// re-added entry.
    pub(crate) async fn upsert_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
        now_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let workspace_id = workspace_name.as_str();
        let name = source.name.as_str();

        self.clear_tombstone(workspace_id, name).await?;

        let statement = Query::insert()
            .into_table(Sources::Table)
            .columns([
                Sources::WorkspaceId,
                Sources::Name,
                Sources::Version,
                Sources::OriginKind,
                Sources::CredentialStorage,
                Sources::CredentialRevision,
                Sources::CreatedAtUnixNanos,
                Sources::UpdatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_owned()),
                Expr::val(name.to_owned()),
                Expr::val(source.version.clone()),
                Expr::val(source.origin.as_config_value().to_owned()),
                Expr::val(
                    source
                        .credential_storage
                        .map(|kind| kind.as_config_value().to_owned()),
                ),
                Expr::val(source.credential_revision.to_string()),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                // `created_at_unix_nanos` is deliberately absent: an update
                // must not restate when the source was first installed.
                OnConflict::columns([Sources::WorkspaceId, Sources::Name])
                    .update_columns([
                        Sources::Version,
                        Sources::OriginKind,
                        Sources::CredentialStorage,
                        Sources::CredentialRevision,
                        Sources::UpdatedAtUnixNanos,
                    ])
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await?;

        self.replace_variables(workspace_id, name, &source.variables)
            .await?;
        self.replace_secret_keys(workspace_id, name, &source.secrets)
            .await
    }

    /// Removes one source and records the deletion, reporting whether a row was
    /// there to remove.
    ///
    /// The child and artifact rows go with it through the schema's cascade, and
    /// the tombstone is written on the same session, so a caller in a
    /// transaction cannot commit a removal without its record.
    ///
    /// The tombstone is written whether or not a row was removed. A source this
    /// database never learned about can still exist in a host's config mirror —
    /// written there by an older binary — and deleting it has to stick for the
    /// same reason deleting a known one does.
    pub(crate) async fn remove_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        now_unix_nanos: i64,
    ) -> Result<bool, DbError> {
        let workspace_id = workspace_name.as_str();
        let name = source_name.as_str();

        let delete = Query::delete()
            .from_table(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(Sources::Name).eq(name))
            .to_owned();
        let removed = self.session.execute_rows_affected(delete).await? == 1;

        let tombstone = Query::insert()
            .into_table(SourceTombstones::Table)
            .columns([
                SourceTombstones::WorkspaceId,
                SourceTombstones::SourceName,
                SourceTombstones::DeletedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_owned()),
                Expr::val(name.to_owned()),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([SourceTombstones::WorkspaceId, SourceTombstones::SourceName])
                    .update_column(SourceTombstones::DeletedAtUnixNanos)
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(tombstone).await?;

        Ok(removed)
    }

    /// Reports whether this database records a deletion for `(workspace, name)`.
    pub(crate) async fn is_tombstoned(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<bool, DbError> {
        let statement = Query::select()
            .column(SourceTombstones::SourceName)
            .from(SourceTombstones::Table)
            .and_where(Expr::col(SourceTombstones::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceTombstones::SourceName).eq(source_name.as_str()))
            .to_owned();
        let found: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(found.is_some())
    }

    /// Reads one workspace's sources, optionally narrowed to a single name.
    ///
    /// Both reads take the same three statements — parent rows plus the two
    /// child sets — so a single source and a whole workspace are assembled by
    /// one piece of code rather than two that can drift apart.
    async fn load(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: Option<&SourceName>,
    ) -> Result<Vec<InstalledSource>, DbError> {
        let workspace_id = workspace_name.as_str();
        let name = source_name.map(SourceName::as_str);

        let mut parents = Query::select()
            .columns([
                Sources::Name,
                Sources::Version,
                Sources::OriginKind,
                Sources::CredentialStorage,
                Sources::CredentialRevision,
            ])
            .from(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
            .to_owned();
        if let Some(name) = name {
            parents = parents
                .and_where(Expr::col(Sources::Name).eq(name))
                .to_owned();
        }
        let rows: Vec<SourceRow> = self.session.fetch_all(parents).await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let variables_query = Query::select()
            .columns([
                SourceVariables::SourceName,
                SourceVariables::Key,
                SourceVariables::Value,
            ])
            .from(SourceVariables::Table)
            .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_id))
            .to_owned();
        let variables: Vec<(String, String, String)> = self
            .session
            .fetch_all(narrow_to_source(
                variables_query,
                SourceVariables::SourceName,
                name,
            ))
            .await?;

        let secrets_query = Query::select()
            .columns([SourceSecretKeys::SourceName, SourceSecretKeys::Key])
            .from(SourceSecretKeys::Table)
            .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_id))
            .to_owned();
        let secrets: Vec<(String, String)> = self
            .session
            .fetch_all(narrow_to_source(
                secrets_query,
                SourceSecretKeys::SourceName,
                name,
            ))
            .await?;

        build_sources(rows, variables, secrets)
    }

    async fn clear_tombstone(
        &mut self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(SourceTombstones::Table)
            .and_where(Expr::col(SourceTombstones::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(SourceTombstones::SourceName).eq(source_name))
            .to_owned();
        self.session.execute(statement).await
    }

    async fn replace_variables(
        &mut self,
        workspace_id: &str,
        source_name: &str,
        variables: &BTreeMap<String, String>,
    ) -> Result<(), DbError> {
        let delete = Query::delete()
            .from_table(SourceVariables::Table)
            .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(SourceVariables::SourceName).eq(source_name))
            .to_owned();
        self.session.execute(delete).await?;
        if variables.is_empty() {
            return Ok(());
        }

        let mut insert = Query::insert()
            .into_table(SourceVariables::Table)
            .columns([
                SourceVariables::WorkspaceId,
                SourceVariables::SourceName,
                SourceVariables::Key,
                SourceVariables::Value,
            ])
            .to_owned();
        for (key, value) in variables {
            insert.values_panic([
                Expr::val(workspace_id.to_owned()),
                Expr::val(source_name.to_owned()),
                Expr::val(key.clone()),
                Expr::val(value.clone()),
            ]);
        }
        self.session.execute(insert).await
    }

    async fn replace_secret_keys(
        &mut self,
        workspace_id: &str,
        source_name: &str,
        secrets: &[String],
    ) -> Result<(), DbError> {
        let delete = Query::delete()
            .from_table(SourceSecretKeys::Table)
            .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name))
            .to_owned();
        self.session.execute(delete).await?;

        // Deduplicated because the table is keyed on the key: a caller that
        // names the same secret twice is asking for one row, not a conflict.
        let keys: BTreeSet<&String> = secrets.iter().collect();
        if keys.is_empty() {
            return Ok(());
        }

        let mut insert = Query::insert()
            .into_table(SourceSecretKeys::Table)
            .columns([
                SourceSecretKeys::WorkspaceId,
                SourceSecretKeys::SourceName,
                SourceSecretKeys::Key,
            ])
            .to_owned();
        for key in keys {
            insert.values_panic([
                Expr::val(workspace_id.to_owned()),
                Expr::val(source_name.to_owned()),
                Expr::val(key.clone()),
            ]);
        }
        self.session.execute(insert).await
    }
}

/// Narrows a workspace-wide child-set read to one source, when one is named.
fn narrow_to_source<C>(
    mut statement: SelectStatement,
    column: C,
    source_name: Option<&str>,
) -> SelectStatement
where
    C: sea_query::IntoColumnRef,
{
    let Some(source_name) = source_name else {
        return statement;
    };
    statement
        .and_where(Expr::col(column).eq(source_name))
        .to_owned()
}

/// Folds the child sets back into their parent rows.
///
/// Both sets are rebuilt through ordered Rust collections rather than an
/// `ORDER BY`, for the collation reason spelled out on
/// [`SourcesRepo::list_workspace_sources`].
fn build_sources(
    rows: Vec<SourceRow>,
    variables: Vec<(String, String, String)>,
    secrets: Vec<(String, String)>,
) -> Result<Vec<InstalledSource>, DbError> {
    let mut grouped_variables: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (source_name, key, value) in variables {
        grouped_variables
            .entry(source_name)
            .or_default()
            .insert(key, value);
    }
    let mut grouped_secrets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (source_name, key) in secrets {
        grouped_secrets.entry(source_name).or_default().insert(key);
    }

    let mut sources = rows
        .into_iter()
        .map(|row| {
            let name = decode_source_name(&row.name)?;
            let variables = grouped_variables.remove(&row.name).unwrap_or_default();
            let secrets = grouped_secrets.remove(&row.name).unwrap_or_default();
            let credential_storage = row
                .credential_storage
                .as_deref()
                .map(decode_credential_storage)
                .transpose()?;
            Ok(InstalledSource {
                name,
                version: row.version,
                variables,
                secrets: secrets.into_iter().collect(),
                credential_storage,
                credential_revision: decode_credential_revision(&row.credential_revision)?,
                origin: decode_origin(&row.origin_kind)?,
            })
        })
        .collect::<Result<Vec<InstalledSource>, DbError>>()?;
    sources.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sources)
}

fn decode_source_name(value: &str) -> Result<SourceName, DbError> {
    SourceName::parse(value)
        .map_err(|error| corrupt(&format!("source row name '{value}' is unusable: {error}")))
}

fn decode_origin(value: &str) -> Result<SourceOrigin, DbError> {
    match value {
        "bundled" => Ok(SourceOrigin::Bundled),
        "imported" => Ok(SourceOrigin::Imported),
        other => Err(corrupt(&format!(
            "source row has an unrecognized origin '{other}'"
        ))),
    }
}

fn decode_credential_storage(value: &str) -> Result<CredentialStorageKind, DbError> {
    match value {
        "file" => Ok(CredentialStorageKind::File),
        "keychain" => Ok(CredentialStorageKind::Keychain),
        other => Err(corrupt(&format!(
            "source row has an unrecognized credential storage '{other}'"
        ))),
    }
}

fn decode_credential_revision(value: &str) -> Result<Uuid, DbError> {
    Uuid::parse_str(value).map_err(|error| {
        corrupt(&format!(
            "source row credential revision '{value}' is not a uuid: {error}"
        ))
    })
}

fn corrupt(detail: &str) -> DbError {
    DbError::CorruptData(detail.to_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sea_query::{Alias, Expr, ExprTrait, Func, Query};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{decode_credential_storage, decode_origin};
    use crate::bootstrap;
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::session::{DbRepos, DbSession};
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn source_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_source_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn source_repository_contract_on_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_source_repository_round_trip(&db).await;
    }

    #[test]
    fn unrecognized_enum_columns_decode_as_corrupt_data() {
        assert_eq!(
            decode_origin("imported").expect("imported decodes"),
            SourceOrigin::Imported
        );
        assert_eq!(
            decode_credential_storage("keychain").expect("keychain decodes"),
            CredentialStorageKind::Keychain
        );
        assert!(
            decode_origin("user").is_err(),
            "a legacy origin value must not decode"
        );
        assert!(
            decode_credential_storage("vault").is_err(),
            "an unknown credential store must not decode"
        );
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };

        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    /// Exercises the whole surface against one backend: install, read back,
    /// update, delete with its tombstone, and re-add.
    async fn assert_source_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace_name();
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace tx");

        let installed = installed_source("beta-source", SourceOrigin::Imported);
        let sibling = installed_source("alpha-source", SourceOrigin::Bundled);
        let mut tx = db.begin().await.expect("begin install tx");
        tx.sources()
            .upsert_source(&workspace, &installed, 10)
            .await
            .expect("install source");
        tx.sources()
            .upsert_source(&workspace, &sibling, 11)
            .await
            .expect("install sibling");
        tx.commit().await.expect("commit install tx");

        let mut session = db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &installed.name)
                .await
                .expect("read installed source"),
            Some(installed.clone())
        );
        assert_eq!(
            session
                .sources()
                .list_workspace_sources(&workspace)
                .await
                .expect("list sources"),
            vec![sibling.clone(), installed.clone()],
            "sources must come back ordered by name"
        );

        assert_update_replaces_child_sets(db, &workspace, &installed).await;
        assert_delete_tombstones_and_readd_clears(db, &workspace, &installed, &sibling).await;
    }

    /// An update rewrites the child sets rather than merging into them, and
    /// leaves the sibling source untouched.
    async fn assert_update_replaces_child_sets(
        db: &CoralDb,
        workspace: &WorkspaceName,
        installed: &InstalledSource,
    ) {
        let mut updated = installed.clone();
        updated.version = None;
        updated.variables = BTreeMap::from([("region".to_owned(), "eu-west-1".to_owned())]);
        updated.secrets = vec!["rotated_token".to_owned()];
        updated.credential_storage = Some(CredentialStorageKind::Keychain);
        updated.credential_revision = Uuid::from_u128(0x5eed);

        let mut tx = db.begin().await.expect("begin update tx");
        tx.sources()
            .upsert_source(workspace, &updated, 20)
            .await
            .expect("update source");
        tx.commit().await.expect("commit update tx");

        let mut session = db;
        assert_eq!(
            session
                .sources()
                .get_source(workspace, &updated.name)
                .await
                .expect("read updated source"),
            Some(updated)
        );
    }

    /// A delete takes the child rows with it and records the removal in the
    /// same transaction; re-adding the source revokes the record.
    async fn assert_delete_tombstones_and_readd_clears(
        db: &CoralDb,
        workspace: &WorkspaceName,
        installed: &InstalledSource,
        sibling: &InstalledSource,
    ) {
        let mut session = db;
        assert!(
            !session
                .sources()
                .is_tombstoned(workspace, &installed.name)
                .await
                .expect("read tombstone before delete")
        );
        assert_eq!(
            child_row_counts(db, workspace, &installed.name).await,
            (1, 1),
            "the update should have left one variable and one secret key behind"
        );

        let mut tx = db.begin().await.expect("begin delete tx");
        assert!(
            tx.sources()
                .remove_source(workspace, &installed.name, 30)
                .await
                .expect("remove source")
        );
        tx.commit().await.expect("commit delete tx");

        assert_eq!(
            session
                .sources()
                .get_source(workspace, &installed.name)
                .await
                .expect("read deleted source"),
            None
        );
        assert!(
            session
                .sources()
                .is_tombstoned(workspace, &installed.name)
                .await
                .expect("read tombstone after delete")
        );
        assert_eq!(
            child_row_counts(db, workspace, &installed.name).await,
            (0, 0),
            "the delete must take the child rows with the source row"
        );
        assert_eq!(
            session
                .sources()
                .list_workspace_sources(workspace)
                .await
                .expect("list after delete"),
            vec![sibling.clone()],
            "the sibling source and its child rows must survive the delete"
        );

        // Deleting again still records the removal but reports nothing removed.
        let mut tx = db.begin().await.expect("begin second delete tx");
        assert!(
            !tx.sources()
                .remove_source(workspace, &installed.name, 31)
                .await
                .expect("remove missing source")
        );
        tx.commit().await.expect("commit second delete tx");

        // Re-adding the source is what revokes the deletion record, and the
        // child rows the cascade took must come back with it.
        let mut tx = db.begin().await.expect("begin re-add tx");
        tx.sources()
            .upsert_source(workspace, installed, 40)
            .await
            .expect("re-add source");
        tx.commit().await.expect("commit re-add tx");

        assert!(
            !session
                .sources()
                .is_tombstoned(workspace, &installed.name)
                .await
                .expect("read tombstone after re-add")
        );
        assert_eq!(
            session
                .sources()
                .get_source(workspace, &installed.name)
                .await
                .expect("read re-added source"),
            Some(installed.clone())
        );
    }

    /// Counts one source's `(variables, secret keys)` rows straight from the
    /// physical tables, so the cascade is proven rather than inferred from a
    /// reassembled read. Both tables name their key columns identically, which
    /// is what lets one loop read them.
    async fn child_row_counts(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> (i64, i64) {
        let mut session = db;
        let mut counts = [0_i64; 2];
        for (slot, table) in counts
            .iter_mut()
            .zip(["source_variables", "source_secret_keys"])
        {
            let statement = Query::select()
                .expr(Func::count(Expr::val(1)))
                .from(Alias::new(table))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.as_str()))
                .and_where(Expr::col(Alias::new("source_name")).eq(source_name.as_str()))
                .to_owned();
            let counted: Option<(i64,)> = session
                .fetch_optional(statement)
                .await
                .expect("count child rows");
            *slot = counted.expect("a count always returns a row").0;
        }
        (counts[0], counts[1])
    }

    fn installed_source(name: &str, origin: SourceOrigin) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("parse source name"),
            version: Some("1.2.3".to_owned()),
            variables: BTreeMap::from([
                ("account".to_owned(), "acme".to_owned()),
                ("region".to_owned(), "us-east-1".to_owned()),
            ]),
            secrets: vec!["api_token".to_owned(), "refresh_token".to_owned()],
            credential_storage: Some(CredentialStorageKind::File),
            credential_revision: Uuid::from_u128(0x00c0_ffee),
            origin,
        }
    }

    fn unique_workspace_name() -> WorkspaceName {
        let suffix = Uuid::new_v4().simple().to_string();
        WorkspaceName::parse(&format!("workspace-{suffix}")).expect("parse workspace name")
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
