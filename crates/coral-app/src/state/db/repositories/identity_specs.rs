#![expect(
    dead_code,
    reason = "identity persistence APIs land before B2 wires production consumers"
)]

use sea_query::{Expr, ExprTrait, Order, Query};

use crate::bootstrap::AppError;
use crate::state::db::schema::IdentitySpecs;
use crate::state::db::{DbError, DbSession};
use crate::workspaces::WorkspaceName;
use coral_spec::validate_identity_spec_name;
use uuid::{Uuid, Variant, Version};

/// Opaque database identity for one persisted identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecId(String);

impl IdentitySpecId {
    pub(super) fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub(super) fn from_storage(value: String) -> Result<Self, DbError> {
        let parsed = Uuid::parse_str(&value).map_err(|error| {
            DbError::CorruptData(format!("invalid identity spec id '{value}': {error}"))
        })?;
        if parsed.get_version() != Some(Version::Random) || parsed.get_variant() != Variant::RFC4122
        {
            return Err(DbError::CorruptData(format!(
                "identity spec id '{value}' is not an RFC 4122 UUID v4"
            )));
        }
        if parsed.to_string() != value {
            return Err(DbError::CorruptData(format!(
                "identity spec id '{value}' is not canonical"
            )));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Definition scope for one durable identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentitySpecScope {
    /// A globally installed identity spec definition.
    Global,
    /// An identity spec definition scoped to one workspace.
    Workspace(WorkspaceName),
}

impl IdentitySpecScope {
    /// Build the global identity-spec scope.
    pub(crate) fn global() -> Self {
        Self::Global
    }

    /// Build a workspace identity-spec scope.
    pub(crate) fn workspace(workspace_name: WorkspaceName) -> Self {
        Self::Workspace(workspace_name)
    }

    pub(super) fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Workspace(workspace_name) => Some(workspace_name.as_str()),
        }
    }
}

/// Logical lookup key for one global or workspace-scoped identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecKey {
    /// Scope that owns this identity spec definition.
    scope: IdentitySpecScope,
    /// Identity spec name unique within the scope.
    name: String,
}

impl IdentitySpecKey {
    /// Build an identity-spec key from a scope and validated name.
    pub(crate) fn new(scope: IdentitySpecScope, name: &str) -> Result<Self, AppError> {
        Ok(Self {
            scope,
            name: parse_identity_spec_name(name)?,
        })
    }

    /// Build a global identity-spec key.
    pub(crate) fn global(name: &str) -> Result<Self, AppError> {
        Self::new(IdentitySpecScope::global(), name)
    }

    /// Build a workspace-scoped identity-spec key.
    pub(crate) fn workspace(workspace_name: WorkspaceName, name: &str) -> Result<Self, AppError> {
        Self::new(IdentitySpecScope::workspace(workspace_name), name)
    }

    /// Borrow the scope selected for this identity spec.
    pub(crate) fn scope(&self) -> &IdentitySpecScope {
        &self.scope
    }

    /// Borrow the validated identity-spec name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn from_spec_storage_parts(
        workspace_id: Option<&str>,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match workspace_id {
            None => IdentitySpecScope::Global,
            Some(workspace_id) => IdentitySpecScope::Workspace(parse_workspace_name(workspace_id)?),
        };
        Ok(Self {
            scope,
            name: parse_persisted_identity_spec_name(name)?,
        })
    }
}

/// Persisted authored definition for one identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecRecord {
    /// Opaque database identity used by dependent persistence rows.
    pub(crate) id: IdentitySpecId,
    /// Scope and name that identify this identity spec.
    pub(crate) key: IdentitySpecKey,
    /// Authored identity spec version string.
    pub(crate) version: String,
    /// Human-readable identity spec description.
    pub(crate) description: String,
    /// Issuer identifier declared by the identity spec.
    pub(crate) issuer: String,
    /// Identity mechanism declared by the identity spec.
    pub(crate) identity_type: String,
    /// Authored identity spec manifest YAML.
    pub(crate) manifest_yaml: String,
    /// Creation timestamp in Unix nanoseconds.
    pub(crate) created_at_unix_nanos: i64,
    /// Last update timestamp in Unix nanoseconds.
    pub(crate) updated_at_unix_nanos: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct IdentitySpecRow {
    id: String,
    workspace_id: Option<String>,
    name: String,
    version: String,
    description: String,
    issuer: String,
    identity_type: String,
    manifest_yaml: String,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentitySpecRow {
    fn validate(self) -> Result<IdentitySpecRecord, DbError> {
        if [
            &self.version,
            &self.issuer,
            &self.identity_type,
            &self.manifest_yaml,
        ]
        .into_iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(DbError::CorruptData(
                "identity spec row has an empty required field".to_string(),
            ));
        }
        if self.created_at_unix_nanos < 0 || self.updated_at_unix_nanos < self.created_at_unix_nanos
        {
            return Err(DbError::CorruptData(
                "identity spec row has invalid timestamps".to_string(),
            ));
        }
        Ok(IdentitySpecRecord {
            id: IdentitySpecId::from_storage(self.id)?,
            key: IdentitySpecKey::from_spec_storage_parts(
                self.workspace_id.as_deref(),
                &self.name,
            )?,
            version: self.version,
            description: self.description,
            issuer: self.issuer,
            identity_type: self.identity_type,
            manifest_yaml: self.manifest_yaml,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository for durable DSL v4 identity spec definitions.
pub(crate) struct IdentitySpecsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Load one identity spec by exact scope and name.
    pub(crate) async fn get(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecRecord>, DbError> {
        let row: Option<IdentitySpecRow> = self
            .session
            .fetch_optional(
                identity_spec_select()
                    .and_where(identity_spec_key_where(key))
                    .to_owned(),
            )
            .await?;
        row.map(IdentitySpecRow::validate).transpose()
    }

    /// List identity specs from one exact scope in name order.
    pub(crate) async fn list(
        &mut self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        let rows: Vec<IdentitySpecRow> = self
            .session
            .fetch_all(
                identity_spec_select()
                    .and_where(identity_spec_scope_where(scope))
                    .order_by(IdentitySpecs::Name, Order::Asc)
                    .to_owned(),
            )
            .await?;
        rows.into_iter().map(IdentitySpecRow::validate).collect()
    }
}

/// Repository shell for encrypted setup-input documents owned by identity specs.
pub(crate) struct IdentitySpecDocumentsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecDocumentsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec document repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }
}

fn parse_identity_spec_name(name: &str) -> Result<String, AppError> {
    validate_identity_spec_name(name).map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(name.to_string())
}

fn parse_workspace_name(workspace_id: &str) -> Result<WorkspaceName, DbError> {
    let workspace_name = WorkspaceName::parse(workspace_id).map_err(|error| {
        DbError::CorruptData(format!("invalid workspace id '{workspace_id}': {error}"))
    })?;
    if workspace_name.as_str() != workspace_id {
        return Err(DbError::CorruptData(format!(
            "workspace id '{workspace_id}' is not normalized"
        )));
    }
    Ok(workspace_name)
}

fn parse_persisted_identity_spec_name(name: &str) -> Result<String, DbError> {
    let parsed = parse_identity_spec_name(name).map_err(|error| {
        DbError::CorruptData(format!("invalid identity spec name '{name}': {error}"))
    })?;
    if parsed != name {
        return Err(DbError::CorruptData(format!(
            "identity spec name '{name}' is not normalized"
        )));
    }
    Ok(parsed)
}

fn identity_spec_select() -> sea_query::SelectStatement {
    Query::select()
        .columns(identity_spec_columns())
        .from(IdentitySpecs::Table)
        .to_owned()
}

fn identity_spec_columns() -> [IdentitySpecs; 10] {
    [
        IdentitySpecs::Id,
        IdentitySpecs::WorkspaceId,
        IdentitySpecs::Name,
        IdentitySpecs::Version,
        IdentitySpecs::Description,
        IdentitySpecs::Issuer,
        IdentitySpecs::IdentityType,
        IdentitySpecs::ManifestYaml,
        IdentitySpecs::CreatedAtUnixNanos,
        IdentitySpecs::UpdatedAtUnixNanos,
    ]
}

fn identity_spec_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    identity_spec_scope_where(&key.scope).and(Expr::col(IdentitySpecs::Name).eq(key.name.as_str()))
}

fn identity_spec_scope_where(scope: &IdentitySpecScope) -> sea_query::SimpleExpr {
    match scope {
        IdentitySpecScope::Global => Expr::col(IdentitySpecs::WorkspaceId).is_null(),
        IdentitySpecScope::Workspace(workspace_name) => {
            Expr::col(IdentitySpecs::WorkspaceId).eq(workspace_name.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{IdentitySpecId, IdentitySpecKey, IdentitySpecRecord, identity_spec_columns};
    use crate::bootstrap::AppError;
    use crate::state::db::schema::IdentitySpecs;
    use crate::state::db::{CoralDb, CoralTx, DbError, DbRepos, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[derive(Clone, Copy)]
    struct SpecSeed {
        version: &'static str,
        description: &'static str,
        issuer: &'static str,
        identity_type: &'static str,
        manifest_yaml: &'static str,
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    }

    const VALID_SPEC: SpecSeed = SpecSeed {
        version: "1.0.0",
        description: "",
        issuer: "github",
        identity_type: "oauth",
        manifest_yaml: "kind: identity\nname: github\n",
        created_at_unix_nanos: 10,
        updated_at_unix_nanos: 20,
    };

    #[test]
    fn caller_names_keep_invalid_input_classification() {
        for name in [
            "bad/name",
            "github-oauth",
            "github oauth",
            "9github",
            " github",
            "github ",
        ] {
            assert!(matches!(
                IdentitySpecKey::global(name),
                Err(AppError::InvalidInput(_))
            ));
        }

        let key = IdentitySpecKey::global("github_oauth2").expect("valid identity spec name");
        assert_eq!(key.name(), "github_oauth2");
        assert!(matches!(key.scope(), super::IdentitySpecScope::Global));
    }

    #[test]
    fn persisted_scope_keys_reject_non_normalized_identifiers() {
        for result in [
            IdentitySpecKey::from_spec_storage_parts(None, " github"),
            IdentitySpecKey::from_spec_storage_parts(Some(" default"), "github"),
            IdentitySpecKey::from_spec_storage_parts(None, "github "),
            IdentitySpecKey::from_spec_storage_parts(None, "github-oauth"),
        ] {
            assert!(matches!(result, Err(DbError::CorruptData(_))));
        }
    }

    #[test]
    fn persisted_identity_spec_ids_must_be_canonical_rfc_4122_uuid_v4_values() {
        let id = IdentitySpecId::new();
        assert_eq!(
            IdentitySpecId::from_storage(id.as_str().to_string()).expect("canonical id"),
            id
        );

        for invalid in [
            "not-a-uuid".to_string(),
            Uuid::nil().to_string(),
            Uuid::new_v4().simple().to_string(),
            Uuid::new_v4().to_string().to_uppercase(),
            "00000000-0000-4000-c000-000000000000".to_string(),
        ] {
            assert!(matches!(
                IdentitySpecId::from_storage(invalid),
                Err(DbError::CorruptData(_))
            ));
        }
    }

    #[tokio::test]
    async fn reads_exact_identity_spec_scopes_from_sqlite() {
        let (_temp, db) = open_sqlite().await;
        let workspace = WorkspaceName::parse("team").expect("workspace");
        let other_workspace = WorkspaceName::parse("other_team").expect("other workspace");
        let global_alpha = IdentitySpecKey::global("alpha").expect("global key");
        let global_zebra = IdentitySpecKey::global("zebra").expect("global key");
        let workspace_alpha =
            IdentitySpecKey::workspace(workspace.clone(), "alpha").expect("workspace key");
        let workspace_beta =
            IdentitySpecKey::workspace(workspace.clone(), "beta").expect("workspace key");
        let other_gamma = IdentitySpecKey::workspace(other_workspace.clone(), "gamma")
            .expect("other workspace key");

        let mut tx = db.begin().await.expect("begin seed transaction");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("create workspace");
        tx.workspaces()
            .ensure(other_workspace.as_str(), 2)
            .await
            .expect("create other workspace");
        let global_alpha_id = insert_spec(&mut tx, &global_alpha, VALID_SPEC).await;
        insert_spec(
            &mut tx,
            &global_zebra,
            SpecSeed {
                version: "2.0.0",
                ..VALID_SPEC
            },
        )
        .await;
        insert_spec(
            &mut tx,
            &workspace_alpha,
            SpecSeed {
                version: "3.0.0",
                ..VALID_SPEC
            },
        )
        .await;
        insert_spec(&mut tx, &workspace_beta, VALID_SPEC).await;
        insert_spec(&mut tx, &other_gamma, VALID_SPEC).await;
        tx.commit().await.expect("commit seed transaction");

        let mut session = &db;
        assert_eq!(
            session
                .identity_specs()
                .get(&global_alpha)
                .await
                .expect("read global spec"),
            Some(expected_record(
                global_alpha_id,
                global_alpha.clone(),
                VALID_SPEC,
            ))
        );
        assert_eq!(
            session
                .identity_specs()
                .get(&workspace_alpha)
                .await
                .expect("read workspace spec")
                .map(|record| record.version),
            Some("3.0.0".to_string())
        );

        let missing_workspace_zebra =
            IdentitySpecKey::workspace(workspace.clone(), "zebra").expect("workspace key");
        assert!(
            session
                .identity_specs()
                .get(&missing_workspace_zebra)
                .await
                .expect("read exact missing spec")
                .is_none(),
            "repository reads must not fall back to the global scope"
        );

        let global = session
            .identity_specs()
            .list(global_alpha.scope())
            .await
            .expect("list global specs");
        assert_eq!(spec_names(&global), ["alpha", "zebra"]);
        let workspace_records = session
            .identity_specs()
            .list(workspace_alpha.scope())
            .await
            .expect("list workspace specs");
        assert_eq!(spec_names(&workspace_records), ["alpha", "beta"]);
        let other_records = session
            .identity_specs()
            .list(other_gamma.scope())
            .await
            .expect("list other workspace specs");
        assert_eq!(spec_names(&other_records), ["gamma"]);
    }

    #[tokio::test]
    async fn rejects_corrupt_identity_spec_rows_on_read() {
        let (_temp, db) = open_sqlite().await;
        let blank_version = IdentitySpecKey::global("blank_version").expect("key");
        let reversed_timestamps = IdentitySpecKey::global("reversed_timestamps").expect("key");
        let malformed_id = IdentitySpecKey::global("malformed_id").expect("key");
        let mut tx = db.begin().await.expect("begin seed transaction");
        insert_spec(
            &mut tx,
            &blank_version,
            SpecSeed {
                version: " ",
                ..VALID_SPEC
            },
        )
        .await;
        insert_spec(
            &mut tx,
            &reversed_timestamps,
            SpecSeed {
                created_at_unix_nanos: 30,
                updated_at_unix_nanos: 29,
                ..VALID_SPEC
            },
        )
        .await;
        insert_spec(&mut tx, &malformed_id, VALID_SPEC).await;
        tx.execute(
            Query::update()
                .table(IdentitySpecs::Table)
                .value(IdentitySpecs::Id, "not-a-uuid")
                .and_where(Expr::col(IdentitySpecs::Name).eq(malformed_id.name()))
                .to_owned(),
        )
        .await
        .expect("corrupt identity spec id");
        tx.commit().await.expect("commit seed transaction");

        let mut session = &db;
        for (key, expected) in [
            (&blank_version, "empty required field"),
            (&reversed_timestamps, "invalid timestamps"),
            (&malformed_id, "invalid identity spec id"),
        ] {
            let error = session
                .identity_specs()
                .get(key)
                .await
                .expect_err("corrupt row must fail closed");
            assert!(
                matches!(&error, DbError::CorruptData(message) if message.contains(expected)),
                "unexpected error: {error}"
            );
        }
    }

    async fn open_sqlite() -> (tempfile::TempDir, CoralDb) {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        (temp, db)
    }

    async fn insert_spec(
        tx: &mut CoralTx<'_>,
        key: &IdentitySpecKey,
        spec: SpecSeed,
    ) -> IdentitySpecId {
        let id = IdentitySpecId::new();
        tx.execute(
            Query::insert()
                .into_table(IdentitySpecs::Table)
                .columns(identity_spec_columns())
                .values_panic([
                    Expr::val(id.as_str()),
                    Expr::val(key.scope.workspace_id().map(ToString::to_string)),
                    Expr::val(key.name.clone()),
                    Expr::val(spec.version),
                    Expr::val(spec.description),
                    Expr::val(spec.issuer),
                    Expr::val(spec.identity_type),
                    Expr::val(spec.manifest_yaml),
                    Expr::val(spec.created_at_unix_nanos),
                    Expr::val(spec.updated_at_unix_nanos),
                ])
                .to_owned(),
        )
        .await
        .expect("insert identity spec");
        id
    }

    fn expected_record(
        id: IdentitySpecId,
        key: IdentitySpecKey,
        spec: SpecSeed,
    ) -> IdentitySpecRecord {
        IdentitySpecRecord {
            id,
            key,
            version: spec.version.to_string(),
            description: spec.description.to_string(),
            issuer: spec.issuer.to_string(),
            identity_type: spec.identity_type.to_string(),
            manifest_yaml: spec.manifest_yaml.to_string(),
            created_at_unix_nanos: spec.created_at_unix_nanos,
            updated_at_unix_nanos: spec.updated_at_unix_nanos,
        }
    }

    fn spec_names(records: &[IdentitySpecRecord]) -> Vec<&str> {
        records.iter().map(|record| record.key.name()).collect()
    }
}
