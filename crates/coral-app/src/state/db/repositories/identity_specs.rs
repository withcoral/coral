use sea_query::{Expr, ExprTrait, OnConflict, Order, Query};

use crate::bootstrap::AppError;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
use crate::state::db::{CoralTx, DbError, DbSession};
use crate::workspaces::WorkspaceName;
use coral_spec::{IdentityManifest, parse_identity_manifest_yaml, validate_identity_spec_name};
use uuid::{Uuid, Variant, Version};

/// Opaque database identity for one persisted identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecId(String);

impl IdentitySpecId {
    pub(crate) fn new() -> Self {
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

    /// Canonical exact key fields authenticated by setup-document encryption.
    pub(crate) fn document_aad_parts(&self) -> (&str, &str, &str) {
        match &self.scope {
            IdentitySpecScope::Global => ("global", "__global__", self.name()),
            IdentitySpecScope::Workspace(workspace_name) => {
                ("workspace", workspace_name.as_str(), self.name())
            }
        }
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
    manifest_yaml: String,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentitySpecRow {
    fn validate(self) -> Result<IdentitySpecRecord, DbError> {
        validate_identity_spec_fields([&self.version, &self.issuer, &self.manifest_yaml])
            .map_err(DbError::CorruptData)?;
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
            manifest_yaml: self.manifest_yaml,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Opaque encrypted setup-input document persisted for one identity spec.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecDocumentRecord {
    /// Identity spec that owns the encrypted document.
    pub(crate) identity_spec_id: IdentitySpecId,
    /// Monotonic storage version incremented on each replacement.
    pub(crate) document_version: i64,
    /// Opaque encrypted setup-input envelope.
    pub(crate) envelope: EncryptedEnvelopeDocument,
    /// Creation timestamp in Unix nanoseconds.
    pub(crate) created_at_unix_nanos: i64,
    /// Last update timestamp in Unix nanoseconds.
    pub(crate) updated_at_unix_nanos: i64,
}

impl std::fmt::Debug for IdentitySpecDocumentRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdentitySpecDocumentRecord")
            .field("identity_spec_id", &self.identity_spec_id)
            .field("document_version", &self.document_version)
            .field("envelope", &self.envelope)
            .finish_non_exhaustive()
    }
}

#[derive(sqlx::FromRow)]
struct IdentitySpecDocumentRow {
    identity_spec_id: String,
    document_version: i64,
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    binding_version: i64,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentitySpecDocumentRow {
    fn validate(self) -> Result<IdentitySpecDocumentRecord, DbError> {
        if self.document_version < 1
            || self.created_at_unix_nanos < 0
            || self.updated_at_unix_nanos < self.created_at_unix_nanos
        {
            return Err(DbError::CorruptData(
                "identity spec document row has invalid version or timestamps".to_string(),
            ));
        }
        let envelope = EncryptedEnvelopeDocument::new(
            self.ciphertext,
            self.nonce,
            self.wrapped_dek,
            self.wrapped_dek_nonce,
            self.key_id,
            self.algorithm,
            self.binding_version,
        )
        .map_err(|error| DbError::CorruptData(error.to_string()))?;
        Ok(IdentitySpecDocumentRecord {
            identity_spec_id: IdentitySpecId::from_storage(self.identity_spec_id)?,
            document_version: self.document_version,
            envelope,
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

impl IdentitySpecsRepo<'_, CoralTx<'_>> {
    /// Insert or replace one exact-scope definition while preserving creation time.
    pub(crate) async fn upsert(
        &mut self,
        key: &IdentitySpecKey,
        manifest: &IdentityManifest,
        manifest_yaml: &str,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecRecord, AppError> {
        validate_identity_spec_write(key, manifest, manifest_yaml)?;
        validate_write_timestamp(now_unix_nanos)?;
        let current_updated_at =
            Expr::col((IdentitySpecs::Table, IdentitySpecs::UpdatedAtUnixNanos));
        let id = IdentitySpecId::new();
        let mut on_conflict = match key.scope() {
            IdentitySpecScope::Global => OnConflict::column(IdentitySpecs::Name),
            IdentitySpecScope::Workspace(_) => {
                OnConflict::columns([IdentitySpecs::WorkspaceId, IdentitySpecs::Name])
            }
        };
        match key.scope() {
            IdentitySpecScope::Global => {
                on_conflict.target_and_where(Expr::col(IdentitySpecs::WorkspaceId).is_null());
            }
            IdentitySpecScope::Workspace(_) => {
                on_conflict.target_and_where(Expr::col(IdentitySpecs::WorkspaceId).is_not_null());
            }
        }
        on_conflict
            .update_columns([
                IdentitySpecs::Version,
                IdentitySpecs::Description,
                IdentitySpecs::Issuer,
                IdentitySpecs::ManifestYaml,
            ])
            .value(
                IdentitySpecs::UpdatedAtUnixNanos,
                Expr::case(
                    current_updated_at.clone().gt(now_unix_nanos),
                    current_updated_at,
                )
                .finally(now_unix_nanos),
            );
        let statement = Query::insert()
            .into_table(IdentitySpecs::Table)
            .columns(identity_spec_columns())
            .values_panic([
                Expr::val(id.as_str()),
                Expr::val(key.scope.workspace_id().map(ToString::to_string)),
                Expr::val(key.name.clone()),
                Expr::val(manifest.version.clone()),
                Expr::val(manifest.description.clone()),
                Expr::val(manifest.issuer.clone()),
                Expr::val(manifest_yaml),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(on_conflict)
            .to_owned();
        let rows_affected = self.session.execute_rows_affected(statement).await?;
        if rows_affected != 1 {
            return Err(AppError::Database(format!(
                "identity spec upsert affected {rows_affected} rows"
            )));
        }
        self.get(key)
            .await?
            .ok_or_else(|| AppError::Database("identity spec disappeared after upsert".to_string()))
    }

    /// Delete one exact-scope definition.
    pub(crate) async fn delete(&mut self, key: &IdentitySpecKey) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_rows_affected(
                Query::delete()
                    .from_table(IdentitySpecs::Table)
                    .and_where(identity_spec_key_where(key))
                    .to_owned(),
            )
            .await?;
        zero_or_one_affected(rows_affected, "identity spec delete")
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

    /// Load one encrypted document by its owning identity spec id.
    pub(crate) async fn get(
        &mut self,
        identity_spec_id: &IdentitySpecId,
    ) -> Result<Option<IdentitySpecDocumentRecord>, DbError> {
        let row: Option<IdentitySpecDocumentRow> = self
            .session
            .fetch_optional(
                Query::select()
                    .columns(identity_spec_document_columns())
                    .from(IdentitySpecDocuments::Table)
                    .and_where(identity_spec_document_id_where(identity_spec_id))
                    .to_owned(),
            )
            .await?;
        row.map(IdentitySpecDocumentRow::validate).transpose()
    }
}

impl IdentitySpecDocumentsRepo<'_, CoralTx<'_>> {
    /// Insert or atomically replace an encrypted document and increment its version.
    pub(crate) async fn upsert(
        &mut self,
        identity_spec_id: &IdentitySpecId,
        document: &EncryptedEnvelopeDocument,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecDocumentRecord, AppError> {
        document
            .validate()
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        validate_write_timestamp(now_unix_nanos)?;
        let current_version = Expr::col((
            IdentitySpecDocuments::Table,
            IdentitySpecDocuments::DocumentVersion,
        ));
        let current_updated_at = Expr::col((
            IdentitySpecDocuments::Table,
            IdentitySpecDocuments::UpdatedAtUnixNanos,
        ));
        let statement = Query::insert()
            .into_table(IdentitySpecDocuments::Table)
            .columns(identity_spec_document_columns())
            .values_panic([
                Expr::val(identity_spec_id.as_str()),
                Expr::val(1),
                Expr::val(document.ciphertext.clone()),
                Expr::val(document.nonce.clone()),
                Expr::val(document.wrapped_dek.clone()),
                Expr::val(document.wrapped_dek_nonce.clone()),
                Expr::val(document.key_id.clone()),
                Expr::val(document.algorithm.clone()),
                Expr::val(document.binding_version),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::column(IdentitySpecDocuments::IdentitySpecId)
                    .value(
                        IdentitySpecDocuments::DocumentVersion,
                        current_version.clone().add(1),
                    )
                    .update_columns([
                        IdentitySpecDocuments::Ciphertext,
                        IdentitySpecDocuments::Nonce,
                        IdentitySpecDocuments::WrappedDek,
                        IdentitySpecDocuments::WrappedDekNonce,
                        IdentitySpecDocuments::KeyId,
                        IdentitySpecDocuments::Algorithm,
                        IdentitySpecDocuments::BindingVersion,
                    ])
                    .value(
                        IdentitySpecDocuments::UpdatedAtUnixNanos,
                        Expr::case(
                            current_updated_at.clone().gt(now_unix_nanos),
                            current_updated_at,
                        )
                        .finally(now_unix_nanos),
                    )
                    .action_and_where(current_version.lt(i64::MAX))
                    .to_owned(),
            )
            .to_owned();
        match self.session.execute_rows_affected(statement).await? {
            1 => {}
            0 => {
                return Err(AppError::FailedPrecondition(format!(
                    "identity spec document version is exhausted for {}",
                    identity_spec_id.as_str()
                )));
            }
            rows_affected => {
                return Err(AppError::Database(format!(
                    "identity spec document upsert affected {rows_affected} rows"
                )));
            }
        }
        self.get(identity_spec_id).await?.ok_or_else(|| {
            AppError::Database("identity spec document disappeared after upsert".to_string())
        })
    }

    /// Delete one encrypted document without deleting its definition.
    pub(crate) async fn delete(
        &mut self,
        identity_spec_id: &IdentitySpecId,
    ) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_rows_affected(
                Query::delete()
                    .from_table(IdentitySpecDocuments::Table)
                    .and_where(identity_spec_document_id_where(identity_spec_id))
                    .to_owned(),
            )
            .await?;
        zero_or_one_affected(rows_affected, "identity spec document delete")
    }
}

fn parse_identity_spec_name(name: &str) -> Result<String, AppError> {
    validate_identity_spec_name(name).map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(name.to_string())
}

fn validate_identity_spec_fields(fields: [&str; 3]) -> Result<(), String> {
    if fields.into_iter().any(|value| value.trim().is_empty()) {
        return Err("identity spec has an empty required field".to_string());
    }
    Ok(())
}

fn validate_identity_spec_write(
    key: &IdentitySpecKey,
    manifest: &IdentityManifest,
    manifest_yaml: &str,
) -> Result<(), AppError> {
    if key.name() != manifest.name {
        return Err(AppError::InvalidInput(format!(
            "identity spec key name '{}' does not match manifest name '{}'",
            key.name(),
            manifest.name
        )));
    }
    validate_identity_spec_fields([&manifest.version, &manifest.issuer, manifest_yaml])
        .map_err(AppError::InvalidInput)?;
    let parsed_manifest = parse_identity_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    if &parsed_manifest != manifest {
        return Err(AppError::InvalidInput(
            "identity spec manifest YAML does not match the validated manifest".to_string(),
        ));
    }
    Ok(())
}

fn validate_write_timestamp(now_unix_nanos: i64) -> Result<(), AppError> {
    match now_unix_nanos {
        0.. => Ok(()),
        _ => Err(AppError::InvalidInput(
            "identity spec timestamp is negative".to_string(),
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

fn identity_spec_columns() -> [IdentitySpecs; 9] {
    [
        IdentitySpecs::Id,
        IdentitySpecs::WorkspaceId,
        IdentitySpecs::Name,
        IdentitySpecs::Version,
        IdentitySpecs::Description,
        IdentitySpecs::Issuer,
        IdentitySpecs::ManifestYaml,
        IdentitySpecs::CreatedAtUnixNanos,
        IdentitySpecs::UpdatedAtUnixNanos,
    ]
}

fn identity_spec_document_columns() -> [IdentitySpecDocuments; 11] {
    [
        IdentitySpecDocuments::IdentitySpecId,
        IdentitySpecDocuments::DocumentVersion,
        IdentitySpecDocuments::Ciphertext,
        IdentitySpecDocuments::Nonce,
        IdentitySpecDocuments::WrappedDek,
        IdentitySpecDocuments::WrappedDekNonce,
        IdentitySpecDocuments::KeyId,
        IdentitySpecDocuments::Algorithm,
        IdentitySpecDocuments::BindingVersion,
        IdentitySpecDocuments::CreatedAtUnixNanos,
        IdentitySpecDocuments::UpdatedAtUnixNanos,
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

fn identity_spec_document_id_where(identity_spec_id: &IdentitySpecId) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecDocuments::IdentitySpecId).eq(identity_spec_id.as_str())
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{
        IdentitySpecId, IdentitySpecKey, IdentitySpecRecord, identity_spec_columns,
        validate_identity_spec_write,
    };
    use crate::bootstrap::{self, AppError};
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::state::db::schema::IdentitySpecs;
    use crate::state::db::{CoralDb, CoralTx, DbError, DbRepos, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;
    use coral_spec::{IdentityManifest, parse_identity_manifest_yaml};

    #[derive(Clone, Copy)]
    struct SpecSeed {
        version: &'static str,
        description: &'static str,
        issuer: &'static str,
        manifest_yaml: &'static str,
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    }

    const VALID_SPEC: SpecSeed = SpecSeed {
        version: "1.0.0",
        description: "",
        issuer: "github",
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

    #[test]
    fn identity_spec_write_inputs_validate_repository_invariants() {
        let key = IdentitySpecKey::global("github").expect("key");
        let (other_manifest, other_yaml) = valid_manifest("other", "1.0.0");
        assert!(matches!(
            validate_identity_spec_write(&key, &other_manifest, &other_yaml),
            Err(AppError::InvalidInput(_))
        ));

        let (mut manifest, manifest_yaml) = valid_manifest(key.name(), "1.0.0");
        manifest.version.clear();
        assert!(matches!(
            validate_identity_spec_write(&key, &manifest, &manifest_yaml),
            Err(AppError::InvalidInput(_))
        ));

        let blank_description_yaml = format!(
            "kind: identity\nspec_version: 1\nname: {}\nversion: 1.0.0\ndescription: ''\nissuer: github\ntype: fixed_token\naudience:\n  host: api.github.com\n",
            key.name()
        );
        let blank_description_manifest = parse_identity_manifest_yaml(&blank_description_yaml)
            .expect("valid manifest with blank description");
        validate_identity_spec_write(&key, &blank_description_manifest, &blank_description_yaml)
            .expect("blank descriptions are valid");
        assert!(matches!(
            validate_identity_spec_write(&key, &blank_description_manifest, "\n"),
            Err(AppError::InvalidInput(_))
        ));

        let (manifest, _) = valid_manifest(key.name(), "1.0.0");
        let (_, mismatched_yaml) = valid_manifest(key.name(), "2.0.0");
        assert!(matches!(
            validate_identity_spec_write(&key, &manifest, &mismatched_yaml),
            Err(AppError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_identity_spec_write(&key, &manifest, "not: [valid"),
            Err(AppError::InvalidInput(_))
        ));
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

    #[tokio::test]
    async fn upserts_preserve_creation_and_monotonic_update_time() {
        let (_temp, db) = open_sqlite().await;
        assert_upserts_preserve_creation_and_monotonic_update_time(&db, "sqlite").await;
    }

    #[tokio::test]
    async fn mutations_are_exact_and_transactional_against_sqlite() {
        let (_temp, db) = open_sqlite().await;
        assert_mutations_are_exact_and_transactional(&db, "sqlite").await;
    }

    /// Runs the identity-spec and document contracts against a live Postgres backend.
    ///
    /// Spec `upsert` selects its arbiter index through `ON CONFLICT ... WHERE`, which
    /// each backend resolves against its own partial-index inference rules, and
    /// document envelopes round-trip through a `BYTEA` column that `SQLite` stores
    /// under a different type affinity, so the tests above cannot stand in for this
    /// coverage. CI selects this test by the shared
    /// `contract_on_postgres` name filter.
    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn identity_spec_repository_contract_on_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_upserts_preserve_creation_and_monotonic_update_time(&db, &unique_suffix()).await;
        assert_mutations_are_exact_and_transactional(&db, &unique_suffix()).await;
        assert_encrypted_documents_round_trip(&db, &unique_suffix()).await;
    }

    async fn assert_upserts_preserve_creation_and_monotonic_update_time(
        db: &CoralDb,
        suffix: &str,
    ) {
        let global = IdentitySpecKey::global(&format!("github_{suffix}")).expect("global key");
        let mut tx = db.begin().await.expect("begin mutation transaction");
        let (manifest, manifest_yaml) = valid_manifest(global.name(), "1.0.0");
        let inserted = tx
            .identity_specs()
            .upsert(&global, &manifest, &manifest_yaml, 10)
            .await
            .expect("insert global spec");
        assert_eq!(
            (
                inserted.version.as_str(),
                inserted.created_at_unix_nanos,
                inserted.updated_at_unix_nanos,
            ),
            ("1.0.0", 10, 10)
        );
        assert_eq!(inserted.manifest_yaml, manifest_yaml);
        upsert_spec(&mut tx, &global, "2.0.0", 30)
            .await
            .expect("replace global spec");
        let stale_clock_update = upsert_spec(&mut tx, &global, "3.0.0", 20)
            .await
            .expect("replace without timestamp regression");
        assert_eq!(
            stale_clock_update.id, inserted.id,
            "upserts must preserve the internal identity spec id"
        );
        assert_eq!(
            (
                stale_clock_update.version.as_str(),
                stale_clock_update.created_at_unix_nanos,
                stale_clock_update.updated_at_unix_nanos,
            ),
            ("3.0.0", 10, 30)
        );
        tx.commit().await.expect("commit mutation transaction");

        let mut session = db;
        let persisted = session
            .identity_specs()
            .get(&global)
            .await
            .expect("read global")
            .expect("global persists");
        assert_eq!(persisted, stale_clock_update);
    }

    async fn assert_mutations_are_exact_and_transactional(db: &CoralDb, suffix: &str) {
        let (global_record, workspace_record) = seed_scoped_specs(db, suffix).await;
        let global = global_record.key;
        let workspace_key = workspace_record.key;
        let negative_timestamp =
            IdentitySpecKey::global(&format!("negative_timestamp_{suffix}")).expect("key");
        let mut tx = db.begin().await.expect("begin validation transaction");
        assert!(matches!(
            upsert_spec(&mut tx, &negative_timestamp, "1.0.0", -1).await,
            Err(AppError::InvalidInput(_))
        ));
        tx.commit().await.expect("commit validation transaction");

        let rolled_back = IdentitySpecKey::global(&format!("rolled_back_{suffix}")).expect("key");
        let mut tx = db.begin().await.expect("begin rollback transaction");
        upsert_spec(&mut tx, &rolled_back, "1.0.0", 40)
            .await
            .expect("insert rolled-back spec");
        assert!(
            tx.identity_specs()
                .delete(&global)
                .await
                .expect("delete global in rollback")
        );
        tx.rollback().await.expect("rollback mutation transaction");

        let missing_workspace =
            WorkspaceName::parse(&format!("missing_team_{suffix}")).expect("workspace");
        let missing_workspace_key =
            IdentitySpecKey::workspace(missing_workspace, global.name()).expect("key");
        let mut tx = db.begin().await.expect("begin foreign-key transaction");
        assert!(matches!(
            upsert_spec(&mut tx, &missing_workspace_key, "1.0.0", 50).await,
            Err(AppError::Database(_))
        ));
        tx.rollback().await.expect("rollback failed upsert");

        let mut tx = db.begin().await.expect("begin delete transaction");
        assert!(
            tx.identity_specs()
                .delete(&workspace_key)
                .await
                .expect("delete workspace spec")
        );
        assert!(
            !tx.identity_specs()
                .delete(&workspace_key)
                .await
                .expect("repeat workspace delete")
        );
        tx.commit().await.expect("commit exact delete");

        let mut session = db;
        let persisted_global = session
            .identity_specs()
            .get(&global)
            .await
            .expect("read global after rollback")
            .expect("global survives exact workspace delete");
        assert_eq!(persisted_global.version, "global");
        for missing in [&negative_timestamp, &rolled_back, &workspace_key] {
            assert!(
                session
                    .identity_specs()
                    .get(missing)
                    .await
                    .expect("read missing exact key")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn encrypted_documents_round_trip_by_exact_key() {
        let (_temp, db) = open_sqlite().await;
        assert_encrypted_documents_round_trip(&db, "sqlite").await;
    }

    async fn assert_encrypted_documents_round_trip(db: &CoralDb, suffix: &str) {
        let (global, workspace) = seed_scoped_specs(db, suffix).await;
        let mut tx = db.begin().await.expect("begin document transaction");
        let mut invalid = valid_document(1, 1);
        invalid.binding_version = 0;
        assert!(matches!(
            tx.identity_spec_documents()
                .upsert(&global.id, &invalid, 50)
                .await,
            Err(AppError::InvalidInput(_))
        ));
        let first = tx
            .identity_spec_documents()
            .upsert(&global.id, &valid_document(1, 1), 50)
            .await
            .expect("insert document");
        let replaced = tx
            .identity_spec_documents()
            .upsert(&global.id, &valid_document(10, 99), 40)
            .await
            .expect("replace document with stale clock");
        tx.identity_spec_documents()
            .upsert(&workspace.id, &valid_document(20, 2), 60)
            .await
            .expect("insert exact workspace document");
        assert_eq!((first.document_version, replaced.document_version), (1, 2));
        assert_eq!(
            (
                replaced.created_at_unix_nanos,
                replaced.updated_at_unix_nanos
            ),
            (50, 50)
        );
        assert_eq!(replaced.envelope.ciphertext, [10]);
        assert_eq!(replaced.envelope.nonce, [11]);
        assert_eq!(replaced.envelope.wrapped_dek, [12]);
        assert_eq!(replaced.envelope.wrapped_dek_nonce, [13]);
        assert_eq!(replaced.envelope.key_id, "key-10");
        assert_eq!(replaced.envelope.algorithm, "alg-10");
        assert_eq!(replaced.envelope.binding_version, 99);
        assert!(!format!("{replaced:?}").contains("key-10"));
        assert!(
            tx.identity_spec_documents()
                .delete(&global.id)
                .await
                .expect("delete")
        );
        assert!(
            !tx.identity_spec_documents()
                .delete(&global.id)
                .await
                .expect("repeat delete")
        );
        tx.commit().await.expect("commit documents");

        let mut session = db;
        assert!(
            session
                .identity_spec_documents()
                .get(&global.id)
                .await
                .expect("read")
                .is_none()
        );
        assert_eq!(
            session
                .identity_spec_documents()
                .get(&workspace.id)
                .await
                .expect("read workspace")
                .map(|record| record.identity_spec_id),
            Some(workspace.id.clone())
        );
        assert!(
            session
                .identity_specs()
                .get(&global.key)
                .await
                .expect("read spec")
                .is_some()
        );
    }

    async fn seed_scoped_specs(
        db: &CoralDb,
        suffix: &str,
    ) -> (IdentitySpecRecord, IdentitySpecRecord) {
        let workspace = WorkspaceName::parse(&format!("team_{suffix}")).expect("workspace");
        let spec_name = format!("github_{suffix}");
        let global = IdentitySpecKey::global(&spec_name).expect("global key");
        let workspace_key =
            IdentitySpecKey::workspace(workspace.clone(), &spec_name).expect("workspace key");
        let mut tx = db.begin().await.expect("begin seed transaction");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("create workspace");
        let global_record = upsert_spec(&mut tx, &global, "global", 10)
            .await
            .expect("insert global spec");
        let workspace_record = upsert_spec(&mut tx, &workspace_key, "workspace", 12)
            .await
            .expect("insert workspace spec");
        tx.commit().await.expect("commit seed transaction");
        (global_record, workspace_record)
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

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }

    /// Keeps every Postgres run isolated inside CI's single shared database.
    fn unique_suffix() -> String {
        Uuid::new_v4().simple().to_string()
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
            manifest_yaml: spec.manifest_yaml.to_string(),
            created_at_unix_nanos: spec.created_at_unix_nanos,
            updated_at_unix_nanos: spec.updated_at_unix_nanos,
        }
    }

    fn spec_names(records: &[IdentitySpecRecord]) -> Vec<&str> {
        records.iter().map(|record| record.key.name()).collect()
    }

    async fn upsert_spec(
        tx: &mut CoralTx<'_>,
        key: &IdentitySpecKey,
        version: &str,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecRecord, AppError> {
        let (manifest, manifest_yaml) = valid_manifest(key.name(), version);
        tx.identity_specs()
            .upsert(key, &manifest, &manifest_yaml, now_unix_nanos)
            .await
    }

    fn valid_manifest(name: &str, version: &str) -> (IdentityManifest, String) {
        let manifest_yaml = format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {version}\ndescription: Test identity spec\nissuer: github\ntype: fixed_token\naudience:\n  host: api.github.com\n"
        );
        let manifest =
            parse_identity_manifest_yaml(&manifest_yaml).expect("valid identity manifest");
        (manifest, manifest_yaml)
    }

    fn valid_document(marker: u8, binding_version: i64) -> EncryptedEnvelopeDocument {
        EncryptedEnvelopeDocument::new(
            vec![marker],
            vec![marker + 1],
            vec![marker + 2],
            vec![marker + 3],
            format!("key-{marker}"),
            format!("alg-{marker}"),
            binding_version,
        )
        .expect("valid identity spec document write")
    }
}
