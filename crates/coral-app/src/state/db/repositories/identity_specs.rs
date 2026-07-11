#![cfg_attr(not(test), expect(dead_code, reason = "B2 wires consumers"))]

use std::collections::BTreeMap;

use sea_query::{Expr, ExprTrait, OnConflict, Order, Query};

use crate::bootstrap::AppError;
use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
use crate::state::db::{CoralTx, DbError, DbSession};
use crate::workspaces::WorkspaceName;
use coral_spec::validate_identity_spec_name;

const GLOBAL_SCOPE_KIND: &str = "global";
const GLOBAL_SCOPE_ID: &str = "__global__";
const WORKSPACE_SCOPE_KIND: &str = "workspace";

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

    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::Global => GLOBAL_SCOPE_KIND,
            Self::Workspace(_workspace_name) => WORKSPACE_SCOPE_KIND,
        }
    }

    pub(super) fn scope_id(&self) -> &str {
        match self {
            Self::Global => GLOBAL_SCOPE_ID,
            Self::Workspace(workspace_name) => workspace_name.as_str(),
        }
    }

    pub(super) fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Workspace(workspace_name) => Some(workspace_name.as_str()),
        }
    }
}

/// Portable primary key for one global or workspace-scoped identity spec.
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
        scope_kind: &str,
        scope_id: &str,
        workspace_id: Option<&str>,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match (scope_kind, workspace_id) {
            (GLOBAL_SCOPE_KIND, None) if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            (GLOBAL_SCOPE_KIND, _) => {
                return Err(DbError::CorruptData(
                    "global identity spec row has invalid scope columns".to_string(),
                ));
            }
            (WORKSPACE_SCOPE_KIND, Some(workspace_id)) if scope_id == workspace_id => {
                IdentitySpecScope::Workspace(parse_workspace_name(workspace_id)?)
            }
            (WORKSPACE_SCOPE_KIND, _) => {
                return Err(DbError::CorruptData(
                    "workspace identity spec row has invalid scope columns".to_string(),
                ));
            }
            (other, _) => {
                return Err(DbError::CorruptData(format!(
                    "identity spec row has invalid scope kind '{other}'"
                )));
            }
        };
        Ok(Self {
            scope,
            name: parse_persisted_identity_spec_name(name)?,
        })
    }

    pub(super) fn from_document_storage_parts(
        scope_kind: &str,
        scope_id: &str,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match scope_kind {
            GLOBAL_SCOPE_KIND if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            GLOBAL_SCOPE_KIND => {
                return Err(DbError::CorruptData(
                    "global identity spec document row has invalid scope columns".to_string(),
                ));
            }
            WORKSPACE_SCOPE_KIND => IdentitySpecScope::Workspace(parse_workspace_name(scope_id)?),
            other => {
                return Err(DbError::CorruptData(format!(
                    "identity spec document row has invalid scope kind '{other}'"
                )));
            }
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

/// Validated authored fields used to insert or replace an identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecWrite {
    version: String,
    description: String,
    issuer: String,
    identity_type: String,
    manifest_yaml: String,
}

impl IdentitySpecWrite {
    /// Validate authored fields before they can reach the database repository.
    pub(crate) fn new(
        version: impl Into<String>,
        description: impl Into<String>,
        issuer: impl Into<String>,
        identity_type: impl Into<String>,
        manifest_yaml: impl Into<String>,
    ) -> Result<Self, AppError> {
        let write = Self {
            version: version.into(),
            description: description.into(),
            issuer: issuer.into(),
            identity_type: identity_type.into(),
            manifest_yaml: manifest_yaml.into(),
        };
        validate_identity_spec_fields([
            &write.version,
            &write.issuer,
            &write.identity_type,
            &write.manifest_yaml,
        ])
        .map_err(AppError::InvalidInput)?;
        Ok(write)
    }
}

#[derive(Debug, sqlx::FromRow)]
struct IdentitySpecRow {
    scope_kind: String,
    scope_id: String,
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
        validate_identity_spec_fields([
            &self.version,
            &self.issuer,
            &self.identity_type,
            &self.manifest_yaml,
        ])
        .map_err(DbError::CorruptData)?;
        if self.created_at_unix_nanos < 0 || self.updated_at_unix_nanos < self.created_at_unix_nanos
        {
            return Err(DbError::CorruptData(
                "identity spec row has invalid timestamps".to_string(),
            ));
        }
        Ok(IdentitySpecRecord {
            key: IdentitySpecKey::from_spec_storage_parts(
                &self.scope_kind,
                &self.scope_id,
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

/// Opaque encrypted setup-input document persisted for one identity spec.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecDocumentRecord {
    /// Identity spec that owns the encrypted document.
    pub(crate) key: IdentitySpecKey,
    /// Monotonic storage version incremented on each replacement.
    pub(crate) document_version: i64,
    /// Opaque encrypted setup-input bytes.
    pub(crate) ciphertext: Vec<u8>,
    /// Nonce paired with the encrypted setup-input bytes.
    pub(crate) nonce: Vec<u8>,
    /// Opaque wrapped data-encryption-key bytes.
    pub(crate) wrapped_dek: Vec<u8>,
    /// Nonce paired with the wrapped data-encryption key.
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    /// Identifier of the key-encryption key used for the envelope.
    pub(crate) key_id: String,
    /// Authored envelope algorithm identifier.
    pub(crate) algorithm: String,
    /// Authored AAD encoding version, interpreted by the crypto layer.
    pub(crate) aad_version: i64,
    /// Creation timestamp in Unix nanoseconds.
    pub(crate) created_at_unix_nanos: i64,
    /// Last update timestamp in Unix nanoseconds.
    pub(crate) updated_at_unix_nanos: i64,
}

/// Validated opaque envelope fields used to insert or replace a document.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecDocumentWrite {
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    aad_version: i64,
}

impl IdentitySpecDocumentWrite {
    /// Validate opaque envelope shape without interpreting crypto policy.
    pub(crate) fn new(
        ciphertext: Vec<u8>,
        nonce: Vec<u8>,
        wrapped_dek: Vec<u8>,
        wrapped_dek_nonce: Vec<u8>,
        key_id: impl Into<String>,
        algorithm: impl Into<String>,
        aad_version: i64,
    ) -> Result<Self, AppError> {
        let write = Self {
            ciphertext,
            nonce,
            wrapped_dek,
            wrapped_dek_nonce,
            key_id: key_id.into(),
            algorithm: algorithm.into(),
            aad_version,
        };
        validate_identity_spec_document_fields(
            &write.ciphertext,
            &write.nonce,
            &write.wrapped_dek,
            &write.wrapped_dek_nonce,
            &write.key_id,
            &write.algorithm,
            write.aad_version,
        )
        .map_err(AppError::InvalidInput)?;
        Ok(write)
    }
}

impl std::fmt::Debug for IdentitySpecDocumentRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentitySpecDocumentRecord")
            .field("key", &self.key)
            .field("document_version", &self.document_version)
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for IdentitySpecDocumentWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentitySpecDocumentWrite")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

#[derive(sqlx::FromRow)]
struct IdentitySpecDocumentRow {
    scope_kind: String,
    scope_id: String,
    name: String,
    document_version: i64,
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    aad_version: i64,
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
        validate_identity_spec_document_fields(
            &self.ciphertext,
            &self.nonce,
            &self.wrapped_dek,
            &self.wrapped_dek_nonce,
            &self.key_id,
            &self.algorithm,
            self.aad_version,
        )
        .map_err(DbError::CorruptData)?;
        Ok(IdentitySpecDocumentRecord {
            key: IdentitySpecKey::from_document_storage_parts(
                &self.scope_kind,
                &self.scope_id,
                &self.name,
            )?,
            document_version: self.document_version,
            ciphertext: self.ciphertext,
            nonce: self.nonce,
            wrapped_dek: self.wrapped_dek,
            wrapped_dek_nonce: self.wrapped_dek_nonce,
            key_id: self.key_id,
            algorithm: self.algorithm,
            aad_version: self.aad_version,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository shell for durable DSL v4 identity spec definitions.
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

    /// Load one identity spec by exact scope and name without fallback.
    pub(crate) async fn load_optional(
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

    /// List globally installed identity specs in name order.
    pub(crate) async fn list_global(&mut self) -> Result<Vec<IdentitySpecRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Global).await
    }

    /// List identity specs scoped to one workspace in name order.
    pub(crate) async fn list_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Workspace(workspace_name.clone()))
            .await
    }

    /// Resolve one spec, preferring a workspace definition over its global fallback.
    pub(crate) async fn resolve_optional(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecRecord>, DbError> {
        if let Some(record) = self.load_optional(key).await? {
            return Ok(Some(record));
        }
        if matches!(&key.scope, IdentitySpecScope::Global) {
            return Ok(None);
        }
        self.load_optional(&IdentitySpecKey {
            scope: IdentitySpecScope::Global,
            name: key.name.clone(),
        })
        .await
    }

    /// List the effective specs for a workspace with one record per name.
    pub(crate) async fn list_resolved_for_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        let mut by_name = BTreeMap::new();
        for record in self.list_global().await? {
            by_name.insert(record.key.name.clone(), record);
        }
        for record in self.list_workspace(workspace_name).await? {
            by_name.insert(record.key.name.clone(), record);
        }
        Ok(by_name.into_values().collect())
    }

    async fn list_scope(
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
        spec: &IdentitySpecWrite,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecRecord, AppError> {
        validate_write_timestamp(now_unix_nanos)?;
        let current_updated_at =
            Expr::col((IdentitySpecs::Table, IdentitySpecs::UpdatedAtUnixNanos));
        let statement = Query::insert()
            .into_table(IdentitySpecs::Table)
            .columns(identity_spec_columns())
            .values_panic([
                Expr::val(key.scope.kind()),
                Expr::val(key.scope.scope_id()),
                Expr::val(key.scope.workspace_id().map(ToString::to_string)),
                Expr::val(key.name.clone()),
                Expr::val(spec.version.clone()),
                Expr::val(spec.description.clone()),
                Expr::val(spec.issuer.clone()),
                Expr::val(spec.identity_type.clone()),
                Expr::val(spec.manifest_yaml.clone()),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    IdentitySpecs::ScopeKind,
                    IdentitySpecs::ScopeId,
                    IdentitySpecs::Name,
                ])
                .update_columns([
                    IdentitySpecs::Version,
                    IdentitySpecs::Description,
                    IdentitySpecs::Issuer,
                    IdentitySpecs::IdentityType,
                    IdentitySpecs::ManifestYaml,
                ])
                .value(
                    IdentitySpecs::UpdatedAtUnixNanos,
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
                "identity spec upsert affected {rows_affected} rows"
            )));
        }
        self.load_optional(key)
            .await?
            .ok_or_else(|| AppError::Database("identity spec disappeared after upsert".to_string()))
    }

    /// Delete one exact-scope definition and cascade its encrypted document.
    pub(crate) async fn delete(&mut self, key: &IdentitySpecKey) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_affected(
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

    /// Load one encrypted document by exact scope and name without fallback.
    pub(crate) async fn load_optional(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecDocumentRecord>, DbError> {
        let row: Option<IdentitySpecDocumentRow> = self
            .session
            .fetch_optional(
                Query::select()
                    .columns(identity_spec_document_columns())
                    .from(IdentitySpecDocuments::Table)
                    .and_where(identity_spec_document_key_where(key))
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
        key: &IdentitySpecKey,
        document: &IdentitySpecDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecDocumentRecord, AppError> {
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
                Expr::val(key.scope.kind()),
                Expr::val(key.scope.scope_id()),
                Expr::val(key.name.clone()),
                Expr::val(1),
                Expr::val(document.ciphertext.clone()),
                Expr::val(document.nonce.clone()),
                Expr::val(document.wrapped_dek.clone()),
                Expr::val(document.wrapped_dek_nonce.clone()),
                Expr::val(document.key_id.clone()),
                Expr::val(document.algorithm.clone()),
                Expr::val(document.aad_version),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    IdentitySpecDocuments::ScopeKind,
                    IdentitySpecDocuments::ScopeId,
                    IdentitySpecDocuments::Name,
                ])
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
                    IdentitySpecDocuments::AadVersion,
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
        match self.session.execute_affected(statement).await? {
            1 => {}
            0 => {
                return Err(AppError::FailedPrecondition(format!(
                    "identity spec document version is exhausted for {}:{}",
                    key.scope.scope_id(),
                    key.name
                )));
            }
            rows_affected => {
                return Err(AppError::Database(format!(
                    "identity spec document upsert affected {rows_affected} rows"
                )));
            }
        }
        self.load_optional(key).await?.ok_or_else(|| {
            AppError::Database("identity spec document disappeared after upsert".to_string())
        })
    }

    /// Delete one exact-scope encrypted document without deleting its definition.
    pub(crate) async fn delete(&mut self, key: &IdentitySpecKey) -> Result<bool, DbError> {
        let rows_affected = self
            .session
            .execute_affected(
                Query::delete()
                    .from_table(IdentitySpecDocuments::Table)
                    .and_where(identity_spec_document_key_where(key))
                    .to_owned(),
            )
            .await?;
        zero_or_one_affected(rows_affected, "identity spec document delete")
    }
}

fn validate_identity_spec_fields(fields: [&str; 4]) -> Result<(), String> {
    if fields.into_iter().any(|value| value.trim().is_empty()) {
        return Err("identity spec has an empty required field".to_string());
    }
    Ok(())
}

fn validate_identity_spec_document_fields(
    ciphertext: &[u8],
    nonce: &[u8],
    wrapped_dek: &[u8],
    wrapped_dek_nonce: &[u8],
    key_id: &str,
    algorithm: &str,
    aad_version: i64,
) -> Result<(), String> {
    if ciphertext.is_empty()
        || nonce.is_empty()
        || wrapped_dek.is_empty()
        || wrapped_dek_nonce.is_empty()
    {
        return Err("identity spec document has an empty encrypted byte field".to_string());
    }
    if key_id.trim().is_empty() || algorithm.trim().is_empty() || aad_version < 1 {
        return Err("identity spec document has invalid envelope metadata".to_string());
    }
    Ok(())
}

fn validate_write_timestamp(now_unix_nanos: i64) -> Result<(), AppError> {
    match now_unix_nanos {
        0.. => Ok(()),
        _ => Err(AppError::InvalidInput(
            "identity spec timestamp is negative".into(),
        )),
    }
}

fn zero_or_one_affected(rows_affected: u64, op: &str) -> Result<bool, DbError> {
    match rows_affected {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DbError::CorruptData(format!(
            "{op} affected {rows_affected} rows"
        ))),
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

fn identity_spec_columns() -> [IdentitySpecs; 11] {
    [
        IdentitySpecs::ScopeKind,
        IdentitySpecs::ScopeId,
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

fn identity_spec_document_columns() -> [IdentitySpecDocuments; 13] {
    [
        IdentitySpecDocuments::ScopeKind,
        IdentitySpecDocuments::ScopeId,
        IdentitySpecDocuments::Name,
        IdentitySpecDocuments::DocumentVersion,
        IdentitySpecDocuments::Ciphertext,
        IdentitySpecDocuments::Nonce,
        IdentitySpecDocuments::WrappedDek,
        IdentitySpecDocuments::WrappedDekNonce,
        IdentitySpecDocuments::KeyId,
        IdentitySpecDocuments::Algorithm,
        IdentitySpecDocuments::AadVersion,
        IdentitySpecDocuments::CreatedAtUnixNanos,
        IdentitySpecDocuments::UpdatedAtUnixNanos,
    ]
}

fn identity_spec_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    identity_spec_scope_where(&key.scope).and(Expr::col(IdentitySpecs::Name).eq(key.name.as_str()))
}

fn identity_spec_scope_where(scope: &IdentitySpecScope) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecs::ScopeKind)
        .eq(scope.kind())
        .and(Expr::col(IdentitySpecs::ScopeId).eq(scope.scope_id()))
}

fn identity_spec_document_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecDocuments::ScopeKind)
        .eq(key.scope.kind())
        .and(Expr::col(IdentitySpecDocuments::ScopeId).eq(key.scope.scope_id()))
        .and(Expr::col(IdentitySpecDocuments::Name).eq(key.name.as_str()))
}

#[cfg(test)]
pub(in crate::state::db) mod tests {
    use sea_query::{Expr, Query};
    use tempfile::tempdir;

    use super::{
        IdentitySpecDocumentWrite, IdentitySpecKey, identity_spec_columns, identity_spec_key_where,
    };
    use crate::bootstrap::AppError;
    use crate::state::db::schema::IdentitySpecs;
    use crate::state::db::{CoralDb, DbError, DbRepos, DbSession, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

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
            IdentitySpecKey::from_spec_storage_parts("global", "__global__", None, " github"),
            IdentitySpecKey::from_spec_storage_parts(
                "workspace",
                " default",
                Some(" default"),
                "github",
            ),
            IdentitySpecKey::from_document_storage_parts("global", "__global__", "github "),
            IdentitySpecKey::from_document_storage_parts("workspace", " default", "github"),
            IdentitySpecKey::from_spec_storage_parts("global", "__global__", None, "github-oauth"),
        ] {
            assert!(matches!(result, Err(DbError::CorruptData(_))));
        }
    }

    #[tokio::test]
    async fn identity_spec_reads_resolve_scopes_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        assert_identity_spec_read_contract(&db).await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "shared backend repository contract fixture"
    )]
    pub(in crate::state::db) async fn assert_identity_spec_read_contract(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace = WorkspaceName::parse(&format!("identity{suffix}")).expect("workspace");
        let other_workspace =
            WorkspaceName::parse(&format!("identity_other_{suffix}")).expect("other workspace");
        let shadow_name = format!("github_{suffix}");
        let fallback_name = format!("stripe_{suffix}");
        let global_shadow = IdentitySpecKey::global(&shadow_name).expect("global shadow key");
        let global_fallback = IdentitySpecKey::global(&fallback_name).expect("global fallback key");
        let workspace_shadow =
            IdentitySpecKey::workspace(workspace.clone(), &shadow_name).expect("workspace key");
        let workspace_fallback =
            IdentitySpecKey::workspace(workspace.clone(), &fallback_name).expect("fallback key");
        let other_workspace_shadow =
            IdentitySpecKey::workspace(other_workspace.clone(), &shadow_name)
                .expect("other workspace shadow key");
        let other_workspace_fallback =
            IdentitySpecKey::workspace(other_workspace.clone(), &fallback_name)
                .expect("other workspace fallback key");

        let mut tx = db.begin().await.expect("begin seed tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.workspaces()
            .ensure(other_workspace.as_str(), 2)
            .await
            .expect("ensure other workspace");
        insert_spec(&mut tx, &global_shadow, "global-shadow", 2).await;
        insert_spec(&mut tx, &global_fallback, "global-fallback", 3).await;
        insert_spec(&mut tx, &workspace_shadow, "workspace-shadow", 4).await;
        insert_spec(
            &mut tx,
            &other_workspace_shadow,
            "other-workspace-shadow",
            5,
        )
        .await;
        insert_spec(
            &mut tx,
            &other_workspace_fallback,
            "other-workspace-fallback",
            6,
        )
        .await;
        let document = IdentitySpecDocumentWrite::new(
            b"secret".to_vec(),
            b"nonce".to_vec(),
            b"wrapped".to_vec(),
            b"wrapped-nonce".to_vec(),
            "key-1",
            "opaque-algorithm",
            7,
        )
        .expect("valid document");
        let first = tx
            .identity_spec_documents()
            .upsert(&global_shadow, &document, 5)
            .await
            .expect("insert document");
        let second = tx
            .identity_spec_documents()
            .upsert(&global_shadow, &document, 6)
            .await
            .expect("replace document");
        assert_eq!((first.document_version, second.document_version), (1, 2));
        assert_eq!(
            (second.created_at_unix_nanos, second.updated_at_unix_nanos),
            (5, 6)
        );
        assert!(!format!("{second:?}").contains("secret"));
        assert!(
            tx.identity_spec_documents()
                .delete(&global_shadow)
                .await
                .expect("delete")
        );
        tx.commit().await.expect("commit seed tx");

        let mut session = db;
        let exact = session
            .identity_specs()
            .load_optional(&global_shadow)
            .await
            .expect("load global");
        assert_eq!(
            exact.map(|record| record.version).as_deref(),
            Some("global-shadow")
        );
        let exact_workspace = session
            .identity_specs()
            .load_optional(&workspace_shadow)
            .await
            .expect("load workspace");
        assert_eq!(
            exact_workspace.map(|record| record.version).as_deref(),
            Some("workspace-shadow")
        );
        let workspace_records: Vec<_> = session
            .identity_specs()
            .list_workspace(&workspace)
            .await
            .expect("list workspace")
            .into_iter()
            .map(|record| (record.key, record.version))
            .collect();
        assert_eq!(
            workspace_records,
            vec![(workspace_shadow.clone(), "workspace-shadow".to_string())]
        );
        let missing_workspace_fallback = session
            .identity_specs()
            .load_optional(&workspace_fallback)
            .await
            .expect("load absent workspace fallback");
        assert!(missing_workspace_fallback.is_none());
        let shadow = session
            .identity_specs()
            .resolve_optional(&workspace_shadow)
            .await
            .expect("resolve shadow");
        assert_eq!(
            shadow.map(|record| record.version).as_deref(),
            Some("workspace-shadow")
        );
        let fallback = session
            .identity_specs()
            .resolve_optional(&workspace_fallback)
            .await
            .expect("resolve fallback");
        assert_eq!(
            fallback.map(|record| record.key),
            Some(global_fallback.clone())
        );
        let resolved: Vec<_> = session
            .identity_specs()
            .list_resolved_for_workspace(&workspace)
            .await
            .expect("list resolved")
            .into_iter()
            .filter(|record| record.key.name.ends_with(&suffix))
            .map(|record| (record.key.name, record.version))
            .collect();
        assert_eq!(
            resolved,
            vec![
                (shadow_name, "workspace-shadow".to_string()),
                (fallback_name, "global-fallback".to_string()),
            ]
        );

        let mut tx = db.begin().await.expect("begin cleanup tx");
        for key in [&global_shadow, &global_fallback] {
            tx.execute(
                Query::delete()
                    .from_table(IdentitySpecs::Table)
                    .and_where(identity_spec_key_where(key))
                    .to_owned(),
            )
            .await
            .expect("delete global spec");
        }
        for workspace_name in [&workspace, &other_workspace] {
            tx.workspaces()
                .delete(workspace_name.as_str())
                .await
                .expect("delete workspace");
        }
        tx.commit().await.expect("commit cleanup tx");
    }

    async fn insert_spec<S>(session: &mut S, key: &IdentitySpecKey, version: &str, now: i64)
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(IdentitySpecs::Table)
                    .columns(identity_spec_columns())
                    .values_panic([
                        Expr::val(key.scope.kind()),
                        Expr::val(key.scope.scope_id()),
                        Expr::val(key.scope.workspace_id().map(ToString::to_string)),
                        Expr::val(key.name.clone()),
                        Expr::val(version),
                        Expr::val("test identity spec"),
                        Expr::val("issuer"),
                        Expr::val("oauth"),
                        Expr::val("kind: identity\n"),
                        Expr::val(now),
                        Expr::val(now),
                    ])
                    .to_owned(),
            )
            .await
            .expect("insert identity spec");
    }
}
